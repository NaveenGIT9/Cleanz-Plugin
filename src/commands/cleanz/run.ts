/*
 * Copyright 2026, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { execSync, spawn } from 'node:child_process';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as readline from 'node:readline';
import { SfCommand, Flags } from '@salesforce/sf-plugins-core';
import { Messages } from '@salesforce/core';

Messages.importMessagesDirectoryFromMetaUrl(import.meta.url);
const messages = Messages.loadMessages('@salesforce/plugin-cleanz', 'cleanz.run');

// ===============================================================
// TYPES
// ===============================================================

type PromotionItem = {
  t: string;
  n: string;
};

type DeployResult = {
  status?: number;
  name?: string;
  message?: string;
  result?: {
    success?: boolean;
    details?: {
      componentFailures?: ComponentFailure[];
    };
  };
};

type ComponentFailure = {
  problem: string;
};

type SummaryRecord = {
  Type: string;
  Name: string;
  Status: string;
  RemovedFields: string;
  SkippedFields: string;
};

type TotalDeploys = { value: number };

// ── Whitelist map — one entry per metadata type ───────────────
type WhitelistMap = {
  fields: string[];
  apps: string[];
  classes: string[];
  pages: string[];
  tabs: string[];
  objects: string[];
  flows: string[];
};

// ── Global missing cache — confirmed absent from org ──────────
// Once discovered missing, every subsequent XML is pre-scrubbed
// for FREE (no deploy call needed) before its loop starts.
type GlobalMissingCache = {
  fields: Set<string>;
  apps: Set<string>;
  classes: Set<string>;
  pages: Set<string>;
  tabs: Set<string>;
  objects: Set<string>;
  flows: Set<string>;
};

function makeGlobalMissingCache(): GlobalMissingCache {
  return {
    fields: new Set(),
    apps: new Set(),
    classes: new Set(),
    pages: new Set(),
    tabs: new Set(),
    objects: new Set(),
    flows: new Set(),
  };
}

// ===============================================================
// CONSTANTS / CONFIG
// ===============================================================

const REPO_PATH = 'D:\\RubrikRepoVDI\\rbk-sfdc-release';
const PS_BASE_PATH = path.join(REPO_PATH, 'force-app', 'main', 'default', 'permissionsets');
const PROFILE_BASE_PATH = path.join(REPO_PATH, 'force-app', 'main', 'default', 'profiles');
const MAX_ITERATIONS = 500;
const MAX_TOTAL_DEPLOYS = 1000;
const DEPLOY_TIMEOUT_MINS = 3;
const MAX_RETRIES = 3;
const UNMATCHED_ERRORS_LOG = path.join(REPO_PATH, 'unmatched_errors.log');

// ===============================================================
// HELPERS
// ===============================================================

function prompt(question: string): Promise<string> {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      rl.close();
      resolve(answer.trim());
    });
  });
}

// ===============================================================
// HELPER: FORMAT XML — pure string-based, no xmldom needed
// ===============================================================

function formatXml(xml: string): string {
  let formatted = '';
  let indent = 0;
  // Normalise line endings first to avoid \r\n mixed-mode issues
  const lines = xml.replace(/\r\n/g, '\n').replace(/>\s*</g, '>\n<').split('\n');

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    if (trimmed.startsWith('</')) {
      indent = Math.max(0, indent - 1);
    }

    formatted += '    '.repeat(indent) + trimmed + '\n';

    if (
      !trimmed.startsWith('<?') &&
      !trimmed.startsWith('</') &&
      !trimmed.endsWith('/>') &&
      !trimmed.includes('</')
    ) {
      indent++;
    }
  }

  return formatted;
}

// ===============================================================
// HELPER: SAVE XML CLEANLY — pure string-based, no xmldom
// ===============================================================

function saveXmlClean(xmlContent: string, filePath: string, metadataType: string): void {
  // Strip ALL existing XML declarations first (handles duplicates from prior runs)
  let content = xmlContent.replace(/<\?xml[^?]*\?>\s*/gi, '');

  // Add exactly one declaration at the top
  content = '<?xml version="1.0" encoding="UTF-8"?>\n' + content;

  content = formatXml(content);

  // Fix malformed closing tags: </SomeTag></PermissionSet> -> </SomeTag>\n</PermissionSet>
  const closingTag = `</${metadataType}>`;
  const escapedClosing = closingTag.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const regex = new RegExp(`</(\\w+)>${escapedClosing}`, 'g');
  content = content.replace(regex, (_match: string, tagName: string) => `</${tagName}>\n${closingTag}`);

  fs.writeFileSync(filePath, content, 'utf8');
}

// ===============================================================
// HELPER: REMOVE fieldPermissions BLOCK — pure regex, no xmldom
// ===============================================================

function removeFieldPermissionsFromXml(
  xmlContent: string,
  missingField: string
): { updated: string; removed: boolean } {
  // Normalise line endings before regex matching
  const normalised = xmlContent.replace(/\r\n/g, '\n');
  const escapedField = missingField.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const innerPattern = '(?:(?!<fieldPermissions>)[\\s\\S])*?';
  const blockRegex = new RegExp(
    `[ \\t]*<fieldPermissions>${innerPattern}<field>[ \\t]*${escapedField}[ \\t]*</field>${innerPattern}</fieldPermissions>[ \\t]*\\r?\\n?`,
    'g'
  );
  const updated = normalised.replace(blockRegex, '');
  const removed = updated !== normalised;
  return { updated, removed };
}

function getRootNodeName(xmlContent: string): string {
  const match = /<(\w+)\s+xmlns=/i.exec(xmlContent) ?? /<(\w+)>/i.exec(xmlContent);
  return match ? match[1] : 'PermissionSet';
}

// ===============================================================
// GENERIC BLOCK REMOVER — reusable for any single-key XML block
// ===============================================================

function removeXmlBlock(
  xmlContent: string,
  blockTag: string,
  keyTag: string,
  missingName: string
): { updated: string; removed: boolean } {
  // Normalise line endings before regex matching
  const normalised = xmlContent.replace(/\r\n/g, '\n');
  const escapedName = missingName.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const escapedBlock = blockTag.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const innerPattern = `(?:(?!<${escapedBlock}>)[\\s\\S])*?`;
  const blockRegex = new RegExp(
    `[ \\t]*<${escapedBlock}>${innerPattern}<${keyTag}>[ \\t]*${escapedName}[ \\t]*</${keyTag}>${innerPattern}</${escapedBlock}>[ \\t]*\\r?\\n?`,
    'g'
  );
  const updated = normalised.replace(blockRegex, '');
  const removed = updated !== normalised;
  return { updated, removed };
}

// ── Typed wrappers ────────────────────────────────────────────

function removeApplicationVisibilityFromXml(xmlContent: string, name: string): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'applicationVisibilities', 'application', name);
}

function removeClassAccessFromXml(xmlContent: string, name: string): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'classAccesses', 'apexClass', name);
}

function removePageAccessFromXml(xmlContent: string, name: string): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'pageAccesses', 'apexPage', name);
}

function removeTabSettingFromXml(xmlContent: string, name: string): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'tabSettings', 'tab', name);
}

function removeObjectPermissionFromXml(xmlContent: string, name: string): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'objectPermissions', 'object', name);
}

function removeFlowAccessFromXml(xmlContent: string, name: string): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'flowAccesses', 'flow', name);
}

// ===============================================================
// HELPER: RUN DEPLOY WITH TIMEOUT & RETRY
// ===============================================================

const TRANSIENT_ERROR_PATTERNS = [
  /rate limit/i,
  /request limit/i,
  /too many requests/i,
  /ECONNRESET/i,
  /ECONNREFUSED/i,
  /ETIMEDOUT/i,
  /ENOTFOUND/i,
  /socket hang up/i,
  /network/i,
  /connection.*reset/i,
  /exceeded.*limit/i,
  /server.*unavailable/i,
  /503/,
  /502/,
  /504/,
  /session.*expired/i,
  /invalid.*session/i,
  /expired.*access/i,
  /authentication/i,
  /INVALID_SESSION_ID/i,
  /Cannot read properties of undefined/i,
];

function isTransientError(raw: string): boolean {
  return TRANSIENT_ERROR_PATTERNS.some((p) => p.test(raw));
}

function getBackoffMs(attempt: number): number {
  return Math.min(15_000 * Math.pow(2, attempt - 1), 120_000);
}

async function invokeDeployWithRetry(
  log: (msg: string) => void,
  metadataType: string,
  itemName: string,
  targetOrg: string,
  outputFile: string,
  timeoutMins: number,
  maxRetries: number
): Promise<DeployResult | null> {
  const MAX_TOTAL_ATTEMPTS = maxRetries + 10;
  let attempt = 0;
  let hardAttempt = 0;

  while (hardAttempt < MAX_TOTAL_ATTEMPTS) {
    attempt++;
    hardAttempt++;
    log(`   Deploy attempt ${attempt} ...`);

    if (fs.existsSync(outputFile)) fs.unlinkSync(outputFile);

    // eslint-disable-next-line no-await-in-loop
    const procResult = await runDeployProcess(metadataType, itemName, targetOrg, outputFile, timeoutMins);

    if (procResult === 'timeout') {
      log(`   Deploy timed out after ${timeoutMins} min(s). Retrying after backoff...`);
      // eslint-disable-next-line no-await-in-loop
      await sleep(getBackoffMs(attempt));
      continue;
    }

    if (!fs.existsSync(outputFile) || fs.statSync(outputFile).size === 0) {
      log('   Deploy output empty — retrying after backoff...');
      // eslint-disable-next-line no-await-in-loop
      await sleep(getBackoffMs(attempt));
      continue;
    }

    let raw = '';
    try {
      raw = fs.readFileSync(outputFile, 'utf8');
      const jsonStart = raw.indexOf('{');
      if (jsonStart > 0) raw = raw.substring(jsonStart);
    } catch {
      log('   Could not read deploy output — retrying...');
      // eslint-disable-next-line no-await-in-loop
      await sleep(getBackoffMs(attempt));
      continue;
    }

    if (isTransientError(raw)) {
      const backoff = getBackoffMs(attempt);
      log(`   Transient error detected — waiting ${backoff / 1000}s before retry...`);
      // eslint-disable-next-line no-await-in-loop
      await sleep(backoff);
      attempt = 0;
      continue;
    }

    let result: DeployResult;
    try {
      result = JSON.parse(raw) as DeployResult;
    } catch {
      log(`   Invalid JSON on attempt ${attempt} — retrying...`);
      // eslint-disable-next-line no-await-in-loop
      await sleep(getBackoffMs(attempt));
      continue;
    }

    const errText = `${result.message ?? ''} ${result.name ?? ''}`;
    if (!result.result && isTransientError(errText)) {
      const backoff = getBackoffMs(attempt);
      log(`   Transient SF CLI error (${result.name ?? 'unknown'}) — waiting ${backoff / 1000}s before retry...`);
      // eslint-disable-next-line no-await-in-loop
      await sleep(backoff);
      attempt = 0;
      continue;
    }

    if (!result.result && result.status !== undefined) {
      log(`   Normalising SF CLI response (status=${result.status}).`);
      result.result = {
        success: result.status === 0,
        details: { componentFailures: [] },
      };
    }

    log('   Deploy response received.');
    return result;
  }

  log(`   Giving up after ${hardAttempt} total attempts for: ${itemName}`);
  return null;
}

function runDeployProcess(
  metadataType: string,
  itemName: string,
  targetOrg: string,
  outputFile: string,
  timeoutMins: number
): Promise<'ok' | 'timeout'> {
  return new Promise((resolve) => {
    const args = [
      'project', 'deploy', 'start',
      '-m', `${metadataType}:${itemName}`,
      '--target-org', targetOrg,
      '--json',
      '--dry-run',
      '--wait', '2',
    ];

    const proc = spawn('sf', args, { shell: true });
    const outputStream = fs.createWriteStream(outputFile, { encoding: 'utf8' });

    proc.stdout.pipe(outputStream);
    proc.stderr.pipe(outputStream);

    const timer = setTimeout(() => {
      proc.kill();
      resolve('timeout');
    }, timeoutMins * 60 * 1000);

    proc.on('close', () => {
      clearTimeout(timer);
      outputStream.end();
      resolve('ok');
    });
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ===============================================================
// HELPER: WHITELIST + REPO CHECK — reusable across all types
// Returns true if the item should be SKIPPED (not removed)
// ===============================================================

function shouldSkip(
  log: (msg: string) => void,
  label: string,
  name: string,
  whitelistEntries: string[],
  repoFilePath: string,
  skippedFields: string[],
  allSkippedFields: string[]
): boolean {
  if (whitelistEntries.includes(name)) {
    log(`   SKIPPING whitelisted ${label} (in JSON): ${name}`);
    const entry = `[${label.charAt(0).toUpperCase() + label.slice(1)}] ${name}`;
    skippedFields.push(entry);
    allSkippedFields.push(entry);
    return true;
  }

  if (fs.existsSync(repoFilePath)) {
    log(`   SKIPPING ${label} exists in repo but missing from org: ${name}`);
    log(`   WARNING: Deploy the ${label} first, then re-run this script.`);
    log(`   Found at: ${repoFilePath}`);
    const entry = `[${label.charAt(0).toUpperCase() + label.slice(1)}] ${name}`;
    skippedFields.push(entry);
    allSkippedFields.push(entry);
    return true;
  }

  return false;
}

// ===============================================================
// METADATA HANDLER REGISTRY
// Each handler now has MULTIPLE patterns (fallbacks) so SF CLI
// wording variations are handled gracefully.
// ===============================================================

type MetadataHandler = {
  patterns: RegExp[];   // Multiple fallback patterns — first match wins
  label: string;
  whitelistKey: keyof WhitelistMap;
  cacheKey: keyof GlobalMissingCache;
  repoPathFn: (repoPath: string, name: string) => string;
  removeFn: (xml: string, name: string) => { updated: string; removed: boolean };
  displayTag: string;
};

const METADATA_HANDLERS: MetadataHandler[] = [
  {
    patterns: [
      /no CustomApplication named (.+?) found/i,
      /Entity of type 'CustomApplication' named '(.+?)' cannot be found/i,
      /CustomApplication[:\s]+(.+?) does not exist/i,
      /In field: application - no CustomApplication named (.+?) found/i,
    ],
    label: 'app', whitelistKey: 'apps', cacheKey: 'apps',
    repoPathFn: (r, n) => path.join(r, 'force-app', 'main', 'default', 'applications', `${n}.app-meta.xml`),
    removeFn: removeApplicationVisibilityFromXml,
    displayTag: '[App]',
  },
  {
    patterns: [
      /no ApexClass named (.+?) found/i,
      /Entity of type 'ApexClass' named '(.+?)' cannot be found/i,
      /ApexClass[:\s]+(.+?) does not exist/i,
      /In field: apexClass - no ApexClass named (.+?) found/i,
    ],
    label: 'class', whitelistKey: 'classes', cacheKey: 'classes',
    repoPathFn: (r, n) => path.join(r, 'force-app', 'main', 'default', 'classes', `${n}.cls`),
    removeFn: removeClassAccessFromXml,
    displayTag: '[Class]',
  },
  {
    patterns: [
      /no ApexPage named (.+?) found/i,
      /Entity of type 'ApexPage' named '(.+?)' cannot be found/i,
      /ApexPage[:\s]+(.+?) does not exist/i,
      /In field: apexPage - no ApexPage named (.+?) found/i,
      /no ApexPage named (.+?) found in your org/i,
    ],
    label: 'page', whitelistKey: 'pages', cacheKey: 'pages',
    repoPathFn: (r, n) => path.join(r, 'force-app', 'main', 'default', 'pages', `${n}.page`),
    removeFn: removePageAccessFromXml,
    displayTag: '[Page]',
  },
  {
    patterns: [
      /no CustomTab named (.+?) found/i,
      /Entity of type 'CustomTab' named '(.+?)' cannot be found/i,
      /CustomTab[:\s]+(.+?) does not exist/i,
      /In field: tab - no CustomTab named (.+?) found/i,
    ],
    label: 'tab', whitelistKey: 'tabs', cacheKey: 'tabs',
    repoPathFn: (r, n) => path.join(r, 'force-app', 'main', 'default', 'tabs', `${n}.tab-meta.xml`),
    removeFn: removeTabSettingFromXml,
    displayTag: '[Tab]',
  },
  {
    patterns: [
      /no CustomObject named (.+?) found/i,
      /Entity of type 'CustomObject' named '(.+?)' cannot be found/i,
      /CustomObject[:\s]+(.+?) does not exist/i,
      /In field: object - no CustomObject named (.+?) found/i,
    ],
    label: 'object', whitelistKey: 'objects', cacheKey: 'objects',
    repoPathFn: (r, n) => path.join(r, 'force-app', 'main', 'default', 'objects', n, `${n}.object-meta.xml`),
    removeFn: removeObjectPermissionFromXml,
    displayTag: '[Object]',
  },
  {
    patterns: [
      /no Flow named (.+?) found/i,
      /Entity of type 'Flow' named '(.+?)' cannot be found/i,
      /Flow[:\s]+(.+?) does not exist/i,
      /In field: flow - no Flow named (.+?) found/i,
      /no Flow named (.+?) found in your org/i,
      /no active version.*Flow named (.+?) found/i,
    ],
    label: 'flow', whitelistKey: 'flows', cacheKey: 'flows',
    repoPathFn: (r, n) => path.join(r, 'force-app', 'main', 'default', 'flows', `${n}.flow-meta.xml`),
    removeFn: removeFlowAccessFromXml,
    displayTag: '[Flow]',
  },
];

// ── Field patterns — same multi-pattern approach ──────────────
const FIELD_PATTERNS: RegExp[] = [
  /no CustomField named (.+?) found/i,
  /Entity of type 'CustomField' named '(.+?)' cannot be found/i,
  /CustomField[:\s]+(.+?) does not exist/i,
  /In field: field - no CustomField named (.+?) found/i,
  /no CustomField named (.+?) found in your org/i,
];

// ===============================================================
// HELPER: LOG UNMATCHED ERRORS TO FILE
// Lets you see the exact SF wording so you can add new patterns.
// ===============================================================

function logUnmatchedError(repoPath: string, itemName: string, errorMessage: string): void {
  const timestamp = new Date().toISOString();
  const line = `[${timestamp}] [${itemName}] ${errorMessage}\n`;
  try {
    fs.appendFileSync(UNMATCHED_ERRORS_LOG, line, 'utf8');
  } catch {
    // best-effort — don't crash the script over a log file
  }
}

// ===============================================================
// HELPER: PRE-SCRUB — apply all globally known missing items
// to a file before its deploy loop starts (zero deploy cost).
// ===============================================================

function applyGlobalMissingCacheToFile(
  log: (msg: string) => void,
  xmlContent: string,
  globalMissing: GlobalMissingCache,
  itemName: string
): { xmlContent: string; removedItems: string[] } {
  let updated = xmlContent;
  const removedItems: string[] = [];

  const totalCached =
    globalMissing.fields.size +
    globalMissing.apps.size +
    globalMissing.classes.size +
    globalMissing.pages.size +
    globalMissing.tabs.size +
    globalMissing.objects.size +
    globalMissing.flows.size;

  if (totalCached === 0) return { xmlContent: updated, removedItems };

  log(`   [Pre-scrub] ${totalCached} globally known missing reference(s) — applying without a deploy call...`);

  // Fields
  for (const field of globalMissing.fields) {
    const { updated: u, removed } = removeFieldPermissionsFromXml(updated, field);
    if (removed) {
      updated = u;
      removedItems.push(field);
      log(`   [Pre-scrub] Removed field: ${field}`);
    }
  }

  // All registered handler types
  for (const handler of METADATA_HANDLERS) {
    const cacheSet = globalMissing[handler.cacheKey];
    for (const name of cacheSet) {
      const { updated: u, removed } = handler.removeFn(updated, name);
      if (removed) {
        updated = u;
        removedItems.push(`${handler.displayTag} ${name}`);
        log(`   [Pre-scrub] Removed ${handler.label}: ${name}`);
      }
    }
  }

  if (removedItems.length > 0) {
    log(`   [Pre-scrub] Cleaned ${removedItems.length} reference(s) from ${itemName} before first deploy.`);
  } else {
    log('   [Pre-scrub] No cached references found in this file. Proceeding normally.');
  }

  return { xmlContent: updated, removedItems };
}

// ===============================================================
// HELPER: PROCESS SINGLE FIELD FAILURE
// ===============================================================

function processFieldFailure(
  log: (msg: string) => void,
  errorMessage: string,
  xmlContent: string,
  whitelist: WhitelistMap,
  globalMissing: GlobalMissingCache,
  repoPath: string,
  removedFields: string[],
  skippedFields: string[],
  allSkippedFields: string[]
): { handled: boolean; xmlContent: string } {
  let missingField: string | null = null;

  for (const pattern of FIELD_PATTERNS) {
    const match = pattern.exec(errorMessage);
    if (match) { missingField = match[1].trim(); break; }
  }

  if (!missingField) return { handled: false, xmlContent };

  const parts = missingField.split('.');
  const fieldRepoPath = parts.length === 2
    ? path.join(repoPath, 'force-app', 'main', 'default', 'objects', parts[0], 'fields', `${parts[1]}.field-meta.xml`)
    : '';

  if (shouldSkip(log, 'field', missingField, whitelist.fields, fieldRepoPath, skippedFields, allSkippedFields)) {
    return { handled: true, xmlContent };
  }

  log(`   Missing field: ${missingField}`);
  const { updated, removed } = removeFieldPermissionsFromXml(xmlContent, missingField);
  if (removed) {
    log(`   Removed fieldPermissions for: ${missingField}`);
    removedFields.push(missingField);
    // ✅ Cache globally so all subsequent items skip this deploy
    globalMissing.fields.add(missingField);
    return { handled: true, xmlContent: updated };
  }
  log(`   Field not found in XML: ${missingField} - already removed or not present.`);
  return { handled: true, xmlContent };
}

// ===============================================================
// HELPER: PROCESS SINGLE REGISTERED METADATA FAILURE
// ===============================================================

function processRegisteredFailure(
  log: (msg: string) => void,
  errorMessage: string,
  xmlContent: string,
  whitelist: WhitelistMap,
  globalMissing: GlobalMissingCache,
  repoPath: string,
  removedFields: string[],
  skippedFields: string[],
  allSkippedFields: string[]
): { handled: boolean; xmlContent: string } {
  for (const handler of METADATA_HANDLERS) {
    let name: string | null = null;

    // Try every fallback pattern — first match wins
    for (const pattern of handler.patterns) {
      const match = pattern.exec(errorMessage);
      if (match) { name = match[1].trim(); break; }
    }

    if (!name) continue;

    const repoFilePath = handler.repoPathFn(repoPath, name);

    if (shouldSkip(log, handler.label, name, whitelist[handler.whitelistKey], repoFilePath, skippedFields, allSkippedFields)) {
      return { handled: true, xmlContent };
    }

    log(`   Missing ${handler.label}: ${name}`);
    const { updated, removed } = handler.removeFn(xmlContent, name);
    if (removed) {
      log(`   Removed ${handler.label} block for: ${name}`);
      removedFields.push(`${handler.displayTag} ${name}`);
      // ✅ Cache globally so all subsequent items skip this deploy
      (globalMissing[handler.cacheKey]).add(name);
      return { handled: true, xmlContent: updated };
    }
    log(`   ${handler.label} block not found in XML: ${name} - already removed or not present.`);
    return { handled: true, xmlContent };
  }

  return { handled: false, xmlContent };
}

// ===============================================================
// HELPER: PROCESS ALL FAILURES FOR ONE ITERATION
// ===============================================================

function processFailures(
  log: (msg: string) => void,
  failures: ComponentFailure[],
  xmlContent: string,
  whitelist: WhitelistMap,
  globalMissing: GlobalMissingCache,
  repoPath: string,
  itemName: string,
  allSkippedFields: string[]
): { xmlContent: string; removedFields: string[]; skippedFields: string[] } {
  let updatedXml = xmlContent;
  const removedFields: string[] = [];
  const skippedFields: string[] = [];
  const unmatchedErrors: string[] = [];

  log(`   [DEBUG] Total failures this iteration: ${failures.length}`);
  failures.forEach((f, i) => log(`   [DEBUG] Failure ${i + 1}: ${f.problem}`));

  for (const failure of failures) {
    const errorMessage = failure.problem;

    // ── CustomField ──────────────────────────────────────────
    const fieldResult = processFieldFailure(
      log, errorMessage, updatedXml, whitelist, globalMissing, repoPath,
      removedFields, skippedFields, allSkippedFields
    );
    if (fieldResult.handled) {
      updatedXml = fieldResult.xmlContent;
      continue;
    }

    // ── All other metadata types via registry ─────────────────
    const registryResult = processRegisteredFailure(
      log, errorMessage, updatedXml, whitelist, globalMissing, repoPath,
      removedFields, skippedFields, allSkippedFields
    );
    if (registryResult.handled) {
      updatedXml = registryResult.xmlContent;
      continue;
    }

    // ── Unmatched — log to file for pattern analysis ──────────
    unmatchedErrors.push(errorMessage);
    log(`   [UNMATCHED] ${errorMessage}`);
  }

  // Persist unmatched errors so you can inspect exact SF wording
  if (unmatchedErrors.length > 0) {
    unmatchedErrors.forEach((e) => logUnmatchedError(repoPath, itemName, e));
    log(`   [WARN] ${unmatchedErrors.length} unmatched error(s) appended to: ${UNMATCHED_ERRORS_LOG}`);
    log('   [WARN] Check that file to add new patterns to METADATA_HANDLERS or FIELD_PATTERNS.');
  }

  return { xmlContent: updatedXml, removedFields, skippedFields };
}

// ===============================================================
// HELPER: PROCESS SINGLE METADATA ITEM
// ===============================================================

async function invokeProcessMetadataItem(
  log: (msg: string) => void,
  params: {
    metadataType: string;
    itemName: string;
    filePath: string;
    targetOrg: string;
    repoPath: string;
    whitelist: WhitelistMap;
    globalMissing: GlobalMissingCache;   // ✅ shared across all items
    maxIterations: number;
    maxTotalDeploys: number;
    totalDeploys: TotalDeploys;
    timeoutMins: number;
    maxRetries: number;
  }
): Promise<SummaryRecord> {
  const {
    metadataType, itemName, filePath, targetOrg, repoPath,
    whitelist, globalMissing, maxIterations, maxTotalDeploys,
    totalDeploys, timeoutMins, maxRetries,
  } = params;

  const deployErrorsFile = path.join(repoPath, `deploy_errors_${itemName}.json`);
  let iteration = 0;
  let itemStatus = 'No Change';
  const allRemovedFields: string[] = [];
  const allSkippedFields: string[] = [];

  const icon = metadataType === 'Profile' ? 'Profile' : 'PermSet';
  log('\n================================================');
  log(`[${icon}] Processing ${metadataType} : ${itemName}`);
  log('================================================');

  if (!fs.existsSync(filePath)) {
    log(`File not found, skipping: ${filePath}`);
    return { Type: metadataType, Name: itemName, Status: 'File Not Found', RemovedFields: '', SkippedFields: '' };
  }

  // ================================================================
  // PRE-SCRUB PASS — apply all globally confirmed missing references
  // BEFORE the first deploy call. Zero network cost, saves deploy
  // slots when earlier items already discovered these failures.
  // ================================================================
  {
    const rawXml = fs.readFileSync(filePath, 'utf8');
    const { xmlContent: scrubbed, removedItems } = applyGlobalMissingCacheToFile(
      log, rawXml, globalMissing, itemName
    );

    if (removedItems.length > 0) {
      const rootNode = getRootNodeName(scrubbed);
      saveXmlClean(scrubbed, filePath, rootNode);
      allRemovedFields.push(...removedItems);
      itemStatus = 'Fixed & Committed';

      try {
        execSync(`git add "${filePath}"`, { cwd: repoPath });
        execSync(`git commit -m "[${itemName}] Pre-scrub: remove globally known missing: ${removedItems.join(', ')}"`, { cwd: repoPath });
        log(`   [Pre-scrub] Committed pre-scrub changes for: ${itemName}`);
      } catch {
        log(`   [Pre-scrub] Nothing to commit or commit failed for: ${itemName}`);
      }
    }
  }

  // ================================================================
  // MAIN DEPLOY LOOP — only runs for errors not yet in global cache
  // ================================================================
  let continueLoop = true;

  while (continueLoop && iteration < maxIterations) {
    iteration++;
    totalDeploys.value++;

    if (totalDeploys.value > maxTotalDeploys) {
      log(`Global deploy limit reached (${maxTotalDeploys}). Stopping entire script.`);
      itemStatus = 'Stopped - Global Limit Reached';
      break;
    }

    log(`\n--- [${itemName}] Iteration ${iteration} | Total Deploys Used: ${totalDeploys.value} / ${maxTotalDeploys} ---`);
    log('Running dry-run deploy...');

    // eslint-disable-next-line no-await-in-loop
    const deployResult = await invokeDeployWithRetry(
      log, metadataType, itemName, targetOrg,
      deployErrorsFile, timeoutMins, maxRetries
    );

    if (!deployResult) {
      log(`[${itemName}] Deploy failed after all attempts. Moving on.`);
      itemStatus = 'Deploy Failed - Exhausted Retries';
      break;
    }

    if (!deployResult.result) {
      log(`[${itemName}] SF CLI returned an unrecognised response shape. Raw keys: ${Object.keys(deployResult).join(', ')}`);
      log(`[${itemName}] Full response: ${JSON.stringify(deployResult)}`);
      log(`[${itemName}] Stopping — re-run the script once the org is reachable.`);
      itemStatus = 'Deploy Failed - Unrecognised Response';
      break;
    }

    if (deployResult.result.success === true) {
      log(`[${itemName}] Deploy validation SUCCESSFUL!`);
      itemStatus = 'Fixed & Committed';
      break;
    }

    const failures = deployResult.result.details?.componentFailures;
    if (!failures || failures.length === 0) {
      log(`[${itemName}] No component failures found. Moving on.`);
      itemStatus = 'Success';
      break;
    }

    const xmlContent = fs.readFileSync(filePath, 'utf8');
    const rootNode = getRootNodeName(xmlContent);

    const { xmlContent: updatedXml, removedFields, skippedFields } = processFailures(
      log, failures, xmlContent, whitelist, globalMissing, repoPath, itemName, allSkippedFields
    );

    allRemovedFields.push(...removedFields);

    if (removedFields.length === 0) {
      if (skippedFields.length > 0) {
        log(`[${itemName}] Only whitelisted/repo items remain. Manual deploy needed.`);
        itemStatus = 'Whitelisted Items Only - Manual Deploy Needed';
      } else {
        log(`[${itemName}] No items removed this iteration. Moving on.`);
        itemStatus = 'Partial / Manual Check Needed';
      }
      continueLoop = false;
      break;
    }

    saveXmlClean(updatedXml, filePath, rootNode);
    log(`Saved updated XML for: ${itemName}`);

    const commitMessage = `[${itemName}] Auto-remove missing metadata: ${removedFields.join(', ')}`;
    log(`Committing changes for ${itemName}...`);

    try {
      execSync(`git add "${filePath}"`, { cwd: repoPath });
      execSync(`git commit -m "${commitMessage}"`, { cwd: repoPath });
      log(`Commit successful for: ${itemName}`);
      itemStatus = 'Fixed & Committed';
    } catch {
      log(`Nothing to commit or commit failed for: ${itemName}`);
      itemStatus = 'Commit Failed';
    }

    if (iteration >= maxIterations) {
      log(`[${itemName}] Reached max iterations. Check remaining errors manually.`);
      itemStatus = 'Max Iterations Reached';
    }
  }

  if (fs.existsSync(deployErrorsFile)) fs.unlinkSync(deployErrorsFile);

  return {
    Type: metadataType,
    Name: itemName,
    Status: itemStatus,
    RemovedFields: allRemovedFields.join('; '),
    SkippedFields: allSkippedFields.join('; '),
  };
}

// ===============================================================
// MODULE-LEVEL PHASE HELPERS
// Kept outside the class so ESLint's class-methods-use-this rule
// is satisfied — none of these need 'this', they are pure logic.
// ===============================================================

// ── Phase 1: interactive prompts ─────────────────────────────
async function resolveInputs(
  log: (msg: string) => void,
  jsonPathFlag: string,
  targetOrgFlag: string
): Promise<{ promotionJsonPath: string; targetOrg: string }> {
  log('\n======================================================');
  log('  AUTOMATED PERMISSION SET & PROFILE DEPLOY & FIX');
  log('======================================================\n');

  let promotionJsonPath = jsonPathFlag;
  while (!promotionJsonPath || !fs.existsSync(promotionJsonPath)) {
    if (promotionJsonPath) log('   File not found at that path. Please try again.\n');
    // eslint-disable-next-line no-await-in-loop
    promotionJsonPath = await prompt(
      'Enter full path to your Copado Promotion JSON\n   (e.g. C:\\Users\\YourName\\Desktop\\promotion.json)\n> '
    );
    promotionJsonPath = promotionJsonPath.replace(/^"|"$/g, '').trim();
  }
  log('   JSON file found.\n');

  let targetOrg = targetOrgFlag;
  if (!targetOrg) {
    // eslint-disable-next-line no-await-in-loop
    targetOrg = await prompt(
      'Enter target org username or alias\n   (e.g. RBKQA or user@rubrik.com.qa)\n> '
    );
    targetOrg = targetOrg.trim();
  }
  log(`\n   Target Org set to: ${targetOrg}\n`);
  return { promotionJsonPath, targetOrg };
}

// ── Phase 2: parse JSON → lists + whitelist ───────────────────
function parsePromotionJson(jsonPath: string): {
  permSets: string[];
  profiles: string[];
  whitelist: WhitelistMap;
} {
  const data = JSON.parse(fs.readFileSync(jsonPath, 'utf8')) as PromotionItem[];
  const uniq = (t: string): string[] => [...new Set(data.filter((i) => i.t === t).map((i) => i.n))].sort();
  return {
    permSets: uniq('PermissionSet'),
    profiles: uniq('Profile'),
    whitelist: {
      fields: uniq('CustomField'),
      apps: uniq('CustomApplication'),
      classes: uniq('ApexClass'),
      pages: uniq('ApexPage'),
      tabs: uniq('CustomTab'),
      objects: uniq('CustomObject'),
      flows: uniq('Flow'),
    },
  };
}

// ── Phase 3: startup banner + confirmation prompt ─────────────
async function printBannerAndConfirm(
  log: (msg: string) => void,
  targetOrg: string,
  permSets: string[],
  profiles: string[],
  whitelist: WhitelistMap
): Promise<boolean> {
  const totalWhitelisted = Object.values(whitelist).reduce((s, a) => s + a.length, 0);

  log('\n======================================================');
  log('STARTING AUTOMATED DEPLOY & FIX LOOP');
  log('======================================================');
  log(`Target Org              : ${targetOrg}`);
  log(`Permission Sets         : ${permSets.length} found in JSON`);
  log(`Profiles                : ${profiles.length} found in JSON`);
  log(`Whitelisted total       : ${totalWhitelisted} items across all types (will never be removed)`);
  log(`  - CustomFields        : ${whitelist.fields.length}`);
  log(`  - CustomApplications  : ${whitelist.apps.length}`);
  log(`  - ApexClasses         : ${whitelist.classes.length}`);
  log(`  - ApexPages           : ${whitelist.pages.length}`);
  log(`  - CustomTabs          : ${whitelist.tabs.length}`);
  log(`  - CustomObjects       : ${whitelist.objects.length}`);
  log(`  - Flows               : ${whitelist.flows.length}`);
  log(`Max per item            : ${MAX_ITERATIONS} iterations`);
  log(`Global deploy cap       : ${MAX_TOTAL_DEPLOYS} total deploys`);
  log(`Deploy timeout          : ${DEPLOY_TIMEOUT_MINS} min(s) per attempt`);
  log(`Max retries             : ${MAX_RETRIES} per deploy call`);
  log(`Unmatched errors log    : ${UNMATCHED_ERRORS_LOG}`);

  log('\nPermission Sets to process:');
  permSets.forEach((ps) => log(`   - ${ps}`));
  log('\nProfiles to process:');
  profiles.forEach((p) => log(`   - ${p}`));

  log('\nWhitelisted items (will never be removed):');
  log('  Fields   : ' + (whitelist.fields.join(', ') || 'none'));
  log('  Apps     : ' + (whitelist.apps.join(', ') || 'none'));
  log('  Classes  : ' + (whitelist.classes.join(', ') || 'none'));
  log('  Pages    : ' + (whitelist.pages.join(', ') || 'none'));
  log('  Tabs     : ' + (whitelist.tabs.join(', ') || 'none'));
  log('  Objects  : ' + (whitelist.objects.join(', ') || 'none'));
  log('  Flows    : ' + (whitelist.flows.join(', ') || 'none'));

  log('');
  const confirm = await prompt("Press ENTER to start or type 'exit' to cancel\n> ");
  if (confirm.trim().toLowerCase() === 'exit') {
    log('\nScript cancelled by user.');
    return false;
  }
  log('\nStarting script...');
  log('\n======================================================');
  return true;
}

// ── Phase 4: process all items (perm sets then profiles) ──────
async function processAllItems(
  log: (msg: string) => void,
  permSets: string[],
  profiles: string[],
  targetOrg: string,
  whitelist: WhitelistMap,
  globalMissing: GlobalMissingCache
): Promise<{ summary: SummaryRecord[]; totalDeploys: TotalDeploys }> {
  const summary: SummaryRecord[] = [];
  const totalDeploys: TotalDeploys = { value: 0 };

  const commonParams = {
    targetOrg,
    repoPath: REPO_PATH,
    whitelist,
    globalMissing,
    maxIterations: MAX_ITERATIONS,
    maxTotalDeploys: MAX_TOTAL_DEPLOYS,
    totalDeploys,
    timeoutMins: DEPLOY_TIMEOUT_MINS,
    maxRetries: MAX_RETRIES,
  };

  log('\n######################################################');
  log(`  PROCESSING PERMISSION SETS (${permSets.length})`);
  log('######################################################');

  for (const psName of permSets) {
    // eslint-disable-next-line no-await-in-loop
    summary.push(await invokeProcessMetadataItem(log, {
      ...commonParams,
      metadataType: 'PermissionSet',
      itemName: psName,
      filePath: path.join(PS_BASE_PATH, `${psName}.permissionset-meta.xml`),
    }));
    if (totalDeploys.value > MAX_TOTAL_DEPLOYS) break;
  }

  if (totalDeploys.value <= MAX_TOTAL_DEPLOYS) {
    log('\n######################################################');
    log(`  PROCESSING PROFILES (${profiles.length})`);
    log('######################################################');

    for (const profileName of profiles) {
      // eslint-disable-next-line no-await-in-loop
      summary.push(await invokeProcessMetadataItem(log, {
        ...commonParams,
        metadataType: 'Profile',
        itemName: profileName,
        filePath: path.join(PROFILE_BASE_PATH, `${profileName}.profile-meta.xml`),
      }));
      if (totalDeploys.value > MAX_TOTAL_DEPLOYS) break;
    }
  }

  return { summary, totalDeploys };
}

// ── Phase 5: print final summary + write CSV ──────────────────
function printFinalSummary(
  log: (msg: string) => void,
  summary: SummaryRecord[],
  totalDeploys: TotalDeploys,
  globalMissing: GlobalMissingCache
): void {
  log('\n======================================================');
  log('ALL ITEMS PROCESSED - FINAL SUMMARY');
  log('======================================================');

  const totalCached =
    globalMissing.fields.size + globalMissing.apps.size + globalMissing.classes.size +
    globalMissing.pages.size + globalMissing.tabs.size + globalMissing.objects.size +
    globalMissing.flows.size;

  log(`\nGlobal Missing Cache (discovered during this run): ${totalCached} unique missing references`);
  if (globalMissing.fields.size) log(`  Fields   : ${[...globalMissing.fields].join(', ')}`);
  if (globalMissing.apps.size) log(`  Apps     : ${[...globalMissing.apps].join(', ')}`);
  if (globalMissing.classes.size) log(`  Classes  : ${[...globalMissing.classes].join(', ')}`);
  if (globalMissing.pages.size) log(`  Pages    : ${[...globalMissing.pages].join(', ')}`);
  if (globalMissing.tabs.size) log(`  Tabs     : ${[...globalMissing.tabs].join(', ')}`);
  if (globalMissing.objects.size) log(`  Objects  : ${[...globalMissing.objects].join(', ')}`);
  if (globalMissing.flows.size) log(`  Flows    : ${[...globalMissing.flows].join(', ')}`);

  if (fs.existsSync(UNMATCHED_ERRORS_LOG)) {
    log(`\n[WARN] Unmatched SF errors were logged to: ${UNMATCHED_ERRORS_LOG}`);
    log('[WARN] Review that file and add new regex patterns to METADATA_HANDLERS or FIELD_PATTERNS.');
  }

  const row = (r: SummaryRecord): string =>
    `   [${r.Name}] Status: ${r.Status} | Removed: ${r.RemovedFields || 'none'} | Skipped: ${r.SkippedFields || 'none'}`;

  log('\nPERMISSION SETS:');
  summary.filter((r) => r.Type === 'PermissionSet').forEach((r) => log(row(r)));

  log('\nPROFILES:');
  summary.filter((r) => r.Type === 'Profile').forEach((r) => log(row(r)));

  const csvPath = path.join(REPO_PATH, 'deploy_fix_summary.csv');
  const csvHeader = 'Type,Name,Status,RemovedFields,SkippedFields';
  const csvRows = summary.map(
    (r) => `${r.Type},"${r.Name}","${r.Status}","${r.RemovedFields}","${r.SkippedFields}"`
  );
  fs.writeFileSync(csvPath, [csvHeader, ...csvRows].join('\n'), 'utf8');

  log(`\nSummary CSV saved to : ${csvPath}`);
  log(`Total deploy calls   : ${totalDeploys.value} / ${MAX_TOTAL_DEPLOYS}`);
}

// ===============================================================
// SF PLUGIN COMMAND
// The class itself is intentionally thin — run() delegates every
// phase to the module-level functions above so that:
//   (a) cyclomatic complexity stays well under the limit of 20
//   (b) class-methods-use-this is fully satisfied (only run()
//       is on the class, and it uses this.parse / this.log)
// ===============================================================

export default class DeployAndFix extends SfCommand<void> {
  public static readonly summary = messages.getMessage('summary');
  public static readonly description = messages.getMessage('description');
  public static readonly examples = messages.getMessages('examples');

  public static readonly flags = {
    'json-path': Flags.string({
      char: 'j',
      summary: messages.getMessage('flags.json-path.summary'),
      required: false,
    }),
    'target-org': Flags.string({
      char: 't',
      summary: messages.getMessage('flags.target-org.summary'),
      required: false,
    }),
  };

  public async run(): Promise<void> {
    const { flags } = await this.parse(DeployAndFix);
    const log = (msg: string): void => { this.log(msg); };

    // Phase 1 — resolve inputs
    const { promotionJsonPath, targetOrg } = await resolveInputs(
      log, flags['json-path'] ?? '', flags['target-org'] ?? ''
    );

    // Phase 2 — parse JSON
    const { permSets, profiles, whitelist } = parsePromotionJson(promotionJsonPath);

    // Phase 3 — banner + confirmation
    const confirmed = await printBannerAndConfirm(log, targetOrg, permSets, profiles, whitelist);
    if (!confirmed) return;

    // Single shared cache — grows across all items so each
    // subsequent file is pre-scrubbed for free before its first deploy.
    const globalMissing = makeGlobalMissingCache();

    // Phase 4 — process all items
    const { summary, totalDeploys } = await processAllItems(
      log, permSets, profiles, targetOrg, whitelist, globalMissing
    );

    // Phase 5 — final summary
    printFinalSummary(log, summary, totalDeploys, globalMissing);
  }
}