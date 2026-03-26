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

// ── Global missing cache ──────────────────────────────────────
// Populated ONLY from actual deploy validation errors.
// Used so subsequent items get those known-missing refs
// removed without burning a deploy call for each one.
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
// XML FORMATTING & SAVING
// ===============================================================

function formatXml(xml: string): string {
  let formatted = '';
  let indent = 0;
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

function saveXmlClean(xmlContent: string, filePath: string, metadataType: string): void {
  let content = xmlContent.replace(/<\?xml[^?]*\?>\s*/gi, '');
  content = '<?xml version="1.0" encoding="UTF-8"?>\n' + content;
  content = formatXml(content);

  const closingTag = `</${metadataType}>`;
  const escapedClose = closingTag.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const regex = new RegExp(`</(\\w+)>${escapedClose}`, 'g');
  content = content.replace(regex, (_m: string, tag: string) => `</${tag}>\n${closingTag}`);

  fs.writeFileSync(filePath, content, 'utf8');
}

function getRootNodeName(xmlContent: string): string {
  const match = /<(\w+)\s+xmlns=/i.exec(xmlContent) ?? /<(\w+)>/i.exec(xmlContent);
  return match ? match[1] : 'PermissionSet';
}

// ===============================================================
// XML BLOCK REMOVERS
// ===============================================================

function removeFieldPermissionsFromXml(
  xmlContent: string,
  missingField: string
): { updated: string; removed: boolean } {
  const norm = xmlContent.replace(/\r\n/g, '\n');
  const escapedField = missingField.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const inner = '(?:(?!<\\/?fieldPermissions>)[\\s\\S])*?';
  const re = new RegExp(
    `[ \\t]*<fieldPermissions>${inner}<field>[ \\t]*${escapedField}[ \\t]*</field>${inner}</fieldPermissions>[ \\t]*\\r?\\n?`,
    'g'
  );
  const updated = norm.replace(re, '');
  return { updated, removed: updated !== norm };
}

function removeXmlBlock(
  xmlContent: string,
  blockTag: string,
  keyTag: string,
  name: string
): { updated: string; removed: boolean } {
  const norm = xmlContent.replace(/\r\n/g, '\n');
  const escapedName = name.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const escapedTag = blockTag.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const inner = `(?:(?!<\\/?${escapedTag}>)[\\s\\S])*?`;
  const re = new RegExp(
    `[ \\t]*<${escapedTag}>${inner}<${keyTag}>[ \\t]*${escapedName}[ \\t]*</${keyTag}>${inner}</${escapedTag}>[ \\t]*\\r?\\n?`,
    'g'
  );
  const updated = norm.replace(re, '');
  return { updated, removed: updated !== norm };
}

const removeApplicationVisibilityFromXml = (xml: string, n: string): { updated: string; removed: boolean } => removeXmlBlock(xml, 'applicationVisibilities', 'application', n);
const removeClassAccessFromXml = (xml: string, n: string): { updated: string; removed: boolean } => removeXmlBlock(xml, 'classAccesses', 'apexClass', n);
const removePageAccessFromXml = (xml: string, n: string): { updated: string; removed: boolean } => removeXmlBlock(xml, 'pageAccesses', 'apexPage', n);
const removeTabSettingFromXml = (xml: string, n: string): { updated: string; removed: boolean } => removeXmlBlock(xml, 'tabSettings', 'tab', n);
const removeObjectPermissionFromXml = (xml: string, n: string): { updated: string; removed: boolean } => removeXmlBlock(xml, 'objectPermissions', 'object', n);
const removeFlowAccessFromXml = (xml: string, n: string): { updated: string; removed: boolean } => removeXmlBlock(xml, 'flowAccesses', 'flow', n);

// ===============================================================
// DEPLOY INFRASTRUCTURE
// ===============================================================

const TRANSIENT_ERROR_PATTERNS = [
  /rate limit/i, /request limit/i, /too many requests/i,
  /ECONNRESET/i, /ECONNREFUSED/i, /ETIMEDOUT/i, /ENOTFOUND/i,
  /socket hang up/i, /network/i, /connection.*reset/i,
  /exceeded.*limit/i, /server.*unavailable/i,
  /503/, /502/, /504/,
  /session.*expired/i, /invalid.*session/i, /expired.*access/i,
  /authentication/i, /INVALID_SESSION_ID/i,
  /Cannot read properties of undefined/i,
];

function isTransientError(raw: string): boolean {
  return TRANSIENT_ERROR_PATTERNS.some((p) => p.test(raw));
}

function getBackoffMs(attempt: number): number {
  return Math.min(15_000 * Math.pow(2, attempt - 1), 120_000);
}

function readDeployOutput(outputFile: string): { raw: string; trimmed: string } | null {
  try {
    const raw = fs.readFileSync(outputFile, 'utf8');
    // SF CLI with --json always starts with '{' — find the FIRST one.
    // Using lastIndexOf was wrong: it sliced into the middle of a valid JSON
    // (e.g. the opening brace of a nested componentFailures object).
    const jsonStart = raw.indexOf('{');
    if (jsonStart < 0) return null;
    return { raw, trimmed: raw.substring(jsonStart) };
  } catch {
    return null;
  }
}

function parseDeployJson(trimmed: string): DeployResult | null {
  try {
    return JSON.parse(trimmed) as DeployResult;
  } catch {
    return null;
  }
}

function isPollingTimeout(result: DeployResult): boolean {
  // Only treat as a polling timeout when SF explicitly says the deploy is still running.
  // Do NOT retry on success:true + no details — that is the normal --dry-run success shape.
  const s = (result.result as Record<string, unknown> | undefined)?.status;
  return s === 'Pending' || s === 'InProgress' || s === 'Canceling';
}

function normaliseDeployResult(result: DeployResult, log: (msg: string) => void): DeployResult {
  if (!result.result && result.status !== undefined) {
    log(`   Normalising SF CLI response (status=${result.status}).`);
    return { ...result, result: { success: result.status === 0, details: { componentFailures: [] } } };
  }
  return result;
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

    const readResult = readDeployOutput(outputFile);
    if (!readResult) {
      log('   Could not read deploy output — retrying...');
      // eslint-disable-next-line no-await-in-loop
      await sleep(getBackoffMs(attempt));
      continue;
    }

    const { raw, trimmed } = readResult;

    if (isTransientError(raw)) {
      const backoff = getBackoffMs(attempt);
      log(`   Transient error detected — waiting ${backoff / 1000}s before retry...`);
      // eslint-disable-next-line no-await-in-loop
      await sleep(backoff);
      attempt = 0;
      continue;
    }

    const result = parseDeployJson(trimmed);
    if (!result) {
      // Log the raw file content so the exact SF CLI output is visible in the terminal
      log(`   Invalid JSON on attempt ${attempt} — raw output below:`);
      log('   --- RAW START ---');
      log(raw.substring(0, 2000)); // cap at 2000 chars to avoid flooding the terminal
      log('   --- RAW END ---');
      log('   Retrying...');
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

    if (isPollingTimeout(result)) {
      const backoff = getBackoffMs(attempt);
      log(`   Deploy returned Pending/InProgress — waiting ${backoff / 1000}s before retry...`);
      // eslint-disable-next-line no-await-in-loop
      await sleep(backoff);
      attempt = 0;
      continue;
    }

    const normResult = normaliseDeployResult(result, log);
    log('   Deploy response received.');
    return normResult;
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
      '--ignore-warnings',
      '--wait', String(timeoutMins * 2),
    ];

    const proc = spawn('sf', args, { shell: true });
    const outputStream = fs.createWriteStream(outputFile, { encoding: 'utf8' });

    // stdout only → file; stderr drained so it never blocks the process
    proc.stdout.pipe(outputStream);
    proc.stderr.resume();

    const timer = setTimeout(() => { proc.kill(); resolve('timeout'); }, timeoutMins * 60 * 1000);

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
// WHITELIST + REPO CHECK
// Returns true → SKIP this item (do NOT remove it)
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
// ===============================================================

type MetadataHandler = {
  patterns: RegExp[];
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
      /In field: apexPage - no ApexPage named (.+?) found/i,
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
      /In field: object - no CustomObject named (.+?) found/i,
    ],
    label: 'object', whitelistKey: 'objects', cacheKey: 'objects',
    // Standard SF objects (OperatingHours, Account, Case, etc.) never have an
    // object-meta.xml in the repo — returning '' means fs.existsSync('') = false
    // so they are never skipped by the repo check and get removed correctly.
    // Only custom objects (__c, __mdt, __e, __b, __x, __kav, __ka, __hd) need
    // the repo check — they have a real file when committed but not yet deployed.
    repoPathFn: (r: string, n: string): string => {
      const isCustom = /__(c|mdt|e|b|x|ka|kav|hd|history)$/i.test(n);
      return isCustom
        ? path.join(r, 'force-app', 'main', 'default', 'objects', n, `${n}.object-meta.xml`)
        : '';
    },
    removeFn: removeObjectPermissionFromXml,
    displayTag: '[Object]',
  },
  {
    patterns: [
      /no Flow named (.+?) found/i,
      /Entity of type 'Flow' named '(.+?)' cannot be found/i,
      /In field: flow - no Flow named (.+?) found/i,
      /no active version.*Flow named (.+?) found/i,
      /no FlowDefinition named (.+?) found/i,
      /In field: flow - no FlowDefinition named (.+?) found/i,
      /Entity of type 'FlowDefinition' named '(.+?)' cannot be found/i,
    ],
    label: 'flow', whitelistKey: 'flows', cacheKey: 'flows',
    repoPathFn: (r, n) => path.join(r, 'force-app', 'main', 'default', 'flows', `${n}.flow-meta.xml`),
    removeFn: removeFlowAccessFromXml,
    displayTag: '[Flow]',
  },
];

const FIELD_PATTERNS: RegExp[] = [
  /no CustomField named (.+?) found/i,
  /Entity of type 'CustomField' named '(.+?)' cannot be found/i,
  /In field: field - no CustomField named (.+?) found/i,
];

// ===============================================================
// UNMATCHED ERROR LOG
// ===============================================================

function logUnmatchedError(itemName: string, errorMessage: string): void {
  const line = `[${new Date().toISOString()}] [${itemName}] ${errorMessage}\n`;
  try { fs.appendFileSync(UNMATCHED_ERRORS_LOG, line, 'utf8'); } catch { /* best-effort */ }
}

// ===============================================================
// PRE-SCRUB — apply globally known-missing refs to a new file
// ONLY populated from real deploy errors — no filesystem guessing.
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
    globalMissing.fields.size + globalMissing.apps.size + globalMissing.classes.size +
    globalMissing.pages.size + globalMissing.tabs.size + globalMissing.objects.size +
    globalMissing.flows.size;

  if (totalCached === 0) return { xmlContent: updated, removedItems };

  log(`   [Pre-scrub] ${totalCached} globally known missing reference(s) — applying without a deploy call...`);

  // Fields
  for (const field of globalMissing.fields) {
    const { updated: u, removed } = removeFieldPermissionsFromXml(updated, field);
    if (removed) { updated = u; removedItems.push(field); log(`   [Pre-scrub] Removed field: ${field}`); }
  }

  // All handler types
  for (const handler of METADATA_HANDLERS) {
    for (const name of globalMissing[handler.cacheKey]) {
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
    log('   [Pre-scrub] No cached references matched this file. Proceeding normally.');
  }

  return { xmlContent: updated, removedItems };
}

// ===============================================================
// PROCESS FAILURES
// Called once per deploy iteration with the real SF error list.
// Decision per error:
//   1. Match a pattern → extract name
//   2. Name in whitelist? → SKIP
//   3. Name file exists in repo? → SKIP (warn: deploy it first)
//   4. Otherwise → REMOVE the block + add to global cache
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
  allSkippedFields: string[],
  removalFailures: string[]
): { handled: boolean; xmlContent: string } {
  let missingField: string | null = null;
  for (const p of FIELD_PATTERNS) {
    const m = p.exec(errorMessage);
    if (m) { missingField = m[1].trim(); break; }
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
    globalMissing.fields.add(missingField);
    return { handled: true, xmlContent: updated };
  }

  const msg = `[Field] ${missingField} — SF reported missing but block NOT found in XML. Raw: ${errorMessage}`;
  log(`   [REMOVAL-FAILED] ${msg}`);
  removalFailures.push(msg);
  return { handled: true, xmlContent };
}

function processRegisteredFailure(
  log: (msg: string) => void,
  errorMessage: string,
  xmlContent: string,
  whitelist: WhitelistMap,
  globalMissing: GlobalMissingCache,
  repoPath: string,
  removedFields: string[],
  skippedFields: string[],
  allSkippedFields: string[],
  removalFailures: string[]
): { handled: boolean; xmlContent: string } {
  for (const handler of METADATA_HANDLERS) {
    let name: string | null = null;
    for (const p of handler.patterns) {
      const m = p.exec(errorMessage);
      if (m) { name = m[1].trim(); break; }
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
      (globalMissing[handler.cacheKey]).add(name);
      return { handled: true, xmlContent: updated };
    }

    const msg = `[${handler.displayTag}] ${name} — SF reported missing but block NOT found in XML. Raw: ${errorMessage}`;
    log(`   [REMOVAL-FAILED] ${msg}`);
    removalFailures.push(msg);
    return { handled: true, xmlContent };
  }

  return { handled: false, xmlContent };
}

function processFailures(
  log: (msg: string) => void,
  failures: ComponentFailure[],
  xmlContent: string,
  whitelist: WhitelistMap,
  globalMissing: GlobalMissingCache,
  repoPath: string,
  itemName: string,
  allSkippedFields: string[]
): { xmlContent: string; removedFields: string[]; skippedFields: string[]; removalFailures: string[] } {
  let updatedXml = xmlContent;
  const removedFields: string[] = [];
  const skippedFields: string[] = [];
  const unmatchedErrors: string[] = [];
  const removalFailures: string[] = [];

  for (const failure of failures) {
    const err = failure.problem;

    const fieldResult = processFieldFailure(
      log, err, updatedXml, whitelist, globalMissing, repoPath,
      removedFields, skippedFields, allSkippedFields, removalFailures
    );
    if (fieldResult.handled) { updatedXml = fieldResult.xmlContent; continue; }

    const regResult = processRegisteredFailure(
      log, err, updatedXml, whitelist, globalMissing, repoPath,
      removedFields, skippedFields, allSkippedFields, removalFailures
    );
    if (regResult.handled) { updatedXml = regResult.xmlContent; continue; }

    unmatchedErrors.push(err);
    log(`   [UNMATCHED] ${err}`);
  }

  if (unmatchedErrors.length > 0) {
    unmatchedErrors.forEach((e) => logUnmatchedError(itemName, e));
    log(`   [WARN] ${unmatchedErrors.length} unmatched error(s) appended to: ${UNMATCHED_ERRORS_LOG}`);
    log('   [WARN] Check that file to add new patterns to METADATA_HANDLERS or FIELD_PATTERNS.');
  }

  if (removalFailures.length > 0) {
    const ts = new Date().toISOString();
    const lines = removalFailures.map((e) => `[${ts}] [${itemName}] ${e}`).join('\n') + '\n';
    try { fs.appendFileSync(UNMATCHED_ERRORS_LOG, lines, 'utf8'); } catch { /* best-effort */ }
    log(`   [WARN] ${removalFailures.length} removal failure(s) logged to: ${UNMATCHED_ERRORS_LOG}`);
  }

  return { xmlContent: updatedXml, removedFields, skippedFields, removalFailures };
}

// ===============================================================
// PROCESS A SINGLE METADATA ITEM
// Flow:
//   1. Pre-scrub with globally known-missing refs (FREE — no deploy)
//   2. Deploy loop:
//        a. Run dry-run deploy
//        b. On success → done
//        c. On failures → process each error → remove/skip/warn
//        d. Save XML, git commit, repeat
// NO static filesystem scanning is done. Every removal decision
// comes from an actual SF validation error message.
// ===============================================================

function commitChange(
  log: (msg: string) => void,
  filePath: string,
  repoPath: string,
  message: string,
  tag: string
): void {
  try {
    execSync(`git add "${filePath}"`, { cwd: repoPath });
    execSync(`git commit -m "${message}"`, { cwd: repoPath });
    log(`   [${tag}] Committed changes.`);
  } catch {
    log(`   [${tag}] Nothing to commit or commit failed.`);
  }
}

// ── Helper: process the result of a single deploy attempt ────────────────────
// Extracted from invokeProcessMetadataItem to keep its cyclomatic complexity
// under the ESLint limit of 20.
type IterationOutcome =
  | { done: true; status: string }
  | { done: false; status: string };

function processDeployIteration(
  log: (msg: string) => void,
  deployResult: DeployResult,
  itemName: string,
  filePath: string,
  repoPath: string,
  whitelist: WhitelistMap,
  globalMissing: GlobalMissingCache,
  allRemovedFields: string[],
  allSkippedFields: string[]
): IterationOutcome {
  if (!deployResult.result) {
    log(`[${itemName}] SF CLI returned an unrecognised response shape. Raw keys: ${Object.keys(deployResult).join(', ')}`);
    log(`[${itemName}] Full response: ${JSON.stringify(deployResult)}`);
    log(`[${itemName}] Stopping — re-run once the org is reachable.`);
    return { done: true, status: 'Deploy Failed - Unrecognised Response' };
  }

  log(`   [Result] success=${String(deployResult.result.success)} | status=${String((deployResult.result as Record<string, unknown>).status ?? 'n/a')} | numberComponentErrors=${String((deployResult.result as Record<string, unknown>).numberComponentErrors ?? 'n/a')}`);

  // componentFailures can be a plain object (not array) when SF returns a single failure.
  const rawFailures = deployResult.result.details?.componentFailures;
  const failures: ComponentFailure[] = !rawFailures
    ? []
    : Array.isArray(rawFailures)
      ? rawFailures
      : [rawFailures as ComponentFailure];

  log(`   [Result] componentFailures count: ${failures.length}`);
  failures.forEach((f, i) => log(`   [Result] Failure ${i + 1}: ${f.problem}`));

  if (deployResult.result.success === true && failures.length === 0) {
    log(`[${itemName}] Deploy validation SUCCESSFUL — no component failures!`);
    return { done: true, status: allRemovedFields.length > 0 ? 'Fixed & Committed' : 'Success' };
  }

  if (failures.length === 0) {
    log(`[${itemName}] Deploy returned success=false but no componentFailures. Full result:`);
    log(JSON.stringify(deployResult.result, null, 2).substring(0, 3000));
    return { done: true, status: 'Partial / Manual Check Needed' };
  }

  const xmlContent = fs.readFileSync(filePath, 'utf8');
  const rootNode = getRootNodeName(xmlContent);

  const { xmlContent: updatedXml, removedFields, skippedFields, removalFailures } = processFailures(
    log, failures, xmlContent, whitelist, globalMissing, repoPath, itemName, allSkippedFields
  );

  allRemovedFields.push(...removedFields);

  if (removedFields.length === 0) {
    if (removalFailures.length > 0) {
      log(`[${itemName}] ${removalFailures.length} removal failure(s) — SF reported errors but blocks not found in XML:`);
      removalFailures.forEach((f) => log(`   ${f}`));
      return { done: true, status: 'Removal Failed - Check Logs' };
    }
    if (skippedFields.length > 0) {
      log(`[${itemName}] Only whitelisted/repo items remain. Manual deploy needed.`);
      return { done: true, status: 'Whitelisted Items Only - Manual Deploy Needed' };
    }
    log(`[${itemName}] No items removed this iteration. Moving on.`);
    return { done: true, status: 'Partial / Manual Check Needed' };
  }

  saveXmlClean(updatedXml, filePath, rootNode);
  log(`Saved updated XML for: ${itemName}`);
  commitChange(log, filePath, repoPath,
    `[${itemName}] Auto-remove missing metadata: ${removedFields.join(', ')}`,
    itemName);
  return { done: false, status: 'Fixed & Committed' };
}

async function invokeProcessMetadataItem(
  log: (msg: string) => void,
  params: {
    metadataType: string;
    itemName: string;
    filePath: string;
    targetOrg: string;
    repoPath: string;
    whitelist: WhitelistMap;
    globalMissing: GlobalMissingCache;
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

  // ── Step 1: Pre-scrub using only the globally known-missing cache ──
  // This cache is populated exclusively from prior deploy errors in
  // this same run — never from filesystem checks. Zero deploy cost.
  {
    const rawXml = fs.readFileSync(filePath, 'utf8');
    const { xmlContent: scrubbed, removedItems } = applyGlobalMissingCacheToFile(log, rawXml, globalMissing, itemName);
    if (removedItems.length > 0) {
      saveXmlClean(scrubbed, filePath, getRootNodeName(scrubbed));
      allRemovedFields.push(...removedItems);
      commitChange(log, filePath, repoPath,
        `[${itemName}] Pre-scrub: remove globally known missing: ${removedItems.join(', ')}`,
        'Pre-scrub');
      itemStatus = 'Fixed & Committed';
    }
  }

  // ── Step 2: Deploy loop — all removals driven by actual SF errors ──
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

    const outcome = processDeployIteration(
      log, deployResult, itemName, filePath, repoPath,
      whitelist, globalMissing, allRemovedFields, allSkippedFields
    );

    itemStatus = outcome.status;

    if (outcome.done) {
      continueLoop = false;
      break;
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
// ===============================================================

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

async function printBannerAndConfirm(
  log: (msg: string) => void,
  targetOrg: string,
  permSets: string[],
  profiles: string[],
  whitelist: WhitelistMap
): Promise<boolean> {
  const total = Object.values(whitelist).reduce((s, a) => s + a.length, 0);

  log('\n======================================================');
  log('STARTING AUTOMATED DEPLOY & FIX LOOP');
  log('======================================================');
  log(`Target Org              : ${targetOrg}`);
  log(`Permission Sets         : ${permSets.length} found in JSON`);
  log(`Profiles                : ${profiles.length} found in JSON`);
  log(`Whitelisted total       : ${total} items across all types (will never be removed)`);
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
  if (confirm.trim().toLowerCase() === 'exit') { log('\nScript cancelled by user.'); return false; }
  log('\nStarting script...');
  log('\n======================================================');
  return true;
}

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

  const common = {
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
      ...common,
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
        ...common,
        metadataType: 'Profile',
        itemName: profileName,
        filePath: path.join(PROFILE_BASE_PATH, `${profileName}.profile-meta.xml`),
      }));
      if (totalDeploys.value > MAX_TOTAL_DEPLOYS) break;
    }
  }

  return { summary, totalDeploys };
}

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

  log(`\nGlobal Missing Cache (discovered from deploy errors this run): ${totalCached} unique missing references`);
  if (globalMissing.fields.size) log(`  Fields   : ${[...globalMissing.fields].join(', ')}`);
  if (globalMissing.apps.size) log(`  Apps     : ${[...globalMissing.apps].join(', ')}`);
  if (globalMissing.classes.size) log(`  Classes  : ${[...globalMissing.classes].join(', ')}`);
  if (globalMissing.pages.size) log(`  Pages    : ${[...globalMissing.pages].join(', ')}`);
  if (globalMissing.tabs.size) log(`  Tabs     : ${[...globalMissing.tabs].join(', ')}`);
  if (globalMissing.objects.size) log(`  Objects  : ${[...globalMissing.objects].join(', ')}`);
  if (globalMissing.flows.size) log(`  Flows    : ${[...globalMissing.flows].join(', ')}`);

  if (fs.existsSync(UNMATCHED_ERRORS_LOG)) {
    log(`\n[WARN] Unmatched or failed removals were logged to: ${UNMATCHED_ERRORS_LOG}`);
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

    const { promotionJsonPath, targetOrg } = await resolveInputs(
      log, flags['json-path'] ?? '', flags['target-org'] ?? ''
    );

    const { permSets, profiles, whitelist } = parsePromotionJson(promotionJsonPath);

    const confirmed = await printBannerAndConfirm(log, targetOrg, permSets, profiles, whitelist);
    if (!confirmed) return;

    // Single shared cache — grows only from real deploy errors.
    // Subsequent items get those known-missing refs removed for free.
    const globalMissing = makeGlobalMissingCache();

    const { summary, totalDeploys } = await processAllItems(
      log, permSets, profiles, targetOrg, whitelist, globalMissing
    );

    printFinalSummary(log, summary, totalDeploys, globalMissing);
  }
}