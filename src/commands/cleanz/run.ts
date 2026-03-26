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

type WhitelistMap = {
  fields: string[];
  apps: string[];
  classes: string[];
  pages: string[];
  tabs: string[];
  objects: string[];
  flows: string[];
};

// Carries enough info to remove a ref from ANY other file in the batch.
type RefType = 'field' | 'app' | 'class' | 'page' | 'tab' | 'object' | 'flow' | 'namespace';

type RemovedRef = {
  type: RefType;
  name: string;
  label: string; // display string e.g. "Account.Name" or "[Class] MyClass"
};

// Result returned by each failure-handler function.
type FailureResult = {
  handled: boolean;
  xmlContent: string;
  removedRef?: RemovedRef; // set only when something was actually removed from XML
};

// ===============================================================
// CONSTANTS / CONFIG
// ===============================================================

const REPO_PATH = 'D:\\RubrikRepoVDI\\rbk-sfdc-release';
const PS_BASE_PATH = path.join(REPO_PATH, 'force-app', 'main', 'default', 'permissionsets');
const PROFILE_BASE_PATH = path.join(REPO_PATH, 'force-app', 'main', 'default', 'profiles');
const MAX_ITERATIONS = 500;
const MAX_TOTAL_DEPLOYS = 1000;
const DEPLOY_TIMEOUT_MINS = 12;
const MAX_RETRIES = 3;

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
  const lines = xml.replace(/>\s*</g, '>\n<').split('\n');

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
  const escapedClosing = closingTag.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const regex = new RegExp(`</(\\w+)>${escapedClosing}`, 'g');
  content = content.replace(regex, (_match: string, tagName: string) => `</${tagName}>\n${closingTag}`);
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
  const escapedField = missingField.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const innerPattern = '(?:(?!<fieldPermissions>)[\\s\\S])*?';
  const blockRegex = new RegExp(
    `[ \\t]*<fieldPermissions>${innerPattern}<field>[ \\t]*${escapedField}[ \\t]*</field>${innerPattern}</fieldPermissions>[ \\t]*\\r?\\n?`,
    'g'
  );
  const updated = xmlContent.replace(blockRegex, '');
  return { updated, removed: updated !== xmlContent };
}

function removeXmlBlock(
  xmlContent: string,
  blockTag: string,
  keyTag: string,
  missingName: string
): { updated: string; removed: boolean } {
  const escapedName = missingName.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const escapedBlock = blockTag.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const innerPattern = `(?:(?!<${escapedBlock}>)[\\s\\S])*?`;
  const blockRegex = new RegExp(
    `[ \\t]*<${escapedBlock}>${innerPattern}<${keyTag}>[ \\t]*${escapedName}[ \\t]*</${keyTag}>${innerPattern}</${escapedBlock}>[ \\t]*\\r?\\n?`,
    'g'
  );
  const updated = xmlContent.replace(blockRegex, '');
  return { updated, removed: updated !== xmlContent };
}

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
  // PermissionSets store tab entries in <tabSettings>; Profiles store them in <tabVisibilities>.
  const psResult = removeXmlBlock(xmlContent, 'tabSettings', 'tab', name);
  const profileResult = removeXmlBlock(psResult.updated, 'tabVisibilities', 'tab', name);
  return {
    updated: profileResult.updated,
    removed: psResult.removed || profileResult.removed,
  };
}
function removeAllFieldPermissionsForObject(
  xmlContent: string,
  objectName: string
): { updated: string; removed: boolean } {
  const escapedObject = objectName.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const innerPattern = '(?:(?!<fieldPermissions>)[\\s\\S])*?';
  // Matches any <fieldPermissions> block whose <field> starts with "ObjectName."
  const blockRegex = new RegExp(
    `[ \\t]*<fieldPermissions>${innerPattern}<field>[ \\t]*${escapedObject}\\.[^<]+[ \\t]*</field>${innerPattern}</fieldPermissions>[ \\t]*\\r?\\n?`,
    'g'
  );
  const updated = xmlContent.replace(blockRegex, '');
  return { updated, removed: updated !== xmlContent };
}

function removeObjectPermissionFromXml(xmlContent: string, name: string): { updated: string; removed: boolean } {
  // Remove the objectPermissions block itself
  const objectResult = removeXmlBlock(xmlContent, 'objectPermissions', 'object', name);
  // Also remove all fieldPermissions for fields on this object (Object.Field__c).
  // Salesforce will reject even fieldPermissions for fields on a missing object,
  // so we must strip both in one pass to avoid the same error on the next iteration.
  const fieldResult = removeAllFieldPermissionsForObject(objectResult.updated, name);
  return {
    updated: fieldResult.updated,
    removed: objectResult.removed || fieldResult.removed,
  };
}
function removeFlowAccessFromXml(xmlContent: string, name: string): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'flowAccesses', 'flow', name);
}

// ===============================================================
// NAMESPACE BULK REMOVAL
// When a managed package is not installed in the org, every single
// component it owns (fields, objects, classes, tabs, flows, apps, pages)
// will fail deployment. Instead of iterating one-by-one, we detect the
// namespace prefix, confirm the package is absent, and strip all its refs
// in one pass — then sweep the same removal across every other file.
// ===============================================================

// Cache: "orgAlias:Namespace" → installed (true/false)
const namespaceCache = new Map<string, boolean>();

function extractNamespaceFromError(errorMessage: string): string | null {
  // Matches "Namespace__" prefix inside names like:
  //   "Account.UniqueEntry__Field__c"  → "UniqueEntry"
  //   "UniqueEntry__Object__c"         → "UniqueEntry"
  const m = /named\s+(?:\w+\.)?([A-Za-z][A-Za-z0-9]*)__\w/.exec(errorMessage);
  return m?.[1] ?? null;
}

async function checkNamespaceInstalled(
  log: (msg: string) => void,
  targetOrg: string,
  namespace: string
): Promise<boolean> {
  const key = `${targetOrg}:${namespace}`;
  if (namespaceCache.has(key)) return namespaceCache.get(key)!;

  log(`   [NS Check] Checking if package "${namespace}" is installed in org...`);
  const query = `"SELECT Id FROM InstalledSubscriberPackage WHERE SubscriberPackage.NamespacePrefix = '${namespace}'"`;
  const count = await new Promise<number>((resolve) => {
    const args = ['data', 'query', '--query', query, '--use-tooling-api', '--target-org', targetOrg, '--json'];
    const proc = spawn('sf', args, { shell: true });
    const chunks: string[] = [];
    proc.stdout.on('data', (d: Buffer) => chunks.push(d.toString()));
    proc.stderr.on('data', (d: Buffer) => chunks.push(d.toString()));
    const timer = setTimeout(() => { proc.kill(); resolve(-1); }, 30_000);
    proc.on('close', () => {
      clearTimeout(timer);
      try {
        const raw = chunks.join('');
        const start = raw.indexOf('{');
        const json = JSON.parse(start >= 0 ? raw.substring(start) : raw) as { result?: { totalSize?: number } };
        resolve(json?.result?.totalSize ?? -1);
      } catch { resolve(-1); } // query error → treat as installed (safe default)
    });
  });

  // count=-1 means query failed: treat as installed so we don't accidentally remove things.
  const installed = count !== 0;
  namespaceCache.set(key, installed);
  log(`   [NS Check] ${namespace}: ${installed ? 'installed — skipping bulk removal' : 'NOT installed — bulk-removing all refs'}`);
  return installed;
}

function removeBlocksWithNamespace(
  xml: string, blockTag: string, keyTag: string, namespace: string
): string {
  const ns = namespace.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const bt = blockTag.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const inner = `(?:(?!<${bt}>)[\\s\\S])*?`;
  return xml.replace(
    new RegExp(`[ \\t]*<${bt}>${inner}<${keyTag}>${ns}__[^<]*</${keyTag}>${inner}</${bt}>[ \\t]*\\r?\\n?`, 'g'),
    ''
  );
}

function bulkRemoveNamespaceRefs(xmlContent: string, namespace: string): { updated: string; removed: boolean } {
  const ns = namespace.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  let xml = xmlContent;

  // fieldPermissions: field = "SomeObject.Namespace__Field__c"
  {
    const inner = '(?:(?!<fieldPermissions>)[\\s\\S])*?';
    xml = xml.replace(
      new RegExp(`[ \\t]*<fieldPermissions>${inner}<field>[^<]*\\.${ns}__[^<]*</field>${inner}</fieldPermissions>[ \\t]*\\r?\\n?`, 'g'), ''
    );
    // fieldPermissions: field = "Namespace__Object__c.AnyField"
    xml = xml.replace(
      new RegExp(`[ \\t]*<fieldPermissions>${inner}<field>${ns}__[^<]*</field>${inner}</fieldPermissions>[ \\t]*\\r?\\n?`, 'g'), ''
    );
  }

  xml = removeBlocksWithNamespace(xml, 'objectPermissions',      'object',      namespace);
  xml = removeBlocksWithNamespace(xml, 'classAccesses',          'apexClass',   namespace);
  xml = removeBlocksWithNamespace(xml, 'pageAccesses',           'apexPage',    namespace);
  xml = removeBlocksWithNamespace(xml, 'tabSettings',            'tab',         namespace);
  xml = removeBlocksWithNamespace(xml, 'tabVisibilities',        'tab',         namespace);
  xml = removeBlocksWithNamespace(xml, 'flowAccesses',           'flow',        namespace);
  xml = removeBlocksWithNamespace(xml, 'applicationVisibilities','application', namespace);

  return { updated: xml, removed: xml !== xmlContent };
}

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

    // Wait for the org's deployment queue to clear before submitting.
    // Prevents our validation from queuing behind active Copado deployments,
    // and handles our own stale dry-runs that are still InProgress from a prior timeout.
    // eslint-disable-next-line no-await-in-loop
    await waitForQueueToClear(log, targetOrg);

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
      const errName = result.name ?? 'none';
      const errMsg = (result.message ?? '').substring(0, 150);
      log(`   SF CLI status=${result.status} | name=${errName} | message=${errMsg}`);
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
    // shell: true is required on Windows to resolve sf.cmd from PATH.
    // The -m value is wrapped in quotes so cmd.exe treats "Profile:Name With Spaces"
    // as a single argument (cmd.exe case-2 stripping: outer quotes removed, inner preserved).
    const metaArg = `"${metadataType}:${itemName}"`;
    const args = [
      'project', 'deploy', 'start',
      '-m', metaArg,
      '--target-org', targetOrg,
      '--json',
      '--dry-run',
      '--wait', '10',
    ];
    // Log the exact shell command for debugging
    const dbgCmd = `sf ${['project', 'deploy', 'start', '-m', metaArg, '--target-org', targetOrg, '--json', '--dry-run', '--wait', '10'].join(' ')}`;
    fs.appendFileSync(outputFile + '.cmd.txt', dbgCmd + '\n', 'utf8');

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
// DEPLOY QUEUE CHECK
// Queries the Tooling API DeployRequest object to count active
// (Pending / InProgress) deployments in the org — including any
// Copado promotions that are currently in flight.
// We wait until the count reaches 0 before submitting our own
// CheckOnly (dry-run) validation, which prevents our job from
// sitting in the queue behind a long-running Copado deployment.
// It also handles our own stale dry-runs: when --wait 10 expires
// the job is still InProgress in the org; by waiting for it to
// finish before retrying we avoid flooding the queue.
// ===============================================================

function queryDeployQueueCount(targetOrg: string): Promise<number> {
  return new Promise((resolve) => {
    // Single quotes inside the SOQL are fine inside cmd.exe-quoted args.
    const query = '"SELECT Id FROM DeployRequest WHERE Status IN (\'Pending\',\'InProgress\')"';
    const args = [
      'data', 'query',
      '--query', query,
      '--use-tooling-api',
      '--target-org', targetOrg,
      '--json',
    ];
    const proc = spawn('sf', args, { shell: true });
    const chunks: string[] = [];
    proc.stdout.on('data', (d: Buffer) => chunks.push(d.toString()));
    proc.stderr.on('data', (d: Buffer) => chunks.push(d.toString()));
    const timer = setTimeout(() => { proc.kill(); resolve(0); }, 30_000);
    proc.on('close', () => {
      clearTimeout(timer);
      try {
        const raw = chunks.join('');
        const start = raw.indexOf('{');
        const json = JSON.parse(start >= 0 ? raw.substring(start) : raw) as {
          result?: { totalSize?: number };
        };
        resolve(json?.result?.totalSize ?? 0);
      } catch {
        resolve(0); // If query fails, assume clear and proceed
      }
    });
  });
}

async function waitForQueueToClear(
  log: (msg: string) => void,
  targetOrg: string,
  maxWaitMins = 30
): Promise<void> {
  const POLL_MS = 30_000;
  const deadline = Date.now() + maxWaitMins * 60_000;

  // eslint-disable-next-line no-await-in-loop
  let count = await queryDeployQueueCount(targetOrg);
  if (count === 0) return;

  log(`   [Queue] ${count} active deployment(s) in org (Copado or previous dry-run). Waiting for queue to clear...`);
  while (count > 0 && Date.now() < deadline) {
    // eslint-disable-next-line no-await-in-loop
    await sleep(POLL_MS);
    // eslint-disable-next-line no-await-in-loop
    count = await queryDeployQueueCount(targetOrg);
    if (count > 0) log(`   [Queue] Still ${count} active deployment(s). Waiting 30s...`);
  }

  if (count === 0) {
    log('   [Queue] Org deployment queue is clear. Proceeding with validation.');
  } else {
    log(`   [Queue] Waited ${maxWaitMins} min — queue did not clear. Proceeding anyway.`);
  }
}

// ===============================================================
// WHITELIST CHECK
// Only the JSON package is the source of truth — repo presence is
// NOT checked. A field/class/etc. may exist in the repo but not be
// in the target org and not be in this JSON; in that case it must
// be removed from the permset/profile.
// ===============================================================

function shouldSkip(
  log: (msg: string) => void,
  label: string,
  name: string,
  whitelistEntries: string[],
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
  return false;
}

// ===============================================================
// METADATA HANDLER REGISTRY
// repoPathFn removed — whitelist is JSON-only now.
// ===============================================================

type MetadataHandler = {
  patterns: RegExp[];  // multiple patterns — SF can phrase the same error differently
  label: string;
  refType: RefType;
  whitelistKey: keyof WhitelistMap;
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
    label: 'app', refType: 'app', whitelistKey: 'apps',
    removeFn: removeApplicationVisibilityFromXml, displayTag: '[App]',
  },
  {
    patterns: [
      /no ApexClass named (.+?) found/i,
      /Entity of type 'ApexClass' named '(.+?)' cannot be found/i,
      /In field: apexClass - no ApexClass named (.+?) found/i,
    ],
    label: 'class', refType: 'class', whitelistKey: 'classes',
    removeFn: removeClassAccessFromXml, displayTag: '[Class]',
  },
  {
    patterns: [
      /no ApexPage named (.+?) found/i,
      /Entity of type 'ApexPage' named '(.+?)' cannot be found/i,
      /In field: apexPage - no ApexPage named (.+?) found/i,
    ],
    label: 'page', refType: 'page', whitelistKey: 'pages',
    removeFn: removePageAccessFromXml, displayTag: '[Page]',
  },
  {
    patterns: [
      /no CustomTab named (.+?) found/i,
      /Entity of type 'CustomTab' named '(.+?)' cannot be found/i,
      /In field: tab - no CustomTab named (.+?) found/i,
    ],
    label: 'tab', refType: 'tab', whitelistKey: 'tabs',
    removeFn: removeTabSettingFromXml, displayTag: '[Tab]',
  },
  {
    patterns: [
      /no CustomObject named (.+?) found/i,
      /Entity of type 'CustomObject' named '(.+?)' cannot be found/i,
      /In field: object - no CustomObject named (.+?) found/i,
    ],
    label: 'object', refType: 'object', whitelistKey: 'objects',
    removeFn: removeObjectPermissionFromXml, displayTag: '[Object]',
  },
  {
    patterns: [
      /no Flow named (.+?) found/i,
      /Entity of type 'Flow' named '(.+?)' cannot be found/i,
      /In field: flow - no Flow named (.+?) found/i,
      /no FlowDefinition named (.+?) found/i,
      /Entity of type 'FlowDefinition' named '(.+?)' cannot be found/i,
      /In field: flow - no FlowDefinition named (.+?) found/i,
    ],
    label: 'flow', refType: 'flow', whitelistKey: 'flows',
    removeFn: removeFlowAccessFromXml, displayTag: '[Flow]',
  },
];

// ===============================================================
// FAILURE HANDLERS
// Each returns FailureResult — removedRef is set only when XML was changed.
// ===============================================================

function processFieldFailure(
  log: (msg: string) => void,
  errorMessage: string,
  xmlContent: string,
  whitelist: WhitelistMap,
  skippedFields: string[],
  allSkippedFields: string[]
): FailureResult {
  const fieldPatterns = [
    /no CustomField named (.+?) found/i,
    /Entity of type 'CustomField' named '(.+?)' cannot be found/i,
    /In field: field - no CustomField named (.+?) found/i,
  ];
  let missingField: string | null = null;
  for (const p of fieldPatterns) {
    const m = p.exec(errorMessage);
    if (m) { missingField = m[1].trim(); break; }
  }
  if (!missingField) return { handled: false, xmlContent };

  if (shouldSkip(log, 'field', missingField, whitelist.fields, skippedFields, allSkippedFields)) {
    return { handled: true, xmlContent };
  }

  log(`   Missing field: ${missingField}`);
  const { updated, removed } = removeFieldPermissionsFromXml(xmlContent, missingField);
  if (removed) {
    log(`   Removed fieldPermissions for: ${missingField}`);
    return {
      handled: true,
      xmlContent: updated,
      removedRef: { type: 'field', name: missingField, label: missingField },
    };
  }
  log(`   Field not found in XML: ${missingField} — already removed or not present.`);
  return { handled: true, xmlContent };
}

function processRegisteredFailure(
  log: (msg: string) => void,
  errorMessage: string,
  xmlContent: string,
  whitelist: WhitelistMap,
  skippedFields: string[],
  allSkippedFields: string[]
): FailureResult {
  for (const handler of METADATA_HANDLERS) {
    let name: string | null = null;
    for (const pattern of handler.patterns) {
      const m = pattern.exec(errorMessage);
      if (m) { name = m[1].trim(); break; }
    }
    if (!name) continue;

    if (shouldSkip(log, handler.label, name, whitelist[handler.whitelistKey], skippedFields, allSkippedFields)) {
      return { handled: true, xmlContent };
    }

    log(`   Missing ${handler.label}: ${name}`);
    const { updated, removed } = handler.removeFn(xmlContent, name);
    if (removed) {
      log(`   Removed ${handler.label} block for: ${name}`);
      return {
        handled: true,
        xmlContent: updated,
        removedRef: { type: handler.refType, name, label: `${handler.displayTag} ${name}` },
      };
    }
    log(`   ${handler.label} block not found in XML: ${name} — already removed or not present.`);
    return { handled: true, xmlContent };
  }

  return { handled: false, xmlContent };
}

// ===============================================================
// PROCESS ALL FAILURES FOR ONE DEPLOY ITERATION
// ===============================================================

function processFailures(
  log: (msg: string) => void,
  failures: ComponentFailure[],
  xmlContent: string,
  whitelist: WhitelistMap,
  allSkippedFields: string[]
): { xmlContent: string; removedRefs: RemovedRef[]; skippedFields: string[] } {
  let updatedXml = xmlContent;
  const removedRefs: RemovedRef[] = [];
  const skippedFields: string[] = [];

  log(`   [DEBUG] Total failures this iteration: ${failures.length}`);
  failures.forEach((f, i) => log(`   [DEBUG] Failure ${i + 1}: ${f.problem}`));

  for (const failure of failures) {
    const err = failure.problem;

    // ── CustomField ───────────────────────────────────────────────
    const fieldResult = processFieldFailure(log, err, updatedXml, whitelist, skippedFields, allSkippedFields);
    if (fieldResult.handled) {
      updatedXml = fieldResult.xmlContent;
      if (fieldResult.removedRef) removedRefs.push(fieldResult.removedRef);
      continue;
    }

    // ── Registered handlers (app / class / page / tab / object / flow) ──
    const regResult = processRegisteredFailure(log, err, updatedXml, whitelist, skippedFields, allSkippedFields);
    if (regResult.handled) {
      updatedXml = regResult.xmlContent;
      if (regResult.removedRef) removedRefs.push(regResult.removedRef);
      continue;
    }

    log(`   Skipping unhandled error: ${err}`);
  }

  return { xmlContent: updatedXml, removedRefs, skippedFields };
}

// ===============================================================
// CROSS-FILE SWEEP
//
// After a ref is confirmed missing and removed from the current file,
// remove it from every other permset/profile file in the JSON batch.
// One combined git commit covers all affected files.
// ===============================================================

function sweepOtherFiles(
  log: (msg: string) => void,
  refs: RemovedRef[],
  currentFilePath: string,
  allFilePaths: string[],
  repoPath: string
): void {
  if (refs.length === 0) return;

  log('\n   [Sweep] Removing same missing refs from all other files in batch...');
  const modifiedFiles: string[] = [];

  for (const filePath of allFilePaths) {
    if (filePath === currentFilePath || !fs.existsSync(filePath)) continue;

    let xml = fs.readFileSync(filePath, 'utf8');
    let fileModified = false;

    for (const ref of refs) {
      let result: { updated: string; removed: boolean };
      if (ref.type === 'namespace') {
        result = bulkRemoveNamespaceRefs(xml, ref.name);
      } else if (ref.type === 'field') {
        result = removeFieldPermissionsFromXml(xml, ref.name);
      } else {
        const handler = METADATA_HANDLERS.find((h) => h.refType === ref.type);
        if (!handler) continue;
        result = handler.removeFn(xml, ref.name);
      }
      if (result.removed) {
        xml = result.updated;
        fileModified = true;
        log(`   [Sweep] Removed ${ref.label} from ${path.basename(filePath)}`);
      }
    }

    if (fileModified) {
      saveXmlClean(xml, filePath, getRootNodeName(xml));
      modifiedFiles.push(filePath);
    }
  }

  if (modifiedFiles.length === 0) {
    log('   [Sweep] No other files contained these missing references.');
    return;
  }

  const refLabels = refs.map((r) => r.label).join(', ');
  try {
    for (const f of modifiedFiles) execSync(`git add "${f}"`, { cwd: repoPath });
    execSync(
      `git commit -m "Cross-file sweep: remove [${refLabels}] from ${modifiedFiles.length} other file(s)"`,
      { cwd: repoPath }
    );
    log(`   [Sweep] Committed cleanup across ${modifiedFiles.length} file(s).`);
  } catch {
    log('   [Sweep] Commit failed or nothing new to stage.');
  }
}

// ===============================================================
// NAMESPACE PRE-CHECK
// Extracted to keep invokeProcessMetadataItem under the complexity limit.
// ===============================================================

async function applyNamespacePreCheck(
  log: (msg: string) => void,
  failures: ComponentFailure[],
  xmlContent: string,
  whitelist: WhitelistMap,
  targetOrg: string,
  itemName: string
): Promise<{ xml: string; refs: RemovedRef[] }> {
  const checked = new Set<string>();
  let xml = xmlContent;
  const refs: RemovedRef[] = [];

  for (const failure of failures) {
    const ns = extractNamespaceFromError(failure.problem);
    if (!ns || checked.has(ns)) continue;
    checked.add(ns);

    const hasWhitelisted = Object.values(whitelist).flat()
      .some((v) => v.startsWith(`${ns}__`) || v.includes(`.${ns}__`));
    if (hasWhitelisted) {
      log(`   [NS Check] ${ns}: some components are whitelisted — skipping bulk removal`);
      continue;
    }

    // eslint-disable-next-line no-await-in-loop
    const installed = await checkNamespaceInstalled(log, targetOrg, ns);
    if (!installed) {
      const { updated, removed } = bulkRemoveNamespaceRefs(xml, ns);
      if (removed) {
        xml = updated;
        refs.push({ type: 'namespace', name: ns, label: `[NS:${ns}] bulk-removed` });
        log(`   [NS Bulk] Removed ALL ${ns}__ refs from ${itemName} in one pass`);
      }
    }
  }

  return { xml, refs };
}

// ===============================================================
// PROCESS A SINGLE METADATA ITEM
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
    maxIterations: number;
    maxTotalDeploys: number;
    totalDeploys: TotalDeploys;
    timeoutMins: number;
    maxRetries: number;
    allFilePaths: string[];
  }
): Promise<SummaryRecord> {
  const {
    metadataType, itemName, filePath, targetOrg, repoPath,
    whitelist, maxIterations, maxTotalDeploys,
    totalDeploys, timeoutMins, maxRetries, allFilePaths,
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

  let continueLoop = true;
  // Tracks consecutive deploys that returned success=false with zero component failures.
  // This happens when --wait 2 expires before the org finishes validating (still InProgress).
  // We retry up to this limit before giving up.
  const MAX_EMPTY_RETRIES = 5;
  let consecutiveEmptyRetries = 0;

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
      log(`[${itemName}] SF CLI returned an unrecognised response shape. Keys: ${Object.keys(deployResult).join(', ')}`);
      log(`[${itemName}] Full response: ${JSON.stringify(deployResult)}`);
      itemStatus = 'Deploy Failed - Unrecognised Response';
      break;
    }

    // ── SUCCESS ────────────────────────────────────────────────────
    if (deployResult.result.success === true) {
      log(`[${itemName}] Deploy validation SUCCESSFUL!`);
      itemStatus = allRemovedFields.length > 0 ? 'Fixed & Committed' : 'Success';
      break;
    }

    const failures = deployResult.result.details?.componentFailures;

    // success=false + zero failures means SF CLI's --wait 2 expired before the org
    // finished validating (deploy still InProgress). Retry to get the real result.
    if (!failures || failures.length === 0) {
      consecutiveEmptyRetries++;
      log(`[${itemName}] success=false but 0 component failures (retry ${consecutiveEmptyRetries}/${MAX_EMPTY_RETRIES}) — deploy may still be running in org. Re-validating...`);
      if (consecutiveEmptyRetries >= MAX_EMPTY_RETRIES) {
        log(`[${itemName}] Giving up after ${MAX_EMPTY_RETRIES} retries with no component failures. Manual check needed.`);
        itemStatus = 'Partial / Manual Check Needed';
        break;
      }
      // eslint-disable-next-line no-await-in-loop
      await sleep(5000);
      continue;
    }

    consecutiveEmptyRetries = 0; // reset when real failures arrive

    // ── FAILURES FOUND ─────────────────────────────────────────────
    const xmlContent = fs.readFileSync(filePath, 'utf8');
    const rootNode = getRootNodeName(xmlContent);

    // ── NAMESPACE PRE-CHECK ─────────────────────────────────────────
    // eslint-disable-next-line no-await-in-loop
    const { xml: nsCleanedXml, refs: nsRemovedRefs } = await applyNamespacePreCheck(
      log, failures, xmlContent, whitelist, targetOrg, itemName
    );

    const { xmlContent: updatedXml, removedRefs: perFailureRefs, skippedFields } = processFailures(
      log, failures, nsCleanedXml, whitelist, allSkippedFields
    );

    const removedRefs = [...nsRemovedRefs, ...perFailureRefs];

    allRemovedFields.push(...removedRefs.map((r) => r.label));

    if (removedRefs.length === 0) {
      if (skippedFields.length > 0) {
        log(`[${itemName}] Only whitelisted items remain. Manual deploy needed.`);
        itemStatus = 'Whitelisted Items Only - Manual Deploy Needed';
      } else {
        log(`[${itemName}] No items removed this iteration. Moving on.`);
        itemStatus = 'Partial / Manual Check Needed';
      }
      continueLoop = false;
      break;
    }

    // 1. Save + commit the current file
    saveXmlClean(updatedXml, filePath, rootNode);
    log(`Saved updated XML for: ${itemName}`);

    const commitMessage = `[${itemName}] Auto-remove missing: ${removedRefs.map((r) => r.label).join(', ')}`;
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

    // 2. Remove the same refs from every other file in the batch (one commit)
    sweepOtherFiles(log, removedRefs, filePath, allFilePaths, repoPath);

    // 3. Loop back to re-validate current file until it passes
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

    // ================= INTERACTIVE PROMPTS =================
    log('\n======================================================');
    log('  AUTOMATED PERMISSION SET & PROFILE DEPLOY & FIX');
    log('======================================================\n');

    let promotionJsonPath = flags['json-path'] ?? '';
    while (!promotionJsonPath || !fs.existsSync(promotionJsonPath)) {
      if (promotionJsonPath) log('   File not found at that path. Please try again.\n');
      // eslint-disable-next-line no-await-in-loop
      promotionJsonPath = await prompt(
        'Enter full path to your Copado Promotion JSON\n   (e.g. C:\\Users\\YourName\\Desktop\\promotion.json)\n> '
      );
      promotionJsonPath = promotionJsonPath.replace(/^"|"$/g, '').trim();
    }
    log('   JSON file found.\n');

    let targetOrg = flags['target-org'] ?? '';
    if (!targetOrg) {
      // eslint-disable-next-line no-await-in-loop
      targetOrg = await prompt(
        'Enter target org username or alias\n   (e.g. RBKQA or user@rubrik.com.qa)\n> '
      );
      targetOrg = targetOrg.trim();
    }
    log(`\n   Target Org set to: ${targetOrg}\n`);

    // ================= LOAD & PARSE JSON =================
    log('Loading promotion JSON...');
    const promotionData = JSON.parse(fs.readFileSync(promotionJsonPath, 'utf8')) as PromotionItem[];

    const permSets = [...new Set(promotionData.filter((i) => i.t === 'PermissionSet').map((i) => i.n))].sort();
    const profiles = [...new Set(promotionData.filter((i) => i.t === 'Profile').map((i) => i.n))].sort();

    const whitelist: WhitelistMap = {
      fields: [...new Set(promotionData.filter((i) => i.t === 'CustomField').map((i) => i.n))].sort(),
      apps: [...new Set(promotionData.filter((i) => i.t === 'CustomApplication').map((i) => i.n))].sort(),
      classes: [...new Set(promotionData.filter((i) => i.t === 'ApexClass').map((i) => i.n))].sort(),
      pages: [...new Set(promotionData.filter((i) => i.t === 'ApexPage').map((i) => i.n))].sort(),
      tabs: [...new Set(promotionData.filter((i) => i.t === 'CustomTab').map((i) => i.n))].sort(),
      objects: [...new Set(promotionData.filter((i) => i.t === 'CustomObject').map((i) => i.n))].sort(),
      flows: [...new Set(promotionData.filter((i) => i.t === 'Flow').map((i) => i.n))].sort(),
    };

    // Build full file path list upfront — sweepOtherFiles needs this.
    const allFilePaths: string[] = [
      ...permSets.map((ps) => path.join(PS_BASE_PATH, `${ps}.permissionset-meta.xml`)),
      ...profiles.map((p) => path.join(PROFILE_BASE_PATH, `${p}.profile-meta.xml`)),
    ];

    const totalWhitelisted = Object.values(whitelist).reduce((sum, arr) => sum + arr.length, 0);

    // ================= STARTUP SUMMARY =================
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
    // eslint-disable-next-line no-await-in-loop
    const confirm = await prompt("Press ENTER to start or type 'exit' to cancel\n> ");
    if (confirm.trim().toLowerCase() === 'exit') {
      log('\nScript cancelled by user.');
      return;
    }
    log('\nStarting script...');
    log('\n======================================================');

    const summary: SummaryRecord[] = [];
    const totalDeploys: TotalDeploys = { value: 0 };

    const common = {
      targetOrg,
      repoPath: REPO_PATH,
      whitelist,
      maxIterations: MAX_ITERATIONS,
      maxTotalDeploys: MAX_TOTAL_DEPLOYS,
      totalDeploys,
      timeoutMins: DEPLOY_TIMEOUT_MINS,
      maxRetries: MAX_RETRIES,
      allFilePaths,
    };

    // ================= PROCESS PERMISSION SETS =================
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

    // ================= PROCESS PROFILES =================
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

    // ================= FINAL SUMMARY =================
    log('\n======================================================');
    log('ALL ITEMS PROCESSED - FINAL SUMMARY');
    log('======================================================');

    log('\nPERMISSION SETS:');
    summary
      .filter((r) => r.Type === 'PermissionSet')
      .forEach((r) => log(`   [${r.Name}] Status: ${r.Status} | Removed: ${r.RemovedFields || 'none'} | Skipped: ${r.SkippedFields || 'none'}`));

    log('\nPROFILES:');
    summary
      .filter((r) => r.Type === 'Profile')
      .forEach((r) => log(`   [${r.Name}] Status: ${r.Status} | Removed: ${r.RemovedFields || 'none'} | Skipped: ${r.SkippedFields || 'none'}`));

    const csvPath = path.join(REPO_PATH, 'deploy_fix_summary.csv');
    const csvHeader = 'Type,Name,Status,RemovedFields,SkippedFields';
    const csvRows = summary.map(
      (r) => `${r.Type},"${r.Name}","${r.Status}","${r.RemovedFields}","${r.SkippedFields}"`
    );
    fs.writeFileSync(csvPath, [csvHeader, ...csvRows].join('\n'), 'utf8');

    log(`Summary CSV saved to : ${csvPath}`);
    log(`Total deploy calls   : ${totalDeploys.value} / ${MAX_TOTAL_DEPLOYS}`);
  }
}
