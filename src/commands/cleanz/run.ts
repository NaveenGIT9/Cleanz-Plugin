/*
 * Copyright 2025, Salesforce, Inc.
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

// ===============================================================
// HELPER: SAVE XML CLEANLY — pure string-based, no xmldom
// (Equivalent to Save-XmlClean in PowerShell)
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
// (Replaces xmldom DOM manipulation)
// ===============================================================

function removeFieldPermissionsFromXml(
  xmlContent: string,
  missingField: string
): { updated: string; removed: boolean } {
  const escapedField = missingField.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  // Match only the single <fieldPermissions> block whose <field> equals missingField exactly.
  // Use (?:(?!<fieldPermissions>)[\s\S])*? so the match cannot bleed into the next block.
  const innerPattern = '(?:(?!<fieldPermissions>)[\\s\\S])*?';
  const blockRegex = new RegExp(
    `[ \\t]*<fieldPermissions>${innerPattern}<field>[ \\t]*${escapedField}[ \\t]*</field>${innerPattern}</fieldPermissions>[ \\t]*\\r?\\n?`,
    'g'
  );
  const updated = xmlContent.replace(blockRegex, '');
  const removed = updated !== xmlContent;
  return { updated, removed };
}

function getRootNodeName(xmlContent: string): string {
  const match = /<(\w+)\s+xmlns=/i.exec(xmlContent) ?? /<(\w+)>/i.exec(xmlContent);
  return match ? match[1] : 'PermissionSet';
}

// ===============================================================
// GENERIC BLOCK REMOVER — reusable for any single-key XML block
// blockTag    : e.g. 'classAccesses'
// keyTag      : the child tag whose text must match, e.g. 'apexClass'
// missingName : the value to match against
// ===============================================================

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
  const removed = updated !== xmlContent;
  return { updated, removed };
}

// ── Typed wrappers (self-documenting call sites) ──────────────

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
// (Equivalent to Invoke-DeployWithRetry in PowerShell)
// ===============================================================

// Transient SF CLI error patterns that should be silently retried with backoff
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
  // Exponential backoff: 15s, 30s, 60s, 120s — capped at 2 min
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
  // Hard cap: keep retrying transient errors up to this many total attempts
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

    // Check for transient errors BEFORE trying to parse — catches non-JSON error dumps too
    if (isTransientError(raw)) {
      const backoff = getBackoffMs(attempt);
      log(`   Transient error detected — waiting ${backoff / 1000}s before retry...`);
      // eslint-disable-next-line no-await-in-loop
      await sleep(backoff);
      attempt = 0; // reset attempt counter so backoff resets after a transient burst
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

    // Check parsed result for transient error messages
    const errText = `${result.message ?? ''} ${result.name ?? ''}`;
    if (!result.result && isTransientError(errText)) {
      const backoff = getBackoffMs(attempt);
      log(`   Transient SF CLI error (${result.name ?? 'unknown'}) — waiting ${backoff / 1000}s before retry...`);
      // eslint-disable-next-line no-await-in-loop
      await sleep(backoff);
      attempt = 0;
      continue;
    }

    // Normalise alternate SF CLI response shapes into the expected shape.
    // Some SF CLI versions wrap the deploy result differently.
    if (!result.result && result.status !== undefined) {
      // Shape: { status: 0, result: undefined } — SF CLI returned top-level fields only.
      // Treat status 0 as success, anything else as a failure with no component details.
      log(`   Normalising SF CLI response (status=${result.status}).`);
      result.result = {
        success: result.status === 0,
        details: { componentFailures: [] },
      };
    }

    // Valid response — return it
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
// HELPER: PROCESS FAILURES FOR ONE ITERATION
// Extracted to reduce complexity of the main loop function
// ===============================================================

function processFailures(
  log: (msg: string) => void,
  failures: ComponentFailure[],
  xmlContent: string,
  whitelistedFields: string[],
  repoPath: string,
  allSkippedFields: string[]
): { xmlContent: string; removedFields: string[]; skippedFields: string[] } {
  let updatedXml = xmlContent;
  const removedFields: string[] = [];
  const skippedFields: string[] = [];

  for (const failure of failures) {
    const errorMessage = failure.problem;

    // ── Missing CustomField ──────────────────────────────────────
    const fieldMatch = /no CustomField named (.+?) found/.exec(errorMessage);
    if (fieldMatch) {
      const missingField = fieldMatch[1].trim();

      if (whitelistedFields.includes(missingField)) {
        log(`   SKIPPING whitelisted field (in JSON): ${missingField}`);
        skippedFields.push(missingField);
        allSkippedFields.push(missingField);
        continue;
      }

      const parts = missingField.split('.');
      if (parts.length === 2) {
        const fieldFilePath = path.join(
          repoPath, 'force-app', 'main', 'default', 'objects',
          parts[0], 'fields', `${parts[1]}.field-meta.xml`
        );
        if (fs.existsSync(fieldFilePath)) {
          log(`   SKIPPING: field exists in repo but missing from org: ${missingField}`);
          log('   WARNING: Deploy the field first, then re-run this script.');
          log(`   Found at: ${fieldFilePath}`);
          skippedFields.push(missingField);
          allSkippedFields.push(missingField);
          continue;
        }
      }

      log(`   Missing field: ${missingField}`);
      const { updated, removed } = removeFieldPermissionsFromXml(updatedXml, missingField);
      if (removed) {
        updatedXml = updated;
        log(`   Removed fieldPermissions for: ${missingField}`);
        removedFields.push(missingField);
      } else {
        log(`   Field not found in XML: ${missingField} - already removed or not present.`);
      }
      continue;
    }

    // ── Missing CustomApplication (applicationVisibilities) ──────
    const appMatch = /no CustomApplication named (.+?) found/.exec(errorMessage);
    if (appMatch) {
      const name = appMatch[1].trim();
      log(`   Missing application: ${name}`);
      const { updated, removed } = removeApplicationVisibilityFromXml(updatedXml, name);
      if (removed) {
        updatedXml = updated;
        log(`   Removed applicationVisibilities for: ${name}`);
        removedFields.push(`[App] ${name}`);
      } else {
        log(`   applicationVisibilities not found in XML: ${name} - already removed or not present.`);
      }
      continue;
    }

    // ── Missing ApexClass (classAccesses) ────────────────────────
    const classMatch = /no ApexClass named (.+?) found/.exec(errorMessage);
    if (classMatch) {
      const name = classMatch[1].trim();
      log(`   Missing ApexClass: ${name}`);
      const { updated, removed } = removeClassAccessFromXml(updatedXml, name);
      if (removed) {
        updatedXml = updated;
        log(`   Removed classAccesses for: ${name}`);
        removedFields.push(`[Class] ${name}`);
      } else {
        log(`   classAccesses not found in XML: ${name} - already removed or not present.`);
      }
      continue;
    }

    // ── Missing ApexPage (pageAccesses) ──────────────────────────
    const pageMatch = /no ApexPage named (.+?) found/.exec(errorMessage);
    if (pageMatch) {
      const name = pageMatch[1].trim();
      log(`   Missing ApexPage: ${name}`);
      const { updated, removed } = removePageAccessFromXml(updatedXml, name);
      if (removed) {
        updatedXml = updated;
        log(`   Removed pageAccesses for: ${name}`);
        removedFields.push(`[Page] ${name}`);
      } else {
        log(`   pageAccesses not found in XML: ${name} - already removed or not present.`);
      }
      continue;
    }

    // ── Missing CustomTab (tabSettings) ──────────────────────────
    const tabMatch = /no CustomTab named (.+?) found/.exec(errorMessage);
    if (tabMatch) {
      const name = tabMatch[1].trim();
      log(`   Missing CustomTab: ${name}`);
      const { updated, removed } = removeTabSettingFromXml(updatedXml, name);
      if (removed) {
        updatedXml = updated;
        log(`   Removed tabSettings for: ${name}`);
        removedFields.push(`[Tab] ${name}`);
      } else {
        log(`   tabSettings not found in XML: ${name} - already removed or not present.`);
      }
      continue;
    }

    // ── Missing CustomObject (objectPermissions) ──────────────────
    const objectMatch = /no CustomObject named (.+?) found/.exec(errorMessage);
    if (objectMatch) {
      const name = objectMatch[1].trim();
      log(`   Missing CustomObject: ${name}`);
      const { updated, removed } = removeObjectPermissionFromXml(updatedXml, name);
      if (removed) {
        updatedXml = updated;
        log(`   Removed objectPermissions for: ${name}`);
        removedFields.push(`[Object] ${name}`);
      } else {
        log(`   objectPermissions not found in XML: ${name} - already removed or not present.`);
      }
      continue;
    }

    // ── Missing Flow (flowAccesses) ───────────────────────────────
    const flowMatch = /no Flow named (.+?) found/.exec(errorMessage);
    if (flowMatch) {
      const name = flowMatch[1].trim();
      log(`   Missing Flow: ${name}`);
      const { updated, removed } = removeFlowAccessFromXml(updatedXml, name);
      if (removed) {
        updatedXml = updated;
        log(`   Removed flowAccesses for: ${name}`);
        removedFields.push(`[Flow] ${name}`);
      } else {
        log(`   flowAccesses not found in XML: ${name} - already removed or not present.`);
      }
      continue;
    }

    // ── Unhandled error ──────────────────────────────────────────
    log(`   Skipping unhandled error: ${errorMessage}`);
  }

  return { xmlContent: updatedXml, removedFields, skippedFields };
}

// ===============================================================
// HELPER: PROCESS SINGLE METADATA ITEM
// (Equivalent to Invoke-ProcessMetadataItem in PowerShell)
// ===============================================================

async function invokeProcessMetadataItem(
  log: (msg: string) => void,
  params: {
    metadataType: string;
    itemName: string;
    filePath: string;
    targetOrg: string;
    repoPath: string;
    whitelistedFields: string[];
    maxIterations: number;
    maxTotalDeploys: number;
    totalDeploys: TotalDeploys;
    timeoutMins: number;
    maxRetries: number;
  }
): Promise<SummaryRecord> {
  const {
    metadataType, itemName, filePath, targetOrg, repoPath,
    whitelistedFields, maxIterations, maxTotalDeploys,
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

    // result object still missing after normalisation — truly unknown shape, stop safely
    if (!deployResult.result) {
      log(`[${itemName}] SF CLI returned an unrecognised response shape. Raw keys: ${Object.keys(deployResult).join(', ')}`);
      log(`[${itemName}] Full response: ${JSON.stringify(deployResult)}`);
      log(`[${itemName}] Stopping — re-run the script once the org is reachable.`);
      itemStatus = 'Deploy Failed - Unrecognised Response';
      break;
    }

    if (deployResult.result.success === true) {
      log(`[${itemName}] Deploy validation SUCCESSFUL!`);
      itemStatus = 'Success';
      break;
    }

    const failures = deployResult.result.details?.componentFailures;
    if (!failures || failures.length === 0) {
      // success is false but no failures listed — treat as success (warnings only)
      log(`[${itemName}] No component failures found. Moving on.`);
      itemStatus = 'Success';
      break;
    }

    const xmlContent = fs.readFileSync(filePath, 'utf8');
    const rootNode = getRootNodeName(xmlContent);

    const { xmlContent: updatedXml, removedFields, skippedFields } = processFailures(
      log, failures, xmlContent, whitelistedFields, repoPath, allSkippedFields
    );

    allRemovedFields.push(...removedFields);

    if (removedFields.length === 0) {
      if (skippedFields.length > 0) {
        log(`[${itemName}] Only whitelisted/repo fields remain. Manual deploy needed.`);
        itemStatus = 'Whitelisted Fields Only - Manual Deploy Needed';
      } else {
        log(`[${itemName}] No fields removed this iteration. Moving on.`);
        itemStatus = 'Partial / Manual Check Needed';
      }
      continueLoop = false;
      break;
    }

    saveXmlClean(updatedXml, filePath, rootNode);
    log(`Saved updated XML for: ${itemName}`);

    const commitMessage = `[${itemName}] Auto-remove missing fields: ${removedFields.join(', ')}`;
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
    const whitelistedFields = [...new Set(promotionData.filter((i) => i.t === 'CustomField').map((i) => i.n))].sort();

    // ================= STARTUP SUMMARY =================
    log('\n======================================================');
    log('STARTING AUTOMATED DEPLOY & FIX LOOP');
    log('======================================================');
    log(`Target Org         : ${targetOrg}`);
    log(`Permission Sets    : ${permSets.length} found in JSON`);
    log(`Profiles           : ${profiles.length} found in JSON`);
    log(`Whitelisted Fields : ${whitelistedFields.length} found in JSON (will never be removed)`);
    log(`Max per item       : ${MAX_ITERATIONS} iterations`);
    log(`Global deploy cap  : ${MAX_TOTAL_DEPLOYS} total deploys`);
    log(`Deploy timeout     : ${DEPLOY_TIMEOUT_MINS} min(s) per attempt`);
    log(`Max retries        : ${MAX_RETRIES} per deploy call`);

    log('\nPermission Sets to process:');
    permSets.forEach((ps) => log(`   - ${ps}`));
    log('\nProfiles to process:');
    profiles.forEach((p) => log(`   - ${p}`));
    log('\nWhitelisted Fields (will never be removed):');
    whitelistedFields.forEach((f) => log(`   - ${f}`));

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

    // ================= PROCESS PERMISSION SETS =================
    log('\n######################################################');
    log(`  PROCESSING PERMISSION SETS (${permSets.length})`);
    log('######################################################');

    for (const psName of permSets) {
      const psFilePath = path.join(PS_BASE_PATH, `${psName}.permissionset-meta.xml`);
      // eslint-disable-next-line no-await-in-loop
      const result = await invokeProcessMetadataItem(log, {
        metadataType: 'PermissionSet',
        itemName: psName,
        filePath: psFilePath,
        targetOrg,
        repoPath: REPO_PATH,
        whitelistedFields,
        maxIterations: MAX_ITERATIONS,
        maxTotalDeploys: MAX_TOTAL_DEPLOYS,
        totalDeploys,
        timeoutMins: DEPLOY_TIMEOUT_MINS,
        maxRetries: MAX_RETRIES,
      });
      summary.push(result);
      if (totalDeploys.value > MAX_TOTAL_DEPLOYS) break;
    }

    // ================= PROCESS PROFILES =================
    if (totalDeploys.value <= MAX_TOTAL_DEPLOYS) {
      log('\n######################################################');
      log(`  PROCESSING PROFILES (${profiles.length})`);
      log('######################################################');

      for (const profileName of profiles) {
        const profileFilePath = path.join(PROFILE_BASE_PATH, `${profileName}.profile-meta.xml`);
        // eslint-disable-next-line no-await-in-loop
        const result = await invokeProcessMetadataItem(log, {
          metadataType: 'Profile',
          itemName: profileName,
          filePath: profileFilePath,
          targetOrg,
          repoPath: REPO_PATH,
          whitelistedFields,
          maxIterations: MAX_ITERATIONS,
          maxTotalDeploys: MAX_TOTAL_DEPLOYS,
          totalDeploys,
          timeoutMins: DEPLOY_TIMEOUT_MINS,
          maxRetries: MAX_RETRIES,
        });
        summary.push(result);
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
