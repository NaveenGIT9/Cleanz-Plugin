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
const messages = Messages.loadMessages('@naveengit9/plugin-cleanz', 'cleanz.run');

// ===============================================================
// TYPES
// ===============================================================

type PromotionItem = {
  t: string;
  n: string;
  a?: string; // operation: "Add" | "Retrieve" — only present in Copado JSON, not package.xml
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
  problem?: string; // older SF CLI versions
  error?: string; // newer SF CLI versions use "error" instead of "problem"
  fullName?: string; // component name e.g. "Rubrik Field Sales User - Old"
  fileName?: string; // relative path (older CLI)
  filePath?: string; // absolute path (newer CLI)
  componentType?: string;
  type?: string; // newer CLI uses "type" instead of "componentType"
};

type SummaryRecord = {
  Type: string;
  Name: string;
  Status: string;
  RemovedFields: string;
  RemovedErrors: string;
  SkippedFields: string;
  UnhandledErrors: string;
};

type TotalDeploys = { value: number };

type BatchItem = {
  metadataType: string;
  itemName: string;
  filePath: string;
  operation: 'ADD' | 'FULL'; // ADD = delta commit, FULL = entire file committed
  status: string;
  allRemovedFields: Array<{ label: string; error: string }>;
  allRemovedRefs: RemovedRef[]; // full ref objects for repo-wide sweep
  allSkippedFields: string[];
  allUnhandledErrors: string[];
  done: boolean;
};

type WhitelistMap = {
  fields: string[];
  apps: string[];
  classes: string[];
  pages: string[];
  tabs: string[];
  objects: string[];
  flows: string[];
  layouts: string[];
  flexipages: string[];
  recordTypes: string[]; // "Object.DeveloperName" — profileActionOverrides blocks referencing these are kept
  customMetadataTypes: string[]; // "ApiName__mdt" — customMetadataTypeAccesses blocks referencing these are kept
  customPermissions: string[]; // "ApiName" — customPermissions blocks referencing these are kept
  recordTypeVisibilities: string[]; // "Object.DeveloperName" — recordTypeVisibilities blocks referencing these are kept
};

// Carries enough info to remove a ref from ANY other file in the batch.
type RefType =
  | 'field'
  | 'app'
  | 'class'
  | 'page'
  | 'tab'
  | 'object'
  | 'flow'
  | 'layout'
  | 'flexipage'
  | 'namespace'
  | 'userPermission'
  | 'objectFlag' // a specific boolean flag inside an objectPermissions block (e.g. viewAllRecords)
  | 'recordTypeOverride' // profileActionOverrides block with an invalid <recordType> reference
  | 'customMetadataType' // customMetadataTypeAccesses block referencing a missing __mdt type
  | 'customPermission' // customPermissions block referencing a missing CustomPermission
  | 'recordTypeVisibility'; // recordTypeVisibilities block referencing a missing RecordType

type RemovedRef = {
  type: RefType;
  name: string;
  label: string; // display string e.g. "Account.Name" or "[Class] MyClass"
  meta?: string; // extra data — used by 'objectFlag' to carry the XML element name (e.g. "viewAllRecords")
  deployError?: string; // the Salesforce error message that triggered this removal
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

const REPO_PATH = execSync('git rev-parse --show-toplevel', { cwd: process.cwd() }).toString().trim();
const PS_BASE_PATH = path.join(REPO_PATH, 'force-app', 'main', 'default', 'permissionsets');
const MUTING_PS_BASE_PATH = path.join(REPO_PATH, 'force-app', 'main', 'default', 'mutingpermissionsets');
const PROFILE_BASE_PATH = path.join(REPO_PATH, 'force-app', 'main', 'default', 'profiles');
const MAX_ITERATIONS = 500;
const MAX_TOTAL_DEPLOYS = 1000;
const DEPLOY_TIMEOUT_MINS = 25;
const MAX_RETRIES = 3;
const MAX_QUEUE_WAIT_MINS = 60; // wait up to 60 min for active Copado deployments to finish

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

export function formatXml(xml: string): string {
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

    if (!trimmed.startsWith('<?') && !trimmed.startsWith('</') && !trimmed.endsWith('/>') && !trimmed.includes('</')) {
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

export function getRootNodeName(xmlContent: string): string {
  const match = /<(\w+)\s+xmlns=/i.exec(xmlContent) ?? /<(\w+)>/i.exec(xmlContent);
  return match ? match[1] : 'PermissionSet';
}

function writeConclusionFile(log: (msg: string) => void, content: string, repoPath: string): void {
  try {
    const branch = execSync('git rev-parse --abbrev-ref HEAD', { cwd: repoPath }).toString().trim();
    const safeBranch = branch.replace(/\//g, '-');
    const now = new Date();
    const istMs = now.getTime() + (5 * 60 + 30) * 60 * 1000;
    const ist = new Date(istMs);
    const months = [
      'January',
      'February',
      'March',
      'April',
      'May',
      'June',
      'July',
      'August',
      'September',
      'October',
      'November',
      'December',
    ];
    const month = months[ist.getUTCMonth()];
    const day = ist.getUTCDate();
    const hours24 = ist.getUTCHours();
    const hours12 = hours24 % 12 || 12;
    const minutes = String(ist.getUTCMinutes()).padStart(2, '0');
    const ampm = hours24 >= 12 ? 'PM' : 'AM';
    const ts = `${month}-${day}-${hours12}-${minutes}-${ampm}`;
    const fileName = `${safeBranch}-Conclusion-${ts}_IST.txt`;
    const driveRoot = path.parse(repoPath).root; // e.g. "D:\"
    const outputDir = path.join(driveRoot, 'CleanzConclusionErrors');
    if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir);
    const filePath = path.join(outputDir, fileName);
    fs.writeFileSync(filePath, content, 'utf8');
    log(`Conclusion file      : ${filePath}`);
  } catch (e) {
    log(`Could not save conclusion file: ${String(e)}`);
  }
}

function logRemovedRefsDetail(log: (msg: string) => void, summary: SummaryRecord[]): void {
  const fixedPermSets = summary.filter((r) => r.Type === 'PermissionSet' && r.RemovedFields);
  const fixedMutingPermSets = summary.filter((r) => r.Type === 'MutingPermissionSet' && r.RemovedFields);
  const fixedProfiles = summary.filter((r) => r.Type === 'Profile' && r.RemovedFields);
  if (fixedPermSets.length === 0 && fixedMutingPermSets.length === 0 && fixedProfiles.length === 0) return;

  const buildRows = (records: SummaryRecord[]): string[][] =>
    records.flatMap((r) => {
      const labels = r.RemovedFields.split('; ').filter(Boolean);
      const errors = r.RemovedErrors ? r.RemovedErrors.split('; ') : [];
      return labels.map((label, i) => [r.Name, label, errors[i] ?? '']);
    });

  log('\nRemoved refs detail:');
  if (fixedPermSets.length > 0) {
    log('\nPERMISSION SETS');
    log(buildAsciiTable(['Name', 'Removed Ref', 'Deployment Error'], buildRows(fixedPermSets), [15, 35, 45]));
  }
  if (fixedMutingPermSets.length > 0) {
    log('\nMUTING PERMISSION SETS');
    log(buildAsciiTable(['Name', 'Removed Ref', 'Deployment Error'], buildRows(fixedMutingPermSets), [15, 35, 45]));
  }
  if (fixedProfiles.length > 0) {
    log('\nPROFILES');
    log(buildAsciiTable(['Name', 'Removed Ref', 'Deployment Error'], buildRows(fixedProfiles), [15, 35, 45]));
  }
}

function wrapText(text: string, maxWidth: number): string[] {
  if (text.length <= maxWidth) return [text];
  const lines: string[] = [];
  let remaining = text;
  while (remaining.length > maxWidth) {
    let breakAt = remaining.lastIndexOf(' ', maxWidth);
    if (breakAt <= 0) breakAt = maxWidth;
    lines.push(remaining.slice(0, breakAt).trimEnd());
    remaining = remaining.slice(breakAt).trimStart();
  }
  if (remaining) lines.push(remaining);
  return lines;
}

export function buildAsciiTable(headers: string[], rows: string[][], maxColWidths?: number[]): string {
  // Expand rows: cells exceeding maxColWidths are split into continuation lines.
  const expandedRows: string[][] = rows.flatMap((row) => {
    const wrappedCells = row.map((cell, colIdx) => {
      const max = maxColWidths?.[colIdx];
      return max && cell.length > max ? wrapText(cell, max) : [cell];
    });
    const maxLines = Math.max(...wrappedCells.map((c) => c.length));
    return Array.from({ length: maxLines }, (_, lineIdx) => wrappedCells.map((lines) => lines[lineIdx] ?? ''));
  });

  const allRows = [headers, ...expandedRows];
  const colWidths = headers.map((_, colIdx) => Math.max(...allRows.map((row) => (row[colIdx] ?? '').length)));
  const sep = '+' + colWidths.map((w) => '-'.repeat(w + 2)).join('+') + '+';
  const formatRow = (cells: string[]): string =>
    '|' + cells.map((c, i) => ` ${(c ?? '').padEnd(colWidths[i])} `).join('|') + '|';
  return [sep, formatRow(headers), sep, ...expandedRows.map(formatRow), sep].join('\n');
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

export function removeXmlBlock(
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
function removeProfileActionOverrideFromXml(xmlContent: string, name: string): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'profileActionOverrides', 'content', name);
}
// Removes profileActionOverrides blocks keyed by their <recordType> value.
// Used during whitelist masking: if a RecordType is in the promotion JSON (being deployed),
// its profileActionOverrides block is masked before each dry-run so the RT-not-found error
// doesn't consume the one-error-per-component slot.
function removeProfileActionOverrideByRecordTypeFromXml(
  xmlContent: string,
  recordTypeName: string
): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'profileActionOverrides', 'recordType', recordTypeName);
}
// Removes profileActionOverrides blocks keyed by their <pageOrSobjectType> value.
// Used during whitelist masking: if an object is in the promotion JSON (being deployed),
// its profileActionOverrides block is masked before each dry-run.
function removeProfileActionOverrideByPageObjectFromXml(
  xmlContent: string,
  objectName: string
): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'profileActionOverrides', 'pageOrSobjectType', objectName);
}
// Removes profileActionOverrides blocks whose <pageOrSobjectType> is a custom object that
// does NOT exist in the org and is NOT being deployed in this promotion.
// Standard objects (no __c suffix) are always kept — they always exist.
export function removeProfileActionOverridesWithMissingObject(
  xmlContent: string,
  existingObjects: Set<string>,
  whitelistedObjects: string[]
): { updated: string; removedObjects: string[] } {
  const inner = '(?:(?!<profileActionOverrides>)[\\s\\S])*?';
  const blockRegex = new RegExp(
    `[ \\t]*<profileActionOverrides>${inner}<pageOrSobjectType>([^<]*)</pageOrSobjectType>${inner}</profileActionOverrides>[ \\t]*\\r?\\n?`,
    'g'
  );
  const removedObjects: string[] = [];
  const updated = xmlContent.replace(blockRegex, (match: string, obj: string) => {
    const o = obj.trim();
    if (!o.endsWith('__c')) return match; // standard object — always exists, keep
    if (existingObjects.has(o)) return match; // exists in org — keep
    if (whitelistedObjects.includes(o)) return match; // being deployed — keep
    removedObjects.push(o);
    return '';
  });
  return { updated, removedObjects };
}
// Removes profileActionOverrides blocks whose <recordType> value is NOT in the org's
// active RecordType set. Returns the updated XML and the list of invalid RT values removed.
// Salesforce doesn't report the specific RecordType name in the error, so we query the org
// for all active RecordTypes and surgically remove only the blocks that reference missing ones.
export function removeProfileActionOverridesWithMissingRecordType(
  xmlContent: string,
  existingRecordTypes: Set<string>,
  whitelistedRecordTypes: string[]
): { updated: string; removedRecordTypes: string[] } {
  const inner = '(?:(?!<profileActionOverrides>)[\\s\\S])*?';
  const blockRegex = new RegExp(
    `[ \\t]*<profileActionOverrides>${inner}<recordType>([^<]*)</recordType>${inner}</profileActionOverrides>[ \\t]*\\r?\\n?`,
    'g'
  );
  const removedRecordTypes: string[] = [];
  const updated = xmlContent.replace(blockRegex, (match: string, recordType: string) => {
    const rt = recordType.trim();
    if (existingRecordTypes.has(rt)) return match; // exists in org — keep
    if (whitelistedRecordTypes.includes(rt)) return match; // being deployed in this promotion — keep
    removedRecordTypes.push(rt);
    return '';
  });
  return { updated, removedRecordTypes };
}
function removeLayoutAssignmentFromXml(xmlContent: string, name: string): { updated: string; removed: boolean } {
  // Profiles store layout refs in <layoutAssignments> blocks keyed by <layout>.
  // A block may also contain a <recordType> child — removeXmlBlock handles this correctly
  // because its inner pattern is non-greedy and stops at the next block opener.
  return removeXmlBlock(xmlContent, 'layoutAssignments', 'layout', name);
}
function removeUserPermissionFromXml(xmlContent: string, name: string): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'userPermissions', 'name', name);
}
function removeCustomMetadataTypeAccessFromXml(
  xmlContent: string,
  name: string
): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'customMetadataTypeAccesses', 'name', name);
}
function removeCustomPermissionFromXml(xmlContent: string, name: string): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'customPermissions', 'name', name);
}
function removeRecordTypeVisibilityFromXml(xmlContent: string, name: string): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'recordTypeVisibilities', 'recordType', name);
}

// Removes a single <flagElement>true</flagElement> line from the objectPermissions block
// for the given object — used when "The user license doesn't allow the permission: X" fires.
// Only removes the flag when its value is "true"; if already false/absent, no-op.
function removeObjectPermissionFlag(
  xmlContent: string,
  objectName: string,
  flagElement: string
): { updated: string; removed: boolean } {
  const escapedObject = objectName.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const escapedFlag = flagElement.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const inner = '(?:(?!<objectPermissions>)[\\s\\S])*?';
  const blockRegex = new RegExp(
    `(<objectPermissions>${inner}<object>[ \\t]*${escapedObject}[ \\t]*</object>${inner}</objectPermissions>)`,
    'g'
  );
  let removed = false;
  const updated = xmlContent.replace(blockRegex, (blockMatch: string) => {
    const flagRegex = new RegExp(`[ \\t]*<${escapedFlag}>true</${escapedFlag}>[ \\t]*\\r?\\n?`, 'g');
    const newBlock = blockMatch.replace(flagRegex, '');
    if (newBlock !== blockMatch) {
      removed = true;
      return newBlock;
    }
    return blockMatch;
  });
  return { updated, removed };
}

// ===============================================================
// NAMESPACE BULK REMOVAL
// When a managed package is not installed in the org, every single
// component it owns (fields, objects, classes, tabs, flows, apps, pages)
// will fail deployment. Instead of iterating one-by-one, we detect the
// namespace prefix, confirm the package is absent, and strip all its refs
// in one pass — then sweep the same removal across every other file.
// ===============================================================

// Caches for RecordType queries (keyed by org alias)
// Set entries are "SobjectType.DeveloperName" (e.g. "Contact.Sales_Rep") — the same
// format used in profileActionOverrides <recordType> elements.
const recordTypeCache = new Map<string, Set<string>>(); // org → Set of active "Object.DevName"
// Keyed by org alias → object API name → exists? Populated lazily per queried object.
const objectExistenceCache = new Map<string, Map<string, boolean>>();

// Caches for namespace queries (keyed by org alias or "org:namespace")
const namespaceCache = new Map<string, boolean>(); // "org:namespace" → installed?
const installedNsCache = new Map<string, Set<string>>(); // org → Set of all installed namespace prefixes

// Salesforce built-in prefixes that look like namespace prefixes but are NOT managed packages.
// These must never be passed to the namespace installer check or bulk-removed.
const SF_RESERVED_PREFIXES = new Set(['standard', 'force', 'chatter', 'sf']);

function extractNamespaceFromError(errorMessage: string): string | null {
  // Matches "Namespace__" prefix inside names like:
  //   "Account.UniqueEntry__Field__c"  → "UniqueEntry"
  //   "UniqueEntry__Object__c"         → "UniqueEntry"
  // A real namespace-prefixed component always has TWO double-underscores:
  //   Namespace__ComponentName__c  (e.g. UniqueEntry__Field__c, Rubrik__Obj__c)
  // A plain custom field has only ONE:
  //   PrecedingOpportunityOwner__c  ← NOT a namespace, just a field name
  // The regex requires a second __ after the captured prefix to avoid false positives.
  const m = /named\s+(?:\w+\.)?([A-Za-z][A-Za-z0-9]*)__\w[^.\s]*__/.exec(errorMessage);
  const ns = m?.[1] ?? null;
  // Skip Salesforce built-in prefixes — they are not managed packages.
  if (ns && SF_RESERVED_PREFIXES.has(ns.toLowerCase())) return null;
  return ns;
}

// Shared SF CLI query helper — returns records array or [] on failure.
// useTooling=true → adds --use-tooling-api (Tooling API); false → regular SOQL.
function runSfQuery<T extends object>(targetOrg: string, quotedQuery: string, useTooling: boolean): Promise<T[]> {
  return new Promise<T[]>((resolve) => {
    const args = [
      'data',
      'query',
      '--query',
      quotedQuery,
      ...(useTooling ? ['--use-tooling-api'] : []),
      '--target-org',
      targetOrg,
      '--json',
    ];
    const proc = spawn('sf', args, { shell: true });
    const chunks: string[] = [];
    proc.stdout.on('data', (d: Buffer) => chunks.push(d.toString()));
    proc.stderr.on('data', (d: Buffer) => chunks.push(d.toString()));
    const timer = setTimeout(() => {
      proc.kill();
      resolve([]);
    }, 30_000);
    proc.on('close', () => {
      clearTimeout(timer);
      try {
        const raw = chunks.join('');
        const start = raw.indexOf('{');
        const json = JSON.parse(start >= 0 ? raw.substring(start) : raw) as { result?: { records?: T[] } };
        resolve(json?.result?.records ?? []);
      } catch {
        resolve([]);
      }
    });
  });
}
function toolingQuery<T extends object>(targetOrg: string, quotedQuery: string): Promise<T[]> {
  return runSfQuery<T>(targetOrg, quotedQuery, true);
}
function soqlQuery<T extends object>(targetOrg: string, quotedQuery: string): Promise<T[]> {
  return runSfQuery<T>(targetOrg, quotedQuery, false);
}

// Loads all ACTIVE RecordTypes from the org and caches the Set as "SobjectType.DeveloperName".
// Uses regular SOQL (not Tooling API) — RecordType is a standard object.
async function loadExistingRecordTypes(log: (msg: string) => void, targetOrg: string): Promise<Set<string>> {
  if (recordTypeCache.has(targetOrg)) return recordTypeCache.get(targetOrg)!;

  log('   [RT Check] Querying org for active RecordTypes...');
  type RTRec = { SobjectType: string; DeveloperName: string };
  const records = await soqlQuery<RTRec>(
    targetOrg,
    '"SELECT SobjectType, DeveloperName FROM RecordType WHERE IsActive = true"'
  );
  const set = new Set(records.map((r) => `${r.SobjectType}.${r.DeveloperName}`));
  recordTypeCache.set(targetOrg, set);
  log(`   [RT Check] Found ${set.size} active RecordType(s) in org`);
  return set;
}

// Checks which of the given custom object API names exist in the org using a targeted
// EntityDefinition Tooling API query. Standard objects (no __c suffix) are assumed to always
// exist and are never queried. Results are cached per org so subsequent profile files pay no
// additional SF CLI cost. Querying EntityDefinition checks metadata existence — not data
// access — so the deployment user's object-level permissions do not affect the result.
async function checkObjectsExistInOrg(
  log: (msg: string) => void,
  targetOrg: string,
  objectNames: string[]
): Promise<Set<string>> {
  if (!objectExistenceCache.has(targetOrg)) objectExistenceCache.set(targetOrg, new Map());
  const orgCache = objectExistenceCache.get(targetOrg)!;

  // Standard objects always exist — add them to result without querying.
  const result = new Set<string>(objectNames.filter((n) => !n.endsWith('__c')));
  const customOnes = objectNames.filter((n) => n.endsWith('__c'));

  const toQuery = customOnes.filter((n) => !orgCache.has(n));
  if (toQuery.length > 0) {
    log(`   [Obj Check] Querying org for ${toQuery.length} custom object(s): ${toQuery.join(', ')}`);
    const inClause = toQuery.map((n) => `'${n}'`).join(', ');
    type ObjRec = { QualifiedApiName: string };
    const records = await toolingQuery<ObjRec>(
      targetOrg,
      `"SELECT QualifiedApiName FROM EntityDefinition WHERE QualifiedApiName IN (${inClause})"`
    );
    const found = new Set(records.map((r) => r.QualifiedApiName));
    for (const obj of toQuery) orgCache.set(obj, found.has(obj));
    log(`   [Obj Check] ${found.size}/${toQuery.length} custom object(s) exist in org`);
  }

  for (const obj of customOnes) {
    if (orgCache.get(obj)) result.add(obj);
  }
  return result;
}

// Loads ALL installed package namespace prefixes for an org in one query and caches the Set.
// NamespacePrefix cannot be used in a WHERE clause on InstalledSubscriberPackage (Tooling API
// restriction), so we pull the full list once and do client-side lookups for every namespace.
async function loadInstalledNamespaces(targetOrg: string): Promise<Set<string>> {
  if (installedNsCache.has(targetOrg)) return installedNsCache.get(targetOrg)!;

  type PkgRec = { SubscriberPackage: { NamespacePrefix: string } };
  const records = await toolingQuery<PkgRec>(
    targetOrg,
    '"SELECT SubscriberPackage.NamespacePrefix FROM InstalledSubscriberPackage"'
  );
  const set = new Set(
    records.map((r) => r.SubscriberPackage?.NamespacePrefix).filter((ns): ns is string => !!ns && ns !== 'null')
  );
  installedNsCache.set(targetOrg, set);
  return set;
}

async function checkNamespaceInstalled(
  log: (msg: string) => void,
  targetOrg: string,
  namespace: string
): Promise<boolean> {
  const key = `${targetOrg}:${namespace}`;
  if (namespaceCache.has(key)) return namespaceCache.get(key)!;

  const installedNs = await loadInstalledNamespaces(targetOrg);
  const installed = installedNs.has(namespace);
  namespaceCache.set(key, installed);
  log(`   [NS Check] ${namespace}: ${installed ? 'installed' : 'NOT installed — bulk-removing all refs'}`);
  return installed;
}

function removeBlocksWithNamespace(xml: string, blockTag: string, keyTag: string, namespace: string): string {
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
      new RegExp(
        `[ \\t]*<fieldPermissions>${inner}<field>[^<]*\\.${ns}__[^<]*</field>${inner}</fieldPermissions>[ \\t]*\\r?\\n?`,
        'g'
      ),
      ''
    );
    // fieldPermissions: field = "Namespace__Object__c.AnyField"
    xml = xml.replace(
      new RegExp(
        `[ \\t]*<fieldPermissions>${inner}<field>${ns}__[^<]*</field>${inner}</fieldPermissions>[ \\t]*\\r?\\n?`,
        'g'
      ),
      ''
    );
  }

  xml = removeBlocksWithNamespace(xml, 'objectPermissions', 'object', namespace);
  xml = removeBlocksWithNamespace(xml, 'classAccesses', 'apexClass', namespace);
  xml = removeBlocksWithNamespace(xml, 'pageAccesses', 'apexPage', namespace);
  xml = removeBlocksWithNamespace(xml, 'tabSettings', 'tab', namespace);
  xml = removeBlocksWithNamespace(xml, 'tabVisibilities', 'tab', namespace);
  xml = removeBlocksWithNamespace(xml, 'flowAccesses', 'flow', namespace);
  xml = removeBlocksWithNamespace(xml, 'applicationVisibilities', 'application', namespace);
  xml = removeBlocksWithNamespace(xml, 'profileActionOverrides', 'content', namespace);
  xml = removeBlocksWithNamespace(xml, 'customMetadataTypeAccesses', 'name', namespace);
  xml = removeBlocksWithNamespace(xml, 'customPermissions', 'name', namespace);

  return { updated: xml, removed: xml !== xmlContent };
}

// ===============================================================
// DEPLOY INFRASTRUCTURE
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
  items: Array<{ metadataType: string; itemName: string }>,
  targetOrg: string,
  outputFile: string,
  timeoutMins: number,
  maxRetries: number,
  verbose: boolean
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
    // eslint-disable-next-line no-await-in-loop
    await waitForQueueToClear(log, targetOrg, MAX_QUEUE_WAIT_MINS);

    // eslint-disable-next-line no-await-in-loop
    const procResult = await runDeployProcess(log, items, targetOrg, outputFile, timeoutMins, verbose);

    if (procResult === 'timeout') {
      log(
        `   Deploy timed out after ${timeoutMins} min(s) (attempt ${attempt}/${MAX_TOTAL_ATTEMPTS}). Retrying after backoff...`
      );
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

  log(`   Giving up after ${hardAttempt} total attempts.`);
  return null;
}

function runDeployProcess(
  log: (msg: string) => void,
  items: Array<{ metadataType: string; itemName: string }>,
  targetOrg: string,
  outputFile: string,
  timeoutMins: number,
  verbose: boolean
): Promise<'ok' | 'timeout'> {
  return new Promise((resolve) => {
    // Build one "-m Type:Name" pair per item.
    // Each value is double-quoted so cmd.exe (shell:true) strips the outer quotes
    // and passes "Type:Name With Spaces" as a single argument.
    const metaArgs: string[] = [];
    for (const item of items) {
      metaArgs.push('-m', `"${item.metadataType}:${item.itemName}"`);
    }
    const args = [
      'project',
      'deploy',
      'start',
      ...metaArgs,
      '--target-org',
      targetOrg,
      '--json',
      '--dry-run',
      '--wait',
      String(timeoutMins),
    ];

    // Log the exact shell command for debugging (verbose only)
    if (verbose) {
      const dbgCmd = `sf ${args.join(' ')}`;
      fs.appendFileSync(outputFile + '.cmd.txt', dbgCmd + '\n', 'utf8');
    }

    const proc = spawn('sf', args, { shell: true });
    const outputStream = fs.createWriteStream(outputFile, { encoding: 'utf8' });

    proc.stdout.pipe(outputStream);
    proc.stderr.pipe(outputStream);

    const deployStart = Date.now();
    const progressTimer = setInterval(() => {
      const elapsed = Date.now() - deployStart;
      const mins = Math.floor(elapsed / 60_000);
      const secs = Math.floor((elapsed % 60_000) / 1000);
      log(`   Still deploying... ${mins}m ${secs}s elapsed (timeout: ${timeoutMins}m)`);
    }, 30_000);

    const timer = setTimeout(() => {
      clearInterval(progressTimer);
      proc.kill();
      resolve('timeout');
    }, timeoutMins * 60 * 1000);

    proc.on('close', () => {
      clearInterval(progressTimer);
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

// Queries the Tooling API DeployRequest object to count active
// (Pending / InProgress) deployments in the org — including any
// Copado promotions that are currently in flight.
// We wait until the count reaches 0 before submitting our own
// CheckOnly (dry-run) validation, which prevents our job from
// sitting in the queue behind a long-running Copado deployment.
// It also handles our own stale dry-runs: when --wait 10 expires
// the job is still InProgress in the org; by waiting for it to
// finish before retrying we avoid flooding the queue.
function queryDeployQueueCount(targetOrg: string): Promise<number> {
  return new Promise((resolve) => {
    const query = "\"SELECT Id FROM DeployRequest WHERE Status IN ('Pending','InProgress')\"";
    const args = ['data', 'query', '--query', query, '--use-tooling-api', '--target-org', targetOrg, '--json'];
    const proc = spawn('sf', args, { shell: true });
    const chunks: string[] = [];
    proc.stdout.on('data', (d: Buffer) => chunks.push(d.toString()));
    proc.stderr.on('data', (d: Buffer) => chunks.push(d.toString()));
    const timer = setTimeout(() => {
      proc.kill();
      resolve(0);
    }, 30_000);
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

async function waitForQueueToClear(log: (msg: string) => void, targetOrg: string, maxWaitMins = 30): Promise<void> {
  const POLL_MS = 30_000;
  const deadline = Date.now() + maxWaitMins * 60_000;
  const queueStart = Date.now();

  // eslint-disable-next-line no-await-in-loop
  let count = await queryDeployQueueCount(targetOrg);
  if (count === 0) return;

  log(`   [Queue] ${count} active deployment(s) in org (Copado or previous dry-run). Waiting for queue to clear...`);
  while (count > 0 && Date.now() < deadline) {
    // eslint-disable-next-line no-await-in-loop
    await sleep(POLL_MS);
    // eslint-disable-next-line no-await-in-loop
    count = await queryDeployQueueCount(targetOrg);
    if (count > 0) {
      const elapsed = Date.now() - queueStart;
      const mins = Math.floor(elapsed / 60_000);
      const secs = Math.floor((elapsed % 60_000) / 1000);
      log(`   [Queue] Still ${count} active deployment(s). Waiting 30s... (${mins}m ${secs}s elapsed)`);
    }
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

// Ref types that are false positives for ALL Profile deployments (ADD and FULL).
// ADD profiles: Copado only enforces flowAccesses, userPermissions, and profileActionOverrides.
// FULL profiles: Copado only enforces standard/big object objectPermissions — everything else
//   (including flows, userPermissions, profileActionOverrides) is masked before dry-run.
// Used in both the sweep guard and the processRegisteredFailure safety net.
// 'recordTypeOverride' is excluded from the sweep because each profile will hit the error
// independently and be fixed by applyRecordTypePreCheck (no sweep needed for ADD profiles).
const PROFILE_SKIPPED_REF_TYPES = new Set<RefType>([
  'app',
  'class',
  'page',
  'field',
  'object',
  'layout',
  'tab',
  'objectFlag',
  'recordTypeOverride',
  'customMetadataType', // Copado TRIM strips customMetadataTypeAccesses from profiles
  'customPermission', // confirmed: Copado TRIM strips customPermissions from profiles
  'recordTypeVisibility', // Copado TRIM strips recordTypeVisibilities from profiles
]);

// ===============================================================
// METADATA HANDLER REGISTRY
// repoPathFn removed — whitelist is JSON-only now.
// ===============================================================

type MetadataHandler = {
  patterns: RegExp[]; // multiple patterns — SF can phrase the same error differently
  label: string;
  refType: RefType;
  whitelistKey: keyof WhitelistMap; // required — all registered types are standalone components with a whitelist
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
    label: 'app',
    refType: 'app',
    whitelistKey: 'apps',
    removeFn: removeApplicationVisibilityFromXml,
    displayTag: '[App]',
  },
  {
    patterns: [
      /no ApexClass named (.+?) found/i,
      /Entity of type 'ApexClass' named '(.+?)' cannot be found/i,
      /In field: apexClass - no ApexClass named (.+?) found/i,
    ],
    label: 'class',
    refType: 'class',
    whitelistKey: 'classes',
    removeFn: removeClassAccessFromXml,
    displayTag: '[Class]',
  },
  {
    patterns: [
      /no ApexPage named (.+?) found/i,
      /Entity of type 'ApexPage' named '(.+?)' cannot be found/i,
      /In field: apexPage - no ApexPage named (.+?) found/i,
    ],
    label: 'page',
    refType: 'page',
    whitelistKey: 'pages',
    removeFn: removePageAccessFromXml,
    displayTag: '[Page]',
  },
  {
    patterns: [
      /no CustomTab named (.+?) found/i,
      /Entity of type 'CustomTab' named '(.+?)' cannot be found/i,
      /In field: tab - no CustomTab named (.+?) found/i,
    ],
    label: 'tab',
    refType: 'tab',
    whitelistKey: 'tabs',
    removeFn: removeTabSettingFromXml,
    displayTag: '[Tab]',
  },
  {
    // Must be before the 'object' handler — both match "no CustomObject named X found"
    // but __mdt types need <customMetadataTypeAccesses> removed, not <objectPermissions>.
    patterns: [/In field: customMetadataType - no CustomObject named (.+?) found/i],
    label: 'customMetadataType',
    refType: 'customMetadataType',
    whitelistKey: 'customMetadataTypes',
    removeFn: removeCustomMetadataTypeAccessFromXml,
    displayTag: '[CustomMetadata]',
  },
  {
    patterns: [
      /no CustomObject named (.+?) found/i,
      /Entity of type 'CustomObject' named '(.+?)' cannot be found/i,
      /In field: object - no CustomObject named (.+?) found/i,
    ],
    label: 'object',
    refType: 'object',
    whitelistKey: 'objects',
    removeFn: removeObjectPermissionFromXml,
    displayTag: '[Object]',
  },
  {
    patterns: [/In field: customPermission - no CustomPermission named (.+?) found/i],
    label: 'customPermission',
    refType: 'customPermission',
    whitelistKey: 'customPermissions',
    removeFn: removeCustomPermissionFromXml,
    displayTag: '[CustomPermission]',
  },
  {
    // PermissionSet recordTypeVisibilities — distinct from profileActionOverrides RecordType pre-check.
    // Error: "In field: recordType - no RecordType named Object.DevName found"
    patterns: [/In field: recordType - no RecordType named (.+?) found/i],
    label: 'recordTypeVisibility',
    refType: 'recordTypeVisibility',
    whitelistKey: 'recordTypeVisibilities',
    removeFn: removeRecordTypeVisibilityFromXml,
    displayTag: '[RecordTypeVisibility]',
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
    label: 'flow',
    refType: 'flow',
    whitelistKey: 'flows',
    removeFn: removeFlowAccessFromXml,
    displayTag: '[Flow]',
  },
  {
    patterns: [
      /no Layout named (.+?) found/i,
      /Entity of type 'Layout' named '(.+?)' cannot be found/i,
      /In field: layout - no Layout named (.+?) found/i,
    ],
    label: 'layout',
    refType: 'layout',
    whitelistKey: 'layouts',
    removeFn: removeLayoutAssignmentFromXml,
    displayTag: '[Layout]',
  },
  {
    patterns: [
      /The (.+?) Lightning page doesn't exist or isn't valid/i,
      /no FlexiPage named (.+?) found/i,
      /Entity of type 'FlexiPage' named '(.+?)' cannot be found/i,
    ],
    label: 'flexipage',
    refType: 'flexipage',
    whitelistKey: 'flexipages',
    removeFn: removeProfileActionOverrideFromXml,
    displayTag: '[ProfileActionOverride] FlexiPage:',
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
    if (m) {
      missingField = m[1].trim();
      break;
    }
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
      removedRef: { type: 'field', name: missingField, label: missingField, deployError: errorMessage },
    };
  }
  log(`   Field not found in XML: ${missingField} — already removed or not present.`);
  return { handled: true, xmlContent };
}

function processUserPermissionFailure(
  log: (msg: string) => void,
  errorMessage: string,
  xmlContent: string
): FailureResult {
  const m = /Unknown user permission:\s*(.+)/i.exec(errorMessage);
  if (!m) return { handled: false, xmlContent };

  const permName = m[1].trim();
  log(`   Unknown user permission: ${permName}`);
  const { updated, removed } = removeUserPermissionFromXml(xmlContent, permName);
  if (removed) {
    log(`   Removed userPermissions block for: ${permName}`);
    return {
      handled: true,
      xmlContent: updated,
      removedRef: {
        type: 'userPermission',
        name: permName,
        label: `[UserPerm] ${permName}`,
        deployError: errorMessage,
      },
    };
  }
  log(`   userPermissions block not found in XML: ${permName} — already removed or not present.`);
  return { handled: true, xmlContent };
}

// Maps the human-readable permission label from the error message to the XML element name.
const USER_LICENSE_FLAG_MAP: Record<string, string> = {
  'View All': 'viewAllRecords',
  'Modify All': 'modifyAllRecords',
  Read: 'allowRead',
  Create: 'allowCreate',
  Edit: 'allowEdit',
  Delete: 'allowDelete',
};

function processUserLicenseFailure(
  log: (msg: string) => void,
  errorMessage: string,
  xmlContent: string
): FailureResult {
  // Only handle object-permission flag errors — e.g.
  // "The user license doesn't allow the permission: View All CodeBuilder__Alert__e"
  const m =
    /The user license doesn't allow the permission:\s*(View All|Modify All|Read|Create|Edit|Delete)\s+(.+)/i.exec(
      errorMessage
    );
  if (!m) return { handled: false, xmlContent };

  const permLabel = m[1].trim();
  const objectName = m[2].trim();
  const flagElement = USER_LICENSE_FLAG_MAP[permLabel];

  if (!flagElement) {
    log(`   [UserLicense] Unrecognised permission label "${permLabel}" — ignoring`);
    return { handled: true, xmlContent };
  }

  log(`   [UserLicense] Removing ${permLabel} flag for object: ${objectName}`);
  const { updated, removed } = removeObjectPermissionFlag(xmlContent, objectName, flagElement);
  if (removed) {
    log(`   Removed <${flagElement}>true from objectPermissions for: ${objectName}`);
    return {
      handled: true,
      xmlContent: updated,
      removedRef: {
        type: 'objectFlag',
        name: objectName,
        label: `[UserLicense] ${permLabel} ${objectName}`,
        meta: flagElement,
        deployError: errorMessage,
      },
    };
  }
  log(`   <${flagElement}>true not found in objectPermissions for: ${objectName} — already removed or not present.`);
  return { handled: true, xmlContent };
}

function processRegisteredFailure(
  log: (msg: string) => void,
  errorMessage: string,
  xmlContent: string,
  whitelist: WhitelistMap,
  skippedFields: string[],
  allSkippedFields: string[],
  metadataType: string
): FailureResult {
  for (const handler of METADATA_HANDLERS) {
    let name: string | null = null;
    for (const pattern of handler.patterns) {
      const m = pattern.exec(errorMessage);
      if (m) {
        name = m[1].trim();
        break;
      }
    }
    if (!name) continue;

    // Ignore standard_ app errors — Copado's pipeline YAML strips these before
    // deploying, so they never appear in real promotion runs. No removal needed.
    if (handler.refType === 'app' && name.startsWith('standard_')) {
      log(`   [StandardApp] Ignoring standard app (handled by Copado YAML): ${name}`);
      return { handled: true, xmlContent };
    }

    // Safety net: skip false-positive types for Profiles.
    // maskProfileFalsePositives strips these before dry-run so they should never
    // reach here, but guard anyway in case masking is incomplete.
    //
    // Exception: 'object' errors for standard (no __c/__mdt suffix) and big objects (__b)
    // are REAL errors that Copado does not filter. Feature-gated standard objects
    // (e.g. AccountContactRelation) and big objects can be absent from the target org
    // and must be removed — unless they are present in the promotion JSON as an ADD.
    if (metadataType === 'Profile' && PROFILE_SKIPPED_REF_TYPES.has(handler.refType)) {
      if (!isStandardOrBigObjectRef(handler.refType, name)) {
        log(`   [Profile] Skipping ${handler.label} (not enforced by Copado deployment): ${name}`);
        return { handled: true, xmlContent };
      }
      log(`   [Profile FULL] Standard/Big object error detected — checking ADD whitelist before removal: ${name}`);
    }

    // Explicit ADD-whitelist guard for standard and big object refs (FULL operation — Profile and
    // PermissionSet). If the object is present in the promotion JSON as an ADD operation it will
    // exist in the org after deployment, so the permission is valid — skip removal.
    if (isStandardOrBigObjectRef(handler.refType, name)) {
      if (whitelist.objects.includes(name)) {
        log(`   [FULL] Skipping [Object] ${name} — present in promotion as ADD, will exist in org after deploy`);
        skippedFields.push(`[Object] ${name}`);
        allSkippedFields.push(`[Object] ${name}`);
        return { handled: true, xmlContent };
      }
      log(`   [FULL] [Object] ${name} not in ADD whitelist — confirmed absent from org, proceeding with removal`);
    }

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
        removedRef: {
          type: handler.refType,
          name,
          label: handler.displayTag.endsWith(':') ? `${handler.displayTag}${name}` : `${handler.displayTag} ${name}`,
          deployError: errorMessage,
        },
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
  allSkippedFields: string[],
  metadataType: string,
  verbose: boolean
): { xmlContent: string; removedRefs: RemovedRef[]; skippedFields: string[]; unhandledErrors: string[] } {
  let updatedXml = xmlContent;
  const removedRefs: RemovedRef[] = [];
  const skippedFields: string[] = [];
  const unhandledErrors: string[] = [];
  const vlog: (msg: string) => void = verbose ? log : (): void => {};

  vlog(`   [DEBUG] Total failures this iteration: ${failures.length}`);
  failures.forEach((f, i) => vlog(`   [DEBUG] Failure ${i + 1}: ${f.problem ?? f.error ?? ''}`));

  for (const failure of failures) {
    const err = failure.problem ?? failure.error ?? '';

    // ── User license errors — fix known object-permission flags; ignore the rest ──
    if (/The user license doesn't allow the permission:/i.test(err)) {
      const ulResult = processUserLicenseFailure(log, err, updatedXml);
      if (ulResult.handled) {
        updatedXml = ulResult.xmlContent;
        if (ulResult.removedRef) removedRefs.push(ulResult.removedRef);
        continue;
      }
      log(`   [UserLicense] Ignoring (needs developer review): ${err}`);
      continue;
    }

    // ── Tab settings errors — validation-only, Copado real deploys ignore these ──
    if (/You can't edit tab settings for .+, as it's not a valid tab/i.test(err)) {
      log(`   [TabSettings] Ignoring validation-only error: ${err}`);
      continue;
    }

    // ── Permission dependency errors — validation-only, not enforced by Copado ──
    if (/Permission .+ depends on permission\(s\):/i.test(err)) {
      log(`   [PermDep] Ignoring validation-only permission dependency error: ${err}`);
      continue;
    }

    // ── profileActionOverrides RecordType error — handled by applyRecordTypePreCheck ──
    if (/The value you specified for RecordType is invalid/i.test(err)) {
      log('   [ProfileActionOverride] RecordType error — handled by pre-check');
      continue;
    }

    // ── profileActionOverrides pageOrSobjectType error — handled by applyObjectPagePreCheck ──
    if (/You must specify a page or object/i.test(err)) {
      log('   [ProfileActionOverride] page/object error — handled by pre-check');
      continue;
    }

    // ── CustomField ───────────────────────────────────────────────
    const fieldResult = processFieldFailure(log, err, updatedXml, whitelist, skippedFields, allSkippedFields);
    if (fieldResult.handled) {
      updatedXml = fieldResult.xmlContent;
      if (fieldResult.removedRef) removedRefs.push(fieldResult.removedRef);
      continue;
    }

    // ── Unknown user permission (not a standalone component — no whitelist) ──
    const upResult = processUserPermissionFailure(log, err, updatedXml);
    if (upResult.handled) {
      updatedXml = upResult.xmlContent;
      if (upResult.removedRef) removedRefs.push(upResult.removedRef);
      continue;
    }

    // ── Registered handlers (app / class / page / tab / object / flow / layout) ──
    const regResult = processRegisteredFailure(
      log,
      err,
      updatedXml,
      whitelist,
      skippedFields,
      allSkippedFields,
      metadataType
    );
    if (regResult.handled) {
      updatedXml = regResult.xmlContent;
      if (regResult.removedRef) removedRefs.push(regResult.removedRef);
      continue;
    }

    log(`   Skipping unhandled error: ${err}`);
    unhandledErrors.push(err);
  }

  return { xmlContent: updatedXml, removedRefs, skippedFields, unhandledErrors };
}

// ===============================================================
// CROSS-FILE SWEEP
//
// After a ref is confirmed missing and removed from the current file,
// remove it from every other permset/profile file in the JSON batch.
// One combined git commit covers all affected files.
// ===============================================================

// Returns true for standard objects (no __c/__mdt suffix) and big objects (__b).
// These are NOT filtered by Copado TRIM and cause real profile deploy failures.
function isStandardOrBigObjectRef(refType: RefType, name: string): boolean {
  return refType === 'object' && !name.endsWith('__c') && !name.endsWith('__mdt');
}

// Returns true when a ref should be SKIPPED (not swept) for a Profile file.
// Copado TRIM handles most ref types for profiles, EXCEPT standard/big object refs.
// Extracted to keep sweepOtherFiles and repoWideSweep under the complexity limit.
function isProfileSweepSkip(isProfile: boolean, refType: RefType, name: string): boolean {
  return isProfile && PROFILE_SKIPPED_REF_TYPES.has(refType) && !isStandardOrBigObjectRef(refType, name);
}

function sweepOtherFiles(
  log: (msg: string) => void,
  refs: RemovedRef[],
  skipPaths: Set<string>,
  allFilePaths: string[],
  repoPath: string,
  dryRun: boolean
): void {
  if (refs.length === 0) return;

  log('\n   [Sweep] Removing same missing refs from all other files in batch...');
  const modifiedFiles: string[] = [];

  for (const filePath of allFilePaths) {
    if (skipPaths.has(filePath) || !fs.existsSync(filePath)) continue;

    let xml = fs.readFileSync(filePath, 'utf8');
    let fileModified = false;

    const isProfile = filePath.endsWith('.profile-meta.xml');
    for (const ref of refs) {
      // Don't sweep refs into Profile files that Copado real deployments ignore.
      // Exception: standard/big object refs (no __c/__mdt suffix) are real errors — sweep them too.
      if (isProfileSweepSkip(isProfile, ref.type, ref.name)) continue;
      let result: { updated: string; removed: boolean };
      if (ref.type === 'namespace') {
        result = bulkRemoveNamespaceRefs(xml, ref.name);
      } else if (ref.type === 'field') {
        result = removeFieldPermissionsFromXml(xml, ref.name);
      } else if (ref.type === 'userPermission') {
        result = removeUserPermissionFromXml(xml, ref.name);
      } else if (ref.type === 'objectFlag') {
        if (!ref.meta) continue;
        result = removeObjectPermissionFlag(xml, ref.name, ref.meta);
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
  if (dryRun) {
    log(`   [Sweep] Dry run — skipped commit for ${modifiedFiles.length} file(s).`);
    return;
  }
  try {
    for (const f of modifiedFiles) execSync(`git add "${f}"`, { cwd: repoPath });
    execSync(`git commit -m "Cross-file sweep: remove [${refLabels}] from ${modifiedFiles.length} other file(s)"`, {
      cwd: repoPath,
    });
    log(`   [Sweep] Committed cleanup across ${modifiedFiles.length} file(s).`);
  } catch {
    log('   [Sweep] Commit failed or nothing new to stage.');
  }
}

function collectBatchRefs(batchItems: BatchItem[]): RemovedRef[] {
  const seen = new Set<string>();
  const refs: RemovedRef[] = [];
  for (const item of batchItems) {
    for (const ref of item.allRemovedRefs) {
      if (!seen.has(ref.label)) {
        seen.add(ref.label);
        refs.push(ref);
      }
    }
  }
  return refs;
}

// ===============================================================
// REPO-WIDE SWEEP
// After all JSON batch items are fixed, sweep every permset/profile
// in the entire repo (outside the batch) and make ONE commit.
// ===============================================================

function repoWideSweep(
  log: (msg: string) => void,
  allRemovedRefs: RemovedRef[],
  batchFilePaths: Set<string>,
  repoPath: string,
  dryRun: boolean
): void {
  if (allRemovedRefs.length === 0) return;

  // Collect ALL permset/profile/mutingpermset files in the repo.
  const psDir = path.join(repoPath, 'force-app', 'main', 'default', 'permissionsets');
  const mpsDir = path.join(repoPath, 'force-app', 'main', 'default', 'mutingpermissionsets');
  const profileDir = path.join(repoPath, 'force-app', 'main', 'default', 'profiles');

  const collectFiles = (dir: string, ext: string): string[] => {
    if (!fs.existsSync(dir)) return [];
    return fs
      .readdirSync(dir)
      .filter((f) => f.endsWith(ext))
      .map((f) => path.join(dir, f));
  };

  const repoFiles = [
    ...collectFiles(psDir, '.permissionset-meta.xml'),
    ...collectFiles(mpsDir, '.mutingpermissionset-meta.xml'),
    ...collectFiles(profileDir, '.profile-meta.xml'),
  ].filter((f) => !batchFilePaths.has(f)); // exclude files already in the batch

  if (repoFiles.length === 0) {
    log('\n   [Repo Sweep] No files outside the batch to sweep.');
    return;
  }

  log(`\n--- Repo-Wide Sweep (${repoFiles.length} file(s) outside batch) ---`);
  const modifiedFiles: string[] = [];
  const removedRefLabels = new Set<string>();

  for (const filePath of repoFiles) {
    if (!fs.existsSync(filePath)) continue;
    let xml = fs.readFileSync(filePath, 'utf8');
    let fileModified = false;
    const isProfile = filePath.endsWith('.profile-meta.xml');

    for (const ref of allRemovedRefs) {
      // Exception: standard/big object refs are real errors — sweep them into profiles too.
      if (isProfileSweepSkip(isProfile, ref.type, ref.name)) continue;
      let result: { updated: string; removed: boolean };
      if (ref.type === 'namespace') {
        result = bulkRemoveNamespaceRefs(xml, ref.name);
      } else if (ref.type === 'field') {
        result = removeFieldPermissionsFromXml(xml, ref.name);
      } else if (ref.type === 'userPermission') {
        result = removeUserPermissionFromXml(xml, ref.name);
      } else if (ref.type === 'objectFlag') {
        if (!ref.meta) continue;
        result = removeObjectPermissionFlag(xml, ref.name, ref.meta);
      } else {
        const handler = METADATA_HANDLERS.find((h) => h.refType === ref.type);
        if (!handler) continue;
        result = handler.removeFn(xml, ref.name);
      }
      if (result.removed) {
        xml = result.updated;
        fileModified = true;
        removedRefLabels.add(ref.label);
        log(`   [Repo Sweep] Removed ${ref.label} from ${path.basename(filePath)}`);
      }
    }

    if (fileModified) {
      saveXmlClean(xml, filePath, getRootNodeName(xml));
      modifiedFiles.push(filePath);
    }
  }

  if (modifiedFiles.length === 0) {
    log('   [Repo Sweep] No outside files contained these missing references.');
    return;
  }

  log(`   [Repo Sweep] Cleaned ${modifiedFiles.length} file(s) outside the batch.`);
  if (dryRun) {
    log('   [Repo Sweep] Dry run — skipped commit.');
    return;
  }
  try {
    for (const f of modifiedFiles) execSync(`git add "${f}"`, { cwd: repoPath });
    execSync(
      `git commit -m "Repo-wide sweep: remove ${removedRefLabels.size} missing ref(s) from ${modifiedFiles.length} file(s) outside promotion batch"`,
      { cwd: repoPath }
    );
    log(`   [Repo Sweep] Committed repo-wide cleanup across ${modifiedFiles.length} file(s).`);
  } catch {
    log('   [Repo Sweep] Commit failed or nothing new to stage.');
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
    const ns = extractNamespaceFromError(failure.problem ?? failure.error ?? '');
    if (!ns || checked.has(ns)) continue;
    checked.add(ns);

    const hasWhitelisted = Object.values(whitelist)
      .flat()
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
        refs.push({
          type: 'namespace',
          name: ns,
          label: `[NS:${ns}] bulk-removed`,
          deployError: failure.problem ?? failure.error ?? '',
        });
        log(`   [NS Bulk] Removed ALL ${ns}__ refs from ${itemName} in one pass`);
      }
    } else {
      log(`   [NS Check] ${ns}: installed in org — skipping removal`);
    }
  }

  return { xml, refs };
}

// ===============================================================
// RECORD TYPE PRE-CHECK
// Salesforce reports "The value you specified for RecordType is invalid or doesn't
// match the object you specified." without naming the specific block. We query the org
// for all active RecordTypes and remove only profileActionOverrides blocks whose
// <recordType> is not in the result set. Each profile in the batch will hit this error
// independently and be fixed by this pre-check (no cross-file sweep needed since the
// RT cache is warm for all subsequent files after the first query).
// ===============================================================

async function applyRecordTypePreCheck(
  log: (msg: string) => void,
  failures: ComponentFailure[],
  xmlContent: string,
  targetOrg: string,
  itemName: string,
  whitelist: WhitelistMap
): Promise<{ xml: string; refs: RemovedRef[] }> {
  const RT_ERROR = /The value you specified for RecordType is invalid/i;
  const hasRTError = failures.some((f) => RT_ERROR.test(f.problem ?? f.error ?? ''));
  if (!hasRTError) return { xml: xmlContent, refs: [] };

  const existingRTs = await loadExistingRecordTypes(log, targetOrg);
  const { updated, removedRecordTypes } = removeProfileActionOverridesWithMissingRecordType(
    xmlContent,
    existingRTs,
    whitelist.recordTypes
  );
  if (removedRecordTypes.length === 0) {
    log(`   [RT Check] No invalid profileActionOverrides found in: ${itemName}`);
    return { xml: xmlContent, refs: [] };
  }

  log(
    `   [RT Check] Removed ${removedRecordTypes.length} profileActionOverrides block(s) with invalid RecordType from: ${itemName}`
  );
  return {
    xml: updated,
    refs: removedRecordTypes.map((rt) => ({
      type: 'recordTypeOverride' as RefType,
      name: rt,
      label: `[ProfileActionOverride] RecordType:${rt}`,
      deployError: 'The value you specified for RecordType is invalid',
    })),
  };
}

// Fires when "You must specify a page or object" error appears for a Profile.
// Collects all <pageOrSobjectType> values from profileActionOverrides blocks, queries the
// org for which custom objects actually exist, and removes only blocks whose object is
// missing AND not being deployed in this promotion. Standard objects are never removed.
async function applyObjectPagePreCheck(
  log: (msg: string) => void,
  failures: ComponentFailure[],
  xmlContent: string,
  targetOrg: string,
  itemName: string,
  whitelist: WhitelistMap
): Promise<{ xml: string; refs: RemovedRef[] }> {
  const OBJ_ERROR = /You must specify a page or object/i;
  if (!failures.some((f) => OBJ_ERROR.test(f.problem ?? f.error ?? ''))) return { xml: xmlContent, refs: [] };

  // Extract all pageOrSobjectType values present in the file.
  const inner = '(?:(?!<profileActionOverrides>)[\\s\\S])*?';
  const matches = [
    ...xmlContent.matchAll(
      new RegExp(
        `<profileActionOverrides>${inner}<pageOrSobjectType>([^<]*)</pageOrSobjectType>${inner}</profileActionOverrides>`,
        'g'
      )
    ),
  ];
  const objectNames = [...new Set(matches.map((m) => m[1].trim()).filter(Boolean))];
  if (objectNames.length === 0) return { xml: xmlContent, refs: [] };

  const existingObjects = await checkObjectsExistInOrg(log, targetOrg, objectNames);
  const { updated, removedObjects } = removeProfileActionOverridesWithMissingObject(
    xmlContent,
    existingObjects,
    whitelist.objects
  );
  if (removedObjects.length === 0) {
    log(`   [Obj Check] No invalid profileActionOverrides (pageOrSobjectType) found in: ${itemName}`);
    return { xml: xmlContent, refs: [] };
  }

  log(
    `   [Obj Check] Removed ${removedObjects.length} profileActionOverrides block(s) with missing object from: ${itemName}`
  );
  return {
    xml: updated,
    refs: removedObjects.map((o) => ({
      type: 'recordTypeOverride' as RefType,
      name: o,
      label: `[ProfileActionOverride] pageOrSobjectType:${o}`,
      deployError: 'You must specify a page or object',
    })),
  };
}

// ===============================================================
// WHITELIST MASKING
// Before each dry-run deploy, temporarily strip whitelisted entries
// from every active item's XML so Salesforce skips them and reports
// ALL remaining real missing refs — not just the first one it finds.
// Files are restored immediately after the deploy result arrives,
// before any error-processing or git operations.
// ===============================================================

function maskWhitelistedEntries(xmlContent: string, whitelist: WhitelistMap): string {
  let xml = xmlContent;
  for (const f of whitelist.fields) xml = removeFieldPermissionsFromXml(xml, f).updated;
  for (const o of whitelist.objects) xml = removeObjectPermissionFromXml(xml, o).updated;
  for (const c of whitelist.classes) xml = removeClassAccessFromXml(xml, c).updated;
  for (const p of whitelist.pages) xml = removePageAccessFromXml(xml, p).updated;
  for (const t of whitelist.tabs) xml = removeTabSettingFromXml(xml, t).updated;
  for (const fl of whitelist.flows) xml = removeFlowAccessFromXml(xml, fl).updated;
  for (const a of whitelist.apps) xml = removeApplicationVisibilityFromXml(xml, a).updated;
  for (const l of whitelist.layouts) xml = removeLayoutAssignmentFromXml(xml, l).updated;
  for (const fp of whitelist.flexipages) xml = removeProfileActionOverrideFromXml(xml, fp).updated;
  for (const rt of whitelist.recordTypes) xml = removeProfileActionOverrideByRecordTypeFromXml(xml, rt).updated;
  for (const o of whitelist.objects) xml = removeProfileActionOverrideByPageObjectFromXml(xml, o).updated;
  for (const cmt of whitelist.customMetadataTypes) xml = removeCustomMetadataTypeAccessFromXml(xml, cmt).updated;
  for (const cp of whitelist.customPermissions) xml = removeCustomPermissionFromXml(xml, cp).updated;
  for (const rtv of whitelist.recordTypeVisibilities) xml = removeRecordTypeVisibilityFromXml(xml, rtv).updated;
  return xml;
}

function maskStandardApps(xmlContent: string): string {
  // Temporarily strip all applicationVisibilities whose <application> starts with
  // "standard_" before each dry-run. Salesforce always errors on these but they are
  // removed by Copado's pipeline YAML before the real deploy. Without this masking
  // they block error discovery — Salesforce reports only one error per component per
  // iteration, so a standard_ app error would hide every subsequent real missing ref.
  const inner = '(?:(?!<applicationVisibilities>)[\\s\\S])*?';
  return xmlContent.replace(
    new RegExp(
      `[ \\t]*<applicationVisibilities>${inner}<application>[ \\t]*standard_[^<]*[ \\t]*</application>${inner}</applicationVisibilities>[ \\t]*\\r?\\n?`,
      'g'
    ),
    ''
  );
}

export function maskProfileFalsePositives(xmlContent: string, isFull = false): string {
  // Mask block types that are false positives for profiles before each dry-run.
  // The original XML is restored after each dry-run.
  //
  // ADD profiles: Copado real deployments only enforce flowAccesses, userPermissions,
  //   and profileActionOverrides. All other sections are stripped by Copado TRIM or
  //   cause unpredictable errors and cannot be usefully fixed. Mask everything else.
  //
  // FULL profiles: Copado real deployments ONLY enforce objectPermissions for standard
  //   and big objects (no __c/__mdt suffix) that are absent from the org. Everything
  //   else — including flowAccesses, userPermissions, and profileActionOverrides — is
  //   also a false positive for FULL profiles and must be masked. Only standard/big
  //   object objectPermissions remain exposed so the dry-run can detect and remove them.
  //
  // FULL vs ADD distinction for objectPermissions:
  //   ADD  — Copado TRIM strips ALL object permissions before the real deploy, so mask them all.
  //   FULL — Copado TRIM only strips __c/__mdt object permissions. Standard objects and big
  //          objects (__b) that don't exist in the org cause hard deployment errors that Copado
  //          does NOT clean. Keep those unmasked so the dry-run detects and removes them.
  let xml = xmlContent;
  xml = xml.replace(/[ \t]*<applicationVisibilities>[\s\S]*?<\/applicationVisibilities>[ \t]*\r?\n?/g, '');
  xml = xml.replace(/[ \t]*<categoryGroupVisibilities>[\s\S]*?<\/categoryGroupVisibilities>[ \t]*\r?\n?/g, '');
  xml = xml.replace(/[ \t]*<classAccesses>[\s\S]*?<\/classAccesses>[ \t]*\r?\n?/g, '');
  xml = xml.replace(/[ \t]*<pageAccesses>[\s\S]*?<\/pageAccesses>[ \t]*\r?\n?/g, '');
  xml = xml.replace(/[ \t]*<fieldPermissions>[\s\S]*?<\/fieldPermissions>[ \t]*\r?\n?/g, '');
  if (isFull) {
    // FULL: mask flowAccesses, userPermissions, and profileActionOverrides — all false positives.
    xml = xml.replace(/[ \t]*<flowAccesses>[\s\S]*?<\/flowAccesses>[ \t]*\r?\n?/g, '');
    xml = xml.replace(/[ \t]*<userPermissions>[\s\S]*?<\/userPermissions>[ \t]*\r?\n?/g, '');
    xml = xml.replace(/[ \t]*<profileActionOverrides>[\s\S]*?<\/profileActionOverrides>[ \t]*\r?\n?/g, '');
    // FULL: only mask __c and __mdt object permissions — keep standard and __b exposed.
    const inner = '(?:(?!<objectPermissions>)[\\s\\S])*?';
    xml = xml.replace(
      new RegExp(
        `[ \\t]*<objectPermissions>${inner}<object>[^<]*(?:__c|__mdt)[^<]*</object>${inner}</objectPermissions>[ \\t]*\\r?\\n?`,
        'g'
      ),
      ''
    );
  } else {
    // ADD: mask ALL object permissions (Copado TRIM handles all of them for ADD profiles).
    xml = xml.replace(/[ \t]*<objectPermissions>[\s\S]*?<\/objectPermissions>[ \t]*\r?\n?/g, '');
  }
  xml = xml.replace(/[ \t]*<recordTypeVisibilities>[\s\S]*?<\/recordTypeVisibilities>[ \t]*\r?\n?/g, '');
  xml = xml.replace(/[ \t]*<layoutAssignments>[\s\S]*?<\/layoutAssignments>[ \t]*\r?\n?/g, '');
  xml = xml.replace(/[ \t]*<tabVisibilities>[\s\S]*?<\/tabVisibilities>[ \t]*\r?\n?/g, '');
  xml = xml.replace(/[ \t]*<customMetadataTypeAccesses>[\s\S]*?<\/customMetadataTypeAccesses>[ \t]*\r?\n?/g, '');
  xml = xml.replace(/[ \t]*<customPermissions>[\s\S]*?<\/customPermissions>[ \t]*\r?\n?/g, ''); // confirmed: Copado TRIM strips these
  return xml;
}

export function maskPermSetFalsePositives(xmlContent: string, isFull = false): string {
  // customMetadataTypeAccesses is NOT masked — missing __mdt references cause real deployment
  // failures and are handled by the customMetadataType METADATA_HANDLER.
  //
  // FULL operation: Copado TRIM strips __c/__mdt object permissions from permission sets before
  // the real deploy, so mask those to avoid false positives consuming the one-error-per-component
  // slot. Standard and __b object permissions are NOT stripped by Copado and cause hard errors —
  // keep them exposed so the dry-run detects and removes them.
  // ADD operation: no masking needed (existing behaviour).
  if (!isFull) return xmlContent;

  const inner = '(?:(?!<objectPermissions>)[\\s\\S])*?';
  return xmlContent.replace(
    new RegExp(
      `[ \\t]*<objectPermissions>${inner}<object>[^<]*(?:__c|__mdt)[^<]*</object>${inner}</objectPermissions>[ \\t]*\\r?\\n?`,
      'g'
    ),
    ''
  );
}

// ===============================================================
// DEDUPLICATION
// Block types where duplication is unambiguous: each entry should
// appear at most once for a given key value. layoutAssignments and
// profileActionOverrides are intentionally excluded — both can have
// multiple valid blocks for the same object/page (different recordType
// or formFactor), so keying on a single field would be incorrect.
// ===============================================================

const DEDUP_BLOCKS: Array<{ blockTag: string; keyTag: string }> = [
  { blockTag: 'fieldPermissions', keyTag: 'field' },
  { blockTag: 'classAccesses', keyTag: 'apexClass' },
  { blockTag: 'pageAccesses', keyTag: 'apexPage' },
  { blockTag: 'tabVisibilities', keyTag: 'tab' },
  { blockTag: 'tabSettings', keyTag: 'tab' },
  { blockTag: 'objectPermissions', keyTag: 'object' },
  { blockTag: 'applicationVisibilities', keyTag: 'application' },
  { blockTag: 'flowAccesses', keyTag: 'flow' },
  { blockTag: 'userPermissions', keyTag: 'name' },
  { blockTag: 'recordTypeVisibilities', keyTag: 'recordType' },
  { blockTag: 'customMetadataTypeAccesses', keyTag: 'name' },
  { blockTag: 'customPermissions', keyTag: 'name' },
  { blockTag: 'categoryGroupVisibilities', keyTag: 'dataCategoryGroup' },
];

export function deduplicateXmlBlocks(xmlContent: string): { updated: string; removedCount: number } {
  let updated = xmlContent;
  let removedCount = 0;

  for (const { blockTag, keyTag } of DEDUP_BLOCKS) {
    const seen = new Set<string>();
    const escapedBlock = blockTag.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
    const innerPattern = `(?:(?!<${escapedBlock}>)[\\s\\S])*?`;
    const blockRegex = new RegExp(
      `[ \\t]*<${escapedBlock}>${innerPattern}<${keyTag}>([^<]*)</${keyTag}>${innerPattern}</${escapedBlock}>[ \\t]*\\r?\\n?`,
      'g'
    );
    updated = updated.replace(blockRegex, (match: string, keyValue: string) => {
      const key = keyValue.trim();
      if (seen.has(key)) {
        removedCount++;
        return '';
      }
      seen.add(key);
      return match;
    });
  }

  return { updated, removedCount };
}

function maskActiveItems(activeItems: BatchItem[], whitelist: WhitelistMap): Map<string, string> {
  const saved = new Map<string, string>();
  for (const item of activeItems) {
    if (!fs.existsSync(item.filePath)) continue;
    const orig = fs.readFileSync(item.filePath, 'utf8');
    let masked = maskWhitelistedEntries(orig, whitelist);
    masked = maskStandardApps(masked);
    if (item.filePath.endsWith('.profile-meta.xml')) {
      masked = maskProfileFalsePositives(masked, item.operation === 'FULL');
    }
    if (item.filePath.endsWith('.permissionset-meta.xml') || item.filePath.endsWith('.mutingpermissionset-meta.xml')) {
      masked = maskPermSetFalsePositives(masked, item.operation === 'FULL');
    }
    saved.set(item.filePath, orig);
    if (masked !== orig) fs.writeFileSync(item.filePath, masked, 'utf8');
  }
  return saved;
}

function restoreItems(saved: Map<string, string>): void {
  for (const [filePath, content] of saved) {
    fs.writeFileSync(filePath, content, 'utf8');
  }
}

// ===============================================================
// BATCH DEPLOY HELPERS
// Extracted to keep runBatchDeploy under the complexity limit.
// ===============================================================

function validateBatchItems(log: (msg: string) => void, items: BatchItem[]): void {
  for (const item of items) {
    if (!fs.existsSync(item.filePath)) {
      log(`File not found, skipping: ${item.filePath}`);
      item.status = 'File Not Found';
      item.done = true;
    }
  }
}

function routeFailuresToItems(failures: ComponentFailure[], activeItems: BatchItem[]): Map<string, ComponentFailure[]> {
  const itemByName = new Map<string, BatchItem>();
  const itemByFile = new Map<string, BatchItem>();
  for (const item of activeItems) {
    itemByName.set(item.itemName.toLowerCase(), item);
    itemByFile.set(path.basename(item.filePath).toLowerCase(), item);
  }
  const failuresByItem = new Map<string, ComponentFailure[]>();
  for (const item of activeItems) failuresByItem.set(item.itemName, []);
  for (const failure of failures) {
    let matched: BatchItem | undefined;
    if (failure.fullName) matched = itemByName.get(failure.fullName.toLowerCase());
    const fp = failure.fileName ?? failure.filePath;
    if (!matched && fp) matched = itemByFile.get(path.basename(fp).toLowerCase());
    if (matched) failuresByItem.get(matched.itemName)?.push(failure);
  }
  return failuresByItem;
}

function sweepPerItemRefs(
  log: (msg: string) => void,
  perItemRefs: Map<string, RemovedRef[]>,
  allFilePaths: string[],
  repoPath: string,
  dryRun: boolean
): void {
  for (const [sourceFilePath, refs] of perItemRefs) {
    sweepOtherFiles(log, refs, new Set([sourceFilePath]), allFilePaths, repoPath, dryRun);
  }
}

function markPassedItems(
  log: (msg: string) => void,
  activeItems: BatchItem[],
  failuresByItem: Map<string, ComponentFailure[]>,
  dryRun: boolean
): void {
  for (const item of activeItems) {
    if ((failuresByItem.get(item.itemName) ?? []).length === 0) {
      log(`   [${item.itemName}] No failures this iteration — passed.`);
      item.status = item.allRemovedFields.length > 0 ? (dryRun ? 'Fixed (Dry Run)' : 'Fixed & Committed') : 'Success';
      item.done = true;
    }
  }
}

type NsResult = { nsXml: string; nsRefs: RemovedRef[]; rootNode: string };

// Applies namespace pre-check to all active items, stages and commits them together.
// Returns a map of per-item NS results and the managed-refs commit hash (if any).
async function applyManagedRefsPass(
  log: (msg: string) => void,
  activeItems: BatchItem[],
  failuresByItem: Map<string, ComponentFailure[]>,
  whitelist: WhitelistMap,
  targetOrg: string,
  repoPath: string,
  vlog: (msg: string) => void,
  dryRun: boolean
): Promise<{ itemNsResults: Map<string, NsResult>; lastManagedRefsCommit: string | null }> {
  const PSG_LOCK = /permission set group is updating/i;
  const itemNsResults = new Map<string, NsResult>();
  let managedRefsStaged = false;

  for (const item of activeItems) {
    if (item.done) continue;
    const itemFailures = failuresByItem.get(item.itemName) ?? [];
    if (itemFailures.length === 0) continue;

    if (itemFailures.some((f) => PSG_LOCK.test(f.problem ?? f.error ?? ''))) {
      log(`   [${item.itemName}] Permission set group is updating — skipping, re-run after group update completes.`);
      item.status = 'Skipped - PSG Updating';
      item.done = true;
      continue;
    }

    log(`\n   [${item.itemName}] ${itemFailures.length} failure(s):`);
    itemFailures.forEach((f, i) => vlog(`   [DEBUG] Failure ${i + 1}: ${f.problem ?? f.error ?? ''}`));

    const xmlContent = fs.readFileSync(item.filePath, 'utf8');
    const rootNode = getRootNodeName(xmlContent);

    // eslint-disable-next-line no-await-in-loop
    const { xml: nsXml, refs: nsRefs } = await applyNamespacePreCheck(
      log,
      itemFailures,
      xmlContent,
      whitelist,
      targetOrg,
      item.itemName
    );

    itemNsResults.set(item.filePath, { nsXml, nsRefs, rootNode });

    if (nsRefs.length > 0 && !dryRun) {
      saveXmlClean(nsXml, item.filePath, rootNode);
      try {
        execSync(`git add "${item.filePath}"`, { cwd: repoPath });
        managedRefsStaged = true;
      } catch {
        log(`   git add failed for managed refs: ${item.itemName}`);
      }
    }
  }

  if (!dryRun && managedRefsStaged) {
    const staged = [...itemNsResults.values()].filter((v) => v.nsRefs.length > 0);
    const totalLabels = staged.flatMap((v) => v.nsRefs.map((r) => r.label));
    const labelSummary = totalLabels.slice(0, 8).join(', ') + (totalLabels.length > 8 ? '...' : '');
    try {
      execSync(
        `git commit -m "Remove managed package refs (${totalLabels.length} ref(s) in ${staged.length} file(s)): ${labelSummary}"`,
        { cwd: repoPath }
      );
      const lastCommit = execSync('git rev-parse HEAD', { cwd: repoPath }).toString().trim();
      log(`   Committed managed package ref removals: ${staged.length} file(s), ${totalLabels.length} ref(s)`);
      return { itemNsResults, lastManagedRefsCommit: lastCommit };
    } catch {
      log('   Managed refs commit failed or nothing staged');
    }
  }

  return { itemNsResults, lastManagedRefsCommit: null };
}

// Saves and commits (or dry-runs) the missing-ref XML for one item.
function commitItemMissingRefs(
  log: (msg: string) => void,
  item: BatchItem,
  updatedXml: string,
  missingRefs: RemovedRef[],
  rootNode: string,
  repoPath: string,
  dryRun: boolean
): void {
  if (dryRun) {
    saveXmlClean(updatedXml, item.filePath, rootNode);
    log(`   Dry run — skipped commit for: ${item.itemName}`);
    // eslint-disable-next-line no-param-reassign
    item.status = 'Fixed (Dry Run)';
  } else if (missingRefs.length > 0) {
    saveXmlClean(updatedXml, item.filePath, rootNode);
    try {
      execSync(`git add "${item.filePath}"`, { cwd: repoPath });
      execSync(
        `git commit -m "[${item.itemName}] Auto-remove missing refs: ${missingRefs.map((r) => r.label).join(', ')}"`,
        { cwd: repoPath }
      );
      log(`   Committed missing ref removals for: ${item.itemName}`);
    } catch {
      log(`   Nothing to commit or commit failed for: ${item.itemName}`);
    }
    // eslint-disable-next-line no-param-reassign
    item.status = 'Fixed & Committed';
  } else {
    // Only managed refs were removed — already committed in Pass 1.
    // eslint-disable-next-line no-param-reassign
    item.status = 'Fixed & Committed';
  }
}

// Marks all unfinished batch items as 'Partial / Manual Check Needed' and stops.
function stopBatchOnNoProgress(log: (msg: string) => void, batchItems: BatchItem[]): void {
  log('No progress this iteration. Stopping batch.');
  for (const item of batchItems.filter((i) => !i.done)) {
    item.status = 'Partial / Manual Check Needed';
    item.done = true;
  }
}

// Squashes all missing-ref commits (from squashBase to HEAD) into one clean commit.
function squashMissingRefCommits(
  log: (msg: string) => void,
  squashBase: string,
  summary: SummaryRecord[],
  repoPath: string
): void {
  try {
    const currentHead = execSync('git rev-parse HEAD', { cwd: repoPath }).toString().trim();
    if (currentHead === squashBase) {
      log('\nNo missing-ref commits to squash.');
      return;
    }
    const changedFiles = execSync(`git diff --name-only ${squashBase} HEAD`, { cwd: repoPath })
      .toString()
      .trim()
      .split('\n')
      .filter(Boolean);
    execSync(`git reset --soft ${squashBase}`, { cwd: repoPath });
    const missingRemoved = summary.flatMap((r) => (r.RemovedFields ? r.RemovedFields.split('; ') : []));
    const squashMsg = `Auto-fix: remove ${missingRemoved.length} missing ref(s) across ${changedFiles.length} file(s)`;
    execSync(`git commit -m "${squashMsg}"`, { cwd: repoPath });
    log(`\nSquashed missing-ref commits into one: "${squashMsg}"`);
  } catch (e) {
    log(`\nSquash failed — intermediate commits preserved. Error: ${String(e)}`);
  }
}

async function processItemsInIteration(
  log: (msg: string) => void,
  activeItems: BatchItem[],
  failuresByItem: Map<string, ComponentFailure[]>,
  whitelist: WhitelistMap,
  targetOrg: string,
  repoPath: string,
  verbose: boolean,
  dryRun: boolean
): Promise<{ perItemRefs: Map<string, RemovedRef[]>; anyProgress: boolean; lastManagedRefsCommit: string | null }> {
  const vlog: (msg: string) => void = verbose ? log : (): void => {};
  // Track refs per source file so the sweep skips only the source file, not all modified files.
  const perItemRefs = new Map<string, RemovedRef[]>();
  let anyProgress = false;

  // ── Pass 1: Namespace / managed-package pre-check ────────────────────────
  // Apply to ALL active items first and commit them ONCE before any missing-ref
  // commits — this gives a clean 4-commit git history.
  // eslint-disable-next-line no-await-in-loop
  const { itemNsResults, lastManagedRefsCommit } = await applyManagedRefsPass(
    log,
    activeItems,
    failuresByItem,
    whitelist,
    targetOrg,
    repoPath,
    vlog,
    dryRun
  );

  // ── Pass 2: Missing-ref removals (per item) ───────────────────────────────
  for (const item of activeItems) {
    if (item.done) continue;
    const itemFailures = failuresByItem.get(item.itemName) ?? [];
    if (itemFailures.length === 0) continue;

    const nsResult = itemNsResults.get(item.filePath);
    const baseXml = nsResult?.nsXml ?? fs.readFileSync(item.filePath, 'utf8');
    const nsRefs = nsResult?.nsRefs ?? [];
    const rootNode = nsResult?.rootNode ?? getRootNodeName(baseXml);

    for (const ref of nsRefs) {
      if (!item.allRemovedFields.some((r) => r.label === ref.label)) {
        item.allRemovedFields.push({ label: ref.label, error: ref.deployError ?? '' });
        item.allRemovedRefs.push(ref);
      }
    }

    // eslint-disable-next-line no-await-in-loop
    const { xml: rtXml, refs: rtRefs } = await applyRecordTypePreCheck(
      log,
      itemFailures,
      baseXml,
      targetOrg,
      item.itemName,
      whitelist
    );
    // eslint-disable-next-line no-await-in-loop
    const { xml: objXml, refs: objRefs } = await applyObjectPagePreCheck(
      log,
      itemFailures,
      rtXml,
      targetOrg,
      item.itemName,
      whitelist
    );
    const {
      xmlContent: updatedXml,
      removedRefs: perFailureRefs,
      skippedFields,
      unhandledErrors,
    } = processFailures(log, itemFailures, objXml, whitelist, item.allSkippedFields, item.metadataType, verbose);

    const missingRefs = [...rtRefs, ...objRefs, ...perFailureRefs];
    const removedRefs = [...nsRefs, ...missingRefs];

    for (const ref of missingRefs) {
      if (!item.allRemovedFields.some((r) => r.label === ref.label)) {
        item.allRemovedFields.push({ label: ref.label, error: ref.deployError ?? '' });
        item.allRemovedRefs.push(ref);
      }
    }
    for (const e of unhandledErrors) {
      if (!item.allUnhandledErrors.includes(e)) item.allUnhandledErrors.push(e);
    }

    if (removedRefs.length === 0) {
      item.status =
        skippedFields.length > 0 ? 'Whitelisted Items Only - Manual Deploy Needed' : 'Partial / Manual Check Needed';
      item.done = true;
      continue;
    }

    anyProgress = true;
    perItemRefs.set(item.filePath, removedRefs);
    commitItemMissingRefs(log, item, updatedXml, missingRefs, rootNode, repoPath, dryRun);
  }

  return { perItemRefs, anyProgress, lastManagedRefsCommit };
}

// ===============================================================
// BATCH DEPLOY LOOP
// Deploys all permsets + profiles together in a single SF call each
// iteration. Failures are routed to the right file via fullName /
// fileName in ComponentFailure. Items not present in failures have
// passed validation and are dropped from subsequent iterations.
// One queue-wait covers the entire batch instead of one per item.
// ===============================================================

async function runBatchDeploy(
  log: (msg: string) => void,
  batchItems: BatchItem[],
  targetOrg: string,
  repoPath: string,
  whitelist: WhitelistMap,
  allFilePaths: string[],
  maxIterations: number,
  maxTotalDeploys: number,
  totalDeploys: TotalDeploys,
  timeoutMins: number,
  maxRetries: number,
  verbose: boolean,
  dryRun: boolean
): Promise<{ summary: SummaryRecord[]; lastManagedRefsCommit: string | null }> {
  validateBatchItems(log, batchItems);

  const MAX_EMPTY_RETRIES = 5;
  let consecutiveEmptyRetries = 0;
  let iteration = 0;
  let overallLastManagedRefsCommit: string | null = null;
  const deployErrorsFile = path.join(repoPath, 'deploy_errors_batch.json');

  while (iteration < maxIterations) {
    const activeItems = batchItems.filter((i) => !i.done);
    if (activeItems.length === 0) break;

    iteration++;
    // eslint-disable-next-line no-param-reassign
    totalDeploys.value++;

    if (totalDeploys.value > maxTotalDeploys) {
      log(`Global deploy limit reached (${maxTotalDeploys}). Stopping.`);
      for (const item of activeItems) {
        item.status = 'Stopped - Global Limit Reached';
        item.done = true;
      }
      break;
    }

    const fixedSoFar = batchItems.filter((i) => i.done && i.allRemovedFields.length > 0).length;
    const totalItems = batchItems.length;
    log(
      `\n--- Batch Iteration ${iteration} | Active: ${activeItems.length} | Fixed: ${fixedSoFar}/${totalItems} | Total Deploys: ${totalDeploys.value} / ${maxTotalDeploys} ---`
    );
    log('Running batch dry-run deploy...');

    // Temporarily strip whitelisted entries so Salesforce skips them and reports
    // ALL real missing refs — not just the first one it encounters.
    // try/finally guarantees restore even if invokeDeployWithRetry throws unexpectedly —
    // without this, a mid-deploy exception leaves files in the masked state and every
    // subsequent iteration reads masked content as "original", silently deleting real blocks.
    const savedContents = maskActiveItems(activeItems, whitelist);
    let deployResult: DeployResult | null = null;
    try {
      // eslint-disable-next-line no-await-in-loop
      deployResult = await invokeDeployWithRetry(
        log,
        activeItems,
        targetOrg,
        deployErrorsFile,
        timeoutMins,
        maxRetries,
        verbose
      );
    } finally {
      restoreItems(savedContents);
    }

    if (!deployResult) {
      log('Batch deploy failed after all retry attempts.');
      for (const item of activeItems) {
        item.status = 'Deploy Failed - Exhausted Retries';
        item.done = true;
      }
      break;
    }
    if (!deployResult.result) {
      log(`SF CLI returned unrecognised response. Keys: ${Object.keys(deployResult).join(', ')}`);
      for (const item of activeItems) {
        item.status = 'Deploy Failed - Unrecognised Response';
        item.done = true;
      }
      break;
    }
    if (deployResult.result.success === true) {
      log('All remaining items passed validation!');
      for (const item of activeItems) {
        item.status = item.allRemovedFields.length > 0 ? 'Fixed & Committed' : 'Success';
        item.done = true;
      }
      break;
    }

    const failures = deployResult.result.details?.componentFailures;
    if (!failures || failures.length === 0) {
      consecutiveEmptyRetries++;
      log(
        `success=false but 0 component failures (retry ${consecutiveEmptyRetries}/${MAX_EMPTY_RETRIES}) — deploy may still be running.`
      );
      if (consecutiveEmptyRetries >= MAX_EMPTY_RETRIES) {
        for (const item of activeItems) {
          item.status = 'Partial / Manual Check Needed';
          item.done = true;
        }
        break;
      }
      // eslint-disable-next-line no-await-in-loop
      await sleep(5000);
      continue;
    }

    consecutiveEmptyRetries = 0;
    const failuresByItem = routeFailuresToItems(failures, activeItems);
    markPassedItems(log, activeItems, failuresByItem, dryRun);

    // eslint-disable-next-line no-await-in-loop
    const { perItemRefs, anyProgress, lastManagedRefsCommit } = await processItemsInIteration(
      log,
      activeItems,
      failuresByItem,
      whitelist,
      targetOrg,
      repoPath,
      verbose,
      dryRun
    );

    if (lastManagedRefsCommit) overallLastManagedRefsCommit = lastManagedRefsCommit;

    if (!anyProgress && batchItems.some((i) => !i.done)) {
      stopBatchOnNoProgress(log, batchItems);
      break;
    }

    // Sweep each item's removed refs to ALL other files except that item's own file.
    // This ensures e.g. Profile B gets swept even if it was also modified this iteration
    // for a different error — it would otherwise be missed if we skipped all modified files.
    sweepPerItemRefs(log, perItemRefs, allFilePaths, repoPath, dryRun);
  }

  if (fs.existsSync(deployErrorsFile)) fs.unlinkSync(deployErrorsFile);

  const summary: SummaryRecord[] = batchItems.map((item) => ({
    Type: item.metadataType,
    Name: item.itemName,
    Status: item.status,
    RemovedFields: item.allRemovedFields.map((r) => r.label).join('; '),
    RemovedErrors: item.allRemovedFields.map((r) => r.error).join('; '),
    SkippedFields: item.allSkippedFields.join('; '),
    UnhandledErrors: item.allUnhandledErrors.join('; '),
  }));
  return { summary, lastManagedRefsCommit: overallLastManagedRefsCommit };
}

// ===============================================================
// INPUT RESOLUTION
// Extracted to keep the run() method under the complexity limit.
// ===============================================================

// Returns the set of valid org aliases + usernames from sf org list.
// Returns empty set if the CLI call fails — callers treat empty as "skip validation".
type OrgEntry = { alias?: string; username?: string };

function getAuthenticatedOrgs(): Promise<{ valid: Set<string>; entries: OrgEntry[] }> {
  return new Promise((resolve) => {
    const proc = spawn('sf', ['org', 'list', '--json'], { shell: true });
    const chunks: string[] = [];
    proc.stdout.on('data', (d: Buffer) => chunks.push(d.toString()));
    proc.stderr.on('data', (d: Buffer) => chunks.push(d.toString()));
    const timer = setTimeout(() => {
      proc.kill();
      resolve({ valid: new Set(), entries: [] });
    }, 15_000);
    proc.on('close', () => {
      clearTimeout(timer);
      try {
        const raw = chunks.join('');
        const start = raw.indexOf('{');
        const json = JSON.parse(start >= 0 ? raw.substring(start) : raw) as {
          result?: Record<string, OrgEntry[]>;
        };
        const valid = new Set<string>();
        const entries: OrgEntry[] = [];
        const seenUsernames = new Set<string>();
        for (const orgs of Object.values(json?.result ?? {})) {
          if (!Array.isArray(orgs)) continue;
          for (const org of orgs) {
            const key = org.username ?? org.alias ?? '';
            if (!key || seenUsernames.has(key)) continue;
            seenUsernames.add(key);
            if (org.alias) valid.add(org.alias);
            if (org.username) valid.add(org.username);
            entries.push(org);
          }
        }
        resolve({ valid, entries });
      } catch {
        resolve({ valid: new Set(), entries: [] });
      }
    });
  });
}

// Parses a package.xml and returns PromotionItem[] in the same shape as promotion.json.
function parsePackageXml(xmlContent: string): PromotionItem[] {
  const items: PromotionItem[] = [];
  const typeBlocks = xmlContent.match(/<types>[\s\S]*?<\/types>/g) ?? [];
  for (const block of typeBlocks) {
    const nameMatch = block.match(/<name>(.*?)<\/name>/);
    if (!nameMatch) continue;
    const metaType = nameMatch[1].trim();
    const members = [...block.matchAll(/<members>(.*?)<\/members>/g)].map((m) => m[1].trim());
    for (const member of members) {
      items.push({ t: metaType, n: member });
    }
  }
  return items;
}

// Returns 'FULL' when the Copado JSON marks this item with operation "Full" (entire file
// committed), or 'ADD' for delta commits and package.xml entries (no "a" field).
function getItemOperation(promotionData: PromotionItem[], metadataType: string, name: string): 'ADD' | 'FULL' {
  const item = promotionData.find((i) => i.t === metadataType && i.n === name);
  return item?.a?.toLowerCase() === 'full' ? 'FULL' : 'ADD';
}

// Reads and parses either a Copado promotion.json or a package.xml into PromotionItem[].
function loadInputFile(log: (msg: string) => void, inputFilePath: string): PromotionItem[] {
  const isXml = path.extname(inputFilePath).toLowerCase() === '.xml';
  log(`Loading ${isXml ? 'package.xml' : 'promotion JSON'}...`);
  const fileContent = fs.readFileSync(inputFilePath, 'utf8');
  return isXml ? parsePackageXml(fileContent) : (JSON.parse(fileContent) as PromotionItem[]);
}

async function resolveInputs(
  log: (msg: string) => void,
  jsonPathFlag: string,
  targetOrgFlag: string
): Promise<{ inputFilePath: string; targetOrg: string }> {
  let inputFilePath = jsonPathFlag;
  while (!inputFilePath || !fs.existsSync(inputFilePath)) {
    if (inputFilePath) log('   File not found at that path. Please try again.\n');
    // eslint-disable-next-line no-await-in-loop
    inputFilePath = await prompt(
      'Enter full path to your Copado Promotion JSON or package.xml\n   (e.g. C:\\Users\\YourName\\Desktop\\promotion.json  OR  ...\\package.xml)\n> '
    );
    inputFilePath = inputFilePath.replace(/^"|"$/g, '').trim();
  }
  const ext = path.extname(inputFilePath).toLowerCase();
  log(`   ${ext === '.xml' ? 'package.xml' : 'JSON'} file found.\n`);

  // eslint-disable-next-line no-await-in-loop
  const { valid: validOrgs, entries: orgEntries } = await getAuthenticatedOrgs();
  const hasOrgList = validOrgs.size > 0;
  if (!hasOrgList) {
    log('   (Could not retrieve org list — skipping alias validation)\n');
  } else {
    log('   Authenticated orgs:');
    orgEntries.forEach((o) => {
      const alias = o.alias ? `${o.alias}` : '';
      const user = o.username ? `(${o.username})` : '';
      log(`      - ${alias} ${user}`.trimEnd());
    });
    log('');
  }

  let targetOrg = targetOrgFlag.trim();
  while (!targetOrg || (hasOrgList && !validOrgs.has(targetOrg))) {
    if (targetOrg) log(`   "${targetOrg}" is not a recognised org alias or username. Please try again.\n`);
    // eslint-disable-next-line no-await-in-loop
    targetOrg = (await prompt('Enter target org username or alias\n   (e.g. RBKQA or user@rubrik.com.qa)\n> ')).trim();
  }
  log(`\n   Target Org set to: ${targetOrg}\n`);
  return { inputFilePath, targetOrg };
}

// Prints the detailed whitelist breakdown to the log.
// Extracted to keep run() under the ESLint complexity limit — each `|| 'none'` counts as +1.
function logWhitelistDetails(log: (msg: string) => void, whitelist: WhitelistMap): void {
  log('\nWhitelisted items (will never be removed):');
  log('  Fields            : ' + (whitelist.fields.join(', ') || 'none'));
  log('  Apps              : ' + (whitelist.apps.join(', ') || 'none'));
  log('  Classes           : ' + (whitelist.classes.join(', ') || 'none'));
  log('  Pages             : ' + (whitelist.pages.join(', ') || 'none'));
  log('  Tabs              : ' + (whitelist.tabs.join(', ') || 'none'));
  log('  Objects           : ' + (whitelist.objects.join(', ') || 'none'));
  log('  Flows             : ' + (whitelist.flows.join(', ') || 'none'));
  log('  Layouts           : ' + (whitelist.layouts.join(', ') || 'none'));
  log('  FlexiPages        : ' + (whitelist.flexipages.join(', ') || 'none'));
  log('  RecordTypes       : ' + (whitelist.recordTypes.join(', ') || 'none'));
  log('  CMT Types         : ' + (whitelist.customMetadataTypes.join(', ') || 'none'));
  log('  CustomPermissions : ' + (whitelist.customPermissions.join(', ') || 'none'));
  log('  RecordTypeVis     : ' + (whitelist.recordTypeVisibilities.join(', ') || 'none'));
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
    verbose: Flags.boolean({
      char: 'v',
      summary: messages.getMessage('flags.verbose.summary'),
      default: false,
    }),
    'dry-run': Flags.boolean({
      char: 'd',
      summary: messages.getMessage('flags.dry-run.summary'),
      default: false,
    }),
  };

  public async run(): Promise<void> {
    const { flags } = await this.parse(DeployAndFix);
    const verbose = flags.verbose;
    const dryRun = flags['dry-run'];
    const log = (msg: string): void => {
      this.log(msg);
    };

    // ================= INTERACTIVE PROMPTS =================
    log('\n======================================================');
    log('  AUTOMATED PERMISSION SET & PROFILE DEPLOY & FIX');
    log('======================================================\n');
    if (dryRun) log('*** DRY RUN MODE — files will be modified but NO commits will be made ***\n');

    // eslint-disable-next-line no-await-in-loop
    const { inputFilePath, targetOrg } = await resolveInputs(log, flags['json-path'] ?? '', flags['target-org'] ?? '');

    // ================= LOAD & PARSE INPUT FILE =================
    const promotionData = loadInputFile(log, inputFilePath);

    const permSets = [...new Set(promotionData.filter((i) => i.t === 'PermissionSet').map((i) => i.n))].sort();
    const mutingPermSets = [
      ...new Set(promotionData.filter((i) => i.t === 'MutingPermissionSet').map((i) => i.n)),
    ].sort();
    const profiles = [...new Set(promotionData.filter((i) => i.t === 'Profile').map((i) => i.n))].sort();

    // Only ADD operations are whitelisted — Retrieve-only components are not being deployed
    // in this package, so if they cause errors they can be safely removed.
    // For package.xml (no "a" field), treat all entries as Add (preserve existing behavior).
    const isAdd = (i: PromotionItem): boolean => !i.a || i.a.toLowerCase() === 'add';
    const whitelist: WhitelistMap = {
      fields: [...new Set(promotionData.filter((i) => i.t === 'CustomField' && isAdd(i)).map((i) => i.n))].sort(),
      apps: [...new Set(promotionData.filter((i) => i.t === 'CustomApplication' && isAdd(i)).map((i) => i.n))].sort(),
      classes: [...new Set(promotionData.filter((i) => i.t === 'ApexClass' && isAdd(i)).map((i) => i.n))].sort(),
      pages: [...new Set(promotionData.filter((i) => i.t === 'ApexPage' && isAdd(i)).map((i) => i.n))].sort(),
      tabs: [...new Set(promotionData.filter((i) => i.t === 'CustomTab' && isAdd(i)).map((i) => i.n))].sort(),
      objects: [...new Set(promotionData.filter((i) => i.t === 'CustomObject' && isAdd(i)).map((i) => i.n))].sort(),
      flows: [...new Set(promotionData.filter((i) => i.t === 'Flow' && isAdd(i)).map((i) => i.n))].sort(),
      layouts: [...new Set(promotionData.filter((i) => i.t === 'Layout' && isAdd(i)).map((i) => i.n))].sort(),
      flexipages: [...new Set(promotionData.filter((i) => i.t === 'FlexiPage' && isAdd(i)).map((i) => i.n))].sort(),
      recordTypes: [...new Set(promotionData.filter((i) => i.t === 'RecordType' && isAdd(i)).map((i) => i.n))].sort(),
      // CustomMetadata type definitions appear as CustomObject with __mdt suffix.
      // Individual CMT records appear as CustomMetadata with "TypeName__mdt.RecordName" format.
      customMetadataTypes: [
        ...new Set([
          ...promotionData.filter((i) => i.t === 'CustomObject' && isAdd(i) && i.n.endsWith('__mdt')).map((i) => i.n),
          ...promotionData
            .filter((i) => i.t === 'CustomMetadata' && isAdd(i))
            .map((i) => {
              const dot = i.n.indexOf('.');
              return dot >= 0 ? i.n.substring(0, dot) : i.n;
            }),
        ]),
      ].sort(),
      customPermissions: [
        ...new Set(promotionData.filter((i) => i.t === 'CustomPermission' && isAdd(i)).map((i) => i.n)),
      ].sort(),
      recordTypeVisibilities: [
        ...new Set(promotionData.filter((i) => i.t === 'RecordType' && isAdd(i)).map((i) => i.n)),
      ].sort(),
    };

    // Build full file path list upfront — sweepOtherFiles needs this.
    const allFilePaths: string[] = [
      ...permSets.map((ps) => path.join(PS_BASE_PATH, `${ps}.permissionset-meta.xml`)),
      ...mutingPermSets.map((mps) => path.join(MUTING_PS_BASE_PATH, `${mps}.mutingpermissionset-meta.xml`)),
      ...profiles.map((p) => path.join(PROFILE_BASE_PATH, `${p}.profile-meta.xml`)),
    ];

    const totalWhitelisted = Object.values(whitelist).reduce((sum, arr) => sum + arr.length, 0);

    // ================= STARTUP SUMMARY =================
    log('\n======================================================');
    log('STARTING AUTOMATED DEPLOY & FIX LOOP');
    log('======================================================');
    log(`Target Org              : ${targetOrg}`);
    log(`Permission Sets         : ${permSets.length} found in JSON`);
    log(`Muting Permission Sets  : ${mutingPermSets.length} found in JSON`);
    log(`Profiles                : ${profiles.length} found in JSON`);
    log(`Whitelisted total       : ${totalWhitelisted} items across all types (will never be removed)`);
    log(`  - CustomFields        : ${whitelist.fields.length}`);
    log(`  - CustomApplications  : ${whitelist.apps.length}`);
    log(`  - ApexClasses         : ${whitelist.classes.length}`);
    log(`  - ApexPages           : ${whitelist.pages.length}`);
    log(`  - CustomTabs          : ${whitelist.tabs.length}`);
    log(`  - CustomObjects       : ${whitelist.objects.length}`);
    log(`  - Flows               : ${whitelist.flows.length}`);
    log(`  - Layouts             : ${whitelist.layouts.length}`);
    log(`  - FlexiPages          : ${whitelist.flexipages.length}`);
    log(`  - RecordTypes         : ${whitelist.recordTypes.length}`);
    log(`  - CustomMetadataTypes : ${whitelist.customMetadataTypes.length}`);
    log(`  - CustomPermissions   : ${whitelist.customPermissions.length}`);
    log(`  - RecordTypeVis       : ${whitelist.recordTypeVisibilities.length}`);
    log(`Max per item            : ${MAX_ITERATIONS} iterations`);
    log(`Global deploy cap       : ${MAX_TOTAL_DEPLOYS} total deploys`);
    log(`Deploy timeout          : ${DEPLOY_TIMEOUT_MINS} min(s) per attempt`);
    log(`Max retries             : ${MAX_RETRIES} per deploy call`);

    log('\nPermission Sets to process:');
    permSets.forEach((ps) => log(`   - ${ps} [${getItemOperation(promotionData, 'PermissionSet', ps)}]`));
    log('\nMuting Permission Sets to process:');
    if (mutingPermSets.length > 0) {
      mutingPermSets.forEach((mps) =>
        log(`   - ${mps} [${getItemOperation(promotionData, 'MutingPermissionSet', mps)}]`)
      );
    } else {
      log('   (none)');
    }
    log('\nProfiles to process:');
    profiles.forEach((p) => log(`   - ${p} [${getItemOperation(promotionData, 'Profile', p)}]`));

    logWhitelistDetails(log, whitelist);

    log('');
    // eslint-disable-next-line no-await-in-loop
    const confirm = await prompt("Press ENTER to start or type 'exit' to cancel\n> ");
    if (confirm.trim().toLowerCase() === 'exit') {
      log('\nScript cancelled by user.');
      return;
    }
    log('\nStarting script...');
    log('\n======================================================');
    const startTime = Date.now();

    // Pre-load all installed package namespaces from the org once upfront.
    // This avoids an extra SF CLI call on the first namespace error and ensures
    // the cache is warm before any item processing begins.
    // eslint-disable-next-line no-await-in-loop
    await loadInstalledNamespaces(targetOrg);

    const totalDeploys: TotalDeploys = { value: 0 };

    // Build one BatchItem per permset + muting permset + profile — all deployed together each iteration.
    const batchItems: BatchItem[] = [
      ...permSets.map((n) => ({
        metadataType: 'PermissionSet',
        itemName: n,
        filePath: path.join(PS_BASE_PATH, `${n}.permissionset-meta.xml`),
        operation: getItemOperation(promotionData, 'PermissionSet', n),
        status: 'No Change',
        allRemovedFields: [] as Array<{ label: string; error: string }>,
        allRemovedRefs: [] as RemovedRef[],
        allSkippedFields: [] as string[],
        allUnhandledErrors: [] as string[],
        done: false,
      })),
      ...mutingPermSets.map((n) => ({
        metadataType: 'MutingPermissionSet',
        itemName: n,
        filePath: path.join(MUTING_PS_BASE_PATH, `${n}.mutingpermissionset-meta.xml`),
        operation: getItemOperation(promotionData, 'MutingPermissionSet', n),
        status: 'No Change',
        allRemovedFields: [] as Array<{ label: string; error: string }>,
        allRemovedRefs: [] as RemovedRef[],
        allSkippedFields: [] as string[],
        allUnhandledErrors: [] as string[],
        done: false,
      })),
      ...profiles.map((n) => ({
        metadataType: 'Profile',
        itemName: n,
        filePath: path.join(PROFILE_BASE_PATH, `${n}.profile-meta.xml`),
        operation: getItemOperation(promotionData, 'Profile', n),
        status: 'No Change',
        allRemovedFields: [] as Array<{ label: string; error: string }>,
        allRemovedRefs: [] as RemovedRef[],
        allSkippedFields: [] as string[],
        allUnhandledErrors: [] as string[],
        done: false,
      })),
    ];

    // Deduplication pre-pass is intentionally skipped.
    // Salesforce does not error on duplicate objectPermissions/PS blocks — it silently
    // deploys the last one. Auto-removing duplicates is risky: file position does not
    // reflect recency (a newer git-blame date can appear earlier in the file), so there
    // is no safe way to know which block represents the intended state. Leaving duplicates
    // in place is harmless; the developer should resolve conflicting blocks manually.
    // runDeduplicationPrePass(log, batchItems, REPO_PATH, dryRun);

    // Record HEAD so the final squash covers only missing-ref removal commits.
    const startingCommit = execSync('git rev-parse HEAD', { cwd: REPO_PATH }).toString().trim();
    log(`   Starting commit: ${startingCommit.substring(0, 8)}`);

    log('\n######################################################');
    log(
      `  PROCESSING BATCH: ${permSets.length} PermSet(s) + ${mutingPermSets.length} MutingPermSet(s) + ${profiles.length} Profile(s)`
    );
    log('######################################################');

    // eslint-disable-next-line no-await-in-loop
    const { summary, lastManagedRefsCommit } = await runBatchDeploy(
      log,
      batchItems,
      targetOrg,
      REPO_PATH,
      whitelist,
      allFilePaths,
      MAX_ITERATIONS,
      MAX_TOTAL_DEPLOYS,
      totalDeploys,
      DEPLOY_TIMEOUT_MINS,
      MAX_RETRIES,
      verbose,
      dryRun
    );

    // ================= FINAL SUMMARY =================
    log('\n======================================================');
    log('ALL ITEMS PROCESSED - FINAL SUMMARY');
    log('======================================================');

    log('\nPERMISSION SETS:');
    summary
      .filter((r) => r.Type === 'PermissionSet')
      .forEach((r) =>
        log(
          `   [${r.Name}] Status: ${r.Status} | Removed: ${r.RemovedFields || 'none'} | Skipped: ${
            r.SkippedFields || 'none'
          }`
        )
      );

    log('\nMUTING PERMISSION SETS:');
    summary
      .filter((r) => r.Type === 'MutingPermissionSet')
      .forEach((r) =>
        log(
          `   [${r.Name}] Status: ${r.Status} | Removed: ${r.RemovedFields || 'none'} | Skipped: ${
            r.SkippedFields || 'none'
          }`
        )
      );

    log('\nPROFILES:');
    summary
      .filter((r) => r.Type === 'Profile')
      .forEach((r) =>
        log(
          `   [${r.Name}] Status: ${r.Status} | Removed: ${r.RemovedFields || 'none'} | Skipped: ${
            r.SkippedFields || 'none'
          }`
        )
      );

    // ================= CONCLUSION =================
    const passedClean = summary.filter((r) => r.Status === 'Success' || r.Status === 'No Change');
    const hadFixes = summary.filter((r) => r.Status === 'Fixed & Committed');
    const needsAttention = summary.filter(
      (r) => r.Status !== 'Success' && r.Status !== 'No Change' && r.Status !== 'Fixed & Committed'
    );

    // Capturing log — writes to terminal AND accumulates for the conclusion file.
    const conclusionLines: string[] = [];
    const clog = (msg: string): void => {
      log(msg);
      conclusionLines.push(msg);
    };

    clog('\n======================================================');
    clog('CONCLUSION');
    clog('======================================================\n');

    const tableHeaders = ['#', 'Type', 'Name', 'Status', 'Removed', 'Skipped'];
    const tableRows = summary.map((r, i) => [
      String(i + 1),
      r.Type === 'PermissionSet' ? 'PermSet' : r.Type === 'MutingPermissionSet' ? 'MutingPS' : 'Profile',
      r.Name,
      r.Status,
      r.RemovedFields ? `${r.RemovedFields.split('; ').filter(Boolean).length} ref(s)` : '—',
      r.SkippedFields ? `${r.SkippedFields.split('; ').filter(Boolean).length} ref(s)` : '—',
    ]);
    clog(buildAsciiTable(tableHeaders, tableRows));

    logRemovedRefsDetail(clog, summary);

    if (needsAttention.length > 0) {
      clog(`\nNeeds manual attention (${needsAttention.length}):`);
      needsAttention.forEach((r) => {
        clog(`   - [${r.Type}] ${r.Name} — ${r.Status}`);
      });
    }

    const itemsWithUnhandled = summary.filter((r) => r.UnhandledErrors);
    if (itemsWithUnhandled.length > 0) {
      clog('\n------------------------------------------------------');
      clog('UNHANDLED ERRORS — These need manual fixes in the XML:');
      clog('------------------------------------------------------');
      itemsWithUnhandled.forEach((r) => {
        clog(`\n   [${r.Type}] ${r.Name}:`);
        r.UnhandledErrors.split('; ')
          .filter(Boolean)
          .forEach((e) => clog(`      ! ${e}`));
      });
      clog('');
    }

    clog(
      `\nPassed clean: ${passedClean.length}  |  Fixed & committed: ${hadFixes.length}  |  Needs manual attention: ${needsAttention.length}`
    );

    writeConclusionFile(log, conclusionLines.join('\n'), REPO_PATH);

    const csvPath = path.join(REPO_PATH, 'deploy_fix_summary.csv');
    const csvHeader = 'Type,Name,Status,RemovedFields,RemovedErrors,SkippedFields';
    const csvRows = summary.map(
      (r) => `${r.Type},"${r.Name}","${r.Status}","${r.RemovedFields}","${r.RemovedErrors}","${r.SkippedFields}"`
    );
    fs.writeFileSync(csvPath, [csvHeader, ...csvRows].join('\n'), 'utf8');

    log(`Summary CSV saved to : ${csvPath}`);
    log(`Total deploy calls   : ${totalDeploys.value} / ${MAX_TOTAL_DEPLOYS}`);

    // ================= SQUASH MISSING-REF COMMITS =================
    // Squash only the per-item missing-ref commits into one clean commit.
    // Managed-ref commits (before squashBase) are preserved as-is.
    // Repo-wide sweep runs AFTER squash so it stays as a separate commit.
    const squashBase = lastManagedRefsCommit ?? startingCommit;
    if (dryRun) {
      log('\nDry run — no commits were made, skipping squash.');
    } else {
      squashMissingRefCommits(log, squashBase, summary, REPO_PATH);
    }

    // ================= REPO-WIDE SWEEP =================
    // Runs after the squash so it appears as a clean separate commit at the end.
    repoWideSweep(log, collectBatchRefs(batchItems), new Set(allFilePaths), REPO_PATH, dryRun);

    const elapsedMs = Date.now() - startTime;
    const elapsedMins = Math.floor(elapsedMs / 60_000);
    const elapsedSecs = Math.floor((elapsedMs % 60_000) / 1000);
    log(`\nTotal time: ${elapsedMins}m ${elapsedSecs}s`);
  }
}
