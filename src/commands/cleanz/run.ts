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
import * as os from 'node:os';
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
  Op: string;
  Status: string;
  RemovedFields: string;
  RemovedErrors: string;
  SkippedFields: string;
  UnhandledErrors: string;
};

type TotalDeploys = { value: number };

type ErrorMask = {
  xmlPattern: RegExp; // regex applied to masked XML to strip the offending block
  label: string; // short identifier (e.g. 'loginIpRanges', 'userPermission:ViewAllData')
  reason: string; // original Salesforce error text, shown in Unhandled/Skipped Errors column
};

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
  errorBasedMasks: ErrorMask[]; // dynamically accumulated masks for blocks that caused unhandled errors
  done: boolean;
  calcFailedRetries: number; // tracks CalculationFailed transient retries for PSG items
  consecutiveZeroFailures: number; // consecutive zero-failure responses while success=false; must reach 2 before marking clean
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
  | 'recordTypeVisibility' // recordTypeVisibilities block referencing a missing RecordType
  | 'reportTypeColumn'; // columns block in a ReportType referencing a missing field/object

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

// Resolved lazily inside run() and runNamespacePurge() — NOT at module load time.
// Calling execSync at the top level blocks the Node.js thread during SF CLI module loading,
// which prevents any stdout output for 30-60 s on VDI/network storage (the "1-min delay" bug).
function resolveRepoPaths(): {
  REPO_PATH: string;
  PS_BASE_PATH: string;
  MUTING_PS_BASE_PATH: string;
  PSG_BASE_PATH: string;
  PROFILE_BASE_PATH: string;
  LAYOUT_BASE_PATH: string;
  REPORT_TYPE_BASE_PATH: string;
} {
  const repoPath = execSync('git rev-parse --show-toplevel', { cwd: process.cwd() }).toString().trim();
  return {
    REPO_PATH: repoPath,
    PS_BASE_PATH: path.join(repoPath, 'force-app', 'main', 'default', 'permissionsets'),
    MUTING_PS_BASE_PATH: path.join(repoPath, 'force-app', 'main', 'default', 'mutingpermissionsets'),
    PSG_BASE_PATH: path.join(repoPath, 'force-app', 'main', 'default', 'permissionsetgroups'),
    PROFILE_BASE_PATH: path.join(repoPath, 'force-app', 'main', 'default', 'profiles'),
    LAYOUT_BASE_PATH: path.join(repoPath, 'force-app', 'main', 'default', 'layouts'),
    REPORT_TYPE_BASE_PATH: path.join(repoPath, 'force-app', 'main', 'default', 'reportTypes'),
  };
}

const MAX_ITERATIONS = 500;
const MAX_TOTAL_DEPLOYS = 1000;
const DEPLOY_TIMEOUT_MINS = 25;
const MAX_RETRIES = 3;
const MAX_QUEUE_WAIT_MINS = 60; // wait up to 60 min for active Copado deployments to finish

// Core CRM standard objects that exist in every Salesforce org regardless of license.
// objectPermissions for these are always false positives in FULL dry-run — mask them.
const CORE_CRM_OBJECTS = [
  'Account',
  'Contact',
  'Lead',
  'Opportunity',
  'OpportunityLineItem',
  'OpportunityContactRole',
  'OpportunityTeamMember',
  'Case',
  'Campaign',
  'CampaignMember',
  'Contract',
  'ContractLineItem',
  'Order',
  'OrderItem',
  'Asset',
  'Quote',
  'QuoteLineItem',
  'Product2',
  'Pricebook2',
  'PricebookEntry',
  'ServiceContract',
  'Entitlement',
  'EntitlementContact',
  'Task',
  'Event',
  'User',
  'Territory2',
  'ContentVersion',
] as const;
const CORE_CRM_ALT = CORE_CRM_OBJECTS.join('|');

// Subset of CORE_CRM used for field-level masking only.
// Excludes Revenue Cloud / feature-licensed objects (Product2, Order, Asset, Quote, etc.)
// whose standard fields may not exist in all orgs (e.g. Product2.AvailabilityDate requires
// Revenue Cloud). Keeping those exposed lets the dry-run catch and auto-remove them.
const CORE_CRM_FIELD_OBJECTS = [
  'Account',
  'Contact',
  'Lead',
  'Opportunity',
  'OpportunityLineItem',
  'OpportunityContactRole',
  'OpportunityTeamMember',
  'Case',
  'Campaign',
  'CampaignMember',
  'Task',
  'Event',
  'User',
  'ContentVersion',
] as const;
const CORE_CRM_FIELD_ALT = CORE_CRM_FIELD_OBJECTS.join('|');

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
  const fixedPSGs = summary.filter((r) => r.Type === 'PermissionSetGroup' && r.RemovedFields);
  const fixedProfiles = summary.filter((r) => r.Type === 'Profile' && r.RemovedFields);
  const fixedReportTypes = summary.filter((r) => r.Type === 'ReportType' && r.RemovedFields);
  const fixedLayouts = summary.filter((r) => r.Type === 'Layout' && r.RemovedFields);
  if (
    fixedPermSets.length === 0 &&
    fixedMutingPermSets.length === 0 &&
    fixedPSGs.length === 0 &&
    fixedProfiles.length === 0 &&
    fixedReportTypes.length === 0 &&
    fixedLayouts.length === 0
  )
    return;

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
  if (fixedPSGs.length > 0) {
    log('\nPERMISSION SET GROUPS');
    log(buildAsciiTable(['Name', 'Removed Ref', 'Deployment Error'], buildRows(fixedPSGs), [15, 35, 45]));
  }
  if (fixedProfiles.length > 0) {
    log('\nPROFILES');
    log(buildAsciiTable(['Name', 'Removed Ref', 'Deployment Error'], buildRows(fixedProfiles), [15, 35, 45]));
  }
  if (fixedReportTypes.length > 0) {
    log('\nREPORT TYPES');
    log(buildAsciiTable(['Name', 'Removed Ref', 'Deployment Error'], buildRows(fixedReportTypes), [15, 35, 45]));
  }
  if (fixedLayouts.length > 0) {
    log('\nLAYOUTS');
    log(buildAsciiTable(['Name', 'Removed Ref', 'Deployment Error'], buildRows(fixedLayouts), [15, 35, 45]));
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

function removeAllLayoutAssignmentsForObject(
  xmlContent: string,
  objectName: string
): { updated: string; removed: boolean } {
  const escapedObject = objectName.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const innerPattern = '(?:(?!<layoutAssignments>)[\\s\\S])*?';
  // Matches any <layoutAssignments> block whose <layout> starts with "ObjectName-"
  const blockRegex = new RegExp(
    `[ \\t]*<layoutAssignments>${innerPattern}<layout>[ \\t]*${escapedObject}-[^<]+[ \\t]*</layout>${innerPattern}</layoutAssignments>[ \\t]*\\r?\\n?`,
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
  // Also remove all layoutAssignments for this object (layout name starts with "ObjectName-").
  // Profiles store layout refs per object — if the object is missing, its layouts are invalid too.
  // Only applies to profiles (permsets don't have layoutAssignments).
  const layoutResult = removeAllLayoutAssignmentsForObject(fieldResult.updated, name);
  return {
    updated: layoutResult.updated,
    removed: objectResult.removed || fieldResult.removed || layoutResult.removed,
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
export function removeColumnFromReportType(
  xmlContent: string,
  fieldName: string,
  objectName?: string
): { updated: string; removed: boolean } {
  const escapedField = fieldName.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const inner = '(?:(?!<columns>)[\\s\\S])*?';
  const blockRegex = new RegExp(
    `[ \\t]*<columns>${inner}<field>[ \\t]*${escapedField}[ \\t]*</field>${inner}</columns>[ \\t]*\\r?\\n?`,
    'g'
  );
  if (!objectName) {
    const updated = xmlContent.replace(blockRegex, '');
    return { updated, removed: updated !== xmlContent };
  }
  // When objectName is known, only remove the <columns> block whose <table> terminal
  // segment relates to objectName. The table path uses object names in plural form
  // (e.g. "Contacts" → Contact, "Accounts__r" → Account). Checking only the last
  // segment avoids false matches on intermediate path segments (e.g. "Accounts__r"
  // inside "Opportunity.OpportunityContactRoles.Accounts__r.Contacts" should not
  // match when objectName is "Account" and the field actually belongs to Contact).
  const objLower = objectName.toLowerCase();
  let removed = false;
  const updated = xmlContent.replace(blockRegex, (match: string) => {
    const tableMatch = match.match(/<table>([\s\S]*?)<\/table>/i);
    const tableVal = tableMatch ? tableMatch[1].trim().toLowerCase() : '';
    if (tableVal) {
      const segments = tableVal.split('.');
      const lastSeg = segments[segments.length - 1].replace(/__r$/i, '');
      // Match singular (opportunity) and y→ies plural (opportunities) — e.g. "opportunities".includes("opportunity") = false
      const objBase = objLower.endsWith('y') ? objLower.slice(0, -1) + 'ie' : objLower;
      if (!lastSeg.includes(objLower) && !lastSeg.includes(objBase)) return match;
    }
    removed = true;
    return '';
  });
  if (removed) return { updated, removed };

  // Fallback: only activate when the bare field name does not appear at all in the XML.
  // If the exact <field>fieldName</field> tag exists, the standard check already evaluated
  // it and chose not to remove it (e.g. it belongs to a different object) — respect that.
  // The fallback only handles the traversal case where the XML has a dotted prefix
  // (e.g. "SBQQSC__RenewalOpportunity__c.ISR_Owner__c") so the bare name was never found.
  const exactFieldPresent = new RegExp(`<field>[ \\t]*${escapedField}[ \\t]*</field>`, 'i').test(xmlContent);
  if (exactFieldPresent) return { updated: xmlContent, removed: false };

  // Count all <columns> blocks whose <field> ends with fieldName (covers traversal prefixes).
  // Only remove if exactly 1 such block exists — if 2+, ambiguous which object, so skip.
  const fallbackBlockRegex = new RegExp(
    `[ \\t]*<columns>${inner}<field>[ \\t]*(?:[^<]*\\.)?${escapedField}[ \\t]*</field>${inner}</columns>[ \\t]*\\r?\\n?`,
    'g'
  );
  const fallbackMatches = [...xmlContent.matchAll(fallbackBlockRegex)];
  if (fallbackMatches.length === 1) {
    return { updated: xmlContent.replace(fallbackBlockRegex, ''), removed: true };
  }
  return { updated: xmlContent, removed: false };
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
// PERMISSION SET GROUP — ORDER / DEDUP FIX
//
// Salesforce enforces that all <permissionSets> elements in a PSG file must
// appear as ONE contiguous block positioned after <label>.  If any tag lands
// above <label> (e.g. a merge conflict resolution) Salesforce throws:
//   "Element permissionSets is duplicated at this location in type PermissionSetGroup"
//
// This function:
//   1. Collects ALL <permissionSets>...</permissionSets> lines from anywhere in the file
//   2. Deduplicates (case-insensitive name comparison)
//   3. Strips them all from their current positions
//   4. Re-inserts them as one sorted block immediately after the <label> element
// ===============================================================

/**
 * Removes duplicate <permissionSets> lines from a PSG file in-place.
 * Does NOT reorder or relocate any lines — preserves original position so
 * git blame stays clean and tracing commits remains straightforward.
 * Only the second (and further) occurrences of a duplicate name are removed.
 */
export function removeDuplicatePsgPermissionSets(xmlContent: string): { updated: string; removed: string[] } {
  const seen = new Set<string>();
  const removedNames: string[] = [];
  const tagRegex = /[ \t]*<permissionSets>([^<]*)<\/permissionSets>[ \t]*\r?\n?/g;
  const updated = xmlContent.replace(tagRegex, (fullMatch, name: string) => {
    const key = name.trim().toLowerCase();
    if (seen.has(key)) {
      removedNames.push(name.trim());
      return ''; // remove duplicate — keep first occurrence only
    }
    seen.add(key);
    return fullMatch; // keep first occurrence as-is
  });
  return { updated, removed: removedNames };
}

export function fixPsgPermissionSetsBlock(xmlContent: string): { updated: string; fixed: boolean } {
  // Collect every <permissionSets> line (single-line tags only — Salesforce always writes them this way)
  const tagRegex = /[ \t]*<permissionSets>([^<]*)<\/permissionSets>[ \t]*\r?\n?/g;
  const collected: string[] = [];
  let match: RegExpExecArray | null;
  // eslint-disable-next-line no-cond-assign
  while ((match = tagRegex.exec(xmlContent)) !== null) {
    collected.push(match[1].trim());
  }

  if (collected.length === 0) return { updated: xmlContent, fixed: false };

  // Deduplicate — preserve first occurrence order, case-insensitive comparison
  const seen = new Set<string>();
  const unique = collected.filter((name) => {
    const key = name.toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  // Sort alphabetically (case-insensitive)
  unique.sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));

  // Strip ALL existing <permissionSets> lines from the XML
  const stripped = xmlContent.replace(tagRegex, '');

  // Build the replacement block — one line per entry, indented with 4 spaces
  const block = unique.map((name) => `    <permissionSets>${name}</permissionSets>`).join('\n') + '\n';

  // Insert the block immediately after the closing </label> tag
  const labelClose = '</label>';
  const labelIdx = stripped.indexOf(labelClose);
  if (labelIdx === -1) {
    // No <label> tag found — append before the closing root tag as a fallback
    const rootClose = stripped.lastIndexOf('</');
    if (rootClose === -1) return { updated: xmlContent, fixed: false };
    const updated = stripped.slice(0, rootClose) + block + stripped.slice(rootClose);
    return { updated, fixed: true };
  }

  const insertAt = labelIdx + labelClose.length;
  // Consume the newline that follows </label> so we don't get a blank line
  const afterLabel = stripped[insertAt] === '\r' ? insertAt + 2 : stripped[insertAt] === '\n' ? insertAt + 1 : insertAt;
  const updated = stripped.slice(0, afterLabel) + block + stripped.slice(afterLabel);
  return { updated, fixed: true };
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
  xml = removeBlocksWithNamespace(xml, 'profileActionOverrides', 'pageOrSobjectType', namespace);
  xml = removeBlocksWithNamespace(xml, 'customMetadataTypeAccesses', 'name', namespace);
  xml = removeBlocksWithNamespace(xml, 'customPermissions', 'name', namespace);

  // After block removal, commented-out sections whose entire content was stripped
  // leave empty comment delimiters behind (e.g. <!---->). Remove them.
  xml = xml.replace(/[ \t]*<!--\s*-->[ \t]*\r?\n?/g, '');

  return { updated: xml, removed: xml !== xmlContent };
}

function applyRefToXml(xml: string, ref: RemovedRef, filePath: string): { updated: string; removed: boolean } | null {
  if (ref.type === 'namespace') return resolveNsRemoval(xml, ref.name, filePath);
  if (ref.type === 'field') return removeFieldPermissionsFromXml(xml, ref.name);
  if (ref.type === 'reportTypeColumn') {
    // ref.name is the full SF error name (e.g. "Opportunity.Field__c").
    // The XML stores only the bare field name in <field> and the object in <table>,
    // so split and use the object-aware removal to avoid touching columns from
    // a different object that happens to share the same field API name.
    const dotIdx = ref.name.lastIndexOf('.');
    const fieldPart = dotIdx >= 0 ? ref.name.substring(dotIdx + 1) : ref.name;
    const objectName = dotIdx >= 0 ? ref.name.substring(0, dotIdx) : undefined;
    return removeColumnFromReportType(xml, fieldPart, objectName);
  }
  if (ref.type === 'userPermission') return removeUserPermissionFromXml(xml, ref.name);
  if (ref.type === 'objectFlag') {
    if (!ref.meta) return null;
    return removeObjectPermissionFlag(xml, ref.name, ref.meta);
  }
  const handler = METADATA_HANDLERS.find((h) => h.refType === ref.type);
  return handler ? handler.removeFn(xml, ref.name) : null;
}

function resolveNsRemoval(xml: string, namespace: string, filePath: string): { updated: string; removed: boolean } {
  if (filePath.endsWith('.layout-meta.xml')) return removeNsFromLayout(xml, namespace);
  if (filePath.toLowerCase().endsWith('.reporttype-meta.xml')) return removeNsFromReportType(xml, namespace);
  return bulkRemoveNamespaceRefs(xml, namespace);
}

function removeNsFromLayout(xml: string, namespace: string): { updated: string; removed: boolean } {
  let updated = removeBlocksWithNamespace(xml, 'layoutItems', 'field', namespace);
  updated = removeBlocksWithNamespace(updated, 'quickActionListItems', 'quickActionName', namespace);
  return { updated, removed: updated !== xml };
}

function removeNsFromReportType(xml: string, namespace: string): { updated: string; removed: boolean } {
  const updated = removeBlocksWithNamespace(xml, 'columns', 'field', namespace);
  return { updated, removed: updated !== xml };
}

function removeLayoutItemByField(xmlContent: string, bareField: string): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'layoutItems', 'field', bareField);
}

function removeLayoutRelatedListByName(
  xmlContent: string,
  relatedListName: string
): { updated: string; removed: boolean } {
  return removeXmlBlock(xmlContent, 'relatedLists', 'relatedList', relatedListName);
}

// Remove a <fields> column entry inside any <relatedLists> block (case-insensitive field name match).
function removeRelatedListFieldEntry(xmlContent: string, fullFieldName: string): { updated: string; removed: boolean } {
  const escaped = fullFieldName.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
  const regex = new RegExp(`[ \\t]*<fields>[ \\t]*${escaped}[ \\t]*</fields>[ \\t]*\\r?\\n?`, 'gi');
  const updated = xmlContent.replace(regex, '');
  return { updated, removed: updated !== xmlContent };
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
      await waitForQueueToClear(log, targetOrg, MAX_QUEUE_WAIT_MINS);
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
    let queueState: 'unknown' | 'queued' | 'deploying' = 'unknown';
    let queryInFlight = false;
    const progressTimer = setInterval(() => {
      const elapsed = Date.now() - deployStart;
      const mins = Math.floor(elapsed / 60_000);
      const secs = Math.floor((elapsed % 60_000) / 1000);

      if (queryInFlight) {
        if (queueState === 'queued') {
          log(`   Copado deployment in progress — waiting in queue (${mins}m ${secs}s elapsed)`);
        } else {
          log(`   Deploying changes... ${mins}m ${secs}s elapsed (timeout: ${timeoutMins}m)`);
        }
        return;
      }

      queryInFlight = true;
      queryDeployQueueCount(targetOrg)
        .then((count) => {
          queryInFlight = false;
          const e = Date.now() - deployStart;
          const m = Math.floor(e / 60_000);
          const s = Math.floor((e % 60_000) / 1000);
          if (count >= 2) {
            queueState = 'queued';
            log(`   Copado deployment in progress — waiting in queue (${m}m ${s}s elapsed)`);
          } else {
            if (queueState === 'queued') {
              log('   Queue cleared — deploying changes');
            } else {
              log(`   Deploying changes... ${m}m ${s}s elapsed (timeout: ${timeoutMins}m)`);
            }
            queueState = 'deploying';
          }
        })
        .catch(() => {
          queryInFlight = false;
          log(`   Deploying changes... ${mins}m ${secs}s elapsed (timeout: ${timeoutMins}m)`);
        });
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
    }, 60_000); // 60s — VDI/network storage can be slow
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

// ===============================================================
// PSG STATUS POLLING
// Salesforce recalculates a PermissionSetGroup in the background
// after each deploy. We poll until Status = Updated (success),
// CalculationFailed (transient — re-deploy fixes it), or any other
// terminal state (manual intervention needed).
// ===============================================================

function queryPsgStatus(targetOrg: string, psgName: string): Promise<string | null> {
  return new Promise((resolve) => {
    const query = `"SELECT Status FROM PermissionSetGroup WHERE DeveloperName = '${psgName}'"`;
    const args = ['data', 'query', '--query', query, '--target-org', targetOrg, '--json'];
    const proc = spawn('sf', args, { shell: true });
    const chunks: string[] = [];
    proc.stdout.on('data', (d: Buffer) => chunks.push(d.toString()));
    proc.stderr.on('data', (d: Buffer) => chunks.push(d.toString()));
    const timer = setTimeout(() => {
      proc.kill();
      resolve(null);
    }, 30_000);
    proc.on('close', () => {
      clearTimeout(timer);
      try {
        const raw = chunks.join('');
        const start = raw.indexOf('{');
        const json = JSON.parse(start >= 0 ? raw.substring(start) : raw) as {
          result?: { records?: Array<{ Status?: string }> };
        };
        resolve(json?.result?.records?.[0]?.Status ?? null);
      } catch {
        resolve(null);
      }
    });
  });
}

// Queries the target org and returns which of the given PermissionSet DeveloperNames exist.
function queryExistingPermSets(targetOrg: string, psNames: string[]): Promise<string[]> {
  return new Promise((resolve) => {
    if (psNames.length === 0) {
      resolve([]);
      return;
    }
    const inClause = psNames.map((n) => `'${n}'`).join(', ');
    // Query by Name — for non-namespaced custom PSes this is identical to DeveloperName.
    // Managed-package PSes are handled separately via checkNamespaceInstalled in handlePsgInvalidPsItem.
    const query = `"SELECT Name FROM PermissionSet WHERE Name IN (${inClause})"`;
    const args = ['data', 'query', '--query', query, '--target-org', targetOrg, '--json'];
    const proc = spawn('sf', args, { shell: true });
    const chunks: string[] = [];
    proc.stdout.on('data', (d: Buffer) => chunks.push(d.toString()));
    proc.stderr.on('data', (d: Buffer) => chunks.push(d.toString()));
    const timer = setTimeout(() => {
      proc.kill();
      resolve([]);
    }, 60_000);
    proc.on('close', () => {
      clearTimeout(timer);
      try {
        const raw = chunks.join('');
        const start = raw.indexOf('{');
        const json = JSON.parse(start >= 0 ? raw.substring(start) : raw) as {
          result?: { records?: Array<{ Name?: string }> };
        };
        resolve(json?.result?.records?.map((r) => r.Name ?? '').filter(Boolean) ?? []);
      } catch {
        resolve([]);
      }
    });
  });
}

async function waitForPsgUpdates(
  log: (msg: string) => void,
  psgName: string,
  targetOrg: string
): Promise<'updated' | 'calc-failed' | 'failed'> {
  const POLL_MS = 30_000;
  const MAX_WAIT_MS = 10 * 60_000; // 10 minutes max
  const deadline = Date.now() + MAX_WAIT_MS;

  log(`   [PSG] ${psgName} — waiting for recalculation to complete...`);

  while (Date.now() < deadline) {
    // eslint-disable-next-line no-await-in-loop
    const status = await queryPsgStatus(targetOrg, psgName);

    if (status === 'Updated') {
      log(`   [PSG] ${psgName} — status: Updated ✓`);
      return 'updated';
    }
    if (status === 'CalculationFailed') {
      log(`   [PSG] ${psgName} — status: CalculationFailed (transient — re-deploy will trigger fresh recalculation)`);
      return 'calc-failed';
    }
    if (status !== 'Updating' && status !== null) {
      log(`   [PSG] ${psgName} — status: ${status}. Manual intervention needed.`);
      return 'failed';
    }

    const elapsed = Math.round((Date.now() - (deadline - MAX_WAIT_MS)) / 1000);
    log(`   [PSG] ${psgName} — status: ${status ?? 'unknown'} (${elapsed}s elapsed). Checking again in 30s...`);
    // eslint-disable-next-line no-await-in-loop
    await sleep(POLL_MS);
  }

  log(`   [PSG] ${psgName} — timed out after 10 min waiting for recalculation.`);
  return 'failed';
}

async function waitForQueueToClear(log: (msg: string) => void, targetOrg: string, maxWaitMins = 30): Promise<void> {
  const POLL_MS = 30_000;
  const deadline = Date.now() + maxWaitMins * 60_000;
  const queueStart = Date.now();

  // eslint-disable-next-line no-await-in-loop
  let count = await queryDeployQueueCount(targetOrg);
  if (count === 0) return;

  log('   [Queue] Active deployment in Copado — waiting for queue to clear...');
  while (count > 0 && Date.now() < deadline) {
    // eslint-disable-next-line no-await-in-loop
    await sleep(POLL_MS);
    // eslint-disable-next-line no-await-in-loop
    count = await queryDeployQueueCount(targetOrg);
    if (count > 0) {
      const elapsed = Date.now() - queueStart;
      const mins = Math.floor(elapsed / 60_000);
      const secs = Math.floor((elapsed % 60_000) / 1000);
      log(`   [Queue] Still waiting for queue to clear — active deployment in Copado (${mins}m ${secs}s elapsed)`);
    }
  }

  if (count === 0) {
    log('   [Queue] Queue cleared — proceeding with validation.');
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

function processReportTypeColumnFailure(
  log: (msg: string) => void,
  errorMessage: string,
  xmlContent: string,
  whitelist: WhitelistMap,
  skippedFields: string[],
  allSkippedFields: string[]
): FailureResult {
  const patterns = [
    /no CustomField named (.+?) found/i,
    /Entity of type 'CustomField' named '(.+?)' cannot be found/i,
    /In field: field - no CustomField named (.+?) found/i,
    /no CustomObject named (.+?) found/i,
    /Entity of type 'CustomObject' named '(.+?)' cannot be found/i,
  ];
  let missingName: string | null = null;
  for (const p of patterns) {
    const m = p.exec(errorMessage);
    if (m) {
      missingName = m[1].trim();
      break;
    }
  }
  if (!missingName) return { handled: false, xmlContent };

  const allWhitelisted = [...whitelist.fields, ...whitelist.objects];
  if (shouldSkip(log, 'column', missingName, allWhitelisted, skippedFields, allSkippedFields)) {
    return { handled: true, xmlContent };
  }

  log(`   Missing column reference: ${missingName}`);

  // SF error includes the object prefix ("Opportunity.Field__c"). The XML <field> tag
  // stores only the bare name, and <table> stores the object. Split and match both.
  const dotIdx = missingName.lastIndexOf('.');
  const fieldPart = dotIdx >= 0 ? missingName.substring(dotIdx + 1) : missingName;
  const objectName = dotIdx >= 0 ? missingName.substring(0, dotIdx) : undefined;
  const result = removeColumnFromReportType(xmlContent, fieldPart, objectName);

  if (result.removed) {
    log(`   Removed column for: ${missingName}`);
    return {
      handled: true,
      xmlContent: result.updated,
      removedRef: {
        type: 'reportTypeColumn',
        name: missingName,
        label: `[Column] ${missingName}`,
        deployError: errorMessage,
      },
    };
  }

  log(`   Column not found in XML: ${missingName} — already removed or not present.`);
  return { handled: true, xmlContent };
}

function processLayoutFailure(
  log: (msg: string) => void,
  errorMessage: string,
  xmlContent: string,
  promotionData: PromotionItem[]
): FailureResult {
  // Missing field in layoutItems: In field: field - no CustomField named Obj.Field__c found
  const fieldMatch =
    /In field: field - no CustomField named (.+?) found/i.exec(errorMessage) ??
    /no CustomField named (.+?) found/i.exec(errorMessage);
  if (fieldMatch && !/In field: relatedList/i.test(errorMessage)) {
    const fullFieldName = fieldMatch[1].trim();
    const dotIdx = fullFieldName.lastIndexOf('.');
    const bareField = dotIdx >= 0 ? fullFieldName.substring(dotIdx + 1) : fullFieldName;
    const isInBatchAsAdd = promotionData.some(
      (i) => i.t === 'CustomField' && i.n === fullFieldName && (!i.a || i.a.toLowerCase().startsWith('add'))
    );
    if (isInBatchAsAdd) {
      log(`   [Layout] Skipping — ${fullFieldName} is ADD in batch`);
      return { handled: true, xmlContent };
    }
    log(`   [Layout] Missing field: ${fullFieldName}`);
    const { updated, removed } = removeLayoutItemByField(xmlContent, bareField);
    if (removed) {
      log(`   [Layout] Removed layoutItem for: ${fullFieldName}`);
      return {
        handled: true,
        xmlContent: updated,
        removedRef: {
          type: 'field',
          name: fullFieldName,
          label: `[Layout.field] ${fullFieldName}`,
          deployError: errorMessage,
        },
      };
    }
    log(`   [Layout] layoutItem not found for: ${fullFieldName}`);
    return { handled: true, xmlContent };
  }

  // Missing field referenced by a relatedList column: In field: relatedList - no CustomField named X found
  const rlFieldMatch = /In field: relatedList - no CustomField named (.+?) found/i.exec(errorMessage);
  if (rlFieldMatch) {
    const fullFieldName = rlFieldMatch[1].trim();
    const isInBatchAsAdd = promotionData.some(
      (i) => i.t === 'CustomField' && i.n === fullFieldName && (!i.a || i.a.toLowerCase().startsWith('add'))
    );
    if (isInBatchAsAdd) {
      log(`   [Layout] Skipping relatedList column — ${fullFieldName} is ADD in batch`);
      return { handled: true, xmlContent };
    }
    log(`   [Layout] Missing relatedList column field: ${fullFieldName}`);
    const { updated, removed } = removeRelatedListFieldEntry(xmlContent, fullFieldName);
    if (removed) {
      log(`   [Layout] Removed <fields> entry for: ${fullFieldName}`);
      return {
        handled: true,
        xmlContent: updated,
        removedRef: {
          type: 'field',
          name: fullFieldName,
          label: `[Layout.relatedList.field] ${fullFieldName}`,
          deployError: errorMessage,
        },
      };
    }
    log(`   [Layout] <fields> entry not found for: ${fullFieldName}`);
    return { handled: true, xmlContent };
  }

  // Missing relatedList object: Cannot find related list: RelationshipName
  const rlMatch = /Cannot find related list:\s*(.+)/i.exec(errorMessage);
  if (rlMatch) {
    const relatedListName = rlMatch[1].trim();
    log(`   [Layout] Missing relatedList: ${relatedListName}`);
    const { updated, removed } = removeLayoutRelatedListByName(xmlContent, relatedListName);
    if (removed) {
      log(`   [Layout] Removed relatedLists block for: ${relatedListName}`);
      return {
        handled: true,
        xmlContent: updated,
        removedRef: {
          type: 'field',
          name: relatedListName,
          label: `[Layout.relatedList] ${relatedListName}`,
          deployError: errorMessage,
        },
      };
    }
    log(`   [Layout] relatedLists block not found for: ${relatedListName}`);
    return { handled: true, xmlContent };
  }

  return { handled: false, xmlContent };
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

    // For Profile metadata: "no CustomObject named X found" can originate from
    // profileActionOverrides.pageOrSobjectType. For ADD profiles Copado TRIM strips
    // objectPermissions so the error must come from profileActionOverrides; for FULL
    // profiles profileActionOverrides is masked in dry-run so this block won't match.
    // Try removing the profileActionOverrides block by pageOrSobjectType before the
    // skip guard fires and swallows the error.
    if (metadataType === 'Profile' && handler.refType === 'object') {
      const { updated, removed } = removeProfileActionOverrideByPageObjectFromXml(xmlContent, name);
      if (removed) {
        log(`   [Profile] Removed profileActionOverrides block for missing object in pageOrSobjectType: ${name}`);
        return {
          handled: true,
          xmlContent: updated,
          removedRef: {
            type: handler.refType,
            name,
            label: `[ProfileActionOverride] Object: ${name}`,
            deployError: errorMessage,
          },
        };
      }
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

// eslint-disable-next-line complexity
function processFailures(
  log: (msg: string) => void,
  failures: ComponentFailure[],
  xmlContent: string,
  whitelist: WhitelistMap,
  allSkippedFields: string[],
  metadataType: string,
  verbose: boolean,
  promotionData: PromotionItem[] = []
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

    // ── Permission dependency errors ──────────────────────────────────────────
    // Object-flag PermDep (e.g. "Permission Edit X depends on Read X") and system
    // userPermission PermDep (e.g. "Permission ViewAllData depends on ModifyAllData")
    // are both left as unhandled so the user is informed. tryAddErrorMask will mask
    // the <userPermissions> block for system perms so validation can continue; for
    // object-flag PermDep it returns false and the item stops as Partial/Manual.
    if (/Permission .+ depends on permission\(s\):/i.test(err)) {
      unhandledErrors.push(err);
      continue;
    }

    // ── PermissionSetGroup duplicate/misplaced <permissionSets> ──
    if (/Element permissionSets is duplicated at this location in type PermissionSetGroup/i.test(err)) {
      const psgResult = fixPsgPermissionSetsBlock(updatedXml);
      if (psgResult.fixed) {
        log('   [PSG] Re-grouped and deduplicated <permissionSets> block after <label>');
        updatedXml = psgResult.updated;
        removedRefs.push({
          type: 'field',
          name: 'permissionSets-reorder',
          label: '[PSG] permissionSets block reordered/deduplicated',
          deployError: err,
        });
      } else {
        log(`   [PSG] Could not fix permissionSets ordering — no <label> tag or no tags found: ${err}`);
        unhandledErrors.push(err);
      }
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

    // ── ReportType column removal — intercept field errors before generic handler ──
    if (metadataType === 'ReportType') {
      const rtResult = processReportTypeColumnFailure(log, err, updatedXml, whitelist, skippedFields, allSkippedFields);
      if (rtResult.handled) {
        updatedXml = rtResult.xmlContent;
        if (rtResult.removedRef) removedRefs.push(rtResult.removedRef);
        continue;
      }
    }

    // ── Layout field / relatedList removal ────────────────────────
    if (metadataType === 'Layout') {
      const layoutResult = processLayoutFailure(log, err, updatedXml, promotionData);
      if (layoutResult.handled) {
        updatedXml = layoutResult.xmlContent;
        if (layoutResult.removedRef) removedRefs.push(layoutResult.removedRef);
        continue;
      }
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

    // Salesforce record locking — transient, not a missing-ref problem. Copado
    // deploys past it; treat as non-fatal so the component doesn't land in
    // "Partial / Manual Check Needed" when this is the only remaining error.
    if (/unable to obtain exclusive access to this record/i.test(err)) {
      log(`   Skipping transient lock error (non-fatal): ${err}`);
      continue;
    }

    // PSG invalid PermissionSet refs — handled by handlePsgInvalidPsItem in applyManagedRefsPass.
    // Don't add to unhandledErrors here; applyManagedRefsPass will query the org, remove
    // the missing PS refs from the PSG XML, commit, and retry.
    if (
      /Cannot create permission set group components since the following permission set names are invalid/i.test(err) &&
      metadataType === 'PermissionSetGroup'
    ) {
      log('   [PSG] Invalid PermissionSet ref(s) — will be resolved in managed refs pass');
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

// Writes XML back preserving the original line endings (CRLF or LF) without
// reformatting. Used for sweep/fix commits so git diffs show only the removed
// blocks — not every line in the file (which formatXml would cause).
function saveXmlPreserved(xml: string, filePath: string): void {
  const original = fs.existsSync(filePath) ? readFileWithRetry(filePath) : '';
  const usesCrlf = original.includes('\r\n');
  const content = usesCrlf ? xml.replace(/\r?\n/g, '\r\n') : xml;
  writeFileWithRetry(filePath, content);
}

function saveSweptFile(xml: string, filePath: string): void {
  if (filePath.endsWith('.layout-meta.xml') || filePath.toLowerCase().endsWith('.reporttype-meta.xml')) {
    writeFileWithRetry(filePath, xml);
  } else {
    saveXmlPreserved(xml, filePath);
  }
}

function shouldSkipSweepRef(
  ref: RemovedRef,
  isReportType: boolean,
  isLayoutOrReport: boolean,
  isProfile: boolean,
  name: string
): boolean {
  if (isReportType) return ref.type !== 'namespace' && ref.type !== 'reportTypeColumn';
  if (isLayoutOrReport) return ref.type !== 'namespace';
  if (ref.type === 'reportTypeColumn') return true;
  return isProfileSweepSkip(isProfile, ref.type, name);
}

function sweepOtherFiles(
  log: (msg: string) => void,
  refs: RemovedRef[],
  skipPaths: Set<string>,
  allFilePaths: string[],
  repoPath: string,
  dryRun: boolean,
  batchItemsByPath?: Map<string, BatchItem>
): void {
  if (refs.length === 0) return;

  log('\n   [Sweep] Removing same missing refs from all other files in batch...');
  const modifiedFiles: string[] = [];

  for (const filePath of allFilePaths) {
    if (skipPaths.has(filePath) || !fs.existsSync(filePath)) continue;

    let xml = readFileWithRetry(filePath);
    let fileModified = false;

    const isProfile = filePath.endsWith('.profile-meta.xml');
    const isReportType = filePath.toLowerCase().endsWith('.reporttype-meta.xml');
    const isLayoutOrReport = filePath.endsWith('.layout-meta.xml') || isReportType;
    for (const ref of refs) {
      if (shouldSkipSweepRef(ref, isReportType, isLayoutOrReport, isProfile, ref.name)) continue;
      const result = applyRefToXml(xml, ref, filePath);
      if (!result) continue;
      if (result.removed) {
        xml = result.updated;
        fileModified = true;
        log(`   [Sweep] Removed ${ref.label} from ${path.basename(filePath)}`);
        const sweptItem = batchItemsByPath?.get(filePath);
        if (sweptItem && !sweptItem.allRemovedFields.some((r) => r.label === ref.label)) {
          sweptItem.allRemovedFields.push({ label: ref.label, error: ref.deployError ?? '' });
          sweptItem.allRemovedRefs.push(ref);
        }
      }
    }

    if (fileModified) {
      saveSweptFile(xml, filePath);
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

  const layoutDir = path.join(repoPath, 'force-app', 'main', 'default', 'layouts');
  const reportTypeDir = path.join(repoPath, 'force-app', 'main', 'default', 'reportTypes');

  const repoFiles = [
    ...collectFiles(psDir, '.permissionset-meta.xml'),
    ...collectFiles(mpsDir, '.mutingpermissionset-meta.xml'),
    ...collectFiles(profileDir, '.profile-meta.xml'),
    ...collectFiles(layoutDir, '.layout-meta.xml'),
    ...collectFiles(reportTypeDir, '.reportType-meta.xml'),
  ].filter((f) => !batchFilePaths.has(f)); // exclude files already in the batch

  if (repoFiles.length === 0) {
    log('\n   [Repo Sweep] No files outside the batch to sweep.');
    return;
  }

  // Skip file types that have no applicable ref types to avoid wasted I/O:
  //   layouts      → only namespace refs apply
  //   report types → namespace + reportTypeColumn refs apply
  const hasNamespaceRef = allRemovedRefs.some((r) => r.type === 'namespace');

  const effectiveFiles = repoFiles.filter((f) => {
    if (f.endsWith('.layout-meta.xml')) return hasNamespaceRef;
    // reportTypeColumn refs excluded from repo-wide sweep — each promotion fixes its own report types
    if (f.toLowerCase().endsWith('.reporttype-meta.xml')) return hasNamespaceRef;
    return true; // permsets / profiles / PSGs always included
  });

  log(`\n--- Repo-Wide Sweep (${effectiveFiles.length} file(s) outside batch) ---`);
  const modifiedFiles: string[] = [];
  const removedRefLabels = new Set<string>();

  // Option 1: emit progress every 50 files so the UI heartbeat never shows "Deploy in progress"
  const PROGRESS_INTERVAL = 50;

  for (let i = 0; i < effectiveFiles.length; i++) {
    const filePath = effectiveFiles[i];
    if (i > 0 && i % PROGRESS_INTERVAL === 0) {
      log(`   [Repo Sweep] Scanning ${i}/${effectiveFiles.length}...`);
    }
    if (!fs.existsSync(filePath)) continue;
    let xml = readFileWithRetry(filePath);
    let fileModified = false;
    const isProfile = filePath.endsWith('.profile-meta.xml');
    const isReportType = filePath.toLowerCase().endsWith('.reporttype-meta.xml');
    const isLayoutOrReport = filePath.endsWith('.layout-meta.xml') || isReportType;

    for (const ref of allRemovedRefs) {
      // reportTypeColumn refs never applied to out-of-batch report type files
      if (isReportType && ref.type === 'reportTypeColumn') continue;
      if (shouldSkipSweepRef(ref, isReportType, isLayoutOrReport, isProfile, ref.name)) continue;
      const result = applyRefToXml(xml, ref, filePath);
      if (!result) continue;
      if (result.removed) {
        xml = result.updated;
        fileModified = true;
        removedRefLabels.add(ref.label);
        log(`   [Repo Sweep] Removed ${ref.label} from ${path.basename(filePath)}`);
      }
    }

    if (fileModified) {
      saveSweptFile(xml, filePath);
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

function maskCoreCrmObjectPermissions(xml: string): string {
  const inner = '(?:(?!<objectPermissions>)[\\s\\S])*?';
  return xml.replace(
    new RegExp(
      `[ \\t]*<objectPermissions>${inner}<object>[ \\t]*(?:${CORE_CRM_ALT})[ \\t]*</object>${inner}</objectPermissions>[ \\t]*\\r?\\n?`,
      'g'
    ),
    ''
  );
}

function maskCoreCrmFieldPermissions(xml: string): string {
  const inner = '(?:(?!<fieldPermissions>)[\\s\\S])*?';
  return xml.replace(
    new RegExp(
      `[ \\t]*<fieldPermissions>${inner}<field>[ \\t]*(?:${CORE_CRM_FIELD_ALT})\\.[^<]*</field>${inner}</fieldPermissions>[ \\t]*\\r?\\n?`,
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
  // FULL profiles — objectPermissions:
  //   Copado TRIM strips __c/__mdt object permissions before real deploy — mask those.
  //   Standard objects, big objects (__b), and platform events (__e) that don't exist
  //   in the org cause hard errors Copado does NOT clean — keep them exposed.
  //
  // FULL profiles — fieldPermissions:
  //   Copado TRIM strips __c field permissions before real deploy — mask those.
  //   fieldPermissions for standard objects, big objects (__b), and platform events (__e)
  //   are NOT stripped by Copado. Feature-gated standard objects (e.g. AccountContactRelation)
  //   may not exist in the target org and will cause hard deployment errors — keep them
  //   exposed so the dry-run detects and removes them.
  //
  // ADD profiles — fieldPermissions and objectPermissions:
  //   Copado TRIM strips ALL of them before real deploy — mask everything.
  let xml = xmlContent;
  xml = xml.replace(/[ \t]*<applicationVisibilities>[\s\S]*?<\/applicationVisibilities>[ \t]*\r?\n?/g, '');
  xml = xml.replace(/[ \t]*<categoryGroupVisibilities>[\s\S]*?<\/categoryGroupVisibilities>[ \t]*\r?\n?/g, '');
  xml = xml.replace(/[ \t]*<classAccesses>[\s\S]*?<\/classAccesses>[ \t]*\r?\n?/g, '');
  xml = xml.replace(/[ \t]*<pageAccesses>[\s\S]*?<\/pageAccesses>[ \t]*\r?\n?/g, '');
  // customSettingAccesses: always __c — Copado treats missing custom setting errors as warnings
  // and deploys successfully. Mask for both ADD and FULL to avoid false positives.
  xml = xml.replace(/[ \t]*<customSettingAccesses>[\s\S]*?<\/customSettingAccesses>[ \t]*\r?\n?/g, '');
  if (isFull) {
    // FULL: mask flowAccesses, userPermissions, and profileActionOverrides — all false positives.
    xml = xml.replace(/[ \t]*<flowAccesses>[\s\S]*?<\/flowAccesses>[ \t]*\r?\n?/g, '');
    xml = xml.replace(/[ \t]*<userPermissions>[\s\S]*?<\/userPermissions>[ \t]*\r?\n?/g, '');
    xml = xml.replace(/[ \t]*<profileActionOverrides>[\s\S]*?<\/profileActionOverrides>[ \t]*\r?\n?/g, '');
    // FULL: only mask __c/__mdt object permissions — keep standard, __b, __e exposed.
    const objInner = '(?:(?!<objectPermissions>)[\\s\\S])*?';
    xml = xml.replace(
      new RegExp(
        `[ \\t]*<objectPermissions>${objInner}<object>[^<]*(?:__c|__mdt)[^<]*</object>${objInner}</objectPermissions>[ \\t]*\\r?\\n?`,
        'g'
      ),
      ''
    );
    // FULL: mask ALL custom field permissions (field name ending in __c).
    // Covers both custom object fields (MyObj__c.Field__c) AND custom fields on standard/CRM
    // objects (Account.Business_Impact__c) — Copado TRIM strips all of these before real deploy.
    // Standard fields (Account.Name, AccountContactRelation.IsActive) are NOT masked so the
    // dry-run can detect and remove permissions for feature-gated standard objects.
    const fieldInner = '(?:(?!<fieldPermissions>)[\\s\\S])*?';
    xml = xml.replace(
      new RegExp(
        `[ \\t]*<fieldPermissions>${fieldInner}<field>[^<]*(?:__c|__mdt)[ \\t]*</field>${fieldInner}</fieldPermissions>[ \\t]*\\r?\\n?`,
        'g'
      ),
      ''
    );
    // FULL: mask core CRM object permissions and field permissions — these always exist in every
    // SF org and are false positives that block discovery of real errors during dry-run.
    xml = maskCoreCrmObjectPermissions(xml);
    xml = maskCoreCrmFieldPermissions(xml);
  } else {
    // ADD: mask ALL object permissions and ALL field permissions — Copado TRIM handles them.
    xml = xml.replace(/[ \t]*<objectPermissions>[\s\S]*?<\/objectPermissions>[ \t]*\r?\n?/g, '');
    xml = xml.replace(/[ \t]*<fieldPermissions>[\s\S]*?<\/fieldPermissions>[ \t]*\r?\n?/g, '');
  }
  xml = xml.replace(/[ \t]*<recordTypeVisibilities>[\s\S]*?<\/recordTypeVisibilities>[ \t]*\r?\n?/g, '');
  if (isFull) {
    // FULL + ADD: mask ALL layoutAssignments — Copado TRIM strips them before real deploy,
    // so any layout error in dry-run is a false positive regardless of object type.
  }
  xml = xml.replace(/[ \t]*<layoutAssignments>[\s\S]*?<\/layoutAssignments>[ \t]*\r?\n?/g, '');
  xml = xml.replace(/[ \t]*<tabVisibilities>[\s\S]*?<\/tabVisibilities>[ \t]*\r?\n?/g, '');
  xml = xml.replace(/[ \t]*<customMetadataTypeAccesses>[\s\S]*?<\/customMetadataTypeAccesses>[ \t]*\r?\n?/g, '');
  xml = xml.replace(/[ \t]*<customPermissions>[\s\S]*?<\/customPermissions>[ \t]*\r?\n?/g, ''); // confirmed: Copado TRIM strips these
  return xml;
}

export function maskPermSetFalsePositives(xmlContent: string, isFull = false): string {
  // ADD operation: no masking needed here (maskStandardApps handles standard apps separately).
  // FULL operation: Copado does NOT reliably auto-clean FULL PS — expose real errors so dry-run
  // catches them before Copado deployment. Only mask blocks that never cause "no X found" errors:
  //   flowAccesses  — no real Copado errors observed; Copado handles flow cleanup
  //   userPermissions — system permissions, never cause missing-component errors
  // Everything else (objectPermissions, fieldPermissions, classAccesses, pageAccesses,
  // tabSettings, recordTypeVisibilities, customPermissions, customMetadataTypeAccesses,
  // applicationVisibilities for custom/managed-package apps) is left exposed so the dry-run
  // surfaces the same errors that would fail in real Copado deployment.
  if (!isFull) return xmlContent;

  let xml = xmlContent;
  xml = xml.replace(/[ \t]*<flowAccesses>[\s\S]*?<\/flowAccesses>[ \t]*\r?\n?/g, '');
  xml = xml.replace(/[ \t]*<userPermissions>[\s\S]*?<\/userPermissions>[ \t]*\r?\n?/g, '');
  return xml;
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
  // Layout-specific blocks
  { blockTag: 'layoutItems', keyTag: 'field' },
  // relatedLists excluded: same <relatedList> key appears legitimately in both <miniLayout> and
  // top-level sections — dedup by key would wrongly remove the top-level block.
  { blockTag: 'platformActionListItems', keyTag: 'actionName' },
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
    updated = updated.replace(blockRegex, (match: string) => {
      // Normalize whitespace so minor indentation differences don't prevent dedup detection,
      // but compare full block content — two blocks with the same key but different values
      // (e.g. same object, different permission flags) are NOT duplicates and must both survive.
      const normalizedBlock = match.replace(/[ \t]+/g, ' ').trim();
      if (seen.has(normalizedBlock)) {
        removedCount++;
        return '';
      }
      seen.add(normalizedBlock);
      return match;
    });
  }

  return { updated, removedCount };
}

// ===============================================================
// MALFORMED TAG FIXER
// Handles two merge-artifact patterns that cause SF XML parse errors:
//
//   Pattern 1 — duplicate opening tag:
//     <classAccesses>
//     <classAccesses>        ← remove the second one
//         <apexClass>Foo</apexClass>
//
//   Pattern 2 — missing opening tag:
//     </classAccesses>
//         <apexClass>Bar</apexClass>   ← insert <classAccesses> before this
//         <enabled>true</enabled>
//     </classAccesses>
// ===============================================================

export function fixMalformedXmlTags(xmlContent: string): { updated: string; fixedCount: number } {
  let updated = xmlContent;
  let fixedCount = 0;
  const esc = (s: string): string => s.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');

  for (const { blockTag, keyTag } of DEDUP_BLOCKS) {
    const bt = esc(blockTag);
    const kt = esc(keyTag);

    // Pattern 1: duplicate consecutive opening tag on the very next line
    const dupOpen = new RegExp(`([ \\t]*<${bt}>[ \\t]*\\r?\\n)[ \\t]*<${bt}>`, 'g');
    updated = updated.replace(dupOpen, (_m, first: string) => {
      fixedCount++;
      return first;
    });

    // Pattern 2: missing opening tag — closing tag immediately followed by a child key element
    const missingOpen = new RegExp(`([ \\t]*)<\\/${bt}>([ \\t]*\\r?\\n)([ \\t]*)<${kt}>`, 'g');
    updated = updated.replace(missingOpen, (_m, closingIndent: string, nl: string, childIndent: string) => {
      fixedCount++;
      return `${closingIndent}</${blockTag}>${nl}${closingIndent}<${blockTag}>${nl}${childIndent}<${keyTag}>`;
    });
  }

  return { updated, fixedCount };
}

function readFileWithRetry(filePath: string, retries = 5, delayMs = 500): string {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return fs.readFileSync(filePath, 'utf8');
    } catch (e) {
      if (attempt === retries) throw e;
      // Windows file lock after git commit (CRLF rewrite / antivirus scan) — wait and retry
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, delayMs);
    }
  }
  return fs.readFileSync(filePath, 'utf8'); // unreachable, satisfies TS
}

// Adds each unhandled error to item.allUnhandledErrors and, for eligible metadata types,
// attempts to add an error-based mask so subsequent dry-runs can bypass the offending block.
// Returns true if at least one new mask was added (caller should treat this as progress).
function collectUnhandledErrors(errors: string[], item: BatchItem, log: (msg: string) => void): boolean {
  let addedAny = false;
  for (const e of errors) {
    if (!item.allUnhandledErrors.includes(e)) item.allUnhandledErrors.push(e);
    if (isEligibleForDynamicMasking(item) && tryAddErrorMask(e, item, log)) addedAny = true;
  }
  return addedAny;
}

// Dynamic error-based masking applies to Profile/PS/MutingPS only.
// PSG errors (e.g. license-dependent PS) cannot be auto-identified and are left for manual fix.
function isEligibleForDynamicMasking(item: BatchItem): boolean {
  return (
    item.metadataType === 'Profile' ||
    item.metadataType === 'PermissionSet' ||
    item.metadataType === 'MutingPermissionSet'
  );
}

function escapeRegexChars(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Tries to identify the XML block responsible for an unhandled error and adds a mask for it.
// Returns true if a new mask was added (i.e. the error is now handled by masking).
function tryAddErrorMask(err: string, item: BatchItem, log: (msg: string) => void): boolean {
  // ── IP login ranges ───────────────────────────────────────────────────────
  if (/limit.*ipv[46]|ipv[46].*limit|ip\s*range/i.test(err)) {
    const label = 'loginIpRanges';
    if (item.errorBasedMasks.some((m) => m.label === label)) return false;
    item.errorBasedMasks.push({
      xmlPattern: /[ \t]*<loginIpRanges>[\s\S]*?<\/loginIpRanges>[ \t]*\r?\n?/g,
      label,
      reason: err,
    });
    log('   [Dynamic Mask] Masking <loginIpRanges> block — needs manual fix after run');
    return true;
  }

  // ── Object-flag PermDep: "Permission Edit/Read/... ObjectName depends on ..." ──
  // Mask ALL <objectPermissions> blocks for that object so validation can continue
  // and surface other errors. Error is already in unhandledErrors — user will see it.
  const objPermDepMatch =
    /Permission\s+(?:View All|Modify All|Read|Edit|Create|Delete)\s+(\S+)\s+depends\s+on\s+permission/i.exec(err);
  if (objPermDepMatch) {
    const objectName = objPermDepMatch[1].trim();
    const label = `objectPermDep:${objectName}`;
    if (item.errorBasedMasks.some((m) => m.label === label)) return false;
    const escapedObj = escapeRegexChars(objectName);
    const inner = '(?:(?!<objectPermissions>)[\\s\\S])*?';
    item.errorBasedMasks.push({
      xmlPattern: new RegExp(
        `[ \\t]*<objectPermissions>${inner}<object>[ \\t]*${escapedObj}[ \\t]*</object>${inner}</objectPermissions>[ \\t]*\\r?\\n?`,
        'g'
      ),
      label,
      reason: err,
    });
    log(`   [Dynamic Mask] Masking <objectPermissions> for '${objectName}' — PermDep, needs manual fix after run`);
    return true;
  }

  // ── System userPermission PermDep: "Permission ViewAllData depends on ..." ──
  // Mask the <userPermissions> block so validation can continue.
  // Error is already in unhandledErrors — user will see it.
  const permMatch = err.match(/Permission\s+(.+?)\s+depends\s+on\s+permission/i);
  if (permMatch) {
    const permName = permMatch[1].trim();
    const permNameNoSpaces = permName.replace(/\s+/g, '');
    const label = `userPermission:${permNameNoSpaces}`;
    if (item.errorBasedMasks.some((m) => m.label === label)) return false;
    // Match both "View All Data" and "ViewAllData" forms inside <name>...</name>
    const nameAlt =
      permNameNoSpaces !== permName
        ? `(?:${escapeRegexChars(permName)}|${permNameNoSpaces})`
        : escapeRegexChars(permName);
    const inner = '(?:(?!<userPermissions>)[\\s\\S])*?';
    item.errorBasedMasks.push({
      xmlPattern: new RegExp(
        `[ \\t]*<userPermissions>${inner}<name>[ \\t]*${nameAlt}[ \\t]*</name>${inner}</userPermissions>[ \\t]*\\r?\\n?`,
        'g'
      ),
      label,
      reason: err,
    });
    log(`   [Dynamic Mask] Masking <userPermissions> block for '${permName}' — needs manual fix after run`);
    return true;
  }

  return false; // unknown error — cannot auto-mask
}

function maskActiveItems(activeItems: BatchItem[], whitelist: WhitelistMap): Map<string, string> {
  const saved = new Map<string, string>();
  for (const item of activeItems) {
    if (!fs.existsSync(item.filePath)) continue;
    // ReportType files have no false-positive masking — skip save/restore so committed
    // fixes persist on disk across iterations rather than being overwritten by restoreItems.
    if (item.filePath.toLowerCase().endsWith('.reporttype-meta.xml')) continue;
    const orig = readFileWithRetry(item.filePath);
    let masked = maskWhitelistedEntries(orig, whitelist);
    if (item.filePath.endsWith('.profile-meta.xml')) {
      masked = maskProfileFalsePositives(masked, item.operation === 'FULL');
    }
    if (item.filePath.endsWith('.permissionset-meta.xml') || item.filePath.endsWith('.mutingpermissionset-meta.xml')) {
      masked = maskStandardApps(masked);
      masked = maskPermSetFalsePositives(masked, item.operation === 'FULL');
    }
    // PSG files: no false-positive masking needed — structure is simple and fixed by fixPsgPermissionSetsBlock

    // Apply dynamic error-based masks accumulated from previous iterations (Profile/PS/MutingPS only).
    if (isEligibleForDynamicMasking(item)) {
      for (const mask of item.errorBasedMasks) {
        masked = masked.replace(mask.xmlPattern, '');
      }
    }

    saved.set(item.filePath, orig);
    if (masked !== orig) writeFileWithRetry(item.filePath, masked);
  }
  return saved;
}

function writeFileWithRetry(filePath: string, content: string, retries = 5, delayMs = 500): void {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      fs.writeFileSync(filePath, content, 'utf8');
      return;
    } catch (e) {
      if (attempt === retries) throw e;
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, delayMs);
    }
  }
}

function restoreItems(saved: Map<string, string>): void {
  for (const [filePath, content] of saved) {
    writeFileWithRetry(filePath, content);
  }
}

// ===============================================================
// BATCH DEPLOY HELPERS
// Extracted to keep runBatchDeploy under the complexity limit.
// ===============================================================

function mergeMapFirst<K, V>(dest: Map<K, V>, src: Map<K, V>): void {
  for (const [k, v] of src) {
    if (!dest.has(k)) dest.set(k, v);
  }
}

function appendUnique(arr: string[], items: string[]): void {
  for (const item of items) {
    if (!arr.includes(item)) arr.push(item);
  }
}

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
  dryRun: boolean,
  batchItemsByPath?: Map<string, BatchItem>
): void {
  for (const [sourceFilePath, refs] of perItemRefs) {
    sweepOtherFiles(log, refs, new Set([sourceFilePath]), allFilePaths, repoPath, dryRun, batchItemsByPath);
  }
}

// Minimum consecutive zero-failure responses (while success=false) before an item is
// declared clean. Salesforce's error reporting is non-deterministic — a single zero-failure
// response after many fixes is not reliable evidence that all errors are gone.
const ZERO_FAILURE_CONFIRM = 2;

// Returns true if the batch should stop because there are undone items but no real progress
// and none of them are in re-verification state (waiting for a second zero-failure confirmation).
function isStuckWithNoProgress(batchItems: BatchItem[]): boolean {
  const hasUndone = batchItems.some((i) => !i.done);
  const hasReVerifying = batchItems.some((i) => !i.done && i.consecutiveZeroFailures > 0);
  return hasUndone && !hasReVerifying;
}

function markPassedItems(
  log: (msg: string) => void,
  activeItems: BatchItem[],
  failuresByItem: Map<string, ComponentFailure[]>,
  dryRun: boolean
): void {
  for (const item of activeItems) {
    if ((failuresByItem.get(item.itemName) ?? []).length === 0) {
      item.consecutiveZeroFailures++;
      if (item.consecutiveZeroFailures < ZERO_FAILURE_CONFIRM) {
        log(
          `   [${item.itemName}] No failures this iteration — re-verifying (${item.consecutiveZeroFailures}/${ZERO_FAILURE_CONFIRM})...`
        );
      } else {
        const hasFixed = item.allRemovedFields.length > 0;
        const hasUnhandled = item.allUnhandledErrors.length > 0;
        log(
          `   [${item.itemName}] No failures this iteration — passed${
            hasUnhandled ? ' (with unhandled/skipped errors — see report)' : ''
          }.`
        );
        if (hasFixed && hasUnhandled) {
          item.status = dryRun ? 'Fixed (Dry Run) + Unhandled Errors' : 'Fixed & Committed + Unhandled Errors';
        } else if (hasFixed) {
          item.status = dryRun ? 'Fixed (Dry Run)' : 'Fixed & Committed';
        } else if (hasUnhandled) {
          item.status = 'Unhandled Errors - Manual Fix Needed';
        } else {
          item.status = 'Success';
        }
        item.done = true;
      }
    } else {
      item.consecutiveZeroFailures = 0;
    }
  }
}

type NsResult = { nsXml: string; nsRefs: RemovedRef[]; rootNode: string };

// Applies namespace pre-check to all active items, stages and commits them together.
// Returns a map of per-item NS results and the managed-refs commit hash (if any).
async function handlePsgLockItem(
  log: (msg: string) => void,
  item: BatchItem,
  failure: ComponentFailure,
  targetOrg: string
): Promise<void> {
  const psgNameMatch = (failure.problem ?? failure.error ?? '').match(
    /The\s+(\w+)\s+permission set group is updating/i
  );
  const blockingPsg = psgNameMatch ? psgNameMatch[1] : 'a Permission Set Group';
  log(
    `   [PSG] ${item.itemName} — ⏳ "${blockingPsg}" is updating. Waiting for recalculation to complete before retrying...`
  );
  const psgResult = await waitForPsgUpdates(log, item.itemName, targetOrg);
  if (psgResult === 'updated') {
    item.status = 'Pending';
  } else if (psgResult === 'calc-failed') {
    item.calcFailedRetries++;
    if (item.calcFailedRetries <= 3) {
      log(`   [PSG] ${item.itemName} — CalculationFailed retry ${item.calcFailedRetries}/3. Re-deploying...`);
      item.status = 'Pending';
    } else {
      log(`   [PSG] ${item.itemName} — CalculationFailed after 3 retries. Giving up.`);
      item.status = 'Skipped - PSG Calculation Failed';
      item.done = true;
    }
  } else {
    item.status = 'Skipped - PSG Update Failed';
    item.done = true;
  }
}

async function handlePsgInvalidPsItem(
  log: (msg: string) => void,
  item: BatchItem,
  targetOrg: string,
  repoPath: string,
  promotionData: PromotionItem[]
): Promise<void> {
  log(`\n   [PSG] ${item.itemName} — invalid <permissionSets> reference(s) detected. Analyzing...`);

  const psgXml = readFileWithRetry(item.filePath);
  const tagRegex = /[ \t]*<permissionSets>([^<]*)<\/permissionSets>/g;
  const allPsRefs: string[] = [];
  let m: RegExpExecArray | null;
  // eslint-disable-next-line no-cond-assign
  while ((m = tagRegex.exec(psgXml)) !== null) {
    const name = m[1].trim();
    if (name) allPsRefs.push(name);
  }

  if (allPsRefs.length === 0) {
    log(`   [PSG] ${item.itemName} — no <permissionSets> refs found in XML. Cannot auto-fix.`);
    item.done = true;
    return;
  }

  log(`   [PSG] ${item.itemName} — refs in XML (${allPsRefs.length}): ${allPsRefs.join(', ')}`);

  const existingInOrg = await queryExistingPermSets(targetOrg, allPsRefs);
  const existingSet = new Set(existingInOrg.map((n) => n.toLowerCase()));
  const missingInOrg = allPsRefs.filter((n) => !existingSet.has(n.toLowerCase()));

  if (missingInOrg.length === 0) {
    log(
      `   [PSG] ${item.itemName} — all referenced PermSets exist in target org. Cannot auto-fix — manual intervention needed.`
    );
    item.done = true;
    return;
  }

  log(`   [PSG] ${item.itemName} — not found by Name query: ${missingInOrg.join(', ')}`);

  // For managed-package PS refs (e.g. "whistic_profile__whistic_user"), SOQL Name field
  // stores only the DeveloperName portion ("whistic_user") — the namespace prefix is absent.
  // So a SOQL Name query returns nothing for them. If the namespace is installed, the PS
  // exists — do not remove it.
  const confirmedMissing: string[] = [];
  for (const psName of missingInOrg) {
    // Namespace prefixes CAN contain underscores (e.g. "whistic_profile__whistic_user").
    // Use [A-Za-z0-9_]* so "whistic_profile" is captured as the namespace, not just "whistic".
    const nsMatch = /^([A-Za-z][A-Za-z0-9_]*)__/.exec(psName);
    if (nsMatch) {
      const namespace = nsMatch[1];
      // eslint-disable-next-line no-await-in-loop
      const installed = await checkNamespaceInstalled(log, targetOrg, namespace);
      if (installed) {
        log(`   [PSG] ${item.itemName} — keeping '${psName}' (namespace '${namespace}' is installed in org)`);
        continue;
      }
    }
    confirmedMissing.push(psName);
  }

  if (confirmedMissing.length === 0) {
    log(`   [PSG] ${item.itemName} — all unresolved refs belong to installed namespaces. No removal needed.`);
    item.status = 'Pending';
    return;
  }

  log(
    `   [PSG] ${item.itemName} — confirmed missing (not in org, not a managed package): ${confirmedMissing.join(', ')}`
  );

  const beingAdded = confirmedMissing.filter((n) =>
    promotionData.some(
      (i) => i.t === 'PermissionSet' && i.n === n && (!i.a || !i.a.toLowerCase().startsWith('retrieve'))
    )
  );
  const beingAddedLower = new Set(beingAdded.map((n) => n.toLowerCase()));
  const toRemove = confirmedMissing.filter((n) => !beingAddedLower.has(n.toLowerCase()));

  if (beingAdded.length > 0) {
    log(`   [PSG] ${item.itemName} — keeping refs (being added in this promotion): ${beingAdded.join(', ')}`);
  }

  if (toRemove.length === 0) {
    log(
      `   [PSG] ${item.itemName} — all missing PermSets are being added in this promotion. Retrying after they deploy.`
    );
    item.status = 'Pending';
    return;
  }

  log(
    `   [PSG] ${item.itemName} — removing ${toRemove.length} ref(s) not in org and not in promotion: ${toRemove.join(
      ', '
    )}`
  );

  const toRemoveSet = new Set(toRemove.map((n) => n.toLowerCase()));
  const removeRegex = /[ \t]*<permissionSets>([^<]*)<\/permissionSets>[ \t]*\r?\n?/g;
  const updatedPsgXml = psgXml.replace(removeRegex, (full, name: string) =>
    toRemoveSet.has(name.trim().toLowerCase()) ? '' : full
  );

  saveXmlPreserved(updatedPsgXml, item.filePath);
  item.allRemovedFields.push(
    ...toRemove.map((n) => ({
      label: `<permissionSets>${n}</permissionSets>`,
      error: 'Invalid PSG ref: not in org and not in promotion',
    }))
  );

  try {
    execSync(`git add "${item.filePath}"`, { cwd: repoPath });
    execSync(
      `git commit -m "[${item.itemName}] Remove invalid PSG refs (not in org, not in promotion): ${toRemove.join(
        ', '
      )}"`,
      { cwd: repoPath }
    );
    log(`   [PSG] ${item.itemName} — committed. Will retry deployment.`);
    item.status = 'Pending';
  } catch (err) {
    log(`   [PSG] ${item.itemName} — git commit failed: ${(err as Error).message ?? err}. Skipping.`);
    item.done = true;
  }
}

// When 43 PS files deploy together and only 1 fails with an OmbudApp error,
// the per-item loop bulk-removes OmbudApp refs from that 1 file only. The other
// 42 files also contain OmbudApp refs but had no failures yet — each subsequent
// deploy iteration exposes one more, causing N iterations for N files instead of 1.
// This function runs after the per-item pass and sweeps ALL active items for every
// namespace already confirmed not installed in the org (via namespaceCache).
function applyCrossItemNamespaceSweep(
  log: (msg: string) => void,
  activeItems: BatchItem[],
  itemNsResults: Map<string, NsResult>,
  nsOriginalXmlMap: Map<string, string>,
  whitelist: WhitelistMap,
  targetOrg: string,
  repoPath: string,
  startingCommit: string,
  dryRun: boolean,
  onStaged: (staged: boolean) => void
): void {
  const confirmedUninstalledNs = new Set<string>();
  for (const [key, installed] of namespaceCache) {
    if (!installed && key.startsWith(`${targetOrg}:`)) {
      confirmedUninstalledNs.add(key.slice(targetOrg.length + 1));
    }
  }
  if (confirmedUninstalledNs.size === 0) return;

  for (const item of activeItems) {
    if (item.done) continue;

    const existingResult = itemNsResults.get(item.filePath);
    const baseXml = existingResult?.nsXml ?? readFileWithRetry(item.filePath);
    const rootNode = existingResult?.rootNode ?? getRootNodeName(baseXml);
    let sweepXml = baseXml;
    const sweepRefs: RemovedRef[] = existingResult?.nsRefs ? [...existingResult.nsRefs] : [];
    let sweptAny = false;

    for (const ns of confirmedUninstalledNs) {
      if (sweepRefs.some((r) => r.name === ns)) continue; // already swept in per-item pass

      const hasWhitelisted = Object.values(whitelist)
        .flat()
        .some((v) => v.startsWith(`${ns}__`) || v.includes(`.${ns}__`));
      if (hasWhitelisted) continue;

      const { updated, removed } = resolveNsRemoval(sweepXml, ns, item.filePath);
      if (removed) {
        sweepXml = updated;
        sweepRefs.push({
          type: 'namespace',
          name: ns,
          label: `[NS:${ns}] bulk-removed`,
          deployError: `Namespace ${ns} not installed in org — cross-item sweep`,
        });
        log(`   [NS Sweep] Removed ALL ${ns}__ refs from ${item.itemName}`);
        sweptAny = true;
      }
    }

    if (!sweptAny) continue;

    itemNsResults.set(item.filePath, { nsXml: sweepXml, nsRefs: sweepRefs, rootNode });

    try {
      const relPath = path.relative(repoPath, item.filePath).replace(/\\/g, '/');
      const originalContent = execSync(`git show "${startingCommit}:${relPath}"`, { cwd: repoPath }).toString();
      let nsOnlyContent = originalContent;
      for (const ref of sweepRefs) {
        const result = applyRefToXml(nsOnlyContent, ref, item.filePath);
        if (result?.removed) nsOnlyContent = result.updated;
      }
      nsOriginalXmlMap.set(item.filePath, nsOnlyContent);
    } catch {
      /* file may not exist in git at startingCommit — skip */
    }

    if (!dryRun) {
      saveXmlPreserved(sweepXml, item.filePath);
      try {
        execSync(`git add "${item.filePath}"`, { cwd: repoPath });
        onStaged(true);
      } catch {
        log(`   git add failed for namespace sweep: ${item.itemName}`);
      }
    }
  }
}

async function applyManagedRefsPass(
  log: (msg: string) => void,
  activeItems: BatchItem[],
  failuresByItem: Map<string, ComponentFailure[]>,
  whitelist: WhitelistMap,
  targetOrg: string,
  repoPath: string,
  vlog: (msg: string) => void,
  dryRun: boolean,
  promotionData: PromotionItem[],
  startingCommit: string
): Promise<{
  itemNsResults: Map<string, NsResult>;
  lastManagedRefsCommit: string | null;
  nsModifiedFiles: string[];
  nsCommitMsg: string | null;
  nsOriginalXmlMap: Map<string, string>;
}> {
  const PSG_LOCK = /permission set group is updating/i;
  const PSG_INVALID_PS =
    /Cannot create permission set group components since the following permission set names are invalid/i;
  const itemNsResults = new Map<string, NsResult>();
  const nsOriginalXmlMap = new Map<string, string>();
  let managedRefsStaged = false;

  for (const item of activeItems) {
    if (item.done) continue;
    const itemFailures = failuresByItem.get(item.itemName) ?? [];
    if (itemFailures.length === 0) continue;

    const psgLockFailure = itemFailures.find((f) => PSG_LOCK.test(f.problem ?? f.error ?? ''));
    if (psgLockFailure) {
      // eslint-disable-next-line no-await-in-loop
      await handlePsgLockItem(log, item, psgLockFailure, targetOrg);
      continue;
    }

    const psgInvalidPsFailure = itemFailures.find((f) => PSG_INVALID_PS.test(f.problem ?? f.error ?? ''));
    if (psgInvalidPsFailure && item.metadataType === 'PermissionSetGroup') {
      // eslint-disable-next-line no-await-in-loop
      await handlePsgInvalidPsItem(log, item, targetOrg, repoPath, promotionData);
      continue;
    }

    log(`\n   [${item.itemName}] ${itemFailures.length} failure(s):`);
    itemFailures.forEach((f, i) => vlog(`   [DEBUG] Failure ${i + 1}: ${f.problem ?? f.error ?? ''}`));

    const xmlContent = readFileWithRetry(item.filePath);
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

    if (nsRefs.length > 0) {
      // Compute NS-only content: start from the file's original state (before any
      // missing-ref commits from earlier iterations) and apply ONLY the NS refs.
      // This gives a clean NS-only diff when the squash re-commits it.
      try {
        const relPath = path.relative(repoPath, item.filePath).replace(/\\/g, '/');
        const originalContent = execSync(`git show "${startingCommit}:${relPath}"`, { cwd: repoPath }).toString();
        let nsOnlyContent = originalContent;
        for (const ref of nsRefs) {
          const result = applyRefToXml(nsOnlyContent, ref, item.filePath);
          if (result?.removed) nsOnlyContent = result.updated;
        }
        nsOriginalXmlMap.set(item.filePath, nsOnlyContent);
      } catch {
        /* file may not exist in git at startingCommit — skip */
      }

      if (!dryRun) {
        saveXmlPreserved(nsXml, item.filePath);
        try {
          execSync(`git add "${item.filePath}"`, { cwd: repoPath });
          managedRefsStaged = true;
        } catch {
          log(`   git add failed for managed refs: ${item.itemName}`);
        }
      }
    }
  }

  // ── Cross-item namespace sweep ──────────────────────────────────────────────
  // Sweep ALL active items for every namespace confirmed not installed above.
  // Extracted to keep applyManagedRefsPass under the complexity limit.
  applyCrossItemNamespaceSweep(
    log,
    activeItems,
    itemNsResults,
    nsOriginalXmlMap,
    whitelist,
    targetOrg,
    repoPath,
    startingCommit,
    dryRun,
    (staged) => {
      managedRefsStaged = managedRefsStaged || staged;
    }
  );

  if (!dryRun && managedRefsStaged) {
    const staged = [...itemNsResults.values()].filter((v) => v.nsRefs.length > 0);
    const totalLabels = staged.flatMap((v) => v.nsRefs.map((r) => r.label));
    const labelSummary = totalLabels.slice(0, 8).join(', ') + (totalLabels.length > 8 ? '...' : '');
    const nsCommitMsg = `Remove managed package refs (${totalLabels.length} ref(s) in ${staged.length} file(s)): ${labelSummary}`;
    const nsModifiedFiles = [...itemNsResults.entries()].filter(([, v]) => v.nsRefs.length > 0).map(([k]) => k);
    try {
      execSync(`git commit -m "${nsCommitMsg}"`, { cwd: repoPath });
      const lastCommit = execSync('git rev-parse HEAD', { cwd: repoPath }).toString().trim();
      log(`   Committed managed package ref removals: ${staged.length} file(s), ${totalLabels.length} ref(s)`);
      return { itemNsResults, lastManagedRefsCommit: lastCommit, nsModifiedFiles, nsCommitMsg, nsOriginalXmlMap };
    } catch {
      log('   Managed refs commit failed or nothing staged');
    }
  }

  return { itemNsResults, lastManagedRefsCommit: null, nsModifiedFiles: [], nsCommitMsg: null, nsOriginalXmlMap };
}

// Saves and commits (or dry-runs) the missing-ref XML for one item.
function commitItemMissingRefs(
  log: (msg: string) => void,
  item: BatchItem,
  updatedXml: string,
  missingRefs: RemovedRef[],
  repoPath: string,
  dryRun: boolean
): void {
  if (dryRun) {
    saveXmlPreserved(updatedXml, item.filePath);
    log(`   Dry run — skipped commit for: ${item.itemName}`);
    // eslint-disable-next-line no-param-reassign
    item.status = 'Fixed (Dry Run)';
  } else if (missingRefs.length > 0) {
    saveXmlPreserved(updatedXml, item.filePath);
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

// Squashes all per-item commits into at most two clean commits: one for NS/managed-ref
// changes, one for all missing-ref removals. Always resets to startingCommit so
// individual per-item commits are never left in history regardless of iteration order.
//
// For files that had BOTH NS and missing-ref changes in the same file, we use
// a pre-computed nsOriginalXmlMap (original file + NS refs applied, no missing-ref
// removals) so the NS commit is always clean regardless of iteration order.
function squashMissingRefCommits(
  log: (msg: string) => void,
  startingCommit: string,
  nsModifiedFiles: string[],
  nsCommitMsg: string | null,
  nsOriginalXmlMap: Map<string, string>,
  summary: SummaryRecord[],
  repoPath: string
): void {
  try {
    const currentHead = execSync('git rev-parse HEAD', { cwd: repoPath }).toString().trim();
    if (currentHead === startingCommit) {
      log('\nNo commits to squash.');
      return;
    }

    // All files changed by this run
    const allChangedFiles = execSync(`git diff --name-only "${startingCommit}" HEAD`, { cwd: repoPath })
      .toString()
      .trim()
      .split('\n')
      .filter(Boolean);
    if (allChangedFiles.length === 0) {
      log('\nNo changed files — nothing to squash.');
      return;
    }

    // Reset all commits from this run — all changes land in the staging area
    execSync(`git reset --soft "${startingCommit}"`, { cwd: repoPath });

    const hasNsFiles = nsModifiedFiles.length > 0 && !!nsCommitMsg && nsOriginalXmlMap.size > 0;
    const nonNsFiles = hasNsFiles ? allChangedFiles.filter((f) => !nsModifiedFiles.includes(f)) : allChangedFiles;

    if (hasNsFiles) {
      // Phase 1: NS commit — write each NS file's true NS-only state (original content
      // with NS refs removed, without any missing-ref removals from other iterations).
      // nsOriginalXmlMap was computed in applyManagedRefsPass from the startingCommit
      // file content, so it is always clean regardless of iteration order.
      execSync('git reset HEAD', { cwd: repoPath }); // unstage all
      const finalContents = new Map<string, string>();
      for (const f of nsModifiedFiles) {
        const nsOnlyContent = nsOriginalXmlMap.get(f);
        if (!nsOnlyContent) continue;
        try {
          finalContents.set(f, fs.readFileSync(f, 'utf8')); // save final state for phase 2
          fs.writeFileSync(f, nsOnlyContent, 'utf8'); // write NS-only state
          execSync(`git add "${f}"`, { cwd: repoPath });
        } catch {
          /* skip */
        }
      }
      execSync(`git commit -m "${nsCommitMsg}"`, { cwd: repoPath });
      log(`\nRe-committed NS/managed-ref changes: "${nsCommitMsg}"`);

      // Restore final content for NS files so phase 2 can diff them correctly
      for (const [f, content] of finalContents) {
        try {
          fs.writeFileSync(f, content, 'utf8');
          execSync(`git add "${f}"`, { cwd: repoPath });
        } catch {
          /* skip */
        }
      }
    }

    // Phase 2: Auto-fix commit — remaining missing-ref files (+ NS files' final→NS-commit diff)
    for (const f of nonNsFiles) {
      try {
        execSync(`git add "${f}"`, { cwd: repoPath });
      } catch {
        /* file may be gone */
      }
    }

    // Only commit if there is actually something staged (NS files may already be fully committed)
    const stagedFiles = execSync('git diff --cached --name-only', { cwd: repoPath })
      .toString()
      .trim()
      .split('\n')
      .filter(Boolean);
    if (stagedFiles.length > 0) {
      const missingRemoved = summary.flatMap((r) => (r.RemovedFields ? r.RemovedFields.split('; ') : []));
      const squashMsg = `Auto-fix: remove ${missingRemoved.length} missing ref(s) across ${stagedFiles.length} file(s)`;
      execSync(`git commit -m "${squashMsg}"`, { cwd: repoPath });
      log(`\nSquashed missing-ref commits into one: "${squashMsg}"`);
    }
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
  dryRun: boolean,
  promotionData: PromotionItem[],
  startingCommit: string
): Promise<{
  perItemRefs: Map<string, RemovedRef[]>;
  anyProgress: boolean;
  lastManagedRefsCommit: string | null;
  nsModifiedFiles: string[];
  nsCommitMsg: string | null;
  nsOriginalXmlMap: Map<string, string>;
}> {
  const vlog: (msg: string) => void = verbose ? log : (): void => {};
  // Track refs per source file so the sweep skips only the source file, not all modified files.
  const perItemRefs = new Map<string, RemovedRef[]>();
  let anyProgress = false;

  // ── Pass 1: Namespace / managed-package pre-check ────────────────────────
  // Apply to ALL active items first and commit them ONCE before any missing-ref
  // commits — this gives a clean 4-commit git history.
  // eslint-disable-next-line no-await-in-loop
  const { itemNsResults, lastManagedRefsCommit, nsModifiedFiles, nsCommitMsg, nsOriginalXmlMap } =
    await applyManagedRefsPass(
      log,
      activeItems,
      failuresByItem,
      whitelist,
      targetOrg,
      repoPath,
      vlog,
      dryRun,
      promotionData,
      startingCommit
    );

  // ── Pass 2: Missing-ref removals (per item) ───────────────────────────────
  for (const item of activeItems) {
    if (item.done) continue;
    const itemFailures = failuresByItem.get(item.itemName) ?? [];
    if (itemFailures.length === 0) continue;

    const nsResult = itemNsResults.get(item.filePath);
    const baseXml = nsResult?.nsXml ?? readFileWithRetry(item.filePath);
    const nsRefs = nsResult?.nsRefs ?? [];

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
    } = processFailures(
      log,
      itemFailures,
      objXml,
      whitelist,
      item.allSkippedFields,
      item.metadataType,
      verbose,
      promotionData
    );

    const missingRefs = [...rtRefs, ...objRefs, ...perFailureRefs];
    const removedRefs = [...nsRefs, ...missingRefs];

    for (const ref of missingRefs) {
      if (!item.allRemovedFields.some((r) => r.label === ref.label)) {
        item.allRemovedFields.push({ label: ref.label, error: ref.deployError ?? '' });
        item.allRemovedRefs.push(ref);
      }
    }

    const addedNewMasks = collectUnhandledErrors(unhandledErrors, item, log);

    if (removedRefs.length === 0) {
      if (addedNewMasks) {
        // New masks were added — next iteration deploys without the offending block,
        // potentially surfacing other real errors. Treat as progress so the loop continues.
        anyProgress = true;
        continue;
      }
      // If every failure was a validation-only ignored error (PermDep, TabSettings, etc.)
      // — nothing to remove, nothing unhandled, nothing skipped — Copado real deploy won't
      // fail on these. Treat the item as passed rather than stuck.
      if (itemFailures.length > 0 && unhandledErrors.length === 0 && skippedFields.length === 0) {
        item.status = 'Success';
        item.done = true;
        log(`   [${item.itemName}] All failures were validation-only (ignored by Copado) — marking as passed`);
        continue;
      }
      item.status =
        skippedFields.length > 0 ? 'Whitelisted Items Only - Manual Deploy Needed' : 'Partial / Manual Check Needed';
      item.done = true;
      continue;
    }

    anyProgress = true;
    perItemRefs.set(item.filePath, removedRefs);
    commitItemMissingRefs(log, item, updatedXml, missingRefs, repoPath, dryRun);
  }

  return { perItemRefs, anyProgress, lastManagedRefsCommit, nsModifiedFiles, nsCommitMsg, nsOriginalXmlMap };
}

function markSuccessItems(items: BatchItem[], dryRun: boolean): void {
  for (const item of items) {
    const hasFixed = item.allRemovedFields.length > 0;
    const hasUnhandled = item.allUnhandledErrors.length > 0;
    if (hasFixed && hasUnhandled) {
      item.status = dryRun ? 'Fixed (Dry Run) + Unhandled Errors' : 'Fixed & Committed + Unhandled Errors';
    } else if (hasFixed) {
      item.status = dryRun ? 'Fixed (Dry Run)' : 'Fixed & Committed';
    } else if (hasUnhandled) {
      item.status = 'Unhandled Errors - Manual Fix Needed';
    } else {
      item.status = 'Success';
    }
    item.done = true;
  }
}

function preRunDeduplicateBlocks(
  log: (msg: string) => void,
  batchItems: BatchItem[],
  repoPath: string,
  dryRun: boolean
): void {
  const modifiedFiles: string[] = [];
  let totalRemoved = 0;
  let totalTagFixes = 0;
  for (const item of batchItems) {
    if (!fs.existsSync(item.filePath)) continue;
    const srcXml = readFileWithRetry(item.filePath);
    const { updated: afterDedup, removedCount } = deduplicateXmlBlocks(srcXml);
    const { updated: afterTagFix, fixedCount } = fixMalformedXmlTags(afterDedup);
    if (removedCount > 0 || fixedCount > 0) {
      writeFileWithRetry(item.filePath, afterTagFix);
      modifiedFiles.push(item.filePath);
      totalRemoved += removedCount;
      totalTagFixes += fixedCount;
      const parts: string[] = [];
      if (removedCount > 0) parts.push(`removed ${removedCount} exact duplicate block(s)`);
      if (fixedCount > 0) parts.push(`fixed ${fixedCount} malformed tag(s)`);
      log(`[Pre-run] ${item.itemName}: ${parts.join(', ')}`);
    }
  }
  if (modifiedFiles.length === 0) return;
  if (dryRun) {
    log(`[Pre-run] Dry run — skipped commit for ${modifiedFiles.length} file(s).`);
    return;
  }
  try {
    for (const f of modifiedFiles) execSync(`git add "${f}"`, { cwd: repoPath });
    const parts: string[] = [];
    if (totalRemoved > 0) parts.push(`remove ${totalRemoved} exact duplicate block(s)`);
    if (totalTagFixes > 0) parts.push(`fix ${totalTagFixes} malformed XML tag(s)`);
    execSync(`git commit -m "Auto-fix: ${parts.join(', ')} across ${modifiedFiles.length} file(s)"`, { cwd: repoPath });
    log(`[Pre-run] Committed: ${parts.join(', ')} across ${modifiedFiles.length} file(s).`);
  } catch (e) {
    log(`[Pre-run] Warning: pre-run fix commit failed — ${String(e)}`);
  }
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
  dryRun: boolean,
  promotionData: PromotionItem[],
  startingCommit: string
): Promise<{
  summary: SummaryRecord[];
  lastManagedRefsCommit: string | null;
  allNsModifiedFiles: string[];
  lastNsCommitMsg: string | null;
  nsOriginalXmlMap: Map<string, string>;
  postDedupCommit: string;
}> {
  validateBatchItems(log, batchItems);
  preRunDeduplicateBlocks(log, batchItems, repoPath, dryRun);
  // Re-read HEAD after the dedup commit so the squash baseline excludes dedup changes.
  // Without this, git reset --soft startingCommit would undo the dedup commit too,
  // folding exact-duplicate removals into the missing-ref commit.
  const postDedupCommit = dryRun ? startingCommit : execSync('git rev-parse HEAD', { cwd: repoPath }).toString().trim();

  const MAX_EMPTY_RETRIES = 5;
  let consecutiveEmptyRetries = 0;
  let iteration = 0;
  let overallLastManagedRefsCommit: string | null = null;
  const allNsModifiedFiles: string[] = [];
  let lastNsCommitMsg: string | null = null;
  const allNsOriginalXmlMap = new Map<string, string>();
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
      markSuccessItems(activeItems, dryRun);
      break;
    }

    const failures = deployResult.result.details?.componentFailures;
    if (!failures?.length) {
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

    const { perItemRefs, anyProgress, lastManagedRefsCommit, nsModifiedFiles, nsCommitMsg, nsOriginalXmlMap } =
      // eslint-disable-next-line no-await-in-loop
      await processItemsInIteration(
        log,
        activeItems,
        failuresByItem,
        whitelist,
        targetOrg,
        repoPath,
        verbose,
        dryRun,
        promotionData,
        postDedupCommit
      );

    if (lastManagedRefsCommit) overallLastManagedRefsCommit = lastManagedRefsCommit;
    appendUnique(allNsModifiedFiles, nsModifiedFiles);
    if (nsCommitMsg) lastNsCommitMsg = nsCommitMsg;
    mergeMapFirst(allNsOriginalXmlMap, nsOriginalXmlMap);

    if (!anyProgress && isStuckWithNoProgress(batchItems)) {
      stopBatchOnNoProgress(log, batchItems);
      break;
    }

    // Sweep each item's removed refs to ALL other files except that item's own file.
    // This ensures e.g. Profile B gets swept even if it was also modified this iteration
    // for a different error — it would otherwise be missed if we skipped all modified files.
    const batchItemsByPath = new Map<string, BatchItem>(batchItems.map((i) => [i.filePath, i]));
    sweepPerItemRefs(log, perItemRefs, allFilePaths, repoPath, dryRun, batchItemsByPath);
  }

  if (fs.existsSync(deployErrorsFile)) fs.unlinkSync(deployErrorsFile);

  const summary: SummaryRecord[] = batchItems.map((item) => ({
    Type: item.metadataType,
    Name: item.itemName,
    Op: item.operation,
    Status: item.status,
    RemovedFields: item.allRemovedFields.map((r) => r.label).join('; '),
    RemovedErrors: item.allRemovedFields.map((r) => r.error).join('; '),
    SkippedFields: item.allSkippedFields.join('; '),
    UnhandledErrors: item.allUnhandledErrors.join('; '),
  }));
  return {
    summary,
    lastManagedRefsCommit: overallLastManagedRefsCommit,
    allNsModifiedFiles,
    lastNsCommitMsg,
    nsOriginalXmlMap: allNsOriginalXmlMap,
    postDedupCommit,
  };
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

function logItemList(
  log: (msg: string) => void,
  names: string[],
  promotionData: PromotionItem[],
  metadataType: string
): void {
  if (names.length === 0) {
    log('   (none)');
    return;
  }
  names.forEach((n) => log(`   - ${n} [${getItemOperation(promotionData, metadataType, n)}]`));
}

// Reads and parses either a Copado promotion.json or a package.xml into PromotionItem[].
function loadInputFile(log: (msg: string) => void, inputFilePath: string): PromotionItem[] {
  const isXml = path.extname(inputFilePath).toLowerCase() === '.xml';
  log(`Loading ${isXml ? 'package.xml' : 'promotion JSON'}...`);
  const fileContent = fs.readFileSync(inputFilePath, 'utf8');
  return isXml ? parsePackageXml(fileContent) : (JSON.parse(fileContent) as PromotionItem[]);
}

// ===============================================================
// NAMESPACE PURGE (Option 2)
// ===============================================================

async function runNamespacePurge(log: (msg: string) => void, repoPath: string): Promise<void> {
  const namespace = (await prompt('Enter namespace to purge (e.g. TSPC): ')).trim();
  if (!namespace) {
    log('No namespace entered. Aborting.');
    return;
  }

  log(`\nPurging all ${namespace}__ references from force-app/...\n`);

  const REPO_PATH = repoPath;
  const forceAppPath = path.join(REPO_PATH, 'force-app');
  if (!fs.existsSync(forceAppPath)) {
    log(`force-app/ not found at ${forceAppPath}. Aborting.`);
    return;
  }

  const deletedFiles: string[] = [];
  const modifiedFiles: string[] = [];
  const nsLower = namespace.toLowerCase();

  function walkDir(dir: string): void {
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        walkDir(fullPath);
      } else if (entry.isFile()) {
        const basenameLower = entry.name.toLowerCase();

        // Delete files whose filename contains namespace__ (e.g. TSPC__Field__c.field-meta.xml)
        if (basenameLower.includes(`${nsLower}__`)) {
          fs.unlinkSync(fullPath);
          deletedFiles.push(fullPath);
          log(`   [DELETE] ${path.relative(REPO_PATH, fullPath)}`);

          // Clean namespace references from Profiles, PermSets, PermSetGroups
        } else if (
          basenameLower.endsWith('.profile-meta.xml') ||
          basenameLower.endsWith('.permissionset-meta.xml') ||
          basenameLower.endsWith('.permissionsetgroup-meta.xml') ||
          basenameLower.endsWith('.mutingpermissionset-meta.xml')
        ) {
          const content = readFileWithRetry(fullPath);
          const { updated, removed } = bulkRemoveNamespaceRefs(content, namespace);
          if (removed) {
            writeFileWithRetry(fullPath, updated);
            modifiedFiles.push(fullPath);
            log(`   [CLEANED] ${path.relative(REPO_PATH, fullPath)}`);
          }
        } else if (basenameLower.endsWith('.layout-meta.xml')) {
          const content = readFileWithRetry(fullPath);
          const { updated, removed } = removeNsFromLayout(content, namespace);
          if (removed) {
            writeFileWithRetry(fullPath, updated);
            modifiedFiles.push(fullPath);
            log(`   [CLEANED] ${path.relative(REPO_PATH, fullPath)}`);
          }
        } else if (basenameLower.endsWith('.reporttype-meta.xml')) {
          const content = readFileWithRetry(fullPath);
          const { updated, removed } = removeNsFromReportType(content, namespace);
          if (removed) {
            writeFileWithRetry(fullPath, updated);
            modifiedFiles.push(fullPath);
            log(`   [CLEANED] ${path.relative(REPO_PATH, fullPath)}`);
          }
        }
      }
    }
  }

  walkDir(forceAppPath);

  log('\n------------------------------------------------------');
  log(`  Namespace Purge Summary — ${namespace}__`);
  log('------------------------------------------------------');
  log(`  Files deleted : ${deletedFiles.length}`);
  log(`  Files cleaned : ${modifiedFiles.length}`);

  if (deletedFiles.length === 0 && modifiedFiles.length === 0) {
    log(`\nNo ${namespace}__ references found. Nothing to commit.`);
    return;
  }

  log('\nCommitting changes...');
  try {
    execSync(`git -C "${REPO_PATH}" add -A`, { stdio: 'pipe' });
    execSync(
      `git -C "${REPO_PATH}" commit -m "chore: purge ${namespace}__ namespace references from repo\n\n${deletedFiles.length} files deleted, ${modifiedFiles.length} files cleaned."`,
      { stdio: 'pipe' }
    );
    log(`✓ Committed. ${deletedFiles.length} deleted, ${modifiedFiles.length} cleaned.`);
  } catch (e) {
    log(`Git commit failed: ${(e as Error).message}`);
  }
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

  // If org was supplied via flag, skip the sf org list call entirely — trust the flag value.
  // The org list check on VDI takes 15s+ and re-prompts if the alias isn't found, which
  // causes the extension to hang waiting for input it never sends.
  let targetOrg = targetOrgFlag.trim();
  if (!targetOrg) {
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
    while (!targetOrg || (hasOrgList && !validOrgs.has(targetOrg))) {
      if (targetOrg) log(`   "${targetOrg}" is not a recognised org alias or username. Please try again.\n`);
      targetOrg = // eslint-disable-next-line no-await-in-loop
        (await prompt('Enter target org username or alias\n   (e.g. RBKQA or user@rubrik.com.qa)\n> ')).trim();
    }
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
    log('  SF CLEANZ — Salesforce Metadata Cleanup Tool');
    log('======================================================\n');
    log('  1) Validate and clean missing references');
    log('  2) Remove specific namespace references all over repo\n');
    // eslint-disable-next-line no-await-in-loop
    const choice = (await prompt('Enter your choice (1 or 2): ')).trim();

    // Resolve repo paths here (after first prompt) — NOT at module load time.
    // This avoids blocking the Node.js thread with execSync during SF CLI import.
    const {
      REPO_PATH,
      PS_BASE_PATH,
      MUTING_PS_BASE_PATH,
      PSG_BASE_PATH,
      PROFILE_BASE_PATH,
      LAYOUT_BASE_PATH,
      REPORT_TYPE_BASE_PATH,
    } = resolveRepoPaths();

    if (choice === '2') {
      await runNamespacePurge(log, REPO_PATH);
      return;
    }

    if (dryRun) log('*** DRY RUN MODE — files will be modified but NO commits will be made ***\n');

    // eslint-disable-next-line no-await-in-loop
    const { inputFilePath, targetOrg } = await resolveInputs(log, flags['json-path'] ?? '', flags['target-org'] ?? '');

    // ================= LOAD & PARSE INPUT FILE =================
    const promotionData = loadInputFile(log, inputFilePath);

    // Exclude RetrieveOnly and Delete items from validation — they are not being deployed.
    // For package.xml (no "a" field), treat all entries as deployable (preserve existing behavior).
    const isDeployable = (i: PromotionItem): boolean => {
      if (!i.a) return true;
      const op = i.a.toLowerCase();
      return !op.startsWith('retrieve') && !op.startsWith('delete');
    };

    const permSets = [
      ...new Set(promotionData.filter((i) => i.t === 'PermissionSet' && isDeployable(i)).map((i) => i.n)),
    ].sort();
    const mutingPermSets = [
      ...new Set(promotionData.filter((i) => i.t === 'MutingPermissionSet' && isDeployable(i)).map((i) => i.n)),
    ].sort();
    const permSetGroups = [
      ...new Set(promotionData.filter((i) => i.t === 'PermissionSetGroup' && isDeployable(i)).map((i) => i.n)),
    ].sort();
    const profiles = [
      ...new Set(promotionData.filter((i) => i.t === 'Profile' && isDeployable(i)).map((i) => i.n)),
    ].sort();
    const reportTypes = [
      ...new Set(promotionData.filter((i) => i.t === 'ReportType' && isDeployable(i)).map((i) => i.n)),
    ].sort();
    const layouts = [
      ...new Set(promotionData.filter((i) => i.t === 'Layout' && isDeployable(i)).map((i) => i.n)),
    ].sort();
    const whitelist: WhitelistMap = {
      fields: [
        ...new Set(promotionData.filter((i) => i.t === 'CustomField' && isDeployable(i)).map((i) => i.n)),
      ].sort(),
      apps: [
        ...new Set(promotionData.filter((i) => i.t === 'CustomApplication' && isDeployable(i)).map((i) => i.n)),
      ].sort(),
      classes: [...new Set(promotionData.filter((i) => i.t === 'ApexClass' && isDeployable(i)).map((i) => i.n))].sort(),
      pages: [...new Set(promotionData.filter((i) => i.t === 'ApexPage' && isDeployable(i)).map((i) => i.n))].sort(),
      tabs: [...new Set(promotionData.filter((i) => i.t === 'CustomTab' && isDeployable(i)).map((i) => i.n))].sort(),
      objects: [
        ...new Set(promotionData.filter((i) => i.t === 'CustomObject' && isDeployable(i)).map((i) => i.n)),
      ].sort(),
      flows: [...new Set(promotionData.filter((i) => i.t === 'Flow' && isDeployable(i)).map((i) => i.n))].sort(),
      layouts: [...new Set(promotionData.filter((i) => i.t === 'Layout' && isDeployable(i)).map((i) => i.n))].sort(),
      flexipages: [
        ...new Set(promotionData.filter((i) => i.t === 'FlexiPage' && isDeployable(i)).map((i) => i.n)),
      ].sort(),
      recordTypes: [
        ...new Set(promotionData.filter((i) => i.t === 'RecordType' && isDeployable(i)).map((i) => i.n)),
      ].sort(),
      // CustomMetadata type definitions appear as CustomObject with __mdt suffix.
      // Individual CMT records appear as CustomMetadata with "TypeName__mdt.RecordName" format.
      customMetadataTypes: [
        ...new Set([
          ...promotionData
            .filter((i) => i.t === 'CustomObject' && isDeployable(i) && i.n.endsWith('__mdt'))
            .map((i) => i.n),
          ...promotionData
            .filter((i) => i.t === 'CustomMetadata' && isDeployable(i))
            .map((i) => {
              const dot = i.n.indexOf('.');
              return dot >= 0 ? i.n.substring(0, dot) : i.n;
            }),
        ]),
      ].sort(),
      customPermissions: [
        ...new Set(promotionData.filter((i) => i.t === 'CustomPermission' && isDeployable(i)).map((i) => i.n)),
      ].sort(),
      recordTypeVisibilities: [
        ...new Set(promotionData.filter((i) => i.t === 'RecordType' && isDeployable(i)).map((i) => i.n)),
      ].sort(),
    };

    // Build full file path list upfront — sweepOtherFiles needs this.
    const collectDir = (dir: string, ext: string): string[] => {
      if (!fs.existsSync(dir)) return [];
      return fs
        .readdirSync(dir)
        .filter((f) => f.toLowerCase().endsWith(ext))
        .map((f) => path.join(dir, f));
    };
    const allFilePaths: string[] = [
      ...permSets.map((ps) => path.join(PS_BASE_PATH, `${ps}.permissionset-meta.xml`)),
      ...mutingPermSets.map((mps) => path.join(MUTING_PS_BASE_PATH, `${mps}.mutingpermissionset-meta.xml`)),
      ...permSetGroups.map((psg) => path.join(PSG_BASE_PATH, `${psg}.permissionsetgroup-meta.xml`)),
      ...profiles.map((p) => path.join(PROFILE_BASE_PATH, `${p}.profile-meta.xml`)),
      ...collectDir(LAYOUT_BASE_PATH, '.layout-meta.xml'),
      // Only JSON batch report types — cross-sweep stays within the batch.
      // Out-of-batch report types are handled by repoWideSweep at the end.
      ...reportTypes.map((n) => path.join(REPORT_TYPE_BASE_PATH, `${n}.reportType-meta.xml`)),
    ];

    const totalWhitelisted = Object.values(whitelist).reduce((sum, arr) => sum + arr.length, 0);

    // ================= STARTUP SUMMARY =================
    log('\n======================================================');
    log('STARTING AUTOMATED DEPLOY & FIX LOOP');
    log('======================================================');
    log(`Target Org              : ${targetOrg}`);
    log(`Permission Sets         : ${permSets.length} found in JSON (deployable)`);
    log(`Muting Permission Sets  : ${mutingPermSets.length} found in JSON (deployable)`);
    log(`Permission Set Groups   : ${permSetGroups.length} found in JSON (deployable)`);
    log(`Profiles                : ${profiles.length} found in JSON (deployable)`);
    log(`Report Types            : ${reportTypes.length} found in JSON (deployable)`);
    const nonDeployable = promotionData.filter(
      (i) =>
        ['PermissionSet', 'PermissionSetGroup', 'Profile', 'MutingPermissionSet', 'ReportType'].includes(i.t) &&
        !isDeployable(i)
    );
    if (nonDeployable.length > 0) {
      log(`Skipped (not deployed)  : ${nonDeployable.length} — not validated`);
      nonDeployable.forEach((i) => log(`   - ${i.n} [${i.t}] (${i.a ?? 'unknown'} — excluded)`));
    }
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
    logItemList(log, mutingPermSets, promotionData, 'MutingPermissionSet');
    log('\nPermission Set Groups to process:');
    logItemList(log, permSetGroups, promotionData, 'PermissionSetGroup');
    log('\nProfiles to process:');
    profiles.forEach((p) => log(`   - ${p} [${getItemOperation(promotionData, 'Profile', p)}]`));

    log('\nReport Types to process:');
    logItemList(log, reportTypes, promotionData, 'ReportType');

    log('\nLayouts to process:');
    logItemList(log, layouts, promotionData, 'Layout');

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
        errorBasedMasks: [] as ErrorMask[],
        done: false,
        calcFailedRetries: 0,
        consecutiveZeroFailures: 0,
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
        errorBasedMasks: [] as ErrorMask[],
        done: false,
        calcFailedRetries: 0,
        consecutiveZeroFailures: 0,
      })),
      ...permSetGroups.map((n) => ({
        metadataType: 'PermissionSetGroup',
        itemName: n,
        filePath: path.join(PSG_BASE_PATH, `${n}.permissionsetgroup-meta.xml`),
        operation: getItemOperation(promotionData, 'PermissionSetGroup', n),
        status: 'No Change',
        allRemovedFields: [] as Array<{ label: string; error: string }>,
        allRemovedRefs: [] as RemovedRef[],
        allSkippedFields: [] as string[],
        allUnhandledErrors: [] as string[],
        errorBasedMasks: [] as ErrorMask[],
        done: false,
        calcFailedRetries: 0,
        consecutiveZeroFailures: 0,
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
        errorBasedMasks: [] as ErrorMask[],
        done: false,
        calcFailedRetries: 0,
        consecutiveZeroFailures: 0,
      })),
      ...reportTypes.map((n) => ({
        metadataType: 'ReportType',
        itemName: n,
        filePath: path.join(REPORT_TYPE_BASE_PATH, `${n}.reportType-meta.xml`),
        operation: getItemOperation(promotionData, 'ReportType', n),
        status: 'No Change',
        allRemovedFields: [] as Array<{ label: string; error: string }>,
        allRemovedRefs: [] as RemovedRef[],
        allSkippedFields: [] as string[],
        allUnhandledErrors: [] as string[],
        errorBasedMasks: [] as ErrorMask[],
        done: false,
        calcFailedRetries: 0,
        consecutiveZeroFailures: 0,
      })),
      ...layouts.map((n) => ({
        metadataType: 'Layout',
        itemName: n,
        filePath: path.join(LAYOUT_BASE_PATH, `${n}.layout-meta.xml`),
        operation: getItemOperation(promotionData, 'Layout', n),
        status: 'No Change',
        allRemovedFields: [] as Array<{ label: string; error: string }>,
        allRemovedRefs: [] as RemovedRef[],
        allSkippedFields: [] as string[],
        allUnhandledErrors: [] as string[],
        errorBasedMasks: [] as ErrorMask[],
        done: false,
        calcFailedRetries: 0,
        consecutiveZeroFailures: 0,
      })),
    ];

    // Deduplication pre-pass is intentionally skipped.
    // Salesforce does not error on duplicate objectPermissions/PS blocks — it silently
    // deploys the last one. Auto-removing duplicates is risky: file position does not
    // reflect recency (a newer git-blame date can appear earlier in the file), so there
    // is no safe way to know which block represents the intended state. Leaving duplicates
    // in place is harmless; the developer should resolve conflicting blocks manually.
    // runDeduplicationPrePass(log, batchItems, REPO_PATH, dryRun);

    // ── PSG deduplication pre-pass ───────────────────────────────────────────────
    // For PermissionSetGroup files, duplicate <permissionSets> entries cause
    // "Element permissionSets is duplicated" or silent PSG recalculation failures.
    // fixPsgPermissionSetsBlock safely deduplicates (case-insensitive) and re-orders
    // the block after <label> — this is always correct for PSGs so we run it proactively.
    {
      const psgItems = batchItems.filter((i) => i.metadataType === 'PermissionSetGroup');
      const psgFixed: string[] = [];
      for (const psgItem of psgItems) {
        if (!fs.existsSync(psgItem.filePath)) continue;
        const psgXml = readFileWithRetry(psgItem.filePath);
        // Only remove duplicates in-place — do NOT reorder so git blame stays clean
        const { updated, removed } = removeDuplicatePsgPermissionSets(psgXml);
        if (removed.length > 0) {
          saveXmlPreserved(updated, psgItem.filePath);
          psgFixed.push(psgItem.itemName);
          log(
            `   [PSG Pre-fix] ${psgItem.itemName} — removed ${
              removed.length
            } duplicate <permissionSets>: ${removed.join(', ')}`
          );
          removed.forEach((name) =>
            psgItem.allRemovedFields.push({
              label: `<permissionSets>${name}</permissionSets>`,
              error: 'Duplicate <permissionSets> entry removed',
            })
          );
        }
      }
      if (psgFixed.length > 0 && !dryRun) {
        try {
          for (const name of psgFixed) {
            execSync(`git add "${path.join(PSG_BASE_PATH, `${name}.permissionsetgroup-meta.xml`)}"`, {
              cwd: REPO_PATH,
            });
          }
          execSync(`git commit -m "Auto-fix: deduplicate <permissionSets> in PSG(s): ${psgFixed.join(', ')}"`, {
            cwd: REPO_PATH,
          });
          log(`   [PSG Pre-fix] Committed dedup fix for: ${psgFixed.join(', ')}`);
        } catch (e) {
          log(`   [PSG Pre-fix] git commit failed: ${(e as Error).message ?? e}`);
        }
      }
    }
    // ── end PSG deduplication pre-pass ───────────────────────────────────────────

    // Record HEAD so the final squash covers only missing-ref removal commits.
    const startingCommit = execSync('git rev-parse HEAD', { cwd: REPO_PATH }).toString().trim();
    log(`   Starting commit: ${startingCommit.substring(0, 8)}`);

    log('\n######################################################');
    log(
      `  PROCESSING BATCH: ${permSets.length} PermSet(s) + ${mutingPermSets.length} MutingPermSet(s) + ${permSetGroups.length} PSG(s) + ${profiles.length} Profile(s)`
    );
    log('######################################################');

    // eslint-disable-next-line no-await-in-loop
    const { summary, allNsModifiedFiles, lastNsCommitMsg, nsOriginalXmlMap, postDedupCommit } = await runBatchDeploy(
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
      dryRun,
      promotionData,
      startingCommit
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

    log('\nPERMISSION SET GROUPS:');
    summary
      .filter((r) => r.Type === 'PermissionSetGroup')
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

    log('\nREPORT TYPES:');
    if (summary.filter((r) => r.Type === 'ReportType').length > 0) {
      summary
        .filter((r) => r.Type === 'ReportType')
        .forEach((r) =>
          log(
            `   [${r.Name}] Status: ${r.Status} | Removed: ${r.RemovedFields || 'none'} | Skipped: ${
              r.SkippedFields || 'none'
            }`
          )
        );
    } else {
      log('   (none)');
    }

    log('\nLAYOUTS:');
    if (summary.filter((r) => r.Type === 'Layout').length > 0) {
      summary
        .filter((r) => r.Type === 'Layout')
        .forEach((r) =>
          log(
            `   [${r.Name}] Status: ${r.Status} | Removed: ${r.RemovedFields || 'none'} | Skipped: ${
              r.SkippedFields || 'none'
            }`
          )
        );
    } else {
      log('   (none)');
    }

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
      r.Type === 'PermissionSet'
        ? 'PermSet'
        : r.Type === 'MutingPermissionSet'
        ? 'MutingPS'
        : r.Type === 'PermissionSetGroup'
        ? 'PSG'
        : r.Type === 'ReportType'
        ? 'ReportType'
        : r.Type === 'Layout'
        ? 'Layout'
        : 'Profile',
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

    const csvPath = path.join(os.tmpdir(), 'cleanz-deploy-summary.csv');
    const csvHeader = 'Type,Name,Op,Status,RemovedFields,RemovedErrors,SkippedFields,UnhandledErrors';
    const csvRows = summary.map(
      (r) =>
        `${r.Type},"${r.Name}",${r.Op},"${r.Status}","${r.RemovedFields}","${r.RemovedErrors}","${
          r.SkippedFields
        }","${r.UnhandledErrors.replace(/"/g, '""')}"`
    );
    fs.writeFileSync(csvPath, [csvHeader, ...csvRows].join('\n'), 'utf8');

    log(`Summary CSV saved to : ${csvPath}`);
    log(`Total deploy calls   : ${totalDeploys.value} / ${MAX_TOTAL_DEPLOYS}`);

    // ================= SQUASH MISSING-REF COMMITS =================
    // Resets to postDedupCommit (HEAD after the dedup pre-pass commit) so the dedup
    // commit stays separate. NS/managed-ref files are re-committed first (Phase 1),
    // then all remaining missing-ref changes are squashed into one commit (Phase 2).
    if (dryRun) {
      log('\nDry run — no commits were made, skipping squash.');
    } else {
      squashMissingRefCommits(
        log,
        postDedupCommit,
        allNsModifiedFiles,
        lastNsCommitMsg,
        nsOriginalXmlMap,
        summary,
        REPO_PATH
      );
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
