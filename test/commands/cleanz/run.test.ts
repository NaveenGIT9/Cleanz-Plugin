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

import { expect } from 'chai';
import {
  buildAsciiTable,
  deduplicateXmlBlocks,
  formatXml,
  getRootNodeName,
  maskPermSetFalsePositives,
  maskProfileFalsePositives,
  removeProfileActionOverridesWithMissingObject,
  removeProfileActionOverridesWithMissingRecordType,
  removeXmlBlock,
} from '../../../src/commands/cleanz/run.js';

// ─── buildAsciiTable ─────────────────────────────────────────────────────────

describe('buildAsciiTable', () => {
  it('produces correct separator and header rows', () => {
    const result = buildAsciiTable(['Name', 'Status'], [['Alice', 'OK']]);
    const lines = result.split('\n');
    // separator, header, separator, data row, separator = 5 lines
    expect(lines).to.have.length(5);
    expect(lines[0]).to.match(/^\+[-+]+\+$/);
    expect(lines[1]).to.include('Name');
    expect(lines[1]).to.include('Status');
    expect(lines[3]).to.include('Alice');
    expect(lines[3]).to.include('OK');
  });

  it('pads columns to the widest value', () => {
    const result = buildAsciiTable(
      ['A', 'B'],
      [
        ['ShortName', 'x'],
        ['LongerNameHere', 'y'],
      ]
    );
    const lines = result.split('\n');
    // all data lines should have the same total length
    const lengths = [lines[1], lines[3], lines[4]].map((l) => l.length);
    expect(new Set(lengths).size).to.equal(1);
  });

  it('handles empty rows array', () => {
    const result = buildAsciiTable(['Col1', 'Col2'], []);
    const lines = result.split('\n');
    // sep + header + sep + closing sep = 4 lines (no data rows)
    expect(lines).to.have.length(4);
    expect(result).to.include('Col1');
    expect(result).to.include('Col2');
  });

  it('handles a cell wider than its header', () => {
    const result = buildAsciiTable(['H'], [['VeryLongCellValue']]);
    expect(result).to.include('VeryLongCellValue');
    // header cell should be padded to match
    const headerLine = result.split('\n')[1];
    expect(headerLine.length).to.equal(result.split('\n')[3].length);
  });
});

// ─── getRootNodeName ─────────────────────────────────────────────────────────

describe('getRootNodeName', () => {
  it('extracts Profile from a profile XML', () => {
    const xml = '<?xml version="1.0"?>\n<Profile xmlns="http://soap.sforce.com/2006/04/metadata">\n</Profile>';
    expect(getRootNodeName(xml)).to.equal('Profile');
  });

  it('extracts PermissionSet from a permset XML', () => {
    const xml = '<PermissionSet xmlns="http://soap.sforce.com/2006/04/metadata"></PermissionSet>';
    expect(getRootNodeName(xml)).to.equal('PermissionSet');
  });

  it('defaults to PermissionSet when no match', () => {
    expect(getRootNodeName('')).to.equal('PermissionSet');
  });
});

// ─── removeXmlBlock ──────────────────────────────────────────────────────────

describe('removeXmlBlock', () => {
  const xml =
    [
      '    <classAccesses>',
      '        <apexClass>GoodClass</apexClass>',
      '        <enabled>true</enabled>',
      '    </classAccesses>',
      '    <classAccesses>',
      '        <apexClass>BadClass</apexClass>',
      '        <enabled>true</enabled>',
      '    </classAccesses>',
    ].join('\n') + '\n';

  it('removes only the matching block', () => {
    const { updated, removed } = removeXmlBlock(xml, 'classAccesses', 'apexClass', 'BadClass');
    expect(removed).to.be.true;
    expect(updated).to.include('GoodClass');
    expect(updated).not.to.include('BadClass');
  });

  it('returns removed=false when name not found', () => {
    const { updated, removed } = removeXmlBlock(xml, 'classAccesses', 'apexClass', 'NonExistent');
    expect(removed).to.be.false;
    expect(updated).to.equal(xml);
  });

  it('handles CRLF line endings', () => {
    const crlfXml = xml.replace(/\n/g, '\r\n');
    const { removed } = removeXmlBlock(crlfXml, 'classAccesses', 'apexClass', 'BadClass');
    expect(removed).to.be.true;
  });

  it('removes all matching blocks when there are duplicates', () => {
    const dupXml = xml + xml;
    const { updated } = removeXmlBlock(dupXml, 'classAccesses', 'apexClass', 'BadClass');
    expect(updated).not.to.include('BadClass');
    expect((updated.match(/GoodClass/g) ?? []).length).to.equal(2);
  });
});

// ─── maskProfileFalsePositives ───────────────────────────────────────────────

describe('maskProfileFalsePositives', () => {
  const profile = [
    '<Profile>',
    '    <classAccesses><apexClass>Foo</apexClass></classAccesses>',
    '    <fieldPermissions><field>Obj__c.F__c</field></fieldPermissions>',
    '    <flowAccesses><flow>MyFlow</flow></flowAccesses>',
    '    <objectPermissions><object>Obj__c</object></objectPermissions>',
    '    <pageAccesses><apexPage>Page1</apexPage></pageAccesses>',
    '    <recordTypeVisibilities><recordType>Obj.RT</recordType></recordTypeVisibilities>',
    '    <layoutAssignments><layout>Obj-Layout</layout></layoutAssignments>',
    '    <tabVisibilities><tab>MyTab</tab></tabVisibilities>',
    '    <customMetadataTypeAccesses><name>CMT__mdt</name></customMetadataTypeAccesses>',
    '    <userPermissions><name>ApiEnabled</name></userPermissions>',
    '</Profile>',
  ].join('\n');

  it('strips classAccesses', () => {
    expect(maskProfileFalsePositives(profile)).not.to.include('<classAccesses>');
  });
  it('strips fieldPermissions', () => {
    expect(maskProfileFalsePositives(profile)).not.to.include('<fieldPermissions>');
  });
  it('strips objectPermissions', () => {
    expect(maskProfileFalsePositives(profile)).not.to.include('<objectPermissions>');
  });
  it('strips pageAccesses', () => {
    expect(maskProfileFalsePositives(profile)).not.to.include('<pageAccesses>');
  });
  it('strips recordTypeVisibilities', () => {
    expect(maskProfileFalsePositives(profile)).not.to.include('<recordTypeVisibilities>');
  });
  it('strips layoutAssignments', () => {
    expect(maskProfileFalsePositives(profile)).not.to.include('<layoutAssignments>');
  });
  it('strips tabVisibilities', () => {
    expect(maskProfileFalsePositives(profile)).not.to.include('<tabVisibilities>');
  });
  it('strips customMetadataTypeAccesses', () => {
    expect(maskProfileFalsePositives(profile)).not.to.include('<customMetadataTypeAccesses>');
  });
  it('keeps flowAccesses', () => {
    expect(maskProfileFalsePositives(profile)).to.include('<flowAccesses>');
  });
  it('keeps userPermissions', () => {
    expect(maskProfileFalsePositives(profile)).to.include('<userPermissions>');
  });
});

// ─── maskPermSetFalsePositives ───────────────────────────────────────────────

describe('maskPermSetFalsePositives', () => {
  // maskPermSetFalsePositives is intentionally a no-op for permission sets.
  // customMetadataTypeAccesses errors are real deployment failures and are handled
  // by the METADATA_HANDLERS loop — they must NOT be masked before dry-run.
  const permSet = [
    '<PermissionSet>',
    '    <classAccesses><apexClass>Foo</apexClass></classAccesses>',
    '    <customMetadataTypeAccesses><name>CMT__mdt</name></customMetadataTypeAccesses>',
    '    <fieldPermissions><field>Obj__c.F__c</field></fieldPermissions>',
    '</PermissionSet>',
  ].join('\n');

  it('returns XML unchanged — all blocks preserved for error detection', () => {
    const result = maskPermSetFalsePositives(permSet);
    expect(result).to.equal(permSet);
    expect(result).to.include('<customMetadataTypeAccesses>');
    expect(result).to.include('<classAccesses>');
    expect(result).to.include('<fieldPermissions>');
  });

  it('preserves multiple customMetadataTypeAccesses blocks', () => {
    const multi = permSet.replace(
      '<fieldPermissions>',
      '<customMetadataTypeAccesses><name>Another__mdt</name></customMetadataTypeAccesses>\n    <fieldPermissions>'
    );
    const result = maskPermSetFalsePositives(multi);
    expect(result).to.equal(multi);
    expect(result).to.include('<customMetadataTypeAccesses>');
  });
});

// ─── deduplicateXmlBlocks ────────────────────────────────────────────────────

describe('deduplicateXmlBlocks', () => {
  it('removes a duplicate fieldPermissions block', () => {
    const xml =
      [
        '    <fieldPermissions>',
        '        <editable>true</editable>',
        '        <field>Account.My_Field__c</field>',
        '        <readable>true</readable>',
        '    </fieldPermissions>',
        '    <fieldPermissions>',
        '        <editable>false</editable>',
        '        <field>Account.My_Field__c</field>',
        '        <readable>true</readable>',
        '    </fieldPermissions>',
      ].join('\n') + '\n';
    const { updated, removedCount } = deduplicateXmlBlocks(xml);
    expect(removedCount).to.equal(1);
    // first occurrence kept, second removed
    expect((updated.match(/Account\.My_Field__c/g) ?? []).length).to.equal(1);
  });

  it('keeps unique blocks untouched', () => {
    const xml =
      [
        '    <classAccesses>',
        '        <apexClass>ClassA</apexClass>',
        '        <enabled>true</enabled>',
        '    </classAccesses>',
        '    <classAccesses>',
        '        <apexClass>ClassB</apexClass>',
        '        <enabled>true</enabled>',
        '    </classAccesses>',
      ].join('\n') + '\n';
    const { updated, removedCount } = deduplicateXmlBlocks(xml);
    expect(removedCount).to.equal(0);
    expect(updated).to.equal(xml);
  });

  it('removes duplicate userPermissions', () => {
    const xml =
      [
        '    <userPermissions>',
        '        <enabled>true</enabled>',
        '        <name>ApiEnabled</name>',
        '    </userPermissions>',
        '    <userPermissions>',
        '        <enabled>false</enabled>',
        '        <name>ApiEnabled</name>',
        '    </userPermissions>',
      ].join('\n') + '\n';
    const { removedCount } = deduplicateXmlBlocks(xml);
    expect(removedCount).to.equal(1);
  });

  it('handles multiple different duplicate types in one file', () => {
    const xml =
      [
        '    <flowAccesses>',
        '        <enabled>true</enabled>',
        '        <flow>Flow1</flow>',
        '    </flowAccesses>',
        '    <flowAccesses>',
        '        <enabled>true</enabled>',
        '        <flow>Flow1</flow>',
        '    </flowAccesses>',
        '    <pageAccesses>',
        '        <apexPage>Page1</apexPage>',
        '        <enabled>true</enabled>',
        '    </pageAccesses>',
        '    <pageAccesses>',
        '        <apexPage>Page1</apexPage>',
        '        <enabled>true</enabled>',
        '    </pageAccesses>',
      ].join('\n') + '\n';
    const { removedCount } = deduplicateXmlBlocks(xml);
    expect(removedCount).to.equal(2);
  });

  it('returns removedCount=0 and unchanged xml when no duplicates', () => {
    const xml = '<PermissionSet></PermissionSet>';
    const { updated, removedCount } = deduplicateXmlBlocks(xml);
    expect(removedCount).to.equal(0);
    expect(updated).to.equal(xml);
  });
});

// ─── formatXml ───────────────────────────────────────────────────────────────

describe('formatXml', () => {
  it('indents nested elements by 4 spaces per level', () => {
    const xml = '<Profile><classAccesses><apexClass>Foo</apexClass></classAccesses></Profile>';
    const result = formatXml(xml);
    const lines = result.split('\n').filter(Boolean);
    const apexLine = lines.find((l) => l.includes('apexClass'))!;
    expect(apexLine.startsWith('        ')).to.be.true; // 8 spaces = 2 levels
  });

  it('does not double-indent self-closing tags', () => {
    const xml = '<Profile><loginHours n1:nil="true" /></Profile>';
    const result = formatXml(xml);
    expect(result).to.include('loginHours');
    // self-closing should not shift subsequent indent
    expect(result).not.to.include('        </Profile>');
  });
});

// ─── Integration: realistic profile XML ──────────────────────────────────────
// These tests use XML that mirrors real Salesforce profile/permset structure
// and exercise the full removal logic end-to-end without mocking.

const REALISTIC_PROFILE = `<?xml version="1.0" encoding="UTF-8"?>
<Profile xmlns="http://soap.sforce.com/2006/04/metadata">
    <classAccesses>
        <apexClass>GoodClass</apexClass>
        <enabled>true</enabled>
    </classAccesses>
    <classAccesses>
        <apexClass>BadClass</apexClass>
        <enabled>true</enabled>
    </classAccesses>
    <fieldPermissions>
        <editable>true</editable>
        <field>Account.Good_Field__c</field>
        <readable>true</readable>
    </fieldPermissions>
    <fieldPermissions>
        <editable>true</editable>
        <field>Account.Bad_Field__c</field>
        <readable>true</readable>
    </fieldPermissions>
    <flowAccesses>
        <enabled>true</enabled>
        <flow>Good_Flow</flow>
    </flowAccesses>
    <flowAccesses>
        <enabled>true</enabled>
        <flow>Bad_Flow</flow>
    </flowAccesses>
    <profileActionOverrides>
        <actionName>View</actionName>
        <content>GoodFlexiPage</content>
        <formFactor>Large</formFactor>
        <pageOrSobjectType>Account</pageOrSobjectType>
        <recordType>Account.Good_RT</recordType>
        <type>Flexipage</type>
    </profileActionOverrides>
    <profileActionOverrides>
        <actionName>View</actionName>
        <content>BadFlexiPage</content>
        <formFactor>Large</formFactor>
        <pageOrSobjectType>Account</pageOrSobjectType>
        <type>Flexipage</type>
    </profileActionOverrides>
    <profileActionOverrides>
        <actionName>View</actionName>
        <content>SomeFlexiPage</content>
        <formFactor>Large</formFactor>
        <pageOrSobjectType>Bad_Object__c</pageOrSobjectType>
        <recordType>Account.Bad_RT</recordType>
        <type>Flexipage</type>
    </profileActionOverrides>
    <userPermissions>
        <enabled>true</enabled>
        <name>ApiEnabled</name>
    </userPermissions>
    <userPermissions>
        <enabled>true</enabled>
        <name>BadPermission</name>
    </userPermissions>
</Profile>
`;

describe('Integration — realistic profile XML', () => {
  it('removes only the bad classAccesses block, keeps the good one', () => {
    const { updated, removed } = removeXmlBlock(REALISTIC_PROFILE, 'classAccesses', 'apexClass', 'BadClass');
    expect(removed).to.be.true;
    expect(updated).to.include('GoodClass');
    expect(updated).not.to.include('BadClass');
  });

  it('removes only the bad fieldPermissions block, keeps the good one', () => {
    const { updated, removed } = removeXmlBlock(REALISTIC_PROFILE, 'fieldPermissions', 'field', 'Account.Bad_Field__c');
    expect(removed).to.be.true;
    expect(updated).to.include('Account.Good_Field__c');
    expect(updated).not.to.include('Account.Bad_Field__c');
  });

  it('removes only the bad flowAccesses block, keeps the good one', () => {
    const { updated, removed } = removeXmlBlock(REALISTIC_PROFILE, 'flowAccesses', 'flow', 'Bad_Flow');
    expect(removed).to.be.true;
    expect(updated).to.include('Good_Flow');
    expect(updated).not.to.include('Bad_Flow');
  });

  it('removes only the bad FlexiPage profileActionOverrides block, keeps the good one', () => {
    const { updated, removed } = removeXmlBlock(REALISTIC_PROFILE, 'profileActionOverrides', 'content', 'BadFlexiPage');
    expect(removed).to.be.true;
    expect(updated).to.include('GoodFlexiPage');
    expect(updated).not.to.include('BadFlexiPage');
  });

  it('removes only the bad userPermissions block, keeps the good one', () => {
    const { updated, removed } = removeXmlBlock(REALISTIC_PROFILE, 'userPermissions', 'name', 'BadPermission');
    expect(removed).to.be.true;
    expect(updated).to.include('ApiEnabled');
    expect(updated).not.to.include('BadPermission');
  });

  it('removeProfileActionOverridesWithMissingRecordType — removes missing RT, keeps valid RT', () => {
    const existingRTs = new Set(['Account.Good_RT']);
    const { updated, removedRecordTypes } = removeProfileActionOverridesWithMissingRecordType(
      REALISTIC_PROFILE,
      existingRTs,
      []
    );
    expect(removedRecordTypes).to.include('Account.Bad_RT');
    expect(removedRecordTypes).not.to.include('Account.Good_RT');
    expect(updated).to.include('GoodFlexiPage'); // block with good RT stays
    expect(updated).not.to.include('SomeFlexiPage'); // block with bad RT removed
  });

  it('removeProfileActionOverridesWithMissingRecordType — keeps RT that is whitelisted (being deployed)', () => {
    const existingRTs = new Set<string>(); // empty — RT not in org
    const { updated, removedRecordTypes } = removeProfileActionOverridesWithMissingRecordType(
      REALISTIC_PROFILE,
      existingRTs,
      ['Account.Good_RT'] // but it IS in the promotion JSON
    );
    expect(removedRecordTypes).not.to.include('Account.Good_RT');
    expect(updated).to.include('GoodFlexiPage'); // whitelisted block preserved
  });

  it('removeProfileActionOverridesWithMissingObject — removes missing custom object block, keeps standard object', () => {
    const existingObjects = new Set<string>(); // Bad_Object__c not in org
    const { updated, removedObjects } = removeProfileActionOverridesWithMissingObject(
      REALISTIC_PROFILE,
      existingObjects,
      []
    );
    expect(removedObjects).to.include('Bad_Object__c');
    // Account is a standard object (no __c) — should never be removed
    expect(updated).to.include('Account.Good_RT'); // Account block stays
  });

  it('removeProfileActionOverridesWithMissingObject — keeps custom object that is whitelisted', () => {
    const existingObjects = new Set<string>(); // not in org
    const { removedObjects } = removeProfileActionOverridesWithMissingObject(REALISTIC_PROFILE, existingObjects, [
      'Bad_Object__c',
    ]);
    expect(removedObjects).not.to.include('Bad_Object__c'); // whitelisted — keep it
  });
});
