const xml =
  '    <columns>\n        <field>ISR_Owner__c</field>\n        <table>Opportunities</table>\n    </columns>\n';
const inner = '(?:(?!<columns>)[\\s\\S])*?';
const escapedField = 'ISR_Owner__c';
const blockRegex = new RegExp(
  `[ \\t]*<columns>${inner}<field>[ \\t]*${escapedField}[ \\t]*</field>${inner}</columns>[ \\t]*\\r?\\n?`,
  'g'
);
const matches = [...xml.matchAll(blockRegex)];
console.log('block matches:', matches.length);
if (matches.length > 0) {
  const m = matches[0][0];
  const tableMatch = m.match(/<table>([\s\S]*?)<\/table>/i);
  console.log('tableMatch:', tableMatch ? tableMatch[1] : 'none');
  const tableVal = tableMatch ? tableMatch[1].trim().toLowerCase() : '';
  const segments = tableVal.split('.');
  const lastSeg = segments[segments.length - 1].replace(/__r$/i, '');
  console.log('lastSeg:', lastSeg);
  console.log('includes opportunity:', lastSeg.includes('opportunity'));
}
