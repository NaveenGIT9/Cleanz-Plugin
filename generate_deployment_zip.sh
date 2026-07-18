#!/bin/bash
# Copado DevOps Function — Generate Deployment Zip
# Creates a deployment-ready zip from the promotion branch for Add/Full/Selective components
# and uploads it as a ContentVersion linked to the Promotion record.
set -euo pipefail

log() { copado -p "zip | $*" 2>/dev/null || echo "[zip] $*"; }

# Debug: log what Copado injected
log "DEBUG PROMOTION_ID=[${PROMOTION_ID:-EMPTY}]"
log "DEBUG CF_DATA_JSON=[${CF_DATA_JSON:-EMPTY}]"
log "DEBUG CF_SF_ENDPOINT=[${CF_SF_ENDPOINT:-EMPTY}]"

# Copado injects custom parameter names as-is (no CF_ prefix).
# Parameter name "PROMOTION_ID" → env var PROMOTION_ID (not CF_PROMOTION_ID).
# CF_SF_ENDPOINT / CF_SF_SESSIONID / CF_DATA_JSON are system vars and DO have CF_ prefix.
if [ -z "${PROMOTION_ID:-}" ] && [ -n "${CF_DATA_JSON:-}" ]; then
  PROMOTION_ID=$(node -e "try{const d=JSON.parse(process.env.CF_DATA_JSON);process.stdout.write(d.PROMOTION_ID||d.promotionId||'')}catch(e){}" 2>/dev/null || true)
fi
PROMOTION_ID="${PROMOTION_ID:-${1:-}}"

[ -z "$PROMOTION_ID" ] && { log "ERROR: PROMOTION_ID not set — check Function parameter configuration"; exit 1; }
[ -z "${CF_SF_ENDPOINT:-}" ] && { log "ERROR: CF_SF_ENDPOINT not set"; exit 1; }
[ -z "${CF_SF_SESSIONID:-}" ] && { log "ERROR: CF_SF_SESSIONID not set"; exit 1; }

log "Starting for Promotion $PROMOTION_ID"

# ─── Step 1: Query SF + parse everything in ONE node process ─────────────────
eval "$(node << 'NODEJS'
const https = require('https');
const url0  = (process.env.CF_SF_ENDPOINT || '').replace(/\/+$/, '');
const tok   = process.env.CF_SF_SESSIONID || '';
const pid   = process.env.PROMOTION_ID || '';

function sfGet(path) {
  return new Promise((res, rej) => {
    const u = new URL(url0 + path);
    https.get({ hostname: u.hostname, path: u.pathname + u.search,
      headers: { Authorization: 'Bearer ' + tok, Accept: 'application/json' } },
      r => { let b = ''; r.on('data', d => b += d);
        r.on('end', () => r.statusCode >= 400 ? rej(new Error('HTTP ' + r.statusCode + ': ' + b)) : res(JSON.parse(b))); }
    ).on('error', rej);
  });
}

async function main() {
  // Parallel: promotion info + content links
  const [promoRes, linksRes] = await Promise.all([
    sfGet('/services/data/v62.0/query?q=' + encodeURIComponent(
      "SELECT Name, copado__Project__r.copado__Deployment_Flow__r.copado__Git_Repository__r.copado__URI__c," +
      "copado__Project__r.copado__Deployment_Flow__r.copado__Git_Repository__r.copado__Personal_Access_Token__c" +
      " FROM copado__Promotion__c WHERE Id = '" + pid + "'")),
    sfGet('/services/data/v62.0/query?q=' + encodeURIComponent(
      "SELECT ContentDocument.LatestPublishedVersionId FROM ContentDocumentLink" +
      " WHERE LinkedEntityId = '" + pid + "'" +
      " AND ContentDocument.Title LIKE 'Copado Promotion changes%'" +
      " ORDER BY ContentDocument.LastModifiedDate DESC LIMIT 1"))
  ]);

  if (!promoRes.records?.length) throw new Error('Promotion not found: ' + pid);
  if (!linksRes.records?.length) throw new Error('No Copado Promotion JSON on Promotion ' + pid);

  const promo  = promoRes.records[0];
  const repo   = promo.copado__Project__r?.copado__Deployment_Flow__r?.copado__Git_Repository__r;
  const repoUri = repo?.copado__URI__c || '';
  const pat     = repo?.copado__Personal_Access_Token__c || '';
  const branch  = 'promotion/' + promo.Name;

  const cvId   = linksRes.records[0].ContentDocument.LatestPublishedVersionId;
  const cvData = await sfGet('/services/data/v62.0/sobjects/ContentVersion/' + cvId + '?fields=VersionData');
  const all    = JSON.parse(Buffer.from(cvData.VersionData, 'base64').toString('utf8'));
  const comps  = all.filter(c => ['Add','Full','Selective'].includes(c.a));

  // Build clone URL
  let cloneUrl = repoUri;
  const ssh = repoUri.match(/^git@([^:]+):(.+)$/);
  if (ssh) cloneUrl = 'https://' + ssh[1] + '/' + ssh[2];
  if (pat) cloneUrl = cloneUrl.replace('https://', 'https://' + pat + '@');

  // Build sparse-checkout patterns
  const TYPE_FOLDER = { ApexClass:'classes',ApexTrigger:'triggers',PermissionSet:'permissionsets',
    PermissionSetGroup:'permissionsetgroups',MutingPermissionSet:'mutingpermissionsets',
    CustomObject:'objects',Flow:'flows',FlowDefinition:'flowDefinitions',Layout:'layouts',
    Profile:'profiles',StaticResource:'staticresources',CustomMetadata:'customMetadata',
    QuickAction:'quickActions',FlexiPage:'flexipages',CustomTab:'tabs',
    CustomApplication:'applications',ReportType:'reportTypes',CustomLabel:'labels',
    ContentAsset:'contentassets',LightningComponentBundle:'lwc',AuraDefinitionBundle:'aura' };
  const NESTED = { CustomField:'fields',ValidationRule:'validationRules',RecordType:'recordTypes',
    FieldSet:'fieldSets',WebLink:'webLinks',ListView:'listViews',CompactLayout:'compactLayouts' };
  const patterns = new Set();
  for (const {t,n} of comps) {
    if (NESTED[t])       { const [o]=n.split('.'); if(o) patterns.add('**/objects/'+o+'/**'); }
    else if (TYPE_FOLDER[t]) patterns.add('**/'+TYPE_FOLDER[t]+'/'+n+'*');
  }

  // Output as shell variables (eval'd by parent)
  const q = s => "'" + String(s).replace(/'/g,"'\\''") + "'";
  console.log('BRANCH=' + q(branch));
  console.log('CLONE_URL=' + q(cloneUrl));
  console.log('COMP_COUNT=' + comps.length);
  console.log('SPARSE_PATTERNS=' + q([...patterns].join('\n')));
  console.log('COMPONENTS_JSON=' + q(JSON.stringify(comps)));
}

main().catch(e => { process.stderr.write(e.message + '\n'); process.exit(1); });
NODEJS
)"

log "Branch: $BRANCH | Components: $COMP_COUNT"

if [ "$COMP_COUNT" = "0" ]; then
  log "No Add/Full/Selective components — nothing to zip"
  exit 0
fi

# ─── Step 2: Sparse clone (only fetch needed file paths) ─────────────────────
WORK_DIR=$(mktemp -d)
CLONE_DIR="$WORK_DIR/repo"
STAGE_DIR="$WORK_DIR/stage"
mkdir -p "$STAGE_DIR" "$CLONE_DIR"

log "Sparse cloning $BRANCH..."
git -C "$CLONE_DIR" init -q
git -C "$CLONE_DIR" remote add origin "$CLONE_URL"
git -C "$CLONE_DIR" config core.sparseCheckout true
printf '%s\n' "$SPARSE_PATTERNS" > "$CLONE_DIR/.git/info/sparse-checkout"
git -C "$CLONE_DIR" fetch --depth 1 origin "$BRANCH" -q
git -C "$CLONE_DIR" checkout FETCH_HEAD -q
log "Clone done"

# ─── Steps 3+4: Collect files + generate package.xml in ONE node process ─────
node << NODEJS
const fs   = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const cloneDir   = process.env.CLONE_DIR;
const stageDir   = process.env.STAGE_DIR;
const components = JSON.parse(process.env.COMPONENTS_JSON);

const TYPE_MAP = {
  ApexClass:            { folder:'classes',             exts:['.cls','.cls-meta.xml'] },
  ApexTrigger:          { folder:'triggers',            exts:['.trigger','.trigger-meta.xml'] },
  PermissionSet:        { folder:'permissionsets',      exts:['.permissionset-meta.xml'] },
  PermissionSetGroup:   { folder:'permissionsetgroups', exts:['.permissionsetgroup-meta.xml'] },
  MutingPermissionSet:  { folder:'mutingpermissionsets',exts:['.mutingpermissionset-meta.xml'] },
  CustomObject:         { folder:'objects',             exts:['.object-meta.xml'] },
  Flow:                 { folder:'flows',               exts:['.flow-meta.xml'] },
  FlowDefinition:       { folder:'flowDefinitions',     exts:['.flowDefinition-meta.xml'] },
  Layout:               { folder:'layouts',             exts:['.layout-meta.xml'] },
  Profile:              { folder:'profiles',            exts:['.profile-meta.xml'] },
  StaticResource:       { folder:'staticresources',     exts:['.resource-meta.xml','.resource'] },
  CustomMetadata:       { folder:'customMetadata',      exts:['.md-meta.xml'] },
  QuickAction:          { folder:'quickActions',        exts:['.quickAction-meta.xml'] },
  FlexiPage:            { folder:'flexipages',          exts:['.flexipage-meta.xml'] },
  CustomTab:            { folder:'tabs',                exts:['.tab-meta.xml'] },
  CustomApplication:    { folder:'applications',        exts:['.app-meta.xml'] },
  ReportType:           { folder:'reportTypes',         exts:['.reportType-meta.xml'] },
  CustomLabel:          { folder:'labels',              exts:['.labels-meta.xml'] },
  ContentAsset:         { folder:'contentassets',       exts:['.asset-meta.xml'] },
  LightningComponentBundle: { folder:'lwc',  isDir:true },
  AuraDefinitionBundle:     { folder:'aura', isDir:true },
  CustomField:      { nested:'fields',          ext:'.field-meta.xml' },
  ValidationRule:   { nested:'validationRules', ext:'.validationRule-meta.xml' },
  RecordType:       { nested:'recordTypes',     ext:'.recordType-meta.xml' },
  FieldSet:         { nested:'fieldSets',       ext:'.fieldSet-meta.xml' },
  WebLink:          { nested:'webLinks',        ext:'.webLink-meta.xml' },
  ListView:         { nested:'listViews',       ext:'.listView-meta.xml' },
  CompactLayout:    { nested:'compactLayouts',  ext:'.compactLayout-meta.xml' },
};

function defaultDir(root) {
  for (const c of [path.join(root,'force-app','main','default'), path.join(root,'src')]) {
    if (fs.existsSync(c)) return c;
  }
  return root;
}

const srcRoot = defaultDir(cloneDir);
let copied = 0, skipped = 0;

function stage(src, rel) {
  const dest = path.join(stageDir,'force-app','main','default', rel);
  fs.mkdirSync(path.dirname(dest), { recursive:true });
  fs.copyFileSync(src, dest);
  copied++;
}

for (const { t, n } of components) {
  const def = TYPE_MAP[t];
  if (!def) {
    const part = n.includes('.') ? n.split('.').pop() : n;
    try {
      execSync('find "'+srcRoot+'" -name "'+part+'*" 2>/dev/null', { encoding:'utf8' })
        .trim().split('\n').filter(Boolean)
        .forEach(f => stage(f, path.relative(srcRoot, f)));
    } catch { skipped++; }
    continue;
  }
  if (def.nested) {
    const [obj, mem] = n.split('.');
    if (!obj || !mem) { skipped++; continue; }
    const fp = path.join(srcRoot,'objects',obj,def.nested,mem+def.ext);
    if (fs.existsSync(fp)) stage(fp, path.join('objects',obj,def.nested,mem+def.ext));
    else skipped++;
    continue;
  }
  if (def.isDir) {
    const dp = path.join(srcRoot,def.folder,n);
    if (fs.existsSync(dp)) {
      execSync('cp -r "'+dp+'" "'+path.join(stageDir,'force-app','main','default',def.folder)+'"');
      copied++;
    } else skipped++;
    continue;
  }
  for (const ext of def.exts) {
    const fp = path.join(srcRoot,def.folder,n+ext);
    if (fs.existsSync(fp)) stage(fp, path.join(def.folder,n+ext));
  }
}

// package.xml
const typeMap = {};
for (const {t,n} of components) { if(!typeMap[t]) typeMap[t]=[]; typeMap[t].push(n); }
const esc = s => (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
let xml = '<?xml version="1.0" encoding="UTF-8"?>\n<Package xmlns="http://soap.sforce.com/2006/04/metadata">\n';
for (const t of Object.keys(typeMap).sort()) {
  xml += '    <types>\n';
  for (const m of typeMap[t].sort()) xml += '        <members>'+esc(m)+'</members>\n';
  xml += '        <name>'+t+'</name>\n    </types>\n';
}
xml += '    <version>62.0</version>\n</Package>';
fs.writeFileSync(path.join(stageDir,'package.xml'), xml);
fs.writeFileSync(path.join(stageDir,'sfdx-project.json'),
  JSON.stringify({ packageDirectories:[{path:'force-app',default:true}],
    namespace:'', sfdcLoginUrl:'https://login.salesforce.com', sourceApiVersion:'62.0' }, null, 2));

process.stdout.write('copied='+copied+' skipped='+skipped+'\n');
NODEJS CLONE_DIR="$CLONE_DIR" STAGE_DIR="$STAGE_DIR" COMPONENTS_JSON="$COMPONENTS_JSON"

log "Files staged. Creating zip..."

# ─── Step 5: Zip ─────────────────────────────────────────────────────────────
ZIP_PATH="$WORK_DIR/deployment.zip"
cd "$STAGE_DIR"
zip -r "$ZIP_PATH" . -x "*.DS_Store" > /dev/null
log "Zip size: $(du -sh "$ZIP_PATH" | cut -f1)"

# ─── Step 6: Upload ───────────────────────────────────────────────────────────
log "Uploading to Salesforce..."
SF_URL="${CF_SF_ENDPOINT%/}"
ENTITY_JSON="{\"Title\":\"deployment.zip\",\"PathOnClient\":\"deployment.zip\",\"FirstPublishLocationId\":\"${PROMOTION_ID}\"}"
RESULT=$(curl -s -w "\n%{http_code}" -X POST \
  -H "Authorization: Bearer ${CF_SF_SESSIONID}" \
  -F "entity_content=${ENTITY_JSON};type=application/json" \
  -F "VersionData=@${ZIP_PATH};type=application/zip" \
  "${SF_URL}/services/data/v62.0/sobjects/ContentVersion")
HTTP_CODE=$(echo "$RESULT" | tail -1)
[ "$HTTP_CODE" != "201" ] && { log "ERROR: Upload failed (HTTP $HTTP_CODE): $(echo "$RESULT" | head -1)"; exit 1; }

log "Done — deployment.zip uploaded to Promotion $PROMOTION_ID"
rm -rf "$WORK_DIR"
