#!/bin/bash
#
# pre_cleanz — SELF-CONTAINED Copado Function pre-step script.
# Register the ENTIRE contents of this file as copado__Function__c.copado__Script__c.
#
# Purpose: Runs sf cleanz BEFORE the SFDX Deploy step to auto-fix Permission Sets,
#          Profiles, Layouts and Report Types in the promotion branch. By the time
#          SFDX Deploy runs, the branch is already clean — no deploy failures due to
#          stale field/object/layout references.
#
# Image:       copado-multicloud-dx:v6   (provides copado-git-get + node + sf CLI)
# Worker size: M                         (cleanz runs dry-run deploys — needs headroom)
# Timeout:     45 min                    (allow for large batches of permsets)
#
# Parameters (set on the Function record — Copado exports them as env vars):
#   branch                 = {$Context.JobExecution__r.DataJson.promotionBranchName}
#   promotionId            = {$Context.JobExecution__r.DataJson.promotionId}
#   git_json               = {$Context.Repository.Credential}
#   destinationInstanceUrl = {$Destination.Credential.Endpoint}
#   destinationSessionid   = {$Destination.Credential.SessionId}
#
# instanceUrl / sessionId are auto-injected by Copado into every Function container and
# point to the PIPELINE ORG (RBKPIPEQA) — the org where copado__Promotion__c and
# ContentDocumentLink records live. Step 4 uses them to query the exact Copado component
# list. If the query fails, Step 4 falls back to git diff.
#
set -euo pipefail
trap 'echo "##### Error on line $LINENO — exit code $?"' ERR

# ── Config ────────────────────────────────────────────────────────────────────
# Fat tgz includes node_modules (prod-only) — installs in seconds via sf plugins link.
CLEANZ_TGZ_URL="https://github.com/NaveenGIT9/Cleanz-Plugin/raw/master/naveengit9-plugin-cleanz-fat-1.0.7.tgz"
CLEANZ_PLUGIN_DIR="/tmp/cleanz-plugin"
PROMOTION_JSON="/tmp/copado_promotion_changes.json"
ORG_ALIAS="cleanz-dest"
API_VERSION="v62.0"

# ── Step 1: Clone promotion branch ────────────────────────────────────────────
copado -p "pre_cleanz | Step 1: Cloning promotion branch: $branch"
copado-git-get "$branch"


# ── Step 2: Install sf cleanz plugin (fat tgz — no npm download needed) ──────
# Fat tgz ships with node_modules (prod-only). We extract it and use
# "sf plugins link" to register the local directory — zero npm downloads,
# no unsigned-plugin prompt, completes in seconds.
copado -p "pre_cleanz | Step 2: Installing sf cleanz plugin from fat tgz"
curl -sSL "$CLEANZ_TGZ_URL" -o /tmp/plugin-cleanz-fat.tgz
mkdir -p "$CLEANZ_PLUGIN_DIR"
tar -xzf /tmp/plugin-cleanz-fat.tgz -C "$CLEANZ_PLUGIN_DIR"
sf plugins link "$CLEANZ_PLUGIN_DIR" --no-prompt 2>/dev/null || sf plugins link "$CLEANZ_PLUGIN_DIR"
echo "  -> cleanz plugin linked: $CLEANZ_PLUGIN_DIR"

# ── Step 3: Auth SF CLI against destination org ───────────────────────────────
# "sf org login access-token" has a strict internal regex that rejects Copado's
# SOAP session token format (e.g. base64 = padding, special chars).
# Bypass it entirely: write the auth files directly to the SF CLI v2 store so
# the token is stored as-is and used as a Bearer header without re-validation.
copado -p "pre_cleanz | Step 3: Authenticating SF CLI to destination org"
node << 'AUTH_EOF'
'use strict';
const https = require('https');
const fs    = require('fs');
const path  = require('path');
const os    = require('os');
const { URL } = require('url');

const instanceUrl = (process.env.destinationInstanceUrl || '').replace(/\/+$/, '');
const sessionId   = process.env.destinationSessionid;
if (!instanceUrl || !sessionId) { console.error('Missing destinationInstanceUrl or destinationSessionid'); process.exit(1); }

(async () => {
    // Fetch real username + orgId — needed for the auth file names.
    const parsed = new URL('/services/oauth2/userinfo', instanceUrl);
    const uiRes = await new Promise((res, rej) => {
        const req = https.request({ hostname: parsed.hostname, path: parsed.pathname,
            method: 'GET', headers: { 'Authorization': 'Bearer ' + sessionId } }, (r) => {
            const buf = [];
            r.on('data', c => buf.push(c));
            r.on('end', () => res({ status: r.statusCode, body: Buffer.concat(buf).toString() }));
        });
        req.on('error', rej);
        req.end();
    });
    if (uiRes.status !== 200) { console.error('userinfo failed (' + uiRes.status + '): ' + uiRes.body); process.exit(1); }
    const ui       = JSON.parse(uiRes.body);
    const username = ui.preferred_username || ui.email;
    const orgId    = ui.organization_id || '';

    // Copado's destinationInstanceUrl may be the generic login URL (test.salesforce.com)
    // not the org's real instance URL.  Extract the real URL from userinfo.urls.custom_domain
    // (present in most orgs) or from the profile field.  Using the real instance URL is
    // required for Metadata API deploy calls — test.salesforce.com returns HTTP 405.
    const realInstanceUrl = (ui.urls && ui.urls.custom_domain)
        ? ui.urls.custom_domain.replace(/\/+$/, '')
        : (ui.profile ? ((ui.profile.match(/^(https:\/\/[^\/]+)/) || [])[1] || instanceUrl) : instanceUrl);

    const isSandbox = realInstanceUrl.includes('--') || realInstanceUrl.includes('.sandbox.');
    const loginUrl  = isSandbox ? 'https://test.salesforce.com' : 'https://login.salesforce.com';
    console.log('  -> Session valid for: ' + username + '  orgId: ' + orgId);
    console.log('  -> Instance URL used: ' + realInstanceUrl);

    function writeAuth(dir, filename, uname) {
        fs.mkdirSync(dir, { recursive: true });
        const obj = { orgId, username: uname, accessToken: sessionId,
                      instanceUrl: realInstanceUrl, loginUrl, clientId: 'PlatformCLI', isDevHub: false };
        fs.writeFileSync(path.join(dir, filename + '.json'), JSON.stringify(obj, null, 2), 'utf8');
    }

    const sfOrgsDir = path.join(os.homedir(), '.sf', 'orgs');
    const sfdxDir   = path.join(os.homedir(), '.sfdx');

    // Write under real username (for alias-based lookup)
    writeAuth(sfOrgsDir, username, username);
    writeAuth(sfdxDir,   username, username);
    // ALSO write under 'cleanz-dest' so --target-org cleanz-dest finds the file
    // directly even if alias resolution doesn't work in this SF CLI version.
    writeAuth(sfOrgsDir, 'cleanz-dest', username);
    writeAuth(sfdxDir,   'cleanz-dest', username);
    console.log('  -> Auth written for ' + username + ' and cleanz-dest in ~/.sf/orgs/ + ~/.sfdx/');

    // Alias: cleanz-dest → real username  (~/.sf/alias.json)
    const aliasFile = path.join(os.homedir(), '.sf', 'alias.json');
    let aliases = { orgs: {} };
    if (fs.existsSync(aliasFile)) { try { aliases = JSON.parse(fs.readFileSync(aliasFile, 'utf8')); } catch {} }
    if (!aliases.orgs) aliases.orgs = {};
    aliases.orgs['cleanz-dest'] = username;
    fs.writeFileSync(aliasFile, JSON.stringify(aliases, null, 2), 'utf8');
    console.log('  -> Alias cleanz-dest -> ' + username);
})();
AUTH_EOF

# Verify SF CLI can actually resolve cleanz-dest before handing to cleanz.
echo "  -> SF CLI org list (filtered):"
sf org list 2>&1 | grep -iE "(trnqa|cleanz|No orgs)" | head -5 || true
echo "  -> SF CLI display cleanz-dest:"
sf org display --target-org cleanz-dest 2>&1 | grep -E "(Username|Status|Instance)" | head -4 || true
echo "  -> SF CLI auth configured for alias: $ORG_ALIAS"

# ── Step 4: Build component list from Copado ContentDocumentLink ──────────────
# Primary: query ContentDocumentLink on the Promotion record from the pipeline org
# (sourceInstanceUrl + sourceSessionid) — this gives the exact set of components
# Copado is promoting, matching the same JSON cleanz uses interactively.
# Fallback: git diff against the default branch. Less accurate because it includes
# ANY permset/profile changed in the branch, not just the promoted components.
copado -p "pre_cleanz | Step 4: Building component list (Copado attachment → git diff fallback)"

node << 'NODE_EOF'
'use strict';
const { execSync } = require('child_process');
const https  = require('https');
const fs     = require('fs');
const { URL } = require('url');

const OUTPUT_FILE = '/tmp/copado_promotion_changes.json';

const EXT_MAP = {
  '.permissionset-meta.xml':       'PermissionSet',
  '.mutingpermissionset-meta.xml': 'MutingPermissionSet',
  '.permissionsetgroup-meta.xml':  'PermissionSetGroup',
  '.profile-meta.xml':             'Profile',
  '.layout-meta.xml':              'Layout',
  '.reportType-meta.xml':          'ReportType',
};

// ── helpers ──────────────────────────────────────────────────────────────────
function httpsGet(urlStr, token) {
    return new Promise((resolve, reject) => {
        const u = new URL(urlStr);
        const req = https.request(
            { hostname: u.hostname, path: u.pathname + u.search,
              method: 'GET', headers: { Authorization: 'Bearer ' + token } },
            (r) => {
                const chunks = [];
                r.on('data', c => chunks.push(c));
                r.on('end', () => resolve({ status: r.statusCode, body: Buffer.concat(chunks).toString() }));
            }
        );
        req.on('error', reject);
        req.end();
    });
}

// ── primary: query ContentDocumentLink from the Copado pipeline org ──────────
async function tryCopadoAttachment() {
    const promotionId   = process.env.promotionId;
    // instanceUrl / sessionId are auto-injected by Copado into every Function
    // container — they point to the PIPELINE ORG (RBKPIPEQA), not source/dest.
    const pipelineUrl   = (process.env.instanceUrl || '').replace(/\/+$/, '');
    const pipelineToken = process.env.sessionId;

    if (!promotionId || !pipelineUrl || !pipelineToken) {
        console.log('  [Step 4] instanceUrl/sessionId (pipeline org) not found — using git diff fallback');
        return null;
    }

    console.log(`  [Step 4] Querying Copado attachments for promotion ${promotionId} at ${pipelineUrl}`);

    // 1. Find JSON ContentDocuments linked to this promotion
    const soql = `SELECT ContentDocumentId, ContentDocument.Title, ContentDocument.LatestPublishedVersionId `
               + `FROM ContentDocumentLink `
               + `WHERE LinkedEntityId = '${promotionId}' `
               + `AND ContentDocument.FileExtension = 'json' `
               + `ORDER BY ContentDocument.CreatedDate DESC LIMIT 20`;
    let queryRes;
    try {
        queryRes = await httpsGet(
            pipelineUrl + '/services/data/v62.0/query?q=' + encodeURIComponent(soql),
            pipelineToken
        );
    } catch (e) {
        console.log('  [Step 4] Pipeline org query error: ' + e.message + ' — using git diff fallback');
        return null;
    }

    if (queryRes.status !== 200) {
        console.log(`  [Step 4] ContentDocumentLink query returned HTTP ${queryRes.status} — using git diff fallback`);
        return null;
    }

    const records = JSON.parse(queryRes.body).records || [];
    console.log(`  [Step 4] Found ${records.length} JSON attachment(s) on promotion`);

    // 2. Download each attachment and find the Copado component list
    for (const rec of records) {
        const versionId = rec.ContentDocument && rec.ContentDocument.LatestPublishedVersionId;
        const title     = (rec.ContentDocument && rec.ContentDocument.Title) || '';
        if (!versionId) continue;

        let vRes;
        try {
            vRes = await httpsGet(
                pipelineUrl + '/services/data/v62.0/sobjects/ContentVersion/' + versionId + '/VersionData',
                pipelineToken
            );
        } catch { continue; }

        if (vRes.status !== 200) continue;

        let parsed;
        try { parsed = JSON.parse(vRes.body); } catch { continue; }

        // Validate: must be an array of objects with at least t (type) and n (name)
        if (Array.isArray(parsed) && parsed.length > 0 && parsed[0].t && parsed[0].n) {
            console.log(`  [Step 4] Using Copado attachment: "${title}" — ${parsed.length} item(s)`);
            return parsed;
        }
    }

    console.log('  [Step 4] No valid Copado component list found in attachments — using git diff fallback');
    return null;
}

// ── fallback: derive from git diff ───────────────────────────────────────────
function buildFromGitDiff() {
    console.log('  [Step 4] Building component list from git diff (less accurate)');
    let changedFiles = [];
    try {
        const defaultBranch = execSync(
            "git remote show origin 2>/dev/null | grep 'HEAD branch' | awk '{print $NF}'",
            { encoding: 'utf8' }
        ).trim() || 'main';

        const raw = execSync(
            `git diff origin/${defaultBranch}...HEAD --name-only 2>/dev/null || git diff HEAD~1..HEAD --name-only`,
            { encoding: 'utf8' }
        );
        changedFiles = raw.trim().split('\n').filter(Boolean);
        console.log(`  [Step 4] git diff vs origin/${defaultBranch}: ${changedFiles.length} changed file(s)`);
    } catch (e) {
        console.error('  [Step 4] git diff failed: ' + e.message);
    }

    const items = [];
    for (const file of changedFiles) {
        for (const [ext, type] of Object.entries(EXT_MAP)) {
            if (file.endsWith(ext)) {
                const basename = file.split('/').pop().replace(ext, '');
                items.push({ t: type, n: basename, a: 'Add' });
                break;
            }
        }
    }
    return items;
}

// ── main ─────────────────────────────────────────────────────────────────────
(async () => {
    let items  = await tryCopadoAttachment();
    const src  = items ? 'Copado ContentDocumentLink' : 'git diff';
    if (!items) items = buildFromGitDiff();

    const ps  = items.filter((i) => i.t === 'PermissionSet').length;
    const mps = items.filter((i) => i.t === 'MutingPermissionSet').length;
    const psg = items.filter((i) => i.t === 'PermissionSetGroup').length;
    const pr  = items.filter((i) => i.t === 'Profile').length;
    const rt  = items.filter((i) => i.t === 'ReportType').length;
    const ly  = items.filter((i) => i.t === 'Layout').length;

    console.log(`  [Step 4] Source: ${src}`);
    console.log(`  [Step 4] ${items.length} cleanable item(s) | PS:${ps} MPS:${mps} PSG:${psg} Profile:${pr} ReportType:${rt} Layout:${ly}`);
    items.filter(i => ['PermissionSet','MutingPermissionSet','PermissionSetGroup'].includes(i.t))
         .forEach(i => console.log(`     - ${i.n} [${i.t}]`));
    items.filter(i => i.t === 'Profile').forEach(i => console.log(`     - ${i.n} [Profile]`));
    items.filter(i => i.t === 'Layout').forEach(i => console.log(`     - ${i.n} [Layout]`));

    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(items, null, 2), 'utf8');
})();
NODE_EOF

# If the attachment wasn't found, the JSON is [] — nothing for cleanz to fix.
# Exit 0 here so SFDX Deploy proceeds normally without running cleanz at all.
if [ "$(cat "$PROMOTION_JSON")" = "[]" ]; then
    copado -p "pre_cleanz | No components in promotion — skipping cleanz, SFDX Deploy can proceed"
    exit 0
fi

# ── Step 5: Run sf cleanz ─────────────────────────────────────────────────────
# --json-path supplied → cleanz auto-selects option 1 (validate & clean).
# cleanz fixes XML files, commits the changes, and pushes back to the branch.
# The subsequent SFDX Deploy step picks up the already-cleaned branch.
copado -p "pre_cleanz | Step 5: Running sf cleanz (10 min timeout)"
# 600s hard cap — prevents cleanz looping forever if it hits an unhandled error type.
timeout 600 sf cleanz run \
  --json-path  "$PROMOTION_JSON" \
  --target-org "$ORG_ALIAS" \
  --verbose
EXIT_CODE=$?
if [ $EXIT_CODE -eq 124 ]; then
    echo "##### sf cleanz timed out after 10 minutes — check verbose output above for the stuck error type"
    exit 1
elif [ $EXIT_CODE -ne 0 ]; then
    echo "##### sf cleanz exited with code $EXIT_CODE"
    exit $EXIT_CODE
fi

git push origin "$branch"
copado -p "pre_cleanz | Complete — branch is clean, SFDX Deploy can proceed"
