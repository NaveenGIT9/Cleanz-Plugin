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
# Writes an ~/.sfdx auth file so cleanz can run dry-run deploys against the org.
copado -p "pre_cleanz | Step 3: Authenticating SF CLI to destination org"
node << 'AUTH_EOF'
'use strict';
const fs   = require('fs');
const path = require('path');
const os   = require('os');

const instanceUrl = (process.env.destinationInstanceUrl || '').replace(/\/+$/, '');
const sessionId   = process.env.destinationSessionid;

if (!instanceUrl || !sessionId) {
    console.error('Missing destinationInstanceUrl or destinationSessionid');
    process.exit(1);
}

// SF CLI reads auth from ~/.sfdx/<username>.json
// We use a fixed username that matches the ORG_ALIAS set in bash.
const authDir  = path.join(os.homedir(), '.sfdx');
const authFile = path.join(authDir, 'cleanz-dest.json');

if (!fs.existsSync(authDir)) fs.mkdirSync(authDir, { recursive: true });

const authObj = {
    orgId:       'cleanz-dest',
    username:    'cleanz-dest',
    accessToken: sessionId,
    instanceUrl: instanceUrl,
    loginUrl:    instanceUrl,
    isDevHub:    false,
    created:     new Date().toISOString(),
};

fs.writeFileSync(authFile, JSON.stringify(authObj, null, 2), 'utf8');
console.log('  -> Auth file written: ' + authFile);
AUTH_EOF
echo "  -> SF CLI auth configured for alias: $ORG_ALIAS"

# ── Step 4: Fetch "Copado Promotion changes" attachment ───────────────────────
copado -p "pre_cleanz | Step 4: Fetching promotion components JSON from Salesforce"
node << 'NODE_EOF'
'use strict';
const https  = require('https');
const fs     = require('fs');
const { URL } = require('url');

const instanceUrl  = (process.env.destinationInstanceUrl || '').replace(/\/+$/, '');
const sessionId    = process.env.destinationSessionid;
const promotionId  = process.env.promotionId;
const API_VERSION  = 'v62.0';
const OUTPUT_FILE  = '/tmp/copado_promotion_changes.json';

if (!instanceUrl || !sessionId || !promotionId) {
    console.error('Missing required env vars: destinationInstanceUrl, destinationSessionid, promotionId');
    process.exit(1);
}

function sfGet(urlPath) {
    return new Promise((resolve, reject) => {
        const url = new URL(urlPath, instanceUrl);
        const options = {
            hostname: url.hostname,
            path:     url.pathname + url.search,
            method:   'GET',
            headers: {
                'Authorization': 'Bearer ' + sessionId,
                'Content-Type':  'application/json',
            },
        };
        const req = https.request(options, (res) => {
            const chunks = [];
            res.on('data', (c) => chunks.push(c));
            res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
        });
        req.on('error', reject);
        req.end();
    });
}

(async () => {
    // 1. Find the attachment ID
    const soql = `SELECT Id, Name FROM Attachment `
               + `WHERE ParentId = '${promotionId}' `
               + `AND Name = 'Copado Promotion changes' `
               + `LIMIT 1`;
    const qRes = await sfGet(`/services/data/${API_VERSION}/query?q=${encodeURIComponent(soql)}`);
    if (qRes.status !== 200) {
        console.error(`Attachment query failed (HTTP ${qRes.status}): ${qRes.body.toString()}`);
        process.exit(1);
    }
    const records = JSON.parse(qRes.body.toString()).records || [];
    if (records.length === 0) {
        // No attachment means nothing was promoted — exit cleanly, let deploy proceed.
        console.log('No "Copado Promotion changes" attachment found — nothing for cleanz to fix.');
        fs.writeFileSync(OUTPUT_FILE, '[]', 'utf8');
        process.exit(0);
    }
    const attachmentId = records[0].Id;
    console.log(`  -> Attachment found: ${attachmentId} (${records[0].Name})`);

    // 2. Download the attachment body (raw JSON text from Salesforce)
    const bRes = await sfGet(`/services/data/${API_VERSION}/sobjects/Attachment/${attachmentId}/Body`);
    if (bRes.status !== 200) {
        console.error(`Attachment body fetch failed (HTTP ${bRes.status}): ${bRes.body.toString()}`);
        process.exit(1);
    }

    fs.writeFileSync(OUTPUT_FILE, bRes.body);
    console.log(`  -> Promotion JSON written: ${OUTPUT_FILE} (${bRes.body.length} bytes)`);

    // Validate it's parseable JSON
    try {
        const items = JSON.parse(bRes.body.toString());
        const ps  = items.filter((i) => i.t === 'PermissionSet').length;
        const mps = items.filter((i) => i.t === 'MutingPermissionSet').length;
        const psg = items.filter((i) => i.t === 'PermissionSetGroup').length;
        const pr  = items.filter((i) => i.t === 'Profile').length;
        const rt  = items.filter((i) => i.t === 'ReportType').length;
        const ly  = items.filter((i) => i.t === 'Layout').length;
        console.log(`  -> ${items.length} total items | PS:${ps} MPS:${mps} PSG:${psg} Profile:${pr} ReportType:${rt} Layout:${ly}`);
    } catch (e) {
        console.error('Promotion JSON is not valid JSON — aborting.');
        process.exit(1);
    }
})();
NODE_EOF

# ── Step 5: Run sf cleanz ─────────────────────────────────────────────────────
# --json-path and --target-org both supplied → cleanz skips all interactive prompts.
# cleanz fixes XML files, commits the changes, and pushes back to the branch.
# The subsequent SFDX Deploy step picks up the already-cleaned branch.
copado -p "pre_cleanz | Step 5: Running sf cleanz"
sf cleanz run \
  --json-path  "$PROMOTION_JSON" \
  --target-org "$ORG_ALIAS"

copado -p "pre_cleanz | Complete — branch is clean, SFDX Deploy can proceed"
