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
# Uses the official "sf org login access-token" command so SF CLI writes the auth
# to its own store (~/.sf/orgs/ in v2) — no manual file writes that break on
# version-specific paths.
copado -p "pre_cleanz | Step 3: Authenticating SF CLI to destination org"

# Verify token is live, log username, and write orgId to a temp file.
# sf org login access-token requires the token in "<orgId>!<rawToken>" format —
# Copado's {$Destination.Credential.SessionId} is the raw token without that prefix.
node << 'VERIFY_EOF'
'use strict';
const https = require('https');
const fs    = require('fs');
const { URL } = require('url');
const instanceUrl = (process.env.destinationInstanceUrl || '').replace(/\/+$/, '');
const sessionId   = process.env.destinationSessionid;
if (!instanceUrl || !sessionId) { console.error('Missing destinationInstanceUrl or destinationSessionid'); process.exit(1); }
const parsed = new URL('/services/oauth2/userinfo', instanceUrl);
const req = https.request({ hostname: parsed.hostname, path: parsed.pathname, method: 'GET',
    headers: { 'Authorization': 'Bearer ' + sessionId } }, (res) => {
    const buf = [];
    res.on('data', (c) => buf.push(c));
    res.on('end', () => {
        if (res.statusCode !== 200) { console.error('userinfo failed (' + res.statusCode + ')'); process.exit(1); }
        const ui = JSON.parse(Buffer.concat(buf).toString());
        const username = ui.preferred_username || ui.email;
        const orgId    = ui.organization_id || '';
        console.log('  -> Session valid for: ' + username + '  orgId: ' + orgId);
        fs.writeFileSync('/tmp/cleanz_orgid.txt', orgId, 'utf8');
    });
});
req.on('error', (e) => { console.error('userinfo error: ' + e.message); process.exit(1); });
req.end();
VERIFY_EOF

# Build the correctly formatted token.  If Copado already includes the prefix
# (some envs do), use as-is; otherwise prepend the orgId.
CLEANZ_ORG_ID=$(cat /tmp/cleanz_orgid.txt)
if echo "$destinationSessionid" | grep -q '!'; then
    CLEANZ_TOKEN="$destinationSessionid"
else
    CLEANZ_TOKEN="${CLEANZ_ORG_ID}!${destinationSessionid}"
fi

printf '%s\n' "$CLEANZ_TOKEN" | sf org login access-token \
  --instance-url "$destinationInstanceUrl" \
  --alias         "$ORG_ALIAS" \
  --no-prompt
echo "  -> SF CLI auth configured for alias: $ORG_ALIAS"

# ── Step 4: Build component list from git diff ────────────────────────────────
# No Copado org session available in the container — derive the components
# directly from the git branch instead of querying ContentDocumentLink.
# We diff the promotion branch against the default remote branch (main/master)
# and collect any PermissionSet/MutingPermissionSet/PSG/Profile/Layout/ReportType
# files that changed. Only those are passed to cleanz.
copado -p "pre_cleanz | Step 4: Building component list from git diff"

# Find the default remote branch (main or master)
DEFAULT_BRANCH=$(git remote show origin 2>/dev/null | grep 'HEAD branch' | awk '{print $NF}' || echo "main")
echo "  -> Default branch: $DEFAULT_BRANCH"

# Get all files changed in this promotion branch vs the default branch
CHANGED_FILES=$(git diff "origin/${DEFAULT_BRANCH}...HEAD" --name-only 2>/dev/null || \
                git diff "HEAD~1..HEAD" --name-only 2>/dev/null || echo "")
echo "  -> Changed files: $(echo "$CHANGED_FILES" | grep -c . || echo 0)"

node << 'NODE_EOF'
'use strict';
const { execSync } = require('child_process');
const fs = require('fs');

const OUTPUT_FILE = '/tmp/copado_promotion_changes.json';

// Map file extension to cleanz metadata type
const EXT_MAP = {
  '.permissionset-meta.xml':        'PermissionSet',
  '.mutingpermissionset-meta.xml':  'MutingPermissionSet',
  '.permissionsetgroup-meta.xml':   'PermissionSetGroup',
  '.profile-meta.xml':              'Profile',
  '.layout-meta.xml':               'Layout',
  '.reportType-meta.xml':           'ReportType',
};

// Get changed files (passed via env from the shell above)
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
} catch (e) {
    console.error('  Warning: git diff failed — ' + e.message);
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

const ps  = items.filter((i) => i.t === 'PermissionSet').length;
const mps = items.filter((i) => i.t === 'MutingPermissionSet').length;
const psg = items.filter((i) => i.t === 'PermissionSetGroup').length;
const pr  = items.filter((i) => i.t === 'Profile').length;
const rt  = items.filter((i) => i.t === 'ReportType').length;
const ly  = items.filter((i) => i.t === 'Layout').length;

console.log(`  -> ${items.length} cleanable items | PS:${ps} MPS:${mps} PSG:${psg} Profile:${pr} ReportType:${rt} Layout:${ly}`);
fs.writeFileSync(OUTPUT_FILE, JSON.stringify(items, null, 2), 'utf8');
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

copado -p "pre_cleanz | Complete — branch is clean, SFDX Deploy can proceed"
