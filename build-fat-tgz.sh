#!/bin/bash
#
# build-fat-tgz.sh
# Creates a "fat" tgz that bundles production node_modules.
# When used in the Copado container via "sf plugins link /tmp/cleanz-plugin",
# no npm downloading happens — installs in seconds instead of ~20 min.
#
# Usage:  bash build-fat-tgz.sh
#
set -euo pipefail

# cd to the script's directory first — everything uses relative paths from here.
cd "$(dirname "$0")"
PLUGIN_DIR="$(pwd)"

VERSION=$(node -p "require('./package.json').version")
FAT_NAME="naveengit9-plugin-cleanz-fat-${VERSION}.tgz"
BUILD_DIR="/tmp/cleanz-fat-build-$$"

echo "Building fat tgz v${VERSION}..."
echo "  Plugin dir: ${PLUGIN_DIR}"
echo "  Build dir:  ${BUILD_DIR}"
echo ""

# ── Step 1: Generate oclif manifest + npm-shrinkwrap ─────────────────────────
# Run independently (skip full yarn build + lint — lib/ is already compiled).
echo "  [1/4] Generating oclif manifest and shrinkwrap..."
./node_modules/.bin/oclif manifest 2>/dev/null || true
./node_modules/.bin/oclif lock    2>/dev/null || true
npm shrinkwrap --silent

# ── Step 2: Stage files in isolated temp dir ──────────────────────────────────
echo "  [2/4] Staging files in temp dir..."
mkdir -p "$BUILD_DIR"

cp -r  lib               "${BUILD_DIR}/lib"
cp -r  messages          "${BUILD_DIR}/messages"
cp     package.json       "${BUILD_DIR}/package.json"
cp     npm-shrinkwrap.json "${BUILD_DIR}/npm-shrinkwrap.json"
cp     oclif.manifest.json "${BUILD_DIR}/oclif.manifest.json"

[ -f oclif.lock  ] && cp oclif.lock   "${BUILD_DIR}/oclif.lock"
[ -d schemas     ] && cp -r schemas   "${BUILD_DIR}/schemas"
[ -f LICENSE.txt ] && cp LICENSE.txt  "${BUILD_DIR}/LICENSE.txt"
[ -f README.md   ] && cp README.md    "${BUILD_DIR}/README.md"

# ── Step 3: Install production-only deps in the staged dir ───────────────────
# Uses npm-shrinkwrap.json for exact versions; --ignore-scripts = no postinstall.
echo "  [3/4] Installing production deps (this may take a minute)..."
cd "$BUILD_DIR"
npm install --omit=dev --ignore-scripts --silent 2>&1 | tail -5 || \
npm install --production --ignore-scripts 2>&1 | tail -5

# ── Step 4: Pack the fat tgz ─────────────────────────────────────────────────
echo "  [4/4] Creating fat archive..."
tar -czf "${PLUGIN_DIR}/${FAT_NAME}" -C "$BUILD_DIR" .

# ── Cleanup ───────────────────────────────────────────────────────────────────
rm -rf "$BUILD_DIR"
cd "$PLUGIN_DIR"
rm -f npm-shrinkwrap.json oclif.manifest.json oclif.lock

SIZE=$(du -sh "${PLUGIN_DIR}/${FAT_NAME}" | cut -f1)
echo ""
echo "  Done: ${FAT_NAME}  (${SIZE})"
echo ""
echo "Push commands:"
echo "  cd ${PLUGIN_DIR}"
echo "  git add ${FAT_NAME} build-fat-tgz.sh pre_cleanz.sh"
echo "  git commit -m 'chore: add fat tgz v${VERSION} for instant Copado container install'"
echo "  git push origin master"
