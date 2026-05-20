# SF CleanZ — Installation Guide

SF CleanZ is a private Salesforce DevOps tool that removes broken metadata references from Permission Sets, Permission Set Groups, Profiles, and Layouts before deploying, preventing failed deployments caused by missing fields, classes, or objects.

It has two components that must both be installed:

| Component         | File                                 | Purpose                              |
| ----------------- | ------------------------------------ | ------------------------------------ |
| SF CLI Plugin     | `naveengit9-plugin-cleanz-1.0.6.tgz` | Runs `sf cleanz run` in the terminal |
| VS Code Extension | `cleanz-latest.vsix`                 | Dashboard UI inside VS Code          |

---

## Prerequisites

Make sure the following are installed before proceeding:

| Requirement           | Minimum Version                    | Check Command    |
| --------------------- | ---------------------------------- | ---------------- |
| Node.js               | **18.0.0 or higher**               | `node --version` |
| Salesforce CLI (`sf`) | **2.x** (`sf` command, not `sfdx`) | `sf --version`   |
| VS Code               | **1.85.0 or higher**               | Help → About     |

> If `sf --version` shows `@salesforce/cli/1.x`, upgrade to SF CLI 2.x first.

---

## Step 1 — Install the SF CLI Plugin (tgz)

The tgz cannot be installed directly via `sf plugins install` with a file path (known SF CLI limitation). Instead, extract it first and link it.

**1a. Extract the tgz**

Open PowerShell and run:

```powershell
# Create a folder and extract into it
mkdir C:\cleanz-plugin
tar -xzf C:\path\to\naveengit9-plugin-cleanz-1.0.6.tgz -C C:\cleanz-plugin
```

This creates a `package\` folder inside `C:\cleanz-plugin\`.

**1b. Link the plugin**

```powershell
sf plugins link C:\cleanz-plugin\package
```

You will see a warning:

> This plugin is not digitally signed and its authenticity cannot be verified. Continue? (y/N)

Type **y** and press Enter. Installation completes in seconds.

**1c. Verify**

```powershell
sf plugins
```

You should see `@naveengit9/plugin-cleanz` in the list.

---

## Step 2 — Install the VS Code Extension (VSIX)

1. Open VS Code
2. Open the Extensions panel (`Ctrl+Shift+X`)
3. Click the `...` menu (top-right of the Extensions panel)
4. Select **Install from VSIX...**
5. Browse to `cleanz-latest.vsix` and select it
6. VS Code will install and ask to reload — click **Reload**

**Verify:** Press `Ctrl+Shift+P` and type `CleanZ` — you should see **Run CleanZ** in the Command Palette.

> Note: The extension may not appear in the Extensions sidebar list in some VS Code versions — this is normal. As long as the Command Palette shows it, it is installed and active.

---

## Usage

1. Open your Salesforce repository folder in VS Code (`File → Open Folder`)
2. Press `Ctrl+Shift+P` → **Run CleanZ**
3. Select an action:
   - **Validate + Fix** — removes broken refs from files and deploys to org
   - **Dry Run** — removes broken refs from files only, no deploy
   - **Namespace Purge** — removes all references to a specific namespace prefix
4. Select your Promotion JSON or package.xml file when prompted
5. Enter your target org alias (e.g. `RBKQA`)
6. The CleanZ dashboard opens and runs automatically

---

## Troubleshooting

### `cleanz run is not a sf command` (exit code 127)

The build cache has gone stale. The VS Code extension **auto-detects and fixes this automatically** — you will see:

```
⚠ Build cache stale — auto-rebuilding plugin (~10s)...
✓ Rebuild complete — retrying...
```

If it does not auto-fix, run manually in the plugin folder:

```powershell
cd C:\cleanz-plugin\package
Remove-Item -Recurse -Force .wireit
npm run build
```

### `Cannot find package '@salesforce/sf-plugins-core'`

The plugin was linked without installing its dependencies. Run:

```powershell
cd C:\cleanz-plugin\package
npm install
```

### Plugin linked but `sf plugins` does not show it

Re-run the link command:

```powershell
sf plugins link C:\cleanz-plugin\package
```

---

## Upgrading

When a new version of CleanZ is released:

1. Download the new `naveengit9-plugin-cleanz-X.X.X.tgz` and `cleanz-latest.vsix`
2. Extract the new tgz to the same folder (overwrite existing files):
   ```powershell
   tar -xzf naveengit9-plugin-cleanz-X.X.X.tgz -C C:\cleanz-plugin
   ```
3. Re-run the link (the existing link updates automatically):
   ```powershell
   sf plugins link C:\cleanz-plugin\package
   ```
4. Uninstall the old VSIX from VS Code Extensions and install the new `cleanz-latest.vsix`
