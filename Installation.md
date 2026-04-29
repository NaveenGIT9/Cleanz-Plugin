# SF CleanZ — Installation Guide

SF CleanZ has two parts that both need to be installed:

| Part                         | What it does                                                          |
| ---------------------------- | --------------------------------------------------------------------- |
| **SF CLI Plugin**            | The backend — runs validation logic, fixes missing refs, sweeps files |
| **VSCode Extension (.vsix)** | The UI dashboard — input, progress, results, CSV download             |

Installing the `.vsix` alone is **not enough**. Both must be installed.

---

## Prerequisites

- [Salesforce CLI](https://developer.salesforce.com/tools/salesforcecli) (`sf` command) — v2 or later
- [Node.js](https://nodejs.org/) — v18 or later
- [Git](https://git-scm.com/)
- [Visual Studio Code](https://code.visualstudio.com/)
- A Salesforce org authenticated via `sf org login web --alias <your-alias>`

---

## Step 1 — Install the SF CLI Plugin

Clone the repo and link it as a local plugin:

```bash
git clone https://github.com/NaveenGIT9/Cleanz-Plugin.git
cd Cleanz-Plugin
npm install
sf plugins link .
```

Verify the plugin is installed:

```bash
sf plugins
# Should show: @naveengit9/plugin-cleanz (linked)
```

---

## Step 2 — Install the VSCode Extension

1. Open **Visual Studio Code**
2. Go to **Extensions** panel (`Ctrl+Shift+X`)
3. Click the **`...`** menu (top-right of Extensions panel)
4. Select **Install from VSIX...**
5. Browse to `vscode-extension/cleanz-latest.vsix` inside the cloned repo
6. Click **Install**
7. Reload VS Code when prompted

---

## Step 3 — Open the Dashboard

1. Open Command Palette (`Ctrl+Shift+P`)
2. Type **SF CleanZ** and select `SF CleanZ: Open Dashboard`
3. The CleanZ dashboard opens as a panel

---

## Using CleanZ

1. **Paste the Copado promotion JSON** into the input field
   - Get this from your Copado Job Execution or promotion branch
2. **Enter your target org alias** (the org you are deploying to)
3. Click **Run** — CleanZ validates, auto-fixes, and re-validates in a loop
4. Review the results table — each item shows status, removed refs, and error details
5. Download the CSV report if needed

---

## Updating

When you pull new changes from the repo:

```bash
cd Cleanz-Plugin
git pull
npm install
npx tsc -p tsconfig.json
```

Then reinstall the `.vsix` from `vscode-extension/cleanz-latest.vsix` if it was updated.

The SF CLI plugin does **not** need relinking — it reads from the local `lib/` folder automatically.

---

## Troubleshooting

**`sf cleanz` command not found**

- Run `sf plugins` to confirm the plugin is linked
- If missing: `cd Cleanz-Plugin && sf plugins link .`

**Dashboard shows blank / won't open**

- Reload VS Code window (`Ctrl+Shift+P` → `Developer: Reload Window`)
- Reinstall the `.vsix`

**Validation hangs or times out**

- Check your org is authenticated: `sf org display --target-org <alias>`
- Re-authenticate if session expired: `sf org login web --alias <alias>`
