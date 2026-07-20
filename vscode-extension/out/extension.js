'use strict';
var __createBinding =
  (this && this.__createBinding) ||
  (Object.create
    ? function (o, m, k, k2) {
        if (k2 === undefined) k2 = k;
        var desc = Object.getOwnPropertyDescriptor(m, k);
        if (!desc || ('get' in desc ? !m.__esModule : desc.writable || desc.configurable)) {
          desc = {
            enumerable: true,
            get: function () {
              return m[k];
            },
          };
        }
        Object.defineProperty(o, k2, desc);
      }
    : function (o, m, k, k2) {
        if (k2 === undefined) k2 = k;
        o[k2] = m[k];
      });
var __setModuleDefault =
  (this && this.__setModuleDefault) ||
  (Object.create
    ? function (o, v) {
        Object.defineProperty(o, 'default', { enumerable: true, value: v });
      }
    : function (o, v) {
        o['default'] = v;
      });
var __importStar =
  (this && this.__importStar) ||
  (function () {
    var ownKeys = function (o) {
      ownKeys =
        Object.getOwnPropertyNames ||
        function (o) {
          var ar = [];
          for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
          return ar;
        };
      return ownKeys(o);
    };
    return function (mod) {
      if (mod && mod.__esModule) return mod;
      var result = {};
      if (mod != null)
        for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== 'default') __createBinding(result, mod, k[i]);
      __setModuleDefault(result, mod);
      return result;
    };
  })();
Object.defineProperty(exports, '__esModule', { value: true });
exports.activate = activate;
exports.deactivate = deactivate;
const vscode = __importStar(require('vscode'));
const path = __importStar(require('path'));
const fs = __importStar(require('fs'));
const child_process_1 = require('child_process');
const DashboardPanel_1 = require('./panels/DashboardPanel');
const SidebarProvider_1 = require('./providers/SidebarProvider');
const ORG_CACHE_TTL_MS = 2 * 60 * 1000;
let _orgCache = null;
let _orgFlight = null;
function fetchOrgList() {
  if (_orgCache && Date.now() - _orgCache.ts < ORG_CACHE_TTL_MS) {
    return Promise.resolve(_orgCache.items);
  }
  if (_orgFlight) return _orgFlight;
  _orgFlight = new Promise((resolve) => {
    // Use cmd.exe /c on Windows so PATH is resolved the same way as a terminal session,
    // avoiding cases where the extension host PATH differs from the user's shell PATH.
    const cmd = process.platform === 'win32' ? 'cmd.exe /c sf org list --json' : 'sf org list --json';
    (0, child_process_1.exec)(cmd, { timeout: 20000 }, (err, stdout) => {
      try {
        if (err && !stdout?.trim()) {
          console.error('[CleanZ] sf org list error:', err);
          resolve([]);
          return;
        }
        const raw = stdout ?? '';
        const start = raw.indexOf('{');
        const json = JSON.parse(start >= 0 ? raw.substring(start) : raw);
        const seen = new Set();
        const items = [];
        for (const group of Object.values(json?.result ?? {})) {
          if (!Array.isArray(group)) continue;
          for (const o of group) {
            const key = o.alias ?? o.username ?? '';
            if (!key || seen.has(key)) continue;
            seen.add(key);
            const connected = (o.connectedStatus ?? '').toLowerCase() === 'connected';
            const displayName = o.alias ?? o.username ?? '';
            items.push({
              label: `${connected ? '🟢' : '🔴'} ${displayName}`,
              detail: o.alias && o.username ? o.username : '',
              alias: displayName,
            });
          }
        }
        _orgCache = { items, ts: Date.now() };
        resolve(items);
      } catch (e) {
        console.error('[CleanZ] sf org list parse error:', e);
        resolve([]);
      } finally {
        _orgFlight = null;
      }
    });
  });
  return _orgFlight;
}
async function pickOrg() {
  const orgs = await fetchOrgList();
  if (orgs.length === 0) {
    return vscode.window.showInputBox({
      title: 'SF CleanZ — Target Org',
      prompt: 'Org alias or username (no orgs found via sf org list)',
      placeHolder: 'RBKQA',
      ignoreFocusOut: true,
      validateInput: (v) => (v?.trim() ? null : 'Org alias is required'),
    });
  }
  const picked = await vscode.window.showQuickPick(orgs, {
    title: 'SF CleanZ — Select Target Org',
    placeHolder: 'Select a connected org',
    ignoreFocusOut: true,
  });
  return picked?.alias;
}
async function prewarmPlugin() {
  const sfBin = process.platform === 'win32' ? 'sf.cmd' : 'sf';
  const pluginPath = await new Promise((resolve) => {
    (0, child_process_1.exec)(`${sfBin} plugins --json`, { timeout: 10000 }, (_err, stdout) => {
      try {
        const plugins = JSON.parse(stdout);
        const cleanz = plugins.find((p) => p.name === '@naveengit9/plugin-cleanz');
        resolve(cleanz?.root ?? null);
      } catch {
        resolve(null);
      }
    });
  });
  if (!pluginPath) return;
  const runJsPath = path.join(pluginPath, 'lib', 'commands', 'cleanz', 'run.js');
  if (fs.existsSync(runJsPath)) return;
  // lib/ is missing — compile silently in the background so the first Run doesn't fail with code=127
  const wireitDir = path.join(pluginPath, '.wireit');
  if (fs.existsSync(wireitDir)) fs.rmSync(wireitDir, { recursive: true, force: true });
  await new Promise((resolve) => {
    (0, child_process_1.exec)('npm run build', { cwd: pluginPath, timeout: 60000 }, () => resolve());
  });
}
function activate(context) {
  void prewarmPlugin();
  // Register Sidebar WebviewView
  const sidebarProvider = new SidebarProvider_1.SidebarProvider(context);
  context.subscriptions.push(vscode.window.registerWebviewViewProvider('cleanz.sidebar', sidebarProvider));
  // Command: Open Dashboard
  context.subscriptions.push(
    vscode.commands.registerCommand('cleanz.openDashboard', () => {
      DashboardPanel_1.DashboardPanel.createOrShow(context);
    })
  );
  // Command: Run Cleanz
  // 3 clean options — no persisted settings, always prompts for fresh input.
  context.subscriptions.push(
    vscode.commands.registerCommand('cleanz.run', async () => {
      // Fire org fetch immediately — runs in parallel while user picks action + JSON file.
      void fetchOrgList();
      const pick = await vscode.window.showQuickPick(
        [
          { label: '$(play) Validate + Fix', description: 'Fix refs based on errors and commit', action: 'validate' },
          { label: '$(eye)  Dry Run', description: 'Fix refs based on errors - No Commit', action: 'dryrun' },
          { label: '$(search) Namespace Purge', description: 'Remove all namespace references', action: 'purge' },
        ],
        {
          title: 'SF CleanZ',
          placeHolder: 'Select action',
        }
      );
      if (!pick) return;
      // Collect ALL user inputs first — no panel work in between.
      // Creating/revealing a webview between dialogs causes IPC contention in VS Code's
      // renderer, which freezes the next dialog for 10-20 s on VDI/network storage.
      let config;
      if (pick.action === 'validate' || pick.action === 'dryrun') {
        const uris = await vscode.window.showOpenDialog({
          canSelectMany: false,
          filters: { 'JSON / XML': ['json', 'xml'] },
          title: 'Select Promotion JSON or package.xml',
        });
        if (!uris?.[0]) return;
        const org = await pickOrg();
        if (!org?.trim()) return;
        config = { jsonPath: uris[0].fsPath, org: org.trim(), mode: pick.action, repoSweep: true, verbose: false };
      } else if (pick.action === 'purge') {
        const ns = await vscode.window.showInputBox({
          title: 'SF CleanZ — Namespace Purge',
          prompt: 'Namespace prefix to purge',
          placeHolder: 'TSPC',
          ignoreFocusOut: true,
          validateInput: (v) => (v?.trim() ? null : 'Namespace prefix is required'),
        });
        if (!ns?.trim()) return;
        config = {
          jsonPath: '',
          org: '',
          mode: 'namespace-purge',
          repoSweep: false,
          verbose: false,
          namespace: ns.trim(),
        };
      }
      if (!config) return;
      // All inputs collected — now show the panel. If pre-warmed via sidebar it's instant;
      // otherwise it loads now and _pendingConfig defers the run until ready.
      DashboardPanel_1.DashboardPanel.createOrShow(context);
      const panel = DashboardPanel_1.DashboardPanel.currentPanel;
      if (panel.isWebviewReady()) {
        panel.queueRun(config);
      } else {
        vscode.window.withProgress(
          { location: vscode.ProgressLocation.Window, title: 'SF CleanZ: starting…', cancellable: false },
          () =>
            new Promise((resolve) => {
              panel.queueRun(config);
              panel.onceReady(resolve);
            })
        );
      }
    })
  );
  // Command: Export Report
  context.subscriptions.push(
    vscode.commands.registerCommand('cleanz.exportReport', () => {
      DashboardPanel_1.DashboardPanel.currentPanel?.postMessage({ command: 'exportReport' });
    })
  );
  // Command: Namespace Purge
  context.subscriptions.push(
    vscode.commands.registerCommand('cleanz.namespacePurge', async () => {
      const ns = await vscode.window.showInputBox({
        prompt: 'Enter namespace prefix to purge (e.g. TSPC)',
        placeHolder: 'TSPC',
      });
      if (ns) {
        DashboardPanel_1.DashboardPanel.createOrShow(context);
        DashboardPanel_1.DashboardPanel.currentPanel?.postMessage({ command: 'namespacePurge', namespace: ns });
      }
    })
  );
  // Command: Re-run Last
  context.subscriptions.push(
    vscode.commands.registerCommand('cleanz.rerunLast', () => {
      DashboardPanel_1.DashboardPanel.currentPanel?.postMessage({ command: 'rerunLast' });
    })
  );
  // Command: Abort
  context.subscriptions.push(
    vscode.commands.registerCommand('cleanz.abort', () => {
      DashboardPanel_1.DashboardPanel.currentPanel?.postMessage({ command: 'abort' });
    })
  );
}
function deactivate() {
  DashboardPanel_1.DashboardPanel.currentPanel?.dispose();
}
//# sourceMappingURL=extension.js.map
