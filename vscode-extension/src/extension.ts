import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';
import { exec } from 'child_process';
import { DashboardPanel } from './panels/DashboardPanel';
import type { RunConfig } from './panels/DashboardPanel';
import { SidebarProvider } from './providers/SidebarProvider';

type OrgQuickPickItem = vscode.QuickPickItem & { alias: string };

const ORG_CACHE_TTL_MS = 2 * 60 * 1000;
let _orgCache: { items: OrgQuickPickItem[]; ts: number } | null = null;
let _orgFlight: Promise<OrgQuickPickItem[]> | null = null;

function fetchOrgList(): Promise<OrgQuickPickItem[]> {
  if (_orgCache && Date.now() - _orgCache.ts < ORG_CACHE_TTL_MS) {
    return Promise.resolve(_orgCache.items);
  }
  if (_orgFlight) return _orgFlight;

  _orgFlight = new Promise<OrgQuickPickItem[]>((resolve) => {
    // Use cmd.exe /c on Windows so PATH is resolved the same way as a terminal session,
    // avoiding cases where the extension host PATH differs from the user's shell PATH.
    const cmd = process.platform === 'win32' ? 'cmd.exe /c sf org list --json' : 'sf org list --json';
    exec(cmd, { timeout: 20_000 }, (err: unknown, stdout: string) => {
      try {
        if (err && !stdout?.trim()) {
          console.error('[CleanZ] sf org list error:', err);
          resolve([]);
          return;
        }
        const raw = stdout ?? '';
        const start = raw.indexOf('{');
        const json = JSON.parse(start >= 0 ? raw.substring(start) : raw) as {
          result?: Record<string, Array<{ alias?: string; username?: string; connectedStatus?: string }>>;
        };
        const seen = new Set<string>();
        const items: OrgQuickPickItem[] = [];
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

async function pickOrg(): Promise<string | undefined> {
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
  const pluginPath = await new Promise<string | null>((resolve) => {
    exec(`${sfBin} plugins --json`, { timeout: 10_000 }, (_err: unknown, stdout: string) => {
      try {
        const plugins = JSON.parse(stdout) as Array<{ name: string; root: string }>;
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
  await new Promise<void>((resolve) => {
    exec('npm run build', { cwd: pluginPath, timeout: 60_000 }, () => resolve());
  });
}

export function activate(context: vscode.ExtensionContext) {
  void prewarmPlugin();
  // Register Sidebar WebviewView
  const sidebarProvider = new SidebarProvider(context);
  context.subscriptions.push(vscode.window.registerWebviewViewProvider('cleanz.sidebar', sidebarProvider));

  // Command: Open Dashboard
  context.subscriptions.push(
    vscode.commands.registerCommand('cleanz.openDashboard', () => {
      DashboardPanel.createOrShow(context);
    })
  );

  // Command: Run Cleanz
  // 3 clean options — no persisted settings, always prompts for fresh input.
  context.subscriptions.push(
    vscode.commands.registerCommand('cleanz.run', async () => {
      // Fire org fetch immediately — runs in parallel while user picks action + JSON file.
      void fetchOrgList();

      type ActionItem = vscode.QuickPickItem & { action: 'validate' | 'dryrun' | 'purge' };
      const pick = await vscode.window.showQuickPick<ActionItem>(
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
      let config: RunConfig | undefined;

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
      DashboardPanel.createOrShow(context);
      const panel = DashboardPanel.currentPanel!;

      if (panel.isWebviewReady()) {
        panel.queueRun(config);
      } else {
        vscode.window.withProgress(
          { location: vscode.ProgressLocation.Window, title: 'SF CleanZ: starting…', cancellable: false },
          () =>
            new Promise<void>((resolve) => {
              panel.queueRun(config!);
              panel.onceReady(resolve);
            })
        );
      }
    })
  );

  // Command: Export Report
  context.subscriptions.push(
    vscode.commands.registerCommand('cleanz.exportReport', () => {
      DashboardPanel.currentPanel?.postMessage({ command: 'exportReport' });
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
        DashboardPanel.createOrShow(context);
        DashboardPanel.currentPanel?.postMessage({ command: 'namespacePurge', namespace: ns });
      }
    })
  );

  // Command: Re-run Last
  context.subscriptions.push(
    vscode.commands.registerCommand('cleanz.rerunLast', () => {
      DashboardPanel.currentPanel?.postMessage({ command: 'rerunLast' });
    })
  );

  // Command: Abort
  context.subscriptions.push(
    vscode.commands.registerCommand('cleanz.abort', () => {
      DashboardPanel.currentPanel?.postMessage({ command: 'abort' });
    })
  );
}

export function deactivate() {
  DashboardPanel.currentPanel?.dispose();
}
