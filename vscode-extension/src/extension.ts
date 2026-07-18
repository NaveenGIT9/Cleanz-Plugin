import * as vscode from 'vscode';
import { exec } from 'child_process';
import { DashboardPanel } from './panels/DashboardPanel';
import type { RunConfig } from './panels/DashboardPanel';
import { SidebarProvider } from './providers/SidebarProvider';

type OrgQuickPickItem = vscode.QuickPickItem & { alias: string };

function fetchOrgList(): Promise<OrgQuickPickItem[]> {
  return new Promise((resolve) => {
    const sfBin = process.platform === 'win32' ? 'sf.cmd' : 'sf';
    exec(`${sfBin} org list --json`, { timeout: 15_000 }, (_err: unknown, stdout: string) => {
      try {
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
            items.push({
              label: o.alias ?? o.username ?? '',
              detail: o.alias && o.username ? o.username : '',
              iconPath: connected
                ? new vscode.ThemeIcon('pass', new vscode.ThemeColor('testing.iconPassed'))
                : new vscode.ThemeIcon('error', new vscode.ThemeColor('testing.iconFailed')),
              alias: o.alias ?? o.username ?? '',
            });
          }
        }
        resolve(items);
      } catch {
        resolve([]);
      }
    });
  });
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

export function activate(context: vscode.ExtensionContext) {
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
