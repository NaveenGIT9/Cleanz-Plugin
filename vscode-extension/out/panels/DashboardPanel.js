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
exports.DashboardPanel = void 0;
const vscode = __importStar(require('vscode'));
const path = __importStar(require('path'));
const fs = __importStar(require('fs'));
const os = __importStar(require('os'));
const child_process_1 = require('child_process');
const CLEANZ_CSV_FILE = path.join(os.tmpdir(), 'cleanz-deploy-summary.csv');
class DashboardPanel {
  _queueLog(level, text) {
    this._logQueue.push({ level, text });
    if (!this._logFlushTimer) {
      this._logFlushTimer = setTimeout(() => {
        this._logFlushTimer = undefined;
        if (this._logQueue.length > 0) {
          this.postMessage({ command: 'logBatch', entries: this._logQueue.splice(0) });
        }
      }, 80);
    }
  }
  _flushLog() {
    if (this._logFlushTimer) {
      clearTimeout(this._logFlushTimer);
      this._logFlushTimer = undefined;
    }
    if (this._logQueue.length > 0) {
      this.postMessage({ command: 'logBatch', entries: this._logQueue.splice(0) });
    }
  }
  // Pre-warm: create the panel silently in the background without stealing focus.
  // Call at extension activation so the webview is ready by the time the user clicks Run.
  static preWarm(context) {
    if (DashboardPanel.currentPanel) return;
    const panel = vscode.window.createWebviewPanel(
      'cleanzDashboard',
      'SF Cleanz',
      { viewColumn: vscode.ViewColumn.Beside, preserveFocus: true },
      {
        enableScripts: true,
        retainContextWhenHidden: true,
        localResourceRoots: [vscode.Uri.joinPath(context.extensionUri, 'media')],
      }
    );
    DashboardPanel.currentPanel = new DashboardPanel(panel, context);
  }
  static createOrShow(context) {
    const column = vscode.window.activeTextEditor?.viewColumn ?? vscode.ViewColumn.One;
    if (DashboardPanel.currentPanel) {
      DashboardPanel.currentPanel._panel.reveal(column);
      return;
    }
    const panel = vscode.window.createWebviewPanel('cleanzDashboard', 'SF Cleanz', column, {
      enableScripts: true,
      retainContextWhenHidden: true,
      localResourceRoots: [vscode.Uri.joinPath(context.extensionUri, 'media')],
    });
    DashboardPanel.currentPanel = new DashboardPanel(panel, context);
  }
  constructor(panel, context) {
    this._disposables = [];
    this._lastCwd = '';
    this._preRunHead = '';
    this._abortDidHardReset = false;
    this._lastBatchItems = [];
    this._aborted = false;
    this._webviewReady = false;
    this._pendingRunDialog = false;
    this._autoRebuildInProgress = false;
    // Batched log queue — flushed every 80 ms to prevent flooding the webview message queue
    this._logQueue = [];
    this._readyCallbacks = [];
    this._panel = panel;
    this._context = context;
    this._panel.webview.html = this._getHtml();
    // Handle messages from the webview UI
    this._panel.webview.onDidReceiveMessage((message) => this._handleMessage(message), null, this._disposables);
    this._panel.onDidDispose(() => this.dispose(), null, this._disposables);
  }
  // Send a message to the webview
  postMessage(message) {
    this._panel.webview.postMessage(message);
  }
  isWebviewReady() {
    return this._webviewReady;
  }
  onceReady(cb) {
    if (this._webviewReady) {
      cb();
    } else {
      this._readyCallbacks.push(cb);
    }
  }
  // Open the run dialog in the webview — deferred if webview JS hasn't loaded yet
  triggerRunDialog() {
    if (this._webviewReady) {
      this._panel.webview.postMessage({ command: 'openRunDialog' });
    } else {
      this._pendingRunDialog = true;
    }
  }
  // Start a run directly from extension host (native input flow) — deferred if webview not ready
  queueRun(config) {
    this._lastConfig = config;
    if (this._webviewReady) {
      this._startCleanzRun(config);
    } else {
      this._pendingConfig = config;
    }
  }
  // Handle incoming messages from the webview
  async _handleMessage(message) {
    switch (message.command) {
      case 'ready': {
        this._webviewReady = true;
        // Fire any onceReady callbacks (e.g. the withProgress resolver)
        const cbs = this._readyCallbacks.splice(0);
        cbs.forEach((cb) => cb());
        if (this._pendingConfig) {
          const cfg = this._pendingConfig;
          this._pendingConfig = undefined;
          this._startCleanzRun(cfg);
        } else if (this._pendingRunDialog) {
          this._pendingRunDialog = false;
          this._panel.webview.postMessage({ command: 'openRunDialog' });
        }
        break;
      }
      case 'startRun': {
        const config = message.config;
        this._lastConfig = config;
        await this._startCleanzRun(config);
        break;
      }
      case 'abort': {
        const proc = this._currentProc;
        if (proc?.pid) {
          const pid = proc.pid;
          this._aborted = true;
          this._currentProc = undefined;
          // Discard any queued log lines — nothing more should reach the UI
          this._logQueue = [];
          if (this._logFlushTimer) {
            clearTimeout(this._logFlushTimer);
            this._logFlushTimer = undefined;
          }
          // Write abort signal to temp dir — run.ts polls this every 3s during deploy
          try {
            fs.writeFileSync(path.join(os.tmpdir(), '.cleanz-abort'), '1', 'utf8');
          } catch {
            /* ignore */
          }
          // Read sf Node.js PID from temp dir (run.ts writes this at startup)
          const pidFile = path.join(os.tmpdir(), '.cleanz-pid');
          let sfPid = 0;
          if (fs.existsSync(pidFile)) {
            try {
              sfPid = parseInt(fs.readFileSync(pidFile, 'utf8').trim(), 10) || 0;
            } catch {
              /* ignore */
            }
          }
          // proc.kill() immediately — signal the process before the async tree-kill finishes
          try {
            proc.kill();
          } catch {
            /* ignore */
          }
          // Async helper — spawns a kill command and resolves when done or timed out.
          // Using async spawn (not spawnSync) keeps the extension-host thread free.
          const runKill = (cmd, args, ms = 3000) =>
            new Promise((resolve) => {
              try {
                const k = (0, child_process_1.spawn)(cmd, args);
                const t = setTimeout(() => {
                  try {
                    k.kill();
                  } catch {
                    /* ignore */
                  }
                  resolve();
                }, ms);
                k.on('close', () => {
                  clearTimeout(t);
                  resolve();
                });
                k.on('error', () => {
                  clearTimeout(t);
                  resolve();
                });
              } catch {
                resolve();
              }
            });
          // All kills fire simultaneously — worst-case 3 s instead of 32 s
          await Promise.all([
            runKill('taskkill', ['/F', '/T', '/PID', String(pid)]),
            sfPid ? runKill('taskkill', ['/F', '/T', '/PID', String(sfPid)]) : Promise.resolve(),
            runKill('wmic', ['process', 'where', "CommandLine like '%cleanz%run%'", 'call', 'terminate']),
            runKill('wmic', ['process', 'where', "CommandLine like '%project%deploy%start%'", 'call', 'terminate']),
          ]);
          // Unlock the UI immediately — don't make the user wait for git reset (can take 60s on VDI)
          const cwd = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? this._lastCwd;
          const mode = this._lastConfig?.mode;
          const sha = this._preRunHead;
          const gitCmd =
            (mode === 'validate' || mode === 'dryrun') && sha ? `git reset --hard ${sha}` : 'git restore .';
          // Mark hard-reset intent NOW so the proc close handler skips stale CSV recovery
          if (gitCmd.startsWith('git reset')) {
            this._abortDidHardReset = true;
            try {
              if (fs.existsSync(CLEANZ_CSV_FILE)) fs.unlinkSync(CLEANZ_CSV_FILE);
            } catch {
              /* ignore */
            }
            this.postMessage({ command: 'statsReset' });
            this.postMessage({
              command: 'abortLog',
              level: 'info',
              text: `⟳ Resetting files to pre-run state — may take a minute on large repos...`,
            });
          } else {
            this.postMessage({ command: 'abortLog', level: 'info', text: '⟳ Discarding partial file changes...' });
          }
          // Send runAborted now so the UI unlocks instantly
          this.postMessage({ command: 'runAborted' });
          // Git runs in background — result appears as a log line when done
          (0, child_process_1.exec)(gitCmd, { cwd }, (err) => {
            if (!err) {
              if (gitCmd.startsWith('git reset')) {
                this.postMessage({
                  command: 'abortLog',
                  level: 'ok',
                  text: `✓ Reset to pre-run commit ${sha.slice(0, 8)} (git reset --hard)`,
                });
              } else {
                this.postMessage({
                  command: 'abortLog',
                  level: 'ok',
                  text: '✓ File changes discarded (git restore .)',
                });
              }
            } else {
              this.postMessage({
                command: 'abortLog',
                level: 'warn',
                text: '⚠ Could not auto-discard changes. Run: git reset --hard or git restore .',
              });
            }
          });
        } else {
          this.postMessage({ command: 'runAborted' });
        }
        break;
      }
      case 'exportReport': {
        await this._exportReport(null);
        break;
      }
      case 'namespacePurge': {
        await this._namespacePurge(message.namespace);
        break;
      }
      case 'rerunLast': {
        if (this._lastConfig) {
          await this._startCleanzRun(this._lastConfig);
        } else {
          this.postMessage({ command: 'log', level: 'warn', text: 'No previous run found.' });
        }
        break;
      }
      case 'triggerRun': {
        // Route through the native command — QuickPick → file dialog → org input — no slow webview modal.
        await vscode.commands.executeCommand('cleanz.run');
        break;
      }
      case 'resetDashboard': {
        // In-place reset — clear all data without reloading HTML.
        // Reloading HTML hides mainContent (display:none initial state) making it feel broken.
        this.postMessage({ command: 'doReset' });
        break;
      }
      case 'browseJson': {
        const uris = await vscode.window.showOpenDialog({
          canSelectMany: false,
          filters: { 'JSON / XML': ['json', 'xml'] },
          title: 'Select Promotion JSON or package.xml',
        });
        if (uris?.[0]) {
          this.postMessage({ command: 'jsonPath', path: uris[0].fsPath });
        }
        break;
      }
    }
  }
  async _startCleanzRun(config) {
    this._aborted = false;
    this._abortDidHardReset = false;
    this._logQueue = [];
    if (this._logFlushTimer) {
      clearTimeout(this._logFlushTimer);
      this._logFlushTimer = undefined;
    }
    // Delete stale files from previous runs — prevents old CSV from bleeding into this run's stats
    try {
      const abortFile = path.join(os.tmpdir(), '.cleanz-abort');
      if (fs.existsSync(abortFile)) fs.unlinkSync(abortFile);
    } catch {
      /* ignore */
    }
    try {
      if (fs.existsSync(CLEANZ_CSV_FILE)) fs.unlinkSync(CLEANZ_CSV_FILE);
    } catch {
      /* ignore */
    }
    this.postMessage({ command: 'runStarted', config });
    const args = ['cleanz', 'run'];
    if (config.jsonPath) args.push('--json-path', `"${config.jsonPath}"`);
    if (config.org) args.push('--target-org', config.org);
    if (config.mode === 'dryrun') args.push('--dry-run');
    if (config.verbose) args.push('--verbose');
    const cwd = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? path.dirname(this._context.extensionPath);
    this._lastCwd = cwd;
    // Snapshot HEAD asynchronously — no thread blocking before the process spawns
    // Fire-and-forget — on VDI/network storage git can take 10-15 s.
    // Don't block the run start: SHA resolves in background before any commits are made.
    this._preRunHead = '';
    (0, child_process_1.exec)('git rev-parse HEAD', { cwd }, (_err, stdout) => {
      if (stdout) this._preRunHead = stdout.trim();
    });
    const proc = (0, child_process_1.spawn)('sf', args, {
      cwd,
      shell: true,
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env, FORCE_COLOR: '0', NO_COLOR: '1' },
    });
    this._currentProc = proc;
    // Immediately tell the UI we're connecting — the CLI takes a few seconds to start
    // on VDI/network storage (SF CLI module loading + git detection). Without this the
    // dashboard shows nothing for 5-30 s after the user confirms the run, which feels broken.
    this._queueLog('dim', '⏳ SF CleanZ starting — connecting to CLI...');
    // ── State ──────────────────────────────────────────────────────
    const menuChoice = config.mode === 'namespace-purge' ? '2' : '1';
    const isNsPurge = config.mode === 'namespace-purge';
    let nsDeleted = 0;
    let nsCleaned = 0;
    let menuAnswered = false;
    let enterAnswered = false;
    let currentPhase = 0;
    const batchItems = [];
    this._lastBatchItems = batchItems;
    let deployCount = 0;
    let jsonFixed = 0;
    let repoFixed = 0;
    let warnCount = 0;
    let attnCount = 0;
    let lineBuffer = '';
    // ── Instant item display: pre-parse JSON before CLI outputs anything ──────
    // This makes the Items table populate immediately instead of after 10+ seconds.
    if (config.jsonPath && config.jsonPath.trim() && fs.existsSync(config.jsonPath)) {
      try {
        const typeMap = {
          PermissionSet: 'PermissionSet',
          PermissionSetGroup: 'PermSetGroup',
          Profile: 'Profile',
          MutingPermissionSet: 'MutingPermSet',
        };
        const raw = JSON.parse(fs.readFileSync(config.jsonPath, 'utf8'));
        const seen = new Set();
        const allKnown = raw
          .filter((i) => typeMap[i.t])
          .filter((i) => {
            if (seen.has(i.n)) return false;
            seen.add(i.n);
            return true;
          });
        const skipped = allKnown
          .filter((i) => (i.a ?? '').toLowerCase().startsWith('retrieve'))
          .map((i) => ({ name: i.n, reason: i.a ?? 'RetrieveOnly' }));
        const quick = allKnown
          .filter((i) => !(i.a ?? '').toLowerCase().startsWith('retrieve'))
          .map((i) => ({
            name: i.n,
            type: typeMap[i.t],
            op: (i.a ?? '').toLowerCase() === 'full' ? 'FULL' : 'ADD',
            status: 'pending',
            statusLabel: 'Pending',
            refs: '—',
          }));
        if (quick.length > 0 || skipped.length > 0) {
          batchItems.push(...quick);
          this.postMessage({ command: 'items', items: batchItems, skipped });
        }
        // Build whitelist tags — all non-RetrieveOnly non-PS/Profile components being deployed.
        // These are pre-masked before validation so their refs in PermSets/Profiles are never removed.
        const isDeployable = (i) => !i.a || !i.a.toLowerCase().startsWith('retrieve');
        const whitelistTypes = {
          ApexClass: 'ApexClass',
          ApexPage: 'VF Page',
          CustomField: 'Field',
          CustomObject: 'Object',
          CustomApplication: 'App',
          CustomTab: 'Tab',
          CustomMetadata: 'CMT',
          Flow: 'Flow',
          RecordType: 'RecordType',
          Layout: 'Layout',
          FlexiPage: 'FlexiPage',
          CustomPermission: 'Custom Permission',
          ApexTrigger: 'ApexTrigger',
          LightningComponentBundle: 'LWC',
          AuraDefinitionBundle: 'Aura',
          StaticResource: 'Static Resource',
          CustomLabel: 'Custom Label',
          EmailTemplate: 'Email Template',
          Report: 'Report',
          Dashboard: 'Dashboard',
          ValidationRule: 'Validation Rule',
          WorkflowRule: 'Workflow Rule',
        };
        const whitelistGroups = {};
        for (const i of raw) {
          const label = whitelistTypes[i.t];
          if (label && isDeployable(i)) {
            if (!whitelistGroups[label]) whitelistGroups[label] = [];
            whitelistGroups[label].push(i.n);
          }
        }
        const whitelistTotal = Object.values(whitelistGroups).reduce((s, a) => s + a.length, 0);
        this.postMessage({ command: 'whitelist', groups: whitelistGroups });
        this.postMessage({ command: 'log', level: 'info', text: `Whitelist: ${whitelistTotal} components pre-loaded` });
      } catch (e) {
        this.postMessage({ command: 'log', level: 'err', text: `[pre-parse] ${e.message ?? e}` });
      }
    }
    // Batch-iteration tracking
    let actualCsvPath; // captured from "Summary CSV saved to :" log line
    let resultsSent = false; // guards against double-send if both parseLine and close handler fire
    const retriedInIteration = new Set(); // items that had refs committed this iteration
    const stillActiveInIteration = new Set(); // items still failing/re-verifying — must not be marked clean
    let batchDeployStarted = false; // true after "Running batch dry-run deploy"
    const emitPhase = (phase, label, pct) => {
      if (phase > currentPhase || (phase === 0 && currentPhase === 0)) {
        currentPhase = phase;
        this.postMessage({ command: 'phase', phase, label, pct });
      }
    };
    const emitItems = () => this.postMessage({ command: 'items', items: batchItems });
    const setItemStatus = (name, status, statusLabel, refs) => {
      const item = batchItems.find((i) => i.name === name);
      if (item) {
        item.status = status;
        item.statusLabel = statusLabel;
        if (refs !== undefined) item.refs = refs;
      }
      this.postMessage({ command: 'itemStatus', name, status, statusLabel, ...(refs !== undefined ? { refs } : {}) });
    };
    // Parse CSV text and send itemStatus with authoritative ref counts for each batch item.
    // Called from both the "Total time:" stdout handler and the close handler so counts
    // are always updated regardless of which fires last.
    const dispatchCsvRefCounts = (csvText) => {
      const lines = csvText.split('\n').filter(Boolean);
      if (lines.length < 2) return;
      const parseCsvRow = (line) => {
        const cols = [];
        let cur = '',
          inQ = false;
        for (const ch of line) {
          if (ch === '"') {
            inQ = !inQ;
          } else if (ch === ',' && !inQ) {
            cols.push(cur);
            cur = '';
          } else {
            cur += ch;
          }
        }
        cols.push(cur);
        return cols.map((s) => s.trim());
      };
      const headers = parseCsvRow(lines[0]);
      const iName = headers.indexOf('Name');
      const iRemoved = headers.indexOf('RemovedFields');
      const iCsvStatus = headers.indexOf('Status');
      const iUnhandled = headers.indexOf('UnhandledErrors');
      let csvJsonFixed = 0;
      for (const line of lines.slice(1)) {
        const cols = parseCsvRow(line);
        const name = cols[iName] ?? '';
        const removed = cols[iRemoved] ?? '';
        const refCount = removed.trim() ? removed.split(';').filter((s) => s.trim()).length : 0;
        csvJsonFixed += refCount;
        const item = batchItems.find((i) => i.name === name);
        if (item) {
          item.refs = String(refCount);
          // Override badge if CSV confirms unhandled errors
          const csvStatus = iCsvStatus >= 0 ? cols[iCsvStatus] ?? '' : '';
          const csvUnhandled = iUnhandled >= 0 ? cols[iUnhandled] ?? '' : '';
          if (csvUnhandled.trim() || /unhandled errors/i.test(csvStatus)) {
            item.status = 'attention';
            item.statusLabel = 'Unhandled Errors';
          }
          this.postMessage({
            command: 'itemStatus',
            name,
            status: item.status,
            statusLabel: item.statusLabel,
            refs: item.refs,
          });
        }
      }
      // Compute authoritative attention count from CSV (replaces regex log-line counting)
      const csvAttnCount = lines.slice(1).filter((line) => {
        const cols = parseCsvRow(line);
        const unhandledVal = iUnhandled >= 0 ? cols[iUnhandled] ?? '' : '';
        return unhandledVal.trim().length > 0;
      }).length;
      if (csvAttnCount > 0 || attnCount === 0) {
        attnCount = csvAttnCount;
      }
      if (csvJsonFixed > 0 || jsonFixed === 0) {
        jsonFixed = csvJsonFixed;
      }
      this.postMessage({
        command: 'stats',
        jsonFixed,
        repoFixed,
        deploys: deployCount,
        warnings: warnCount,
        attention: attnCount,
      });
    };
    const parseLine = (line) => {
      const t = line.trim();
      if (!t) return;
      // ── Phase detection ─────────────────────────────────────────
      if (/Loading promotion JSON/i.test(t)) emitPhase(0, 'Load JSON', 10);
      if (/STARTING AUTOMATED DEPLOY/i.test(t)) emitPhase(1, 'NS Pre-check', 25);
      if (/Running batch dry-run deploy|Attempting deploy|check.only deploy/i.test(t))
        emitPhase(2, 'Dry-run Deploy', 50);
      if (/repo.wide sweep|REPO SWEEP/i.test(t)) emitPhase(3, 'Repo Sweep', 85);
      if (/^Total time:/i.test(t)) {
        this.postMessage({ command: 'runDone' });
        // Send results and per-item ref counts from inside stdout handler — most reliable point:
        // actualCsvPath is already set ("Summary CSV saved to:" always precedes "Total time:")
        // The close handler repeats this as a safety net.
        const earlyPath = actualCsvPath || CLEANZ_CSV_FILE;
        if (fs.existsSync(earlyPath)) {
          try {
            const earlyText = fs.readFileSync(earlyPath, 'utf8');
            if (!resultsSent) {
              resultsSent = true;
              this.postMessage({ command: 'resultsReady', csv: earlyText });
            }
            dispatchCsvRefCounts(earlyText);
          } catch {
            /* ignore — close handler is a fallback */
          }
        }
      }
      const csvMatch = t.match(/Summary CSV saved to\s*:\s*(.+)/i);
      if (csvMatch) actualCsvPath = csvMatch[1].trim();
      // Queue / deploy sub-status — updates progress bar label and color
      if (/\[Queue\] Waiting for queue to clear/i.test(t) || /\[Queue\] Still waiting/i.test(t))
        this.postMessage({ command: 'deployStatus', state: 'queued' });
      if (/\[Queue\] No active deployments/i.test(t) || /\[Queue\] Queue cleared/i.test(t))
        this.postMessage({ command: 'deployStatus', state: 'idle' });
      if (/Still deploying|Deploy in progress|waiting for Salesforce response/i.test(t))
        this.postMessage({ command: 'deployStatus', state: 'deploying' });
      if (/All remaining items passed|deployment.*succeeded|Deploy succeeded/i.test(t))
        this.postMessage({ command: 'deployStatus', state: 'idle' });
      // Items are populated exclusively from JSON pre-parse (instant, no duplicates).
      // CLI stdout section headers are still tracked only to trigger emitItems() at the right time.
      // ── Batch deploy started → mark all non-done items as "Validating" ──
      if (/Running batch dry-run deploy/i.test(t)) {
        batchDeployStarted = true;
        retriedInIteration.clear();
        stillActiveInIteration.clear();
        batchItems.forEach((i) => {
          if (i.status !== 'clean' && i.status !== 'fixed' && i.status !== 'attention') {
            i.status = 'running';
            i.statusLabel = 'Validating';
            this.postMessage({
              command: 'itemStatus',
              name: i.name,
              status: i.status,
              statusLabel: i.statusLabel,
              refs: i.refs,
            });
          }
        });
      }
      // ── Item had refs committed (or skipped in dry-run) → still failing ──
      const committedMatch =
        t.match(/Committed missing ref removals for:\s*(.+)/i) ?? t.match(/Dry run[^:]*skipped commit for:\s*(.+)/i);
      if (committedMatch) {
        const name = committedMatch[1].trim();
        retriedInIteration.add(name);
        setItemStatus(name, 'running', 'Validating');
      }
      // ── Item is re-verifying (consecutive zero-failure check) → keep as running ──
      const reVerifyMatch = t.match(/\[([^\]]+)\]\s+No failures this iteration.*re-verifying/i);
      if (reVerifyMatch) {
        const name = reVerifyMatch[1].trim();
        stillActiveInIteration.add(name);
        setItemStatus(name, 'running', 'Re-verifying...');
      }
      // ── Item still has failures (including masked/unhandled) → keep as running ──
      const failureLineMatch = t.match(/\[([^\]]+)\]\s+\d+\s+failure\(s\)/i);
      if (failureLineMatch) {
        const name = failureLineMatch[1].trim();
        stillActiveInIteration.add(name);
      }
      // ── Batch iteration summary → only items not still-active and not committed passed ──
      const iterMatch = t.match(/Batch Iteration\s+(\d+)\s*\|\s*Active:\s*(\d+)\s*\|\s*Fixed:\s*(\d+)\/(\d+)/i);
      if (iterMatch && batchDeployStarted) {
        batchItems.forEach((i) => {
          if (i.status === 'running' && !retriedInIteration.has(i.name) && !stillActiveInIteration.has(i.name)) {
            i.status = 'clean';
            i.statusLabel = 'Clean ✓';
            if (i.refs === '—') i.refs = '0';
            this.postMessage({
              command: 'itemStatus',
              name: i.name,
              status: i.status,
              statusLabel: i.statusLabel,
              refs: i.refs,
            });
          }
        });
      }
      // ── All items passed on the first try (no second "Batch Iteration" header fires) ──
      // When the deploy succeeds on the first attempt, the batch loop exits immediately
      // and no subsequent iteration header triggers the clear above. Detect the success
      // log line and clear proactively so items don't stay stuck on "Validating".
      if (/All remaining items passed validation/i.test(t) && batchDeployStarted) {
        batchItems.forEach((i) => {
          if (i.status === 'running') {
            i.status = 'clean';
            i.statusLabel = 'Clean ✓';
            if (i.refs === '—') i.refs = '0';
            this.postMessage({
              command: 'itemStatus',
              name: i.name,
              status: i.status,
              statusLabel: i.statusLabel,
              refs: i.refs,
            });
          }
        });
      }
      // ── Count individual ref removals from log lines ─────────────
      // "Removed fieldPermissions for: X", "Removed class block for: X", etc.
      const removedForMatch = t.match(/^\s*Removed .+? for:\s*(.+)/i);
      if (removedForMatch && !/\[(?:Sweep|Repo Sweep|NS Bulk)\]/i.test(t)) {
        jsonFixed++;
        this.postMessage({ command: 'stats', jsonFixed, repoFixed });
        const itemName = removedForMatch[1].trim();
        const item = batchItems.find((i) => i.name === itemName);
        if (item) {
          item.refs = String((parseInt(item.refs) || 0) + 1);
          this.postMessage({
            command: 'itemStatus',
            name: itemName,
            status: item.status,
            statusLabel: item.statusLabel,
            refs: item.refs,
          });
        }
      }
      // Cross-file sweep and repo-wide sweep: "[Sweep] Removed X from file"
      if (/\[(?:Sweep|Repo Sweep)\] Removed .+ from .+/i.test(t)) {
        repoFixed++;
        this.postMessage({ command: 'stats', jsonFixed, repoFixed });
      }
      // Namespace purge: count [DELETE] lines live; use authoritative "Files deleted/cleaned" summary
      if (isNsPurge) {
        if (/^\s*\[DELETE\]/i.test(t)) {
          nsDeleted++;
          this.postMessage({ command: 'nsPurgeStats', deleted: nsDeleted, cleaned: nsCleaned });
        }
        const delMatch = t.match(/Files deleted\s*:\s*(\d+)/i);
        if (delMatch) {
          nsDeleted = parseInt(delMatch[1], 10);
          this.postMessage({ command: 'nsPurgeStats', deleted: nsDeleted, cleaned: nsCleaned });
        }
        const cleanMatch = t.match(/Files cleaned\s*:\s*(\d+)/i);
        if (cleanMatch) {
          nsCleaned = parseInt(cleanMatch[1], 10);
          this.postMessage({ command: 'nsPurgeStats', deleted: nsDeleted, cleaned: nsCleaned });
        }
      }
      // RT / Obj pre-check batch removals: "Removed N profileActionOverrides block(s)"
      const batchRmMatch = t.match(/Removed (\d+) profileActionOverrides block\(s\)/i);
      if (batchRmMatch) {
        jsonFixed += parseInt(batchRmMatch[1], 10);
        this.postMessage({ command: 'stats', jsonFixed, repoFixed });
      }
      // ── Deploy count ─────────────────────────────────────────────
      if (/^Deploy attempt\s+\d+/i.test(t)) {
        deployCount++;
        this.postMessage({ command: 'stats', deploys: deployCount });
      }
      const totalMatch = t.match(/Total deploy calls?\s*[:\-]\s*(\d+)/i);
      if (totalMatch) {
        deployCount = parseInt(totalMatch[1]);
        this.postMessage({
          command: 'stats',
          deploys: deployCount,
          jsonFixed,
          repoFixed,
          warnings: warnCount,
          attention: attnCount,
        });
      }
      // ── Log to UI (filter noise) ─────────────────────────────────
      const skip =
        /^(>|={3,}|Warning: Detected unsettled|await cli\.run|^\^$)/.test(t) ||
        /Enter your choice|Press ENTER to start/.test(t) ||
        t.length === 0;
      if (!skip) {
        const level = /error|failed|✗|×/i.test(t)
          ? 'err'
          : /warn/i.test(t)
          ? 'warn'
          : /✓|√|clean ✓|committed|removed/i.test(t)
          ? 'ok'
          : 'info';
        this._queueLog(level, t);
      }
    };
    // ── stdout ─────────────────────────────────────────────────────
    proc.stdout.on('data', (data) => {
      if (this._aborted || proc !== this._currentProc) return;
      const text = data.toString();
      if (!menuAnswered && text.includes('Enter your choice')) {
        proc.stdin?.write(`${menuChoice}\n`);
        menuAnswered = true;
      }
      if (!enterAnswered && text.includes('Press ENTER to start')) {
        proc.stdin?.write('\n');
        // Do NOT call proc.stdin?.end() — sending EOF to cmd.exe on Windows
        // can disrupt the sf CLI grandchild's event loop and stall deploys.
        enterAnswered = true;
      }
      // Namespace purge: respond to namespace prompt, then close stdin so cmd.exe exits cleanly
      if (text.includes('Enter namespace to purge') && config.namespace) {
        proc.stdin?.write(`${config.namespace}\n`);
        // Closing stdin signals EOF — namespace purge needs no further input and leaving
        // stdin open causes cmd.exe (shell:true) to hang indefinitely on Windows.
        setTimeout(() => {
          try {
            proc.stdin?.destroy();
          } catch {
            /* ignore */
          }
        }, 500);
      }
      // Buffer lines to handle partial chunks
      lineBuffer += text;
      const lines = lineBuffer.split('\n');
      lineBuffer = lines.pop() ?? '';
      lines.forEach(parseLine);
    });
    proc.stderr.on('data', (data) => {
      if (this._aborted || proc !== this._currentProc) return;
      data
        .toString()
        .split('\n')
        .filter(Boolean)
        .forEach((line) => {
          if (!/unsettled top-level await|await cli\.run|\^\s*$/.test(line)) {
            this._queueLog('warn', line.trim());
          }
        });
    });
    proc.on('close', (code) => {
      // Wrap in async IIFE so we can await inside the synchronous event callback
      (async () => {
        this.postMessage({
          command: 'log',
          level: 'info',
          text: `[close] code=${code} aborted=${this._aborted} procMatch=${proc === this._currentProc}`,
        });
        // Aborted or superseded by a newer run — recover stats from CSV + git log then bail
        if (this._aborted || proc !== this._currentProc) {
          const noActiveRun = !this._currentProc; // was set to undefined in abort handler
          this._aborted = false;
          this._abortDidHardReset = false;
          lineBuffer = '';
          this._logQueue = [];
          if (this._logFlushTimer) {
            clearTimeout(this._logFlushTimer);
            this._logFlushTimer = undefined;
          }
          const abortCsvPath = CLEANZ_CSV_FILE;
          // Skip CSV recovery when git reset --hard was done — those stats no longer reflect the branch
          if (noActiveRun && !this._abortDidHardReset && fs.existsSync(abortCsvPath)) {
            try {
              const csvText = fs.readFileSync(abortCsvPath, 'utf8');
              this.postMessage({ command: 'resultsReady', csv: csvText });
              // Count JSON fixes from CSV (authoritative)
              const fl = csvText.split('\n').filter(Boolean);
              let csvJson = 0;
              if (fl.length > 1) {
                const pc = (ln) => {
                  const cs = [];
                  let c = '',
                    q = false;
                  for (const ch of ln) {
                    if (ch === '"') q = !q;
                    else if (ch === ',' && !q) {
                      cs.push(c);
                      c = '';
                    } else c += ch;
                  }
                  cs.push(c);
                  return cs.map((s) => s.trim());
                };
                const hh = pc(fl[0]);
                const ir = hh.indexOf('RemovedFields');
                for (const ln of fl.slice(1)) {
                  const rem = pc(ln)[ir] ?? '';
                  csvJson += rem.trim() ? rem.split(';').filter((s) => s.trim()).length : 0;
                }
              }
              // Count repo-wide sweep fixes from the most recent git commit message (non-blocking)
              await new Promise((resolve) => {
                (0, child_process_1.exec)('git log --oneline -1 HEAD', { cwd, timeout: 3000 }, (_err, stdout) => {
                  let gitRepo = 0;
                  if (stdout) {
                    const gm = stdout.match(/Repo-wide sweep.*?remove (\d+) missing ref/i);
                    if (gm) gitRepo = parseInt(gm[1], 10);
                  }
                  this.postMessage({
                    command: 'stats',
                    jsonFixed: csvJson,
                    repoFixed: gitRepo || repoFixed,
                    deploys: deployCount,
                    warnings: warnCount,
                    attention: attnCount,
                  });
                  resolve();
                });
              });
            } catch {
              /* ignore */
            }
          }
          return;
        }
        // ── Auto-rebuild on stale wireit cache (exit 127) ─────────────
        if (code === 127 && !this._autoRebuildInProgress) {
          this._autoRebuildInProgress = true;
          lineBuffer = '';
          this._currentProc = undefined;
          this._flushLog();
          this._queueLog('warn', '⚠ Build cache stale — auto-rebuilding plugin (~10s)...');
          this._flushLog();
          let rebuilt = false;
          try {
            const pluginPath = await this._findPluginCleanzPath();
            if (pluginPath) {
              // Clear wireit cache then rebuild
              const wireitDir = path.join(pluginPath, '.wireit');
              if (fs.existsSync(wireitDir)) fs.rmSync(wireitDir, { recursive: true, force: true });
              await new Promise((resolve, reject) => {
                (0, child_process_1.exec)('npm run build', { cwd: pluginPath, timeout: 60000 }, (err) =>
                  err ? reject(err) : resolve()
                );
              });
              rebuilt = true;
              this._queueLog('ok', '✓ Rebuild complete — retrying...');
              this._flushLog();
            }
          } catch (e) {
            this._queueLog('err', `✗ Auto-rebuild failed: ${e}`);
            this._flushLog();
          }
          this._autoRebuildInProgress = false;
          if (rebuilt && this._lastConfig) {
            await this._startCleanzRun(this._lastConfig);
          } else {
            this.postMessage({ command: 'runComplete', exitCode: 127 });
          }
          return;
        }
        // ── Normal completion ──────────────────────────────────────────
        if (lineBuffer.trim()) parseLine(lineBuffer);
        lineBuffer = '';
        this._currentProc = undefined;
        this._flushLog();
        const success = code === 0 || code === 13;
        // Final status sweep — resolve any items still showing "Validating"
        batchItems.forEach((i) => {
          if (i.status === 'running') {
            i.status = success ? 'clean' : 'attention';
            i.statusLabel = success ? 'Clean ✓' : 'Check Log';
            // Don't reset refs here — CSV update below sets the authoritative count.
            this.postMessage({
              command: 'itemStatus',
              name: i.name,
              status: i.status,
              statusLabel: i.statusLabel,
              refs: i.refs,
            });
          }
        });
        // Send final stats (ref count is now accurate from incremental counting)
        this.postMessage({
          command: 'stats',
          deploys: deployCount,
          jsonFixed,
          repoFixed,
          warnings: warnCount,
          attention: attnCount,
        });
        // Load conclusion CSV — send resultsReady if not already sent, then dispatch
        // per-item ref counts (safety net in case "Total time:" handler ran first).
        const csvPath = actualCsvPath || CLEANZ_CSV_FILE;
        this.postMessage({
          command: 'log',
          level: 'info',
          text: `[CSV] path="${csvPath}" exists=${fs.existsSync(csvPath)} alreadySent=${resultsSent}`,
        });
        if (fs.existsSync(csvPath)) {
          try {
            const csvText = fs.readFileSync(csvPath, 'utf8');
            if (!resultsSent) {
              resultsSent = true;
              this.postMessage({
                command: 'log',
                level: 'info',
                text: `[CSV] sending resultsReady — ${csvText.split('\n').filter(Boolean).length} lines`,
              });
              this.postMessage({ command: 'resultsReady', csv: csvText });
            }
            dispatchCsvRefCounts(csvText);
          } catch {
            /* ignore */
          }
        }
        const level = success ? 'ok' : 'err';
        this.postMessage({ command: 'log', level, text: `Cleanz finished — exit code ${code}` });
        this.postMessage({ command: 'runComplete', exitCode: code });
        // Auto-export report — skip for namespace purge (no CSV, and old validate+fix CSV would be exported)
        if (!isNsPurge) {
          this._exportReport(null).catch(() => {
            /* ignore export errors */
          });
        }
      })(); // end async IIFE
    });
  }
  async _exportReport(_data) {
    const csvPath = CLEANZ_CSV_FILE;
    if (!fs.existsSync(csvPath)) {
      vscode.window.showWarningMessage('No summary file found — run Cleanz first.');
      return;
    }
    // Branch name
    let branch = 'unknown-branch';
    await new Promise((resolve) => {
      (0, child_process_1.exec)('git rev-parse --abbrev-ref HEAD', { cwd: this._lastCwd }, (_err, stdout) => {
        if (stdout?.trim()) branch = stdout.trim();
        resolve();
      });
    });
    // IST datetime
    const now = new Date();
    const ist = new Date(now.getTime() + 5.5 * 60 * 60 * 1000);
    const dd = String(ist.getUTCDate()).padStart(2, '0');
    const mm = String(ist.getUTCMonth() + 1).padStart(2, '0');
    const yyyy = ist.getUTCFullYear();
    const hh = ist.getUTCHours();
    const min = String(ist.getUTCMinutes()).padStart(2, '0');
    const ampm = hh >= 12 ? 'PM' : 'AM';
    const h12 = hh % 12 || 12;
    const dateLabel = `${dd}-${mm}-${yyyy} ${h12}:${min}${ampm} IST`;
    const fileDate = `${dd}-${mm}-${yyyy}_${String(h12).padStart(2, '0')}-${min}${ampm}`;
    // Parse CSV
    const lines = fs.readFileSync(csvPath, 'utf8').split('\n').filter(Boolean);
    const header = lines[0].split(',');
    const idx = (col) => header.findIndex((h) => h.toLowerCase() === col.toLowerCase());
    const iName = idx('Name');
    const iRemoved = idx('RemovedFields');
    const iErrors = idx('RemovedErrors');
    const iUnhandledCol = idx('UnhandledErrors');
    const iStatusCol = idx('Status');
    const parseRow = (line) => {
      const cols = [];
      let cur = '',
        inQ = false;
      for (const ch of line) {
        if (ch === '"') {
          inQ = !inQ;
        } else if (ch === ',' && !inQ) {
          cols.push(cur);
          cur = '';
        } else {
          cur += ch;
        }
      }
      cols.push(cur);
      return cols;
    };
    const rows = lines.slice(1).map(parseRow);
    // Split a cell value into individual lines (semicolon or pipe separated)
    const splitLines = (val) =>
      val.trim()
        ? val
            .split(/[;|]/)
            .map((s) => s.trim())
            .filter(Boolean)
        : [];
    // Build HTML rows
    const rowsHtml = rows
      .map((cols) => {
        const name = cols[iName] ?? '';
        const removed = cols[iRemoved] ?? '';
        const errors = cols[iErrors] ?? '';
        const unhandled = iUnhandledCol >= 0 ? cols[iUnhandledCol] ?? '' : '';
        const remLines = splitLines(removed);
        const errLines = splitLines(errors);
        const unhandledLines = splitLines(unhandled);
        const remCell = remLines.length
          ? remLines.map((r) => `<div class="line-item line-green">&#10003; ${esc(r)}</div>`).join('')
          : '<span class="cell-muted">—</span>';
        const errCell = errLines.length
          ? errLines.map((e) => `<div class="line-item line-red">&#9888; ${esc(e)}</div>`).join('')
          : '<span class="cell-muted">—</span>';
        const rowClass = unhandledLines.length
          ? 'row-unhandled'
          : errLines.length
          ? 'row-err'
          : remLines.length
          ? 'row-ok'
          : '';
        return `<tr class="${rowClass}">
        <td class="td-name">${esc(name)}</td>
        <td>${remCell}</td>
        <td>${errCell}</td>
      </tr>`;
      })
      .join('\n');
    // Build unhandled errors conclusion section
    const unhandledRows = rows.filter(
      (cols) => splitLines(iUnhandledCol >= 0 ? cols[iUnhandledCol] ?? '' : '').length > 0
    );
    const unhandledConclusionHtml =
      unhandledRows.length > 0
        ? `<div class="unhandled-section">
  <div class="unhandled-header">&#9888;&nbsp; Unhandled / Skipped Errors &mdash; Manual Fix Required</div>
  ${unhandledRows
    .map((cols) => {
      const name = cols[iName] ?? '';
      const status = iStatusCol >= 0 ? cols[iStatusCol] ?? '' : '';
      const errs = splitLines(iUnhandledCol >= 0 ? cols[iUnhandledCol] ?? '' : '');
      return `<div class="unhandled-item">
    <div class="unhandled-name">${esc(name)}<span class="unhandled-status">${esc(status)}</span></div>
    ${errs.map((e) => `<div class="unhandled-err-line">&#8227; ${esc(e)}</div>`).join('')}
  </div>`;
    })
    .join('\n')}
</div>`
        : '';
    const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<title>SF CLEANZ Conclusion — ${branch}</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:'Segoe UI',system-ui,Arial,sans-serif;background:#f0f4f8;color:#1a2030;padding:36px 44px;min-height:100vh}
  .report-header{margin-bottom:28px;border-bottom:2px solid #d0dce8;padding-bottom:18px;display:flex;align-items:flex-end;justify-content:space-between;gap:16px;flex-wrap:wrap}
  .report-brand{font-size:26px;font-weight:800;background:linear-gradient(135deg,#1a6fbf 0%,#2ca8d8 55%,#1890a0 100%);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;letter-spacing:.06em;font-family:'Consolas',monospace}
  .report-sub{text-align:right}
  .report-title{font-size:13px;color:#4a6080;font-family:'Consolas',monospace;font-weight:600}
  .report-meta{font-size:11.5px;color:#7090a8;margin-top:3px}
  table{width:100%;border-collapse:collapse;font-size:13px;background:#fff;border-radius:10px;overflow:hidden;box-shadow:0 2px 16px rgba(30,70,120,0.08)}
  thead tr{background:linear-gradient(90deg,#1a5a8a,#1e7ab0);color:#fff}
  th{text-align:left;padding:13px 16px;font-size:10.5px;text-transform:uppercase;letter-spacing:.1em;font-weight:700;color:rgba(255,255,255,.85)}
  tbody tr{border-bottom:1px solid #e8eff6;transition:background .12s}
  tbody tr:last-child{border-bottom:none}
  tbody tr:hover{background:#f5f9ff}
  tbody tr.row-err{background:#fff8f8}
  tbody tr.row-err:hover{background:#fff0f0}
  tbody tr.row-ok{background:#f8fffc}
  tbody tr.row-ok:hover{background:#f0fff8}
  td{padding:11px 16px;vertical-align:top}
  .td-name{font-weight:600;color:#1a2a40;font-family:'Consolas',monospace;font-size:12px;white-space:nowrap;min-width:220px}
  .line-item{padding:2px 0;line-height:1.6;font-size:12.5px;font-family:'Consolas',monospace}
  .line-green{color:#1a7a50}
  .line-red{color:#c0392b}
  .cell-muted{color:#b0bcc8;font-size:13px}
  .footer{margin-top:20px;font-size:11px;color:#9aacbc;text-align:right}
  .summary-chips{display:flex;gap:10px;margin-bottom:20px;flex-wrap:wrap}
  .chip{background:#fff;border:1px solid #d0dce8;border-radius:20px;padding:5px 14px;font-size:12px;color:#4a6080;box-shadow:0 1px 4px rgba(30,70,120,0.07)}
  .chip b{color:#1a5a8a}
  .chip-clean{background:#f0fff8;border-color:#a0dcc0;color:#1a6a40}.chip-clean b{color:#1a6a40}
  .chip-warn{background:#fff4f4;border-color:#f0a0a0;color:#a02020}.chip-warn b{color:#a02020}
  tbody tr.row-unhandled{background:#fff5f5}
  tbody tr.row-unhandled:hover{background:#ffecec}
  .line-unhandled{color:#b02020;font-weight:600}
  .unhandled-section{margin-top:24px;border:2px solid rgba(180,30,30,.3);border-radius:10px;overflow:hidden;background:#fff8f8}
  .unhandled-header{background:rgba(180,30,30,.1);padding:10px 18px;font-size:11px;font-weight:800;color:#b02020;text-transform:uppercase;letter-spacing:.14em;border-bottom:1px solid rgba(180,30,30,.18)}
  .unhandled-item{padding:10px 18px;border-bottom:1px solid rgba(180,30,30,.1)}
  .unhandled-item:last-child{border-bottom:none}
  .unhandled-name{font-family:'Consolas',monospace;font-size:12px;font-weight:700;color:#1a2a40;margin-bottom:4px}
  .unhandled-status{font-size:10px;font-weight:400;color:#a02020;margin-left:10px;font-family:'Segoe UI',system-ui,Arial,sans-serif}
  .unhandled-err-line{font-size:12px;color:#b02020;padding:2px 0 2px 12px;border-left:2px solid rgba(180,30,30,.3);margin:3px 0;word-break:break-word}
</style>
</head>
<body>
<div class="report-header">
  <div class="report-brand">SF CLEANZ</div>
  <div class="report-sub">
    <div class="report-title">Conclusion &mdash; ${esc(branch)}</div>
    <div class="report-meta">${dateLabel}</div>
  </div>
</div>
${(() => {
  const unhandledCount = rows.filter(
    (c) => splitLines(iUnhandledCol >= 0 ? c[iUnhandledCol] ?? '' : '').length > 0
  ).length;
  const refCount = rows.reduce((n, c) => n + splitLines(c[iRemoved] ?? '').length, 0);
  const statusChip =
    unhandledCount > 0
      ? `<div class="chip chip-warn"><b>${unhandledCount} of ${rows.length}</b> components need manual fixes</div>`
      : `<div class="chip chip-clean"><b>All ${rows.length} components</b> deployed cleanly ✓</div>`;
  return `<div class="summary-chips">
  <div class="chip"><b>${rows.length}</b> components processed</div>
  <div class="chip"><b>${refCount}</b> references removed</div>
  ${statusChip}
</div>`;
})()}
<table>
  <thead>
    <tr>
      <th>PermSet / Profile</th>
      <th>Component References Removed</th>
      <th>Deployment Error (per ref)</th>
    </tr>
  </thead>
  <tbody>
    ${rowsHtml}
  </tbody>
</table>
${unhandledConclusionHtml}
<div class="footer">Generated by SF CLEANZ VS Code Extension</div>
</body>
</html>`;
    function esc(s) {
      return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }
    const safeBranch = branch.replace(/[^a-zA-Z0-9._-]/g, '-');
    const fileName = `SF-CleanZ-${safeBranch}-${fileDate}.html`;
    // Save to ~/Documents/CleanzConclusions/
    const os = require('os');
    const outDir = path.join(os.homedir(), 'Documents', 'CleanzConclusions');
    if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
    const outPath = path.join(outDir, fileName);
    fs.writeFileSync(outPath, html, 'utf8');
    // Auto-open in browser immediately, also show notification for manual access
    vscode.env.openExternal(vscode.Uri.file(outPath));
    vscode.window.showInformationMessage(`Report saved: CleanzConclusions/${fileName}`);
  }
  async _namespacePurge(namespace) {
    // Route through the standard run path — CLI menu option 2 handles purge via stdin.
    // The old approach (--namespace flag) was wrong: that flag does not exist in the CLI.
    await this._startCleanzRun({
      jsonPath: '',
      org: '',
      mode: 'namespace-purge',
      repoSweep: false,
      verbose: false,
      namespace,
    });
  }
  _findPluginCleanzPath() {
    return new Promise((resolve) => {
      (0, child_process_1.exec)('sf plugins --json', { timeout: 10000 }, (_err, stdout) => {
        try {
          const plugins = JSON.parse(stdout);
          const cleanz = plugins.find((p) => p.name === '@naveengit9/plugin-cleanz');
          resolve(cleanz?.root ?? null);
        } catch {
          resolve(null);
        }
      });
    });
  }
  _getHtml() {
    if (!DashboardPanel._htmlCache) {
      const htmlPath = vscode.Uri.joinPath(this._context.extensionUri, 'media', 'dashboard.html');
      DashboardPanel._htmlCache = fs.readFileSync(htmlPath.fsPath, 'utf8');
    }
    return DashboardPanel._htmlCache;
  }
  dispose() {
    DashboardPanel.currentPanel = undefined;
    this._webviewReady = false;
    this._pendingRunDialog = false;
    this._panel.dispose();
    this._disposables.forEach((d) => d.dispose());
    this._disposables = [];
  }
}
exports.DashboardPanel = DashboardPanel;
//# sourceMappingURL=DashboardPanel.js.map
