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
exports.SidebarProvider = void 0;
const vscode = __importStar(require('vscode'));
const DashboardPanel_1 = require('../panels/DashboardPanel');
class SidebarProvider {
  constructor(_context) {
    this._context = _context;
  }
  resolveWebviewView(webviewView) {
    this._view = webviewView;
    webviewView.webview.options = { enableScripts: true };
    webviewView.webview.html = this._getSidebarHtml();
    // Pre-warm the dashboard panel 1.5 s after the sidebar settles.
    // By the time the user clicks Run CleanZ, the webview is already initialized —
    // createOrShow just reveals it (instant) instead of creating it from scratch (10-20 s).
    setTimeout(() => DashboardPanel_1.DashboardPanel.preWarm(this._context), 1500);
    webviewView.webview.onDidReceiveMessage((message) => {
      switch (message.command) {
        case 'run':
          vscode.commands.executeCommand('cleanz.run');
          break;
        case 'openDashboard':
          vscode.commands.executeCommand('cleanz.openDashboard');
          break;
        case 'exportReport':
          vscode.commands.executeCommand('cleanz.exportReport');
          break;
        case 'namespacePurge':
          vscode.commands.executeCommand('cleanz.namespacePurge');
          break;
        case 'rerunLast':
          vscode.commands.executeCommand('cleanz.rerunLast');
          break;
      }
    });
  }
  updateStats(stats) {
    this._view?.webview.postMessage({ command: 'updateStats', stats });
  }
  _getSidebarHtml() {
    return /* html */ `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:var(--vscode-font-family);background:var(--vscode-sideBar-background);color:var(--vscode-foreground);font-size:12px;padding:8px 0}
    .run-btn{width:calc(100% - 16px);margin:8px;background:var(--vscode-button-background);color:var(--vscode-button-foreground);border:none;border-radius:4px;padding:7px 12px;font-size:12.5px;cursor:pointer;display:flex;align-items:center;gap:6px;justify-content:center;font-family:var(--vscode-font-family)}
    .run-btn:hover{background:var(--vscode-button-hoverBackground)}
    .section-title{font-size:10.5px;color:var(--vscode-descriptionForeground);text-transform:uppercase;letter-spacing:.08em;padding:8px 16px 4px}
    .item{display:flex;align-items:center;gap:8px;padding:5px 16px;cursor:pointer;border-radius:0}
    .item:hover{background:var(--vscode-list-hoverBackground)}
    .item.active{background:var(--vscode-list-activeSelectionBackground);color:var(--vscode-list-activeSelectionForeground)}
    .dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
    .dot-green{background:#4ec9b0}.dot-blue{background:#569cd6}.dot-yellow{background:#cca700}.dot-red{background:#f48771}.dot-gray{background:#666}
    .badge{margin-left:auto;background:var(--vscode-badge-background);color:var(--vscode-badge-foreground);font-size:10px;padding:1px 6px;border-radius:10px}
    .divider{height:1px;background:var(--vscode-sideBarSectionHeader-border);margin:6px 0}
    .action-item{display:flex;align-items:center;gap:8px;padding:5px 16px;cursor:pointer;color:var(--vscode-descriptionForeground)}
    .action-item:hover{background:var(--vscode-list-hoverBackground);color:var(--vscode-foreground)}
    .org-info{padding:8px 16px;font-size:10.5px;color:var(--vscode-descriptionForeground)}
    .org-info span{color:#4ec9b0}
    .stat-row{display:grid;grid-template-columns:1fr 1fr;gap:6px;padding:6px 10px}
    .stat-box{background:var(--vscode-input-background);border-radius:4px;padding:6px 8px;text-align:center}
    .stat-val{font-size:18px;font-weight:600}
    .stat-lbl{font-size:9.5px;color:var(--vscode-descriptionForeground);margin-top:2px}
    .green{color:#4ec9b0}.yellow{color:#cca700}.red{color:#f48771}.blue{color:#569cd6}
  </style>
</head>
<body>

  <button class="run-btn" onclick="send('run')">
    ▶ Run Cleanz
  </button>

  <div class="stat-row" id="statRow" style="display:none">
    <div class="stat-box"><div class="stat-val green" id="sFixed">—</div><div class="stat-lbl">Fixed</div></div>
    <div class="stat-box"><div class="stat-val yellow" id="sWarn">—</div><div class="stat-lbl">Warnings</div></div>
    <div class="stat-box"><div class="stat-val red" id="sAttn">—</div><div class="stat-lbl">Attention</div></div>
    <div class="stat-box"><div class="stat-val blue" id="sDeploy">—</div><div class="stat-lbl">Deploys</div></div>
  </div>

  <div class="divider"></div>
  <div class="section-title">Last Run</div>

  <div class="item active" onclick="send('openDashboard')">
    <div class="dot dot-green"></div>
    <span>Dashboard</span>
  </div>
  <div class="item" onclick="send('openDashboard')">
    <div class="dot dot-blue"></div>
    <span>Permission Sets</span>
    <span class="badge" id="psCount">—</span>
  </div>
  <div class="item" onclick="send('openDashboard')">
    <div class="dot dot-yellow"></div>
    <span>Profiles</span>
    <span class="badge" id="profileCount">—</span>
  </div>
  <div class="item" onclick="send('openDashboard')">
    <div class="dot dot-red"></div>
    <span>PSG</span>
    <span class="badge" id="psgCount">—</span>
  </div>
  <div class="item" onclick="send('openDashboard')">
    <div class="dot dot-gray"></div>
    <span>Muting PermSets</span>
  </div>

  <div class="divider"></div>
  <div class="section-title">Actions</div>

  <div class="action-item" onclick="send('exportReport')">
    <span>↓</span> Export Report
  </div>
  <div class="action-item" onclick="send('namespacePurge')">
    <span>⌕</span> Namespace Purge
  </div>
  <div class="action-item" onclick="send('rerunLast')">
    <span>↻</span> Re-run Last
  </div>

  <div class="divider"></div>
  <div class="org-info" id="orgInfo">Org: <span>—</span></div>

  <script>
    const vscode = acquireVsCodeApi();
    function send(command) { vscode.postMessage({ command }); }

    window.addEventListener('message', e => {
      const msg = e.data;
      if (msg.command === 'updateStats') {
        const s = msg.stats;
        document.getElementById('sFixed').textContent = s.fixed;
        document.getElementById('sWarn').textContent = s.warnings;
        document.getElementById('sAttn').textContent = s.attention;
        document.getElementById('sDeploy').textContent = s.deploys;
        document.getElementById('orgInfo').innerHTML = 'Org: <span>' + s.org + '</span>';
        document.getElementById('statRow').style.display = 'grid';
      }
    });
  </script>
</body>
</html>`;
  }
}
exports.SidebarProvider = SidebarProvider;
//# sourceMappingURL=SidebarProvider.js.map
