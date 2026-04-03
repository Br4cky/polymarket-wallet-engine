/* ============================================================================
   Global State & Config
   ============================================================================ */

// DATA_BASE comes from config.js (loaded before this script)
// Falls back to relative path for local dev / if config not set
const DATA_BASE = (typeof CONFIG !== 'undefined' && CONFIG.githubUser !== 'YOUR_GITHUB_USERNAME')
  ? CONFIG.dataBase
  : '../data/';

let data = {
  analytics: null,
  wallets: null,
  markets: null
};

let currentTab = 'dashboard';
let sortState = {};
let detailExpandedRows = {};
let chartInstances = {};
let currentTimeRange = 'all';
let activeWalletsOnly = false;
let walletsData = null; // Full wallets.json with per-position data

/* ============================================================================
   Utility Functions
   ============================================================================ */

function fmt(n, decimals = 2) {
  if (n === null || n === undefined) return '0';
  if (typeof n !== 'number') return String(n);

  if (Math.abs(n) >= 1e9) {
    return (n / 1e9).toFixed(decimals) + 'B';
  } else if (Math.abs(n) >= 1e6) {
    return (n / 1e6).toFixed(decimals) + 'M';
  } else if (Math.abs(n) >= 1e3) {
    return (n / 1e3).toFixed(decimals) + 'K';
  }
  return n.toFixed(decimals);
}

function fmtDollars(n, decimals = 2) {
  if (n === null || n === undefined) return '$0';
  return '$' + fmt(n, decimals);
}

function pnlClass(v) {
  if (v > 0) return 'badge-positive';
  if (v < 0) return 'badge-negative';
  return '';
}

function scoreClass(s) {
  if (s >= 70) return 'badge-high';
  if (s >= 40) return 'badge-mid';
  return 'badge-low';
}

function truncAddr(addr) {
  if (!addr || addr.length < 10) return addr || '';
  return addr.slice(0, 6) + '...' + addr.slice(-4);
}

function polymarketUrl(slug) {
  if (!slug) return '#';
  return `https://polymarket.com/event/${slug}`;
}

function relativeTime(isoStr) {
  if (!isoStr) return '-';
  const ms = Date.now() - new Date(isoStr).getTime();
  const days = Math.floor(ms / (24 * 60 * 60 * 1000));
  if (days === 0) return 'Today';
  if (days === 1) return '1d ago';
  if (days < 7) return `${days}d ago`;
  if (days < 30) return `${Math.floor(days / 7)}w ago`;
  if (days < 365) return `${Math.floor(days / 30)}mo ago`;
  return new Date(isoStr).toLocaleDateString();
}

function getTimeCutoff(range) {
  if (range === 'all') return 0;
  const days = range === '7d' ? 7 : range === '30d' ? 30 : 90;
  return Date.now() - days * 24 * 60 * 60 * 1000;
}

function filterPositionsByTime(positions, range) {
  if (range === 'all') return positions;
  const cutoff = getTimeCutoff(range);
  return positions.filter(p => {
    const ts = p.firstSeenTimestamp ? new Date(p.firstSeenTimestamp).getTime() : 0;
    return ts >= cutoff;
  });
}

function recomputeStats(positions) {
  // Wins/Losses: ONLY closed positions (amount ≈ 0) — resolved predictions
  // Open positions are excluded because the prediction outcome isn't decided yet
  let wins = 0, losses = 0, winSum = 0, lossSum = 0;
  let totalPnl = 0, realizedPnl = 0, unrealizedPnl = 0;
  let totalVolume = 0, openCount = 0, openProfitable = 0, openLosing = 0;
  const uniqueTokens = new Set();

  for (const pos of positions) {
    const pnl = pos.pnl || 0;
    totalPnl += pnl;
    totalVolume += pos.totalBought || 0;
    if (pos.tokenId) uniqueTokens.add(pos.tokenId);

    const isOpen = (pos.amount || 0) > 0.01;

    if (isOpen) {
      openCount++;
      unrealizedPnl += pnl;
      if (pnl > 0.01) openProfitable++;
      else if (pnl < -0.01) openLosing++;
    } else if ((pos.totalBought || 0) > 0.01) {
      // Closed position — resolved prediction
      realizedPnl += pnl;
      if (pnl > 0) { wins++; winSum += pnl; }
      else if (pnl < 0) { losses++; lossSum += -pnl; }
    }
  }

  const resolved = wins + losses;
  const wr = resolved > 0 ? wins / resolved : 0;
  const avgW = wins > 0 ? winSum / wins : 0;
  const avgL = losses > 0 ? lossSum / losses : 0;
  const efficiency = totalVolume > 0 ? totalPnl / totalVolume : 0;
  const edgeRatio = avgL > 0 ? avgW / avgL : (avgW > 0 ? 10 : 0);

  return {
    wins, losses, resolved, wr, avgW, avgL,
    totalPnl, realizedPnl, unrealizedPnl,
    totalVolume,
    uniqueTokens: uniqueTokens.size,
    estimatedMarkets: Math.max(1, Math.ceil(uniqueTokens.size / 2)),
    efficiency, edgeRatio, openCount, openProfitable, openLosing,
  };
}

function openPolymarketProfile(address) {
  window.open(`https://polymarket.com/profile/${address}`, '_blank');
}

/* ============================================================================
   Data Loading
   ============================================================================ */

/**
 * Fetch and decompress a gzipped JSON file
 * Falls back to plain JSON if .gz fetch fails
 */
async function fetchGzJSON(url) {
  try {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error('Not found');
    const ds = new DecompressionStream('gzip');
    const decompressed = resp.body.pipeThrough(ds);
    const text = await new Response(decompressed).text();
    return JSON.parse(text);
  } catch (gzErr) {
    // Fall back to plain JSON (without .gz extension)
    try {
      const plainUrl = url.replace(/\.gz$/, '');
      const resp = await fetch(plainUrl);
      if (!resp.ok) throw new Error('Not found');
      return resp.json();
    } catch {
      return null;
    }
  }
}

async function loadData() {
  try {
    // Only load analytics on startup (~20MB) — wallets (80MB) and markets (38MB)
    // are lazy-loaded when specific features need them
    const analytics = await fetchGzJSON(DATA_BASE + 'analytics.json.gz');
    return { analytics, wallets: null, markets: null };
  } catch (error) {
    console.error('Data load error:', error);
    return { analytics: null, wallets: null, markets: null };
  }
}

let _walletsLoading = null;
async function ensureWalletsLoaded() {
  if (walletsData) return walletsData;
  if (_walletsLoading) return _walletsLoading;
  _walletsLoading = fetchGzJSON(DATA_BASE + 'wallets.json.gz').then(d => {
    walletsData = d;
    _walletsLoading = null;
    return d;
  }).catch(() => {
    _walletsLoading = null;
    return null;
  });
  return _walletsLoading;
}

function updateStatusBar() {
  const statusBar = document.getElementById('statusBar');

  if (!data.analytics) {
    statusBar.textContent = 'No scan data yet. Run the scanner to populate.';
    statusBar.className = 'status error';
  } else {
    const timestamp = data.analytics.timestamp || new Date().toISOString();
    const time = new Date(timestamp).toLocaleTimeString();
    statusBar.textContent = `Data loaded at ${time}`;
    statusBar.className = 'status loaded';
  }
}

function showEmptyState(message = 'No data available') {
  const container = document.querySelector('.container');
  if (!container.querySelector('.empty-message')) {
    const msg = document.createElement('div');
    msg.className = 'empty-message';
    msg.style.cssText = `
      text-align: center;
      padding: 60px 20px;
      color: var(--text-dim);
      font-size: 16px;
    `;
    msg.textContent = message;
    container.appendChild(msg);
  }
}

/* ============================================================================
   Tab System
   ============================================================================ */

function switchTab(tabName) {
  // Hide all content
  document.querySelectorAll('.tab-content').forEach(el => {
    el.classList.remove('active');
  });

  // Update tab buttons
  document.querySelectorAll('.tab').forEach(btn => {
    btn.classList.remove('active');
  });

  // Show selected tab
  const tabEl = document.getElementById('tab-' + tabName);
  if (tabEl) {
    tabEl.classList.add('active');
  }

  // Update active button
  const btnEl = document.querySelector(`[data-tab="${tabName}"]`);
  if (btnEl) {
    btnEl.classList.add('active');
  }

  currentTab = tabName;

  // Lazy render tab content
  switch (tabName) {
    case 'dashboard':
      if (!data.analytics) {
        showEmptyState('No scan data yet. Run the scanner to populate the dashboard.');
      } else {
        renderDashboard();
      }
      break;
    case 'consensus':
      renderConsensus();
      break;
    case 'portfolio':
      renderPortfolio();
      break;
    case 'patterns':
      renderPatterns();
      break;
    case 'signals':
      renderSignals();
      break;
    case 'paper':
      renderPaperTrader();
      break;
  }

  // Scroll to top
  window.scrollTo(0, 0);
}

/* ============================================================================
   Table Rendering Helpers
   ============================================================================ */

function createSortableTable(containerId, columns, rows, onRowClick) {
  const container = document.getElementById(containerId);
  if (!container) return;

  const tbody = container.querySelector('tbody');
  if (!tbody) return;

  // Clear existing rows
  tbody.innerHTML = '';

  // Add rows
  if (!rows || rows.length === 0) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td colspan="${columns.length}" class="empty-state">No data</td>`;
    tbody.appendChild(tr);
    return;
  }

  rows.forEach((row, idx) => {
    const tr = document.createElement('tr');
    tr.dataset.rowIndex = idx;

    columns.forEach(col => {
      const td = document.createElement('td');
      const val = row[col.field];

      if (col.render) {
        td.innerHTML = col.render(val, row);
      } else {
        td.textContent = val !== null && val !== undefined ? String(val) : '-';
      }

      tr.appendChild(td);
    });

    if (onRowClick) {
      tr.style.cursor = 'pointer';
      tr.addEventListener('click', () => onRowClick(row, idx));
    }

    tbody.appendChild(tr);
  });

  // Add sort handlers to headers
  const table = container.closest('.data-table') || container;
  const headers = table.querySelectorAll('th.sortable');

  headers.forEach(header => {
    header.addEventListener('click', () => {
      sortTable(table, header, rows, columns, onRowClick);
    });
  });
}

function sortTable(table, header, rows, columns, onRowClick) {
  const field = header.dataset.field;
  if (!field) return;

  const isAsc = header.classList.contains('sorted-asc');

  // Clear sort indicators
  table.querySelectorAll('th').forEach(h => {
    h.classList.remove('sorted-asc', 'sorted-desc');
  });

  // Apply new sort
  rows.sort((a, b) => {
    let valA = a[field];
    let valB = b[field];

    if (typeof valA === 'string') valA = valA.toLowerCase();
    if (typeof valB === 'string') valB = valB.toLowerCase();

    if (isAsc) {
      return valA > valB ? -1 : 1;
    } else {
      return valA < valB ? -1 : 1;
    }
  });

  if (isAsc) {
    header.classList.add('sorted-desc');
  } else {
    header.classList.add('sorted-asc');
  }

  // Re-render
  const tbody = table.querySelector('tbody');
  tbody.innerHTML = '';

  rows.forEach((row, idx) => {
    const tr = document.createElement('tr');
    tr.dataset.rowIndex = idx;

    columns.forEach(col => {
      const td = document.createElement('td');
      const val = row[col.field];

      if (col.render) {
        td.innerHTML = col.render(val, row);
      } else {
        td.textContent = val !== null && val !== undefined ? String(val) : '-';
      }

      tr.appendChild(td);
    });

    if (onRowClick) {
      tr.style.cursor = 'pointer';
      tr.addEventListener('click', () => onRowClick(row, idx));
    }

    tbody.appendChild(tr);
  });
}

/* ============================================================================
   Detail Panels
   ============================================================================ */

function showDetailPanel(tabName, html) {
  const panel = document.getElementById(`detail-panel-${tabName}`);
  const content = document.getElementById(`detail-content-${tabName}`);

  if (panel && content) {
    content.innerHTML = html;
    panel.classList.remove('hidden');
  }
}

function closeDetailPanel(tabName) {
  const panel = document.getElementById(`detail-panel-${tabName}`);
  if (panel) {
    panel.classList.add('hidden');
  }
}

/* ============================================================================
   Chart Configuration
   ============================================================================ */

const CHART_DEFAULTS = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: {
      labels: {
        color: '#8888a0',
        font: { size: 12 }
      }
    }
  },
  scales: {
    x: {
      ticks: { color: '#8888a0' },
      grid: { color: 'rgba(255,255,255,0.05)' }
    },
    y: {
      ticks: { color: '#8888a0' },
      grid: { color: 'rgba(255,255,255,0.05)' }
    }
  }
};

function destroyChart(id) {
  if (chartInstances[id]) {
    chartInstances[id].destroy();
    delete chartInstances[id];
  }
}

/* ============================================================================
   Dashboard Tab
   ============================================================================ */

function renderDashboard() {
  if (!data.analytics) return;

  let leaderboard = data.analytics.leaderboard || [];

  // Apply active-only filter (last 30 days)
  if (activeWalletsOnly) {
    const cutoff30 = Date.now() - 30 * 24 * 60 * 60 * 1000;
    leaderboard = leaderboard.filter(w => {
      const ts = w.lastActiveTimestamp ? new Date(w.lastActiveTimestamp).getTime() : 0;
      return ts >= cutoff30;
    });
  }

  // If time filtering + wallets data available, recompute stats from filtered positions
  const isFiltered = currentTimeRange !== 'all';
  const walletPositions = walletsData?.wallets || {};

  // Build leaderboard with potentially filtered stats
  const leaderboardData = leaderboard.map((w, idx) => {
    let s = w.stats || {};

    if (isFiltered && walletPositions[w.address]) {
      const allPos = walletPositions[w.address].positions || [];
      const filtered = filterPositionsByTime(allPos, currentTimeRange);
      if (filtered.length > 0) {
        s = recomputeStats(filtered);
      } else {
        s = { totalPnl: 0, realizedPnl: 0, unrealizedPnl: 0, wr: 0, estimatedMarkets: 0, resolved: 0, efficiency: 0, edgeRatio: 0, avgW: 0, avgL: 0, wins: 0, losses: 0, totalVolume: 0, openCount: 0, openProfitable: 0, openLosing: 0 };
      }
    }

    return {
      rank: idx + 1,
      score: w.score || 0,
      address: w.address || '',
      totalPnl: s.totalPnl || 0,
      winRate: s.wr || 0,
      markets: s.estimatedMarkets || 0,
      resolved: s.resolved || 0,
      efficiency: s.efficiency || 0,
      edgeRatio: s.edgeRatio || 0,
      avgW: s.avgW || 0,
      avgL: s.avgL || 0,
      wins: s.wins || 0,
      losses: s.losses || 0,
      volume: s.totalVolume || 0,
      openCount: s.openCount || 0,
      positionsPerWeek: s.positionsPerWeek || 0,
      tradingDays: s.tradingDays || 0,
      suspiciousWinRate: s.suspiciousWinRate || false,
      lastActive: w.lastActiveTimestamp || w.stats?.lastActiveTimestamp || null,
    };
  });

  // Update metric cards (from filtered leaderboard)
  const totalPnl = leaderboardData.reduce((s, w) => s + w.totalPnl, 0);
  const avgScore = leaderboardData.length > 0 ? leaderboardData.reduce((s, w) => s + w.score, 0) / leaderboardData.length : 0;
  // Use pooled win rate (total wins / total resolved) for consistency with patterns tab
  const totalWins = leaderboardData.reduce((s, w) => s + (w.wins || 0), 0);
  const totalResolved = leaderboardData.reduce((s, w) => s + (w.resolved || 0), 0);
  const pooledWinRate = totalResolved > 0 ? totalWins / totalResolved : 0;

  const avgTradesPerWeek = leaderboardData.length > 0
    ? (leaderboardData.reduce((s, w) => s + (w.positionsPerWeek || 0), 0) / leaderboardData.length) : 0;
  const totalOpen = leaderboardData.reduce((s, w) => s + (w.openCount || 0), 0);

  document.getElementById('metric-wallets').textContent = leaderboardData.length.toLocaleString();
  document.getElementById('metric-avg-score').textContent = avgScore.toFixed(1);
  document.getElementById('metric-pnl').textContent = fmtDollars(totalPnl);
  document.getElementById('metric-win-rate').textContent = (pooledWinRate * 100).toFixed(1) + '%';
  document.getElementById('metric-trades-per-week').textContent = avgTradesPerWeek.toFixed(1);
  document.getElementById('metric-open-total').textContent = totalOpen.toLocaleString();

  const leaderboardColumns = [
    { field: 'rank', render: v => String(v) },
    { field: 'score', render: v => `<span class="badge ${scoreClass(v)}">${v.toFixed(1)}</span>` },
    { field: 'address', render: v => `<span class="address-link" onclick="openPolymarketProfile('${v}')">${truncAddr(v)}</span>` },
    { field: 'totalPnl', render: v => `<span class="${pnlClass(v)}">${fmtDollars(v)}</span>` },
    { field: 'winRate', render: (v, row) => {
      const pct = ((v || 0) * 100).toFixed(1) + '%';
      if (row.suspiciousWinRate) return `<span style="color: var(--yellow);" title="Suspicious: 100% WR with unrealized losses in open positions">${pct} ⚠</span>`;
      return pct;
    }},
    { field: 'markets', render: v => String(v) },
    { field: 'resolved', render: v => String(v) },
    { field: 'efficiency', render: v => ((v || 0) * 100).toFixed(2) + '%' },
    { field: 'positionsPerWeek', render: v => `<span style="color: var(--accent-light);">${(v || 0).toFixed(1)}</span>` },
    { field: 'lastActive', render: v => `<span style="color: var(--text-dim); font-size: 12px;">${relativeTime(v)}</span>` }
  ];

  createSortableTable('leaderboard-table', leaderboardColumns, leaderboardData, (row) => {
    const wallet = leaderboard.find(w => w.address === row.address) || row;
    showLeaderboardDetail(wallet);
  });

  // Charts
  renderScoreDistribution();
  renderTrendline();
}

function showLeaderboardDetail(wallet) {
  const s = wallet.stats || {};
  const totalPnl = s.totalPnl || 0;
  const html = `
    <div class="detail-grid">
      <div class="detail-item">
        <div class="detail-item-label">Address</div>
        <div class="detail-item-value" style="font-size: 14px; font-family: monospace;">${wallet.address}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Score</div>
        <div class="detail-item-value">${(wallet.score || 0).toFixed(1)}/100</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Total PnL</div>
        <div class="detail-item-value" style="color: ${totalPnl >= 0 ? 'var(--green)' : 'var(--red)'};">${fmtDollars(totalPnl)}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Realized PnL</div>
        <div class="detail-item-value" style="color: ${(s.realizedPnl || 0) >= 0 ? 'var(--green)' : 'var(--red)'};">${fmtDollars(s.realizedPnl || 0)}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Unrealized PnL</div>
        <div class="detail-item-value" style="color: ${(s.unrealizedPnl || 0) >= 0 ? 'var(--green)' : 'var(--red)'};">${fmtDollars(s.unrealizedPnl || 0)}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Win Rate (Resolved)</div>
        <div class="detail-item-value">${((s.wr || 0) * 100).toFixed(1)}% (${s.wins || 0}W / ${s.losses || 0}L)</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Markets</div>
        <div class="detail-item-value">${s.estimatedMarkets || 0}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Avg Win</div>
        <div class="detail-item-value" style="color: var(--green);">${fmtDollars(s.avgW || 0)}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Avg Loss</div>
        <div class="detail-item-value" style="color: var(--red);">${fmtDollars(s.avgL || 0)}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Efficiency</div>
        <div class="detail-item-value">${((s.efficiency || 0) * 100).toFixed(2)}%</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Edge Ratio</div>
        <div class="detail-item-value">${(s.edgeRatio || 0).toFixed(2)}x</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Resolved Trades</div>
        <div class="detail-item-value">${s.resolved || 0}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Volume</div>
        <div class="detail-item-value">${fmtDollars(s.totalVolume || 0)}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Open Positions</div>
        <div class="detail-item-value">${s.openCount || 0}${(s.openCount || 0) > 0 ? ` <span style="font-size: 11px; color: var(--text-dim);">(${s.openProfitable || 0} in profit, ${s.openLosing || 0} losing)</span>` : ''}</div>
      </div>
    </div>

    <div class="detail-section">
      <h3>Recent Positions</h3>
      <div class="detail-list">
        ${(wallet.recentPositions || []).slice(0, 5).map(pos => `
          <div class="detail-list-item">
            <div class="detail-list-item-label">${pos.market || 'Unknown'}</div>
            <div class="detail-list-item-value">${fmtDollars(pos.pnl || 0)}</div>
          </div>
        `).join('')}
      </div>
    </div>
  `;

  showDetailPanel('dashboard', html);
}

function renderScoreDistribution() {
  if (!data.analytics) return;

  destroyChart('distribution');

  const scores = (data.analytics.leaderboard || []).map(w => w.score || 0);
  const buckets = Array(10).fill(0);

  scores.forEach(s => {
    const bucket = Math.floor(s / 10);
    if (bucket >= 0 && bucket < 10) {
      buckets[bucket]++;
    }
  });

  const ctx = document.getElementById('chart-distribution');
  if (ctx) {
    chartInstances['distribution'] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: buckets.map((_, i) => `${i * 10}-${(i + 1) * 10}`),
        datasets: [{
          label: 'Wallet Count',
          data: buckets,
          backgroundColor: 'rgba(108, 92, 231, 0.4)',
          borderColor: '#6c5ce7',
          borderWidth: 1
        }]
      },
      options: {
        ...CHART_DEFAULTS,
        indexAxis: undefined,
        plugins: {
          ...CHART_DEFAULTS.plugins,
          legend: { display: true }
        }
      }
    });
  }
}

function renderTrendline() {
  if (!data.analytics || !data.analytics.trendline) return;

  destroyChart('trendline');

  const timeline = data.analytics.trendline || [];

  const ctx = document.getElementById('chart-trendline');
  if (ctx) {
    chartInstances['trendline'] = new Chart(ctx, {
      type: 'line',
      data: {
        labels: timeline.map((_, i) => `Scan ${i + 1}`),
        datasets: [
          {
            label: 'Avg Score',
            data: timeline.map(t => t.avgScore || 0),
            borderColor: '#6c5ce7',
            backgroundColor: 'rgba(108, 92, 231, 0.1)',
            borderWidth: 2,
            yAxisID: 'y',
            tension: 0.4
          },
          {
            label: 'Wallet Count',
            data: timeline.map(t => t.walletCount || 0),
            borderColor: '#a29bfe',
            backgroundColor: 'rgba(162, 155, 254, 0.1)',
            borderWidth: 2,
            yAxisID: 'y1',
            tension: 0.4
          }
        ]
      },
      options: {
        ...CHART_DEFAULTS,
        interaction: { mode: 'index', intersect: false },
        scales: {
          y: {
            type: 'linear',
            display: true,
            position: 'left',
            ticks: { color: '#8888a0' },
            grid: { color: 'rgba(255,255,255,0.05)' },
            title: { display: true, text: 'Avg Score' }
          },
          y1: {
            type: 'linear',
            display: true,
            position: 'right',
            ticks: { color: '#8888a0' },
            grid: { drawOnChartArea: false },
            title: { display: true, text: 'Wallet Count' }
          },
          x: {
            ticks: { color: '#8888a0' },
            grid: { color: 'rgba(255,255,255,0.05)' }
          }
        }
      }
    });
  }
}

/* ============================================================================
   Consensus Tab
   ============================================================================ */

function renderConsensus() {
  if (!data.analytics) return;

  const consensus = (data.analytics.consensus || []).slice(0, 200); // Display top 200

  const totalWallets = data.analytics.summary?.totalWallets || 1;

  // Build raw data with raw conviction scores
  const rawConsensus = consensus.map(m => {
    const wc = m.walletCount || m.wallets?.length || 0;
    const as = m.avgScore || m.avgHolderScore || 0;
    const yc = m.yesCount || 0;
    const nc = m.noCount || 0;
    return {
      title: m.marketTitle || m.title || 'Unknown',
      slug: m.slug || m.tokenId || '',
      marketId: m.tokenId || m.marketId || '',
      walletCount: wc,
      yesCount: yc,
      noCount: nc,
      direction: m.direction || (yc > 0 && nc === 0 ? 'yes' : nc > 0 && yc === 0 ? 'no' : 'mixed'),
      topOutcome: m.topOutcome || null,
      topOutcomeCount: m.topOutcomeCount || 0,
      outcomeCounts: m.outcomeCounts || {},
      consensusStrength: m.consensusStrength || 0,
      avgScore: as,
      totalPnl: m.avgPnl || m.totalPnl || 0,
      rawConviction: m.conviction || wc * as,
      holders: m.wallets || m.holdingWallets || []
    };
  });

  // Normalize conviction to 0-100 scale
  const maxRaw = Math.max(...rawConsensus.map(m => m.rawConviction), 1);
  const consensusData = rawConsensus.map(m => ({
    ...m,
    conviction: (m.rawConviction / maxRaw) * 100
  }));

  const consensusColumns = [
    { field: 'title', render: (v, row) => `<a href="${polymarketUrl(row.slug)}" target="_blank" style="color: var(--accent-light);">${v}</a>` },
    { field: 'direction', render: (v, row) => {
      const y = row.yesCount || 0;
      const n = row.noCount || 0;
      // Binary markets: show Yes/No counts
      if (v === 'yes') return `<span class="badge badge-high">YES ${y}</span>`;
      if (v === 'no') return `<span class="badge badge-low">NO ${n}</span>`;
      // Non-binary markets: show top outcome (e.g., "Lakers", "Over 2.5")
      if (v !== 'mixed' && v !== 'yes' && v !== 'no' && row.topOutcome) {
        const count = row.topOutcomeCount || 0;
        return `<span class="badge badge-high">${row.topOutcome} (${count})</span>`;
      }
      // Mixed: show top two outcomes
      if (row.outcomeCounts && Object.keys(row.outcomeCounts).length > 0) {
        const sorted = Object.entries(row.outcomeCounts).sort((a, b) => b[1] - a[1]);
        return sorted.slice(0, 2).map(([outcome, count]) => {
          const cls = outcome === 'Yes' ? 'badge-high' : outcome === 'No' ? 'badge-low' : 'badge-mid';
          return `<span class="badge ${cls}" style="margin-right:4px;">${outcome} ${count}</span>`;
        }).join('');
      }
      return `<span class="badge badge-high" style="margin-right:4px;">YES ${y}</span><span class="badge badge-low">NO ${n}</span>`;
    }},
    { field: 'walletCount', render: v => {
      const pct = (v / totalWallets * 100).toFixed(0);
      return `
        <div class="bar-indicator">
          <div class="bar-indicator-bg">
            <div class="bar-indicator-fill" style="width: ${Math.min(pct, 100)}%"></div>
          </div>
          <span>${v}</span>
        </div>
      `;
    }},
    { field: 'consensusStrength', render: v => {
      const pct = (v * 100).toFixed(0);
      const cls = v >= 0.8 ? 'badge-high' : v >= 0.6 ? 'badge-mid' : 'badge-low';
      return `<span class="badge ${cls}">${pct}%</span>`;
    }},
    { field: 'avgScore', render: v => `<span class="badge ${scoreClass(v)}">${v.toFixed(1)}</span>` },
    { field: 'totalPnl', render: v => `<span class="${pnlClass(v)}">${fmtDollars(v)}</span>` },
    { field: 'conviction', render: (v) => {
      const cls = v >= 70 ? 'badge-high' : v >= 40 ? 'badge-mid' : 'badge-low';
      return `<span class="badge ${cls}">${v.toFixed(1)}</span>`;
    }}
  ];

  createSortableTable('consensus-table', consensusColumns, consensusData, (row) => {
    showConsensusDetail(row);
  });
}

function showConsensusDetail(market) {
  const holders = market.holders.map((h, idx) => {
    const side = h.outcome || 'Unknown';
    const sideClass = side === 'Yes' ? 'badge-high' : side === 'No' ? 'badge-low' : 'badge-mid';
    return `
      <div class="detail-list-item">
        <div class="detail-list-item-label">${idx + 1}. <span class="address-link" onclick="openPolymarketProfile('${h.address}')">${truncAddr(h.address)}</span></div>
        <div class="detail-list-item-value">
          <span class="badge ${sideClass}" style="margin-right:4px;">${side}</span>
          <span class="badge ${scoreClass(h.score)}">${h.score.toFixed(1)}</span>
          ${fmtDollars(h.pnl || 0)}
        </div>
      </div>
    `;
  }).join('');

  let dirLabel;
  if (market.outcomeCounts && Object.keys(market.outcomeCounts).length > 0) {
    // Show full outcome breakdown
    const sorted = Object.entries(market.outcomeCounts).sort((a, b) => b[1] - a[1]);
    dirLabel = sorted.map(([outcome, count]) => {
      const cls = outcome === 'Yes' ? 'badge-high' : outcome === 'No' ? 'badge-low' : 'badge-mid';
      return `<span class="badge ${cls}" style="margin-right:4px;">${outcome} (${count})</span>`;
    }).join('');
  } else if (market.direction === 'yes') {
    dirLabel = `<span class="badge badge-high">ALL YES (${market.yesCount})</span>`;
  } else if (market.direction === 'no') {
    dirLabel = `<span class="badge badge-low">ALL NO (${market.noCount})</span>`;
  } else {
    dirLabel = `<span class="badge badge-high" style="margin-right:4px;">YES ${market.yesCount}</span><span class="badge badge-low">NO ${market.noCount}</span>`;
  }

  const html = `
    <div class="detail-grid">
      <div class="detail-item">
        <div class="detail-item-label">Market Title</div>
        <div class="detail-item-value" style="font-size: 14px;">${market.slug ? `<a href="${polymarketUrl(market.slug)}" target="_blank" style="color: var(--accent-light);">${market.title}</a>` : market.title}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Direction</div>
        <div class="detail-item-value">${dirLabel}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Consensus Wallets</div>
        <div class="detail-item-value">${market.walletCount}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Avg Holder Score</div>
        <div class="detail-item-value">${market.avgScore.toFixed(1)}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Total PnL</div>
        <div class="detail-item-value" style="color: ${market.totalPnl >= 0 ? 'var(--green)' : 'var(--red)'};">${fmtDollars(market.totalPnl)}</div>
      </div>
    </div>

    <div class="detail-section">
      <h3>Holding Wallets</h3>
      <div class="detail-list">
        ${holders}
      </div>
    </div>
  `;

  showDetailPanel('consensus', html);
}

/* ============================================================================
   Portfolio Tab
   ============================================================================ */

function renderPortfolio() {
  if (!data.analytics) return;

  const active = data.analytics.activePositions || [];
  const summary = data.analytics.summary || {};

  const uniqueWallets = new Set();
  active.forEach(m => {
    (m.holders || []).forEach(w => uniqueWallets.add(w.address));
  });

  document.getElementById('metric-open-positions').textContent = active.reduce((acc, m) => acc + (m.holderCount || m.holders?.length || 0), 0).toLocaleString();
  document.getElementById('metric-unique-markets').textContent = active.length.toLocaleString();
  document.getElementById('metric-active-wallets').textContent = uniqueWallets.size.toLocaleString();

  const portfolioData = active.map(m => ({
    title: m.marketTitle || m.title || 'Unknown',
    slug: m.slug || m.tokenId || '',
    holdingCount: m.holderCount || m.holders?.length || 0,
    totalShares: m.totalShares || 0,
    totalValue: m.totalValue || (m.holders || []).reduce((s, h) => s + (h.positionValue || h.shares * (h.entryPrice || 0)), 0),
    totalPnl: m.totalPnl || (m.holders || []).reduce((s, h) => s + (h.currentPnl || 0), 0),
    avgEntryPrice: m.avgEntryPrice || (m.holders?.length ? m.holders.reduce((s, h) => s + (h.entryPrice || 0), 0) / m.holders.length : 0),
    holders: m.holders || []
  }));

  const portfolioColumns = [
    { field: 'title', render: (v, row) => `<a href="${polymarketUrl(row.slug)}" target="_blank" style="color: var(--accent-light);">${v}</a>` },
    { field: 'holdingCount', render: v => String(v) },
    { field: 'totalValue', render: v => fmtDollars(v) },
    { field: 'totalShares', render: v => fmt(v, 0) },
    { field: 'avgEntryPrice', render: v => '$' + v.toFixed(2) },
    { field: 'totalPnl', render: v => `<span class="${pnlClass(v)}">${fmtDollars(v)}</span>` }
  ];

  createSortableTable('portfolio-table', portfolioColumns, portfolioData, (row) => {
    showPortfolioDetail(row);
  });
}

function showPortfolioDetail(market) {
  const holders = market.holders.map((h, idx) => {
    const val = h.positionValue || (h.shares * (h.entryPrice || 0));
    return `
    <div class="detail-list-item">
      <div class="detail-list-item-label">${idx + 1}. <span class="address-link" onclick="openPolymarketProfile('${h.address}')">${truncAddr(h.address)}</span></div>
      <div class="detail-list-item-value">${fmt(h.shares || 0, 0)} shares @ $${(h.entryPrice || 0).toFixed(2)} (${fmtDollars(val)}) <span class="${pnlClass(h.currentPnl || 0)}">${fmtDollars(h.currentPnl || 0)}</span></div>
    </div>
  `;}).join('');

  const html = `
    <div class="detail-grid">
      <div class="detail-item">
        <div class="detail-item-label">Market</div>
        <div class="detail-item-value" style="font-size: 14px;">${market.title}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Holding Wallets</div>
        <div class="detail-item-value">${market.holdingCount}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Total Value</div>
        <div class="detail-item-value">${fmtDollars(market.totalValue || 0)}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Total PnL</div>
        <div class="detail-item-value"><span class="${pnlClass(market.totalPnl || 0)}">${fmtDollars(market.totalPnl || 0)}</span></div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Total Shares</div>
        <div class="detail-item-value">${fmt(market.totalShares, 0)}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Avg Entry Price</div>
        <div class="detail-item-value">$${(market.avgEntryPrice || 0).toFixed(2)}</div>
      </div>
    </div>

    <div class="detail-section">
      <h3>Holding Wallets</h3>
      <div class="detail-list">
        ${holders}
      </div>
    </div>
  `;

  showDetailPanel('portfolio', html);
}

/* ============================================================================
   Patterns Tab
   ============================================================================ */

function renderPatterns() {
  if (!data.analytics) return;

  const patterns = data.analytics.winPatterns || data.analytics.patterns || {};
  const summary = data.analytics.summary || {};
  const overallStats = patterns.overallStats || {};

  document.getElementById('metric-overall-winrate').textContent = ((overallStats.winRate || 0) * 100).toFixed(1) + '%';
  document.getElementById('metric-avg-position').textContent = overallStats.totalTrades ? fmtDollars(overallStats.totalPnl / overallStats.totalTrades) : '0';
  document.getElementById('metric-median-pnl').textContent = fmtDollars(overallStats.avgPnl || 0);
  document.getElementById('metric-resolved-count').textContent = (overallStats.totalTrades || 0).toLocaleString();

  // Win rate by size chart
  renderWinRateBySize(patterns);

  // Top markets chart
  renderTopMarketsChart(patterns);

  // Winning markets table
  const winningMarkets = (patterns.topWinningMarkets || []).map(m => ({
    title: m.title || 'Unknown',
    slug: m.slug || '',
    winRate: m.winRate || 0,
    avgPnl: m.avgPnl || 0,
    positionCount: m.totalTrades || m.positionCount || 0
  }));

  const patternsColumns = [
    { field: 'title', render: (v, row) => row.slug ? `<a href="${polymarketUrl(row.slug)}" target="_blank" style="color: var(--accent-light);">${v}</a>` : `<span style="color: var(--accent-light);">${v}</span>` },
    { field: 'winRate', render: v => ((v || 0) * 100).toFixed(1) + '%' },
    { field: 'avgPnl', render: v => `<span class="${pnlClass(v)}">${fmtDollars(v)}</span>` },
    { field: 'positionCount', render: v => String(v) }
  ];

  createSortableTable('patterns-table', patternsColumns, winningMarkets);
}

function renderWinRateBySize(patterns) {
  destroyChart('winrate-size');

  const buckets = patterns.sizeBuckets || patterns.winRateBySize || { small: {}, medium: {}, large: {} };
  const getWinRate = (b) => b && b.count > 0 ? (b.wins / b.count) : 0;

  const ctx = document.getElementById('chart-winrate-size');
  if (ctx) {
    chartInstances['winrate-size'] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: ['Small', 'Medium', 'Large'],
        datasets: [{
          label: 'Win Rate (%)',
          data: [
            getWinRate(buckets.small) * 100,
            getWinRate(buckets.medium) * 100,
            getWinRate(buckets.large) * 100
          ],
          backgroundColor: ['rgba(0, 184, 148, 0.4)', 'rgba(253, 203, 110, 0.4)', 'rgba(225, 112, 85, 0.4)'],
          borderColor: ['#00b894', '#fdcb6e', '#e17055'],
          borderWidth: 1
        }]
      },
      options: {
        ...CHART_DEFAULTS,
        indexAxis: undefined,
        scales: {
          ...CHART_DEFAULTS.scales,
          y: {
            ...CHART_DEFAULTS.scales.y,
            max: 100
          }
        }
      }
    });
  }
}

function renderTopMarketsChart(patterns) {
  destroyChart('top-markets');

  const topMarkets = (patterns.topWinningMarkets || []).slice(0, 10);

  const ctx = document.getElementById('chart-top-markets');
  if (ctx) {
    chartInstances['top-markets'] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: topMarkets.map(m => m.title || 'Unknown'),
        datasets: [{
          label: 'Total PnL ($)',
          data: topMarkets.map(m => m.totalPnl || 0),
          backgroundColor: topMarkets.map(m => (m.totalPnl || 0) >= 0 ? 'rgba(0, 184, 148, 0.4)' : 'rgba(225, 112, 85, 0.4)'),
          borderColor: topMarkets.map(m => (m.totalPnl || 0) >= 0 ? '#00b894' : '#e17055'),
          borderWidth: 1
        }]
      },
      options: {
        ...CHART_DEFAULTS,
        indexAxis: 'y',
        scales: {
          ...CHART_DEFAULTS.scales,
          x: {
            ...CHART_DEFAULTS.scales.x,
            ticks: { color: '#8888a0' },
            grid: { color: 'rgba(255,255,255,0.05)' }
          }
        }
      }
    });
  }
}

/* ============================================================================
   Signals Tab
   ============================================================================ */

/**
 * Helper: render a direction badge that handles binary and non-binary outcomes
 */
function renderDirectionBadge(direction, row) {
  const oc = row.outcomeCounts || {};

  // Use outcomeCounts for wallet tallies (always populated on signals)
  if (direction === 'yes') return `<span class="badge badge-high">YES ${oc['Yes'] || ''}</span>`;
  if (direction === 'no') return `<span class="badge badge-low">NO ${oc['No'] || ''}</span>`;

  // Non-binary with outcomeCounts: show only the signal's chosen direction (top outcome)
  // Previously showed top 2 outcomes which made it look like the signal was calling both sides
  if (direction !== 'mixed' && Object.keys(oc).length > 0) {
    const sorted = Object.entries(oc).sort((a, b) => b[1] - a[1]);
    const top = sorted[0];
    if (top) {
      const total = sorted.reduce((s, [, c]) => s + c, 0);
      const label = top[0];
      const pct = total > 0 ? Math.round(top[1] / total * 100) : 100;
      return `<span class="badge badge-high">${label} <span style="opacity:0.6;font-size:11px">${pct}%</span></span>`;
    }
  }

  // Fallback for paper trades / contexts without outcomeCounts — just show direction
  if (direction && direction !== 'mixed' && direction !== 'unknown') {
    const label = direction.charAt(0).toUpperCase() + direction.slice(1);
    return `<span class="badge badge-high">${label}</span>`;
  }

  return `<span class="badge badge-mid">MIXED</span>`;
}

/**
 * Render the Signal Engine section (active signals + history + stats)
 */
function renderSignalEngine() {
  const signals = data.analytics.signals || {};
  const activeSignals = signals.active || [];
  const signalHistory = signals.history || [];
  const stats = signals.stats || {};

  // --- Signal Engine Metrics ---
  const el = (id) => document.getElementById(id);

  const activeEl = el('metric-active-signals');
  if (activeEl) activeEl.textContent = (stats.activeCount || activeSignals.length || 0).toString();

  const tiersEl = el('metric-signal-tiers');
  if (tiersEl) {
    const tb = stats.tierBreakdown || {};
    const tt = stats.typeBreakdown || {};
    tiersEl.innerHTML = `${tb.elite || 0} Elite / ${tb.pro || 0} Pro / ${tb.starter || 0} Starter &middot; ${tt.consensus || 0} consensus, ${tt.cluster || 0} cluster, ${tt.solo || 0} solo`;
  }

  const hrEl = el('metric-signal-hitrate');
  if (hrEl) {
    const hr = stats.hitRate || 0;
    hrEl.innerHTML = hr > 0 ? `<span style="color: ${hr >= 50 ? 'var(--green)' : 'var(--red)'}">${hr}%</span>` : '-';
  }

  const recordEl = el('metric-signal-record');
  if (recordEl) recordEl.textContent = stats.totalResolved > 0 ? `${stats.wins || 0}W / ${stats.losses || 0}L` : 'No resolved signals yet';

  const pnlEl = el('metric-signal-pnl');
  if (pnlEl) {
    const pnl = stats.totalPnl || 0;
    pnlEl.innerHTML = pnl !== 0 ? `<span style="color: ${pnl >= 0 ? 'var(--green)' : 'var(--red)'}">${fmtDollars(pnl)}</span>` : '-';
  }

  const avgConfEl = el('metric-signal-avg-conf');
  if (avgConfEl) avgConfEl.textContent = stats.avgConfidence > 0 ? `Avg confidence: ${stats.avgConfidence}` : '';

  const confEl = el('metric-signal-confidence');
  if (confEl) confEl.textContent = stats.avgConfidence > 0 ? stats.avgConfidence.toFixed(1) : '-';

  const scanActEl = el('metric-signal-scan-activity');
  if (scanActEl) {
    const parts = [];
    if (stats.openedThisScan) parts.push(`+${stats.openedThisScan} new`);
    if (stats.closedThisScan) parts.push(`-${stats.closedThisScan} closed`);
    scanActEl.textContent = parts.length > 0 ? parts.join(', ') + ' this scan' : 'No changes this scan';
  }

  // --- Active Signals Table ---
  const tierClass = (tier) => {
    if (tier === 'elite') return 'badge-high';
    if (tier === 'pro') return 'badge-mid';
    return 'badge-low';
  };

  const signalData = activeSignals.map(s => ({
    marketTitle: s.marketTitle || 'Unknown',
    slug: s.slug || '',
    signalType: s.signalType || 'consensus',
    tier: s.tier || 'starter',
    direction: s.direction || 'mixed',
    topOutcome: s.topOutcome || null,
    topOutcomeCount: s.topOutcomeCount || 0,
    outcomeCounts: s.outcomeCounts || {},
    walletCount: s.walletCount || 0,
    confidence: s.confidence || 0,
    avgScore: s.avgScore || 0,
    scansActive: s.scansActive || 0,
    peakConfidence: s.peakConfidence || 0,
    currentWallets: s.currentWallets || [],
    signalId: s.signalId || '',
    openedAt: s.openedAt || null,
    // Market prices
    openMarketPrice: s.openMarketPrice || 0,
    currentMarketPrice: s.currentMarketPrice || 0,
    // Solo-specific fields
    soloWallet: s.soloWallet || null,
    walletScore: s.walletScore || 0,
    walletWinRate: s.walletWinRate || 0,
    walletResolved: s.walletResolved || 0,
    walletPnl: s.walletPnl || 0,
    positionValue: s.positionValue || 0,
  }));

  const signalColumns = [
    { field: 'marketTitle', render: (v, row) => row.slug ? `<a href="${polymarketUrl(row.slug)}" target="_blank" style="color: var(--accent-light);">${v}</a>` : `<span style="color: var(--accent-light);">${v}</span>` },
    { field: 'signalType', render: v => {
      if (v === 'solo') return `<span class="badge badge-solo">SOLO</span>`;
      if (v === 'cluster') return `<span class="badge badge-cluster">CLUSTER</span>`;
      return `<span class="badge badge-consensus">CONSENSUS</span>`;
    }},
    { field: 'tier', render: v => `<span class="badge ${tierClass(v)}">${v.toUpperCase()}</span>` },
    { field: 'direction', render: (v, row) => renderDirectionBadge(v, row) },
    { field: 'walletCount', render: v => String(v) },
    { field: 'openMarketPrice', render: v => {
      if (!v || v === 0) return `<span style="color:var(--text-dim)">—</span>`;
      const pct = (v * 100).toFixed(1);
      const cls = v >= 0.85 ? 'color:var(--red)' : v >= 0.65 ? 'color:var(--orange)' : 'color:var(--green)';
      return `<span style="${cls};font-weight:600">${pct}¢</span>`;
    }},
    { field: 'currentMarketPrice', render: (v, row) => {
      if (!v || v === 0) return `<span style="color:var(--text-dim)">—</span>`;
      const pct = (v * 100).toFixed(1);
      const open = row.openMarketPrice || 0;
      const diff = open > 0 ? v - open : 0;
      const arrow = diff > 0.005 ? '▲' : diff < -0.005 ? '▼' : '';
      const diffColor = diff > 0.005 ? 'var(--green)' : diff < -0.005 ? 'var(--red)' : 'var(--text-dim)';
      return `<span style="font-weight:600">${pct}¢</span>${arrow ? ` <span style="color:${diffColor};font-size:11px">${arrow}${Math.abs(diff * 100).toFixed(1)}</span>` : ''}`;
    }},
    { field: 'confidence', render: v => {
      const cls = v >= 80 ? 'badge-high' : v >= 55 ? 'badge-mid' : 'badge-low';
      return `<span class="badge ${cls}">${v.toFixed(1)}</span>`;
    }},
    { field: 'avgScore', render: v => `<span class="badge ${scoreClass(v)}">${v.toFixed(1)}</span>` },
    { field: 'unrealised', render: (v, row) => {
      const open = row.openMarketPrice || 0;
      const live = row.currentMarketPrice || 0;
      if (!open || !live || open === live) return `<span style="color:var(--text-dim)">0.0%</span>`;
      const pct = ((live - open) / open * 100).toFixed(1);
      const cls = pct > 0 ? 'var(--green)' : pct < 0 ? 'var(--red)' : 'var(--text-dim)';
      return `<span style="color:${cls};font-weight:600">${pct > 0 ? '+' : ''}${pct}%</span>`;
    }},
    { field: 'scansActive', render: v => {
      const hours = v * 6; // each scan is ~6 hours
      if (hours < 24) return `${hours}h`;
      return `${(hours / 24).toFixed(1)}d`;
    }},
  ];

  createSortableTable('active-signals-table', signalColumns, signalData, (row) => {
    showSignalDetail(row);
  });

  // --- Filter buttons (tier + type) ---
  document.querySelectorAll('.signal-filter').forEach(btn => {
    btn.onclick = () => {
      document.querySelectorAll('.signal-filter').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      const tier = btn.dataset.tier;
      const type = btn.dataset.type;
      let filtered = signalData;
      if (tier && tier !== 'all') filtered = filtered.filter(s => s.tier === tier);
      if (type && type !== 'all') filtered = filtered.filter(s => s.signalType === type);
      createSortableTable('active-signals-table', signalColumns, filtered, (row) => {
        showSignalDetail(row);
      });
    };
  });

  // --- Signal History Table ---
  const historyData = signalHistory.slice().reverse().map(s => ({
    marketTitle: s.marketTitle || 'Unknown',
    slug: s.slug || '',
    outcome: s.outcome || 'unknown',
    direction: s.direction || 'mixed',
    topOutcome: s.topOutcome || null,
    outcomeCounts: s.outcomeCounts || {},
    peakConfidence: s.peakConfidence || 0,
    peakWallets: s.peakWallets || 0,
    openMarketPrice: s.openMarketPrice || 0,
    signalReturn: (() => {
      const price = s.openMarketPrice || 0;
      if (!price) return null;
      if (s.outcome === 'win') return +((1 / price - 1) * 100).toFixed(1);
      if (s.outcome === 'loss') return -100;
      return 0;
    })(),
    duration: s.duration || 0,
    closeReason: s.closeReason || 'unknown',
  }));

  const historyColumns = [
    { field: 'marketTitle', render: (v, row) => row.slug ? `<a href="${polymarketUrl(row.slug)}" target="_blank" style="color: var(--accent-light);">${v}</a>` : `<span style="color: var(--accent-light);">${v}</span>` },
    { field: 'outcome', render: v => {
      if (v === 'win') return `<span class="badge badge-high">WIN</span>`;
      if (v === 'loss') return `<span class="badge badge-low">LOSS</span>`;
      return `<span class="badge badge-mid">${(v || 'unknown').toUpperCase()}</span>`;
    }},
    { field: 'direction', render: (v, row) => renderDirectionBadge(v, row) },
    { field: 'openMarketPrice', render: v => {
      if (!v || v === 0) return `<span style="color:var(--text-dim)">—</span>`;
      const pct = (v * 100).toFixed(1);
      const cls = v >= 0.85 ? 'color:var(--red)' : v >= 0.65 ? 'color:var(--orange)' : 'color:var(--green)';
      return `<span style="${cls};font-weight:600">${pct}¢</span>`;
    }},
    { field: 'peakConfidence', render: v => {
      const cls = v >= 80 ? 'badge-high' : v >= 55 ? 'badge-mid' : 'badge-low';
      return `<span class="badge ${cls}">${v.toFixed(1)}</span>`;
    }},
    { field: 'peakWallets', render: v => String(v) },
    { field: 'signalReturn', render: v => {
      if (v === null) return `<span style="color:var(--text-dim)">—</span>`;
      const cls = v > 0 ? 'var(--green)' : v < 0 ? 'var(--red)' : 'var(--text-dim)';
      return `<span style="color:${cls};font-weight:600">${v > 0 ? '+' : ''}${v.toFixed(1)}%</span>`;
    }},
    { field: 'duration', render: v => {
      const hours = v * 6;
      if (hours < 24) return `${hours}h`;
      return `${(hours / 24).toFixed(1)}d`;
    }},
    { field: 'closeReason', render: v => `<span class="badge badge-mid">${v}</span>` },
  ];

  createSortableTable('signal-history-table', historyColumns, historyData);
}

/**
 * Show detail panel for a signal
 */
function showSignalDetail(signal) {
  const wallets = (signal.currentWallets || []).map((w, idx) => {
    const sideClass = w.outcome === 'Yes' ? 'badge-high' : w.outcome === 'No' ? 'badge-low' : 'badge-mid';
    return `
      <div class="detail-list-item">
        <div class="detail-list-item-label">${idx + 1}. <span class="address-link" onclick="openPolymarketProfile('${w.address}')">${truncAddr(w.address)}</span></div>
        <div class="detail-list-item-value">
          <span class="badge ${sideClass}" style="margin-right:4px;">${w.outcome || 'Unknown'}</span>
          <span class="badge ${scoreClass(w.score || 0)}">${(w.score || 0).toFixed(1)}</span>
          <span class="${pnlClass(w.pnl || 0)}">${fmtDollars(w.pnl || 0)}</span>
        </div>
      </div>
    `;
  }).join('');

  const tierCls = signal.tier === 'elite' ? 'badge-high' : signal.tier === 'pro' ? 'badge-mid' : 'badge-low';
  const confClass = signal.confidence >= 80 ? 'badge-high' : signal.confidence >= 55 ? 'badge-mid' : 'badge-low';
  const isSolo = signal.signalType === 'solo';
  const typeBadge = isSolo
    ? `<span class="badge" style="background: var(--purple, #9b59b6); color: white;">SOLO</span>`
    : `<span class="badge badge-mid">CONSENSUS</span>`;

  // Solo-specific section
  const soloSection = isSolo ? `
    <div class="detail-section">
      <h3>Solo Wallet Profile</h3>
      <div class="detail-grid">
        <div class="detail-item">
          <div class="detail-item-label">Wallet</div>
          <div class="detail-item-value"><span class="address-link" onclick="openPolymarketProfile('${signal.soloWallet}')">${truncAddr(signal.soloWallet || '')}</span></div>
        </div>
        <div class="detail-item">
          <div class="detail-item-label">Win Rate</div>
          <div class="detail-item-value">${((signal.walletWinRate || 0) * 100).toFixed(1)}%</div>
        </div>
        <div class="detail-item">
          <div class="detail-item-label">Resolved Positions</div>
          <div class="detail-item-value">${signal.walletResolved || 0}</div>
        </div>
        <div class="detail-item">
          <div class="detail-item-label">Wallet Realized PnL</div>
          <div class="detail-item-value"><span class="${pnlClass(signal.walletPnl || 0)}">${fmtDollars(signal.walletPnl || 0)}</span></div>
        </div>
        <div class="detail-item">
          <div class="detail-item-label">Position Value</div>
          <div class="detail-item-value">${fmtDollars(signal.positionValue || 0)}</div>
        </div>
      </div>
    </div>
  ` : '';

  const html = `
    <div class="detail-grid">
      <div class="detail-item">
        <div class="detail-item-label">Market</div>
        <div class="detail-item-value" style="font-size: 14px;">${signal.slug ? `<a href="${polymarketUrl(signal.slug)}" target="_blank" style="color: var(--accent-light);">${signal.marketTitle}</a>` : signal.marketTitle}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Type / Tier</div>
        <div class="detail-item-value">${typeBadge} <span class="badge ${tierCls}">${(signal.tier || 'starter').toUpperCase()}</span></div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Confidence</div>
        <div class="detail-item-value"><span class="badge ${confClass}">${(signal.confidence || 0).toFixed(1)}</span> (peak: ${(signal.peakConfidence || 0).toFixed(1)})</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">${isSolo ? 'Wallet' : 'Backing Wallets'}</div>
        <div class="detail-item-value">${isSolo ? `<span class="address-link" onclick="openPolymarketProfile('${signal.soloWallet}')">${truncAddr(signal.soloWallet || '')}</span>` : signal.walletCount}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Entry Price</div>
        <div class="detail-item-value">${signal.openMarketPrice ? `<span style="font-weight:600;color:${signal.openMarketPrice >= 0.85 ? 'var(--red)' : signal.openMarketPrice >= 0.65 ? 'var(--orange)' : 'var(--green)'}">${(signal.openMarketPrice * 100).toFixed(1)}¢</span>` : '<span style="color:var(--text-dim)">—</span>'}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Live Price</div>
        <div class="detail-item-value">${signal.currentMarketPrice ? (() => {
          const diff = signal.openMarketPrice ? signal.currentMarketPrice - signal.openMarketPrice : 0;
          const arrow = diff > 0.005 ? '▲' : diff < -0.005 ? '▼' : '';
          const diffColor = diff > 0.005 ? 'var(--green)' : diff < -0.005 ? 'var(--red)' : 'var(--text-dim)';
          return `<span style="font-weight:600">${(signal.currentMarketPrice * 100).toFixed(1)}¢</span>${arrow ? ` <span style="color:${diffColor};font-size:12px">${arrow}${Math.abs(diff * 100).toFixed(1)}¢</span>` : ''}`;
        })() : '<span style="color:var(--text-dim)">—</span>'}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Unrealised</div>
        <div class="detail-item-value">${(() => {
          const open = signal.openMarketPrice || 0;
          const live = signal.currentMarketPrice || 0;
          if (!open || !live || open === live) return '<span style="color:var(--text-dim)">0.0%</span>';
          const pct = ((live - open) / open * 100).toFixed(1);
          const cls = pct > 0 ? 'var(--green)' : pct < 0 ? 'var(--red)' : 'var(--text-dim)';
          return `<span style="color:${cls};font-weight:600">${pct > 0 ? '+' : ''}${pct}%</span>`;
        })()}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Signal Age</div>
        <div class="detail-item-value">${signal.openedAt ? relativeTime(signal.openedAt) : `${signal.scansActive} scans`}</div>
      </div>
    </div>

    ${soloSection}

    <div class="detail-section">
      <h3>${isSolo ? 'Wallet Position' : 'Backing Wallets'}</h3>
      <div class="detail-list">
        ${wallets || '<div class="empty-state">No wallet data available</div>'}
      </div>
    </div>
  `;

  showDetailPanel('signals', html);
}

function renderSignals() {
  if (!data.analytics) return;

  // === Signal Engine Stats & Tables ===
  renderSignalEngine();

}

/* ============================================================================
   Paper Trader Tab
   ============================================================================ */

let currentPaperPortfolio = 'combined';

function renderPaperTrader() {
  const pt = data.analytics?.paperTrading;
  if (!pt) {
    document.getElementById('paper-open-tbody').innerHTML =
      '<tr><td colspan="7" class="empty-state">No paper trading data yet. Run the scanner with signals enabled.</td></tr>';
    return;
  }

  const portfolio = pt[currentPaperPortfolio];
  if (!portfolio) return;

  const stats = portfolio.stats || {};
  const equity = portfolio.equity || 0;
  const startBal = portfolio.startingBalance || 10000;
  const totalReturn = ((equity / startBal - 1) * 100).toFixed(2);
  const deployed = portfolio.openTrades.reduce((s, t) => s + t.tradeSize, 0);
  const totalTrades = stats.wins + stats.losses;
  const winRate = totalTrades > 0 ? ((stats.wins / totalTrades) * 100).toFixed(1) : '-';
  const avgTrade = totalTrades > 0 ? (stats.totalPnl / totalTrades).toFixed(2) : '-';

  // Update metric cards
  const el = id => document.getElementById(id);
  el('paper-equity').textContent = fmtDollars(equity);
  el('paper-equity').className = `metric-value ${equity >= startBal ? '' : 'text-negative'}`;
  el('paper-return').textContent = `${totalReturn >= 0 ? '+' : ''}${totalReturn}%`;
  el('paper-return').style.color = totalReturn >= 0 ? 'var(--green)' : 'var(--red)';

  el('paper-balance').textContent = fmtDollars(portfolio.balance);
  el('paper-deployed').textContent = `$${deployed.toFixed(0)} deployed`;

  el('paper-winrate').textContent = winRate === '-' ? '-' : winRate + '%';
  el('paper-record').textContent = `${stats.wins}W / ${stats.losses}L`;

  el('paper-pnl').textContent = fmtDollars(stats.totalPnl);
  el('paper-pnl').className = `metric-value ${stats.totalPnl >= 0 ? '' : 'text-negative'}`;
  const streakText = stats.currentStreak > 0 ? `${stats.currentStreak}W streak` :
    stats.currentStreak < 0 ? `${Math.abs(stats.currentStreak)}L streak` : '-';
  el('paper-streaks').textContent = streakText;

  el('paper-drawdown').textContent = (stats.maxDrawdown || 0).toFixed(1) + '%';
  el('paper-peak').textContent = `Peak: ${fmtDollars(stats.peakEquity)}`;

  el('paper-avg-trade').textContent = avgTrade === '-' ? '-' : fmtDollars(+avgTrade);
  el('paper-best-worst').textContent = totalTrades > 0
    ? `Best: +$${(stats.biggestWin || 0).toFixed(0)} / Worst: $${(stats.biggestLoss || 0).toFixed(0)}`
    : '-';

  // Render equity curve chart
  renderEquityCurve(portfolio.equityCurve || []);

  // Render open trades table
  const openTrades = portfolio.openTrades || [];
  el('paper-open-count').textContent = `(${openTrades.length})`;
  const openTbody = el('paper-open-tbody');

  if (openTrades.length === 0) {
    openTbody.innerHTML = '<tr><td colspan="8" class="empty-state">No open trades</td></tr>';
  } else {
    openTbody.innerHTML = openTrades.map(t => {
      const latestScan = data.analytics?.scanCount || 0;
      const age = latestScan - (t.openedScan || 0);
      const typeLabel = (t.signalType || 'consensus').toUpperCase();
      const typeClass = t.signalType === 'solo' ? 'badge-solo' : t.signalType === 'cluster' ? 'badge-cluster' : 'badge-consensus';
      const mp = t.openMarketPrice || 0;
      const mpStr = mp > 0 ? `<span style="font-weight:600;color:${mp >= 0.85 ? 'var(--red)' : mp >= 0.65 ? 'var(--orange)' : 'var(--green)'}">${(mp * 100).toFixed(1)}¢</span>` : '<span style="color:var(--text-dim)">—</span>';
      return `<tr>
        <td title="${t.marketTitle}">${truncate(t.marketTitle, 50)}</td>
        <td><span class="badge ${typeClass}">${typeLabel}</span></td>
        <td><span class="tier-badge tier-${t.tier || 'starter'}">${(t.tier || 'starter').toUpperCase()}</span></td>
        <td>${renderDirectionBadge(t.direction, t)}</td>
        <td>${mpStr}</td>
        <td>${(t.confidence || 0).toFixed(1)}</td>
        <td>$${t.tradeSize}</td>
        <td>${age}</td>
      </tr>`;
    }).join('');
  }

  // Render closed trades table
  const closedTrades = [...(portfolio.closedTrades || [])].reverse(); // Most recent first
  el('paper-closed-count').textContent = `(${closedTrades.length})`;
  const closedTbody = el('paper-closed-tbody');

  if (closedTrades.length === 0) {
    closedTbody.innerHTML = '<tr><td colspan="10" class="empty-state">No closed trades yet</td></tr>';
  } else {
    closedTbody.innerHTML = closedTrades.map(t => {
      const typeLabel = (t.signalType || 'consensus').toUpperCase();
      const typeClass = t.signalType === 'solo' ? 'badge-solo' : t.signalType === 'cluster' ? 'badge-cluster' : 'badge-consensus';
      const outcomeClass = t.outcome === 'win' ? 'badge-positive' :
        t.outcome === 'loss' ? 'badge-negative' : 'badge-neutral';
      const outcomeLabel = (t.outcome || 'unknown').toUpperCase();
      const mp = t.openMarketPrice || 0;
      const mpStr = mp > 0 ? `<span style="font-weight:600;color:${mp >= 0.85 ? 'var(--red)' : mp >= 0.65 ? 'var(--orange)' : 'var(--green)'}">${(mp * 100).toFixed(1)}¢</span>` : '<span style="color:var(--text-dim)">—</span>';
      return `<tr>
        <td title="${t.marketTitle}">${truncate(t.marketTitle, 45)}</td>
        <td><span class="badge ${typeClass}">${typeLabel}</span></td>
        <td><span class="tier-badge tier-${t.tier || 'starter'}">${(t.tier || 'starter').toUpperCase()}</span></td>
        <td>${renderDirectionBadge(t.direction, t)}</td>
        <td>${mpStr}</td>
        <td><span class="badge ${outcomeClass}">${outcomeLabel}</span></td>
        <td class="${t.pnl >= 0 ? 'text-positive' : 'text-negative'}">${t.pnl >= 0 ? '+' : ''}$${t.pnl.toFixed(2)}</td>
        <td class="${t.returnPct >= 0 ? 'text-positive' : 'text-negative'}">${t.returnPct >= 0 ? '+' : ''}${t.returnPct}%</td>
        <td>${t.duration || 0} scans</td>
        <td>${(t.closeReason || '').replace(/_/g, ' ')}</td>
      </tr>`;
    }).join('');
  }

  // Render tier comparison table
  renderTierComparison(pt);
}

function renderEquityCurve(equityCurve) {
  const canvas = document.getElementById('chart-equity-curve');
  if (!canvas) return;

  // Destroy existing chart
  if (chartInstances['equity-curve']) {
    chartInstances['equity-curve'].destroy();
  }

  if (equityCurve.length < 2) {
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = '#666';
    ctx.textAlign = 'center';
    ctx.fillText('Not enough data points yet', canvas.width / 2, canvas.height / 2);
    return;
  }

  const labels = equityCurve.map(p => `Scan ${p.scan}`);
  const equityData = equityCurve.map(p => p.equity);
  const startBal = equityCurve[0]?.equity || 10000;

  chartInstances['equity-curve'] = new Chart(canvas, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Portfolio Equity',
        data: equityData,
        borderColor: '#3b82f6',
        backgroundColor: 'rgba(59, 130, 246, 0.1)',
        fill: true,
        tension: 0.3,
        pointRadius: equityCurve.length > 50 ? 0 : 3,
      }, {
        label: 'Starting Balance',
        data: equityCurve.map(() => startBal),
        borderColor: 'rgba(255,255,255,0.2)',
        borderDash: [5, 5],
        pointRadius: 0,
        fill: false,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: '#ccc' } },
      },
      scales: {
        x: { ticks: { color: '#888', maxTicksLimit: 10 }, grid: { color: 'rgba(255,255,255,0.05)' } },
        y: {
          ticks: { color: '#888', callback: v => '$' + v.toLocaleString() },
          grid: { color: 'rgba(255,255,255,0.05)' },
        },
      },
    },
  });
}

function renderTierComparison(pt) {
  const tbody = document.getElementById('paper-comparison-tbody');
  if (!tbody) return;

  const rows = ['combined', 'elite', 'pro', 'starter'].map(name => {
    const p = pt[name];
    if (!p) return '';
    const s = p.stats || {};
    const equity = p.equity || 0;
    const startBal = p.startingBalance || 10000;
    const ret = ((equity / startBal - 1) * 100).toFixed(2);
    const total = s.wins + s.losses;
    const wr = total > 0 ? ((s.wins / total) * 100).toFixed(1) + '%' : '-';
    const label = name.charAt(0).toUpperCase() + name.slice(1);

    return `<tr>
      <td><strong>${label}</strong></td>
      <td>${fmtDollars(equity)}</td>
      <td class="${ret >= 0 ? 'text-positive' : 'text-negative'}">${ret >= 0 ? '+' : ''}${ret}%</td>
      <td>${total}</td>
      <td>${wr}</td>
      <td class="${s.totalPnl >= 0 ? 'text-positive' : 'text-negative'}">${fmtDollars(s.totalPnl)}</td>
      <td>${(s.maxDrawdown || 0).toFixed(1)}%</td>
    </tr>`;
  });

  tbody.innerHTML = rows.join('');
}

function truncate(str, max) {
  if (!str) return '';
  return str.length > max ? str.slice(0, max) + '...' : str;
}

/* ============================================================================
   Initialization
   ============================================================================ */

async function init() {
  // Load data
  data = await loadData();
  updateStatusBar();

  // Wallets data is now lazy-loaded only when time filtering needs it

  // Attach tab listeners
  document.querySelectorAll('.tab').forEach(btn => {
    btn.addEventListener('click', () => {
      const tabName = btn.dataset.tab;
      switchTab(tabName);
    });
  });

  // Paper trader portfolio filter buttons
  document.querySelectorAll('.paper-portfolio-filter').forEach(btn => {
    btn.addEventListener('click', function() {
      document.querySelectorAll('.paper-portfolio-filter').forEach(b => b.classList.remove('active'));
      this.classList.add('active');
      currentPaperPortfolio = this.dataset.portfolio;
      renderPaperTrader();
    });
  });

  // Time filter buttons — lazy-load wallets data when a non-'all' filter is selected
  document.querySelectorAll('.time-btn').forEach(btn => {
    btn.addEventListener('click', async function() {
      document.querySelectorAll('.time-btn').forEach(b => b.classList.remove('active'));
      this.classList.add('active');
      currentTimeRange = this.dataset.range;
      if (currentTimeRange !== 'all' && !walletsData) {
        await ensureWalletsLoaded();
      }
      if (currentTab === 'dashboard') renderDashboard();
    });
  });

  // Active wallets only checkbox
  const activeOnlyEl = document.getElementById('active-only');
  if (activeOnlyEl) {
    activeOnlyEl.addEventListener('change', function() {
      activeWalletsOnly = this.checked;
      if (currentTab === 'dashboard') renderDashboard();
    });
  }

  // Render initial dashboard
  if (data.analytics) {
    renderDashboard();
  } else {
    showEmptyState('No scan data yet. Run the scanner to populate the dashboard.');
  }
}

// Start when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
