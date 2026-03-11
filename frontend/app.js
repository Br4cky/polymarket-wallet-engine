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
  let wins = 0, losses = 0, winSum = 0, lossSum = 0, totalPnl = 0;
  let totalVolume = 0, openCount = 0;
  const uniqueTokens = new Set();

  for (const pos of positions) {
    totalPnl += pos.pnl || 0;
    totalVolume += pos.totalBought || 0;
    if (pos.tokenId) uniqueTokens.add(pos.tokenId);
    if ((pos.amount || 0) > 0.01) openCount++;
    if ((pos.totalBought || 0) > 0.01) {
      if ((pos.pnl || 0) > 0) { wins++; winSum += pos.pnl; }
      else if ((pos.pnl || 0) < 0) { losses++; lossSum += -pos.pnl; }
    }
  }

  const resolved = wins + losses;
  const wr = resolved > 0 ? wins / resolved : 0;
  const avgW = wins > 0 ? winSum / wins : 0;
  const avgL = losses > 0 ? lossSum / losses : 0;
  const efficiency = totalVolume > 0 ? totalPnl / totalVolume : 0;
  const edgeRatio = avgL > 0 ? avgW / avgL : (avgW > 0 ? 10 : 0);

  return {
    wins, losses, resolved, wr, avgW, avgL, totalPnl, totalVolume,
    uniqueTokens: uniqueTokens.size,
    estimatedMarkets: Math.max(1, Math.ceil(uniqueTokens.size / 2)),
    efficiency, edgeRatio, openCount,
  };
}

function openPolymarketProfile(address) {
  window.open(`https://polymarket.com/profile/${address}`, '_blank');
}

/* ============================================================================
   Data Loading
   ============================================================================ */

async function loadData() {
  try {
    const [analytics, wallets, markets] = await Promise.all([
      fetch(DATA_BASE + 'analytics.json').then(r => {
        if (!r.ok) throw new Error('Not found');
        return r.json();
      }).catch(() => null),
      fetch(DATA_BASE + 'wallets.json').then(r => {
        if (!r.ok) throw new Error('Not found');
        return r.json();
      }).catch(() => null),
      fetch(DATA_BASE + 'markets.json').then(r => {
        if (!r.ok) throw new Error('Not found');
        return r.json();
      }).catch(() => null),
    ]);

    return { analytics, wallets, markets };
  } catch (error) {
    console.error('Data load error:', error);
    return { analytics: null, wallets: null, markets: null };
  }
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
    case 'screener':
      initScreener();
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
        s = { totalPnl: 0, wr: 0, estimatedMarkets: 0, resolved: 0, efficiency: 0, edgeRatio: 0, avgW: 0, avgL: 0, wins: 0, losses: 0, totalVolume: 0, openCount: 0 };
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

  document.getElementById('metric-wallets').textContent = leaderboardData.length.toLocaleString();
  document.getElementById('metric-avg-score').textContent = avgScore.toFixed(1);
  document.getElementById('metric-pnl').textContent = fmtDollars(totalPnl);
  document.getElementById('metric-win-rate').textContent = (pooledWinRate * 100).toFixed(1) + '%';

  const leaderboardColumns = [
    { field: 'rank', render: v => String(v) },
    { field: 'score', render: v => `<span class="badge ${scoreClass(v)}">${v.toFixed(1)}</span>` },
    { field: 'address', render: v => `<span class="address-link" onclick="openPolymarketProfile('${v}')">${truncAddr(v)}</span>` },
    { field: 'totalPnl', render: v => `<span class="${pnlClass(v)}">${fmtDollars(v)}</span>` },
    { field: 'winRate', render: v => ((v || 0) * 100).toFixed(1) + '%' },
    { field: 'markets', render: v => String(v) },
    { field: 'resolved', render: v => String(v) },
    { field: 'efficiency', render: v => ((v || 0) * 100).toFixed(2) + '%' },
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
        <div class="detail-item-label">Win Rate</div>
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
        <div class="detail-item-label">Resolved</div>
        <div class="detail-item-value">${s.resolved || 0}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Volume</div>
        <div class="detail-item-value">${fmtDollars(s.totalVolume || 0)}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Open Positions</div>
        <div class="detail-item-value">${s.openCount || 0}</div>
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

  const consensus = data.analytics.consensus || [];

  const totalWallets = data.analytics.summary?.totalWallets || 1;

  // Build raw data with raw conviction scores
  const rawConsensus = consensus.map(m => {
    const wc = m.walletCount || m.wallets?.length || 0;
    const as = m.avgScore || m.avgHolderScore || 0;
    return {
      title: m.marketTitle || m.title || 'Unknown',
      slug: m.slug || m.tokenId || '',
      marketId: m.tokenId || m.marketId || '',
      walletCount: wc,
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
  const holders = market.holders.map((h, idx) => `
    <div class="detail-list-item">
      <div class="detail-list-item-label">${idx + 1}. <span class="address-link" onclick="openPolymarketProfile('${h.address}')">${truncAddr(h.address)}</span></div>
      <div class="detail-list-item-value">
        <span class="badge ${scoreClass(h.score)}">${h.score.toFixed(1)}</span>
        ${fmtDollars(h.pnl || 0)}
      </div>
    </div>
  `).join('');

  const html = `
    <div class="detail-grid">
      <div class="detail-item">
        <div class="detail-item-label">Market Title</div>
        <div class="detail-item-value" style="font-size: 14px;">${market.slug ? `<a href="${polymarketUrl(market.slug)}" target="_blank" style="color: var(--accent-light);">${market.title}</a>` : market.title}</div>
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
    avgEntryPrice: m.holders?.length ? m.holders.reduce((s, h) => s + (h.entryPrice || 0), 0) / m.holders.length : 0,
    direction: m.consensusDirection || 'MIXED',
    holders: m.holders || []
  }));

  const portfolioColumns = [
    { field: 'title', render: (v, row) => `<a href="${polymarketUrl(row.slug)}" target="_blank" style="color: var(--accent-light);">${v}</a>` },
    { field: 'holdingCount', render: v => String(v) },
    { field: 'totalShares', render: v => fmt(v, 0) },
    { field: 'avgEntryPrice', render: v => '$' + v.toFixed(2) },
    { field: 'direction', render: v => `<span class="badge ${v === 'YES' ? 'badge-positive' : v === 'NO' ? 'badge-negative' : 'badge-mid'}">${v}</span>` }
  ];

  createSortableTable('portfolio-table', portfolioColumns, portfolioData, (row) => {
    showPortfolioDetail(row);
  });
}

function showPortfolioDetail(market) {
  const holders = market.holders.map((h, idx) => `
    <div class="detail-list-item">
      <div class="detail-list-item-label">${idx + 1}. <span class="address-link" onclick="openPolymarketProfile('${h.address}')">${truncAddr(h.address)}</span></div>
      <div class="detail-list-item-value">${fmt(h.shares || 0, 0)} shares @ $${(h.entryPrice || 0).toFixed(2)}</div>
    </div>
  `).join('');

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
        <div class="detail-item-label">Total Shares</div>
        <div class="detail-item-value">${fmt(market.totalShares, 0)}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Consensus Direction</div>
        <div class="detail-item-value">${market.direction}</div>
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

function renderSignals() {
  if (!data.analytics) return;

  // === Estate Performance Summary ===
  const rp = data.analytics.resolvedPositions || {};
  const periods = rp.periodStats || {};

  function setPeriodMetrics(period, idSuffix) {
    const s = periods[period] || {};
    const el = document.getElementById('metric-resolved-' + idSuffix);
    const wrEl = document.getElementById('metric-wr-' + idSuffix);
    if (el) el.textContent = (s.total || 0).toLocaleString();
    if (wrEl) {
      const wr = ((s.winRate || 0) * 100).toFixed(1);
      const pnl = s.totalPnl || 0;
      wrEl.innerHTML = `<span style="color: ${pnl >= 0 ? 'var(--green)' : 'var(--red)'}">${fmtDollars(pnl)}</span> &middot; ${wr}% WR (${s.wins || 0}W/${s.losses || 0}L)`;
    }
  }

  setPeriodMetrics('today', 'today');
  setPeriodMetrics('week', 'week');
  setPeriodMetrics('month', 'month');
  setPeriodMetrics('quarter', 'quarter');
  setPeriodMetrics('allTime', 'all');

  const allTime = periods.allTime || {};
  const pnlEl = document.getElementById('metric-resolved-pnl');
  if (pnlEl) {
    const pnl = allTime.totalPnl || 0;
    pnlEl.innerHTML = `<span style="color: ${pnl >= 0 ? 'var(--green)' : 'var(--red)'}">${fmtDollars(pnl)}</span>`;
  }
  const avgEl = document.getElementById('metric-avg-win-loss');
  if (avgEl) avgEl.textContent = `Avg Win: ${fmtDollars(allTime.avgWin || 0)} / Avg Loss: ${fmtDollars(allTime.avgLoss || 0)}`;

  // === Resolved Positions Table ===
  const resolvedData = (rp.positions || []).map(p => ({
    marketTitle: p.marketTitle || 'Unknown',
    slug: p.slug || '',
    address: p.address || '',
    pnl: p.pnl || 0,
    roi: p.roi || 0,
    totalBought: p.totalBought || 0,
    walletScore: p.walletScore || 0,
    timestamp: p.timestamp || null,
  }));

  const resolvedColumns = [
    { field: 'marketTitle', render: (v, row) => row.slug ? `<a href="${polymarketUrl(row.slug)}" target="_blank" style="color: var(--accent-light);">${v}</a>` : `<span style="color: var(--accent-light);">${v}</span>` },
    { field: 'address', render: v => `<span class="address-link" onclick="openPolymarketProfile('${v}')">${truncAddr(v)}</span>` },
    { field: 'pnl', render: v => `<span class="${pnlClass(v)}">${fmtDollars(v)}</span>` },
    { field: 'roi', render: v => `<span class="${pnlClass(v)}">${(v * 100).toFixed(1)}%</span>` },
    { field: 'totalBought', render: v => fmtDollars(v) },
    { field: 'walletScore', render: v => `<span class="badge ${scoreClass(v)}">${v.toFixed(1)}</span>` },
    { field: 'timestamp', render: v => relativeTime(v) },
  ];

  createSortableTable('resolved-table', resolvedColumns, resolvedData);

  // === Existing sections ===
  const biggestWins = data.analytics.biggestWins || [];
  const hotWallets = data.analytics.hotWallets || [];
  const emergingConsensus = data.analytics.consensus?.filter(m => m.isEmerging) || [];

  // Biggest wins table
  const winsData = biggestWins.slice(0, 50).map(w => ({
    marketTitle: w.marketTitle || 'Unknown',
    slug: w.slug || '',
    wallet: w.address || w.wallet || '',
    pnl: w.pnl || 0,
    roi: w.roi || 0,
    score: w.walletScore || w.score || 0
  }));

  const winsColumns = [
    { field: 'marketTitle', render: (v, row) => row.slug ? `<a href="${polymarketUrl(row.slug)}" target="_blank" style="color: var(--accent-light);">${v}</a>` : `<span style="color: var(--accent-light);">${v}</span>` },
    { field: 'wallet', render: v => `<span class="address-link" onclick="openPolymarketProfile('${v}')">${truncAddr(v)}</span>` },
    { field: 'pnl', render: v => `<span class="${pnlClass(v)}">${fmtDollars(v)}</span>` },
    { field: 'roi', render: v => `<span class="${pnlClass(v)}">${(v * 100).toFixed(1)}%</span>` },
    { field: 'score', render: v => `<span class="badge ${scoreClass(v)}">${v.toFixed(1)}</span>` }
  ];

  createSortableTable('wins-table', winsColumns, winsData);

  // Hot wallets table
  const hotData = hotWallets.slice(0, 50).map(w => ({
    address: w.address || '',
    score: w.score || 0,
    recentPnL: w.recentPnL || 0,
    activeMarkets: w.activeMarkets || 0,
    recency: w.recency || 0
  }));

  const hotColumns = [
    { field: 'address', render: v => `<span class="address-link" onclick="openPolymarketProfile('${v}')">${truncAddr(v)}</span>` },
    { field: 'score', render: v => `<span class="badge ${scoreClass(v)}">${v.toFixed(1)}</span>` },
    { field: 'recentPnL', render: v => `<span class="${pnlClass(v)}">${fmtDollars(v)}</span>` },
    { field: 'activeMarkets', render: v => String(v) },
    { field: 'recency', render: v => (v || 0).toFixed(1) + '%' }
  ];

  createSortableTable('hot-wallets-table', hotColumns, hotData);

  // Emerging consensus table
  const emergingData = emergingConsensus.map(m => ({
    marketTitle: m.marketTitle || m.title || 'Unknown',
    slug: m.slug || '',
    walletCount: m.walletCount || m.wallets?.length || 0,
    avgScore: m.avgScore || m.avgHolderScore || 0
  }));

  const emergingColumns = [
    { field: 'marketTitle', render: (v, row) => row.slug ? `<a href="${polymarketUrl(row.slug)}" target="_blank" style="color: var(--accent-light);">${v}</a>` : `<span style="color: var(--accent-light);">${v}</span>` },
    { field: 'walletCount', render: v => String(v) },
    { field: 'avgScore', render: v => `<span class="badge ${scoreClass(v)}">${v.toFixed(1)}</span>` }
  ];

  createSortableTable('emerging-table', emergingColumns, emergingData);
}

/* ============================================================================
   Screener Tab - Full Implementation
   ============================================================================ */

let screenerState = {
  allWallets: [],
  filteredWallets: [],
  stage2Results: []
};

const SCREENER_STORAGE_KEY = 'polymarket_screener_results';
const STAGE1_STORAGE_KEY = 'polymarket_stage1_wallets';
const NETWORK_TIMEOUT = 15000;

// Subgraph query functions for screener
async function queryAllWallets(first = 1000, skip = 0) {
  const query = `
    query {
      accounts(first: ${first}, skip: ${skip}) {
        id
        profitLoss
      }
    }
  `;

  try {
    const response = await fetch('https://api.goldsky.com/api/public/project_clr7yc6vv0000qg0h82f17yqy/subgraphs/Polymarket_Trades/0.0.1/gql', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query })
    });

    if (!response.ok) throw new Error('Network error');
    const data = await response.json();
    return data.data?.accounts || [];
  } catch (error) {
    console.error('queryAllWallets error:', error);
    return [];
  }
}

async function queryWalletPositions(address) {
  const query = `
    query {
      account(id: "${address.toLowerCase()}") {
        id
        profitLoss
        trades {
          id
          outcome {
            market {
              id
              title
            }
          }
          shares
          price
          createdAtBlockNumber
        }
      }
    }
  `;

  try {
    const response = await fetch('https://api.goldsky.com/api/public/project_clr7yc6vv0000qg0h82f17yqy/subgraphs/Polymarket_Trades/0.0.1/gql', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query })
    });

    if (!response.ok) throw new Error('Network error');
    const data = await response.json();
    return data.data?.account || null;
  } catch (error) {
    console.error('queryWalletPositions error:', error);
    return null;
  }
}

async function queryResolvedMarkets() {
  const query = `
    query {
      markets(first: 1000, where: { resolutionTime_not: null }) {
        id
        title
        resolutionTime
      }
    }
  `;

  try {
    const response = await fetch('https://api.goldsky.com/api/public/project_clr7yc6vv0000qg0h82f17yqy/subgraphs/Polymarket_Trades/0.0.1/gql', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query })
    });

    if (!response.ok) throw new Error('Network error');
    const data = await response.json();
    return data.data?.markets || [];
  } catch (error) {
    console.error('queryResolvedMarkets error:', error);
    return [];
  }
}

function logProgress(message, type = 'info') {
  const logEl = document.getElementById('progressLog');
  if (logEl) {
    const entry = document.createElement('div');
    entry.className = `progress-log-entry ${type}`;
    entry.textContent = message;
    logEl.appendChild(entry);
    logEl.scrollTop = logEl.scrollHeight;
  }
  console.log(message);
}

function updateProgress(percent, text) {
  document.getElementById('progressFill').style.width = percent + '%';
  document.getElementById('progressText').textContent = text;
}

async function discoverWallets(minScore, minWinRate, minPositions, maxWallets) {
  logProgress('Starting Stage 1: Wallet Discovery...', 'info');

  const discovered = [];
  let skip = 0;
  let allFetched = [];

  // Fetch wallets in batches
  for (let i = 0; i < 5; i++) {
    updateProgress(i * 20, `Fetching wallet batch ${i + 1}/5...`);
    const batch = await queryAllWallets(1000, skip);
    if (!batch.length) break;
    allFetched = allFetched.concat(batch);
    skip += 1000;
  }

  logProgress(`Fetched ${allFetched.length} wallets from subgraph`, 'success');

  // Analyze each wallet
  for (let i = 0; i < Math.min(allFetched.length, maxWallets); i++) {
    updateProgress(20 + (i / maxWallets) * 30, `Analyzing wallet ${i + 1}/${Math.min(allFetched.length, maxWallets)}...`);

    const walletData = await queryWalletPositions(allFetched[i].id);
    if (!walletData) continue;

    const score = computeScore(walletData);

    if (score >= minScore) {
      discovered.push({
        address: allFetched[i].id,
        score: score,
        pnl: walletData.profitLoss || 0,
        tradeCount: walletData.trades?.length || 0
      });
    }
  }

  screenerState.allWallets = discovered.sort((a, b) => b.score - a.score);
  localStorage.setItem(STAGE1_STORAGE_KEY, JSON.stringify(screenerState.allWallets));

  logProgress(`Stage 1 complete: ${discovered.length} wallets passed filters`, 'success');
  updateProgress(50, `Stage 1 complete: ${discovered.length} wallets...`);

  return discovered;
}

function computeScore(walletData) {
  const trades = walletData.trades || [];
  if (trades.length === 0) return 0;

  const pnl = walletData.profitLoss || 0;
  const winCount = trades.filter(t => (t.shares || 0) * (t.price || 0) > 0).length;
  const winRate = winCount / trades.length;

  const pnlScore = Math.min(100, (pnl / 10000) * 50 + 50);
  const winRateScore = winRate * 100;

  return (pnlScore + winRateScore) / 2;
}

async function analyzePositions(wallets) {
  logProgress('Starting Stage 2: Deep Position Analysis...', 'info');

  const results = [];

  for (let i = 0; i < wallets.length; i++) {
    updateProgress(50 + (i / wallets.length) * 40, `Analyzing positions for wallet ${i + 1}/${wallets.length}...`);

    const walletData = await queryWalletPositions(wallets[i].address);
    if (!walletData) continue;

    const analysis = {
      address: wallets[i].address,
      score: wallets[i].score,
      winRate: (computeScore(walletData) / 100).toFixed(2),
      positionCount: walletData.trades?.length || 0,
      avgPnL: (walletData.profitLoss || 0) / (walletData.trades?.length || 1),
      consensusMarkets: 0
    };

    results.push(analysis);
  }

  screenerState.stage2Results = results.sort((a, b) => b.score - a.score);
  localStorage.setItem(SCREENER_STORAGE_KEY, JSON.stringify(screenerState.stage2Results));

  logProgress(`Stage 2 complete: ${results.length} wallets analyzed`, 'success');
  updateProgress(90, 'Finalizing results...');

  return results;
}

async function runScreener() {
  const progressArea = document.getElementById('progressArea');
  const resultsArea = document.getElementById('resultsArea');
  const logEl = document.getElementById('progressLog');

  progressArea.classList.remove('hidden');
  resultsArea.classList.add('hidden');
  logEl.innerHTML = '';

  try {
    const minScore = parseInt(document.getElementById('minScore').value) || 30;
    const minWinRate = parseInt(document.getElementById('minWinRate').value) || 40;
    const minPositions = parseInt(document.getElementById('minPositions').value) || 5;
    const maxWallets = parseInt(document.getElementById('maxWallets').value) || 100;
    const minConviction = parseInt(document.getElementById('minConviction').value) || 50;

    logProgress('Initiating screener...', 'info');

    const discovered = await discoverWallets(minScore, minWinRate, minPositions, maxWallets);
    const results = await analyzePositions(discovered);

    const filtered = results.filter(r => r.score >= minScore);

    renderScreenerResults(filtered);
    resultsArea.classList.remove('hidden');

    updateProgress(100, 'Screener complete!');
    logProgress('Screener finished successfully!', 'success');
  } catch (error) {
    console.error('Screener error:', error);
    logProgress(`Error: ${error.message}`, 'error');
  }
}

function renderScreenerResults(results) {
  const tbody = document.getElementById('screener-results-tbody');
  tbody.innerHTML = '';

  if (!results || results.length === 0) {
    const tr = document.createElement('tr');
    tr.innerHTML = '<td colspan="6" class="empty-state">No results match criteria</td>';
    tbody.appendChild(tr);
    return;
  }

  results.forEach((row, idx) => {
    const tr = document.createElement('tr');
    tr.dataset.rowIndex = idx;

    tr.innerHTML = `
      <td><span class="address-link" onclick="openPolymarketProfile('${row.address}')">${truncAddr(row.address)}</span></td>
      <td><span class="badge ${scoreClass(row.score)}">${row.score.toFixed(1)}</span></td>
      <td>${(row.winRate * 100).toFixed(1)}%</td>
      <td>${row.positionCount}</td>
      <td><span class="${pnlClass(row.avgPnL)}">${fmtDollars(row.avgPnL)}</span></td>
      <td>${row.consensusMarkets}</td>
    `;

    tr.style.cursor = 'pointer';
    tr.addEventListener('click', () => showScreenerDetail(row));

    tbody.appendChild(tr);
  });
}

function showScreenerDetail(wallet) {
  const html = `
    <div class="detail-grid">
      <div class="detail-item">
        <div class="detail-item-label">Address</div>
        <div class="detail-item-value" style="font-size: 12px; font-family: monospace; word-break: break-all;">${wallet.address}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Score</div>
        <div class="detail-item-value">${wallet.score.toFixed(1)}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Win Rate</div>
        <div class="detail-item-value">${(wallet.winRate * 100).toFixed(1)}%</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Avg PnL</div>
        <div class="detail-item-value" style="color: ${wallet.avgPnL >= 0 ? 'var(--green)' : 'var(--red)'};">${fmtDollars(wallet.avgPnL)}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Positions</div>
        <div class="detail-item-value">${wallet.positionCount}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Consensus Markets</div>
        <div class="detail-item-value">${wallet.consensusMarkets}</div>
      </div>
    </div>
  `;

  showDetailPanel('screener', html);
}

function loadPrevious() {
  const stored = localStorage.getItem(SCREENER_STORAGE_KEY);
  if (stored) {
    try {
      const results = JSON.parse(stored);
      renderScreenerResults(results);
      document.getElementById('resultsArea').classList.remove('hidden');
      logProgress('Loaded previous results from storage', 'success');
    } catch (error) {
      logProgress('Failed to load previous results', 'error');
    }
  } else {
    logProgress('No previous results found', 'info');
  }
}

function exportResults() {
  const tbody = document.getElementById('screener-results-tbody');
  const rows = tbody.querySelectorAll('tr');

  let csv = 'Address,Score,Win Rate,Position Count,Avg PnL,Consensus Markets\n';

  rows.forEach(row => {
    if (!row.querySelector('.empty-state')) {
      const cells = row.querySelectorAll('td');
      const address = cells[0].innerText.replace(/\.\.\./g, '');
      const score = cells[1].innerText.replace(/[^\d.]/g, '');
      const winRate = cells[2].innerText;
      const posCount = cells[3].innerText;
      const avgPnL = cells[4].innerText;
      const consensus = cells[5].innerText;

      csv += `${address},${score},${winRate},${posCount},${avgPnL},${consensus}\n`;
    }
  });

  const blob = new Blob([csv], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `polymarket-screener-${new Date().toISOString().split('T')[0]}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

function initScreener() {
  const runBtn = document.getElementById('runScreener');
  const loadBtn = document.getElementById('loadPreviousBtn');
  const exportBtn = document.getElementById('exportResults');
  const clearBtn = document.getElementById('clearResults');

  if (runBtn && !runBtn.dataset.initialized) {
    runBtn.addEventListener('click', runScreener);
    loadBtn.addEventListener('click', loadPrevious);
    exportBtn.addEventListener('click', exportResults);
    clearBtn.addEventListener('click', () => {
      localStorage.removeItem(SCREENER_STORAGE_KEY);
      document.getElementById('screener-results-tbody').innerHTML = '<tr><td colspan="6" class="empty-state">No results yet</td></tr>';
      document.getElementById('resultsArea').classList.add('hidden');
      logProgress('Results cleared', 'info');
    });

    runBtn.dataset.initialized = 'true';

    // Load previous if exists
    const stored = localStorage.getItem(SCREENER_STORAGE_KEY);
    if (stored) {
      try {
        const results = JSON.parse(stored);
        if (results.length > 0) {
          renderScreenerResults(results);
          document.getElementById('resultsArea').classList.remove('hidden');
        }
      } catch (e) {
        // Ignore parse errors
      }
    }
  }
}

/* ============================================================================
   Initialization
   ============================================================================ */

async function init() {
  // Load data
  data = await loadData();
  updateStatusBar();

  // Pre-load wallets.json for time filtering (has per-position data)
  try {
    const walletsResp = await fetch(DATA_BASE + 'wallets.json');
    if (walletsResp.ok) walletsData = await walletsResp.json();
  } catch { /* ok — time filtering just won't recompute stats */ }

  // Attach tab listeners
  document.querySelectorAll('.tab').forEach(btn => {
    btn.addEventListener('click', () => {
      const tabName = btn.dataset.tab;
      switchTab(tabName);
    });
  });

  // Time filter buttons
  document.querySelectorAll('.time-btn').forEach(btn => {
    btn.addEventListener('click', function() {
      document.querySelectorAll('.time-btn').forEach(b => b.classList.remove('active'));
      this.classList.add('active');
      currentTimeRange = this.dataset.range;
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
