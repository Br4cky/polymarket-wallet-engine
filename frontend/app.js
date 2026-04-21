/* ============================================================================
   Polymarket Signal Engine v2 — Dashboard
   ============================================================================ */

const DATA_BASE = (typeof CONFIG !== 'undefined' && CONFIG.githubUser !== 'YOUR_GITHUB_USERNAME')
  ? CONFIG.dataBase
  : '../data/';

let data = { analytics: null };
let currentTab = 'wallets';
let sortState = {};
let chartInstances = {};
let currentPaperPortfolio = 'combined';
let cvMinFilter = 0;
let sigFilterTier = 'all';
let sigFilterType = null;

/* ============================================================================
   Utility Functions
   ============================================================================ */

function fmt(n, decimals = 2) {
  if (n === null || n === undefined) return '0';
  if (typeof n !== 'number') return String(n);
  if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(decimals) + 'B';
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(decimals) + 'M';
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(decimals) + 'K';
  return n.toFixed(decimals);
}

function fmtDollars(n, d = 2) {
  if (n === null || n === undefined) return '$0';
  return '$' + fmt(n, d);
}

function pnlClass(v) {
  if (v > 0) return 'badge-positive';
  if (v < 0) return 'badge-negative';
  return '';
}

// Score lives on a tight scale: elite underdog tops out around 48,
// good directional traders in the 15-25 range, mid pool in 5-15.
function scoreClass(s) {
  if (s >= 25) return 'badge-high';
  if (s >= 10) return 'badge-mid';
  return 'badge-low';
}

function roiClass(roi) {
  if (roi == null) return '';
  if (roi >= 0.15) return 'badge-high';
  if (roi >= 0.05) return 'badge-mid';
  return 'badge-low';
}

function truncAddr(addr) {
  if (!addr || addr.length < 10) return addr || '';
  return addr.slice(0, 6) + '...' + addr.slice(-4);
}

function truncate(str, max) {
  if (!str) return '';
  return str.length > max ? str.slice(0, max) + '...' : str;
}

function polymarketUrl(slug, eventSlug) {
  if (!slug) return '#';
  // Polymarket URLs are /event/{eventSlug}/{marketSlug}
  // If slug already contains a /, it's already the full path
  if (slug.includes('/')) return `https://polymarket.com/event/${slug}`;
  if (eventSlug && eventSlug !== slug) return `https://polymarket.com/event/${eventSlug}/${slug}`;
  return `https://polymarket.com/event/${slug}`;
}

function openPolymarketProfile(address) {
  window.open(`https://polymarket.com/profile/${address}`, '_blank');
}

function destroyChart(id) {
  if (chartInstances[id]) {
    chartInstances[id].destroy();
    delete chartInstances[id];
  }
}

const CHART_DEFAULTS = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: { labels: { color: '#8888a0', font: { size: 12 } } },
    tooltip: { backgroundColor: '#1a1a26', titleColor: '#e4e4ef', bodyColor: '#e4e4ef', borderColor: '#2a2a3a', borderWidth: 1 },
  },
  scales: {
    x: { ticks: { color: '#8888a0', font: { size: 11 } }, grid: { color: 'rgba(42,42,58,0.5)' } },
    y: { ticks: { color: '#8888a0', font: { size: 11 } }, grid: { color: 'rgba(42,42,58,0.5)' } },
  },
};

/* ============================================================================
   Data Loading
   ============================================================================ */

async function fetchGzJSON(url) {
  try {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error('Not found');
    const ds = new DecompressionStream('gzip');
    const decompressed = resp.body.pipeThrough(ds);
    const text = await new Response(decompressed).text();
    return JSON.parse(text);
  } catch {
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
    const analytics = await fetchGzJSON(DATA_BASE + 'analytics.json.gz');
    return { analytics };
  } catch (error) {
    console.error('Data load error:', error);
    return { analytics: null };
  }
}

function updateStatusBar() {
  const bar = document.getElementById('statusBar');
  if (!data.analytics) {
    bar.textContent = 'No scan data yet. Run the scanner to populate.';
    bar.className = 'status error';
  } else {
    const ts = data.analytics.timestamp || new Date().toISOString();
    const time = new Date(ts).toLocaleTimeString();
    const scan = data.analytics.scanCount || '?';
    bar.textContent = `Scan #${scan} at ${time}`;
    bar.className = 'status loaded';
  }
}

/* ============================================================================
   Sortable Table Helper
   ============================================================================ */

function createSortableTable(tableId, columns, rowData, onRowClick) {
  const table = document.getElementById(tableId);
  if (!table) return;
  const thead = table.querySelector('thead');
  const tbody = table.querySelector('tbody');

  if (!sortState[tableId]) sortState[tableId] = { field: null, dir: 'desc' };

  // Header click sorting
  thead.querySelectorAll('th.sortable').forEach(th => {
    th.onclick = () => {
      const field = th.dataset.field;
      const st = sortState[tableId];
      if (st.field === field) {
        st.dir = st.dir === 'desc' ? 'asc' : 'desc';
      } else {
        st.field = field;
        st.dir = 'desc';
      }
      thead.querySelectorAll('th').forEach(h => h.classList.remove('sorted-asc', 'sorted-desc'));
      th.classList.add(st.dir === 'asc' ? 'sorted-asc' : 'sorted-desc');
      renderRows();
    };
  });

  function renderRows() {
    const st = sortState[tableId];
    let sorted = [...rowData];
    if (st.field) {
      sorted.sort((a, b) => {
        const av = a[st.field] ?? 0, bv = b[st.field] ?? 0;
        if (typeof av === 'string') return st.dir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av);
        return st.dir === 'asc' ? av - bv : bv - av;
      });
    }

    if (sorted.length === 0) {
      tbody.innerHTML = `<tr><td colspan="${columns.length}" class="empty-state">No data</td></tr>`;
      return;
    }

    tbody.innerHTML = sorted.map(row => {
      const cells = columns.map(col => `<td>${col.render(row[col.field], row)}</td>`).join('');
      return `<tr>${cells}</tr>`;
    }).join('');

    if (onRowClick) {
      tbody.querySelectorAll('tr').forEach((tr, i) => {
        tr.addEventListener('click', () => onRowClick(sorted[i]));
      });
    }
  }

  renderRows();
}

/* ============================================================================
   Detail Panel
   ============================================================================ */

function showDetailPanel(tab, html) {
  const panel = document.getElementById('detail-panel-' + tab);
  const content = document.getElementById('detail-content-' + tab);
  if (panel && content) {
    content.innerHTML = html;
    panel.classList.remove('hidden');
  }
}

function closeDetailPanel(tab) {
  const panel = document.getElementById('detail-panel-' + tab);
  if (panel) panel.classList.add('hidden');
}

// Expose to onclick handlers
window.closeDetailPanel = closeDetailPanel;
window.openPolymarketProfile = openPolymarketProfile;

/* ============================================================================
   Tab 1: Wallet Pool
   ============================================================================ */

function renderWalletPool() {
  if (!data.analytics) return;
  const lb = data.analytics.leaderboard || [];

  const totalPnl = lb.reduce((s, w) => s + (w.stats?.totalPnl || 0), 0);
  const avgScore = lb.length > 0 ? lb.reduce((s, w) => s + (w.score || 0), 0) / lb.length : 0;
  const totalWins = lb.reduce((s, w) => s + (w.stats?.wins || 0), 0);
  const totalResolved = lb.reduce((s, w) => s + (w.stats?.resolved || 0), 0);
  const avgWR = totalResolved > 0 ? (totalWins / totalResolved * 100) : 0;

  document.getElementById('wp-pool-size').textContent = lb.length.toLocaleString();
  document.getElementById('wp-avg-score').textContent = avgScore.toFixed(1);
  document.getElementById('wp-total-pnl').textContent = fmtDollars(totalPnl);
  document.getElementById('wp-avg-wr').textContent = avgWR.toFixed(1) + '%';

  // Scoring-health tiles — optional, only populated if present in DOM.
  const scoredCount = lb.filter(w => typeof w.score === 'number').length;
  const scoringCoverage = lb.length > 0 ? (scoredCount / lb.length * 100) : 0;
  const meanPickerCount = lb.filter(w => w.stats?.isMeanPickerShape === true).length;
  const wouldEvictCount = lb.filter(w => w.wouldEvict).length;
  const meanPickerCap = lb
    .filter(w => w.stats?.isMeanPickerShape === true)
    .reduce((s, w) => s + (w.stats?.decidedCapital || 0), 0);
  const covEl = document.getElementById('wp-scoring-coverage');
  if (covEl) covEl.textContent = `${scoredCount}/${lb.length} (${scoringCoverage.toFixed(0)}%)`;
  const mpEl = document.getElementById('wp-mean-pickers');
  if (mpEl) mpEl.textContent = `${meanPickerCount} wallets / ${fmtDollars(meanPickerCap)}`;
  const evEl = document.getElementById('wp-would-evict');
  if (evEl) evEl.textContent = `${wouldEvictCount} flagged`;

  // Three PnL numbers with different meanings:
  //   onChainPnl   — Goldsky realizedPnl. Only counts positions explicitly
  //                  redeemed/sold on-chain. Zero for unredeemed winners.
  //   samplePnl    — Analyzer PnL from /activity events (3000-event cap).
  //                  Handles unredeemed winners via marketLookup inference,
  //                  but truncates deep history for power users.
  //   effectivePnl — max(onChain, sample). What scoring uses, because both
  //                  measurement systems are incomplete in opposite ways.
  const walletData = lb.map((w, i) => ({
    rank: i + 1,
    score: typeof w.score === 'number' ? w.score : null,
    address: w.address || '',
    winRate: w.stats?.wr || 0,
    onChainPnl: w.stats?.totalPnl || 0,                 // Goldsky realized
    samplePnl: w.stats?.samplePnl || 0,                  // analyzer 3000-event sample
    effectivePnl: w.stats?.effectivePnl
      || Math.max(w.stats?.totalPnl || 0, w.stats?.samplePnl || 0),
    decidedROI: w.stats?.decidedROI ?? null,
    decidedCapital: w.stats?.decidedCapital ?? null,
    isMeanPicker: w.stats?.isMeanPickerShape === true,
    wouldEvict: w.wouldEvict ? w.wouldEvict.reason : null,
    statsSpanDays: w.stats?.statsSpanDays || 0,
    tradesTruncated: w.stats?.tradesTruncated === true,
    resolved: w.stats?.resolved || 0,
    tradesPerWeek: w.stats?.positionsPerWeek || 0,
    consistency: w.stats?.weeklyConsistency || 0,
  }));

  createSortableTable('wallet-table', [
    { field: 'rank', render: v => String(v) },
    { field: 'score', label: 'Score', render: (v, row) => {
      // Primary score (decidedROI-weighted + MM/alpha penalties).
      // null when the wallet hasn't been scored yet — dim placeholder.
      if (v == null) return '<span style="opacity:0.35" title="Not yet scored">—</span>';
      const badge = `<span class="badge ${scoreClass(v)}">${v.toFixed(1)}</span>`;
      const flag = row && row.wouldEvict
        ? ` <span title="Shadow eviction: ${row.wouldEvict} — would be removed once EVICTION_MODE flips to live" style="color:#e17055">✂</span>`
        : (row && row.isMeanPicker
          ? ` <span title="Mean-picker shape (high WR, tiny ROI) — score penalised 5×" style="color:#fdcb6e">◇</span>`
          : '');
      return `${badge}${flag}`;
    }},
    { field: 'address', render: v => `<span class="address-link" onclick="openPolymarketProfile('${v}')">${truncAddr(v)}</span>` },
    { field: 'decidedROI', label: 'Decided ROI', render: v => {
      if (v == null) return '<span style="opacity:0.35">—</span>';
      const cls = roiClass(v);
      const pct = (v * 100).toFixed(1) + '%';
      return `<span class="badge ${cls}" title="Ground-truth ROI on resolved capital: (realizedPnl + decidedPnl) / (totalCost − openCapitalAtRisk)">${pct}</span>`;
    }},
    { field: 'decidedCapital', label: 'Decided cap', render: v => {
      if (v == null) return '<span style="opacity:0.35">—</span>';
      return `<span title="Capital-at-risk on resolved positions (totalCost excluding open bets)">${fmtDollars(v)}</span>`;
    }},
    { field: 'winRate', render: v => ((v || 0) * 100).toFixed(1) + '%' },
    { field: 'effectivePnl', label: 'PnL (effective)', render: v => `<span class="${pnlClass(v)}" title="max(on-chain realized, analyzer sample) — what scoring actually uses">${fmtDollars(v)}</span>` },
    { field: 'onChainPnl', label: 'On-chain', render: v => `<span class="${pnlClass(v)}" style="opacity:0.7" title="Goldsky realizedPnl — only counts positions the wallet explicitly redeemed or sold. $0 for unredeemed winners.">${fmtDollars(v)}</span>` },
    { field: 'samplePnl', label: 'Sample', render: v => `<span class="${pnlClass(v)}" style="opacity:0.7" title="Analyzer PnL from /activity events (3000-event cap). Handles unredeemed winners but truncates deep history.">${fmtDollars(v)}</span>` },
    { field: 'statsSpanDays', render: (v, row) => {
      if (!v) return '<span style="opacity:0.4">-</span>';
      const badge = row && row.tradesTruncated
        ? ` <span title="Hit 3000-event API cap — sample is a fixed-size recency window, not full history" style="color:#fdcb6e">⚠</span>`
        : '';
      return `${v}d${badge}`;
    }},
    { field: 'resolved', render: v => String(v) },
    { field: 'tradesPerWeek', render: (v, row) => {
      const val = (v || 0).toFixed(1);
      const badge = row && row.tradesTruncated
        ? ` <span title="Hit 3000-event API cap — based on the fixed 90-day recency window, not full history" style="color:#fdcb6e">⚠</span>`
        : '';
      return `${val}${badge}`;
    }},
  ], walletData);

  renderScoreDistribution(lb);
  renderPoolTrend();
}

function renderScoreDistribution(lb) {
  destroyChart('score-dist');
  // Score distribution on the unified scale (effective max ~55, practical
  // top decile ~25+). Buckets sized for that range: 0-5, 5-10, 10-20, 20-30, 30+.
  const scores = lb
    .map(w => typeof w.score === 'number' ? w.score : null)
    .filter(s => s != null);
  const bucketEdges = [5, 10, 20, 30];
  const buckets = [0, 0, 0, 0, 0];
  scores.forEach(s => {
    const idx = bucketEdges.findIndex(e => s < e);
    buckets[idx === -1 ? 4 : idx]++;
  });
  const labelEl = document.getElementById('wp-score-dist-label');
  if (labelEl) labelEl.textContent = 'Score distribution';

  const ctx = document.getElementById('chart-score-dist');
  if (!ctx) return;
  chartInstances['score-dist'] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: ['0-20', '20-40', '40-60', '60-80', '80-100'],
      datasets: [{
        label: 'Wallets',
        data: buckets,
        backgroundColor: ['rgba(225,112,85,0.4)', 'rgba(253,203,110,0.4)', 'rgba(253,203,110,0.4)', 'rgba(0,184,148,0.4)', 'rgba(108,92,231,0.4)'],
        borderColor: ['#e17055', '#fdcb6e', '#fdcb6e', '#00b894', '#6c5ce7'],
        borderWidth: 1,
      }],
    },
    options: { ...CHART_DEFAULTS },
  });
}

function renderPoolTrend() {
  destroyChart('pool-trend');
  const trendline = data.analytics?.trendline || [];
  if (trendline.length < 2) return;

  const ctx = document.getElementById('chart-pool-trend');
  if (!ctx) return;
  chartInstances['pool-trend'] = new Chart(ctx, {
    type: 'line',
    data: {
      labels: trendline.map(t => new Date(t.timestamp).toLocaleDateString()),
      datasets: [{
        label: 'Tracked Wallets',
        data: trendline.map(t => t.trackedWallets || 0),
        borderColor: '#6c5ce7',
        backgroundColor: 'rgba(108,92,231,0.1)',
        fill: true,
        tension: 0.3,
      }],
    },
    options: { ...CHART_DEFAULTS },
  });
}

/* ============================================================================
   Tab 2: Convergence
   ============================================================================ */

function renderConvergence() {
  if (!data.analytics) return;
  const consensus = data.analytics.consensus || [];

  const consensusCount = consensus.filter(c => (c.walletCount || 0) >= 8).length;
  const clusterCount = consensus.filter(c => (c.walletCount || 0) >= 3 && (c.walletCount || 0) < 8).length;
  const emergingCount = consensus.filter(c => (c.walletCount || 0) === 2).length;

  document.getElementById('cv-total').textContent = consensus.length;
  document.getElementById('cv-consensus').textContent = consensusCount;
  document.getElementById('cv-cluster').textContent = clusterCount;
  document.getElementById('cv-emerging').textContent = emergingCount;

  const filtered = cvMinFilter > 0
    ? consensus.filter(c => (c.walletCount || 0) >= cvMinFilter)
    : consensus;

  const cvData = filtered.map(c => ({
    title: c.marketTitle || 'Unknown',
    slug: c.slug || '',
    eventSlug: c.eventSlug || '',
    direction: c.direction || 'Unknown',
    walletCount: c.walletCount || 0,
    avgScore: c.avgScore || 0,
    totalBuySize: c.conviction || c.totalBuySize || 0,
    avgEntryPrice: c.avgEntryPrice || 0,
    convergenceSpanHours: c.convergenceSpanHours || 0,
    strength: getConvergenceStrength(c.walletCount || 0),
    wallets: c.wallets || [],
    consensusStrength: c.consensusStrength || 0,
  }));

  createSortableTable('convergence-table', [
    { field: 'title', render: (v, row) => `<a href="${polymarketUrl(row.slug, row.eventSlug)}" target="_blank" style="color: var(--accent-light);">${truncate(v, 50)}</a>` },
    { field: 'direction', render: v => `<span class="badge badge-high">${v}</span>` },
    { field: 'walletCount', render: v => {
      const cls = v >= 8 ? 'badge-high' : v >= 3 ? 'badge-mid' : 'badge-low';
      return `<span class="badge ${cls}">${v}</span>`;
    }},
    { field: 'avgScore', render: v => `<span class="badge ${scoreClass(v)}">${v.toFixed(1)}</span>` },
    { field: 'totalBuySize', render: v => fmtDollars(v) },
    { field: 'avgEntryPrice', render: v => v > 0 ? (v * 100).toFixed(1) + '\u00A2' : '-' },
    { field: 'convergenceSpanHours', render: v => v > 0 ? v.toFixed(1) + 'h' : '-' },
    { field: 'strength', render: v => {
      const cls = v === 'Consensus' ? 'badge-consensus' : v === 'Cluster' ? 'badge-cluster' : 'badge-solo';
      return `<span class="badge ${cls}">${v.toUpperCase()}</span>`;
    }},
  ], cvData, (row) => showConvergenceDetail(row));
}

function getConvergenceStrength(walletCount) {
  if (walletCount >= 8) return 'Consensus';
  if (walletCount >= 3) return 'Cluster';
  return 'Emerging';
}

function showConvergenceDetail(row) {
  const walletsHtml = (row.wallets || []).map((w, i) =>
    `<div class="detail-list-item">
      <div class="detail-list-item-label">${i + 1}. <span class="address-link" onclick="openPolymarketProfile('${w.address}')">${truncAddr(w.address)}</span></div>
      <div class="detail-list-item-value">Score: ${(w.score || 0).toFixed(1)}</div>
    </div>`
  ).join('');

  const html = `
    <div class="detail-grid">
      <div class="detail-item">
        <div class="detail-item-label">Market</div>
        <div class="detail-item-value" style="font-size: 14px;">${row.title}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Direction</div>
        <div class="detail-item-value">${row.direction}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Wallets</div>
        <div class="detail-item-value">${row.walletCount}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Total Buy Size</div>
        <div class="detail-item-value">${fmtDollars(row.totalBuySize)}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Avg Entry Price</div>
        <div class="detail-item-value">${row.avgEntryPrice > 0 ? (row.avgEntryPrice * 100).toFixed(1) + '\u00A2' : '-'}</div>
      </div>
      <div class="detail-item">
        <div class="detail-item-label">Convergence Span</div>
        <div class="detail-item-value">${row.convergenceSpanHours > 0 ? row.convergenceSpanHours.toFixed(1) + ' hours' : '-'}</div>
      </div>
    </div>
    <div class="detail-section">
      <h3>Converging Wallets</h3>
      <div class="detail-list">${walletsHtml || '<p style="color: var(--text-dim);">No wallet details available</p>'}</div>
    </div>
  `;
  showDetailPanel('convergence', html);
}

/* ============================================================================
   Tab 3: Signals
   ============================================================================ */

function renderSignals() {
  if (!data.analytics) return;
  const signals = data.analytics.signals || {};
  const activeSignals = signals.active || [];
  const history = signals.history || [];
  const stats = signals.stats || {};

  // Metrics
  const el = id => document.getElementById(id);
  el('sig-active').textContent = (stats.activeCount || activeSignals.length || 0).toString();

  const tb = stats.tierBreakdown || {};
  const tt = stats.typeBreakdown || {};
  el('sig-tiers').innerHTML = `${tb.elite || 0} Elite / ${tb.pro || 0} Pro / ${tb.starter || 0} Starter`;

  const hr = stats.hitRate || 0;
  el('sig-hitrate').innerHTML = hr > 0 ? `<span style="color: ${hr >= 50 ? 'var(--green)' : 'var(--red)'}">${hr}%</span>` : '-';
  el('sig-record').textContent = stats.totalResolved > 0 ? `${stats.wins || 0}W / ${stats.losses || 0}L` : 'No resolved signals yet';

  el('sig-confidence').textContent = stats.avgConfidence > 0 ? stats.avgConfidence.toFixed(1) : '-';
  const parts = [];
  if (stats.openedThisScan) parts.push(`+${stats.openedThisScan} new`);
  if (stats.closedThisScan) parts.push(`-${stats.closedThisScan} closed`);
  el('sig-activity').textContent = parts.length > 0 ? parts.join(', ') + ' this scan' : '';

  const pnl = stats.totalPnl || 0;
  el('sig-pnl').innerHTML = pnl !== 0 ? `<span style="color: ${pnl >= 0 ? 'var(--green)' : 'var(--red)'}">${fmtDollars(pnl)}</span>` : '-';

  // Filter active signals
  let filtered = [...activeSignals];
  if (sigFilterTier && sigFilterTier !== 'all') {
    filtered = filtered.filter(s => s.tier === sigFilterTier);
  }
  if (sigFilterType) {
    filtered = filtered.filter(s => s.signalType === sigFilterType);
  }

  const sigData = filtered.map(s => ({
    marketTitle: s.marketTitle || 'Unknown',
    slug: s.slug || '',
    eventSlug: s.eventSlug || '',
    signalType: s.signalType || 'consensus',
    tier: s.tier || 'starter',
    direction: s.direction || 'mixed',
    walletCount: s.walletCount || 0,
    openMarketPrice: s.openMarketPrice || 0,
    currentMarketPrice: s.currentMarketPrice || 0,
    confidence: s.confidence || 0,
    avgScore: s.avgScore || 0,
    scansActive: s.scansActive || 0,
  }));

  createSortableTable('active-signals-table', [
    { field: 'marketTitle', render: (v, row) => `<a href="${polymarketUrl(row.slug, row.eventSlug)}" target="_blank" style="color: var(--accent-light);">${truncate(v, 45)}</a>` },
    { field: 'signalType', render: v => {
      const cls = v === 'solo' ? 'badge-solo' : v === 'cluster' ? 'badge-cluster' : 'badge-consensus';
      return `<span class="badge ${cls}">${v.toUpperCase()}</span>`;
    }},
    { field: 'tier', render: v => `<span class="tier-badge tier-${v}">${v.toUpperCase()}</span>` },
    { field: 'direction', render: v => `<span class="badge badge-high">${v}</span>` },
    { field: 'walletCount', render: v => String(v) },
    { field: 'openMarketPrice', render: v => v > 0 ? (v * 100).toFixed(1) + '\u00A2' : '-' },
    { field: 'currentMarketPrice', render: v => v > 0 ? (v * 100).toFixed(1) + '\u00A2' : '-' },
    { field: 'confidence', render: v => `<span class="badge ${v >= 70 ? 'badge-high' : v >= 40 ? 'badge-mid' : 'badge-low'}">${v.toFixed(1)}</span>` },
    { field: 'avgScore', render: v => `<span class="badge ${scoreClass(v)}">${v.toFixed(1)}</span>` },
    { field: 'scansActive', render: v => String(v) },
  ], sigData);

  // History table — shows all closed signals, sorted by closedAt (most
  // recent resolution first). Sorting explicitly on closedAt is more
  // reliable than trusting array order — signals can close out-of-order
  // (e.g. a repair pass closes a batch of old stragglers).
  const histData = history
    .slice()
    .sort((a, b) => (b.closedAt || 0) - (a.closedAt || 0))
    .map(s => ({
      marketTitle: s.marketTitle || 'Unknown',
      slug: s.slug || '',
      eventSlug: s.eventSlug || '',
      outcome: s.outcome || 'unknown',
      direction: s.direction || 'mixed',
      openMarketPrice: s.openMarketPrice || 0,
      peakConfidence: s.peakConfidence || 0,
      peakWallets: s.peakWallets || 0,
      signalReturn: s.signalReturn || 0,
      closeReason: s.closeReason || '-',
      closedAt: s.closedAt || 0,
      closedScan: s.closedScan || 0,
    }));

  createSortableTable('signal-history-table', [
    { field: 'closedAt', label: 'Closed', render: v => {
      if (!v) return '<span style="opacity:0.4">-</span>';
      const secs = Math.floor(Date.now() / 1000) - v;
      let rel;
      if (secs < 60)            rel = `${secs}s ago`;
      else if (secs < 3600)     rel = `${Math.floor(secs / 60)}m ago`;
      else if (secs < 86400)    rel = `${Math.floor(secs / 3600)}h ago`;
      else if (secs < 7 * 86400) rel = `${Math.floor(secs / 86400)}d ago`;
      else                       rel = new Date(v * 1000).toISOString().slice(0, 10);
      const abs = new Date(v * 1000).toISOString().replace('T', ' ').slice(0, 16) + 'Z';
      return `<span title="${abs}">${rel}</span>`;
    }},
    { field: 'marketTitle', render: (v, row) => `<a href="${polymarketUrl(row.slug, row.eventSlug)}" target="_blank" style="color: var(--accent-light);">${truncate(v, 45)}</a>` },
    { field: 'outcome', render: (v, row) => {
      if (v === 'win') return `<span class="badge badge-high">WIN</span>`;
      if (v === 'loss') return `<span class="badge badge-low">LOSS</span>`;
      // For non-resolved closures, show the reason (stale, majority exit, expired)
      const reason = (row.closeReason || 'closed').replace(/_/g, ' ');
      return `<span class="badge badge-mid">${reason.toUpperCase()}</span>`;
    }},
    { field: 'direction', render: v => v },
    { field: 'openMarketPrice', render: v => v > 0 ? (v * 100).toFixed(1) + '\u00A2' : '-' },
    { field: 'peakConfidence', render: v => v ? v.toFixed(1) : '-' },
    { field: 'peakWallets', render: v => String(v || '-') },
    { field: 'signalReturn', render: v => {
      if (v === undefined || v === null) return '-';
      const pct = typeof v === 'number' ? (v).toFixed(1) : '0.0';
      return `<span class="${v >= 0 ? 'badge-positive' : 'badge-negative'}">${v >= 0 ? '+' : ''}${pct}%</span>`;
    }},
    { field: 'closeReason', render: v => `<span style="color: var(--text-dim);">${(v || '-').replace(/_/g, ' ')}</span>` },
    { field: 'closedScan', label: 'Scan #', render: v => v ? `#${v}` : '-' },
  ], histData);
}

/* ============================================================================
   Tab 4: Paper Trader
   ============================================================================ */

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
  const deployed = (portfolio.openTrades || []).reduce((s, t) => s + (t.tradeSize || 0), 0);
  const totalTrades = (stats.wins || 0) + (stats.losses || 0);
  const winRate = totalTrades > 0 ? ((stats.wins / totalTrades) * 100).toFixed(1) : '-';
  const avgTrade = totalTrades > 0 ? ((stats.totalPnl || 0) / totalTrades).toFixed(2) : '-';

  const el = id => document.getElementById(id);
  el('paper-equity').textContent = fmtDollars(equity);
  el('paper-return').textContent = `${totalReturn >= 0 ? '+' : ''}${totalReturn}%`;
  el('paper-return').style.color = totalReturn >= 0 ? 'var(--green)' : 'var(--red)';
  el('paper-balance').textContent = fmtDollars(portfolio.balance);
  el('paper-deployed').textContent = `$${deployed.toFixed(0)} deployed`;
  el('paper-winrate').textContent = winRate === '-' ? '-' : winRate + '%';
  el('paper-record').textContent = `${stats.wins || 0}W / ${stats.losses || 0}L`;
  el('paper-pnl').textContent = fmtDollars(stats.totalPnl || 0);
  el('paper-pnl').className = `metric-value ${(stats.totalPnl || 0) >= 0 ? '' : 'text-negative'}`;

  const streak = stats.currentStreak || 0;
  el('paper-streaks').textContent = streak > 0 ? `${streak}W streak` : streak < 0 ? `${Math.abs(streak)}L streak` : '-';
  el('paper-drawdown').textContent = (stats.maxDrawdown || 0).toFixed(1) + '%';
  el('paper-peak').textContent = `Peak: ${fmtDollars(stats.peakEquity || startBal)}`;
  el('paper-avg-trade').textContent = avgTrade === '-' ? '-' : fmtDollars(+avgTrade);
  el('paper-best-worst').textContent = totalTrades > 0
    ? `Best: +$${(stats.biggestWin || 0).toFixed(0)} / Worst: $${(stats.biggestLoss || 0).toFixed(0)}`
    : '-';

  // Equity curve
  renderEquityCurve(portfolio.equityCurve || []);

  // Open trades
  const openTrades = portfolio.openTrades || [];
  el('paper-open-count').textContent = `(${openTrades.length})`;

  const scanCount = data.analytics?.scanCount || 0;
  createSortableTable('paper-open-table', [
    { field: 'marketTitle', render: v => truncate(v, 45) },
    { field: 'signalType', render: v => {
      const cls = v === 'solo' ? 'badge-solo' : v === 'cluster' ? 'badge-cluster' : 'badge-consensus';
      return `<span class="badge ${cls}">${(v || 'consensus').toUpperCase()}</span>`;
    }},
    { field: 'tier', render: v => `<span class="tier-badge tier-${v || 'starter'}">${(v || 'starter').toUpperCase()}</span>` },
    { field: 'direction', render: v => v || '-' },
    { field: 'confidence', render: v => (v || 0).toFixed(1) },
    { field: 'tradeSize', render: v => fmtDollars(v || 0) },
    { field: 'openedScan', render: v => String(scanCount - (v || 0)) },
  ], openTrades.map(t => ({ ...t })));

  // Closed trades
  const closedTrades = portfolio.closedTrades || [];
  el('paper-closed-count').textContent = `(${closedTrades.length})`;

  createSortableTable('paper-closed-table', [
    { field: 'marketTitle', render: v => truncate(v, 45) },
    { field: 'signalType', render: v => {
      const cls = v === 'solo' ? 'badge-solo' : v === 'cluster' ? 'badge-cluster' : 'badge-consensus';
      return `<span class="badge ${cls}">${(v || 'consensus').toUpperCase()}</span>`;
    }},
    { field: 'tier', render: v => `<span class="tier-badge tier-${v || 'starter'}">${(v || 'starter').toUpperCase()}</span>` },
    { field: 'outcome', render: (v, row) => {
      if (v === 'win') return `<span class="badge badge-high">WIN</span>`;
      if (v === 'loss') return `<span class="badge badge-low">LOSS</span>`;
      const reason = (row.closeReason || v || 'pending').replace(/_/g, ' ');
      return `<span class="badge badge-mid">${reason.toUpperCase()}</span>`;
    }},
    { field: 'pnl', render: v => `<span class="${pnlClass(v || 0)}">${fmtDollars(v || 0)}</span>` },
    { field: 'returnPct', render: v => {
      const val = v || 0;
      return `<span class="${val >= 0 ? 'badge-positive' : 'badge-negative'}">${val >= 0 ? '+' : ''}${val.toFixed(1)}%</span>`;
    }},
    { field: 'closeReason', render: v => `<span style="color: var(--text-dim);">${v || '-'}</span>` },
  ], closedTrades.slice(0, 100).map(t => ({ ...t })));

  // Tier comparison
  renderTierComparison(pt);
}

function renderEquityCurve(curve) {
  destroyChart('equity-curve');
  if (!curve || curve.length < 2) return;
  const ctx = document.getElementById('chart-equity-curve');
  if (!ctx) return;

  chartInstances['equity-curve'] = new Chart(ctx, {
    type: 'line',
    data: {
      labels: curve.map((_, i) => `Scan ${i + 1}`),
      datasets: [{
        label: 'Equity',
        data: curve.map(p => p.equity || p),
        borderColor: '#6c5ce7',
        backgroundColor: 'rgba(108,92,231,0.1)',
        fill: true,
        tension: 0.3,
        pointRadius: 0,
      }],
    },
    options: {
      ...CHART_DEFAULTS,
      plugins: {
        ...CHART_DEFAULTS.plugins,
        legend: { display: false },
      },
    },
  });
}

function renderTierComparison(pt) {
  const tbody = document.getElementById('paper-comparison-tbody');
  if (!tbody) return;

  const tiers = ['combined', 'elite', 'pro', 'starter'];
  const rows = tiers.map(name => {
    const p = pt[name];
    if (!p) return null;
    const s = p.stats || {};
    const eq = p.equity || 0;
    const start = p.startingBalance || 10000;
    const ret = ((eq / start - 1) * 100).toFixed(2);
    const total = (s.wins || 0) + (s.losses || 0);
    const wr = total > 0 ? ((s.wins / total) * 100).toFixed(1) + '%' : '-';
    return `<tr>
      <td style="font-weight: 600;">${name.charAt(0).toUpperCase() + name.slice(1)}</td>
      <td>${fmtDollars(eq)}</td>
      <td><span class="${+ret >= 0 ? 'badge-positive' : 'badge-negative'}">${ret >= 0 ? '+' : ''}${ret}%</span></td>
      <td>${total}</td>
      <td>${wr}</td>
      <td><span class="${pnlClass(s.totalPnl || 0)}">${fmtDollars(s.totalPnl || 0)}</span></td>
      <td>${(s.maxDrawdown || 0).toFixed(1)}%</td>
    </tr>`;
  }).filter(Boolean);

  tbody.innerHTML = rows.length > 0 ? rows.join('') : '<tr><td colspan="7" class="empty-state">No tier data</td></tr>';
}

/* ============================================================================
   Tab Navigation & Init
   ============================================================================ */

function switchTab(tabName) {
  currentTab = tabName;
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tabName));
  document.querySelectorAll('.tab-content').forEach(tc => tc.classList.toggle('active', tc.id === 'tab-' + tabName));
  renderCurrentTab();
}

function renderCurrentTab() {
  switch (currentTab) {
    case 'wallets': renderWalletPool(); break;
    case 'convergence': renderConvergence(); break;
    case 'signals': renderSignals(); break;
    case 'paper': renderPaperTrader(); break;
  }
}

async function init() {
  // Tab navigation
  document.querySelectorAll('.tab').forEach(t => {
    t.addEventListener('click', () => switchTab(t.dataset.tab));
  });

  // Convergence filters
  document.querySelectorAll('.cv-filter').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.cv-filter').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      cvMinFilter = parseInt(btn.dataset.min) || 0;
      renderConvergence();
    });
  });

  // Signal filters
  document.querySelectorAll('.sig-filter').forEach(btn => {
    btn.addEventListener('click', () => {
      if (btn.dataset.tier) {
        document.querySelectorAll('.sig-filter[data-tier]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        sigFilterTier = btn.dataset.tier;
        sigFilterType = null;
        document.querySelectorAll('.sig-filter[data-type]').forEach(b => b.classList.remove('active'));
      } else if (btn.dataset.type) {
        document.querySelectorAll('.sig-filter[data-type]').forEach(b => b.classList.remove('active'));
        const wasActive = sigFilterType === btn.dataset.type;
        sigFilterType = wasActive ? null : btn.dataset.type;
        if (!wasActive) btn.classList.add('active');
      }
      renderSignals();
    });
  });

  // Paper portfolio filters
  document.querySelectorAll('.paper-filter').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.paper-filter').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      currentPaperPortfolio = btn.dataset.portfolio;
      renderPaperTrader();
    });
  });

  // Load data
  data = await loadData();
  updateStatusBar();
  renderCurrentTab();
}

document.addEventListener('DOMContentLoaded', init);
