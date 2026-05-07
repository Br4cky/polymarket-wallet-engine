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

// V2 scoring went live 2026-04-30 ~13:00 UTC (commit 34b94be). Signals
// with openedAt at or after this timestamp were emitted under the new
// scoring formula and are flagged in the UI with a V2 badge so old +
// new performance can be visually separated.
const V2_CUTOVER_MS = Date.parse('2026-04-30T13:00:00Z');

function isV2Signal(s) {
  const opened = toMs(s && (s.openedAt || s.signalTime || s.createdAt));
  return opened > 0 && opened >= V2_CUTOVER_MS;
}

// Normalise a timestamp value to milliseconds. Handles ISO strings,
// numeric seconds (pre-2001 era values), and numeric milliseconds.
// Used by the signals renderer (active + history) to make sort/format
// agree across legacy entries with mixed timestamp shapes.
function toMs(t) {
  if (!t) return 0;
  if (typeof t === 'string') {
    const p = Date.parse(t);
    return isFinite(p) ? p : 0;
  }
  if (typeof t !== 'number' || !isFinite(t)) return 0;
  return t > 1e11 ? t : t * 1000;
}

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
    const [analytics, handpicked] = await Promise.all([
      fetchGzJSON(DATA_BASE + 'analytics.json.gz'),
      fetchGzJSON(DATA_BASE + 'handpicked-wallets.json.gz').catch(() => null),
    ]);
    return { analytics, handpicked };
  } catch (error) {
    console.error('Data load error:', error);
    return { analytics: null, handpicked: null };
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

  // Pool total PnL — sum of directionalPnl across the pool (trade-only,
  // MERGE-excluded, matches Polymarket profile). Wallets predating the
  // directionalPnl field fall back to totalPnl; the per-wallet display
  // tags those rows with a tooltip until they're rescored.
  const totalPnl = lb.reduce((s, w) => {
    const dp = w.stats?.directionalPnl;
    return s + (typeof dp === 'number' ? dp : (w.stats?.totalPnl || 0));
  }, 0);
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
    .reduce((s, w) => s + (w.stats?.decidedCapital || w.stats?.singleSideCapital || 0), 0);
  const covEl = document.getElementById('wp-scoring-coverage');
  if (covEl) covEl.textContent = `${scoredCount}/${lb.length} (${scoringCoverage.toFixed(0)}%)`;
  const mpEl = document.getElementById('wp-mean-pickers');
  if (mpEl) mpEl.textContent = `${meanPickerCount} wallets / ${fmtDollars(meanPickerCap)}`;
  const evEl = document.getElementById('wp-would-evict');
  if (evEl) evEl.textContent = `${wouldEvictCount} flagged`;

  // Four PnL numbers with different meanings:
  //   directionalPnl — Trade PnL with MERGE-derived synthetic SELLs
  //                  excluded. Matches what the wallet's Polymarket
  //                  profile shows. The right number for "did following
  //                  this wallet's BUYs make money?" — MERGE / rebate
  //                  income belongs to the wallet, not to a follower.
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
    // Directional metrics: the canonical follower-replicable PnL/ROI/capital.
    // Matches Polymarket profile. Falls back to legacy fields for wallets
    // whose stats predate the directionalPnl/directionalROI fields.
    directionalPnl: typeof w.stats?.directionalPnl === 'number'
      ? w.stats.directionalPnl
      : null,
    directionalROI: typeof w.stats?.directionalROI === 'number'
      ? w.stats.directionalROI
      : null,
    directionalCapital: typeof w.stats?.directionalCapital === 'number'
      ? w.stats.directionalCapital
      : null,
    mergeUsdcTotal: w.stats?.mergeUsdcTotal || 0,
    // Robustness / lottery-winner metrics — surface concentration so we
    // can spot wallets whose PnL is one-or-three-trade luck. Higher is
    // worse. Gate at 0.85 evicts these but useful to see all values.
    top3ConcentrationShare: typeof w.stats?.top3ConcentrationShare === 'number'
      ? w.stats.top3ConcentrationShare : null,
    pnlExTop3: typeof w.stats?.pnlExTop3 === 'number' ? w.stats.pnlExTop3 : null,
    medianTradePnL: typeof w.stats?.medianTradePnL === 'number' ? w.stats.medianTradePnL : null,
    // Legacy fallback fields — only used when directional* is null.
    onChainPnl: w.stats?.totalPnl || 0,
    samplePnl: w.stats?.samplePnl || 0,
    effectivePnl: w.stats?.effectivePnl
      || Math.max(w.stats?.totalPnl || 0, w.stats?.samplePnl || 0),
    decidedROI: w.stats?.decidedROI ?? null,
    decidedCapital: w.stats?.decidedCapital ?? null,
    // Fallback fields shown when decidedROI/decidedCapital are null. ~30% of
    // pool wallets lack decided metrics because their positions are on
    // negRisk markets (Gamma can't resolve) or their cursor scan didn't
    // capture position data. Scoring already uses these via 0.5× haircut.
    singleSideROI: w.stats?.singleSideROI ?? null,
    singleSideCapital: w.stats?.singleSideCapital ?? null,
    metricSource: w.scoreComponents?.metricSource ?? null,
    isMeanPicker: w.stats?.isMeanPickerShape === true,
    wouldEvict: w.wouldEvict ? w.wouldEvict.reason : null,
    statsSpanDays: w.stats?.statsSpanDays || 0,
    tradesTruncated: w.stats?.tradesTruncated === true,
    resolved: w.stats?.resolved || 0,
    tradesPerWeek: w.stats?.positionsPerWeek || 0,
    consistency: w.stats?.weeklyConsistency || 0,
  }));

  // Wallet table — single PnL column (directionalPnl) and single ROI
  // column (directionalROI). Both match what the wallet's Polymarket
  // profile shows. The legacy fields (singleSideROI, decidedROI,
  // effectivePnl, samplePnl, onChainPnl) are kept in the leaderboard
  // payload for diagnostic transparency but are no longer surfaced as
  // their own columns — they appear only in tooltips on the canonical
  // numbers when there's a meaningful divergence worth flagging.
  createSortableTable('wallet-table', [
    { field: 'rank', render: v => String(v) },
    { field: 'score', label: 'Score', render: (v, row) => {
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
    { field: 'directionalROI', label: 'ROI', render: (v, row) => {
      // Single ROI metric. directionalROI = directionalPnl /
      // directionalCapital, both over resolved markets, MERGE income
      // excluded. Matches the wallet's Polymarket profile lifetime ROI.
      // Falls back to singleSideROI for stats predating directionalROI.
      const useV = (typeof v === 'number') ? v : (row && row.singleSideROI);
      if (typeof useV !== 'number') return '<span style="opacity:0.35">—</span>';
      const cls = roiClass(useV);
      const pct = (useV * 100).toFixed(1) + '%';
      const fallbackNote = (typeof v !== 'number') ? ' (fallback: singleSideROI — wallet not yet rescored under directionalROI)' : '';
      return `<span class="badge ${cls}" title="Lifetime ROI on resolved positions, MERGE/rebate income excluded — matches Polymarket profile.${fallbackNote}">${pct}</span>`;
    }},
    { field: 'directionalCapital', label: 'Capital', render: (v, row) => {
      const useV = (typeof v === 'number') ? v : (row && row.singleSideCapital);
      if (typeof useV !== 'number') return '<span style="opacity:0.35">—</span>';
      return `<span title="Capital deployed on resolved positions">${fmtDollars(useV)}</span>`;
    }},
    { field: 'winRate', render: v => ((v || 0) * 100).toFixed(1) + '%' },
    { field: 'directionalPnl', label: 'PnL', render: (v, row) => {
      // Single PnL metric. Trade PnL with MERGE-derived income excluded.
      // Matches the wallet's Polymarket profile lifetime PnL.
      // Falls back to totalPnl for stats predating directionalPnl.
      const useV = (typeof v === 'number') ? v : (row && row.onChainPnl);
      if (typeof useV !== 'number') return '<span style="opacity:0.35">—</span>';
      const fallbackNote = (typeof v !== 'number') ? ' (fallback: totalPnl — wallet not yet rescored)' : '';
      const mergeNote = (row && typeof row.mergeUsdcTotal === 'number' && row.mergeUsdcTotal > 100)
        ? ` MERGE income excluded: $${row.mergeUsdcTotal.toFixed(0)}.`
        : '';
      return `<span class="${pnlClass(useV)}" title="Lifetime trade PnL — matches Polymarket profile.${fallbackNote}${mergeNote}">${fmtDollars(useV)}</span>`;
    }},
    { field: 'top3ConcentrationShare', label: 'Top3%', render: (v, row) => {
      // Concentration: what fraction of all win-PnL came from this
      // wallet's top 3 outlier wins. > 85% triggers lottery-winner
      // eviction. Color-coded: red = lottery-driven, green = consistent.
      if (typeof v !== 'number') return '<span style="opacity:0.35" title="Not yet rescored under analyzer with concentration metrics">—</span>';
      const pct = Math.round(v * 100);
      let style = '';
      if (v > 0.85) style = 'class="text-negative"';
      else if (v > 0.70) style = 'style="color:#fdcb6e"';
      else if (v <= 0.50) style = 'class="text-positive"';
      const pnlEx3 = row && typeof row.pnlExTop3 === 'number' ? `$${row.pnlExTop3.toFixed(0)}` : '—';
      const median = row && typeof row.medianTradePnL === 'number' ? `$${row.medianTradePnL.toFixed(2)}` : '—';
      return `<span ${style} title="Top 3 wins' share of all positive PnL. >85% = lottery-driven (auto-evicted). PnL excluding top 3 wins: ${pnlEx3}. Median per-trade PnL: ${median}.">${pct}%</span>`;
    }},
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
      // Labels match bucketEdges = [5, 10, 20, 30]:
      //   <5 | 5-10 | 10-20 | 20-30 | 30+
      // Practical max score is ~50 (top decile ~25+); the old 0-100 labels
      // were a leftover from when scoring was on the legacy 0-100 scale.
      labels: ['0-5', '5-10', '10-20', '20-30', '30+'],
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
    openedAtMs: toMs(s.openedAt),
    isV2: isV2Signal(s),
  }))
  // Default sort: newest first. createSortableTable doesn't apply any
  // sort until a column header is clicked, so the natural data order
  // (oldest signals at the top of signals.active) was rendering first.
  .sort((a, b) => b.openedAtMs - a.openedAtMs);

  createSortableTable('active-signals-table', [
    { field: 'marketTitle', render: (v, row) => {
      const v2 = row.isV2 ? '<span class="badge badge-v2" title="Emitted under V2 scoring (post 2026-04-30 13:00 UTC). Useful for separating old vs new signals when measuring whether the redesign worked.">V2</span> ' : '';
      return `${v2}<a href="${polymarketUrl(row.slug, row.eventSlug)}" target="_blank" style="color: var(--accent-light);">${truncate(v, 45)}</a>`;
    } },
    { field: 'signalType', render: v => {
      // Map each emit-path to its badge color class. Defaults to consensus
      // for any future type so we always render *something* sensible.
      const cls = v === 'solo' ? 'badge-solo'
        : v === 'cluster' ? 'badge-cluster'
        : v === 'micro-cluster' ? 'badge-micro-cluster'
        : v === 'mid-favorite' ? 'badge-mid-favorite'
        : v === 'handpicked' ? 'badge-handpicked'
        : 'badge-consensus';
      return `<span class="badge ${cls}">${(v || 'consensus').toUpperCase()}</span>`;
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
  //
  // closedAt is stored as unix SECONDS for new signals but some legacy
  // entries have MILLISECONDS. toMs (module scope) normalises both.
  const histData = history
    .slice()
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
      closedAt: toMs(s.closedAt),   // always ms after this
      closedScan: s.closedScan || 0,
      isV2: isV2Signal(s),
    }))
    .sort((a, b) => b.closedAt - a.closedAt);
  // Tell the sortable-table helper this is the canonical sort so the
  // header arrow renders correctly + clicking 'closedAt' header again
  // toggles instead of re-applying the same sort silently.
  if (typeof sortState !== 'undefined') {
    sortState['signal-history-table'] = { field: 'closedAt', dir: 'desc' };
  }

  createSortableTable('signal-history-table', [
    { field: 'closedAt', label: 'Closed', render: v => {
      if (!v) return '<span style="opacity:0.4">-</span>';
      const d = new Date(v);
      if (isNaN(d.getTime())) return '<span style="opacity:0.4">-</span>';
      const secsAgo = Math.max(0, Math.floor((Date.now() - v) / 1000));
      let rel;
      if (secsAgo < 60)            rel = `${secsAgo}s ago`;
      else if (secsAgo < 3600)     rel = `${Math.floor(secsAgo / 60)}m ago`;
      else if (secsAgo < 86400)    rel = `${Math.floor(secsAgo / 3600)}h ago`;
      else if (secsAgo < 7 * 86400) rel = `${Math.floor(secsAgo / 86400)}d ago`;
      else                          rel = d.toISOString().slice(0, 10);
      const abs = d.toISOString().replace('T', ' ').slice(0, 16) + 'Z';
      return `<span title="${abs}">${rel}</span>`;
    }},
    { field: 'marketTitle', render: (v, row) => {
      const v2 = row.isV2 ? '<span class="badge badge-v2" title="Emitted under V2 scoring (post 2026-04-30 13:00 UTC)">V2</span> ' : '';
      return `${v2}<a href="${polymarketUrl(row.slug, row.eventSlug)}" target="_blank" style="color: var(--accent-light);">${truncate(v, 45)}</a>`;
    } },
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
      const cls = v === 'solo' ? 'badge-solo'
        : v === 'cluster' ? 'badge-cluster'
        : v === 'micro-cluster' ? 'badge-micro-cluster'
        : v === 'mid-favorite' ? 'badge-mid-favorite'
        : v === 'handpicked' ? 'badge-handpicked'
        : 'badge-consensus';
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
      const cls = v === 'solo' ? 'badge-solo'
        : v === 'cluster' ? 'badge-cluster'
        : v === 'micro-cluster' ? 'badge-micro-cluster'
        : v === 'mid-favorite' ? 'badge-mid-favorite'
        : v === 'handpicked' ? 'badge-handpicked'
        : 'badge-consensus';
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
    case 'handpicked': renderHandpicked(); break;
  }
}

function renderHandpicked() {
  const el = id => document.getElementById(id);
  // Handpicked tab — manually-curated wallets + the signal track they
  // produce. Decoupled from the automated scanner. As wallets are added
  // (via scripts/add-handpicked.mjs) and signals resolve, this tab
  // accumulates clean ground-truth attribution data.
  const store = data.handpicked;
  const allSignals = data.analytics?.signals || {};
  const active = Object.values(allSignals.active || {});
  const history = Array.isArray(allSignals.history) ? allSignals.history : Object.values(allSignals.history || {});

  const wallets = store?.wallets || [];

  // Per-wallet: accumulate signals from this wallet
  const sigByWallet = {};
  function key(addr) { return (addr || '').toLowerCase(); }
  function attachSig(s, list) {
    if (s.signalType !== 'handpicked') return;
    const w = key(s.soloWallet);
    if (!w) return;
    if (!sigByWallet[w]) sigByWallet[w] = { active: 0, wins: 0, losses: 0, pending: 0, totalReturn: 0, returnCount: 0 };
    list.push(s);
    if (s.outcome === 'win') { sigByWallet[w].wins++; if (typeof s.signalReturn === 'number') { sigByWallet[w].totalReturn += s.signalReturn; sigByWallet[w].returnCount++; } }
    else if (s.outcome === 'loss') { sigByWallet[w].losses++; if (typeof s.signalReturn === 'number') { sigByWallet[w].totalReturn += s.signalReturn; sigByWallet[w].returnCount++; } }
    else if (s.outcome === 'win' || s.outcome === 'loss') {} // unreachable
    else sigByWallet[w].pending++;
  }
  const allHpSignals = [];
  for (const s of active) attachSig(s, allHpSignals);
  for (const s of history) attachSig(s, allHpSignals);
  for (const w of wallets) {
    const k = key(w.address);
    if (!sigByWallet[k]) sigByWallet[k] = { active: 0, wins: 0, losses: 0, pending: 0, totalReturn: 0, returnCount: 0 };
    sigByWallet[k].active = active.filter(s => s.signalType === 'handpicked' && key(s.soloWallet) === k).length;
  }

  // Aggregate stats
  const totalSigs = allHpSignals.length;
  const totalActive = active.filter(s => s.signalType === 'handpicked').length;
  const totalWins = allHpSignals.filter(s => s.outcome === 'win').length;
  const totalLosses = allHpSignals.filter(s => s.outcome === 'loss').length;
  const totalResolved = totalWins + totalLosses;
  const wr = totalResolved > 0 ? (totalWins / totalResolved * 100) : 0;
  const returns = allHpSignals.map(s => s.signalReturn).filter(r => typeof r === 'number');
  const avgRet = returns.length > 0 ? returns.reduce((a, b) => a + b, 0) / returns.length : 0;

  // Top metric tiles
  el('hp-pool-size').textContent = wallets.length.toString();
  el('hp-total-signals').textContent = totalSigs.toString();
  el('hp-active-signals').textContent = totalActive.toString();
  el('hp-resolved').textContent = totalResolved.toString();
  el('hp-wr').textContent = wr.toFixed(1) + '%';
  el('hp-avg-return').textContent = (avgRet >= 0 ? '+' : '') + avgRet.toFixed(1) + '%';

  // Wallet table
  const walletData = wallets.map(w => {
    const k = key(w.address);
    const sigStats = sigByWallet[k] || { active: 0, wins: 0, losses: 0, pending: 0, totalReturn: 0, returnCount: 0 };
    const resolvedSigs = sigStats.wins + sigStats.losses;
    const walletWR = resolvedSigs > 0 ? (sigStats.wins / resolvedSigs * 100) : null;
    const walletAvgRet = sigStats.returnCount > 0 ? (sigStats.totalReturn / sigStats.returnCount) : null;
    return {
      address: w.address,
      addedAt: w.addedAt,
      notes: w.notes || '—',
      // Prefer directionalPnl (matches Polymarket profile, excludes MERGE
       // income). Falls back to totalPnl for stats predating directionalPnl.
       walletPnl: typeof w.stats?.directionalPnl === 'number'
         ? w.stats.directionalPnl
         : (w.stats?.totalPnl ?? null),
      walletWR: w.stats?.winRate ?? null,
      // Wallet ROI: prefer directionalROI (matches Polymarket profile,
      // MERGE income excluded). Falls back to singleSideROI for stats
      // predating directionalROI.
      walletROI: typeof w.stats?.directionalROI === 'number'
        ? w.stats.directionalROI
        : (w.stats?.singleSideROI ?? null),
      walletResolved: w.stats?.resolvedMarkets ?? null,
      sigsActive: sigStats.active,
      sigsResolved: resolvedSigs,
      sigsPending: sigStats.pending - sigStats.active,  // history pending only
      sigWR: walletWR,
      sigAvgRet: walletAvgRet,
    };
  });

  createSortableTable('handpicked-wallet-table', [
    { field: 'address', label: 'Wallet', render: v => `<span class="address-link" onclick="openPolymarketProfile('${v}')">${truncAddr(v)}</span>` },
    { field: 'notes', label: 'Notes', render: v => `<span style="opacity:0.85; font-size:12px;">${v}</span>` },
    { field: 'walletPnl', label: 'Wallet PnL', render: v => v != null ? `<span class="${pnlClass(v)}">${fmtDollars(v)}</span>` : '—' },
    { field: 'walletROI', label: 'Wallet ROI', render: v => v != null ? `<span class="${pnlClass(v)}">${(v * 100).toFixed(0)}%</span>` : '—' },
    { field: 'walletWR', label: 'Wallet WR', render: v => v != null ? `${(v * 100).toFixed(0)}%` : '—' },
    { field: 'walletResolved', label: 'Resolved', render: v => v != null ? String(v) : '—' },
    { field: 'sigsActive', label: 'HP Active', render: v => `<span class="badge badge-handpicked">${v}</span>` },
    { field: 'sigsResolved', label: 'HP Resolved', render: v => String(v) },
    { field: 'sigWR', label: 'HP WR', render: v => v != null ? `${v.toFixed(0)}%` : '—' },
    { field: 'sigAvgRet', label: 'HP AvgRet', render: v => v != null ? `<span class="${pnlClass(v)}">${(v >= 0 ? '+' : '') + v.toFixed(0)}%</span>` : '—' },
  ], walletData);

  // Recent handpicked signals (cap 50, newest first)
  allHpSignals.sort((a, b) => toMs(b.openedAt) - toMs(a.openedAt));
  const sigDisplay = allHpSignals.slice(0, 50).map(s => ({
    marketTitle: s.marketTitle || 'Unknown',
    slug: s.slug || '',
    eventSlug: s.eventSlug || '',
    soloWallet: s.soloWallet,
    openedAt: s.openedAt,
    outcome: s.outcome || 'pending',
    signalReturn: s.signalReturn,
    direction: s.direction || '',
    avgEntryPrice: s.avgEntryPrice || 0,
    currentMarketPrice: s.currentMarketPrice || 0,
    closeReason: s.closeReason || null,
  }));

  createSortableTable('handpicked-signals-table', [
    { field: 'openedAt', label: 'Opened', render: v => {
      if (!v) return '<span style="opacity:0.4">-</span>';
      const ms = toMs(v);
      const secsAgo = Math.max(0, Math.floor((Date.now() - ms) / 1000));
      let rel;
      if (secsAgo < 60) rel = `${secsAgo}s ago`;
      else if (secsAgo < 3600) rel = `${Math.floor(secsAgo / 60)}m ago`;
      else if (secsAgo < 86400) rel = `${Math.floor(secsAgo / 3600)}h ago`;
      else rel = `${Math.floor(secsAgo / 86400)}d ago`;
      return `<span title="${new Date(ms).toISOString()}">${rel}</span>`;
    }},
    { field: 'marketTitle', render: (v, row) => `<a href="${polymarketUrl(row.slug, row.eventSlug)}" target="_blank" style="color:var(--accent-light);">${truncate(v, 50)}</a>` },
    { field: 'soloWallet', label: 'From', render: v => v ? `<span class="address-link" onclick="openPolymarketProfile('${v}')">${truncAddr(v)}</span>` : '—' },
    { field: 'direction', render: v => v || '—' },
    { field: 'avgEntryPrice', label: 'Entry', render: v => v > 0 ? (v * 100).toFixed(0) + '¢' : '—' },
    { field: 'currentMarketPrice', label: 'Now', render: v => v > 0 ? (v * 100).toFixed(0) + '¢' : '—' },
    { field: 'outcome', render: (v, row) => {
      if (v === 'win') return `<span class="badge badge-high">WIN</span>`;
      if (v === 'loss') return `<span class="badge badge-low">LOSS</span>`;
      return `<span style="opacity:0.6">${v}</span>`;
    }},
    { field: 'signalReturn', label: 'Return', render: v => v != null ? `<span class="${pnlClass(v)}">${(v >= 0 ? '+' : '') + v.toFixed(0)}%</span>` : '—' },
  ], sigDisplay);
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

  // Auto-refresh: re-fetch every 5 min and re-render if the analytics
  // timestamp moved. Cron pushes every 2 hours so most ticks no-op,
  // but the 5-min cadence keeps the dashboard within one CDN cache
  // window of fresh data without requiring a manual reload.
  setInterval(async () => {
    try {
      const fresh = await loadData();
      if (!fresh.analytics) return;
      const oldTs = data?.analytics?.timestamp;
      const newTs = fresh.analytics?.timestamp;
      if (oldTs === newTs) return; // nothing changed, skip re-render
      data = fresh;
      updateStatusBar();
      renderCurrentTab();
      console.log('[dashboard] data refreshed', newTs);
    } catch (e) {
      console.warn('[dashboard] refresh failed', e);
    }
  }, 5 * 60 * 1000);
}

document.addEventListener('DOMContentLoaded', init);
