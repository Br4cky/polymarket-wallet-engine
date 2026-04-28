// Wallet quality audit — assess trade-level performance INDEPENDENT of
// how we use the wallet for signals. Answers: are the wallets we've
// admitted actually good Polymarket traders, or is the pool full of
// dormant / low-edge / mm-like trash that shouldn't be here?
//
// Sections:
//   1. Trade-level performance distribution across the pool
//   2. Quality by STYLE (do sniper wallets actually trade well, or is
//      style just a classification artifact?)
//   3. Quality for wallets that DO vs DON'T emit signals (are we hiding
//      good alpha, or are the non-contributors genuinely dormant?)
//   4. Top 20 by trade-level ROI + whether they emit signals
//   5. Bottom-pool junk — wallets admitted with weak trade metrics
//   6. Gate diagnosis — what quality dimensions pass/fail

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const pool = walletsData.pool || walletsData;

function classifyStyle(stats) {
  if (!stats) return 'unknown';
  if ((stats.dualSideRate || 0) > 0.30 || (stats.mmScore || 0) >= 3) return 'mm-like';
  const tt = stats.totalTrades || 0, um = stats.uniqueMarkets || 0;
  const tpm = um > 0 ? tt / um : 0;
  const sellRatio = stats.sellRatio ?? 1;
  const hold = stats.avgHoldTimeHours || 0;
  if (tpm > 8) return 'churner';
  if (tpm >= 3 && sellRatio > 0.30) return 'averager';
  if (tpm <= 2 && hold < 48) return 'sniper';
  if (sellRatio < 0.15) return 'holder';
  return 'mixed';
}

// Build signal-attribution set (who has contributed to any resolved signal)
const attr = new Map();
const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');
for (const sig of resolved) {
  const ws = new Set();
  if (Array.isArray(sig.currentWallets)) sig.currentWallets.forEach(w => w && w.address && ws.add(w.address.toLowerCase()));
  if (sig.soloWallet) ws.add(String(sig.soloWallet).toLowerCase());
  const ret = typeof sig.signalReturn === 'number' ? sig.signalReturn : null;
  for (const a of ws) {
    if (!attr.has(a)) attr.set(a, { signals: 0, wins: 0, totalRet: 0, retN: 0 });
    const r = attr.get(a);
    r.signals++;
    if (sig.outcome === 'win') r.wins++;
    if (ret !== null) { r.totalRet += ret; r.retN++; }
  }
}

// Collect wallet entries
const wallets = [];
for (const [addr, w] of Object.entries(pool)) {
  if (!w || typeof w !== 'object' || w.status === 'removed') continue;
  const s = w.stats || {};
  const a = attr.get(addr.toLowerCase());
  wallets.push({
    addr,
    score: w.score || 0,
    style: classifyStyle(s),
    // trade-level performance
    decidedROI: s.decidedROI,
    decidedCapital: s.decidedCapital,
    decidedPnl: s.decidedPnl,
    totalPnl: s.totalPnl,
    winRate: s.winRate,
    recentWinRate: s.recentWinRate,
    avgTradeRoi: s.avgTradeRoi,
    avgWinRoi: s.avgWinRoi,
    edgeRatio: s.edgeRatio,
    roiEdgeRatio: s.roiEdgeRatio,
    // activity
    totalTrades: s.totalTrades || 0,
    uniqueMarkets: s.uniqueMarkets || 0,
    resolvedMarkets: s.resolvedMarkets || 0,
    tradingSpanDays: s.tradingSpanDays || 0,
    avgHoldTimeHours: s.avgHoldTimeHours || 0,
    recentTradesPerDay: s.recentTradesPerDay || 0,
    // MM signals
    mmScore: s.mmScore || 0,
    dualSideRate: s.dualSideRate || 0,
    sellRatio: s.sellRatio,
    mergeRate: s.mergeRate || 0,
    rebateUsdcTotal: s.rebateUsdcTotal || 0,
    rewardUsdcTotal: s.rewardUsdcTotal || 0,
    // alpha test
    alphaVerdict: s.alphaVerdict,
    // signal side
    emitsSignals: !!a,
    signals: a?.signals || 0,
  });
}

const pct = (n) => (n * 100).toFixed(0) + '%';
const countIn = (arr, pred) => arr.filter(pred).length;

// ═══════════════════════════════════════════════════════════════════
console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Wallet trade-quality audit — are the tracked wallets actually good?');
console.log('═══════════════════════════════════════════════════════════════════');

// ── 1. Trade-level performance distribution ────────────────────────
console.log('\n  ── 1. TRADE-LEVEL PERFORMANCE (all ' + wallets.length + ' active) ──');
console.log('  decidedROI (capital-weighted P&L/Capital on RESOLVED positions):');
const roiBands = [[-5,0],[0,0.10],[0.10,0.25],[0.25,0.50],[0.50,1.0],[1.0,5.0],[5.0,100]];
for (const [lo, hi] of roiBands) {
  const n = countIn(wallets, w => w.decidedROI != null && w.decidedROI >= lo && w.decidedROI < hi);
  const bar = '█'.repeat(Math.round(n / 10));
  console.log('    ' + (lo*100).toFixed(0) + '% to ' + (hi*100).toFixed(0) + '%'.padEnd(6) + ' ' + String(n).padStart(4) + '  ' + bar);
}
const withROI = wallets.filter(w => w.decidedROI != null).sort((a, b) => a.decidedROI - b.decidedROI);
if (withROI.length > 0) {
  const m = withROI[Math.floor(withROI.length / 2)].decidedROI;
  const p25 = withROI[Math.floor(withROI.length * 0.25)].decidedROI;
  const p75 = withROI[Math.floor(withROI.length * 0.75)].decidedROI;
  const neg = countIn(wallets, w => w.decidedROI != null && w.decidedROI < 0);
  console.log('    p25=' + (p25*100).toFixed(0) + '%  median=' + (m*100).toFixed(0) + '%  p75=' + (p75*100).toFixed(0) + '%  negative ROI: ' + neg);
}

console.log('\n  Total P&L (dollar gains across all trades):');
const pnlBands = [[-1e9,-1000],[-1000,0],[0,1000],[1000,10000],[10000,50000],[50000,1e9]];
for (const [lo, hi] of pnlBands) {
  const n = countIn(wallets, w => w.totalPnl != null && w.totalPnl >= lo && w.totalPnl < hi);
  const bar = '█'.repeat(Math.round(n / 10));
  const label = (lo <= -1e8 ? 'more loss' : '$' + (lo/1000).toFixed(0) + 'k') + ' to ' + (hi >= 1e8 ? '$∞' : '$' + (hi/1000).toFixed(0) + 'k');
  console.log('    ' + label.padEnd(16) + ' ' + String(n).padStart(4) + '  ' + bar);
}
const pnlSum = wallets.reduce((a, w) => a + (w.totalPnl || 0), 0);
console.log('    Total aggregate P&L: $' + pnlSum.toLocaleString('en-US', {maximumFractionDigits: 0}));

console.log('\n  Capital committed (decidedCapital):');
const capBands = [[0,1000],[1000,10000],[10000,50000],[50000,250000],[250000,1e9]];
for (const [lo, hi] of capBands) {
  const n = countIn(wallets, w => w.decidedCapital != null && w.decidedCapital >= lo && w.decidedCapital < hi);
  const bar = '█'.repeat(Math.round(n / 10));
  const label = (lo < 1000 ? '<$1k' : '$' + (lo/1000).toFixed(0) + 'k') + ' to ' + (hi >= 1e8 ? '$∞' : '$' + (hi/1000).toFixed(0) + 'k');
  console.log('    ' + label.padEnd(16) + ' ' + String(n).padStart(4) + '  ' + bar);
}
console.log('    Wallets with <$5k decidedCapital (thin sample): ' + countIn(wallets, w => (w.decidedCapital || 0) < 5000));

console.log('\n  Win rate distribution:');
const wrBands = [[0,0.30],[0.30,0.50],[0.50,0.70],[0.70,0.85],[0.85,0.95],[0.95,1.01]];
for (const [lo, hi] of wrBands) {
  const n = countIn(wallets, w => w.winRate != null && w.winRate >= lo && w.winRate < hi);
  const bar = '█'.repeat(Math.round(n / 10));
  console.log('    ' + (lo*100).toFixed(0) + '-' + (hi*100).toFixed(0) + '%'.padEnd(6) + ' ' + String(n).padStart(4) + '  ' + bar);
}

// ── 2. Quality by STYLE ─────────────────────────────────────────────
console.log('\n  ── 2. TRADE QUALITY BY STYLE ──');
console.log('  ' + 'Style'.padEnd(12) + 'Count'.padStart(6) + '  MedROI  MedCap'.padStart(16) + '  MedWR  MedPnL'.padStart(16) + '  %with$10k+');
const byStyle = {};
for (const w of wallets) {
  if (!byStyle[w.style]) byStyle[w.style] = [];
  byStyle[w.style].push(w);
}
for (const [s, arr] of Object.entries(byStyle).sort((a, b) => b[1].length - a[1].length)) {
  const sortedROI = arr.filter(w => w.decidedROI != null).sort((a, b) => a.decidedROI - b.decidedROI);
  const medROI = sortedROI.length > 0 ? sortedROI[Math.floor(sortedROI.length / 2)].decidedROI : null;
  const sortedCap = arr.filter(w => w.decidedCapital != null).sort((a, b) => a.decidedCapital - b.decidedCapital);
  const medCap = sortedCap.length > 0 ? sortedCap[Math.floor(sortedCap.length / 2)].decidedCapital : 0;
  const sortedWR = arr.filter(w => w.winRate != null).sort((a, b) => a.winRate - b.winRate);
  const medWR = sortedWR.length > 0 ? sortedWR[Math.floor(sortedWR.length / 2)].winRate : null;
  const sortedPnL = arr.filter(w => w.totalPnl != null).sort((a, b) => a.totalPnl - b.totalPnl);
  const medPnL = sortedPnL.length > 0 ? sortedPnL[Math.floor(sortedPnL.length / 2)].totalPnl : null;
  const with10k = countIn(arr, w => (w.decidedCapital || 0) >= 10000);
  console.log('  ' + s.padEnd(12) + String(arr.length).padStart(6) +
    (medROI != null ? ('  ' + (medROI * 100).toFixed(0) + '%').padStart(8) : '       —') +
    ('  $' + (medCap / 1000).toFixed(0) + 'k').padStart(8) +
    (medWR != null ? ('  ' + (medWR * 100).toFixed(0) + '%').padStart(8) : '       —') +
    (medPnL != null ? ('  $' + medPnL.toFixed(0).padStart(6)) : '       —') +
    '  ' + pct(with10k / arr.length).padStart(10));
}

// ── 3. Contributors vs non-contributors ─────────────────────────────
console.log('\n  ── 3. SIGNAL-EMITTING vs NOT ──');
const contribs = wallets.filter(w => w.emitsSignals);
const nonContribs = wallets.filter(w => !w.emitsSignals);
console.log('  Emitting signals:        ' + contribs.length);
console.log('  NOT emitting signals:    ' + nonContribs.length);
function med(arr, f) {
  const s = arr.map(f).filter(x => x != null).sort((a, b) => a - b);
  return s.length > 0 ? s[Math.floor(s.length / 2)] : null;
}
const stats = (arr) => ({
  n: arr.length,
  roi: med(arr, w => w.decidedROI),
  cap: med(arr, w => w.decidedCapital),
  pnl: med(arr, w => w.totalPnl),
  wr: med(arr, w => w.winRate),
  score: med(arr, w => w.score),
  hold: med(arr, w => w.avgHoldTimeHours),
});
function describe(label, s) {
  console.log('  ' + label.padEnd(24) + 'medROI=' + (s.roi != null ? (s.roi * 100).toFixed(0) + '%' : '—') +
    '  medCap=$' + (s.cap != null ? (s.cap / 1000).toFixed(0) + 'k' : '—') +
    '  medPnL=$' + (s.pnl != null ? s.pnl.toFixed(0) : '—') +
    '  medWR=' + (s.wr != null ? (s.wr * 100).toFixed(0) + '%' : '—') +
    '  medScore=' + (s.score != null ? s.score.toFixed(1) : '—'));
}
describe('Contributors:', stats(contribs));
describe('Non-contributors:', stats(nonContribs));
console.log('  → If non-contributors have similar trade quality, our gates are filtering them out artificially.');
console.log('  → If non-contributors are clearly worse, the pool is accurately filtered.');

// ── 4. Top 20 by decidedROI × capital ───────────────────────────────
console.log('\n  ── 4. TOP 20 TRADE-LEVEL PERFORMERS (sorted by decidedROI × sqrt(capital)) ──');
const ranked = wallets
  .filter(w => w.decidedROI != null && w.decidedCapital != null && w.decidedCapital >= 5000)
  .map(w => ({ ...w, rank: w.decidedROI * Math.sqrt(w.decidedCapital) }))
  .sort((a, b) => b.rank - a.rank);
console.log('  ' + 'Wallet'.padEnd(14) + 'Style'.padEnd(10) + 'Score'.padStart(6) +
  ' ROI'.padStart(7) + ' Cap'.padStart(9) + ' PnL'.padStart(10) +
  ' WR'.padStart(6) + ' Sigs'.padStart(5) + ' Alpha');
for (const w of ranked.slice(0, 20)) {
  console.log('  ' + w.addr.slice(0, 12).padEnd(14) + w.style.padEnd(10) + w.score.toFixed(1).padStart(6) +
    (' ' + (w.decidedROI * 100).toFixed(0) + '%').padStart(7) +
    (' $' + (w.decidedCapital / 1000).toFixed(0) + 'k').padStart(9) +
    (' $' + (w.totalPnl || 0).toLocaleString('en-US', {maximumFractionDigits: 0})).padStart(10) +
    (' ' + ((w.winRate || 0) * 100).toFixed(0) + '%').padStart(6) +
    (' ' + w.signals).padStart(5) +
    '  ' + (w.alphaVerdict || '—'));
}

// ── 5. Bottom pool — who got admitted that shouldn't be here ────────
console.log('\n  ── 5. QUESTIONABLE ADMITS (low edge, high MM tells, or thin capital) ──');
const flagged = wallets.filter(w =>
  (w.decidedROI != null && w.decidedROI < 0.05) ||  // essentially breakeven
  (w.mmScore >= 2 && w.decidedCapital < 20000) ||    // small MM
  (w.winRate > 0.95 && w.decidedROI < 0.10) ||        // suspiciously high WR on weak ROI
  (w.alphaVerdict === 'fails')
).sort((a, b) => (a.decidedROI || 0) - (b.decidedROI || 0));
console.log('  Wallets flagged: ' + flagged.length + ' (' + pct(flagged.length / wallets.length) + ' of pool)');
console.log('  ' + 'Wallet'.padEnd(14) + 'Style'.padEnd(10) + 'Score'.padStart(6) + ' ROI'.padStart(7) +
  ' Cap'.padStart(9) + ' WR'.padStart(6) + '  mm'.padStart(5) + '  dualRate'.padStart(10) + '  alphaVerdict');
for (const w of flagged.slice(0, 15)) {
  console.log('  ' + w.addr.slice(0, 12).padEnd(14) + w.style.padEnd(10) + w.score.toFixed(1).padStart(6) +
    (' ' + (w.decidedROI != null ? (w.decidedROI * 100).toFixed(0) + '%' : '—')).padStart(7) +
    (' $' + (w.decidedCapital / 1000).toFixed(0) + 'k').padStart(9) +
    (' ' + ((w.winRate || 0) * 100).toFixed(0) + '%').padStart(6) +
    ('  ' + w.mmScore).padStart(5) +
    ('  ' + w.dualSideRate.toFixed(2)).padStart(10) +
    '  ' + (w.alphaVerdict || '—'));
}

// ── 6. Gate diagnosis ──────────────────────────────────────────────
console.log('\n  ── 6. QUALITY GATE PASS/FAIL (what should have evicted) ──');
const weak = countIn(wallets, w => w.decidedROI != null && w.decidedROI < 0.10);
const thin = countIn(wallets, w => (w.decidedCapital || 0) < 10000);
const negative = countIn(wallets, w => w.decidedROI != null && w.decidedROI < 0 && (w.decidedCapital || 0) >= 10000);
const suspWR = countIn(wallets, w => w.winRate != null && w.winRate > 0.95);
const softMM = countIn(wallets, w => w.mmScore >= 1 && w.mmScore < 3);
const hardMM = countIn(wallets, w => w.mmScore >= 3);
const alphaFail = countIn(wallets, w => w.alphaVerdict === 'fails');
const dormant = countIn(wallets, w => w.recentTradesPerDay < 0.1);

console.log('  decidedROI < 10% (weak edge):                  ' + weak + '  (' + pct(weak / wallets.length) + ')');
console.log('  decidedCapital < $10k (thin sample):           ' + thin + '  (' + pct(thin / wallets.length) + ')');
console.log('  Negative ROI + capital ≥ $10k (evict-worthy):  ' + negative);
console.log('  winRate > 95% (mean-picker candidate):         ' + suspWR + '  (' + pct(suspWR / wallets.length) + ')');
console.log('  mmScore 1-2 (soft-penalized):                  ' + softMM + '  (' + pct(softMM / wallets.length) + ')');
console.log('  mmScore ≥ 3 (should be evicted):               ' + hardMM);
console.log('  alphaVerdict = fails:                          ' + alphaFail);
console.log('  recentTradesPerDay < 0.1 (dormant):            ' + dormant + '  (' + pct(dormant / wallets.length) + ')');

// ── 7. Summary verdict ────────────────────────────────────────────
console.log('\n  ── 7. VERDICT ──');
const truPlayers = wallets.filter(w =>
  w.decidedROI != null && w.decidedROI >= 0.15
  && (w.decidedCapital || 0) >= 10000
  && w.mmScore < 3
  && w.alphaVerdict !== 'fails'
  && (w.recentTradesPerDay || 0) >= 0.1
);
console.log('  High-quality, active, directional wallets: ' + truPlayers.length + ' / ' + wallets.length +
  ' (' + pct(truPlayers.length / wallets.length) + ')');
console.log('    Criteria: decidedROI ≥ 15%  AND  decidedCapital ≥ $10k  AND  mmScore < 3');
console.log('              AND  alpha not failed  AND  ≥0.1 recent trades/day\n');
console.log('  If this number is small, the pool has lots of marginal wallets we should cull.');
console.log('  If it\'s large but few emit signals, the issue is downstream (gates, freshness).\n');
