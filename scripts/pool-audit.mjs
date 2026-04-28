// Pool audit — comprehensive analysis of currently tracked wallets.
//
// Sections:
//   1. Composition (total, active, removed + eviction reasons)
//   2. Score distribution (histogram + top/bottom quartiles)
//   3. Style distribution (sniper/averager/churner/mixed/holder/mm-like)
//   4. Quality dimensions (decidedROI, singleSideROI, mmScore, churn, WR)
//   5. Signal attribution leaderboards (top producers, top avg return)
//   6. Freshness (last trade, dormancy)
//   7. Category preferences (what categories is the pool buying?)
//   8. Gap analysis (high-score wallets with zero contribution)
//
// Usage: node scripts/pool-audit.mjs [--top N]

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const args = process.argv.slice(2);
const TOP_N = parseInt((args.find(a => a.startsWith('--top=')) || '--top=20').split('=')[1], 10);

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

// Build attribution map
const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');
const attr = new Map();
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

// Classify every wallet
const wallets = [];
for (const [addr, w] of Object.entries(pool)) {
  if (!w || typeof w !== 'object') continue;
  const style = classifyStyle(w.stats);
  const a = attr.get(addr.toLowerCase());
  wallets.push({
    addr, score: w.score || 0, status: w.status || 'active',
    style,
    decidedROI: w.stats?.decidedROI,
    singleSideROI: w.stats?.singleSideROI,
    decidedCapital: w.stats?.decidedCapital,
    singleSideCapital: w.stats?.singleSideCapital,
    mmScore: w.stats?.mmScore || 0,
    winRate: w.stats?.winRate,
    totalTrades: w.stats?.totalTrades || 0,
    uniqueMarkets: w.stats?.uniqueMarkets || 0,
    churn: (w.stats?.uniqueMarkets || 0) > 0 ? (w.stats.totalTrades / w.stats.uniqueMarkets) : 0,
    lastTradeTs: w.stats?.lastTradeTs || 0,
    removeReason: w.removeReason,
    attrMultiplier: w.scoreComponents?.attrMultiplier,
    churnPenalty: w.scoreComponents?.churnPenalty,
    signals: a?.signals || 0,
    sigWR: a ? (a.wins / a.signals) : 0,
    sigAvgRet: a && a.retN > 0 ? (a.totalRet / a.retN) : 0,
  });
}

const active = wallets.filter(w => w.status !== 'removed');
const removed = wallets.filter(w => w.status === 'removed');

// ═══════════════════════════════════════════════════════════════════
console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Pool audit — ' + new Date().toISOString());
console.log('═══════════════════════════════════════════════════════════════════');

// 1. Composition
console.log('\n  ── 1. POOL COMPOSITION ──');
console.log('  Total tracked wallets:     ' + wallets.length);
console.log('  Active:                    ' + active.length);
console.log('  Removed:                   ' + removed.length);
const removeReasons = {};
for (const w of removed) removeReasons[w.removeReason || 'unknown'] = (removeReasons[w.removeReason || 'unknown'] || 0) + 1;
if (Object.keys(removeReasons).length > 0) {
  console.log('  Eviction reasons:');
  for (const [k, v] of Object.entries(removeReasons).sort((a, b) => b[1] - a[1])) {
    console.log('    ' + k.padEnd(24) + String(v).padStart(4));
  }
}

// 2. Score distribution
console.log('\n  ── 2. SCORE DISTRIBUTION (active) ──');
const bands = [[0,15],[15,20],[20,25],[25,30],[30,35],[35,40],[40,100]];
for (const [lo, hi] of bands) {
  const n = active.filter(w => w.score >= lo && w.score < hi).length;
  const pct = (n / active.length * 100).toFixed(0);
  const bar = '█'.repeat(Math.round(n / 10));
  console.log('    ' + (lo + '-' + (hi === 100 ? '∞' : hi)).padEnd(8) + String(n).padStart(4) + '  ' + pct.padStart(3) + '%  ' + bar);
}
const scores = active.map(w => w.score).sort((a, b) => a - b);
const median = scores[Math.floor(scores.length / 2)];
const p25 = scores[Math.floor(scores.length * 0.25)];
const p75 = scores[Math.floor(scores.length * 0.75)];
console.log('    p25=' + p25.toFixed(1) + '  median=' + median.toFixed(1) + '  p75=' + p75.toFixed(1) + '  max=' + scores[scores.length - 1].toFixed(1));

// 3. Style distribution with score stats per style
console.log('\n  ── 3. STYLE DISTRIBUTION (active) ──');
console.log('  ' + 'Style'.padEnd(12) + 'Count'.padStart(6) + 'AvgScore'.padStart(10) + 'MedScore'.padStart(10) + '  AvgSignals  AvgRet');
const byStyle = {};
for (const w of active) {
  if (!byStyle[w.style]) byStyle[w.style] = [];
  byStyle[w.style].push(w);
}
for (const [s, arr] of Object.entries(byStyle).sort((a, b) => b[1].length - a[1].length)) {
  const avgScore = arr.reduce((a, w) => a + w.score, 0) / arr.length;
  const sortedScores = [...arr].sort((a, b) => a.score - b.score);
  const medScore = sortedScores[Math.floor(sortedScores.length / 2)].score;
  const contribs = arr.filter(w => w.signals > 0);
  const avgSigs = contribs.length > 0 ? (contribs.reduce((a, w) => a + w.signals, 0) / contribs.length).toFixed(1) : '—';
  const avgRet = contribs.length > 0 ?
    ((contribs.reduce((a, w) => a + w.sigAvgRet, 0) / contribs.length >= 0 ? '+' : '') +
      (contribs.reduce((a, w) => a + w.sigAvgRet, 0) / contribs.length).toFixed(1) + '%') : '—';
  console.log('  ' + s.padEnd(12) + String(arr.length).padStart(6) + avgScore.toFixed(1).padStart(10) + medScore.toFixed(1).padStart(10) + '  ' + String(avgSigs).padStart(10) + '  ' + avgRet.padStart(7));
}

// 4. Quality dimensions
console.log('\n  ── 4. QUALITY DIMENSIONS (active) ──');
const countIn = (arr, pred) => arr.filter(pred).length;
console.log('    decidedROI > 0.50 (>50%):       ' + countIn(active, w => w.decidedROI > 0.50));
console.log('    decidedROI > 0.30 (>30%):       ' + countIn(active, w => w.decidedROI > 0.30));
console.log('    decidedROI null (fallback):     ' + countIn(active, w => w.decidedROI == null));
console.log('    decidedCapital > $50k:          ' + countIn(active, w => w.decidedCapital > 50000));
console.log('    mmScore >= 3 (should be 0):     ' + countIn(active, w => w.mmScore >= 3));
console.log('    mmScore 1-2 (soft penalty):     ' + countIn(active, w => w.mmScore >= 1 && w.mmScore < 3));
console.log('    churnRatio > 8 (heavy):         ' + countIn(active, w => w.churn > 8));
console.log('    churnRatio 3-8 (moderate):      ' + countIn(active, w => w.churn >= 3 && w.churn <= 8));
console.log('    winRate > 90%:                  ' + countIn(active, w => w.winRate > 0.90));
console.log('    winRate 70-90%:                 ' + countIn(active, w => w.winRate >= 0.70 && w.winRate <= 0.90));

// 5. Signal attribution leaderboards
console.log('\n  ── 5. SIGNAL ATTRIBUTION ──');
const contributors = active.filter(w => w.signals > 0);
const nonContribs = active.filter(w => w.signals === 0);
console.log('  Contributing wallets:      ' + contributors.length + ' / ' + active.length + ' (' + (contributors.length / active.length * 100).toFixed(0) + '%)');
console.log('  Non-contributing:          ' + nonContribs.length + ' (dormant or not in signal range)');

console.log('\n  Top ' + TOP_N + ' by signal return (≥ 5 signals):');
const qualifyingContribs = contributors.filter(w => w.signals >= 5).sort((a, b) => b.sigAvgRet - a.sigAvgRet);
console.log('  ' + 'Wallet'.padEnd(14) + 'Style'.padEnd(10) + 'Score'.padStart(6) + '  Sigs'.padStart(6) + '  WR'.padStart(5) + '  AvgRet'.padStart(8) + '  attrMult');
for (const w of qualifyingContribs.slice(0, TOP_N)) {
  const wr = (w.sigWR * 100).toFixed(0) + '%';
  const ret = (w.sigAvgRet >= 0 ? '+' : '') + w.sigAvgRet.toFixed(0) + '%';
  const am = w.attrMultiplier != null ? w.attrMultiplier.toFixed(2) : '—';
  console.log('  ' + w.addr.slice(0, 12).padEnd(14) + w.style.padEnd(10) + w.score.toFixed(1).padStart(6) + '  ' + String(w.signals).padStart(4) + '  ' + wr.padStart(5) + '  ' + ret.padStart(7) + '  ' + am.padStart(7));
}

console.log('\n  Bottom 10 by signal return (≥ 5 signals):');
for (const w of qualifyingContribs.slice(-10)) {
  const wr = (w.sigWR * 100).toFixed(0) + '%';
  const ret = (w.sigAvgRet >= 0 ? '+' : '') + w.sigAvgRet.toFixed(0) + '%';
  const am = w.attrMultiplier != null ? w.attrMultiplier.toFixed(2) : '—';
  console.log('  ' + w.addr.slice(0, 12).padEnd(14) + w.style.padEnd(10) + w.score.toFixed(1).padStart(6) + '  ' + String(w.signals).padStart(4) + '  ' + wr.padStart(5) + '  ' + ret.padStart(7) + '  ' + am.padStart(7));
}

// 6. Freshness
console.log('\n  ── 6. FRESHNESS / DORMANCY (active) ──');
const now = Math.floor(Date.now() / 1000);
const dayBuckets = [1, 3, 7, 14, 30, 90];
let last = 0;
for (const d of dayBuckets) {
  const n = active.filter(w => (now - w.lastTradeTs) / 86400 < d).length - last;
  last = active.filter(w => (now - w.lastTradeTs) / 86400 < d).length;
  const pct = (n / active.length * 100).toFixed(0);
  console.log('    Last trade ' + (d === 1 ? '< 24h' : (dayBuckets[dayBuckets.indexOf(d) - 1] || 0) + '-' + d + 'd').padEnd(14) + String(n).padStart(4) + '  ' + pct.padStart(3) + '%');
}
const dormant30 = active.filter(w => (now - w.lastTradeTs) / 86400 >= 30).length;
console.log('    Dormant 30d+:               ' + dormant30 + ' (' + (dormant30 / active.length * 100).toFixed(0) + '%)');

// 7. Category preferences (need market data)
// Skip for now — uses marketLookup

// 8. Gap analysis
console.log('\n  ── 7. GAP ANALYSIS ──');
const highScoreNoContrib = active.filter(w => w.score >= 30 && w.signals === 0).sort((a, b) => b.score - a.score);
console.log('  High-score (≥30) non-contributors: ' + highScoreNoContrib.length);
console.log('  Top 10 (good scores, zero signals — likely dormant or off-category):');
console.log('  ' + 'Wallet'.padEnd(14) + 'Style'.padEnd(10) + 'Score'.padStart(6) + '  LastTrade'.padStart(13) + '  Churn'.padStart(6));
for (const w of highScoreNoContrib.slice(0, 10)) {
  const days = ((now - w.lastTradeTs) / 86400).toFixed(0) + 'd ago';
  console.log('  ' + w.addr.slice(0, 12).padEnd(14) + w.style.padEnd(10) + w.score.toFixed(1).padStart(6) + '  ' + days.padStart(11) + '  ' + w.churn.toFixed(1).padStart(6));
}

// Sniper wallets not making the solo cut
const snipersJustBelow = active.filter(w => w.style === 'sniper' && w.score >= 15 && w.score < 25).sort((a, b) => b.score - a.score);
console.log('\n  Snipers below new SOLO_MIN_SCORE=25 (would emit if threshold lowered to 20):');
console.log('  Count in 20-25 band: ' + active.filter(w => w.style === 'sniper' && w.score >= 20 && w.score < 25).length);
console.log('  Count in 15-20 band: ' + active.filter(w => w.style === 'sniper' && w.score >= 15 && w.score < 20).length);
console.log('  Top 5 in 20-25 (closest to unlock):');
for (const w of snipersJustBelow.filter(w => w.score >= 20).slice(0, 5)) {
  console.log('  ' + w.addr.slice(0, 12).padEnd(14) + '  Score=' + w.score.toFixed(1) + '  Sigs=' + w.signals + '  Churn=' + w.churn.toFixed(1));
}

// 9. Option 1 solo-eligible count (what we just tuned to)
const soloAllowed = new Set(['sniper', 'averager', 'churner']);
const soloEligibleNow = active.filter(w => soloAllowed.has(w.style) && w.score >= 25).length;
console.log('\n  ── 8. EMISSION HEALTH ──');
console.log('  Solo-eligible (post-tune):       ' + soloEligibleNow + ' wallets');
console.log('  Convergence-eligible:            ' + active.filter(w => !['holder', 'mm-like'].includes(w.style) && w.score >= 15).length + ' wallets');
console.log('  Active signals in pool:          ' + Object.keys(signalsData.active || {}).length);
console.log('  Resolved-signal history:         ' + resolved.length);

console.log('\n═══════════════════════════════════════════════════════════════════\n');
