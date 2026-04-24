// Wallet-style profiler
// ─────────────────────
// Classifies every pool wallet into a trading-style archetype based on
// stats we already compute (trades-per-market, sell ratio, hold time,
// dual-side rate, entry price, churn). Then joins against signal
// attribution to measure which styles produce copyable alpha vs
// un-copyable alpha (great trader, bad signal).
//
// Archetypes (in priority order — most specific tested first):
//   mm-like       : dualSideRate > 0.30 OR mmScore ≥ 3
//   churner       : trades/market > 8
//   averager      : trades/market ∈ [3, 8], sellRatio > 0.30
//   sniper        : trades/market ≤ 2, holdTime < 48h
//   holder        : sellRatio < 0.15 (wins via REDEEM)
//   mixed         : anything else
//
// Cross-cut: short-term (<24h hold) vs medium-term (24-240h) vs long-term (>240h)
//
// Output: per-style signal counts, WR, avg return, and wallet examples.

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
  const dualRate = stats.dualSideRate || 0;
  const mmScore = stats.mmScore || 0;
  if (dualRate > 0.30 || mmScore >= 3) return 'mm-like';

  const tt = stats.totalTrades || 0;
  const um = stats.uniqueMarkets || 0;
  const tpm = um > 0 ? tt / um : 0;
  const sellRatio = stats.sellRatio ?? 1;
  const hold = stats.avgHoldTimeHours || 0;

  if (tpm > 8) return 'churner';
  if (tpm >= 3 && sellRatio > 0.30) return 'averager';
  if (tpm <= 2 && hold < 48) return 'sniper';
  if (sellRatio < 0.15) return 'holder';
  return 'mixed';
}

function classifyHorizon(stats) {
  const h = stats?.avgHoldTimeHours || 0;
  if (h < 24) return 'short';
  if (h < 240) return 'medium';
  return 'long';
}

// Build attribution map
const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');
const attr = new Map();
for (const sig of resolved) {
  const ws = new Set();
  if (Array.isArray(sig.currentWallets)) sig.currentWallets.forEach(w => w && w.address && ws.add(w.address.toLowerCase()));
  if (sig.soloWallet) ws.add(sig.soloWallet.toLowerCase());
  const ret = typeof sig.signalReturn === 'number' ? sig.signalReturn : null;
  for (const a of ws) {
    if (!attr.has(a)) attr.set(a, { signals: 0, wins: 0, totalRet: 0, retN: 0 });
    const r = attr.get(a);
    r.signals++;
    if (sig.outcome === 'win') r.wins++;
    if (ret !== null) { r.totalRet += ret; r.retN++; }
  }
}

// Classify every pool wallet
const styleBuckets = new Map();  // style → { walletCount, signals, wins, totalRet, retN, wallets: [] }
const horizonBuckets = new Map();

for (const [addr, w] of Object.entries(pool)) {
  if (!w || w.status === 'removed') continue;
  const style = classifyStyle(w.stats);
  const horizon = classifyHorizon(w.stats);
  const a = attr.get(addr.toLowerCase());

  if (!styleBuckets.has(style)) styleBuckets.set(style, { walletCount: 0, signals: 0, wins: 0, totalRet: 0, retN: 0, wallets: [] });
  const sb = styleBuckets.get(style);
  sb.walletCount++;
  if (a) {
    sb.signals += a.signals;
    sb.wins += a.wins;
    sb.totalRet += a.totalRet;
    sb.retN += a.retN;
    sb.wallets.push({ addr: addr.slice(0, 12), score: w.score, signals: a.signals, wr: a.wins / a.signals, avgRet: a.retN > 0 ? a.totalRet / a.retN : 0 });
  }

  if (!horizonBuckets.has(horizon)) horizonBuckets.set(horizon, { walletCount: 0, signals: 0, wins: 0, totalRet: 0, retN: 0 });
  const hb = horizonBuckets.get(horizon);
  hb.walletCount++;
  if (a) {
    hb.signals += a.signals;
    hb.wins += a.wins;
    hb.totalRet += a.totalRet;
    hb.retN += a.retN;
  }
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Wallet style profiles vs signal outcomes');
console.log('═══════════════════════════════════════════════════════════════════\n');

console.log('  ── By TRADING STYLE ──');
console.log(`  ${'Style'.padEnd(12)}  ${'Wallets'.padStart(7)}  ${'Sigs'.padStart(5)}  ${'WR'.padStart(5)}  ${'AvgRet'.padStart(8)}  Top contributors`);
console.log('  ' + '─'.repeat(90));
const styleRows = [...styleBuckets.entries()].map(([style, b]) => ({
  style,
  walletCount: b.walletCount,
  signals: b.signals,
  wr: b.signals > 0 ? b.wins / b.signals : 0,
  avgRet: b.retN > 0 ? b.totalRet / b.retN : 0,
  wallets: b.wallets,
}));
styleRows.sort((a, b) => b.avgRet - a.avgRet);
for (const r of styleRows) {
  const wr = r.signals > 0 ? (r.wr * 100).toFixed(0) + '%' : '—';
  const ret = r.signals > 0 ? (r.avgRet >= 0 ? '+' : '') + r.avgRet.toFixed(1) + '%' : '—';
  const topWallets = r.wallets
    .filter(w => w.signals >= 3)
    .sort((a, b) => b.avgRet - a.avgRet)
    .slice(0, 2)
    .map(w => `${w.addr}… (${w.signals} sigs, ${(w.avgRet >= 0 ? '+' : '')}${w.avgRet.toFixed(0)}%)`)
    .join('  |  ') || '—';
  console.log(`  ${r.style.padEnd(12)}  ${String(r.walletCount).padStart(7)}  ${String(r.signals).padStart(5)}  ${wr.padStart(5)}  ${ret.padStart(8)}  ${topWallets}`);
}

console.log('\n  ── By HOLD-TIME HORIZON ──');
console.log(`  ${'Horizon'.padEnd(12)}  ${'Wallets'.padStart(7)}  ${'Sigs'.padStart(5)}  ${'WR'.padStart(5)}  ${'AvgRet'.padStart(8)}`);
console.log('  ' + '─'.repeat(55));
for (const [horizon, b] of horizonBuckets) {
  const wr = b.signals > 0 ? (b.wins / b.signals * 100).toFixed(0) + '%' : '—';
  const ret = b.retN > 0 ? ((b.totalRet / b.retN >= 0 ? '+' : '') + (b.totalRet / b.retN).toFixed(1) + '%') : '—';
  console.log(`  ${horizon.padEnd(12)}  ${String(b.walletCount).padStart(7)}  ${String(b.signals).padStart(5)}  ${wr.padStart(5)}  ${ret.padStart(8)}`);
}

// ── Cross-cut: style × horizon ───────────────────────────────────────
console.log('\n  ── Style × Horizon matrix (avg return) ──');
const matrix = new Map();
for (const [addr, w] of Object.entries(pool)) {
  if (!w || w.status === 'removed') continue;
  const style = classifyStyle(w.stats);
  const horizon = classifyHorizon(w.stats);
  const key = `${style}|${horizon}`;
  const a = attr.get(addr.toLowerCase());
  if (!matrix.has(key)) matrix.set(key, { signals: 0, wins: 0, totalRet: 0, retN: 0, walletCount: 0 });
  const m = matrix.get(key);
  m.walletCount++;
  if (a) {
    m.signals += a.signals;
    m.wins += a.wins;
    m.totalRet += a.totalRet;
    m.retN += a.retN;
  }
}
const horizons = ['short', 'medium', 'long'];
console.log(`  ${'Style'.padEnd(12)}  ${horizons.map(h => h.padStart(10)).join('  ')}`);
console.log('  ' + '─'.repeat(55));
for (const row of styleRows) {
  const cells = horizons.map(h => {
    const m = matrix.get(`${row.style}|${h}`);
    if (!m || m.signals < 5) return '—';
    const avgRet = m.retN > 0 ? m.totalRet / m.retN : 0;
    const wc = m.walletCount;
    return `${(avgRet >= 0 ? '+' : '') + avgRet.toFixed(0)}% (N${m.signals})`;
  });
  console.log(`  ${row.style.padEnd(12)}  ${cells.map(c => c.padStart(10)).join('  ')}`);
}

// ── Style × entry-price segmentation ─────────────────────────────────
console.log('\n  ── Style × avg-entry-price (wallet characteristic) ──');
const priceBuckets = ['<30¢', '30-50¢', '50-70¢', '70-85¢', '>85¢'];
function priceBucket(p) {
  if (!p || p <= 0) return null;
  if (p < 0.30) return '<30¢';
  if (p < 0.50) return '30-50¢';
  if (p < 0.70) return '50-70¢';
  if (p < 0.85) return '70-85¢';
  return '>85¢';
}
const byStylePrice = new Map();
for (const [addr, w] of Object.entries(pool)) {
  if (!w || w.status === 'removed') continue;
  const style = classifyStyle(w.stats);
  const pb = priceBucket(w.stats?.avgEntryPrice);
  if (!pb) continue;
  const key = `${style}|${pb}`;
  const a = attr.get(addr.toLowerCase());
  if (!byStylePrice.has(key)) byStylePrice.set(key, { signals: 0, wins: 0, totalRet: 0, retN: 0, walletCount: 0 });
  const m = byStylePrice.get(key);
  m.walletCount++;
  if (a) {
    m.signals += a.signals;
    m.wins += a.wins;
    m.totalRet += a.totalRet;
    m.retN += a.retN;
  }
}
console.log(`  ${'Style'.padEnd(12)}  ${priceBuckets.map(p => p.padStart(12)).join('  ')}`);
console.log('  ' + '─'.repeat(80));
for (const row of styleRows) {
  const cells = priceBuckets.map(p => {
    const m = byStylePrice.get(`${row.style}|${p}`);
    if (!m || m.signals < 5) return '—';
    const avgRet = m.retN > 0 ? m.totalRet / m.retN : 0;
    return `${(avgRet >= 0 ? '+' : '') + avgRet.toFixed(0)}% (${m.signals})`;
  });
  console.log(`  ${row.style.padEnd(12)}  ${cells.map(c => c.padStart(12)).join('  ')}`);
}

// ── Summary insights ─────────────────────────────────────────────────
console.log('\n  ── HEADLINE INSIGHTS ──');
const bestStyle = styleRows.filter(r => r.signals >= 30)[0];
const worstStyle = styleRows.filter(r => r.signals >= 30).slice(-1)[0];
if (bestStyle && worstStyle && bestStyle.style !== worstStyle.style) {
  console.log(`  Best-producing style: ${bestStyle.style} (${bestStyle.signals} sigs, ${(bestStyle.wr*100).toFixed(0)}% WR, ${(bestStyle.avgRet>=0?'+':'')}${bestStyle.avgRet.toFixed(1)}% avg ret)`);
  console.log(`  Worst-producing style: ${worstStyle.style} (${worstStyle.signals} sigs, ${(worstStyle.wr*100).toFixed(0)}% WR, ${(worstStyle.avgRet>=0?'+':'')}${worstStyle.avgRet.toFixed(1)}% avg ret)`);
  console.log(`  Delta: ${(bestStyle.avgRet - worstStyle.avgRet).toFixed(1)}pp — ${Math.abs(bestStyle.avgRet - worstStyle.avgRet) > 15 ? 'LARGE — style matters' : 'small — style not the primary predictor'}`);
}

console.log('\n═══════════════════════════════════════════════════════════════════\n');
