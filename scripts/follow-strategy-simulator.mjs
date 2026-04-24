// Follow-strategy simulator
// ──────────────────────────
// Stacks the findings from wallet-style-profiles + signal-feature-regression
// into filter strategies. Each strategy is a sequence of AND-gates; we
// replay historical resolved signals under each strategy and report
// N kept, WR, weighted avg return.
//
// This tells us which strategy gives the best P&L per dollar deployed
// AND at what volume cost. Final recommendation balances return vs throughput.

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const pool = walletsData.pool || walletsData;
const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');

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

function contributors(sig) {
  const s = new Set();
  if (Array.isArray(sig.currentWallets)) sig.currentWallets.forEach(w => w && w.address && s.add(String(w.address).toLowerCase()));
  if (sig.soloWallet) s.add(String(sig.soloWallet).toLowerCase());
  return [...s];
}

function hasHolder(sig) {
  for (const a of contributors(sig)) {
    const w = pool[a];
    if (w && classifyStyle(w.stats) === 'holder') return true;
  }
  return false;
}

function hasMMLike(sig) {
  for (const a of contributors(sig)) {
    const w = pool[a];
    if (w && classifyStyle(w.stats) === 'mm-like') return true;
  }
  return false;
}

function hasSniperOnly(sig) {
  const cs = contributors(sig);
  if (cs.length === 0) return false;
  let anySniper = false;
  for (const a of cs) {
    const w = pool[a];
    if (!w) continue;
    const s = classifyStyle(w.stats);
    if (s === 'sniper') anySniper = true;
    if (s === 'holder' || s === 'mm-like') return false;  // disqualifier
  }
  return anySniper;
}

const STRATEGIES = [
  { name: 'baseline (all signals)', fn: () => true },
  { name: 'A: exclude mm-like contributors', fn: s => !hasMMLike(s) },
  { name: 'B: exclude holder contributors', fn: s => !hasHolder(s) },
  { name: 'C: A + B (no mm-like, no holder)', fn: s => !hasMMLike(s) && !hasHolder(s) },
  { name: 'D: C + walletCount ≥ 6', fn: s => !hasMMLike(s) && !hasHolder(s) && (s.walletCount || 0) >= 6 },
  { name: 'E: C + walletCount ≥ 8 (consensus floor)', fn: s => !hasMMLike(s) && !hasHolder(s) && (s.walletCount || 0) >= 8 },
  { name: 'F: C + entry 50-85¢ only', fn: s => !hasMMLike(s) && !hasHolder(s) && (s.avgEntryPrice || 0) >= 0.50 && (s.avgEntryPrice || 1) < 0.85 },
  { name: 'G: C + entry 70-85¢ only (sweet spot)', fn: s => !hasMMLike(s) && !hasHolder(s) && (s.avgEntryPrice || 0) >= 0.70 && (s.avgEntryPrice || 1) < 0.85 },
  { name: 'H: D + F (walletCount ≥ 6, entry 50-85¢)', fn: s => !hasMMLike(s) && !hasHolder(s) && (s.walletCount || 0) >= 6 && (s.avgEntryPrice || 0) >= 0.50 && (s.avgEntryPrice || 1) < 0.85 },
  { name: 'I: H + confidence ≤ 70 (cap)', fn: s => !hasMMLike(s) && !hasHolder(s) && (s.walletCount || 0) >= 6 && (s.avgEntryPrice || 0) >= 0.50 && (s.avgEntryPrice || 1) < 0.85 && (s.confidence || 0) <= 70 },
  { name: 'J: sniper-only (no holder, sniper present)', fn: s => hasSniperOnly(s) },
  { name: 'K: J + walletCount ≥ 6', fn: s => hasSniperOnly(s) && (s.walletCount || 0) >= 6 },
  { name: 'L: consensus only (walletCount ≥ 8, no holder)', fn: s => (s.walletCount || 0) >= 8 && !hasHolder(s) && !hasMMLike(s) },
  { name: 'M: L + entry 50-85¢', fn: s => (s.walletCount || 0) >= 8 && !hasHolder(s) && !hasMMLike(s) && (s.avgEntryPrice || 0) >= 0.50 && (s.avgEntryPrice || 1) < 0.85 },
];

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Follow-strategy simulator — stack filters, measure lift');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log(`  Historical resolved signals: ${resolved.length}\n`);
console.log(`  ${'Strategy'.padEnd(48)}  ${'N'.padStart(5)}  ${'%vol'.padStart(5)}  ${'WR'.padStart(5)}  ${'WgtRet'.padStart(7)}  ${'Total$'.padStart(8)}`);
console.log('  ' + '─'.repeat(92));

const results = [];
for (const strat of STRATEGIES) {
  let N = 0, wins = 0, totalRet = 0;
  for (const sig of resolved) {
    if (!strat.fn(sig)) continue;
    N++;
    if (sig.outcome === 'win') wins++;
    if (typeof sig.signalReturn === 'number') totalRet += sig.signalReturn;
  }
  const wr = N > 0 ? wins / N : 0;
  const weightedRet = N > 0 ? totalRet / N : 0;
  results.push({ name: strat.name, N, wr, weightedRet, totalRet });
  const pct = (N / resolved.length * 100).toFixed(0) + '%';
  const wrStr = (wr * 100).toFixed(0) + '%';
  const retStr = (weightedRet >= 0 ? '+' : '') + weightedRet.toFixed(1) + '%';
  const pnlStr = (totalRet >= 0 ? '+' : '') + totalRet.toFixed(0);
  console.log(`  ${strat.name.padEnd(48)}  ${String(N).padStart(5)}  ${pct.padStart(5)}  ${wrStr.padStart(5)}  ${retStr.padStart(7)}  ${pnlStr.padStart(8)}`);
}

// Find the best P&L / dollar strategy
results.sort((a, b) => b.weightedRet - a.weightedRet);
console.log(`\n  Top 3 by weighted return:`);
for (let i = 0; i < Math.min(3, results.length); i++) {
  const r = results[i];
  console.log(`    ${i+1}. ${r.name}  →  ${(r.weightedRet >= 0 ? '+' : '') + r.weightedRet.toFixed(1)}% on ${r.N} signals`);
}

console.log('\n═══════════════════════════════════════════════════════════════════\n');
