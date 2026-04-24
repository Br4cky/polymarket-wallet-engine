// Signal-feature regression
// ──────────────────────────
// For every resolved signal, decompose along each signal-time feature and
// measure outcome conditional on that feature alone. Answers "which bits
// of information we had at signal open actually predicted the outcome?"
//
// Features evaluated:
//   - confidence bucket
//   - walletCount bucket
//   - avgEntryPrice bucket
//   - avgScore bucket (the anti-predictive one — confirm)
//   - signal duration (open → close)
//   - closeReason (resolved vs stale vs other)
//   - category (already mostly mapped)
//   - composite: sniper × 50-70¢ (from style profiler)
//
// For each feature bucket we report N, WR, avg return, and
// info-value-like approximation of predictive lift.

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

// Reuse style classifier inline
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
function primaryContributor(sig) {
  if (sig.soloWallet) return String(sig.soloWallet).toLowerCase();
  if (Array.isArray(sig.currentWallets) && sig.currentWallets.length > 0) {
    // highest score wallet
    const sorted = [...sig.currentWallets].sort((a, b) => (b.score || 0) - (a.score || 0));
    return String(sorted[0].address || '').toLowerCase();
  }
  return null;
}

// ── Bucket helpers ──
const bucketize = (val, bounds, labels) => {
  for (let i = 0; i < bounds.length; i++) if (val < bounds[i]) return labels[i];
  return labels[labels.length - 1];
};

function report(title, getKey, orderedLabels) {
  const b = new Map();
  for (const sig of resolved) {
    const key = getKey(sig);
    if (key == null) continue;
    if (!b.has(key)) b.set(key, { N: 0, wins: 0, totalRet: 0, retN: 0 });
    const r = b.get(key);
    r.N++;
    if (sig.outcome === 'win') r.wins++;
    if (typeof sig.signalReturn === 'number') { r.totalRet += sig.signalReturn; r.retN++; }
  }
  console.log(`\n  ── ${title} ──`);
  console.log(`  ${'Bucket'.padEnd(20)}  ${'N'.padStart(5)}  ${'WR'.padStart(5)}  ${'AvgRet'.padStart(8)}`);
  console.log('  ' + '─'.repeat(48));
  const labels = orderedLabels || [...b.keys()].sort();
  for (const lbl of labels) {
    const r = b.get(lbl);
    if (!r) { console.log(`  ${String(lbl).padEnd(20)}  ${'—'.padStart(5)}  ${'—'.padStart(5)}  ${'—'.padStart(8)}`); continue; }
    const wr = r.N > 0 ? (r.wins / r.N * 100).toFixed(0) + '%' : '—';
    const ret = r.retN > 0 ? ((r.totalRet / r.retN >= 0 ? '+' : '') + (r.totalRet / r.retN).toFixed(1) + '%') : '—';
    console.log(`  ${String(lbl).padEnd(20)}  ${String(r.N).padStart(5)}  ${wr.padStart(5)}  ${ret.padStart(8)}`);
  }
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Signal feature regression — what predicted wins?');
console.log('═══════════════════════════════════════════════════════════════════');

report('confidence bucket', sig => bucketize(sig.confidence || 0, [40, 55, 70, 85], ['<40', '40-55', '55-70', '70-85', '≥85']), ['<40', '40-55', '55-70', '70-85', '≥85']);

report('walletCount bucket', sig => bucketize(sig.walletCount || 1, [2, 4, 6, 8, 12], ['1', '2-3', '4-5', '6-7', '8-11', '≥12']), ['1', '2-3', '4-5', '6-7', '8-11', '≥12']);

report('avgEntryPrice bucket', sig => bucketize(sig.avgEntryPrice || 0.5, [0.15, 0.30, 0.50, 0.70, 0.85], ['<15¢', '15-30¢', '30-50¢', '50-70¢', '70-85¢', '≥85¢']), ['<15¢', '15-30¢', '30-50¢', '50-70¢', '70-85¢', '≥85¢']);

report('avgScore bucket (convergence sigs)', sig => {
  if (sig.signalType === 'solo') return null;
  return bucketize(sig.avgScore || 0, [15, 20, 25, 30, 35], ['<15', '15-20', '20-25', '25-30', '30-35', '≥35']);
}, ['<15', '15-20', '20-25', '25-30', '30-35', '≥35']);

report('signal duration (hours)', sig => {
  const h = sig.lifetimeHours || 0;
  return bucketize(h, [6, 24, 72, 168, 720], ['<6h', '6-24h', '1-3d', '3-7d', '1-4w', '>4w']);
}, ['<6h', '6-24h', '1-3d', '3-7d', '1-4w', '>4w']);

report('closeReason', sig => sig.closeReason || 'unknown');

report('signalType', sig => sig.signalType || 'unknown');

report('tier', sig => sig.tier || 'unknown');

// ── Composite: primary contributor style ─────────────────────────────
report('primary contributor style', sig => {
  const addr = primaryContributor(sig);
  if (!addr) return null;
  const w = pool[addr];
  if (!w || !w.stats) return null;
  return classifyStyle(w.stats);
});

// ── Composite: sniper × 50-70¢ ───────────────────────────────────────
report('composite: sniper × 50-70¢', sig => {
  const addr = primaryContributor(sig);
  const p = sig.avgEntryPrice || 0;
  if (!addr) return null;
  const w = pool[addr];
  if (!w || !w.stats) return null;
  const s = classifyStyle(w.stats);
  const inBand = p >= 0.50 && p < 0.70;
  if (s === 'sniper' && inBand) return 'sniper_50-70¢';
  if (s === 'sniper') return 'sniper_other_price';
  if (inBand) return 'nonsniper_50-70¢';
  return 'other';
}, ['sniper_50-70¢', 'sniper_other_price', 'nonsniper_50-70¢', 'other']);

// ── Composite: avoid-holder filter ───────────────────────────────────
report('composite: has any holder contributor', sig => {
  const ws = new Set();
  if (Array.isArray(sig.currentWallets)) sig.currentWallets.forEach(w => w && w.address && ws.add(String(w.address).toLowerCase()));
  if (sig.soloWallet) ws.add(String(sig.soloWallet).toLowerCase());
  if (ws.size === 0) return null;
  let hasHolder = false, hasSniper = false;
  for (const a of ws) {
    const w = pool[a];
    if (!w || !w.stats) continue;
    const s = classifyStyle(w.stats);
    if (s === 'holder') hasHolder = true;
    if (s === 'sniper') hasSniper = true;
  }
  if (hasSniper && !hasHolder) return '+sniper, -holder';
  if (hasHolder && !hasSniper) return '-sniper, +holder';
  if (hasHolder && hasSniper) return 'mixed: both';
  return 'neither';
}, ['+sniper, -holder', 'mixed: both', '-sniper, +holder', 'neither']);

console.log('\n═══════════════════════════════════════════════════════════════════\n');
