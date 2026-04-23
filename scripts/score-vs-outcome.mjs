// Score-vs-outcome validator
// ---------------------------
// For every resolved signal, look up the CURRENT score of the wallets that
// contributed. Bucket by score band and report per-bucket signal WR and
// avg return.  If score is predictive, higher buckets should show
// monotonically higher WR / return.
//
// This is THE sanity check on the scoring function — it doesn't matter
// how elegant the formula is if bucketing by its output doesn't correlate
// with real-world signal outcomes.

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

// For each resolved signal, find the MAX score among contributing wallets
// (this is the wallet that drove the signal's promotion) and the MEAN score.
// Use max for headline bucketing — it's what the signal filter uses.
const buckets = new Map();
// Score bands, wide enough to hold N≥20 per bucket given 1446 signals
const BANDS = [
  [0, 20], [20, 25], [25, 30], [30, 35], [35, 40], [40, 50], [50, 1000],
];
const bandLabel = (s) => {
  for (const [lo, hi] of BANDS) {
    if (s >= lo && s < hi) return `${lo}-${hi === 1000 ? '∞' : hi}`;
  }
  return 'unknown';
};

let missingScores = 0, totalSigs = 0;
for (const sig of resolved) {
  const ws = new Set();
  if (Array.isArray(sig.currentWallets)) sig.currentWallets.forEach(w => w && w.address && ws.add(w.address.toLowerCase()));
  if (sig.soloWallet) ws.add(sig.soloWallet.toLowerCase());
  if (Array.isArray(sig.wallets)) sig.wallets.forEach(w => w && w.address && ws.add(w.address.toLowerCase()));
  if (ws.size === 0) continue;

  const scores = [];
  for (const a of ws) {
    const w = pool[a];
    if (w && typeof w.score === 'number') scores.push(w.score);
  }
  if (scores.length === 0) { missingScores++; continue; }
  totalSigs++;

  const maxScore = Math.max(...scores);
  const meanScore = scores.reduce((x, y) => x + y, 0) / scores.length;

  const band = bandLabel(maxScore);
  if (!buckets.has(band)) {
    buckets.set(band, { signals: 0, wins: 0, totalRet: 0, retN: 0, maxScoreSum: 0, meanScoreSum: 0 });
  }
  const b = buckets.get(band);
  b.signals++;
  if (sig.outcome === 'win') b.wins++;
  if (typeof sig.signalReturn === 'number') { b.totalRet += sig.signalReturn; b.retN++; }
  b.maxScoreSum += maxScore;
  b.meanScoreSum += meanScore;
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Score-vs-outcome validator — does high score predict wins?');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log(`  Resolved signals scored: ${totalSigs}  (skipped ${missingScores} — wallet not in current pool)\n`);
console.log(`  ${'Score band'.padEnd(10)}  ${'N'.padStart(5)}  ${'WR'.padStart(6)}  ${'AvgRet'.padStart(9)}  ${'Mean score'.padStart(10)}`);
console.log('  ' + '─'.repeat(58));

const ordered = BANDS.map(([lo, hi]) => `${lo}-${hi === 1000 ? '∞' : hi}`);
for (const band of ordered) {
  const b = buckets.get(band);
  if (!b) { console.log(`  ${band.padEnd(10)}  ${'—'.padStart(5)}  ${'—'.padStart(6)}  ${'—'.padStart(9)}  ${'—'.padStart(10)}`); continue; }
  const wr = b.wins / b.signals;
  const avgRet = b.retN > 0 ? b.totalRet / b.retN : 0;
  const meanScore = b.meanScoreSum / b.signals;
  const wrStr = (wr * 100).toFixed(1) + '%';
  const retStr = (avgRet >= 0 ? '+' : '') + avgRet.toFixed(1) + '%';
  console.log(`  ${band.padEnd(10)}  ${String(b.signals).padStart(5)}  ${wrStr.padStart(6)}  ${retStr.padStart(9)}  ${meanScore.toFixed(1).padStart(10)}`);
}

// Also compute Spearman-like correlation — rank wallets by their MAX-score
// contribution and see if higher rank = higher return.
console.log('\n  ── Diagnostic: monotonicity check ──');
const rowsRet = [];
for (const band of ordered) {
  const b = buckets.get(band);
  if (b && b.retN > 0) rowsRet.push({ band, avgRet: b.totalRet / b.retN });
}
let monotonic = true;
for (let i = 1; i < rowsRet.length; i++) {
  if (rowsRet[i].avgRet < rowsRet[i - 1].avgRet - 2) { monotonic = false; break; } // allow 2% slack
}
console.log(`  Score → avg-return monotonicity: ${monotonic ? '✓ (score predicts return)' : '✗ (BROKEN — higher score does not mean higher return)'}`);

// Head-to-head: worst-performing band vs best-performing band
const sorted = rowsRet.slice().sort((a, b) => a.avgRet - b.avgRet);
if (sorted.length >= 2) {
  const worst = sorted[0], best = sorted[sorted.length - 1];
  const delta = best.avgRet - worst.avgRet;
  console.log(`  Best band (${best.band}) beats worst band (${worst.band}) by ${delta.toFixed(1)}% avg return`);
  if (delta < 20) console.log('  → Signal is WEAK: bands are nearly indistinguishable by score alone');
  else console.log('  → Signal is STRONG: score DOES separate bands');
}

console.log('\n═══════════════════════════════════════════════════════════════════\n');
