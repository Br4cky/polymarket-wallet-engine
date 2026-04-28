// Test volume-tuning options by replaying resolved signals through
// alternative gating rules and reporting volume + per-signal performance
// per option. Lets us decide what to tune without flying blind.
//
// Options compared:
//   A. Current (70-85¢ micro-cluster, 6+ cluster, 8+ consensus)
//   B. Widen micro-cluster price band 50-85¢
//   C. Widen micro-cluster price band 50-90¢ + lower size floor
//   D. Lower CLUSTER_MIN_WALLETS from 6 to 5 (extend cluster down)
//   E. Combine B + D
//
// We can't simulate the candidates that NEVER reached emit (those weren't
// stored). But we CAN segment historical resolved signals by the gate
// they would have hit under each option, and compute the resulting WR /
// avg return for each option's emission set.

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');

// ── Gating predicates ──────────────────────────────────────────────
const ENTRY_BANDS = {
  '70-85¢': p => p >= 0.70 && p < 0.85,
  '50-85¢': p => p >= 0.50 && p < 0.85,
  '50-90¢': p => p >= 0.50 && p < 0.90,
  '40-90¢': p => p >= 0.40 && p < 0.90,
};

function classifySignal(s, opts) {
  const wc = s.walletCount || 0;
  const entry = s.avgEntryPrice || 0;
  const totalSize = s.totalBuySize || 0;
  const avgScore = s.avgScore || 0;

  // CONSENSUS
  if (wc >= 8) {
    if (avgScore >= 12 && totalSize >= 1000) return 'consensus';
    return 'consensus_below_thresh';
  }
  // CLUSTER (Option D widens to 5)
  const clusterMin = opts.clusterMin || 6;
  if (wc >= clusterMin && wc <= 7) {
    if (avgScore >= 25 && totalSize >= 750) return 'cluster';
    return 'cluster_below_thresh';
  }
  // MICRO-CLUSTER
  const microMin = 2, microMax = clusterMin - 1;
  if (wc >= microMin && wc <= microMax) {
    const inBand = opts.microBand(entry);
    if (!inBand) return 'micro_band_miss';
    if (avgScore >= (opts.microMinScore ?? 20) && totalSize >= (opts.microMinSize ?? 500)) return 'micro-cluster';
    return 'micro_below_thresh';
  }
  return 'no_path';
}

const OPTIONS = [
  { name: 'A: current (70-85¢ micro)',       microBand: ENTRY_BANDS['70-85¢'], clusterMin: 6, microMinSize: 500, microMinScore: 20 },
  { name: 'B: widen micro 50-85¢',           microBand: ENTRY_BANDS['50-85¢'], clusterMin: 6, microMinSize: 500, microMinScore: 20 },
  { name: 'C: widen micro 50-90¢ + lower size', microBand: ENTRY_BANDS['50-90¢'], clusterMin: 6, microMinSize: 250, microMinScore: 18 },
  { name: 'D: cluster min 5 (was 6)',        microBand: ENTRY_BANDS['70-85¢'], clusterMin: 5, microMinSize: 500, microMinScore: 20 },
  { name: 'E: B + D combined',               microBand: ENTRY_BANDS['50-85¢'], clusterMin: 5, microMinSize: 500, microMinScore: 20 },
  { name: 'F: aggressive (40-90¢ + min 5)',  microBand: ENTRY_BANDS['40-90¢'], clusterMin: 5, microMinSize: 250, microMinScore: 18 },
];

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Volume-tuning options — historical what-if');
console.log('═══════════════════════════════════════════════════════════════════');
console.log('  Resolved signals analyzed: ' + resolved.length);
console.log();

console.log(`  ${'Option'.padEnd(40)} ${'N'.padStart(5)} ${'WR'.padStart(5)} ${'AvgRet'.padStart(8)} ${'TotalRet'.padStart(10)}`);
console.log('  ' + '─'.repeat(75));

for (const opt of OPTIONS) {
  const buckets = {};
  for (const s of resolved) {
    const cls = classifySignal(s, opt);
    if (!buckets[cls]) buckets[cls] = [];
    buckets[cls].push(s);
  }
  const emitted = [
    ...(buckets.consensus || []),
    ...(buckets.cluster || []),
    ...(buckets['micro-cluster'] || []),
  ];
  const N = emitted.length;
  const wins = emitted.filter(s => s.outcome === 'win').length;
  const totalRet = emitted.reduce((a, s) => a + (typeof s.signalReturn === 'number' ? s.signalReturn : 0), 0);
  const retN = emitted.filter(s => typeof s.signalReturn === 'number').length;
  const wr = N > 0 ? (wins / N * 100).toFixed(0) + '%' : '—';
  const avgRet = retN > 0 ? ((totalRet / retN >= 0 ? '+' : '') + (totalRet / retN).toFixed(1) + '%') : '—';
  console.log(`  ${opt.name.padEnd(40)} ${String(N).padStart(5)} ${wr.padStart(5)} ${avgRet.padStart(8)} ${(totalRet >= 0 ? '+' : '') + totalRet.toFixed(0)}`.padStart(85));
}

// ── Per-option detailed breakdown for option B (widening micro to 50-85¢) ──
console.log('\n  ── Detail: Option B vs Option A (where the new emissions come from) ──');
const bucketsA = {};
const bucketsB = {};
for (const s of resolved) {
  bucketsA[classifySignal(s, OPTIONS[0])] = (bucketsA[classifySignal(s, OPTIONS[0])] || []);
  bucketsA[classifySignal(s, OPTIONS[0])].push(s);
  bucketsB[classifySignal(s, OPTIONS[1])] = (bucketsB[classifySignal(s, OPTIONS[1])] || []);
  bucketsB[classifySignal(s, OPTIONS[1])].push(s);
}
const aMicro = (bucketsA['micro-cluster'] || []).length;
const bMicro = (bucketsB['micro-cluster'] || []).length;
console.log(`  Option A micro-cluster signals (70-85¢): ${aMicro}`);
console.log(`  Option B micro-cluster signals (50-85¢): ${bMicro}`);
console.log(`  Net new from widening band:               ${bMicro - aMicro}`);

// What's the WR/return of those NEW (50-70¢) signals specifically?
const newOnly = (bucketsB['micro-cluster'] || []).filter(s => {
  const p = s.avgEntryPrice || 0;
  return p >= 0.50 && p < 0.70;
});
if (newOnly.length > 0) {
  const w = newOnly.filter(s => s.outcome === 'win').length;
  const r = newOnly.reduce((a, s) => a + (typeof s.signalReturn === 'number' ? s.signalReturn : 0), 0);
  const rN = newOnly.filter(s => typeof s.signalReturn === 'number').length;
  console.log(`  Quality of the ${newOnly.length} NEW 50-70¢ signals:  WR=${(w/newOnly.length*100).toFixed(0)}%  AvgRet=${rN > 0 ? (r/rN >= 0 ? '+' : '') + (r/rN).toFixed(1) + '%' : '—'}`);
  console.log('  → If WR/return are positive, Option B is a clean win. If negative, the band is dragging quality.');
}

// ── walletCount distribution of currently-killed candidates ──
console.log('\n  ── Distribution of historical signals by walletCount × entryPrice ──');
const wcBands = [[1,1], [2,2], [3,3], [4,5], [6,7], [8,11], [12,1000]];
const priceBands = [['<30¢', p => p < 0.30], ['30-50¢', p => p >= 0.30 && p < 0.50], ['50-70¢', p => p >= 0.50 && p < 0.70], ['70-85¢', p => p >= 0.70 && p < 0.85], ['≥85¢', p => p >= 0.85]];
console.log('  ' + 'WalletCount'.padEnd(12) + priceBands.map(([l]) => l.padStart(10)).join(' '));
for (const [lo, hi] of wcBands) {
  const label = lo === hi ? String(lo) : `${lo}-${hi === 1000 ? '∞' : hi}`;
  const cells = priceBands.map(([_, pred]) => {
    const matches = resolved.filter(s => (s.walletCount || 0) >= lo && (s.walletCount || 0) <= hi && pred(s.avgEntryPrice || 0));
    if (matches.length === 0) return '—';
    const wins = matches.filter(s => s.outcome === 'win').length;
    const r = matches.reduce((a, s) => a + (typeof s.signalReturn === 'number' ? s.signalReturn : 0), 0);
    const rN = matches.filter(s => typeof s.signalReturn === 'number').length;
    const avgRet = rN > 0 ? (r/rN >= 0 ? '+' : '') + (r/rN).toFixed(0) + '%' : '—';
    return `${matches.length}/${avgRet}`;
  });
  console.log('  ' + label.padEnd(12) + cells.map(c => c.padStart(10)).join(' '));
}
console.log('  Cells: count / avgReturn — empty = "—"');

console.log('\n═══════════════════════════════════════════════════════════════════\n');
