// Test the impact of the 150-cap on Gamma resolution by comparing two
// metrics that SHOULD agree for healthy directional wallets:
//
//   decidedROI    — PnL/Capital on RESOLVED positions (uses marketLookup
//                    → cap-affected)
//   singleSideROI — PnL/Capital on directional trades (computed from trade
//                    events alone, no marketLookup needed → cap-immune)
//
// Hypothesis: if the cap is biasing scores, CAPPED wallets should show a
// systematically larger gap (singleSideROI - decidedROI) than UNCAPPED
// wallets. Specifically, capped wallets should under-report decidedROI
// because we miss winning resolutions.
//
// Outputs:
//   - Median (singleSideROI - decidedROI) gap for each cap bucket
//   - Top 20 capped wallets with the biggest gap (likely most under-scored)
//   - Estimate of how much score they'd gain if cap weren't there

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const pool = walletsData.pool || walletsData;

// Pull stats per wallet
const wallets = [];
for (const [addr, w] of Object.entries(pool)) {
  if (!w || typeof w !== 'object' || w.status === 'removed') continue;
  const s = w.stats || {};
  if (s.decidedROI == null || s.singleSideROI == null) continue;  // need both
  wallets.push({
    addr,
    score: w.score || 0,
    openPositions: s.openPositions || s.decidedOpenPositions || 0,
    uniqueMarkets: s.uniqueMarkets || 0,
    decidedROI: s.decidedROI,
    singleSideROI: s.singleSideROI,
    decidedCapital: s.decidedCapital || 0,
    singleSideCapital: s.singleSideCapital || 0,
    gap: s.singleSideROI - s.decidedROI,  // positive gap = decided UNDER-counts
    capRatio: s.decidedCapital > 0 ? s.singleSideCapital / s.decidedCapital : 0,
  });
}

const CAP = 150;
const buckets = [
  ['1-25 open', w => w.openPositions >= 1 && w.openPositions <= 25],
  ['26-150 open (in cap)', w => w.openPositions >= 26 && w.openPositions <= 150],
  ['151-500 open (CAPPED)', w => w.openPositions >= 151 && w.openPositions <= 500],
  ['501-1000 (HEAVY CAP)', w => w.openPositions >= 501 && w.openPositions <= 1000],
  ['1000+ open (EXTREME)', w => w.openPositions > 1000],
];

const med = (arr) => arr.length ? arr.sort((a, b) => a - b)[Math.floor(arr.length / 2)] : null;
const mean = (arr) => arr.length ? arr.reduce((s, x) => s + x, 0) / arr.length : null;

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Cap-impact test: singleSideROI vs decidedROI gap');
console.log('═══════════════════════════════════════════════════════════════════');
console.log('  Wallets analyzed: ' + wallets.length);
console.log('  Hypothesis: if cap is biasing scores, gap should grow with open-position count.\n');

console.log('  ── Gap by open-position bucket ──');
console.log(`  ${'Bucket'.padEnd(28)} ${'N'.padStart(4)}  ${'medROIgap'.padStart(10)}  ${'meanGap'.padStart(8)}  ${'medCapRatio'.padStart(11)}`);
console.log('  ' + '─'.repeat(75));
for (const [label, pred] of buckets) {
  const arr = wallets.filter(pred);
  if (arr.length === 0) {
    console.log('  ' + label.padEnd(28) + ' ' + '—'.padStart(4));
    continue;
  }
  const gaps = arr.map(w => w.gap);
  const ratios = arr.filter(w => w.capRatio > 0).map(w => w.capRatio);
  const medGap = med(gaps.slice());
  const meanGap = mean(gaps);
  const medRatio = med(ratios.slice());
  console.log('  ' + label.padEnd(28) + ' ' + String(arr.length).padStart(4) +
    '  ' + ((medGap >= 0 ? '+' : '') + (medGap * 100).toFixed(1) + 'pp').padStart(10) +
    '  ' + ((meanGap >= 0 ? '+' : '') + (meanGap * 100).toFixed(1) + 'pp').padStart(8) +
    '  ' + (medRatio != null ? medRatio.toFixed(2) + 'x' : '—').padStart(11));
}

console.log('\n  Interpretation:');
console.log('  - medROIgap = median(singleSideROI - decidedROI) per bucket');
console.log('  - Positive = decidedROI is UNDER-counting (wallets look worse than they are)');
console.log('  - medCapRatio = singleSideCapital / decidedCapital');
console.log('  - >1.0x means singleSide capital is larger than decided (cap missing capital)');

// ── Top wallets with biggest under-counting ───────────────────────────
console.log('\n  ── Top 20 wallets where cap appears to most distort score ──');
const distorted = wallets
  .filter(w => w.openPositions > CAP && w.gap > 0.05 && w.singleSideCapital > 5000)
  .sort((a, b) => b.gap - a.gap);
console.log(`  ${'Wallet'.padEnd(14)} ${'Open'.padStart(5)} ${'Score'.padStart(6)} ${'decROI'.padStart(7)} ${'ssROI'.padStart(7)} ${'Gap'.padStart(6)} ${'decCap'.padStart(8)} ${'ssCap'.padStart(8)} ${'Ratio'.padStart(6)}`);
for (const w of distorted.slice(0, 20)) {
  console.log('  ' + w.addr.slice(0, 12).padEnd(14) +
    ' ' + String(w.openPositions).padStart(5) +
    ' ' + w.score.toFixed(1).padStart(6) +
    ' ' + ((w.decidedROI * 100).toFixed(0) + '%').padStart(7) +
    ' ' + ((w.singleSideROI * 100).toFixed(0) + '%').padStart(7) +
    ' ' + ('+' + (w.gap * 100).toFixed(0) + 'pp').padStart(6) +
    ' ' + ('$' + (w.decidedCapital / 1000).toFixed(0) + 'k').padStart(8) +
    ' ' + ('$' + (w.singleSideCapital / 1000).toFixed(0) + 'k').padStart(8) +
    ' ' + (w.capRatio.toFixed(1) + 'x').padStart(6));
}

// ── Score-impact estimate ─────────────────────────────────────────────
console.log('\n  ── Estimated score impact if cap weren\'t there ──');
console.log('  (Assumes uncapped decidedROI ≈ singleSideROI; recomputes roi component)');

function roiPoints(roi) {
  if (roi == null || !isFinite(roi) || roi <= 0) return 0;
  return 50 * (1 - Math.exp(-roi * 3));
}

const shifts = [];
for (const w of distorted) {
  const oldRoiPts = roiPoints(w.decidedROI);
  const newRoiPts = roiPoints(w.singleSideROI);
  const ptsDelta = newRoiPts - oldRoiPts;
  // Score scales linearly with roi component (other multipliers ~constant)
  // Approximate score shift = ptsDelta × (current score / current roi pts)
  const ratio = oldRoiPts > 0 ? w.score / oldRoiPts : 1;
  const estNewScore = w.score + ptsDelta * ratio;
  shifts.push({ ...w, oldRoiPts, newRoiPts, estNewScore, scoreShift: estNewScore - w.score });
}

shifts.sort((a, b) => b.scoreShift - a.scoreShift);
console.log(`  ${'Wallet'.padEnd(14)} ${'Open'.padStart(5)} ${'CurScore'.padStart(8)} ${'EstScore'.padStart(8)} ${'Shift'.padStart(6)}`);
for (const w of shifts.slice(0, 15)) {
  console.log('  ' + w.addr.slice(0, 12).padEnd(14) +
    ' ' + String(w.openPositions).padStart(5) +
    ' ' + w.score.toFixed(1).padStart(8) +
    ' ' + w.estNewScore.toFixed(1).padStart(8) +
    ' ' + ('+' + w.scoreShift.toFixed(1)).padStart(6));
}

// Aggregate estimate
const totalAffected = shifts.length;
const bigShifts = shifts.filter(w => w.scoreShift > 5);
const wouldEnterTop100 = shifts.filter(w => w.estNewScore > 30 && w.score < 30);
console.log('\n  Summary:');
console.log('  - Wallets with score-shift estimate > 5pts: ' + bigShifts.length);
console.log('  - Wallets that would cross score=30 threshold: ' + wouldEnterTop100.length);
console.log('  - (score=30 unlocks SOLO_MIN_SCORE eligibility)\n');
