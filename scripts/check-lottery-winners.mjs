#!/usr/bin/env node
/**
 * Diagnostic — count how many active pool wallets currently fail the
 * lottery-winner gate. Read-only: doesn't evict anything. Use this to
 * preview what evict-lottery-winners.mjs would do, or to check whether
 * the gate is converging the pool to non-lottery wallets over time.
 *
 * Same conditions as the discovery gate and eviction script:
 *   pnlExTop3 < 0
 *   OR top3ConcentrationShare > 0.85
 *   AND resolvedMarkets >= 20
 *
 * Usage:  node scripts/check-lottery-winners.mjs
 */
import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const WALLETS_PATH = path.join(ROOT, 'data/wallets.json.gz');

const MIN_RESOLVED_SAMPLE = 20;
const MAX_PNL_EX_TOP3 = 0;
const MIN_TOP3_FOR_AND = 0.50;
const MAX_TOP3_CONCENTRATION = 0.85;

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(WALLETS_PATH)).toString());
const pool = walletsData.pool || walletsData;
const candidates = Object.entries(pool).filter(([, w]) => w?.status !== 'removed');

let withMetrics = 0;
let failingPnlEx3 = 0;
let failingConcentration = 0;
let failingEither = 0;
let insufficientSample = 0;
let missingMetrics = 0;

const concentrationBuckets = { '0-50': 0, '50-70': 0, '70-85': 0, '85-100': 0, missing: 0 };
const pnlEx3Buckets = { 'negative': 0, '0-1k': 0, '1k+': 0, missing: 0 };

const flagged = [];

for (const [addr, w] of candidates) {
  const s = w.stats || {};
  const resolved = s.resolvedMarkets || 0;

  if (resolved < MIN_RESOLVED_SAMPLE) {
    insufficientSample++;
    continue;
  }

  const pnlEx3 = s.pnlExTop3;
  const conc3 = s.top3ConcentrationShare;

  if (typeof pnlEx3 !== 'number' && typeof conc3 !== 'number') {
    missingMetrics++;
    concentrationBuckets.missing++;
    pnlEx3Buckets.missing++;
    continue;
  }
  withMetrics++;

  // Bucket distributions
  if (typeof conc3 === 'number') {
    const pct = conc3 * 100;
    if (pct < 50) concentrationBuckets['0-50']++;
    else if (pct < 70) concentrationBuckets['50-70']++;
    else if (pct < 85) concentrationBuckets['70-85']++;
    else concentrationBuckets['85-100']++;
  }
  if (typeof pnlEx3 === 'number') {
    if (pnlEx3 < 0) pnlEx3Buckets['negative']++;
    else if (pnlEx3 < 1000) pnlEx3Buckets['0-1k']++;
    else pnlEx3Buckets['1k+']++;
  }

  // Two-tier check (matches scan.js + evict-lottery-winners.mjs):
  //   AND: pnlExTop3 < 0 AND top3 > 50%
  //   OR (extreme): top3 > 85% alone
  const failsAnd = typeof pnlEx3 === 'number' && pnlEx3 < MAX_PNL_EX_TOP3
    && typeof conc3 === 'number' && conc3 > MIN_TOP3_FOR_AND;
  const failsExtreme = typeof conc3 === 'number' && conc3 > MAX_TOP3_CONCENTRATION;

  if (failsAnd) failingPnlEx3++;
  if (failsExtreme) failingConcentration++;
  if (failsAnd || failsExtreme) {
    failingEither++;
    flagged.push({
      addr, score: w.score || 0,
      directionalPnl: s.directionalPnl,
      directionalROI: s.directionalROI,
      pnlExTop3: pnlEx3,
      top3ConcentrationShare: conc3,
      top1ConcentrationShare: s.top1ConcentrationShare,
      medianTradePnL: s.medianTradePnL,
      resolved,
    });
  }
}

console.log('═'.repeat(78));
console.log('  Lottery-winner gate diagnostic');
console.log('═'.repeat(78));
console.log(`  Active pool:                       ${candidates.length}`);
console.log(`  Insufficient sample (<20 resolved): ${insufficientSample}`);
console.log(`  Missing metrics (not yet rescored): ${missingMetrics}`);
console.log(`  Eligible for the gate:              ${withMetrics}`);
console.log();
console.log(`  Of ${withMetrics} eligible:`);
console.log(`    fails AND-tier (pnlEx3<0 & top3>50%):  ${failingPnlEx3}`);
console.log(`    fails extreme tier (top3>85%):         ${failingConcentration}`);
console.log(`    fails either (would evict):            ${failingEither}`);
console.log();
console.log('  Concentration distribution:');
for (const [bucket, n] of Object.entries(concentrationBuckets)) {
  if (bucket === 'missing') continue;
  const pct = withMetrics > 0 ? ((n / withMetrics) * 100).toFixed(1) : '0';
  console.log(`    top3 ${bucket.padEnd(6)}%:  ${String(n).padStart(4)}  (${pct}%)`);
}
console.log();
console.log('  PnL-without-top-3 distribution:');
for (const [bucket, n] of Object.entries(pnlEx3Buckets)) {
  if (bucket === 'missing') continue;
  const pct = withMetrics > 0 ? ((n / withMetrics) * 100).toFixed(1) : '0';
  console.log(`    ${bucket.padEnd(10)}:  ${String(n).padStart(4)}  (${pct}%)`);
}

if (flagged.length > 0) {
  console.log();
  console.log(`  Worst 25 flagged wallets:`);
  flagged.sort((a, b) => {
    const aS = (typeof a.pnlExTop3 === 'number' ? -a.pnlExTop3 : 0)
             + (typeof a.top3ConcentrationShare === 'number' ? a.top3ConcentrationShare * 1000 : 0);
    const bS = (typeof b.pnlExTop3 === 'number' ? -b.pnlExTop3 : 0)
             + (typeof b.top3ConcentrationShare === 'number' ? b.top3ConcentrationShare * 1000 : 0);
    return bS - aS;
  });
  for (const e of flagged.slice(0, 25)) {
    const dirPnl = typeof e.directionalPnl === 'number' ? `$${e.directionalPnl.toFixed(0).padStart(6)}` : '—';
    const dirROI = typeof e.directionalROI === 'number' ? `${(e.directionalROI * 100).toFixed(0)}%` : '—';
    const pnlEx3 = typeof e.pnlExTop3 === 'number' ? `$${e.pnlExTop3.toFixed(0).padStart(6)}` : '—';
    const conc3 = typeof e.top3ConcentrationShare === 'number' ? `${(e.top3ConcentrationShare * 100).toFixed(0)}%` : '—';
    const median = typeof e.medianTradePnL === 'number' ? `$${e.medianTradePnL.toFixed(0)}` : '—';
    console.log(`    ${e.addr.slice(0, 12)}…  score=${e.score.toFixed(1).padStart(5)}  pnl=${dirPnl}  roi=${dirROI.padStart(5)}  pnlEx3=${pnlEx3}  top3=${conc3.padStart(4)}  median=${median}  res=${e.resolved}`);
  }
  if (flagged.length > 25) console.log(`    … +${flagged.length - 25} more`);
}

console.log();
console.log('  Run scripts/evict-lottery-winners.mjs to apply.');
