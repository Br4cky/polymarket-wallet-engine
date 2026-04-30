// One-time sweep: recompute every active wallet's score under the V2
// formula (signal-EV-relevance composite). Evict wallets that fall
// below the score floor under the new math.
//
// V2 was validated 2026-04-30 against n=32 calibration wallets:
//   Current formula r vs signal EV:    -0.222
//   V2 formula r vs signal EV:         +0.601
//   Improvement:                       +0.823
//
// V2 replaces decidedROI as the magnitude term with a composite of
// metrics that empirically correlate with signal-emission EV
// (avgEntryPrice, resolvedMarkets, sellRatio) plus an aggressive
// attribution multiplier band [0.0, 2.5] that lets actual signal
// outcomes dominate the score once 2+ signals exist.
//
// Without this sweep, the V2 formula only takes effect on wallets'
// next individual rescore cycle (every 24h-ish per wallet). The sweep
// makes it apply to the whole pool in one pass.
//
// Usage:
//   node scripts/rescore-pool-v2.mjs              # dry-run, report
//   node scripts/rescore-pool-v2.mjs --apply      # write back

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  buildAttributionMap,
  attachAttribution,
} from '../scanner/signalAttribution.js';
import { computeWalletScore } from '../scanner/dataApi.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const APPLY = process.argv.includes('--apply');

const SCORE_FLOOR = 5;     // wallets below this get evicted as "no measurable edge"

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());

const pool = walletsData.pool || walletsData;
const history = Array.isArray(signalsData.history) ? signalsData.history : Object.values(signalsData.history || {});
const attrMap = buildAttributionMap(history);

const allActive = Object.entries(pool).filter(([, w]) => w?.status !== 'removed');

console.log('═'.repeat(78));
console.log('  V2 scoring sweep — 2026-04-30');
console.log('═'.repeat(78));
console.log(`  Active pool size:               ${allActive.length}`);
console.log(`  Signal history (resolved):      ${history.length}`);
console.log(`  Wallets with attribution data:  ${attrMap.size}`);
console.log(`  Mode:                           ${APPLY ? 'APPLY' : 'DRY-RUN'}`);
console.log();

const results = [];

for (const [addr, w] of allActive) {
  const stats = w.stats || {};
  attachAttribution(stats, attrMap, addr);
  const oldScore = w.score || 0;
  const result = computeWalletScore(stats);
  const newScore = result.score ?? 0;
  results.push({ addr, w, oldScore, newScore, components: result.components, reason: result.reason });
}

// Eviction list — wallets below floor
const evictions = results.filter(r => (r.newScore || 0) < SCORE_FLOOR);
const survivors = results.filter(r => (r.newScore || 0) >= SCORE_FLOOR);

// Buckets for distribution
const buckets = [
  ['= 0',   r => r.newScore === 0],
  ['0-5',   r => r.newScore > 0 && r.newScore < 5],
  ['5-15',  r => r.newScore >= 5 && r.newScore < 15],
  ['15-25', r => r.newScore >= 15 && r.newScore < 25],
  ['25-35', r => r.newScore >= 25 && r.newScore < 35],
  ['35-50', r => r.newScore >= 35 && r.newScore < 50],
  ['50+',   r => r.newScore >= 50],
];

console.log('── Score distribution: old → new ──');
console.log('  ' + 'Band'.padEnd(8) + 'Old'.padStart(6) + 'New'.padStart(6));
for (const [lbl, fn] of buckets) {
  const oldFn = (r) => {
    const fnOnNew = fn;
    return fnOnNew({ newScore: r.oldScore });
  };
  const o = results.filter(oldFn).length;
  const n = results.filter(fn).length;
  console.log('  ' + lbl.padEnd(8) + String(o).padStart(6) + String(n).padStart(6));
}

console.log();
console.log('── Eviction breakdown ──');
console.log(`  Score=0 (mean-picker hard reject):     ${evictions.filter(e => e.reason === 'mean_picker').length}`);
console.log(`  Score=0 (other — style=holder/mm):     ${evictions.filter(e => e.newScore === 0 && e.reason !== 'mean_picker').length}`);
console.log(`  Score 0-5 (sub-floor):                 ${evictions.filter(e => e.newScore > 0 && e.newScore < 5).length}`);
console.log(`  TOTAL EVICTED:                         ${evictions.length}`);
console.log(`  Survivors:                             ${survivors.length}`);

console.log();
console.log('── Top 15 score boosts (V2 elevates these) ──');
const boosted = [...survivors].sort((a, b) => (b.newScore - b.oldScore) - (a.newScore - a.oldScore)).slice(0, 15);
for (const r of boosted) {
  const c = r.components || {};
  console.log(`  ${r.addr.slice(0, 12)}  ${r.oldScore.toFixed(1).padStart(5)} → ${r.newScore.toFixed(1).padStart(5)}  style=${(c.style||'?').padEnd(10)} aep=${(c.avgEntryPrice||0).toFixed(2)}  attrMul=${(c.attrMultiplier||0).toFixed(2)} sigs=${c.attrSignals||0}`);
}

console.log();
console.log('── Top 15 score penalties (V2 demotes these) ──');
const penalised = [...survivors].sort((a, b) => (a.newScore - a.oldScore) - (b.newScore - b.oldScore)).slice(0, 15);
for (const r of penalised) {
  const c = r.components || {};
  console.log(`  ${r.addr.slice(0, 12)}  ${r.oldScore.toFixed(1).padStart(5)} → ${r.newScore.toFixed(1).padStart(5)}  style=${(c.style||'?').padEnd(10)} aep=${(c.avgEntryPrice||0).toFixed(2)}  attrMul=${(c.attrMultiplier||0).toFixed(2)} sigs=${c.attrSignals||0}`);
}

console.log();
console.log('── Gate-pass counts under V2 ──');
const gates = [
  ['PER_WALLET_MIN_SCORE >= 15', 15],
  ['MICRO_CLUSTER_MIN_AVG >= 20', 20],
  ['CLUSTER_MIN_PER_WALLET >= 18', 18],
  ['SOLO_MIN_SCORE >= 25', 25],
];
for (const [lbl, t] of gates) {
  const o = results.filter(r => r.oldScore >= t).length;
  const n = results.filter(r => r.newScore >= t).length;
  console.log(`  ${lbl.padEnd(35)} old=${String(o).padStart(4)}  new=${String(n).padStart(4)}`);
}

if (APPLY) {
  for (const r of results) {
    const w = pool[r.addr];
    if (r.newScore < SCORE_FLOOR) {
      w.status = 'removed';
      w.removeReason = 'v2_score_floor';
      w.removeDetail = `oldScore=${r.oldScore.toFixed(1)} newScore=${r.newScore.toFixed(1)} reason=${r.reason}`;
      w.removedAt = new Date().toISOString();
    } else {
      w.score = r.newScore;
      w.scoreComponents = r.components;
      w.lastScored = new Date().toISOString();
    }
  }
  walletsData.pool = pool;
  if (!walletsData.metadata) walletsData.metadata = {};
  walletsData.metadata.lastV2Sweep = new Date().toISOString();
  walletsData.metadata.v2SweepEvicted = evictions.length;
  walletsData.metadata.v2SweepRescored = survivors.length;
  fs.writeFileSync(path.join(ROOT, 'data/wallets.json.gz'),
    zlib.gzipSync(Buffer.from(JSON.stringify(walletsData))));
  console.log(`\n  ✓ Applied — evicted ${evictions.length}, rescored ${survivors.length}`);
} else {
  console.log('\n  Dry-run. Pass --apply to write back.');
}
console.log();
