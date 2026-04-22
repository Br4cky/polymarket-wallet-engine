// Apply current discovery + eviction gates retroactively to the existing pool.
//
// Problem this solves: when you tighten a gate in scan.js (raise
// MIN_RESOLVED_MARKETS, DISCOVERY_MIN_DECIDED_ROI, etc.), existing pool
// members don't get re-evaluated. They sit below the new thresholds until
// their 24-72h rescore cycle arrives (if it does at all). This script
// applies the current gates as a one-shot sweep.
//
// Gates checked (mirror scan.js CONFIG):
//   1. decidedROI < MIN_ROI                         → evict (low ROI)
//   2. resolvedMarkets < MIN_RESOLVED               → evict (insufficient history)
//   3. decidedCapital < MIN_CAPITAL                 → evict (tiny sample)
//   4. no decidedROI data                           → keep (rescore will populate)
//   5. dormant (last trade > DORMANCY_DAYS)         → evict
//
// Usage:
//   node scripts/apply-gates.mjs                           # dry-run
//   node scripts/apply-gates.mjs --apply                   # commit evictions
//   node scripts/apply-gates.mjs --min-roi 0.10            # override threshold
//   node scripts/apply-gates.mjs --min-resolved 20         # override threshold
//   node scripts/apply-gates.mjs --min-capital 5000        # override threshold
//   node scripts/apply-gates.mjs --verbose                 # per-wallet log

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const args = process.argv.slice(2);
const APPLY = args.includes('--apply');
const VERBOSE = args.includes('--verbose');
const get = (flag, def) => {
  const i = args.indexOf(flag);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
};

// Defaults mirror the NEW (tightened) config in scan.js. Override per run.
const MIN_ROI = parseFloat(get('--min-roi', '0.10'));
const MIN_RESOLVED = parseInt(get('--min-resolved', '20'), 10);
const MIN_CAPITAL = parseFloat(get('--min-capital', '5000'));
const DORMANCY_DAYS = parseInt(get('--dormancy', '30'), 10);

const walletsFile = path.join(ROOT, 'data/wallets.json.gz');
if (!fs.existsSync(walletsFile)) {
  console.error(`Missing ${walletsFile}`);
  process.exit(1);
}
const rawJson = JSON.parse(zlib.gunzipSync(fs.readFileSync(walletsFile)).toString());
const pool = rawJson.pool || rawJson;

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Apply-gates sweep — retroactive cleanup against current thresholds');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log(`  Thresholds:`);
console.log(`    min decidedROI:      ${(MIN_ROI * 100).toFixed(1)}%`);
console.log(`    min resolvedMarkets: ${MIN_RESOLVED}`);
console.log(`    min decidedCapital:  $${MIN_CAPITAL.toLocaleString()}`);
console.log(`    dormancy days:       ${DORMANCY_DAYS}`);
console.log(`  Mode: ${APPLY ? 'APPLY (will rewrite wallets.json.gz)' : 'DRY-RUN'}`);
console.log();

const active = Object.entries(pool).filter(([, w]) => w && w.status !== 'removed');
const evictions = { low_roi: [], low_resolved: [], low_capital: [], dormant: [] };
const skipped = { no_roi: 0, no_stats: 0, kept: 0 };
const now = Math.floor(Date.now() / 1000);

for (const [addr, w] of active) {
  const stats = w.stats;
  if (!stats) { skipped.no_stats++; continue; }

  const roi = stats.decidedROI;
  const cap = stats.decidedCapital || 0;
  const resolved = (stats.decidedWins || 0) + (stats.decidedLosses || 0)
                 || stats.resolvedMarkets || 0;

  // Dormancy — always checked (not gated on sample size)
  const daysSinceLastTrade = stats.lastTradeTs > 0
    ? (now - stats.lastTradeTs) / 86400
    : Infinity;
  if (daysSinceLastTrade > DORMANCY_DAYS) {
    evictions.dormant.push({ addr, detail: `${Math.round(daysSinceLastTrade)}d since last trade` });
    continue;
  }

  // No decidedROI = rescore hasn't populated yet. Leave alone.
  if (roi == null) { skipped.no_roi++; continue; }

  // Check gates in order of severity (one reason per wallet)
  if (cap < MIN_CAPITAL) {
    evictions.low_capital.push({ addr, detail: `cap=$${Math.round(cap).toLocaleString()} < $${MIN_CAPITAL.toLocaleString()}`, roi, resolved });
    continue;
  }
  if (resolved < MIN_RESOLVED) {
    evictions.low_resolved.push({ addr, detail: `resolved=${resolved} < ${MIN_RESOLVED}`, roi, cap });
    continue;
  }
  if (roi < MIN_ROI) {
    evictions.low_roi.push({ addr, detail: `ROI=${(roi * 100).toFixed(1)}% < ${(MIN_ROI * 100).toFixed(0)}%`, cap, resolved });
    continue;
  }

  skipped.kept++;
}

console.log(`  Active pool:        ${active.length}`);
const totalEvict = Object.values(evictions).reduce((s, arr) => s + arr.length, 0);
console.log(`  Would evict:        ${totalEvict}  (${(totalEvict / active.length * 100).toFixed(1)}%)`);
console.log(`  Kept:               ${skipped.kept}`);
console.log(`  Skipped (no ROI yet, await rescore): ${skipped.no_roi}`);
console.log(`  Skipped (no stats): ${skipped.no_stats}`);
console.log();

console.log('  Eviction breakdown:');
for (const [reason, list] of Object.entries(evictions)) {
  console.log(`    ${reason.padEnd(15)}  ${String(list.length).padStart(4)}`);
}
console.log();

for (const [reason, list] of Object.entries(evictions)) {
  if (list.length === 0) continue;
  console.log(`  Top 10 ${reason} evictions:`);
  list.slice(0, 10).forEach(e => {
    console.log(`    ${e.addr.slice(0, 14)}...  ${e.detail}`);
  });
  if (list.length > 10) console.log(`    ... and ${list.length - 10} more`);
  console.log();
  if (VERBOSE) {
    list.forEach(e => console.log(`    ${e.addr}  ${e.detail}`));
    console.log();
  }
}

if (APPLY && totalEvict > 0) {
  for (const [reason, list] of Object.entries(evictions)) {
    for (const e of list) {
      const w = pool[e.addr];
      w.status = 'removed';
      w.removeReason = `apply_gates_${reason}`;
      w.removeDetail = e.detail;
      w.removedAt = new Date().toISOString();
    }
  }
  rawJson.pool = pool;
  if (!rawJson.metadata) rawJson.metadata = {};
  rawJson.metadata.lastApplyGatesSweep = new Date().toISOString();
  rawJson.metadata.applyGatesEvicted = totalEvict;
  rawJson.metadata.applyGatesThresholds = { MIN_ROI, MIN_RESOLVED, MIN_CAPITAL, DORMANCY_DAYS };
  fs.writeFileSync(walletsFile, zlib.gzipSync(Buffer.from(JSON.stringify(rawJson))));
  console.log(`  ✓ Applied — evicted ${totalEvict}, rewrote ${walletsFile}\n`);
} else if (APPLY) {
  console.log(`  No evictions — nothing to do.\n`);
} else {
  console.log(`  Dry-run — pass --apply to commit.\n`);
}
