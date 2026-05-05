#!/usr/bin/env node
/**
 * One-shot eviction of MM-style wallets whose cached totalPnl is
 * positive but real (CLOB-resolved diff) PnL is negative. These slipped
 * through evict-negative-pnl.mjs because their cached number is propped
 * up by MERGE-derived revenue (synthetic SELLs from YES+NO pairing
 * arbitrage) that totalPnl currently includes — but a follower copying
 * a BUY can't replicate the MERGE, so MERGE income is wallet-side
 * arbitrage, not edge.
 *
 * The directionalPnl patch will catch this class structurally on the
 * next rescore. This script evicts the specific wallets surfaced by the
 * 2026-05-04 pool-vs-profile diff so we don't have to wait.
 *
 * Usage:  node scripts/evict-mm-stragglers.mjs   [--dry]
 */
import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const WALLETS_PATH = path.join(ROOT, 'data/wallets.json.gz');
const DRY = process.argv.includes('--dry');

// Wallets where diff-pool-vs-profile.mjs (CLOB-resolved) showed real
// PnL < 0 but cached totalPnl > 0. Source: 2026-05-04 pool diff run.
// Address-prefix matched (lowercase, full address required at runtime).
const MM_STRAGGLERS = [
  ['0x6619fda0ac', { realPnl: -7161,  cachedPnl: 3634,  reason: 'mm_straggler: real -$7,161 vs cached +$3,634 (MERGE-inflated)' }],
  ['0x35c8700bdf', { realPnl: -6169,  cachedPnl: 2940,  reason: 'mm_straggler: real -$6,169 vs cached +$2,940 (MERGE-inflated)' }],
  ['0x2c549cef0d', { realPnl: -5473,  cachedPnl: 10730, reason: 'mm_straggler: real -$5,473 vs cached +$10,730 (MERGE-inflated)' }],
  ['0x39201bdb6f', { realPnl: -3109,  cachedPnl: 6597,  reason: 'mm_straggler: real -$3,109 vs cached +$6,597 (MERGE-inflated)' }],
  ['0x0a21d0f5df', { realPnl: -2960,  cachedPnl: 2029,  reason: 'mm_straggler: real -$2,960 vs cached +$2,029 (MERGE-inflated)' }],
  ['0x2f2b6e3176', { realPnl: -2755,  cachedPnl: 4833,  reason: 'mm_straggler: real -$2,755 vs cached +$4,833 (MERGE-inflated)' }],
];

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(WALLETS_PATH)).toString());
const pool = walletsData.pool || walletsData;

const evictions = [];
const notFound = [];
const alreadyRemoved = [];
for (const [prefix, meta] of MM_STRAGGLERS) {
  const match = Object.keys(pool).find(a => a.toLowerCase().startsWith(prefix));
  if (!match) { notFound.push(prefix); continue; }
  const w = pool[match];
  if (w.status === 'removed') { alreadyRemoved.push(match); continue; }
  evictions.push({ addr: match, ...meta, score: w.score || 0, totalPnl: w.stats?.totalPnl });
}

console.log(`Targeted: ${MM_STRAGGLERS.length} addresses`);
console.log(`To evict: ${evictions.length}`);
console.log(`Already removed: ${alreadyRemoved.length}`);
console.log(`Not in pool: ${notFound.length}\n`);

for (const e of evictions) {
  console.log(`  ${e.addr.slice(0, 14)}…  cached=$${e.cachedPnl}  real=$${e.realPnl}  score=${e.score.toFixed(1)}`);
}
if (notFound.length > 0) console.log(`\nNot found prefixes: ${notFound.join(', ')}`);

if (DRY) { console.log('\n--dry mode, no files written'); process.exit(0); }
if (evictions.length === 0) { console.log('\nNothing to evict.'); process.exit(0); }

const now = new Date().toISOString();
for (const e of evictions) {
  const w = pool[e.addr];
  w.status = 'removed';
  w.removeReason = 'mm_straggler';
  w.removeDetail = JSON.stringify({ realPnl: e.realPnl, cachedPnl: e.cachedPnl, score: e.score, source: 'pool-diff-2026-05-04' });
  w.removedAt = now;
}
walletsData.pool = pool;
if (!walletsData.metadata) walletsData.metadata = {};
walletsData.metadata.lastMmStragglerEviction = now;

fs.writeFileSync(WALLETS_PATH, zlib.gzipSync(Buffer.from(JSON.stringify(walletsData))));
console.log(`\n✓ Evicted ${evictions.length} MM stragglers, wrote ${WALLETS_PATH}`);
