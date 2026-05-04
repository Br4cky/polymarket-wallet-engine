#!/usr/bin/env node
/**
 * One-shot eviction script for the handpicked pool.
 *
 * Removes wallets that fell out of one of three filters established by the
 * 2026-05-04 triangulation analysis:
 *
 *   1. Hard FLIPPERS  — holdRatio < 50% on the CLOB-resolved exit-style
 *      classifier. Selling before resolution means a follower copying the
 *      BUY can't replicate the exit; structurally unsuitable as alpha.
 *
 *   2. Market-makers / scalpers — wallets touching hundreds of markets
 *      with TOO_FEW resolved-and-classifiable positions and zero
 *      handpicked signals over the last 25 scans. The classifier sees
 *      no exit timing because they exit too fast for /activity polling
 *      to catch a "BUY" worth following.
 *
 *   3. Holders with bad signal economics — structurally fine on holdRatio
 *      but their markets are penny-lottery noise (weather binaries,
 *      sub-minute BTC up/down) or their signal track has collapsed
 *      to a clearly negative expectation.
 *
 * The evicted wallet records are NOT discarded — they're moved to
 * `data/handpicked-wallets-evicted.json.gz` so we can revisit later or
 * re-add a wallet if its numbers recover.
 *
 * Usage:  node scripts/evict-handpicked.mjs   [--dry]
 */
import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const HP_PATH = path.join(ROOT, 'data/handpicked-wallets.json.gz');
const EVICTED_PATH = path.join(ROOT, 'data/handpicked-wallets-evicted.json.gz');
const DRY = process.argv.includes('--dry');

// Address → eviction reason. Lowercase keys for normalised matching.
const EVICTIONS = {
  // Hard flippers (holdRatio < 50%)
  '0xaae9b2c5': { category: 'flipper',     reason: 'holdRatio 35% — sells before resolution on 85/131 positions' },
  '0xaaddfa10': { category: 'flipper',     reason: 'holdRatio 15% — sells before resolution on 10/13 positions' },
  '0xe74d4976': { category: 'flipper',     reason: 'holdRatio 46% MIXED — sells before resolution on 98/191 positions' },

  // Market-makers / scalpers (TOO_FEW classifiable + zero handpicked signals)
  '0xe3726a1b': { category: 'market_maker', reason: '892 markets, 0 REDEEMs, 0 handpicked signals — exits too fast' },
  '0xada20000': { category: 'market_maker', reason: '521 markets, 0 REDEEMs, 0 handpicked signals — scalper pattern' },
  '0xada20056': { category: 'market_maker', reason: '920 markets, 0 REDEEMs, 0 handpicked signals — scalper despite $675k PnL' },

  // Holders with bad signal economics (penny-lottery / signal-track collapse)
  '0x1c0f3e4c': { category: 'noise',        reason: 'weather-binary spammer — 28% lifetime WR, signal track −54% avg / 19 losses' },
  '0x30f91b6d': { category: 'noise',        reason: 'penny-lottery — $570 lifetime PnL on 145 markets, signal track −42% avg' },
  '0x777d9f00': { category: 'collapsed',    reason: 'signal track 2W/8L −75% avg — recent regime change despite +$68k lifetime' },
};

// ── Load current store ────────────────────────────────────────────────
const buf = fs.readFileSync(HP_PATH);
const handpicked = JSON.parse(zlib.gunzipSync(buf).toString());
const wrappedAsObject = !Array.isArray(handpicked);
const list = wrappedAsObject ? (handpicked.wallets || handpicked.list || []) : handpicked;

console.log(`Loaded ${list.length} handpicked wallets from ${HP_PATH}`);

const evictedSet = new Set(Object.keys(EVICTIONS).map(a => a.toLowerCase()));
const matchPrefix = (addr, prefixes) => {
  // Prefix match: '0xaae9b2c5' matches '0xaae9b2c5...'
  const a = addr.toLowerCase();
  for (const p of prefixes) if (a.startsWith(p)) return p;
  return null;
};

const kept = [];
const evicted = [];
for (const w of list) {
  const addr = (w.address || '').toLowerCase();
  const matchedPrefix = matchPrefix(addr, evictedSet);
  if (matchedPrefix) {
    const meta = EVICTIONS[matchedPrefix];
    evicted.push({
      ...w,
      evictedAt: new Date().toISOString(),
      evictionCategory: meta.category,
      evictionReason: meta.reason,
    });
  } else {
    kept.push(w);
  }
}

console.log(`\n=== EVICTION SUMMARY ===`);
console.log(`Keeping: ${kept.length}`);
for (const w of kept) console.log(`  ✓ ${w.address.slice(0, 12)}…  ${(w.notes || '').slice(0, 50)}`);
console.log(`\nEvicting: ${evicted.length}`);
for (const e of evicted) {
  console.log(`  ✗ ${e.address.slice(0, 12)}…  [${e.evictionCategory}]  ${e.evictionReason}`);
}

// Sanity check: every entry in EVICTIONS must have matched something
const matchedPrefixes = new Set(evicted.map(e => matchPrefix(e.address, evictedSet)));
const unmatched = [...evictedSet].filter(p => !matchedPrefixes.has(p));
if (unmatched.length > 0) {
  console.log(`\n⚠️  Eviction list has prefixes that didn't match any wallet:`);
  for (const u of unmatched) console.log(`     ${u}`);
}

if (DRY) {
  console.log('\n--dry mode, no files written');
  process.exit(0);
}

// ── Persist new handpicked store ──────────────────────────────────────
let outHandpicked;
if (wrappedAsObject) {
  outHandpicked = { ...handpicked };
  if (handpicked.wallets) outHandpicked.wallets = kept;
  else if (handpicked.list) outHandpicked.list = kept;
} else {
  outHandpicked = kept;
}
fs.writeFileSync(HP_PATH, zlib.gzipSync(JSON.stringify(outHandpicked)));
console.log(`\nWrote ${HP_PATH} (${kept.length} wallets remain)`);

// ── Append to eviction archive ────────────────────────────────────────
let archive = [];
if (fs.existsSync(EVICTED_PATH)) {
  try {
    archive = JSON.parse(zlib.gunzipSync(fs.readFileSync(EVICTED_PATH)).toString());
    if (!Array.isArray(archive)) archive = [];
  } catch { archive = []; }
}
archive.push(...evicted);
fs.writeFileSync(EVICTED_PATH, zlib.gzipSync(JSON.stringify(archive)));
console.log(`Wrote ${EVICTED_PATH} (${archive.length} total evictions on record)`);
console.log(`\nDone. Re-add a wallet later by editing scripts/add-handpicked.mjs.`);
