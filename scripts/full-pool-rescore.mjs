// One-shot: refresh trade-level stats AND positions for every active
// wallet, then re-run the full scoring pipeline (style, attribution,
// V2 score). Replaces the natural rescore loop's 24h-rolling cadence
// with a single pass.
//
// What gets refreshed per wallet:
//   1. /activity events     →  analyzeTradeHistory  →  stats refresh
//      (avgEntryPrice, sellRatio, mmScore, dualSideRate, alphaVerdict,
//       tradesPerActiveWeek, etc. — V2's primary inputs)
//   2. /positions events    →  aggregatePositions   →  decidedROI refresh
//      (kept for diagnostic display; not used in V2 scoring)
//   3. attachAttribution    →  attribution multiplier refresh
//   4. computeWalletScore   →  V2 score with all-fresh inputs
//   5. Bot-pattern check    →  evict any high-frequency drift
//
// Usage:
//   node scripts/full-pool-rescore.mjs              # dry-run
//   node scripts/full-pool-rescore.mjs --apply      # write back
//
// Runtime: ~2 min for 600 active wallets (one /activity + one /positions
// API call each, 150ms/wallet).

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  fetchAllActivity,
  fetchWalletPositions,
  analyzeTradeHistory,
  computeWalletScore,
} from '../scanner/dataApi.js';
import { aggregatePositions } from '../scanner/positionLedger.js';
import {
  buildAttributionMap,
  attachAttribution,
} from '../scanner/signalAttribution.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const APPLY = process.argv.includes('--apply');

const SCORE_FLOOR = 5;
const BOT_TPAW_FLOOR = 700;
const RATE_MS = 150;

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const marketsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/markets.json.gz'))).toString());

const pool = walletsData.pool || walletsData;
const history = Array.isArray(signalsData.history) ? signalsData.history : Object.values(signalsData.history || {});
const marketLookup = new Map(Object.entries(marketsData));
const attrMap = buildAttributionMap(history);

const allActive = Object.entries(pool).filter(([, w]) => w?.status !== 'removed');

console.log('═'.repeat(78));
console.log('  Full-pool rescore — fresh /activity + /positions for every active wallet');
console.log('═'.repeat(78));
console.log(`  Active pool size:               ${allActive.length}`);
console.log(`  Markets in lookup:              ${marketLookup.size}`);
console.log(`  Mode:                           ${APPLY ? 'APPLY' : 'DRY-RUN'}`);
console.log(`  Estimated runtime:              ~${Math.ceil(allActive.length * 2 * RATE_MS / 1000 / 60)} min`);
console.log();

const results = {
  rescored: [],
  evicted_bot: [],
  evicted_score: [],
  evicted_mean_picker: [],
  fetch_failed: [],
  no_events: [],
};

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
let processed = 0;

for (const [addr, w] of allActive) {
  processed++;
  if (processed % 25 === 0 || processed === allActive.length) {
    process.stdout.write(`\r  ${processed}/${allActive.length}  rescored=${results.rescored.length} evictions: bot=${results.evicted_bot.length} score=${results.evicted_score.length} mp=${results.evicted_mean_picker.length} failed=${results.fetch_failed.length}`);
  }

  // 1. Fresh /activity → trade history
  let events;
  try {
    events = await fetchAllActivity(addr);
  } catch (e) {
    results.fetch_failed.push({ addr, err: e.message });
    await sleep(RATE_MS);
    continue;
  }
  if (!Array.isArray(events) || events.length === 0) {
    results.no_events.push({ addr });
    await sleep(RATE_MS);
    continue;
  }

  // 2. Bot-pattern check on fresh events
  const allTrades = events.filter(e => e.type === 'TRADE' && typeof e.timestamp === 'number');
  if (allTrades.length >= 50) {
    const weeks = new Set(allTrades.map(t => Math.floor(t.timestamp / (86400 * 7))));
    if (weeks.size > 0) {
      const tpaw = allTrades.length / weeks.size;
      if (tpaw > BOT_TPAW_FLOOR) {
        results.evicted_bot.push({ addr, tpaw: Math.round(tpaw) });
        await sleep(RATE_MS);
        continue;
      }
    }
  }

  // 3. Run analyzeTradeHistory → fresh stats
  const stats = analyzeTradeHistory(events, { marketLookup });
  if (!stats) {
    results.no_events.push({ addr });
    await sleep(RATE_MS);
    continue;
  }

  // 4. Refresh /positions → decidedROI metrics
  let positions = null;
  try {
    positions = await fetchWalletPositions(addr);
  } catch (e) { /* non-fatal — fall back to stats without decided metrics */ }
  if (Array.isArray(positions) && positions.length > 0) {
    const agg = aggregatePositions(positions, marketLookup);
    if (agg) {
      stats.decidedPnl = agg.decidedPnl;
      stats.decidedCapital = agg.decidedCapital;
      stats.decidedROI = agg.decidedROI;
      stats.decidedWins = agg.wins;
      stats.decidedLosses = agg.losses;
      stats.decidedWinRate = agg.winRate;
      stats.decidedOpenPositions = agg.open;
      stats.decidedOpenCapitalAtRisk = agg.openCapitalAtRisk;
      stats.decidedUnredeemedWinsPositions = agg.unredeemedWins;
      stats.decidedWorthlessLosses = agg.worthlessLosses;
      if (agg.isMeanPickerShape != null) stats.isMeanPickerShape = agg.isMeanPickerShape;
    }
  }

  // 5. Attach attribution + V2 score
  attachAttribution(stats, attrMap, addr);
  const result = computeWalletScore(stats);
  const newScore = result.score ?? 0;
  const oldScore = w.score || 0;

  if (result.reason === 'mean_picker') {
    results.evicted_mean_picker.push({ addr, oldScore });
    await sleep(RATE_MS);
    continue;
  }

  if (newScore < SCORE_FLOOR) {
    results.evicted_score.push({ addr, oldScore, newScore });
    await sleep(RATE_MS);
    continue;
  }

  results.rescored.push({
    addr, oldScore, newScore,
    style: result.components?.style,
    attrMul: result.components?.attrMultiplier,
    attrSigs: result.components?.attrSignals,
    aep: result.components?.avgEntryPrice,
  });

  if (APPLY) {
    w.stats = stats;
    w.score = newScore;
    w.scoreComponents = result.components;
    w.lastScored = new Date().toISOString();
  }

  await sleep(RATE_MS);
}

console.log('\n');
console.log('═'.repeat(78));
console.log('  Full-pool rescore summary');
console.log('═'.repeat(78));
console.log(`  rescored:               ${results.rescored.length}`);
console.log(`  evicted_bot:            ${results.evicted_bot.length}  (tpaw > ${BOT_TPAW_FLOOR})`);
console.log(`  evicted_mean_picker:    ${results.evicted_mean_picker.length}`);
console.log(`  evicted_score (<${SCORE_FLOOR}):    ${results.evicted_score.length}`);
console.log(`  fetch_failed:           ${results.fetch_failed.length}`);
console.log(`  no_events:              ${results.no_events.length}`);
const totalEvicted = results.evicted_bot.length + results.evicted_mean_picker.length + results.evicted_score.length;
console.log(`  TOTAL EVICTED:          ${totalEvicted}`);

console.log();
console.log('── Largest score swings ──');
const swings = [...results.rescored].sort((a, b) => Math.abs(b.newScore - b.oldScore) - Math.abs(a.newScore - a.oldScore)).slice(0, 15);
for (const r of swings) {
  const arrow = r.newScore > r.oldScore ? '↑' : '↓';
  console.log(`  ${r.addr.slice(0, 12)}  ${r.oldScore.toFixed(1).padStart(5)} ${arrow} ${r.newScore.toFixed(1).padStart(5)}  style=${(r.style||'?').padEnd(10)} aep=${(r.aep||0).toFixed(2)}  attrMul=${(r.attrMul||0).toFixed(2)} sigs=${r.attrSigs||0}`);
}

if (APPLY) {
  for (const arr of [results.evicted_bot, results.evicted_mean_picker, results.evicted_score]) {
    for (const ev of arr) {
      const w = pool[ev.addr];
      w.status = 'removed';
      w.removeReason = ev.tpaw ? 'bot_pattern_full_rescore' : (ev === results.evicted_mean_picker[0] || results.evicted_mean_picker.includes(ev) ? 'mean_picker_full_rescore' : 'score_below_floor_full_rescore');
      w.removeDetail = JSON.stringify(ev);
      w.removedAt = new Date().toISOString();
    }
  }
  walletsData.pool = pool;
  if (!walletsData.metadata) walletsData.metadata = {};
  walletsData.metadata.lastFullRescore = new Date().toISOString();
  fs.writeFileSync(path.join(ROOT, 'data/wallets.json.gz'),
    zlib.gzipSync(Buffer.from(JSON.stringify(walletsData))));
  console.log(`\n  ✓ Applied — rescored ${results.rescored.length}, evicted ${totalEvicted}`);
} else {
  console.log('\n  Dry-run. Pass --apply to write back.');
}
console.log();
