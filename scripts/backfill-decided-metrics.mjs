// One-shot backfill: populate decidedROI / decidedCapital / decidedWins /
// decidedLosses for EVERY active pool wallet, regardless of how they
// were originally discovered.
//
// Why this is needed: the cursor-scan discovery path captures positions
// only for wallets it iterates through. Wallets discovered via /activity
// (specialists, attribution-promoted, etc.) and any wallet where the
// per-rescore /positions fetch was skipped or failed end up with
// decidedROI = null. They fall back to singleSideROI × 0.5 in scoring,
// which is structurally noisier and shows up as metricSource='singleside'
// or null on the dashboard.
//
// Pre-sweep (2026-04-30) state: 449 / 732 active wallets had decidedROI
// (61%); 229 fell back to singleSide; 54 had neither.
//
// Usage:
//   node scripts/backfill-decided-metrics.mjs              # dry-run, report only
//   node scripts/backfill-decided-metrics.mjs --apply      # write back
//
// Requires Polymarket data-api access (for /positions). Run locally or
// from CI runner. Per-wallet rate ~150 ms; full sweep of 732 wallets
// finishes in ~2 min.

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';
import { fetchWalletPositions } from '../scanner/dataApi.js';
import { aggregatePositions } from '../scanner/positionLedger.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const APPLY = process.argv.includes('--apply');
const ONLY_NULL = !process.argv.includes('--all');  // default: only wallets missing decidedROI

const RATE_MS = 150;  // delay between API calls

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const marketsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/markets.json.gz'))).toString());

const pool = walletsData.pool || walletsData;
const marketLookup = new Map(Object.entries(marketsData));

const allActive = Object.entries(pool).filter(([, w]) => w?.status !== 'removed');

let targets;
if (ONLY_NULL) {
  // Only the wallets where decidedROI is currently null/undefined
  targets = allActive.filter(([, w]) => w.stats?.decidedROI == null);
} else {
  targets = allActive;
}

console.log('═'.repeat(78));
console.log('  Decided-metrics backfill');
console.log('═'.repeat(78));
console.log(`  Active pool size:        ${allActive.length}`);
console.log(`  Markets in lookup:       ${marketLookup.size}`);
console.log(`  Targets to fetch:        ${targets.length} ${ONLY_NULL ? '(decidedROI == null only)' : '(--all flag)'}`);
console.log(`  Mode:                    ${APPLY ? 'APPLY (writes back wallets.json.gz)' : 'DRY-RUN'}`);
console.log(`  Rate:                    ~${(targets.length * RATE_MS / 1000 / 60).toFixed(1)} min at ${RATE_MS}ms/wallet`);
console.log();

const results = {
  populated: [],
  empty: [],     // wallet returned 0 positions from API
  failed: [],    // fetch error
  no_decided: [],   // positions returned but aggregate had decidedROI null (no decided capital)
};

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
let processed = 0;

for (const [addr, w] of targets) {
  processed++;
  if (processed % 25 === 0 || processed === targets.length) {
    process.stdout.write(`\r  Processed ${processed}/${targets.length}  (populated=${results.populated.length} empty=${results.empty.length} failed=${results.failed.length} no_decided=${results.no_decided.length})`);
  }

  let positions;
  try {
    positions = await fetchWalletPositions(addr);
  } catch (e) {
    results.failed.push({ addr, err: e.message });
    continue;
  }
  if (!Array.isArray(positions) || positions.length === 0) {
    results.empty.push({ addr });
    await sleep(RATE_MS);
    continue;
  }

  const agg = aggregatePositions(positions, marketLookup);
  if (!agg || agg.decidedROI == null) {
    results.no_decided.push({ addr, positions: positions.length, aggCap: agg?.decidedCapital });
    await sleep(RATE_MS);
    continue;
  }

  const decided = {
    decidedPnl: agg.decidedPnl,
    decidedCapital: agg.decidedCapital,
    openCapitalAtRisk: agg.openCapitalAtRisk,
    decidedROI: agg.decidedROI,
    wins: agg.wins,
    losses: agg.losses,
    open: agg.open,
    winRate: agg.winRate,
    unredeemedWins: agg.unredeemedWins,
    worthlessLosses: agg.worthlessLosses,
    isMeanPickerShape: agg.isMeanPickerShape,
  };

  results.populated.push({ addr, decidedROI: agg.decidedROI, decidedCapital: agg.decidedCapital, wins: agg.wins, losses: agg.losses });

  if (APPLY) {
    if (!w.stats) w.stats = {};
    w.stats.decidedPnl = decided.decidedPnl;
    w.stats.decidedCapital = decided.decidedCapital;
    w.stats.decidedROI = decided.decidedROI;
    w.stats.decidedWins = decided.wins;
    w.stats.decidedLosses = decided.losses;
    w.stats.decidedWinRate = decided.winRate;
    w.stats.decidedOpenPositions = decided.open;
    w.stats.decidedOpenCapitalAtRisk = decided.openCapitalAtRisk;
    w.stats.decidedUnredeemedWinsPositions = decided.unredeemedWins;
    w.stats.decidedWorthlessLosses = decided.worthlessLosses;
    if (decided.isMeanPickerShape != null) w.stats.isMeanPickerShape = decided.isMeanPickerShape;
    if (!w.scoreComponents) w.scoreComponents = {};
    w.scoreComponents.metricSource = 'decided';
    w.scoreComponents.roiInput = +agg.decidedROI.toFixed(4);
    w.scoreComponents.capInput = Math.round(agg.decidedCapital);
    w.lastScored = new Date().toISOString();
  }

  await sleep(RATE_MS);
}

console.log('\n');
console.log('═'.repeat(78));
console.log('  Backfill summary');
console.log('═'.repeat(78));
console.log(`  populated:     ${results.populated.length}   (decidedROI now set)`);
console.log(`  no_decided:    ${results.no_decided.length}  (positions returned but no resolved capital — open-only wallet)`);
console.log(`  empty:         ${results.empty.length}      (API returned 0 positions)`);
console.log(`  failed:        ${results.failed.length}     (fetch errored)`);
console.log();

if (results.populated.length > 0) {
  console.log('  Top 10 newly-populated by decidedROI:');
  const top = [...results.populated].sort((a, b) => b.decidedROI - a.decidedROI).slice(0, 10);
  for (const r of top) {
    console.log(`    ${r.addr.slice(0, 12)}  decidedROI=${(r.decidedROI * 100).toFixed(0)}%  cap=$${r.decidedCapital.toFixed(0)}  ${r.wins}W/${r.losses}L`);
  }
}

if (results.failed.length > 0) {
  console.log('\n  First 5 failures:');
  for (const r of results.failed.slice(0, 5)) {
    console.log(`    ${r.addr.slice(0, 12)}  ${r.err}`);
  }
}

if (APPLY && results.populated.length > 0) {
  walletsData.pool = pool;
  if (!walletsData.metadata) walletsData.metadata = {};
  walletsData.metadata.lastDecidedBackfill = new Date().toISOString();
  walletsData.metadata.decidedBackfillCount = results.populated.length;
  fs.writeFileSync(path.join(ROOT, 'data/wallets.json.gz'),
    zlib.gzipSync(Buffer.from(JSON.stringify(walletsData))));
  console.log(`\n  ✓ Applied — wrote decided metrics for ${results.populated.length} wallets`);
} else {
  console.log('\n  Dry-run. Pass --apply to write back.');
}
console.log();
