// One-shot deep rescore of every active wallet in the pool.
//
// Fetches fresh /activity for each wallet and runs the full Stage 0/1/2
// pipeline (analyzeTradeHistory → attachMMClassification → attachAlphaEvaluation
// → computeWalletScore) so every wallet has populated mmScore, mmPenalty,
// edgePP, alphaVerdict, mergeRate, rebateUsdcTotal, rewardUsdcTotal, etc.
//
// Then applies the same eviction rules as scan.js to kick any wallet that
// now fails the new gates. This collapses the 2.5-day natural rescore
// rotation into a single ~30-minute local run.
//
// What it DOES NOT refresh: Goldsky per-position decidedROI/decidedCapital.
// Those update on their own discovery cycle and change slowly (positions
// are long-lived). The fields we need populated for sourcing gates are
// all analyzer-derived (Stage 0/1/2), not Goldsky-derived.
//
// Usage:
//   node scripts/deep-rescore-pool.mjs            # dry-run: report only
//   node scripts/deep-rescore-pool.mjs --apply    # rewrite wallets.json.gz
//   node scripts/deep-rescore-pool.mjs --apply --verbose
//
// Time: ~2 sec/wallet × ~950 wallets = ~30 min. Can be interrupted and
// resumed — checkpoints every 50 wallets. If interrupted, re-run with
// --resume to pick up where we left off.

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const { fetchAllActivity, analyzeTradeHistory, computeWalletScore } =
  await import(path.join(ROOT, 'scanner/dataApi.js'));
const { attachMMClassification } = await import(path.join(ROOT, 'scanner/mmClassifier.js'));
const { attachAlphaEvaluation } = await import(path.join(ROOT, 'scanner/alphaTest.js'));

const args = process.argv.slice(2);
const APPLY = args.includes('--apply');
const VERBOSE = args.includes('--verbose');
const RESUME = args.includes('--resume');

const THRESHOLDS = {
  MIN_SCORE_POOL: 5,
  NEG_ROI_CAPITAL_FLOOR: 10000,
  NEG_ROI_MIN_RESOLVED: 25,
  DORMANCY_DAYS: 30,
};

// ── Load pool + marketLookup ──────────────────────────────────────────────
const walletsFile = path.join(ROOT, 'data/wallets.json.gz');
const marketsFile = path.join(ROOT, 'data/markets.json.gz');
const checkpointFile = path.join(ROOT, 'data/.deep-rescore-checkpoint.json.gz');

if (!fs.existsSync(walletsFile)) {
  console.error(`Missing ${walletsFile}`);
  process.exit(1);
}
const rawJson = JSON.parse(zlib.gunzipSync(fs.readFileSync(walletsFile)).toString());
const pool = rawJson.pool || rawJson;

let marketLookupObj = {};
if (fs.existsSync(marketsFile)) {
  marketLookupObj = JSON.parse(zlib.gunzipSync(fs.readFileSync(marketsFile)).toString());
}
const marketLookup = new Map(Object.entries(marketLookupObj));

// ── Resume checkpoint support ─────────────────────────────────────────────
let checkpoint = { done: [], started: new Date().toISOString() };
if (RESUME && fs.existsSync(checkpointFile)) {
  try {
    checkpoint = JSON.parse(zlib.gunzipSync(fs.readFileSync(checkpointFile)).toString());
    console.log(`  Resuming — ${checkpoint.done.length} wallets already processed`);
  } catch (e) {
    console.log(`  Checkpoint file corrupt, starting over`);
  }
}
const doneSet = new Set(checkpoint.done);

// ── Identify wallets to rescore ──────────────────────────────────────────
const active = Object.entries(pool).filter(([, w]) => w && w.status !== 'removed');
const todo = active.filter(([addr]) => !doneSet.has(addr));

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Deep rescore — full Stage 0/1/2 refresh per wallet');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log(`  Pool size:    ${Object.keys(pool).length} total (${active.length} active)`);
console.log(`  To rescore:   ${todo.length}${doneSet.size > 0 ? ` (${doneSet.size} skipped — already done)` : ''}`);
console.log(`  Mode:         ${APPLY ? 'APPLY' : 'DRY-RUN'}`);
console.log(`  ETA:          ~${Math.ceil(todo.length * 2 / 60)} minutes at 2s/wallet`);
console.log();

// ── Per-wallet processing ─────────────────────────────────────────────────
const evictions = { mm: [], alpha: [], mean_picker: [], neg_roi: [], dormant: [], low_score: [] };
const fieldUpdates = { populated_mm: 0, populated_alpha: 0, populated_merge: 0 };
let processed = 0;
let errors = 0;
let kept = 0;
const startTs = Date.now();
const now = Math.floor(Date.now() / 1000);

function checkpointSave() {
  const data = zlib.gzipSync(Buffer.from(JSON.stringify(checkpoint)));
  fs.writeFileSync(checkpointFile, data);
}

async function rescoreWallet(addr, wallet) {
  // Consolidation migration (same as scan.js startup)
  if (typeof wallet.scoreV2 === 'number' && typeof wallet.score !== 'number') {
    wallet.score = wallet.scoreV2;
  }
  if (wallet.scoreV2Components && !wallet.scoreComponents) {
    wallet.scoreComponents = wallet.scoreV2Components;
  }
  delete wallet.scoreV2;
  delete wallet.scoreV2Components;
  delete wallet.v2Strikes;
  delete wallet.v2WouldEvict;

  // Fetch fresh /activity — the expensive step
  const events = await fetchAllActivity(addr, { maxEvents: 5000 });
  if (!events || events.length === 0) {
    // Treat no-activity as dormant
    return { evict: { reason: 'dormant', detail: 'no activity found' } };
  }

  // Run analyzer with the full Stage 0+ pipeline
  const freshStats = analyzeTradeHistory(events, { marketLookup });
  if (!freshStats) {
    return { error: 'analyzer returned null' };
  }

  // Merge analyzer output into existing stats — preserve Goldsky-derived
  // fields (decidedROI, decidedCapital, isMeanPickerShape) which the
  // analyzer doesn't compute.
  const oldStats = wallet.stats || {};
  const preservedDecided = {
    decidedROI: oldStats.decidedROI,
    decidedCapital: oldStats.decidedCapital,
    decidedPnl: oldStats.decidedPnl,
    decidedWins: oldStats.decidedWins,
    decidedLosses: oldStats.decidedLosses,
    decidedWinRate: oldStats.decidedWinRate,
    decidedOpenPositions: oldStats.decidedOpenPositions,
    decidedOpenCapitalAtRisk: oldStats.decidedOpenCapitalAtRisk,
    decidedUnredeemedWinsPositions: oldStats.decidedUnredeemedWinsPositions,
    decidedWorthlessLosses: oldStats.decidedWorthlessLosses,
    isMeanPickerShape: oldStats.isMeanPickerShape,
    decidedMeasuredAt: oldStats.decidedMeasuredAt,
    goldskyPnl: oldStats.goldskyPnl,
  };
  const stats = { ...freshStats, ...preservedDecided };

  // Economic-PnL aware effectivePnl (Stage 0)
  stats.effectivePnl = Math.max(
    stats.economicPnl != null ? stats.economicPnl : (stats.totalPnl || 0),
    stats.goldskyPnl || 0
  );

  // Attach MM + alpha classifications
  attachMMClassification(stats);
  attachAlphaEvaluation(stats);

  // Track which new fields got populated
  if (typeof stats.mmScore === 'number') fieldUpdates.populated_mm++;
  if (stats.alphaVerdict) fieldUpdates.populated_alpha++;
  if (typeof stats.mergeRate === 'number') fieldUpdates.populated_merge++;

  // Recompute score
  const scored = computeWalletScore(stats);
  if (scored && scored.score != null) {
    wallet.score = scored.score;
    wallet.scoreComponents = scored.components;
  }

  // Apply gates
  const daysSinceLastTrade = stats.lastTradeTs > 0
    ? (now - stats.lastTradeTs) / 86400 : Infinity;
  const resolved = (stats.decidedWins || 0) + (stats.decidedLosses || 0);

  wallet.stats = stats;
  wallet.lastScored = new Date().toISOString();

  if (stats.isLikelyMM === true) {
    return { evict: { reason: 'mm', detail: `mmScore=${stats.mmScore}/6` } };
  }
  if (stats.alphaVerdict === 'fails') {
    return { evict: { reason: 'alpha', detail: stats.alphaReason } };
  }
  if (stats.isMeanPickerShape === true) {
    return { evict: { reason: 'mean_picker',
             detail: `WR=${((stats.decidedWinRate || 0) * 100).toFixed(0)}% ROI=${((stats.decidedROI || 0) * 100).toFixed(1)}%` } };
  }
  if (stats.decidedROI != null && stats.decidedROI < 0
      && (stats.decidedCapital || 0) >= THRESHOLDS.NEG_ROI_CAPITAL_FLOOR
      && resolved >= THRESHOLDS.NEG_ROI_MIN_RESOLVED) {
    return { evict: { reason: 'neg_roi', detail: `ROI=${(stats.decidedROI * 100).toFixed(1)}% on $${Math.round(stats.decidedCapital).toLocaleString()}` } };
  }
  if (daysSinceLastTrade > THRESHOLDS.DORMANCY_DAYS) {
    return { evict: { reason: 'dormant', detail: `${Math.round(daysSinceLastTrade)}d since last trade` } };
  }
  if (typeof wallet.score === 'number' && wallet.score < THRESHOLDS.MIN_SCORE_POOL) {
    return { evict: { reason: 'low_score', detail: `score=${wallet.score.toFixed(1)}` } };
  }

  return { ok: true };
}

// ── Main loop ─────────────────────────────────────────────────────────────
for (const [addr, wallet] of todo) {
  processed++;
  try {
    const result = await rescoreWallet(addr, wallet);
    if (result.evict) {
      evictions[result.evict.reason].push({ addr, detail: result.evict.detail });
      if (APPLY) {
        wallet.status = 'removed';
        wallet.removeReason = `deep_rescore_${result.evict.reason}`;
        wallet.removeDetail = result.evict.detail;
        wallet.removedAt = new Date().toISOString();
      }
      if (VERBOSE) console.log(`  ✂ ${addr.slice(0, 12)}…  ${result.evict.reason.padEnd(12)}  ${result.evict.detail}`);
    } else if (result.error) {
      errors++;
      if (VERBOSE) console.log(`  ⚠ ${addr.slice(0, 12)}…  error: ${result.error}`);
    } else {
      kept++;
    }
    checkpoint.done.push(addr);
  } catch (err) {
    errors++;
    if (VERBOSE) console.log(`  ⚠ ${addr.slice(0, 12)}…  exception: ${err.message}`);
  }

  // Progress log every 25 wallets
  if (processed % 25 === 0 || processed === todo.length) {
    const elapsed = (Date.now() - startTs) / 1000;
    const rate = processed / elapsed;
    const remaining = todo.length - processed;
    const eta = remaining / rate;
    const totalEvict = Object.values(evictions).reduce((s, arr) => s + arr.length, 0);
    console.log(`  [${processed}/${todo.length}] kept=${kept} evict=${totalEvict} err=${errors}  ${rate.toFixed(2)}/s · ETA ${Math.ceil(eta / 60)}m`);
  }

  // Checkpoint every 50 wallets so a crash doesn't lose progress
  if (processed % 50 === 0) {
    checkpointSave();
    if (APPLY) {
      // Also save the pool incrementally so an interrupted run leaves
      // partial progress persisted in the wallets file
      rawJson.pool = pool;
      fs.writeFileSync(walletsFile, zlib.gzipSync(Buffer.from(JSON.stringify(rawJson))));
    }
  }
}

// ── Final report ──────────────────────────────────────────────────────────
console.log();
console.log('  Field population after rescore:');
console.log(`    mmScore populated:      ${fieldUpdates.populated_mm}/${processed}`);
console.log(`    alphaVerdict populated: ${fieldUpdates.populated_alpha}/${processed}`);
console.log(`    mergeRate populated:    ${fieldUpdates.populated_merge}/${processed}`);
console.log();
console.log('  Eviction counts:');
const reasons = ['mm', 'alpha', 'mean_picker', 'neg_roi', 'dormant', 'low_score'];
let totalEvicted = 0;
for (const r of reasons) {
  const n = evictions[r].length;
  totalEvicted += n;
  console.log(`    ${r.padEnd(12)}  ${String(n).padStart(4)}`);
}
console.log('  ' + '─'.repeat(25));
console.log(`    ${'total'.padEnd(12)}  ${String(totalEvicted).padStart(4)}  (${active.length > 0 ? (totalEvicted / active.length * 100).toFixed(1) : 0}%)`);
console.log();
console.log(`  Processed:  ${processed}`);
console.log(`  Kept:       ${kept}`);
console.log(`  Errors:     ${errors}`);
console.log();

// Preview top evictions per category
for (const r of reasons) {
  if (evictions[r].length === 0) continue;
  console.log(`  ${r} evictions (first 5):`);
  for (const e of evictions[r].slice(0, 5)) {
    console.log(`    ${e.addr.slice(0, 14)}…  ${e.detail}`);
  }
  if (evictions[r].length > 5) console.log(`    … and ${evictions[r].length - 5} more`);
  console.log();
}

if (APPLY) {
  rawJson.pool = pool;
  if (!rawJson.metadata) rawJson.metadata = {};
  rawJson.metadata.lastDeepRescore = new Date().toISOString();
  rawJson.metadata.deepRescoreEvicted = totalEvicted;
  fs.writeFileSync(walletsFile, zlib.gzipSync(Buffer.from(JSON.stringify(rawJson))));
  console.log(`  ✓ Applied — rewrote ${walletsFile}`);

  // Clean up checkpoint file on successful completion
  if (fs.existsSync(checkpointFile)) fs.unlinkSync(checkpointFile);
} else {
  console.log('  Dry-run — pass --apply to commit changes.');
}
console.log();
