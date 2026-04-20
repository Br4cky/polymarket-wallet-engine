// One-shot pool backfill: apply the post-consolidation gates to every
// wallet currently in the pool and evict those that fail.
//
// Why: the existing 1000 wallets were admitted under looser pre-consolidation
// gates. Many are mean-pickers, soft market-makers, dormant, or negative-ROI
// at scale — shapes the new discovery gate would reject at the door. Waiting
// for the 24h rescore rotation to catch them all takes days and leaks stale
// signals in the meantime. This script does the sweep in one pass.
//
// Checks applied (same order as scan.js rescore eviction):
//   1. isLikelyMM (MM classifier score ≥ 4)                  → evict
//   2. alphaVerdict === 'fails'                               → evict
//   3. isMeanPickerShape (WR ≥ 95% + decROI < 5% + $50k cap) → evict
//   4. Neg ROI with $10k+ capital + 25+ resolved              → evict
//   5. Dormant > 30 days                                      → evict
//   6. Computed score < MIN_SCORE_POOL (after penalties)      → evict
//
// Usage:
//   node scripts/backfill-pool.mjs             # dry-run — report only
//   node scripts/backfill-pool.mjs --apply     # actually rewrite wallets.json.gz
//   node scripts/backfill-pool.mjs --verbose   # log each eviction reason

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const { computeWalletScore } = await import(path.join(ROOT, 'scanner/dataApi.js'));
const { attachMMClassification } = await import(path.join(ROOT, 'scanner/mmClassifier.js'));
const { attachAlphaEvaluation } = await import(path.join(ROOT, 'scanner/alphaTest.js'));

const args = process.argv.slice(2);
const APPLY = args.includes('--apply');
const VERBOSE = args.includes('--verbose');

// These mirror scan.js CONFIG. Kept in sync by reference, not by import
// (the scan.js CONFIG object is not exported).
const THRESHOLDS = {
  MIN_SCORE_POOL: 5,                  // post-consolidation pool admission floor
  NEG_ROI_CAPITAL_FLOOR: 10000,
  NEG_ROI_MIN_RESOLVED: 25,
  DORMANCY_DAYS: 30,
};

// ── Load pool ────────────────────────────────────────────────────────────
const walletsFile = path.join(ROOT, 'data/wallets.json.gz');
if (!fs.existsSync(walletsFile)) {
  console.error(`Missing ${walletsFile}`);
  process.exit(1);
}
const rawJson = JSON.parse(zlib.gunzipSync(fs.readFileSync(walletsFile)).toString());
const pool = rawJson.pool || rawJson;

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Pool backfill — one-shot gate sweep against existing wallets');
console.log('═══════════════════════════════════════════════════════════════════\n');
const total = Object.keys(pool).length;
const activeBefore = Object.values(pool).filter(w => w && w.status !== 'removed').length;
console.log(`  Pool size: ${total} total (${activeBefore} active)`);
console.log(`  Mode: ${APPLY ? 'APPLY (will rewrite wallets.json.gz)' : 'DRY-RUN (no writes)'}\n`);

// ── Process ──────────────────────────────────────────────────────────────
const evictions = { mm: [], alpha: [], mean_picker: [], neg_roi: [], dormant: [], low_score: [], no_decided: [] };
let kept = 0;
let alreadyRemoved = 0;
const now = Math.floor(Date.now() / 1000);

for (const [addr, wallet] of Object.entries(pool)) {
  if (!wallet || typeof wallet !== 'object') continue;
  if (wallet.status === 'removed') { alreadyRemoved++; continue; }

  // Consolidation migration — inherit scoreV2 → score if the file predates
  // the unified schema. scan.js does this on startup; the backfill needs it
  // too so we score against the authoritative number.
  if (typeof wallet.scoreV2 === 'number' && typeof wallet.score !== 'number') {
    wallet.score = wallet.scoreV2;
  }
  if (wallet.scoreV2Components && !wallet.scoreComponents) {
    wallet.scoreComponents = wallet.scoreV2Components;
  }

  const stats = wallet.stats;
  if (!stats) {
    // No stats — can't evaluate, skip (will be rescored on next cycle)
    kept++;
    continue;
  }

  // Attach fresh MM + alpha verdicts using current stats
  attachMMClassification(stats);
  attachAlphaEvaluation(stats);

  // Recompute score under the unified formula so the low-score gate uses
  // the current penalty stack, not whatever was stored from an older run.
  const scored = computeWalletScore(stats);
  if (scored && scored.score != null) {
    wallet.score = scored.score;
    wallet.scoreComponents = scored.components;
  }

  // ── Gates, ordered from highest to lowest confidence ──────────────────
  const daysSinceLastTrade = stats.lastTradeTs > 0 ? (now - stats.lastTradeTs) / 86400 : Infinity;
  const resolved = (stats.decidedWins || 0) + (stats.decidedLosses || 0);

  let evict = null;

  if (stats.isLikelyMM === true) {
    evict = { reason: 'mm', detail: `mmScore=${stats.mmScore}/6` };
  } else if (stats.alphaVerdict === 'fails') {
    evict = { reason: 'alpha', detail: `edgePP=${stats.edgePP ?? 'null'}pp, ${stats.alphaReason}` };
  } else if (stats.isMeanPickerShape === true) {
    evict = { reason: 'mean_picker',
              detail: `WR=${((stats.decidedWinRate || 0) * 100).toFixed(0)}% ROI=${((stats.decidedROI || 0) * 100).toFixed(1)}% cap=$${Math.round(stats.decidedCapital || 0).toLocaleString()}` };
  } else if (stats.decidedROI != null && stats.decidedROI < 0
             && (stats.decidedCapital || 0) >= THRESHOLDS.NEG_ROI_CAPITAL_FLOOR
             && resolved >= THRESHOLDS.NEG_ROI_MIN_RESOLVED) {
    evict = { reason: 'neg_roi', detail: `ROI=${(stats.decidedROI * 100).toFixed(1)}% on $${Math.round(stats.decidedCapital).toLocaleString()}` };
  } else if (daysSinceLastTrade > THRESHOLDS.DORMANCY_DAYS) {
    evict = { reason: 'dormant', detail: `${Math.round(daysSinceLastTrade)}d since last trade` };
  } else if (typeof wallet.score === 'number' && wallet.score < THRESHOLDS.MIN_SCORE_POOL) {
    evict = { reason: 'low_score', detail: `score=${wallet.score.toFixed(1)} < ${THRESHOLDS.MIN_SCORE_POOL}` };
  } else if (scored.score == null) {
    // Wallet has stats but score came back null — usually missing decided metrics.
    // Don't evict; these populate on next rescore cycle.
    evict = null;
  }

  if (evict) {
    evictions[evict.reason].push({ addr, detail: evict.detail });
    if (APPLY) {
      wallet.status = 'removed';
      wallet.removeReason = `backfill_${evict.reason}`;
      wallet.removeDetail = evict.detail;
      wallet.removedAt = new Date().toISOString();
    }
    if (VERBOSE) {
      console.log(`  ✂ ${addr.slice(0, 12)}…  ${evict.reason.padEnd(12)}  ${evict.detail}`);
    }
  } else {
    kept++;
  }
}

// ── Report ────────────────────────────────────────────────────────────────
console.log();
console.log('  Eviction counts:');
const reasons = ['mm', 'alpha', 'mean_picker', 'neg_roi', 'dormant', 'low_score'];
let totalEvicted = 0;
for (const r of reasons) {
  const n = evictions[r].length;
  totalEvicted += n;
  const pct = activeBefore > 0 ? (n / activeBefore * 100).toFixed(1) : '0.0';
  console.log(`    ${r.padEnd(12)}  ${String(n).padStart(4)}  (${pct}% of active)`);
}
console.log('  ' + '─'.repeat(30));
console.log(`    ${'total'.padEnd(12)}  ${String(totalEvicted).padStart(4)}  (${activeBefore > 0 ? (totalEvicted / activeBefore * 100).toFixed(1) : '0.0'}%)`);
console.log();
console.log(`  Kept:     ${kept}`);
console.log(`  Evicted:  ${totalEvicted}`);
console.log(`  Already removed (skipped): ${alreadyRemoved}`);
console.log();

// Preview top evictions per category
for (const r of reasons) {
  if (evictions[r].length === 0) continue;
  console.log(`  Sample ${r} evictions (first 5):`);
  for (const e of evictions[r].slice(0, 5)) {
    console.log(`    ${e.addr.slice(0, 14)}…  ${e.detail}`);
  }
  if (evictions[r].length > 5) console.log(`    … and ${evictions[r].length - 5} more`);
  console.log();
}

if (APPLY) {
  // Update metadata + rewrite file
  rawJson.pool = pool;
  if (rawJson.metadata) {
    rawJson.metadata.lastBackfill = new Date().toISOString();
    rawJson.metadata.backfillEvicted = totalEvicted;
  }
  const out = zlib.gzipSync(Buffer.from(JSON.stringify(rawJson)));
  fs.writeFileSync(walletsFile, out);
  console.log(`  ✓ Applied — rewrote ${walletsFile}`);
  console.log(`    Pool now: ${kept} active (was ${activeBefore})`);
  console.log();
} else {
  console.log('  Dry-run — no file modified. Pass --apply to commit.');
  console.log();
}
