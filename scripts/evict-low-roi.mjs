// One-shot low-ROI sweep. Evicts pool wallets whose decidedROI falls below
// a threshold, provided they have enough sample (resolvedMarkets) + capital
// to make the signal meaningful.
//
// Sits alongside backfill-pool.mjs (shape-based evictions) and
// deep-rescore-pool.mjs (Stage 0/1/2 classification) as the third
// post-consolidation cleanup. This one specifically targets wallets that
// passed all prior gates but are sitting at chronically low decidedROI —
// typically wallets that entered under pre-consolidation discovery gates
// which didn't check decidedROI at all.
//
// Usage:
//   node scripts/evict-low-roi.mjs                           # dry-run at default 5% floor
//   node scripts/evict-low-roi.mjs --min-roi 0.08            # stricter threshold
//   node scripts/evict-low-roi.mjs --min-roi 0.05 --apply    # actually evict
//   node scripts/evict-low-roi.mjs --verbose                 # per-wallet log

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

const MIN_ROI = parseFloat(get('--min-roi', '0.05'));
const MIN_CAPITAL = parseFloat(get('--min-capital', '3000'));
const MIN_RESOLVED = parseInt(get('--min-resolved', '20'), 10);

// ── Load pool ────────────────────────────────────────────────────────────
const walletsFile = path.join(ROOT, 'data/wallets.json.gz');
if (!fs.existsSync(walletsFile)) {
  console.error(`Missing ${walletsFile}`);
  process.exit(1);
}
const rawJson = JSON.parse(zlib.gunzipSync(fs.readFileSync(walletsFile)).toString());
const pool = rawJson.pool || rawJson;

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Low-ROI sweep — evict chronically underperforming wallets');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log(`  Thresholds:`);
console.log(`    min decidedROI:      ${(MIN_ROI * 100).toFixed(1)}%`);
console.log(`    min decidedCapital:  $${MIN_CAPITAL.toLocaleString()}`);
console.log(`    min resolvedMarkets: ${MIN_RESOLVED}`);
console.log(`  Mode: ${APPLY ? 'APPLY (will rewrite wallets.json.gz)' : 'DRY-RUN'}`);
console.log();

// ── Scan ─────────────────────────────────────────────────────────────────
const active = Object.entries(pool).filter(([, w]) => w && w.status !== 'removed');
const evictions = [];
let skippedSmallSample = 0;
let skippedSmallCap = 0;
let skippedNoRoi = 0;
let skippedHighRoi = 0;

for (const [addr, w] of active) {
  const stats = w.stats;
  if (!stats) { skippedNoRoi++; continue; }

  const roi = stats.decidedROI;
  const cap = stats.decidedCapital || 0;
  const resolved = (stats.decidedWins || 0) + (stats.decidedLosses || 0)
                 || stats.resolvedMarkets || 0;

  if (roi == null) { skippedNoRoi++; continue; }

  // Only evict if we can trust the sample
  if (cap < MIN_CAPITAL) { skippedSmallCap++; continue; }
  if (resolved < MIN_RESOLVED) { skippedSmallSample++; continue; }

  if (roi >= MIN_ROI) { skippedHighRoi++; continue; }

  evictions.push({
    addr,
    roi,
    cap,
    resolved,
    score: w.score,
    wr: stats.decidedWinRate,
  });
}

// Sort worst-first
evictions.sort((a, b) => a.roi - b.roi);

// ── Report ────────────────────────────────────────────────────────────────
console.log(`  Active pool: ${active.length}`);
console.log(`  Evictions:   ${evictions.length}  (${(evictions.length / active.length * 100).toFixed(1)}%)`);
console.log(`  Kept:        ${active.length - evictions.length}`);
console.log();
console.log(`  Skip reasons (would have evicted but didn't):`);
console.log(`    small sample (resolved < ${MIN_RESOLVED}):  ${skippedSmallSample}`);
console.log(`    small capital (cap < $${MIN_CAPITAL}):      ${skippedSmallCap}`);
console.log(`    above ROI threshold:                  ${skippedHighRoi}`);
console.log(`    no decidedROI data:                   ${skippedNoRoi}`);
console.log();

if (evictions.length > 0) {
  console.log(`  Top 10 evictions by worst ROI:`);
  console.log(`    ${'address'.padEnd(14)}  ${'ROI'.padStart(7)}  ${'Capital'.padStart(12)}  ${'N'.padStart(4)}  ${'Score'.padStart(6)}  ${'WR'.padStart(5)}`);
  console.log(`    ${'─'.repeat(70)}`);
  for (const e of evictions.slice(0, 10)) {
    const roiS = (e.roi * 100).toFixed(1) + '%';
    const capS = '$' + Math.round(e.cap).toLocaleString();
    const scoreS = typeof e.score === 'number' ? e.score.toFixed(1) : '—';
    const wrS = typeof e.wr === 'number' ? (e.wr * 100).toFixed(0) + '%' : '—';
    console.log(`    ${e.addr.slice(0, 12)}…  ${roiS.padStart(7)}  ${capS.padStart(12)}  ${String(e.resolved).padStart(4)}  ${scoreS.padStart(6)}  ${wrS.padStart(5)}`);
  }
  if (evictions.length > 10) {
    console.log(`    … and ${evictions.length - 10} more`);
  }
  console.log();

  if (VERBOSE) {
    console.log(`  Full eviction list:`);
    for (const e of evictions) {
      console.log(`    ${e.addr}  ROI=${(e.roi * 100).toFixed(1)}%  cap=$${Math.round(e.cap)}  n=${e.resolved}`);
    }
    console.log();
  }
}

// ── Apply ────────────────────────────────────────────────────────────────
if (APPLY && evictions.length > 0) {
  for (const e of evictions) {
    const w = pool[e.addr];
    w.status = 'removed';
    w.removeReason = 'low_roi_sweep';
    w.removeDetail = `ROI=${(e.roi * 100).toFixed(1)}% < ${(MIN_ROI * 100).toFixed(1)}% floor (cap=$${Math.round(e.cap)}, n=${e.resolved})`;
    w.removedAt = new Date().toISOString();
  }
  rawJson.pool = pool;
  if (!rawJson.metadata) rawJson.metadata = {};
  rawJson.metadata.lastLowRoiSweep = new Date().toISOString();
  rawJson.metadata.lowRoiSweepEvicted = evictions.length;
  rawJson.metadata.lowRoiSweepThreshold = MIN_ROI;
  fs.writeFileSync(walletsFile, zlib.gzipSync(Buffer.from(JSON.stringify(rawJson))));
  console.log(`  ✓ Applied — evicted ${evictions.length} wallets, rewrote ${walletsFile}`);
} else if (APPLY) {
  console.log(`  No evictions — nothing to do.`);
} else {
  console.log(`  Dry-run — pass --apply to commit changes.`);
}
console.log();
