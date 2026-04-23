// Backfill specialist wallets into the pool.
//
// Identifies wallets that appear in signal history with strong per-category
// or overall attribution but are NOT in the current pool, then runs them
// through the discovery pipeline (fetchAllActivity → analyzeTradeHistory
// → MM classifier → alpha test → attribution → score) and admits them if
// they pass.
//
// Usage:
//   node scripts/backfill-specialists.mjs              # dry-run, reports only
//   node scripts/backfill-specialists.mjs --apply      # rewrite wallets.json.gz
//   node scripts/backfill-specialists.mjs --min-signals 5 --min-ret 10
//
// Flags:
//   --min-signals N       Minimum resolved signals contributed (default 5)
//   --min-ret PCT         Minimum avg signal return % (default 10)
//   --min-wr FRAC         Minimum signal win rate fraction (default 0.60)
//   --limit N             Cap total candidates processed (default 50)
//   --verbose             Print per-wallet stage details
//   --apply               Commit changes to data/wallets.json.gz

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const args = process.argv.slice(2);
const APPLY = args.includes('--apply');
const VERBOSE = args.includes('--verbose');
const get = (f, d) => { const i = args.indexOf(f); return i >= 0 && i + 1 < args.length ? args[i + 1] : d; };

const MIN_SIGNALS = parseInt(get('--min-signals', '5'), 10);
const MIN_RET = parseFloat(get('--min-ret', '10'));     // percent
const MIN_WR = parseFloat(get('--min-wr', '0.60'));
const LIMIT = parseInt(get('--limit', '50'), 10);
const MIN_SCORE_ADMIT = parseFloat(get('--min-score', '10'));

const { fetchAllActivity, analyzeTradeHistory, computeWalletScore } =
  await import(path.join(ROOT, 'scanner/dataApi.js'));
const { attachMMClassification } = await import(path.join(ROOT, 'scanner/mmClassifier.js'));
const { attachAlphaEvaluation } = await import(path.join(ROOT, 'scanner/alphaTest.js'));
const { buildAttributionMap, attachAttribution } = await import(path.join(ROOT, 'scanner/signalAttribution.js'));

// ── Load data ────────────────────────────────────────────────────────────
const walletsFile = path.join(ROOT, 'data/wallets.json.gz');
const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(walletsFile)).toString());
const pool = walletsData.pool || walletsData;

const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');

// ── Build attribution map from signals ───────────────────────────────────
const attrMap = buildAttributionMap(signalsData.history || []);

// ── Identify candidates ──────────────────────────────────────────────────
// Pick wallets with attribution records passing the filter AND not currently
// in pool (either missing entirely or evicted).
const candidates = [];
for (const [addr, a] of attrMap) {
  if (a.signals < MIN_SIGNALS) continue;
  const retPct = a.avgReturn * 100; // attrMap stores as fraction
  if (retPct < MIN_RET) continue;
  if (a.wr < MIN_WR) continue;

  const existing = pool[addr];
  const inPoolActive = existing && existing.status !== 'removed';
  if (inPoolActive) continue;                       // already tracked, skip
  candidates.push({ addr, ...a, retPct, existingStatus: existing?.status || 'missing', existingRemoveReason: existing?.removeReason });
}

candidates.sort((a, b) => b.retPct - a.retPct);
candidates.splice(LIMIT);

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Backfill specialists — admit strong-attribution wallets into pool');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log(`  Criteria:`);
console.log(`    ≥ ${MIN_SIGNALS} resolved signals`);
console.log(`    ≥ ${MIN_RET.toFixed(1)}% avg signal return`);
console.log(`    ≥ ${(MIN_WR * 100).toFixed(0)}% signal win rate`);
console.log(`    ≥ ${MIN_SCORE_ADMIT} computed score after discovery`);
console.log(`    (not already active in pool)\n`);
console.log(`  Candidates: ${candidates.length} (of ${attrMap.size} wallets with signal history)\n`);
if (candidates.length === 0) {
  console.log('  No candidates meeting criteria.\n');
  process.exit(0);
}

// ── Process each candidate ───────────────────────────────────────────────
const admitted = [];
const rejected = [];
let idx = 0;

for (const c of candidates) {
  idx++;
  const shortAddr = c.addr.slice(0, 10);
  const prefix = `  [${idx}/${candidates.length}] ${shortAddr}…`;

  try {
    if (VERBOSE) console.log(`${prefix}  Fetching activity…`);
    const events = await fetchAllActivity(c.addr, { pageLimit: 500, maxPages: 20 });
    if (!events || events.length === 0) {
      rejected.push({ ...c, reason: 'no_activity' });
      console.log(`${prefix}  REJECT — no activity`);
      continue;
    }

    if (VERBOSE) console.log(`${prefix}  ${events.length} events — analyzing…`);
    const stats = analyzeTradeHistory(events);
    attachMMClassification(stats);
    attachAlphaEvaluation(stats);
    attachAttribution(stats, attrMap, c.addr);

    if (stats.isLikelyMM === true) {
      rejected.push({ ...c, reason: 'mm_detected', stats });
      console.log(`${prefix}  REJECT — MM classifier triggered (score ${stats.mmScore})`);
      continue;
    }
    if (stats.isMeanPickerShape === true) {
      rejected.push({ ...c, reason: 'mean_picker' });
      console.log(`${prefix}  REJECT — mean-picker shape`);
      continue;
    }
    if (stats.alphaVerdict === 'fails') {
      rejected.push({ ...c, reason: 'alpha_fails' });
      console.log(`${prefix}  REJECT — alpha test fails`);
      continue;
    }

    const result = computeWalletScore(stats);
    if (!result || result.reason !== 'ok') {
      rejected.push({ ...c, reason: `score_${result?.reason || 'null'}` });
      console.log(`${prefix}  REJECT — score reason=${result?.reason}`);
      continue;
    }
    if (result.score < MIN_SCORE_ADMIT) {
      rejected.push({ ...c, reason: 'score_too_low', score: result.score });
      console.log(`${prefix}  REJECT — score ${result.score} < ${MIN_SCORE_ADMIT}`);
      continue;
    }

    admitted.push({
      addr: c.addr,
      signals: c.signals,
      wr: c.wr,
      retPct: c.retPct,
      score: result.score,
      stats,
      scoreComponents: result.components,
      wasPreviously: c.existingStatus,
    });

    console.log(`${prefix}  ADMIT — score ${result.score} (sigs ${c.signals}, WR ${(c.wr*100).toFixed(0)}%, ret +${c.retPct.toFixed(1)}%, wasPreviously ${c.existingStatus})`);
  } catch (err) {
    rejected.push({ ...c, reason: 'error', error: err.message });
    console.log(`${prefix}  ERROR — ${err.message}`);
  }
}

// ── Summary ──────────────────────────────────────────────────────────────
console.log(`\n  ── Summary ──`);
console.log(`  Admitted: ${admitted.length}`);
console.log(`  Rejected: ${rejected.length}`);
if (rejected.length > 0) {
  const byReason = {};
  for (const r of rejected) byReason[r.reason] = (byReason[r.reason] || 0) + 1;
  console.log(`  Rejection reasons:`);
  for (const [k, v] of Object.entries(byReason)) console.log(`    ${k}: ${v}`);
}

if (APPLY && admitted.length > 0) {
  for (const a of admitted) {
    pool[a.addr] = {
      address: a.addr,
      score: a.score,
      scoreComponents: a.scoreComponents,
      stats: a.stats,
      status: 'active',
      addedAt: new Date().toISOString(),
      addedBy: 'backfill-specialists',
      backfillAttribution: {
        signals: a.signals,
        wr: +a.wr.toFixed(3),
        avgReturn: +(a.retPct / 100).toFixed(3),
      },
    };
  }
  walletsData.pool = pool;
  if (!walletsData.metadata) walletsData.metadata = {};
  walletsData.metadata.lastSpecialistBackfill = new Date().toISOString();
  walletsData.metadata.specialistsAdmitted = (walletsData.metadata.specialistsAdmitted || 0) + admitted.length;
  fs.writeFileSync(walletsFile, zlib.gzipSync(Buffer.from(JSON.stringify(walletsData))));
  console.log(`\n  ✓ Applied — admitted ${admitted.length} wallets to pool\n`);
} else if (APPLY) {
  console.log(`\n  Nothing to admit.\n`);
} else {
  console.log(`\n  Dry-run. Pass --apply to commit.\n`);
}
