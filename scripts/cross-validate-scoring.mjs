// Cross-validation pipeline — Stage 4.
//
// Offline sanity check on the current pool's top-ranked wallets. Catches
// scoring regressions that would otherwise drift unnoticed between scan
// cycles.
//
// What it checks:
//   1. Every Tier-A wallet (score ≥ 50) should pass the MM classifier.
//      If any top-ranked wallet is also flagged as isLikelyMM, the
//      penalty isn't being applied — alert and list them.
//   2. Every Tier-A wallet should pass the alpha test (edgePP ≥ 1.5pp)
//      OR be in a "can't-yet-evaluate" verdict (insufficient_sample/capital).
//      Hard failure (alphaVerdict === 'fails') at the top is a red flag.
//   3. Spearman correlation between score and decidedROI across the full
//      pool — should be positive (target ≥ 0.5). Running baseline.
//   4. Evict-rank sanity: bottom 10% by score should have low/negative
//      decidedROI median. If not, scoring is upside-down somewhere.
//
// Output: prints to stdout + writes a dated report to out/validation/.
// Exit code 0 if all checks pass or are in "warning" band; 1 if any hard
// failure is detected.
//
// Usage:
//   node scripts/cross-validate-scoring.mjs
//   node scripts/cross-validate-scoring.mjs --top 50
//   node scripts/cross-validate-scoring.mjs --json   # machine-readable output

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const { classifyMarketMaker } = await import(path.join(ROOT, 'scanner/mmClassifier.js'));
const { evaluateAlpha } = await import(path.join(ROOT, 'scanner/alphaTest.js'));

const args = process.argv.slice(2);
const get = (flag, def) => {
  const i = args.indexOf(flag);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
};
const has = (flag) => args.includes(flag);

const TOP_N = parseInt(get('--top', '50'), 10);
const JSON_OUT = has('--json');
const TIER_A_CUTOFF = 50;
const TARGET_SPEARMAN = 0.5;

// ── Load pool ───────────────────────────────────────────────────────────────
const walletsFile = path.join(ROOT, 'data/wallets.json.gz');
if (!fs.existsSync(walletsFile)) {
  console.error(`Missing ${walletsFile}. Has a scan run yet?`);
  process.exit(1);
}
const raw = JSON.parse(zlib.gunzipSync(fs.readFileSync(walletsFile)).toString());
const pool = raw.pool || raw;

// Mirror scan.js's consolidation migration: if the JSON predates the
// V1/V2 consolidation, the authoritative value lives in scoreV2. Promote
// it to .score here so downstream analysis reads the right field.
// Once scan.js has run post-consolidation at least once, .scoreV2 stops
// being written and this migration becomes a no-op.
for (const w of Object.values(pool)) {
  if (w && typeof w.scoreV2 === 'number') w.score = w.scoreV2;
}

const allWallets = Object.values(pool).filter(w => w && w.address && w.status !== 'removed');
const ranked = allWallets
  .filter(w => typeof w.score === 'number')
  .sort((a, b) => b.score - a.score);

if (ranked.length === 0) {
  console.error(`No wallets in pool have score populated. Cannot validate.`);
  process.exit(1);
}

// ── Spearman correlation (score, decidedROI) ──────────────────────────────
function spearman(xs, ys) {
  if (xs.length !== ys.length || xs.length < 2) return null;
  const rank = (arr) => {
    const sorted = arr.map((v, i) => ({ v, i })).sort((a, b) => a.v - b.v);
    const ranks = new Array(arr.length);
    for (let i = 0; i < sorted.length; i++) ranks[sorted[i].i] = i + 1;
    return ranks;
  };
  const rx = rank(xs), ry = rank(ys);
  const n = xs.length;
  const meanRx = rx.reduce((s, v) => s + v, 0) / n;
  const meanRy = ry.reduce((s, v) => s + v, 0) / n;
  let num = 0, dx = 0, dy = 0;
  for (let i = 0; i < n; i++) {
    num += (rx[i] - meanRx) * (ry[i] - meanRy);
    dx += (rx[i] - meanRx) ** 2;
    dy += (ry[i] - meanRy) ** 2;
  }
  return num / Math.sqrt(dx * dy);
}

// ── Checks ──────────────────────────────────────────────────────────────────
const tierA = ranked.filter(w => (w.score || 0) >= TIER_A_CUTOFF);
const topN = ranked.slice(0, TOP_N);

// Check 1: MM classifier disagreements at Tier A
const mmDisagreements = [];
for (const w of tierA) {
  const stats = w.stats || {};
  const mm = classifyMarketMaker(stats);
  if (mm.isLikelyMM) {
    mmDisagreements.push({
      address: w.address,
      score: w.score,
      mmScore: mm.score,
      mmReason: mm.reason,
      triggered: mm.signals.filter(s => s.triggered).map(s => s.name),
    });
  }
}

// Check 2: Alpha test disagreements at Tier A
const alphaFailures = [];
for (const w of tierA) {
  const stats = w.stats || {};
  const alpha = evaluateAlpha(stats);
  if (alpha.verdict === 'fails') {
    alphaFailures.push({
      address: w.address,
      score: w.score,
      edgePP: alpha.edgePP,
      reason: alpha.reason,
    });
  }
}

// Check 3: Spearman on the full scorable pool
const scores = [];
const decidedROIs = [];
for (const w of ranked) {
  if (w.stats?.decidedROI != null) {
    scores.push(w.score);
    decidedROIs.push(w.stats.decidedROI);
  }
}
const corr = spearman(scores, decidedROIs);

// Check 4: bottom-decile sanity — median decidedROI should not exceed top-decile
const withROI = ranked.filter(w => w.stats?.decidedROI != null);
const decileSize = Math.max(1, Math.floor(withROI.length / 10));
const topDecile = withROI.slice(0, decileSize);
const bottomDecile = withROI.slice(-decileSize);
const med = (arr) => {
  if (!arr.length) return null;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
};
const topDecileMedROI = med(topDecile.map(w => w.stats.decidedROI));
const bottomDecileMedROI = med(bottomDecile.map(w => w.stats.decidedROI));
const bandsInverted = topDecileMedROI != null && bottomDecileMedROI != null &&
  bottomDecileMedROI >= topDecileMedROI;

// ── Summarise ───────────────────────────────────────────────────────────────
const report = {
  runAt: new Date().toISOString(),
  poolSize: allWallets.length,
  scoreableCount: ranked.length,
  tierACount: tierA.length,
  topN: topN.length,
  checks: {
    mmDisagreementsAtTierA: {
      count: mmDisagreements.length,
      severity: mmDisagreements.length === 0 ? 'ok' : 'fail',
      items: mmDisagreements,
    },
    alphaFailuresAtTierA: {
      count: alphaFailures.length,
      severity: alphaFailures.length === 0 ? 'ok' : 'warn',
      items: alphaFailures,
    },
    spearmanScoreV2VsDecidedROI: {
      value: corr,
      target: TARGET_SPEARMAN,
      severity: corr == null ? 'skip'
              : corr >= TARGET_SPEARMAN ? 'ok'
              : corr >= 0.3 ? 'warn'
              : 'fail',
      sampleSize: scores.length,
    },
    decileBandSanity: {
      topDecileMedROI,
      bottomDecileMedROI,
      inverted: bandsInverted,
      severity: bandsInverted ? 'fail' : 'ok',
      sampleSize: withROI.length,
    },
  },
};

const hardFailures =
  (report.checks.mmDisagreementsAtTierA.severity === 'fail' ? 1 : 0) +
  (report.checks.spearmanScoreV2VsDecidedROI.severity === 'fail' ? 1 : 0) +
  (report.checks.decileBandSanity.severity === 'fail' ? 1 : 0);

// ── Output ──────────────────────────────────────────────────────────────────
if (JSON_OUT) {
  process.stdout.write(JSON.stringify(report, null, 2) + '\n');
} else {
  console.log('\n═══════════════════════════════════════════════════════════════════');
  console.log('  Cross-validation report');
  console.log('═══════════════════════════════════════════════════════════════════\n');
  console.log(`  Pool size: ${report.poolSize}   Scoreable: ${report.scoreableCount}   Tier A: ${report.tierACount}\n`);

  const icon = (sev) => sev === 'ok' ? '✓' : sev === 'warn' ? '⚠' : sev === 'fail' ? '✗' : '—';

  const c1 = report.checks.mmDisagreementsAtTierA;
  console.log(`  ${icon(c1.severity)}  MM classifier disagreements at Tier A: ${c1.count}`);
  if (c1.items.length > 0) {
    for (const x of c1.items.slice(0, 5)) {
      console.log(`       ${x.address.slice(0, 12)}…  score=${x.score}  mm=${x.mmScore}/6  ${x.triggered.join(', ')}`);
    }
    if (c1.items.length > 5) console.log(`       … and ${c1.items.length - 5} more`);
  }

  const c2 = report.checks.alphaFailuresAtTierA;
  console.log(`\n  ${icon(c2.severity)}  Alpha test failures at Tier A: ${c2.count}`);
  if (c2.items.length > 0) {
    for (const x of c2.items.slice(0, 5)) {
      console.log(`       ${x.address.slice(0, 12)}…  score=${x.score}  edgePP=${x.edgePP}  ${x.reason}`);
    }
    if (c2.items.length > 5) console.log(`       … and ${c2.items.length - 5} more`);
  }

  const c3 = report.checks.spearmanScoreV2VsDecidedROI;
  console.log(`\n  ${icon(c3.severity)}  Spearman(score, decidedROI): ${c3.value != null ? c3.value.toFixed(3) : 'n/a'} (target ≥ ${c3.target}, n=${c3.sampleSize})`);

  const c4 = report.checks.decileBandSanity;
  console.log(`\n  ${icon(c4.severity)}  Decile ROI bands: top=${c4.topDecileMedROI != null ? (c4.topDecileMedROI * 100).toFixed(1) + '%' : 'n/a'}, bottom=${c4.bottomDecileMedROI != null ? (c4.bottomDecileMedROI * 100).toFixed(1) + '%' : 'n/a'}${c4.inverted ? '  ← INVERTED!' : ''}`);

  console.log(`\n  Summary: ${hardFailures === 0 ? '✓ all checks passing' : `✗ ${hardFailures} hard failure${hardFailures > 1 ? 's' : ''} — investigate before pushing`}\n`);
}

// ── Persist dated report ────────────────────────────────────────────────────
const outDir = path.join(ROOT, 'out/validation');
fs.mkdirSync(outDir, { recursive: true });
const today = new Date().toISOString().slice(0, 10);
const outFile = path.join(outDir, `cross-validate-${today}.json`);
fs.writeFileSync(outFile, JSON.stringify(report, null, 2));
if (!JSON_OUT) console.log(`  Report written: ${outFile}\n`);

process.exit(hardFailures > 0 ? 1 : 0);
