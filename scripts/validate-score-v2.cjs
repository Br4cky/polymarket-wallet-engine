#!/usr/bin/env node
/**
 * Phase 3 validator: measure Spearman rank correlation between
 * shadow V2 score and ground-truth decidedROI across the live pool.
 *
 * Data source: data/wallets.json.gz (the live pool, written every scan).
 * Reads each pool member's wallet.scoreV2 and wallet.stats.decidedROI
 * (both populated by the Phase 1+2 rescore pass). Skips wallets that
 * haven't been rescored since the feature flag rolled out (scoreV2 null
 * or decidedROI null).
 *
 * Targets, per SCORING-REDESIGN.md validation plan:
 *   Spearman(scoreV2, decidedROI) ≥ 0.5    ← primary gate for Phase 5
 *   Spearman(legacyScore, decidedROI) < 0  ← confirms legacy is inverted
 *
 * Also reports:
 *   - How many wallets are eligible (have both shadow fields)
 *   - Score-band breakdown (decile buckets by V2) with mean decidedROI
 *   - Mean-picker flag coverage and their score distribution
 *
 * Usage: node scripts/validate-score-v2.cjs
 *        node scripts/validate-score-v2.cjs --csv > v2-validation.csv
 */

const fs = require('fs');
const zlib = require('zlib');
const path = require('path');

const POOL_PATH = path.join(__dirname, '..', 'data', 'wallets.json.gz');
const WANT_CSV = process.argv.includes('--csv');

function loadPool(p) {
  const raw = JSON.parse(zlib.gunzipSync(fs.readFileSync(p)));
  return raw.pool || raw;
}

// Spearman = Pearson on ranks. Ties get average rank.
function rank(values) {
  const indexed = values.map((v, i) => [v, i]);
  indexed.sort((a, b) => a[0] - b[0]);
  const ranks = new Array(values.length);
  let i = 0;
  while (i < indexed.length) {
    let j = i;
    while (j + 1 < indexed.length && indexed[j + 1][0] === indexed[i][0]) j++;
    const avgRank = (i + j) / 2 + 1; // 1-indexed average
    for (let k = i; k <= j; k++) ranks[indexed[k][1]] = avgRank;
    i = j + 1;
  }
  return ranks;
}

function pearson(xs, ys) {
  const n = xs.length;
  if (n < 3) return null;
  const mx = xs.reduce((a, b) => a + b, 0) / n;
  const my = ys.reduce((a, b) => a + b, 0) / n;
  let num = 0, dx2 = 0, dy2 = 0;
  for (let i = 0; i < n; i++) {
    const dx = xs[i] - mx;
    const dy = ys[i] - my;
    num += dx * dy;
    dx2 += dx * dx;
    dy2 += dy * dy;
  }
  const denom = Math.sqrt(dx2 * dy2);
  return denom > 0 ? num / denom : null;
}

function spearman(xs, ys) {
  return pearson(rank(xs), rank(ys));
}

function main() {
  const pool = loadPool(POOL_PATH);
  const addrs = Object.keys(pool);
  console.error(`Loaded ${addrs.length} wallets from pool`);

  const rows = [];
  let skipNoV2 = 0, skipNoROI = 0, skipRemoved = 0;

  for (const addr of addrs) {
    const w = pool[addr];
    if (!w) continue;
    if (w.status === 'removed') { skipRemoved++; continue; }
    const legacy = typeof w.score === 'number' ? w.score : null;
    const v2 = typeof w.scoreV2 === 'number' ? w.scoreV2 : null;
    const roi = w.stats && typeof w.stats.decidedROI === 'number' ? w.stats.decidedROI : null;
    if (v2 == null) { skipNoV2++; continue; }
    if (roi == null) { skipNoROI++; continue; }
    rows.push({
      addr,
      legacy,
      v2,
      roi,
      decidedCapital: w.stats.decidedCapital ?? null,
      resolved: (w.stats.decidedWins ?? 0) + (w.stats.decidedLosses ?? 0),
      isMeanPicker: w.stats.isMeanPickerShape === true,
      lastScored: w.lastScored || null,
      components: w.scoreV2Components || null,
    });
  }

  console.error(`Eligible (has scoreV2 AND decidedROI): ${rows.length}`);
  console.error(`Skipped: ${skipRemoved} removed, ${skipNoV2} no scoreV2 yet, ${skipNoROI} no decidedROI`);

  if (rows.length < 10) {
    console.error('\n⚠ Not enough eligible wallets yet. The scanner rescore pass');
    console.error('  needs more cycles to populate shadow data across the pool.');
    console.error('  At RESCORE_BATCH_SIZE=100 and DISCOVERY_INTERVAL_SCANS=3,');
    console.error('  the full pool is measured every ~17 scans.');
    process.exit(0);
  }

  if (WANT_CSV) {
    console.log('address,legacy,v2,decidedROI,decidedCapital,resolved,isMeanPicker,lastScored');
    for (const r of rows) {
      console.log([
        r.addr, r.legacy ?? '', r.v2, r.roi.toFixed(4),
        r.decidedCapital ?? '', r.resolved, r.isMeanPicker ? 1 : 0,
        r.lastScored ?? '',
      ].join(','));
    }
    return;
  }

  // Filter out extreme ROI outliers for cleaner correlation
  // (leaves head and tail trimming to reporting)
  const v2s = rows.map(r => r.v2);
  const rois = rows.map(r => r.roi);
  const legacies = rows.filter(r => r.legacy != null).map(r => r.legacy);
  const roisForLegacy = rows.filter(r => r.legacy != null).map(r => r.roi);

  const sp_v2 = spearman(v2s, rois);
  const sp_legacy = spearman(legacies, roisForLegacy);

  console.log('\n=== Rank correlations vs ground-truth decidedROI ===');
  console.log(`  Spearman(scoreV2,     decidedROI) = ${sp_v2?.toFixed(3) ?? 'n/a'}   target ≥ 0.50`);
  console.log(`  Spearman(legacyScore, decidedROI) = ${sp_legacy?.toFixed(3) ?? 'n/a'}   baseline (was -0.152)`);

  if (sp_v2 != null && sp_v2 >= 0.5) {
    console.log(`  ✓ PASS — V2 meets target, safe to promote to primary in Phase 5`);
  } else if (sp_v2 != null && sp_v2 > 0 && sp_legacy != null && sp_v2 > sp_legacy) {
    console.log(`  ~ PARTIAL — V2 beats legacy but below 0.5 target, tune before promotion`);
  } else {
    console.log(`  ✗ FAIL — V2 not tracking truth; revisit formula weights or shadow-data coverage`);
  }

  // Score-band breakdown: quintiles of V2, mean decidedROI per bucket.
  // If V2 is ranking correctly, mean ROI should rise monotonically.
  const sorted = [...rows].sort((a, b) => a.v2 - b.v2);
  const bucketCount = 5;
  const bucketSize = Math.floor(sorted.length / bucketCount);
  console.log('\n=== V2 quintile → mean decidedROI ===');
  for (let i = 0; i < bucketCount; i++) {
    const lo = i * bucketSize;
    const hi = i === bucketCount - 1 ? sorted.length : (i + 1) * bucketSize;
    const slice = sorted.slice(lo, hi);
    if (slice.length === 0) continue;
    const meanROI = slice.reduce((a, r) => a + r.roi, 0) / slice.length;
    const medROI = slice.map(r => r.roi).sort((a, b) => a - b)[Math.floor(slice.length / 2)];
    const v2Lo = slice[0].v2;
    const v2Hi = slice[slice.length - 1].v2;
    const mpCount = slice.filter(r => r.isMeanPicker).length;
    console.log(`  Q${i + 1} (n=${slice.length}, v2 ${v2Lo.toFixed(1)}–${v2Hi.toFixed(1)}): mean ROI=${(meanROI * 100).toFixed(1)}% median=${(medROI * 100).toFixed(1)}% mean-pickers=${mpCount}`);
  }

  // Mean-picker coverage
  const mps = rows.filter(r => r.isMeanPicker);
  if (mps.length > 0) {
    const v2s = mps.map(r => r.v2).sort((a, b) => a - b);
    const legs = mps.filter(r => r.legacy != null).map(r => r.legacy).sort((a, b) => a - b);
    const med = arr => arr.length > 0 ? arr[Math.floor(arr.length / 2)] : null;
    console.log(`\n=== Mean-picker suppression ===`);
    console.log(`  ${mps.length} wallets flagged (${(mps.length / rows.length * 100).toFixed(1)}% of eligible)`);
    console.log(`  Median scoreV2:     ${med(v2s)?.toFixed(1) ?? 'n/a'}`);
    console.log(`  Median legacyScore: ${med(legs)?.toFixed(1) ?? 'n/a'}  (these are what V2 is fixing)`);
    const capTrapped = mps.reduce((a, r) => a + (r.decidedCapital || 0), 0);
    console.log(`  Total decidedCapital trapped in flagged wallets: $${Math.round(capTrapped).toLocaleString()}`);
  }

  // Coverage & freshness
  const now = Date.now();
  const freshCount = rows.filter(r => {
    if (!r.lastScored) return false;
    const age = (now - new Date(r.lastScored).getTime()) / (1000 * 60 * 60);
    return age <= 24;
  }).length;
  console.log(`\n=== Coverage ===`);
  console.log(`  Eligible / pool:     ${rows.length}/${addrs.length - skipRemoved} active (${(rows.length / (addrs.length - skipRemoved) * 100).toFixed(1)}%)`);
  console.log(`  Scored in last 24h:  ${freshCount} (${(freshCount / rows.length * 100).toFixed(1)}%)`);
  if (rows.length < addrs.length * 0.5) {
    console.log(`  ⚠ <50% coverage — let the scanner run more cycles for a cleaner signal.`);
  }
}

main();
