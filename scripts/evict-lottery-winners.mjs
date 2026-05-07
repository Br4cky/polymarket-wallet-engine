#!/usr/bin/env node
/**
 * One-shot eviction of pool wallets whose PnL is dominated by 1-3 outlier
 * wins (the "lottery winner" pattern). Mirrors the discovery-time gate in
 * scan.js so existing pool members get the same filter applied.
 *
 * The pattern: wallet looks profitable on totalPnl/directionalPnl, but
 * remove their top 3 wins and they're net-negative. Or those top 3 wins
 * account for >85% of all positive PnL. Followers copying the wallet's
 * BUYs experience the bulk distribution — the 90%+ of trades that aren't
 * the outliers — and lose money.
 *
 * Eviction conditions (either fires):
 *   - pnlExTop3 < 0
 *   - top3ConcentrationShare > 0.85
 *
 * Sample requirement: resolvedMarkets >= 20 (same as discovery gate).
 * Below this we don't have enough trades to distinguish lottery from skill.
 *
 * Idempotent: safe to run every cron cycle. Wallets already removed are
 * skipped. Written to be added to .github/workflows/scan.yml's
 * post-scan eviction step alongside evict-negative-pnl.mjs.
 *
 * Usage:  node scripts/evict-lottery-winners.mjs   [--dry]
 */
import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const WALLETS_PATH = path.join(ROOT, 'data/wallets.json.gz');
const DRY = process.argv.includes('--dry');

const MIN_RESOLVED_SAMPLE = 20;
const MAX_PNL_EX_TOP3 = 0;                  // pnl ex-top-3 must be ≥ 0
const MIN_TOP3_FOR_AND = 0.50;              // AND-gate: concentration must also be >50%
const MAX_TOP3_CONCENTRATION = 0.85;        // OR-gate: extreme concentration alone evicts

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(WALLETS_PATH)).toString());
const pool = walletsData.pool || walletsData;

const candidates = Object.entries(pool).filter(([, w]) => w?.status !== 'removed');
console.log(`Loaded ${candidates.length} active wallets`);
console.log(`Threshold: pnlExTop3 < ${MAX_PNL_EX_TOP3}  OR  top3ConcentrationShare > ${MAX_TOP3_CONCENTRATION}`);
console.log(`Sample requirement: resolvedMarkets >= ${MIN_RESOLVED_SAMPLE}\n`);

const evictions = [];
const skipped = { insufficient_sample: 0, missing_metrics: 0, ok: 0 };

for (const [addr, w] of candidates) {
  const s = w.stats || {};
  const resolved = s.resolvedMarkets || 0;
  if (resolved < MIN_RESOLVED_SAMPLE) {
    skipped.insufficient_sample++;
    continue;
  }
  const pnlEx3 = s.pnlExTop3;
  const conc3 = s.top3ConcentrationShare;
  if (typeof pnlEx3 !== 'number' && typeof conc3 !== 'number') {
    // Wallet hasn't been rescored under the new analyzer yet — its stats
    // pre-date the field. Skip rather than evict on missing data.
    skipped.missing_metrics++;
    continue;
  }
  // Two-tier check, mirrors scan.js:
  //   AND: pnlExTop3 < 0 AND top3 > 0.50  → concentrated outliers carrying the wallet
  //   OR (extreme): top3 > 0.85 alone     → near-total dependence on top 3
  const failsAnd = typeof pnlEx3 === 'number' && pnlEx3 < MAX_PNL_EX_TOP3
    && typeof conc3 === 'number' && conc3 > MIN_TOP3_FOR_AND;
  const failsExtreme = typeof conc3 === 'number' && conc3 > MAX_TOP3_CONCENTRATION;
  if (failsAnd || failsExtreme) {
    evictions.push({
      addr,
      score: w.score || 0,
      directionalPnl: s.directionalPnl,
      pnlExTop3: pnlEx3,
      top3ConcentrationShare: conc3,
      top1ConcentrationShare: s.top1ConcentrationShare,
      medianTradePnL: s.medianTradePnL,
      resolved,
      reason: [
        failsAnd ? `pnlExTop3=${pnlEx3?.toFixed(0)} AND top3=${(conc3 * 100).toFixed(0)}%>50%` : null,
        failsExtreme && !failsAnd ? `top3Share=${(conc3 * 100).toFixed(0)}%>85%` : null,
      ].filter(Boolean).join(', '),
    });
  } else {
    skipped.ok++;
  }
}

evictions.sort((a, b) => {
  // Sort worst-first by composite ugliness: most negative pnlEx3 + highest concentration
  const aScore = (typeof a.pnlExTop3 === 'number' ? -a.pnlExTop3 : 0)
    + (typeof a.top3ConcentrationShare === 'number' ? a.top3ConcentrationShare * 1000 : 0);
  const bScore = (typeof b.pnlExTop3 === 'number' ? -b.pnlExTop3 : 0)
    + (typeof b.top3ConcentrationShare === 'number' ? b.top3ConcentrationShare * 1000 : 0);
  return bScore - aScore;
});

console.log(`Active:                    ${candidates.length}`);
console.log(`  passing the gate:        ${skipped.ok}`);
console.log(`  insufficient sample:     ${skipped.insufficient_sample}  (resolvedMarkets < ${MIN_RESOLVED_SAMPLE})`);
console.log(`  missing metrics (skip):  ${skipped.missing_metrics}  (not yet rescored under new analyzer)`);
console.log(`  to evict:                ${evictions.length}`);

if (evictions.length > 0) {
  console.log('\nWorst 25 (by composite outlier-dependence):');
  for (const e of evictions.slice(0, 25)) {
    const pnlEx3 = typeof e.pnlExTop3 === 'number' ? `$${e.pnlExTop3.toFixed(0).padStart(6)}` : '—';
    const conc3 = typeof e.top3ConcentrationShare === 'number'
      ? `${(e.top3ConcentrationShare * 100).toFixed(0)}%`
      : '—';
    const dirPnl = typeof e.directionalPnl === 'number'
      ? `$${e.directionalPnl.toFixed(0).padStart(7)}`
      : '—';
    console.log(`  ${e.addr.slice(0, 12)}…  score=${e.score.toFixed(1).padStart(5)}  dirPnl=${dirPnl}  pnlEx3=${pnlEx3}  top3=${conc3.padStart(4)}  resolved=${e.resolved}  [${e.reason}]`);
  }
  if (evictions.length > 25) console.log(`  … +${evictions.length - 25} more`);
}

if (DRY) {
  console.log('\n--dry mode, no files written');
  process.exit(0);
}

if (evictions.length === 0) {
  console.log('\nNothing to evict.');
  process.exit(0);
}

const now = new Date().toISOString();
for (const e of evictions) {
  const w = pool[e.addr];
  if (!w) continue;
  w.status = 'removed';
  w.removeReason = 'lottery_winner';
  w.removeDetail = JSON.stringify({
    pnlExTop3: e.pnlExTop3,
    top3ConcentrationShare: e.top3ConcentrationShare,
    top1ConcentrationShare: e.top1ConcentrationShare,
    directionalPnl: e.directionalPnl,
    resolvedMarkets: e.resolved,
    score: e.score,
    reason: e.reason,
  });
  w.removedAt = now;
}

walletsData.pool = pool;
if (!walletsData.metadata) walletsData.metadata = {};
walletsData.metadata.lastLotteryWinnerEviction = now;

fs.writeFileSync(WALLETS_PATH, zlib.gzipSync(Buffer.from(JSON.stringify(walletsData))));
console.log(`\n✓ Evicted ${evictions.length} lottery-winner wallets, wrote ${WALLETS_PATH}`);
