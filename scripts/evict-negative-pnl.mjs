#!/usr/bin/env node
/**
 * One-shot eviction of pool wallets whose corrected (post CLOB-override)
 * trade PnL is negative.
 *
 * Run AFTER full-pool-rescore.mjs has applied with the CLOB closure-
 * override patch, so cached stats reflect real numbers. The diff-pool-
 * vs-profile run on 2026-05-04 surfaced 73 wallets with negative real
 * PnL — those got admitted to the pool on inflated stats and are now
 * correctly priced as net-loser wallets.
 *
 * Why we evict on trade PnL specifically (not on V2 score):
 *   - V2's scoring uses economicPnl = trade + rewards + rebates + MERGE.
 *     A market-maker-style wallet can lose $80k trading and still score
 *     positively because they earned $120k in rebates. That's not a
 *     useful follower signal — a follower copying their BUY can't
 *     replicate the rebate income, so they'd just inherit the trade
 *     loss.
 *   - For follower-signal generation, trade PnL is the only honest
 *     measure of edge. Rewards / rebates are wallet-side income that
 *     doesn't transfer.
 *
 * Gates:
 *   - stats.totalPnl < MIN_TRADE_PNL  (default 0 — must be net positive)
 *   - sample size: stats.resolvedMarkets >= 10  (don't evict on tiny samples)
 *
 * Usage:  node scripts/evict-negative-pnl.mjs   [--dry]   [--floor N]
 */
import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const argv = process.argv.slice(2);
const DRY = argv.includes('--dry');
let MIN_TRADE_PNL = 0;
const fi = argv.indexOf('--floor');
if (fi >= 0 && argv[fi+1]) MIN_TRADE_PNL = parseFloat(argv[fi+1]);
const MIN_SAMPLE = 10;

const WALLETS_PATH = path.join(ROOT, 'data/wallets.json.gz');
const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(WALLETS_PATH)).toString());
const pool = walletsData.pool || walletsData;

const candidates = Object.entries(pool).filter(([, w]) => w?.status !== 'removed');
console.log(`Loaded ${candidates.length} active wallets`);
console.log(`Threshold: trade PnL < $${MIN_TRADE_PNL}, sample ≥ ${MIN_SAMPLE} resolved markets\n`);

const evictions = [];
const kept = [];
for (const [addr, w] of candidates) {
  // Prefer directionalPnl (totalPnl with MERGE-derived revenue stripped
  // out — the right input for follower-signal scoring). Falls back to
  // totalPnl for wallets whose stats predate the directionalPnl field.
  const pnl = w.stats?.directionalPnl != null ? w.stats.directionalPnl : w.stats?.totalPnl;
  const resolved = w.stats?.resolvedMarkets || 0;
  if (typeof pnl !== 'number' || resolved < MIN_SAMPLE) {
    kept.push({ addr, reason: 'insufficient_sample' });
    continue;
  }
  if (pnl < MIN_TRADE_PNL) {
    evictions.push({
      addr,
      pnl,
      totalPnl: w.stats?.totalPnl,
      resolved,
      score: w.score || 0,
      wins: w.stats?.wins,
      losses: w.stats?.losses,
      winRate: w.stats?.winRate,
      goldskyPnl: w.stats?.goldskyPnl,
      economicPnl: w.stats?.economicPnl,
    });
  } else {
    kept.push({ addr });
  }
}

evictions.sort((a, b) => a.pnl - b.pnl);

console.log(`Keeping: ${kept.length}`);
console.log(`Evicting: ${evictions.length}\n`);

if (evictions.length > 0) {
  console.log('Worst 25 (by directional PnL):');
  for (const e of evictions.slice(0, 25)) {
    const wr = e.winRate != null ? (e.winRate * 100).toFixed(0) + '%' : '—';
    const totalDelta = e.totalPnl != null && Math.abs(e.totalPnl - e.pnl) > 100
      ? `  (totalPnl=$${e.totalPnl.toFixed(0)} incl. MERGE)`
      : '';
    const econNote = e.economicPnl != null && e.economicPnl > 0 && e.pnl < 0
      ? `  (econPnl=$${e.economicPnl.toFixed(0)} incl. rebates)`
      : '';
    console.log(`  ${e.addr.slice(0, 12)}…  dirPnl=$${e.pnl.toFixed(0).padStart(8)}  ${e.wins||0}W/${e.losses||0}L  WR=${wr}  score=${e.score.toFixed(1)}${totalDelta}${econNote}`);
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
  w.removeReason = 'negative_trade_pnl';
  w.removeDetail = JSON.stringify({
    pnl: e.pnl,
    resolved: e.resolved,
    wins: e.wins,
    losses: e.losses,
    score: e.score,
    economicPnl: e.economicPnl,
  });
  w.removedAt = now;
}

walletsData.pool = pool;
if (!walletsData.metadata) walletsData.metadata = {};
walletsData.metadata.lastNegativePnlEviction = now;

fs.writeFileSync(WALLETS_PATH, zlib.gzipSync(Buffer.from(JSON.stringify(walletsData))));
console.log(`\n✓ Evicted ${evictions.length} wallets, wrote ${WALLETS_PATH}`);
