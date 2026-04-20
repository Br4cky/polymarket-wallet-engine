// Stage 0 smoke test — analyzeTradeHistory event-type ingestion.
//
// Runs analyzeTradeHistory against a hand-crafted event fixture that contains
// every event type (TRADE, REDEEM, MERGE, SPLIT, CONVERSION, REWARD,
// MAKER_REBATE) and asserts that the new stats fields are populated correctly.
//
// The fixture is intentionally whale-01-flavoured: heavy rebate + reward
// income, moderate merges, few directional trades. Previously analyzeTradeHistory
// would have reported totalPnl=~$10 (tiny) and hidden the rest. After Stage 0
// it should report economicPnl including the rebate + reward income.
//
// Usage: node scripts/test-stage0-pnl.mjs
//
// Exit code 0 on success, 1 on any assertion failure.

import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const { analyzeTradeHistory } = await import(path.join(ROOT, 'scanner/dataApi.js'));

const now = Math.floor(Date.now() / 1000);
const daysAgo = (d) => now - d * 86400;

// Synthetic whale-01 shape: 2 TRADEs (small), 1 REDEEM, 5 MERGEs, 2 SPLITs,
// 1 CONVERSION, 3 REWARDs totalling $800, 4 MAKER_REBATEs totalling $1200.
// Expected: economicPnl ≈ trade + 800 + 1200 ≈ $2k+ ; mergeRate > 0; non-zero
// rebateUsdcTotal/rewardUsdcTotal.
const events = [
  // Two directional BUYs on two different markets (small trading activity)
  { type: 'TRADE', side: 'BUY',  size: 100, price: 0.45, conditionId: 'mkt-A', asset: 'tok-A-yes', outcome: 'YES', timestamp: daysAgo(30) },
  { type: 'TRADE', side: 'BUY',  size: 100, price: 0.40, conditionId: 'mkt-B', asset: 'tok-B-yes', outcome: 'YES', timestamp: daysAgo(25) },
  // One REDEEM on mkt-A (winner) — payout $100 for 100 shares
  { type: 'REDEEM', size: 100, usdcSize: 100, conditionId: 'mkt-A', asset: 'tok-A-yes', outcome: 'YES', timestamp: daysAgo(20) },
  // Five MERGEs on five different markets — whale-01 signature move
  { type: 'MERGE', size: 500,  usdcSize: 495,  conditionId: 'mkt-C', asset: 'tok-C-yes', timestamp: daysAgo(18) },
  { type: 'MERGE', size: 800,  usdcSize: 792,  conditionId: 'mkt-D', asset: 'tok-D-yes', timestamp: daysAgo(15) },
  { type: 'MERGE', size: 1200, usdcSize: 1188, conditionId: 'mkt-E', asset: 'tok-E-yes', timestamp: daysAgo(12) },
  { type: 'MERGE', size: 600,  usdcSize: 594,  conditionId: 'mkt-F', asset: 'tok-F-yes', timestamp: daysAgo(10) },
  { type: 'MERGE', size: 300,  usdcSize: 297,  conditionId: 'mkt-G', asset: 'tok-G-yes', timestamp: daysAgo(8)  },
  // Two SPLITs (preparatory for MERGEs, typical MM plumbing)
  { type: 'SPLIT', size: 500, usdcSize: 500, conditionId: 'mkt-C', timestamp: daysAgo(20) },
  { type: 'SPLIT', size: 800, usdcSize: 800, conditionId: 'mkt-D', timestamp: daysAgo(17) },
  // One CONVERSION
  { type: 'CONVERSION', size: 50, conditionId: 'mkt-H', timestamp: daysAgo(5) },
  // Three REWARDs (LP yield)
  { type: 'REWARD', usdcSize: 150, timestamp: daysAgo(22) },
  { type: 'REWARD', usdcSize: 300, timestamp: daysAgo(14) },
  { type: 'REWARD', usdcSize: 350, timestamp: daysAgo(6)  },
  // Four MAKER_REBATEs
  { type: 'MAKER_REBATE', usdcSize: 250, timestamp: daysAgo(19) },
  { type: 'MAKER_REBATE', usdcSize: 350, timestamp: daysAgo(13) },
  { type: 'MAKER_REBATE', usdcSize: 320, timestamp: daysAgo(7)  },
  { type: 'MAKER_REBATE', usdcSize: 280, timestamp: daysAgo(2)  },
];

const stats = analyzeTradeHistory(events);
if (!stats) {
  console.error('FAIL: analyzeTradeHistory returned null');
  process.exit(1);
}

// ── Assertions ─────────────────────────────────────────────────────────────
const failures = [];
const ok = (cond, msg) => { if (!cond) failures.push(msg); };

ok(stats.mergeCount === 5, `mergeCount should be 5, got ${stats.mergeCount}`);
ok(stats.mergeMarkets === 5, `mergeMarkets should be 5, got ${stats.mergeMarkets}`);
ok(Math.abs(stats.mergeUsdcTotal - 3366) < 1, `mergeUsdcTotal ≈ 3366, got ${stats.mergeUsdcTotal}`);
ok(stats.splitCount === 2, `splitCount should be 2, got ${stats.splitCount}`);
ok(stats.splitMarkets === 2, `splitMarkets should be 2, got ${stats.splitMarkets}`);
ok(stats.conversionCount === 1, `conversionCount should be 1, got ${stats.conversionCount}`);
ok(Math.abs(stats.rewardUsdcTotal - 800) < 0.5, `rewardUsdcTotal should be 800, got ${stats.rewardUsdcTotal}`);
ok(Math.abs(stats.rebateUsdcTotal - 1200) < 0.5, `rebateUsdcTotal should be 1200, got ${stats.rebateUsdcTotal}`);
ok(Math.abs(stats.nonDirectionalIncome - 2000) < 0.5, `nonDirectionalIncome should be 2000, got ${stats.nonDirectionalIncome}`);
ok(stats.economicPnl > stats.totalPnl, `economicPnl (${stats.economicPnl}) should exceed totalPnl (${stats.totalPnl}) by ~2000`);
ok(Math.abs((stats.economicPnl - stats.totalPnl) - 2000) < 1, `economicPnl - totalPnl should be ~2000, got ${stats.economicPnl - stats.totalPnl}`);
ok(stats.mergeRate > 0, `mergeRate should be > 0, got ${stats.mergeRate}`);
// mergeRate = mergeCount (5) / uniqueMarkets. Markets with activity: A (trade+redeem), B (trade),
// C (merge+split), D (merge+split), E, F, G (merges). So 7 markets total. 5/7 ≈ 0.714.
ok(Math.abs(stats.mergeRate - 5/7) < 0.01, `mergeRate should be ~${(5/7).toFixed(3)}, got ${stats.mergeRate}`);

// MERGE should register as SELL events so the per-market positions resolve.
// Before Stage 0: mkt-C/D/E/F/G would show as openPositions=5 (never sold).
// After Stage 0: they should resolve via the synthetic SELL from MERGE.
// We don't have TRADE buys on C/D/E/F/G in this fixture so they won't be in
// marketTrades at all (no buy = no market tracked). That's expected.
// Markets WITH buys: A (traded + redeemed → resolved), B (traded, no sell → open).
ok(stats.resolvedMarkets >= 1, `should have ≥1 resolved market, got ${stats.resolvedMarkets}`);
ok(stats.uniqueMarkets >= 2, `should have ≥2 unique markets, got ${stats.uniqueMarkets}`);

// Sanity: legacy totalPnl should still be present (unchanged semantics)
ok(typeof stats.totalPnl === 'number', `totalPnl should be a number`);

console.log('\n═════════════════════════════════════════════════════════════════');
console.log('  Stage 0 PnL fix — smoke test');
console.log('═════════════════════════════════════════════════════════════════\n');
console.log('  Synthetic whale-01-shape fixture results:');
console.log(`    totalPnl (trades only)     : $${stats.totalPnl.toFixed(2)}`);
console.log(`    rewardUsdcTotal (LP rewards) : $${stats.rewardUsdcTotal.toFixed(2)}`);
console.log(`    rebateUsdcTotal (maker rebates): $${stats.rebateUsdcTotal.toFixed(2)}`);
console.log(`    nonDirectionalIncome       : $${stats.nonDirectionalIncome.toFixed(2)}`);
console.log(`    economicPnl (all sources)  : $${stats.economicPnl.toFixed(2)}`);
console.log(`    mergeCount / mergeRate     : ${stats.mergeCount} (${(stats.mergeRate * 100).toFixed(1)}% of markets)`);
console.log(`    splitCount / conversionCount: ${stats.splitCount} / ${stats.conversionCount}`);
console.log(`    resolvedMarkets / unique   : ${stats.resolvedMarkets} / ${stats.uniqueMarkets}`);
console.log();

if (failures.length === 0) {
  console.log('  ✓ ALL 15 ASSERTIONS PASSED\n');
  process.exit(0);
} else {
  console.log(`  ✗ ${failures.length} ASSERTION(S) FAILED:\n`);
  for (const f of failures) console.log(`    - ${f}`);
  console.log();
  process.exit(1);
}
