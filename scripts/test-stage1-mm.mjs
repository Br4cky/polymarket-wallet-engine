// Stage 1 smoke test — MM classifier.
//
// Runs classifyMarketMaker against three synthetic wallet-stats shapes:
//   1. Clean directional wallet  — should score 0/6, mmPenalty = 1.0
//   2. Soft MM (ambiguous)       — should score 3/6, mmPenalty = 0.5
//   3. Whale-01-class MM          — should score 6/6, mmPenalty = 0.0
//
// Also exercises end-to-end integration with computeWalletScore to confirm
// that a whale-01-shape wallet gets effectively zeroed by the new penalty.
//
// Usage: node scripts/test-stage1-mm.mjs

import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const { classifyMarketMaker, mmPenaltyForScore, attachMMClassification } =
  await import(path.join(ROOT, 'scanner/mmClassifier.js'));
const { computeWalletScore } = await import(path.join(ROOT, 'scanner/dataApi.js'));

const now = Math.floor(Date.now() / 1000);

// ── Case 1: Clean directional wallet ───────────────────────────────────────
// Active trader, sells positions, rarely dual-sides, no rebate income.
const directional = {
  uniqueMarkets: 80,
  sellRatio: 0.35,                // sells 1:3 ratio to buys — normal exit behavior
  dualSideRate: 0.08,             // occasionally dual-sides (trade swap)
  mergeRate: 0.02,                // very rare merges
  avgDualSidePriceSum: 1.04,      // no free-arb pattern
  rebateUsdcTotal: 0,
  rewardUsdcTotal: 0,
  // decided* required for V2
  decidedROI: 0.18,
  decidedCapital: 75000,
  resolvedMarkets: 60,
  recentTradesPerDay: 3,
  lastTradeTs: now - 86400 * 2,
  isMeanPickerShape: false,
};

// ── Case 2: Soft MM / ambiguous ────────────────────────────────────────────
// Mix of directional and MM — low sells, some dual-side, some rewards.
const softMM = {
  uniqueMarkets: 120,
  sellRatio: 0.02,                // hits signal 1
  dualSideRate: 0.15,             // below 0.4 threshold, no trigger
  mergeRate: 0.12,                // hits signal 3
  avgDualSidePriceSum: 1.00,      // hits signal 4
  rebateUsdcTotal: 40,            // below $100 — no trigger
  rewardUsdcTotal: 85,            // below $100 — no trigger
  decidedROI: 0.11,
  decidedCapital: 95000,
  resolvedMarkets: 90,
  recentTradesPerDay: 5,
  lastTradeTs: now - 86400 * 1,
  isMeanPickerShape: false,
};

// ── Case 3: Whale-01-class confirmed MM ────────────────────────────────────
// All six signals lit. Huge PnL but no directional alpha.
const whale01 = {
  uniqueMarkets: 250,
  sellRatio: 0.01,                // signal 1 — never sells, closes via merge
  dualSideRate: 0.78,             // signal 2 — dual-sides 78% of markets
  mergeRate: 0.42,                // signal 3 — merges 42% of markets
  avgDualSidePriceSum: 0.97,      // signal 4 — buys below $1 total
  rebateUsdcTotal: 450000,        // signal 5 — $450k in maker rebates
  rewardUsdcTotal: 120000,        // signal 6 — $120k in LP rewards
  decidedROI: 0.27,               // looks great on paper
  decidedCapital: 500000,
  resolvedMarkets: 200,
  recentTradesPerDay: 12,
  lastTradeTs: now - 86400 * 1,
  isMeanPickerShape: false,
};

// ── Case 4: Small-sample rookie — classifier must refuse to score ─────────
const rookie = {
  uniqueMarkets: 15,              // below 25-market floor
  sellRatio: 0.02,
  dualSideRate: 0.9,
  mergeRate: 0.8,
  avgDualSidePriceSum: 0.95,
  rebateUsdcTotal: 500,
  rewardUsdcTotal: 500,
};

// ── Run ────────────────────────────────────────────────────────────────────
const failures = [];
const ok = (cond, msg) => { if (!cond) failures.push(msg); };

function printCase(name, stats, expected) {
  const result = classifyMarketMaker(stats);
  const penalty = mmPenaltyForScore(result.score);
  const triggered = result.signals.filter(s => s.triggered).map(s => s.name);
  console.log(`  ${name}`);
  console.log(`    score: ${result.score}/6   penalty: ${penalty.toFixed(2)}   mm?: ${result.isLikelyMM}`);
  console.log(`    reason: ${result.reason}`);
  console.log(`    triggered: ${triggered.length ? triggered.join(', ') : '(none)'}`);
  console.log();
  ok(result.score === expected.score,
    `${name}: expected score=${expected.score}, got ${result.score}`);
  ok(result.isLikelyMM === expected.isLikelyMM,
    `${name}: expected isLikelyMM=${expected.isLikelyMM}, got ${result.isLikelyMM}`);
  return result;
}

console.log('\n═════════════════════════════════════════════════════════════════════');
console.log('  Stage 1 MM Classifier — per-wallet scoring');
console.log('═════════════════════════════════════════════════════════════════════\n');

printCase('directional (clean)', directional, { score: 0, isLikelyMM: false });
printCase('soft MM (ambiguous)', softMM, { score: 3, isLikelyMM: false });
printCase('whale-01 class', whale01, { score: 6, isLikelyMM: true });
const rookieResult = classifyMarketMaker(rookie);
console.log(`  rookie (<25 markets)`);
console.log(`    score: ${rookieResult.score}/6   reason: ${rookieResult.reason}\n`);
ok(rookieResult.score === 0, `rookie should refuse to score: got ${rookieResult.score}`);
ok(rookieResult.reason.includes('insufficient_sample'),
   `rookie reason should flag sample-size, got: ${rookieResult.reason}`);

// ── End-to-end: MM penalty impact on score ──────────────────────────────
console.log('═════════════════════════════════════════════════════════════════════');
console.log('  End-to-end score impact (with vs without MM classification)');
console.log('═════════════════════════════════════════════════════════════════════\n');

// Without MM classification attached — old behavior
const whale01NoClass = { ...whale01 };
const v2Before = computeWalletScore(whale01NoClass);

// With Stage 1 classification — new behavior
const whale01WithClass = { ...whale01 };
attachMMClassification(whale01WithClass);
const v2After = computeWalletScore(whale01WithClass);

console.log(`  whale-01 class stats:`);
console.log(`    before Stage 1 (no mmPenalty)  → score = ${v2Before.score}`);
console.log(`    after  Stage 1 (mmPenalty=${whale01WithClass.mmPenalty})  → score = ${v2After.score}`);
console.log();

ok(v2Before.score > 20, `whale-01 before penalty should score high, got ${v2Before.score}`);
ok(v2After.score < 5, `whale-01 after penalty should score near zero, got ${v2After.score}`);
ok(v2Before.score > v2After.score * 4,
   `mmPenalty should drop whale-01 score by >4x, went ${v2Before.score} → ${v2After.score}`);

// Check directional wallet is NOT affected
const directionalWithClass = { ...directional };
attachMMClassification(directionalWithClass);
const v2Dir = computeWalletScore(directionalWithClass);
const v2DirNoClass = computeWalletScore({ ...directional });
console.log(`  directional wallet:`);
console.log(`    before Stage 1                 → score = ${v2DirNoClass.score}`);
console.log(`    after  Stage 1 (mmPenalty=${directionalWithClass.mmPenalty})  → score = ${v2Dir.score}`);
console.log();
ok(Math.abs(v2Dir.score - v2DirNoClass.score) < 0.5,
   `directional score should be unchanged by Stage 1, delta was ${Math.abs(v2Dir.score - v2DirNoClass.score)}`);

if (failures.length === 0) {
  console.log('═════════════════════════════════════════════════════════════════════');
  console.log('  ✓ ALL ASSERTIONS PASSED');
  console.log('═════════════════════════════════════════════════════════════════════\n');
  process.exit(0);
} else {
  console.log('═════════════════════════════════════════════════════════════════════');
  console.log(`  ✗ ${failures.length} FAILURES:`);
  console.log('═════════════════════════════════════════════════════════════════════');
  for (const f of failures) console.log(`    - ${f}`);
  console.log();
  process.exit(1);
}
