// Stage 2 smoke test — single-side alpha test.
//
// Verifies that analyzeTradeHistory computes edgePP correctly on synthetic
// single-side markets, and that evaluateAlpha/attachAlphaEvaluation produce
// the right verdict for each of the canonical shapes.
//
// Usage: node scripts/test-stage2-alpha.mjs

import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const { analyzeTradeHistory } = await import(path.join(ROOT, 'scanner/dataApi.js'));
const { evaluateAlpha, attachAlphaEvaluation } =
  await import(path.join(ROOT, 'scanner/alphaTest.js'));

const now = Math.floor(Date.now() / 1000);
const daysAgo = (d) => now - d * 86400;
const failures = [];
const ok = (cond, msg) => { if (!cond) failures.push(msg); };

// Helper to build a single-side resolved market — single outcome bought,
// then REDEEMed at $1 (win) or $0 (loss/no-redeem).
function market(id, outcome, entryPrice, shares, won, dayAgoBuy = 30, dayAgoResolve = 5) {
  const events = [
    {
      type: 'TRADE', side: 'BUY',
      size: shares, price: entryPrice,
      conditionId: id, asset: `${id}-${outcome}`,
      outcome, timestamp: daysAgo(dayAgoBuy),
    },
  ];
  if (won) {
    events.push({
      type: 'REDEEM',
      size: shares, usdcSize: shares, // won at $1
      conditionId: id, asset: `${id}-${outcome}`,
      outcome, timestamp: daysAgo(dayAgoResolve),
    });
  }
  // If lost, no REDEEM — shares go worthless. Need marketLookup to close it.
  return events;
}

// Need a marketLookup so losing single-side positions close out cleanly.
function buildLookup(marketSpecs) {
  const lookup = new Map();
  for (const m of marketSpecs) {
    lookup.set(`${m.id}-${m.outcome}`, {
      marketClosed: true,
      winningOutcome: m.won ? m.outcome : (m.outcome === 'YES' ? 'NO' : 'YES'),
    });
  }
  return lookup;
}

// ── Case A: Clear directional alpha ────────────────────────────────────────
// Entry 0.40, wins 55% — edge_pp = +15pp. Mix of wins and losses.
const caseA_specs = [];
for (let i = 0; i < 55; i++) caseA_specs.push({ id: `A-win-${i}`, outcome: 'YES', price: 0.40, shares: 200, won: true });
for (let i = 0; i < 45; i++) caseA_specs.push({ id: `A-loss-${i}`, outcome: 'YES', price: 0.40, shares: 200, won: false });
const caseA_events = caseA_specs.flatMap(s => market(s.id, s.outcome, s.price, s.shares, s.won));
const caseA_lookup = buildLookup(caseA_specs);
const caseA_stats = analyzeTradeHistory(caseA_events, { marketLookup: caseA_lookup });
const caseA_alpha = evaluateAlpha(caseA_stats);

console.log('\n═════════════════════════════════════════════════════════════════');
console.log('  Stage 2 — single-side alpha test');
console.log('═════════════════════════════════════════════════════════════════\n');
console.log('  Case A: real directional alpha (entry $0.40, wins 55%)');
console.log(`    edge_pp       = ${caseA_stats.edgePP}pp`);
console.log(`    hit_rate      = ${(caseA_stats.singleSideHitRate * 100).toFixed(1)}%`);
console.log(`    avg_entry     = ${caseA_stats.singleSideAvgEntry.toFixed(3)}`);
console.log(`    sample n      = ${caseA_stats.singleSideResolved}`);
console.log(`    capital       = $${caseA_stats.singleSideCapital.toFixed(0)}`);
console.log(`    verdict       = ${caseA_alpha.verdict} — ${caseA_alpha.reason}`);
console.log();
ok(caseA_stats.edgePP >= 14 && caseA_stats.edgePP <= 16, `caseA edgePP should be ~15, got ${caseA_stats.edgePP}`);
ok(caseA_alpha.verdict === 'tier_a', `caseA should be tier_a, got ${caseA_alpha.verdict}`);
ok(caseA_alpha.passesAlphaGate === true, `caseA should pass alpha gate`);

// ── Case B: Mean-picker — high WR, no edge ────────────────────────────────
// Entry 0.95, wins 95% — edge_pp = 0. Looks great on paper, zero actual skill.
const caseB_specs = [];
for (let i = 0; i < 95; i++) caseB_specs.push({ id: `B-win-${i}`, outcome: 'YES', price: 0.95, shares: 200, won: true });
for (let i = 0; i < 5; i++)  caseB_specs.push({ id: `B-loss-${i}`, outcome: 'YES', price: 0.95, shares: 200, won: false });
const caseB_events = caseB_specs.flatMap(s => market(s.id, s.outcome, s.price, s.shares, s.won));
const caseB_lookup = buildLookup(caseB_specs);
const caseB_stats = analyzeTradeHistory(caseB_events, { marketLookup: caseB_lookup });
const caseB_alpha = evaluateAlpha(caseB_stats);

console.log('  Case B: mean-picker (entry $0.95, wins 95%)');
console.log(`    edge_pp       = ${caseB_stats.edgePP}pp`);
console.log(`    hit_rate      = ${(caseB_stats.singleSideHitRate * 100).toFixed(1)}%`);
console.log(`    avg_entry     = ${caseB_stats.singleSideAvgEntry.toFixed(3)}`);
console.log(`    verdict       = ${caseB_alpha.verdict} — ${caseB_alpha.reason}`);
console.log();
ok(Math.abs(caseB_stats.edgePP) < 1, `caseB edgePP should be ≈ 0, got ${caseB_stats.edgePP}`);
ok(caseB_alpha.verdict === 'fails', `caseB should fail alpha gate, got ${caseB_alpha.verdict}`);
ok(caseB_alpha.passesAlphaGate === false, `caseB should not pass alpha gate`);

// ── Case C: Lucky small-sample — refused ──────────────────────────────────
const caseC_specs = [];
for (let i = 0; i < 20; i++) caseC_specs.push({ id: `C-${i}`, outcome: 'YES', price: 0.35, shares: 200, won: true });
const caseC_events = caseC_specs.flatMap(s => market(s.id, s.outcome, s.price, s.shares, s.won));
const caseC_lookup = buildLookup(caseC_specs);
const caseC_stats = analyzeTradeHistory(caseC_events, { marketLookup: caseC_lookup });
const caseC_alpha = evaluateAlpha(caseC_stats);
console.log('  Case C: lucky 20-game run (sample too small)');
console.log(`    edge_pp raw   = ${caseC_stats.edgePP}pp`);
console.log(`    sample n      = ${caseC_stats.singleSideResolved}`);
console.log(`    verdict       = ${caseC_alpha.verdict} — ${caseC_alpha.reason}`);
console.log();
ok(caseC_alpha.verdict === 'insufficient_sample',
   `caseC should be rejected on sample size, got ${caseC_alpha.verdict}`);

// ── Case D: Weak positive (Tier B) ─────────────────────────────────────────
// Entry 0.40, wins 42% — edge_pp ~2pp. Not enough for tier_a, but not failure.
// Wait — ROI at 0.40 entry, 42% hit rate: cost = 100 * 0.40 = 40. Each win pays 100. Each loss -40.
// 42 wins × 60 + 58 losses × (-40) = 2520 - 2320 = +200. ROI = 200 / (100*40) = 0.05 = 5%.
const caseD_specs = [];
for (let i = 0; i < 42; i++) caseD_specs.push({ id: `D-win-${i}`, outcome: 'YES', price: 0.40, shares: 200, won: true });
for (let i = 0; i < 58; i++) caseD_specs.push({ id: `D-loss-${i}`, outcome: 'YES', price: 0.40, shares: 200, won: false });
const caseD_events = caseD_specs.flatMap(s => market(s.id, s.outcome, s.price, s.shares, s.won));
const caseD_lookup = buildLookup(caseD_specs);
const caseD_stats = analyzeTradeHistory(caseD_events, { marketLookup: caseD_lookup });
const caseD_alpha = evaluateAlpha(caseD_stats);
console.log('  Case D: weak positive (entry $0.40, wins 42% — tier B)');
console.log(`    edge_pp       = ${caseD_stats.edgePP}pp`);
console.log(`    ROI           = ${(caseD_stats.singleSideROI * 100).toFixed(1)}%`);
console.log(`    verdict       = ${caseD_alpha.verdict} — ${caseD_alpha.reason}`);
console.log();
ok(caseD_stats.edgePP >= 1 && caseD_stats.edgePP <= 3, `caseD edgePP should be ~2, got ${caseD_stats.edgePP}`);
ok(caseD_alpha.verdict === 'tier_b', `caseD should be tier_b, got ${caseD_alpha.verdict}`);

// ── attachAlphaEvaluation mutates in place ─────────────────────────────────
const mutStats = { ...caseA_stats };
attachAlphaEvaluation(mutStats);
ok(mutStats.alphaVerdict === 'tier_a', `attach didn't set alphaVerdict`);
ok(mutStats.alphaPasses === true, `attach didn't set alphaPasses`);

console.log('═════════════════════════════════════════════════════════════════');
if (failures.length === 0) {
  console.log('  ✓ ALL ASSERTIONS PASSED');
  console.log('═════════════════════════════════════════════════════════════════\n');
  process.exit(0);
} else {
  console.log(`  ✗ ${failures.length} FAILURES:`);
  console.log('═════════════════════════════════════════════════════════════════');
  for (const f of failures) console.log(`    - ${f}`);
  console.log();
  process.exit(1);
}
