// One-time sweep — applies the 2026-04-30 structural restructure to the
// existing pool without waiting for individual rescore cycles.
//
// Three changes from the same audit:
//   1. Hard-ban holders + mm-like from any signal contribution.
//      (Code change in signals.js detectConvergence — but existing pool
//      wallets already classified as those styles can be removed up-front.)
//   2. Attribution multiplier widened from [0.2, 1.5] to [0.0, 2.0] and
//      sample floor lowered from 5 to 3 signals.
//      (Code change in signalAttribution.js — but existing pool scores
//      were computed under the old multiplier.)
//   3. Bot-pattern high-frequency floor (already shipped commit 4aa2b96 —
//      this sweep applies it retroactively too.)
//
// Result: every active pool wallet gets its attribution multiplier and
// score recomputed under the new rules. Wallets falling below the score
// floor are evicted with reason='structural_restructure_2026_04_30'.
//
// Usage:
//   node scripts/sweep-restructure-2026-04-30.mjs              # dry-run, report
//   node scripts/sweep-restructure-2026-04-30.mjs --apply      # write back

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  buildAttributionMap,
  attachAttribution,
  attributionMultiplier,
} from '../scanner/signalAttribution.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const APPLY = process.argv.includes('--apply');

const SCORE_FLOOR = 5;        // wallets below this get evicted
const BOT_TPAW_FLOOR = 700;   // matches CONFIG.BOT_PATTERN_MAX_TRADES_PER_ACTIVE_WEEK

// Load fresh blobs
const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());

const pool = walletsData.pool || walletsData;
const history = Array.isArray(signalsData.history) ? signalsData.history : Object.values(signalsData.history || {});

// Style classifier — mirror of signals.js classifyWalletStyle
function classifyStyle(s) {
  if (!s) return 'unknown';
  if ((s.dualSideRate || 0) > 0.30 || (s.mmScore || 0) >= 3) return 'mm-like';
  const tt = s.totalTrades || 0;
  const um = s.uniqueMarkets || 0;
  const tpm = um > 0 ? tt / um : 0;
  const sr = s.sellRatio ?? 1;
  const hold = s.avgHoldTimeHours || 0;
  if (tpm > 8) return 'churner';
  if (tpm >= 3 && sr > 0.30) return 'averager';
  if (tpm <= 2 && hold < 48) return 'sniper';
  if (sr < 0.15) return 'holder';
  return 'mixed';
}

// Build attribution map under NEW rules (min 3 sigs, [0.0, 2.0] band)
const attrMap = buildAttributionMap(history);

const all = Object.entries(pool);
const active = all.filter(([, w]) => w?.status !== 'removed');

console.log('═'.repeat(78));
console.log('  Structural restructure sweep — 2026-04-30');
console.log('═'.repeat(78));
console.log(`  Active pool size:               ${active.length}`);
console.log(`  Mode:                           ${APPLY ? 'APPLY (write back)' : 'DRY-RUN'}`);
console.log();

const evictions = {
  style_holder: [],
  style_mm_like: [],
  bot_high_frequency: [],
  attribution_collapse: [],   // new attribution band drops them below floor
  score_below_floor: [],      // mostly attribution-driven score collapse
};
const survivors = [];
let scoreBoosts = 0, scorePenalties = 0;

for (const [addr, w] of active) {
  const stats = w.stats || {};
  const style = classifyStyle(stats);
  const tpaw = stats.tradesPerActiveWeek || stats.recentTradesPerActiveWeek || 0;

  // Pre-eviction style + bot checks (run BEFORE rescore to avoid wasted work)
  if (style === 'holder') {
    evictions.style_holder.push({ addr, score: w.score, style });
    continue;
  }
  if (style === 'mm-like') {
    evictions.style_mm_like.push({ addr, score: w.score, style });
    continue;
  }
  if (tpaw > BOT_TPAW_FLOOR) {
    evictions.bot_high_frequency.push({ addr, score: w.score, tpaw });
    continue;
  }

  // Apply NEW attribution multiplier (lower min, wider band)
  const oldAttrMul = w.scoreComponents?.attrMultiplier ?? 1.0;
  const attr = attrMap.get(addr.toLowerCase());
  const newAttrMul = attr ? attributionMultiplier(attr, { minSignals: 3 }) : 1.0;

  if (newAttrMul > oldAttrMul + 0.01) scoreBoosts++;
  if (newAttrMul < oldAttrMul - 0.01) scorePenalties++;

  // Reproject score: oldScore was core × oldAttrMul + activityBonus.
  // We don't have the raw core here, but components are stored. Use:
  //   newScore ≈ ((oldScore - activityBonus) / oldAttrMul) × newAttrMul + activityBonus
  // when oldAttrMul > 0. For oldAttrMul = 0 (rare under old rules — floor
  // was 0.2), we treat it as a full recompute target and project from
  // attrMul alone — newScore = oldScore × (newAttrMul / oldAttrMul) when
  // both are nonzero.
  const activityBonus = w.scoreComponents?.activityBonus ?? 0;
  const oldCore = oldAttrMul > 0 ? (w.score - activityBonus) / oldAttrMul : 0;
  const newCore = oldCore * newAttrMul;
  const newScore = newCore > 0 ? +(newCore + activityBonus).toFixed(1) : 0;

  if (newAttrMul === 0 && attr) {
    evictions.attribution_collapse.push({
      addr,
      oldScore: w.score,
      newScore,
      attrSigs: attr.signals,
      attrAvgRet: (attr.avgReturn * 100).toFixed(1) + '%',
    });
    continue;
  }
  if (newScore < SCORE_FLOOR) {
    evictions.score_below_floor.push({
      addr,
      oldScore: w.score,
      newScore,
      oldAttrMul: oldAttrMul.toFixed(2),
      newAttrMul: newAttrMul.toFixed(2),
      attrSigs: attr ? attr.signals : 0,
      attrAvgRet: attr ? (attr.avgReturn * 100).toFixed(1) + '%' : '—',
    });
    continue;
  }

  survivors.push({
    addr, oldScore: w.score, newScore, oldAttrMul, newAttrMul,
    style, attrSigs: attr ? attr.signals : 0,
  });
}

// Report
console.log('── Eviction breakdown ──');
console.log(`  style_holder          ${String(evictions.style_holder.length).padStart(5)}`);
console.log(`  style_mm_like         ${String(evictions.style_mm_like.length).padStart(5)}`);
console.log(`  bot_high_frequency    ${String(evictions.bot_high_frequency.length).padStart(5)}`);
console.log(`  attribution_collapse  ${String(evictions.attribution_collapse.length).padStart(5)}`);
console.log(`  score_below_floor     ${String(evictions.score_below_floor.length).padStart(5)}`);
const totalEvicted = Object.values(evictions).reduce((s, arr) => s + arr.length, 0);
console.log(`  TOTAL EVICTED         ${String(totalEvicted).padStart(5)}`);
console.log();
console.log('── Survivors ──');
console.log(`  Surviving pool size:  ${survivors.length}`);
console.log(`  Score boosts (≥+0.01 attrMul): ${scoreBoosts}`);
console.log(`  Score penalties (≤-0.01):      ${scorePenalties}`);

// Show 10 biggest score swings (winners + losers)
console.log();
console.log('── Top 10 score boosts (proven signal alpha rewarded) ──');
const boosted = survivors.filter(s => s.newScore > s.oldScore).sort((a, b) => (b.newScore - b.oldScore) - (a.newScore - a.oldScore)).slice(0, 10);
for (const s of boosted) {
  console.log(`  ${s.addr.slice(0, 12)}  ${s.oldScore.toFixed(1)}→${s.newScore.toFixed(1)}  attrMul ${s.oldAttrMul.toFixed(2)}→${s.newAttrMul.toFixed(2)}  sigs=${s.attrSigs}  style=${s.style}`);
}
console.log();
console.log('── Top 10 score penalties (proven negative signal EV) ──');
const penalised = survivors.filter(s => s.newScore < s.oldScore).sort((a, b) => (a.newScore - a.oldScore) - (b.newScore - b.oldScore)).slice(0, 10);
for (const s of penalised) {
  console.log(`  ${s.addr.slice(0, 12)}  ${s.oldScore.toFixed(1)}→${s.newScore.toFixed(1)}  attrMul ${s.oldAttrMul.toFixed(2)}→${s.newAttrMul.toFixed(2)}  sigs=${s.attrSigs}  style=${s.style}`);
}

// Apply
if (APPLY) {
  for (const arr of Object.values(evictions)) {
    for (const ev of arr) {
      const w = pool[ev.addr];
      w.status = 'removed';
      w.removeReason = 'structural_restructure_2026_04_30';
      w.removeDetail = JSON.stringify(ev);
      w.removedAt = new Date().toISOString();
    }
  }
  for (const s of survivors) {
    const w = pool[s.addr];
    w.score = s.newScore;
    if (!w.scoreComponents) w.scoreComponents = {};
    w.scoreComponents.attrMultiplier = +s.newAttrMul.toFixed(3);
    w.scoreComponents.attrSignals = s.attrSigs;
    if (attrMap.has(s.addr.toLowerCase())) {
      const a = attrMap.get(s.addr.toLowerCase());
      w.scoreComponents.attrAvgReturn = +a.avgReturn.toFixed(3);
      w.stats = w.stats || {};
      w.stats.signalAttribution = {
        signals: a.signals, wins: a.wins, losses: a.losses,
        wr: +a.wr.toFixed(3), avgReturn: +a.avgReturn.toFixed(3),
      };
      w.stats.attributionMultiplier = +s.newAttrMul.toFixed(3);
    }
    w.lastScored = new Date().toISOString();
  }
  walletsData.pool = pool;
  if (!walletsData.metadata) walletsData.metadata = {};
  walletsData.metadata.lastStructuralRestructure = new Date().toISOString();
  walletsData.metadata.structuralRestructureEvicted = totalEvicted;
  fs.writeFileSync(path.join(ROOT, 'data/wallets.json.gz'),
    zlib.gzipSync(Buffer.from(JSON.stringify(walletsData))));
  console.log(`\n  ✓ Applied — evicted ${totalEvicted}, rescored ${survivors.length}`);
} else {
  console.log('\n  Dry-run. Pass --apply to write back.');
}
console.log();
