// Full breakdown of signal performance — active + closed — with timestamp
// tracking so you can see what's happening over different windows.
//
// Usage:
//   node scripts/analyze-signals.mjs                     # full report
//   node scripts/analyze-signals.mjs --since 2026-04-20  # filter opened >= date

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const args = process.argv.slice(2);
const since = (() => {
  const i = args.indexOf('--since');
  if (i < 0) return null;
  const d = new Date(args[i + 1]);
  return isNaN(d.getTime()) ? null : d.getTime();
})();

const data = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const active = Object.values(data.active || {});
const history = data.history || [];
const now = Date.now();

// Normalize timestamps to ms — may be ISO string, unix seconds, or ms
const toMs = (t) => {
  if (!t) return 0;
  if (typeof t === 'string') {
    const d = new Date(t);
    return isNaN(d.getTime()) ? 0 : d.getTime();
  }
  if (typeof t !== 'number' || !isFinite(t)) return 0;
  return t > 1e11 ? t : t * 1000;
};

function windowStats(signals, label) {
  const total = signals.length;
  const wins = signals.filter(s => s.outcome === 'win').length;
  const losses = signals.filter(s => s.outcome === 'loss').length;
  const resolved = wins + losses;
  const wr = resolved > 0 ? (wins / resolved * 100).toFixed(1) + '%' : '—';
  const returns = signals
    .filter(s => typeof s.signalReturn === 'number' && s.outcome !== 'void')
    .map(s => s.signalReturn);
  const avgReturn = returns.length > 0 ? (returns.reduce((a, b) => a + b, 0) / returns.length).toFixed(1) + '%' : '—';
  const winReturns = signals.filter(s => s.outcome === 'win' && typeof s.signalReturn === 'number').map(s => s.signalReturn);
  const avgWinReturn = winReturns.length > 0 ? (winReturns.reduce((a, b) => a + b, 0) / winReturns.length).toFixed(1) + '%' : '—';
  return { label, total, wins, losses, resolved, wr, avgReturn, avgWinReturn };
}

function bucketByTime(signals, tsField = 'closedAt') {
  const buckets = { '24h': [], '7d': [], '30d': [], 'all': [] };
  for (const s of signals) {
    const ts = toMs(s[tsField]);
    if (!ts) continue;
    const ageMs = now - ts;
    if (ageMs <= 86400 * 1000) buckets['24h'].push(s);
    if (ageMs <= 7 * 86400 * 1000) buckets['7d'].push(s);
    if (ageMs <= 30 * 86400 * 1000) buckets['30d'].push(s);
    buckets['all'].push(s);
  }
  return buckets;
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Signal analytics — ' + new Date().toISOString().slice(0, 16).replace('T', ' ') + 'Z');
console.log('═══════════════════════════════════════════════════════════════════\n');

// ── ACTIVE SIGNALS ────────────────────────────────────────────────────────
console.log(`  ACTIVE SIGNALS (${active.length} open)`);
console.log('  ' + '─'.repeat(65));
const activeByTier = { elite: [], pro: [], starter: [], other: [] };
const activeByType = { consensus: [], cluster: [], solo: [], other: [] };
for (const s of active) {
  if (activeByTier[s.tier]) activeByTier[s.tier].push(s);
  else activeByTier.other.push(s);
  if (activeByType[s.signalType]) activeByType[s.signalType].push(s);
  else activeByType.other.push(s);
}
console.log(`    By tier:  elite=${activeByTier.elite.length}  pro=${activeByTier.pro.length}  starter=${activeByTier.starter.length}`);
console.log(`    By type:  consensus=${activeByType.consensus.length}  cluster=${activeByType.cluster.length}  solo=${activeByType.solo.length}`);

// Age distribution of active signals
const ageBuckets = { '< 12h': 0, '< 48h': 0, '< 5d': 0, '< 14d': 0, '≥ 14d': 0 };
for (const s of active) {
  const age = now - toMs(s.openedAt || s.firstTradeTs || 0);
  if (age < 12 * 3600 * 1000) ageBuckets['< 12h']++;
  else if (age < 48 * 3600 * 1000) ageBuckets['< 48h']++;
  else if (age < 5 * 86400 * 1000) ageBuckets['< 5d']++;
  else if (age < 14 * 86400 * 1000) ageBuckets['< 14d']++;
  else ageBuckets['≥ 14d']++;
}
console.log(`    Age of open signals:`);
for (const [band, count] of Object.entries(ageBuckets)) {
  console.log(`      ${band.padEnd(7)}  ${String(count).padStart(4)}`);
}
console.log();

// ── HISTORY BY OUTCOME ────────────────────────────────────────────────────
const histWins = history.filter(s => s.outcome === 'win');
const histLosses = history.filter(s => s.outcome === 'loss');
const histVoid = history.filter(s => s.outcome === 'void');
const histOther = history.filter(s => !['win', 'loss', 'void'].includes(s.outcome));

console.log(`  CLOSED HISTORY (${history.length} total)`);
console.log('  ' + '─'.repeat(65));
console.log(`    Wins:     ${histWins.length}`);
console.log(`    Losses:   ${histLosses.length}`);
console.log(`    Void:     ${histVoid.length}`);
console.log(`    Other:    ${histOther.length}  (stale, expired, majority-exit, etc.)`);
console.log();
const resolvedOnly = histWins.length + histLosses.length;
const cumWR = resolvedOnly > 0 ? (histWins.length / resolvedOnly * 100).toFixed(1) : '—';
console.log(`    Resolved:  ${resolvedOnly}   Cumulative WR:  ${cumWR}%`);
console.log();

// ── CLOSED BY TIME WINDOW (all outcomes) ──────────────────────────────────
console.log('  CLOSED BY TIME WINDOW (by closedAt)');
console.log('  ' + '─'.repeat(65));
const buckets = bucketByTime(history, 'closedAt');
const windows = [
  { key: '24h', label: 'Last 24h' },
  { key: '7d',  label: 'Last 7d ' },
  { key: '30d', label: 'Last 30d' },
  { key: 'all', label: 'All time' },
];
console.log(`    ${'Window'.padEnd(10)} ${'Resolved'.padStart(10)} ${'W/L'.padStart(12)} ${'WR'.padStart(8)} ${'Avg Ret'.padStart(10)} ${'Avg Win'.padStart(10)}`);
for (const { key, label } of windows) {
  const s = windowStats(buckets[key], label);
  console.log(`    ${label.padEnd(10)} ${String(s.resolved).padStart(10)} ${`${s.wins}W/${s.losses}L`.padStart(12)} ${s.wr.padStart(8)} ${s.avgReturn.padStart(10)} ${s.avgWinReturn.padStart(10)}`);
}
console.log();

// ── SINCE A SPECIFIC DATE ─────────────────────────────────────────────────
if (since) {
  console.log(`  CLOSED SINCE ${new Date(since).toISOString().slice(0, 10)}`);
  console.log('  ' + '─'.repeat(65));
  const sinceSignals = history.filter(s => toMs(s.closedAt) >= since);
  const s = windowStats(sinceSignals, 'since');
  console.log(`    Resolved: ${s.resolved}  ${s.wins}W/${s.losses}L  WR=${s.wr}  AvgRet=${s.avgReturn}  AvgWin=${s.avgWinReturn}`);
  console.log();
}

// ── BY SIGNAL TIER (all time) ─────────────────────────────────────────────
console.log('  CLOSED BY TIER');
console.log('  ' + '─'.repeat(65));
const byTier = {};
for (const s of history) {
  const tier = s.tier || 'unknown';
  if (!byTier[tier]) byTier[tier] = [];
  byTier[tier].push(s);
}
console.log(`    ${'Tier'.padEnd(10)} ${'Resolved'.padStart(10)} ${'W/L'.padStart(12)} ${'WR'.padStart(8)} ${'Avg Ret'.padStart(10)} ${'Avg Win'.padStart(10)}`);
for (const tier of Object.keys(byTier).sort()) {
  const s = windowStats(byTier[tier], tier);
  console.log(`    ${tier.padEnd(10)} ${String(s.resolved).padStart(10)} ${`${s.wins}W/${s.losses}L`.padStart(12)} ${s.wr.padStart(8)} ${s.avgReturn.padStart(10)} ${s.avgWinReturn.padStart(10)}`);
}
console.log();

// ── BY SIGNAL TYPE (all time) ─────────────────────────────────────────────
console.log('  CLOSED BY TYPE');
console.log('  ' + '─'.repeat(65));
const byType = {};
for (const s of history) {
  const type = s.signalType || 'unknown';
  if (!byType[type]) byType[type] = [];
  byType[type].push(s);
}
console.log(`    ${'Type'.padEnd(10)} ${'Resolved'.padStart(10)} ${'W/L'.padStart(12)} ${'WR'.padStart(8)} ${'Avg Ret'.padStart(10)} ${'Avg Win'.padStart(10)}`);
for (const type of Object.keys(byType).sort()) {
  const s = windowStats(byType[type], type);
  console.log(`    ${type.padEnd(10)} ${String(s.resolved).padStart(10)} ${`${s.wins}W/${s.losses}L`.padStart(12)} ${s.wr.padStart(8)} ${s.avgReturn.padStart(10)} ${s.avgWinReturn.padStart(10)}`);
}
console.log();

// ── CLOSE REASONS (non-resolved) ──────────────────────────────────────────
const nonResolved = history.filter(s => s.outcome !== 'win' && s.outcome !== 'loss');
if (nonResolved.length > 0) {
  console.log('  NON-WIN/LOSS CLOSURES (stale, expired, void, etc.)');
  console.log('  ' + '─'.repeat(65));
  const reasons = {};
  for (const s of nonResolved) {
    const r = s.closeReason || s.outcome || 'unknown';
    reasons[r] = (reasons[r] || 0) + 1;
  }
  for (const [reason, count] of Object.entries(reasons).sort((a, b) => b[1] - a[1])) {
    console.log(`    ${reason.padEnd(22)}  ${String(count).padStart(4)}`);
  }
  console.log();
}

// ── RECENT RESOLUTIONS (sample) ───────────────────────────────────────────
const recentResolved = history
  .filter(s => (s.outcome === 'win' || s.outcome === 'loss') && toMs(s.closedAt) > 0)
  .sort((a, b) => toMs(b.closedAt) - toMs(a.closedAt))
  .slice(0, 10);

if (recentResolved.length > 0) {
  console.log('  MOST-RECENTLY RESOLVED (last 10)');
  console.log('  ' + '─'.repeat(65));
  console.log(`    ${'Closed'.padEnd(11)} ${'Outcome'.padEnd(7)} ${'Type'.padEnd(10)} ${'Ret'.padStart(8)} Market`);
  for (const s of recentResolved) {
    const ageDays = (now - toMs(s.closedAt)) / (86400 * 1000);
    const rel = ageDays < 1 ? `${Math.floor(ageDays * 24)}h ago` : `${ageDays.toFixed(1)}d ago`;
    const ret = typeof s.signalReturn === 'number' ? `${s.signalReturn >= 0 ? '+' : ''}${s.signalReturn.toFixed(0)}%` : '—';
    console.log(`    ${rel.padEnd(11)} ${(s.outcome || '').padEnd(7)} ${(s.signalType || '').padEnd(10)} ${ret.padStart(8)} ${(s.marketTitle || '').slice(0, 55)}`);
  }
  console.log();
}
