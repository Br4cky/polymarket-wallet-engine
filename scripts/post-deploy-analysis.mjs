// Post-deploy analysis — assess emission volume and resolution outcomes
// since Option 2 + whale-bypass shipped. Splits signals into cohorts
// based on when they OPENED so we can separate old-code from new-code.
//
// Code-deploy timeline (UTC):
//   2026-04-23 23:06  c2afc4e  Option 2 composite emission ships
//   2026-04-24 ~10:00 stale-trade bug fix
//   2026-04-24 ~12:00 SOLO_MIN_SCORE 25, widened SOLO_ALLOWED_STYLES
//   2026-04-24 ~18:00 whale bypass
//   2026-04-27 ~21:00 majority-disqualified + soccer re-added + min-time-to-resolution
//
// Output:
//   1. Per-day open/close volume + WR for last 14 days
//   2. Code-version cohort split (pre/post each major fix)
//   3. Per-path emission activity in last 7 days
//   4. Currently active signals (likely from new code)
//   5. Cumulative P&L by cohort

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const all = signalsData.history || [];
const active = signalsData.active || {};

const now = new Date();
const daysAgo = (n) => new Date(now.getTime() - n * 86400 * 1000);

// Cohort cutoffs
const OPTION2_TS = new Date('2026-04-23T23:06:00Z').getTime();
const STALE_FIX_TS = new Date('2026-04-24T10:00:00Z').getTime();
const TUNING_TS = new Date('2026-04-24T12:00:00Z').getTime();
const WHALE_BYPASS_TS = new Date('2026-04-24T18:00:00Z').getTime();
const VOLUME_TUNE_TS = new Date('2026-04-27T21:00:00Z').getTime();

function getDate(s) { return s.openedAt ? new Date(s.openedAt) : null; }
function getCloseDate(s) { return s.closedAt ? new Date(s.closedAt) : null; }
function ymd(d) { return d.toISOString().slice(0, 10); }

// ── 1. Per-day open + close + win rate ─────────────────────────────────
console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Post-deploy analysis');
console.log('═══════════════════════════════════════════════════════════════════');

const byOpenDay = new Map();
const byCloseDay = new Map();
for (const s of all) {
  const od = getDate(s);
  const cd = getCloseDate(s);
  if (od) {
    const k = ymd(od);
    if (!byOpenDay.has(k)) byOpenDay.set(k, { opened: 0 });
    byOpenDay.get(k).opened++;
  }
  if (cd && (s.outcome === 'win' || s.outcome === 'loss')) {
    const k = ymd(cd);
    if (!byCloseDay.has(k)) byCloseDay.set(k, { closed: 0, wins: 0, losses: 0, totalRet: 0, retN: 0 });
    const r = byCloseDay.get(k);
    r.closed++;
    if (s.outcome === 'win') r.wins++; else r.losses++;
    if (typeof s.signalReturn === 'number') { r.totalRet += s.signalReturn; r.retN++; }
  }
}
// Active too — count opens
for (const s of Object.values(active)) {
  const od = getDate(s);
  if (od) {
    const k = ymd(od);
    if (!byOpenDay.has(k)) byOpenDay.set(k, { opened: 0 });
    byOpenDay.get(k).opened++;
  }
}

console.log('\n  ── 1. PER-DAY OPENS / CLOSES (last 14 days) ──');
console.log(`  ${'Date'.padEnd(11)} ${'Opened'.padStart(7)} ${'Closed'.padStart(7)} ${'WR'.padStart(5)} ${'AvgRet'.padStart(8)} ${'NetPnL'.padStart(8)}`);
console.log('  ' + '─'.repeat(56));
for (let i = 13; i >= 0; i--) {
  const k = ymd(daysAgo(i));
  const o = byOpenDay.get(k)?.opened || 0;
  const c = byCloseDay.get(k);
  const closed = c?.closed || 0;
  const wr = c && closed > 0 ? (c.wins / closed * 100).toFixed(0) + '%' : '—';
  const avgRet = c && c.retN > 0 ? ((c.totalRet / c.retN >= 0 ? '+' : '') + (c.totalRet / c.retN).toFixed(0) + '%') : '—';
  const netPnL = c && c.retN > 0 ? ((c.totalRet >= 0 ? '+' : '') + c.totalRet.toFixed(0)) : '—';
  console.log(`  ${k.padEnd(11)} ${String(o).padStart(7)} ${String(closed).padStart(7)} ${wr.padStart(5)} ${avgRet.padStart(8)} ${netPnL.padStart(8)}`);
}

// ── 2. Code-version cohort split ─────────────────────────────────────
console.log('\n  ── 2. COHORTS BY OPEN-TIME (when this signal\'s emission code shipped) ──');
const cohorts = [
  { name: 'pre-Option2 (older code)',          gate: (t) => t < OPTION2_TS },
  { name: 'Option2 ship - stale-fix',          gate: (t) => t >= OPTION2_TS && t < STALE_FIX_TS },
  { name: 'stale-fix - tuning',                gate: (t) => t >= STALE_FIX_TS && t < TUNING_TS },
  { name: 'tuning - whale-bypass',             gate: (t) => t >= TUNING_TS && t < WHALE_BYPASS_TS },
  { name: 'whale-bypass - volume-tune', gate: (t) => t >= WHALE_BYPASS_TS && t < VOLUME_TUNE_TS },
  { name: 'volume-tune-and-after (NEWEST)',  gate: (t) => t >= VOLUME_TUNE_TS },
];
console.log(`  ${'Cohort'.padEnd(35)} ${'N'.padStart(5)} ${'Resolved'.padStart(8)} ${'WR'.padStart(5)} ${'AvgRet'.padStart(8)} ${'Active'.padStart(7)}`);
console.log('  ' + '─'.repeat(75));
for (const c of cohorts) {
  let opened = 0, resolved = 0, wins = 0, totalRet = 0, retN = 0, active_ = 0;
  for (const s of all) {
    const t = s.openedAt ? new Date(s.openedAt).getTime() : 0;
    if (!c.gate(t)) continue;
    opened++;
    if (s.outcome === 'win' || s.outcome === 'loss') {
      resolved++;
      if (s.outcome === 'win') wins++;
      if (typeof s.signalReturn === 'number') { totalRet += s.signalReturn; retN++; }
    }
  }
  for (const s of Object.values(active)) {
    const t = s.openedAt ? new Date(s.openedAt).getTime() : 0;
    if (!c.gate(t)) continue;
    opened++;
    active_++;
  }
  const wr = resolved > 0 ? (wins / resolved * 100).toFixed(0) + '%' : '—';
  const avgRet = retN > 0 ? ((totalRet / retN >= 0 ? '+' : '') + (totalRet / retN).toFixed(1) + '%') : '—';
  console.log(`  ${c.name.padEnd(35)} ${String(opened).padStart(5)} ${String(resolved).padStart(8)} ${wr.padStart(5)} ${avgRet.padStart(8)} ${String(active_).padStart(7)}`);
}

// ── 3. Per-path activity in last 7 days ────────────────────────────────
console.log('\n  ── 3. PATH ACTIVITY (last 7 days, by openedAt) ──');
const cutoff7 = daysAgo(7).getTime();
const recent7 = [
  ...all.filter(s => s.openedAt && new Date(s.openedAt).getTime() >= cutoff7),
  ...Object.values(active).filter(s => s.openedAt && new Date(s.openedAt).getTime() >= cutoff7),
];
const byPath = {};
for (const s of recent7) {
  const p = s.signalType || 'unknown';
  if (!byPath[p]) byPath[p] = { opened: 0, resolved: 0, wins: 0, totalRet: 0, retN: 0 };
  const r = byPath[p];
  r.opened++;
  if (s.outcome === 'win' || s.outcome === 'loss') {
    r.resolved++;
    if (s.outcome === 'win') r.wins++;
    if (typeof s.signalReturn === 'number') { r.totalRet += s.signalReturn; r.retN++; }
  }
}
console.log(`  ${'Path'.padEnd(16)} ${'Opened'.padStart(7)} ${'Resolved'.padStart(8)} ${'WR'.padStart(5)} ${'AvgRet'.padStart(8)}`);
console.log('  ' + '─'.repeat(50));
for (const [p, r] of Object.entries(byPath)) {
  const wr = r.resolved > 0 ? (r.wins / r.resolved * 100).toFixed(0) + '%' : '—';
  const avgRet = r.retN > 0 ? ((r.totalRet / r.retN >= 0 ? '+' : '') + (r.totalRet / r.retN).toFixed(1) + '%') : '—';
  console.log(`  ${p.padEnd(16)} ${String(r.opened).padStart(7)} ${String(r.resolved).padStart(8)} ${wr.padStart(5)} ${avgRet.padStart(8)}`);
}

// ── 4. Currently active ───────────────────────────────────────────────
console.log('\n  ── 4. CURRENTLY ACTIVE SIGNALS ──');
const activeArr = Object.values(active).sort((a, b) => (b.openedAt || '').localeCompare(a.openedAt || ''));
console.log(`  Total active: ${activeArr.length}`);
const newest = activeArr.slice(0, 10);
console.log('  10 newest active:');
console.log(`    ${'Type'.padEnd(15)} ${'Conf'.padStart(5)} ${'Tier'.padEnd(8)} ${'Opened'.padEnd(20)} Title`);
for (const s of newest) {
  const conf = (s.confidence || 0).toFixed(0);
  console.log('    ' + (s.signalType || '?').padEnd(15) + ' ' + conf.padStart(5) + ' ' +
    (s.tier || '?').padEnd(8) + ' ' + (s.openedAt || '').slice(0, 19).padEnd(20) + ' ' +
    (s.marketTitle || '').slice(0, 40));
}

// ── 5. New-code cohort detail ──────────────────────────────────────────
console.log('\n  ── 5. NEWEST COHORT (volume-tune and after) DETAIL ──');
const newCodeSignals = [
  ...all.filter(s => s.openedAt && new Date(s.openedAt).getTime() >= VOLUME_TUNE_TS),
  ...Object.values(active).filter(s => s.openedAt && new Date(s.openedAt).getTime() >= VOLUME_TUNE_TS),
];
console.log(`  Total signals opened in NEW-code era: ${newCodeSignals.length}`);
const newResolved = newCodeSignals.filter(s => s.outcome === 'win' || s.outcome === 'loss');
const newActive = newCodeSignals.filter(s => !s.closedAt);
console.log(`  Resolved: ${newResolved.length}    Still active: ${newActive.length}`);
if (newResolved.length > 0) {
  const w = newResolved.filter(s => s.outcome === 'win').length;
  const totalRet = newResolved.reduce((a, s) => a + (typeof s.signalReturn === 'number' ? s.signalReturn : 0), 0);
  const retN = newResolved.filter(s => typeof s.signalReturn === 'number').length;
  console.log(`  Resolved WR: ${(w / newResolved.length * 100).toFixed(0)}%  Avg ret: ${retN > 0 ? ((totalRet / retN >= 0 ? '+' : '') + (totalRet / retN).toFixed(1) + '%') : '—'}`);
  console.log(`  Total cumulative %ret: ${totalRet >= 0 ? '+' : ''}${totalRet.toFixed(0)}%`);
}

// All NEWEST-code signals with outcome
console.log('\n  All signals opened after volume-tune landed:');
console.log(`    ${'Type'.padEnd(15)} ${'Outcome'.padEnd(8)} ${'Ret'.padStart(7)} ${'Opened'.padEnd(20)} Title`);
for (const s of newCodeSignals.sort((a, b) => (a.openedAt || '').localeCompare(b.openedAt || ''))) {
  const ret = typeof s.signalReturn === 'number' ?
    ((s.signalReturn >= 0 ? '+' : '') + s.signalReturn.toFixed(0) + '%') :
    (s.outcome ? '—' : 'open');
  const outcome = s.outcome || 'active';
  console.log('    ' + (s.signalType || '?').padEnd(15) + ' ' + outcome.padEnd(8) + ' ' + ret.padStart(7) + ' ' +
    (s.openedAt || '').slice(0, 19).padEnd(20) + ' ' + (s.marketTitle || '').slice(0, 40));
}

console.log('\n═══════════════════════════════════════════════════════════════════\n');
