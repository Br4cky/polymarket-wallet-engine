// Time-pattern analyzer — when do our signals win/lose?
//
// Dimensions:
//   - Day of week (opened)
//   - Hour of day UTC (opened)
//   - Signal duration (open → close)
//   - Rolling 7-day window performance over time
//
// Usage:
//   node scripts/time-patterns.mjs
//   node scripts/time-patterns.mjs --since 2026-04-01

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const args = process.argv.slice(2);
const sinceMs = (() => { const i = args.indexOf('--since'); if (i < 0) return null; const d = new Date(args[i + 1]); return isNaN(d.getTime()) ? null : d.getTime(); })();

const data = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const history = data.history || [];

const toMs = (t) => {
  if (!t) return 0;
  if (typeof t === 'string') { const d = new Date(t); return isNaN(d.getTime()) ? 0 : d.getTime(); }
  if (typeof t !== 'number' || !isFinite(t)) return 0;
  return t > 1e11 ? t : t * 1000;
};

let resolved = history.filter(s => s.outcome === 'win' || s.outcome === 'loss');
if (sinceMs) resolved = resolved.filter(s => toMs(s.closedAt) >= sinceMs);

function windowStats(signals) {
  const n = signals.length;
  const wins = signals.filter(s => s.outcome === 'win').length;
  const losses = n - wins;
  const wr = n > 0 ? (wins / n * 100).toFixed(1) : '—';
  const rets = signals.filter(s => typeof s.signalReturn === 'number').map(s => s.signalReturn);
  const avgRet = rets.length > 0 ? (rets.reduce((a, b) => a + b, 0) / rets.length).toFixed(1) : '—';
  return { n, wins, losses, wr, avgRet };
}

function printTable(title, grouped) {
  console.log(`  ${title}`);
  console.log('  ' + '─'.repeat(70));
  console.log(`    ${'Bucket'.padEnd(14)}  ${'N'.padStart(4)}  ${'W/L'.padStart(12)}  ${'WR'.padStart(7)}  ${'AvgRet'.padStart(8)}`);
  for (const [k, arr] of Object.entries(grouped)) {
    const s = windowStats(arr);
    if (s.n === 0) continue;
    const sign = s.avgRet === '—' ? '' : parseFloat(s.avgRet) > 0 ? '  ✓' : parseFloat(s.avgRet) < -3 ? '  ✗' : '  ⚠';
    console.log(`    ${k.padEnd(14)}  ${String(s.n).padStart(4)}  ${`${s.wins}W/${s.losses}L`.padStart(12)}  ${(s.wr + '%').padStart(7)}  ${(s.avgRet + '%').padStart(8)}${sign}`);
  }
  console.log();
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log(`  Time-pattern analysis — ${resolved.length} resolved signals${sinceMs ? ` since ${new Date(sinceMs).toISOString().slice(0, 10)}` : ''}`);
console.log('═══════════════════════════════════════════════════════════════════\n');

// By day of week (opened)
const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const byDow = Object.fromEntries(days.map(d => [d, []]));
for (const s of resolved) {
  const t = toMs(s.openedAt);
  if (!t) continue;
  const d = days[new Date(t).getUTCDay()];
  byDow[d].push(s);
}
printTable('By DAY OF WEEK (signal opened)', byDow);

// By hour of day UTC (opened)
const byHour = {};
for (let h = 0; h < 24; h += 4) byHour[`${String(h).padStart(2, '0')}:00-${String(h + 3).padStart(2, '0')}:59`] = [];
for (const s of resolved) {
  const t = toMs(s.openedAt);
  if (!t) continue;
  const h = new Date(t).getUTCHours();
  const bucketHour = Math.floor(h / 4) * 4;
  const key = `${String(bucketHour).padStart(2, '0')}:00-${String(bucketHour + 3).padStart(2, '0')}:59`;
  byHour[key].push(s);
}
printTable('By HOUR OF DAY UTC (signal opened, 4h buckets)', byHour);

// By duration
const byDuration = { '< 1h': [], '1-6h': [], '6-24h': [], '1-3d': [], '3-7d': [], '> 7d': [] };
for (const s of resolved) {
  const open = toMs(s.openedAt), close = toMs(s.closedAt);
  if (!open || !close) continue;
  const hours = (close - open) / 3600000;
  if (hours < 1) byDuration['< 1h'].push(s);
  else if (hours < 6) byDuration['1-6h'].push(s);
  else if (hours < 24) byDuration['6-24h'].push(s);
  else if (hours < 72) byDuration['1-3d'].push(s);
  else if (hours < 168) byDuration['3-7d'].push(s);
  else byDuration['> 7d'].push(s);
}
printTable('By SIGNAL DURATION (open → close)', byDuration);

// Rolling 7-day windows (chronological)
const resolvedSorted = [...resolved].sort((a, b) => toMs(a.closedAt) - toMs(b.closedAt));
const windowMs = 7 * 86400 * 1000;
const windows = {};
for (const s of resolvedSorted) {
  const t = toMs(s.closedAt);
  if (!t) continue;
  const weekStart = new Date(t - ((new Date(t).getUTCDay()) * 86400 * 1000));
  const key = weekStart.toISOString().slice(0, 10);
  (windows[key] = windows[key] || []).push(s);
}
console.log(`  Rolling 7-day performance (by ISO week closed)`);
console.log('  ' + '─'.repeat(70));
console.log(`    ${'Week start'.padEnd(14)}  ${'N'.padStart(4)}  ${'W/L'.padStart(12)}  ${'WR'.padStart(7)}  ${'AvgRet'.padStart(8)}`);
for (const [k, arr] of Object.entries(windows)) {
  const s = windowStats(arr);
  const sign = s.avgRet === '—' ? '' : parseFloat(s.avgRet) > 0 ? '  ✓' : parseFloat(s.avgRet) < -3 ? '  ✗' : '  ⚠';
  console.log(`    ${k.padEnd(14)}  ${String(s.n).padStart(4)}  ${`${s.wins}W/${s.losses}L`.padStart(12)}  ${(s.wr + '%').padStart(7)}  ${(s.avgRet + '%').padStart(8)}${sign}`);
}
console.log();
console.log('  Trend indicator — watch for WR/AvgRet climbing in post-consolidation weeks.');
console.log();
