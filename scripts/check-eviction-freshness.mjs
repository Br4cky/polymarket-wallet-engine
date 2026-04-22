// Before evicting on current stats, verify how fresh those stats are.
// Shows rescore-age distribution of the wallets that would be evicted
// by apply-gates.mjs, so you can confirm you're not acting on stale data.

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const MIN_ROI = 0.10;
const MIN_RESOLVED = 20;
const MIN_CAPITAL = 5000;
const DORMANCY_DAYS = 30;

const walletsFile = path.join(ROOT, 'data/wallets.json.gz');
const rawJson = JSON.parse(zlib.gunzipSync(fs.readFileSync(walletsFile)).toString());
const pool = rawJson.pool || rawJson;
const active = Object.values(pool).filter(w => w && w.status !== 'removed');
const now = Date.now();

const evictees = [];
for (const w of active) {
  const stats = w.stats;
  if (!stats || stats.decidedROI == null) continue;

  const cap = stats.decidedCapital || 0;
  const resolved = (stats.decidedWins || 0) + (stats.decidedLosses || 0) || stats.resolvedMarkets || 0;
  const daysSinceLastTrade = stats.lastTradeTs > 0 ? (Date.now() / 1000 - stats.lastTradeTs) / 86400 : Infinity;

  let reason = null;
  if (daysSinceLastTrade > DORMANCY_DAYS) reason = 'dormant';
  else if (cap < MIN_CAPITAL) reason = 'low_capital';
  else if (resolved < MIN_RESOLVED) reason = 'low_resolved';
  else if (stats.decidedROI < MIN_ROI) reason = 'low_roi';

  if (reason) {
    const lastScoredMs = w.lastScored ? new Date(w.lastScored).getTime() : 0;
    const ageHours = lastScoredMs > 0 ? Math.floor((now - lastScoredMs) / 3600000) : null;
    evictees.push({ addr: w.address, reason, ageHours, lastScored: w.lastScored });
  }
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Eviction freshness audit');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log(`  Total wallets that would be evicted: ${evictees.length}\n`);

// Bucket by rescore age
const buckets = {
  '≤ 6h':   0,
  '≤ 24h':  0,
  '≤ 48h':  0,
  '≤ 72h':  0,
  '≤ 7d':   0,
  '> 7d':   0,
  'never':  0,
};
for (const e of evictees) {
  if (e.ageHours == null) buckets['never']++;
  else if (e.ageHours <= 6) buckets['≤ 6h']++;
  else if (e.ageHours <= 24) buckets['≤ 24h']++;
  else if (e.ageHours <= 48) buckets['≤ 48h']++;
  else if (e.ageHours <= 72) buckets['≤ 72h']++;
  else if (e.ageHours <= 168) buckets['≤ 7d']++;
  else buckets['> 7d']++;
}

console.log('  Rescore age distribution:');
for (const [band, count] of Object.entries(buckets)) {
  const pct = evictees.length > 0 ? (count / evictees.length * 100).toFixed(1) : '0.0';
  const bar = '█'.repeat(Math.round(count / evictees.length * 40));
  console.log(`    ${band.padEnd(7)}  ${String(count).padStart(4)}  (${pct}%)  ${bar}`);
}
console.log();

// Same but per eviction reason
const byReason = {};
for (const e of evictees) {
  if (!byReason[e.reason]) byReason[e.reason] = { total: 0, stale: 0 };
  byReason[e.reason].total++;
  if (e.ageHours == null || e.ageHours > 48) byReason[e.reason].stale++;
}
console.log('  Per-reason freshness (stale = rescored > 48h ago or never):');
for (const [reason, info] of Object.entries(byReason)) {
  const stalePct = (info.stale / info.total * 100).toFixed(0);
  console.log(`    ${reason.padEnd(14)}  total=${String(info.total).padStart(4)}   stale=${String(info.stale).padStart(4)} (${stalePct}%)`);
}
console.log();

// Sample the stalest evictions so you can eyeball specific ones
const stalest = evictees
  .filter(e => e.ageHours != null)
  .sort((a, b) => b.ageHours - a.ageHours)
  .slice(0, 10);

if (stalest.length > 0) {
  console.log('  Top 10 stalest wallets being evicted (check these specifically):');
  stalest.forEach(e => {
    const days = (e.ageHours / 24).toFixed(1);
    console.log(`    ${e.addr.slice(0, 14)}...  reason=${e.reason.padEnd(14)}  last_scored=${days}d ago`);
  });
  console.log();
}

const never = evictees.filter(e => e.ageHours == null);
if (never.length > 0) {
  console.log(`  ${never.length} wallets have NO lastScored timestamp (never rescored).`);
  console.log(`  First 5:`);
  never.slice(0, 5).forEach(e => {
    console.log(`    ${e.addr.slice(0, 14)}...  reason=${e.reason}`);
  });
  console.log();
}
