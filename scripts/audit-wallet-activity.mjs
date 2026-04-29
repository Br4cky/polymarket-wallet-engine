// Audit pool wallets' position-opening activity over the last 14 days.
// Identifies likely arbitrageurs / MMs / churners that should be evicted.
//
// For each wallet:
//   1. Fetch all activity (TRADE + REDEEM events) for last 14 days
//   2. Count unique markets they OPENED a new position on (first BUY)
//   3. Count distinct trade days
//   4. Categorize by positions/day rate
//   5. Cross-reference with stored stats (style, mmScore, attribution)
//
// Output:
//   - Histogram of positions/day across the pool
//   - Top 30 highest-activity wallets with full profile
//   - Wallets that look like MM/arb based on activity + style
//   - Recommended eviction candidates
//
// Usage:
//   node scripts/audit-wallet-activity.mjs              # all 1000 wallets
//   node scripts/audit-wallet-activity.mjs --limit 200  # sample first 200
//   node scripts/audit-wallet-activity.mjs --days 7     # custom window
//
// Runtime: ~30s per 50 wallets at default 2s sleep between API calls.
// 1000 wallets ≈ 8-10 minutes total.

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const args = process.argv.slice(2);
const get = (f, d) => { const i = args.indexOf(f); return i >= 0 && i + 1 < args.length ? args[i + 1] : d; };
const LIMIT = parseInt(get('--limit', '1000'), 10);
const DAYS = parseInt(get('--days', '14'), 10);

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const pool = walletsData.pool || walletsData;

function classifyStyle(stats) {
  if (!stats) return 'unknown';
  if ((stats.dualSideRate || 0) > 0.30 || (stats.mmScore || 0) >= 3) return 'mm-like';
  const tt = stats.totalTrades || 0, um = stats.uniqueMarkets || 0;
  const tpm = um > 0 ? tt / um : 0;
  const sellRatio = stats.sellRatio ?? 1;
  const hold = stats.avgHoldTimeHours || 0;
  if (tpm > 8) return 'churner';
  if (tpm >= 3 && sellRatio > 0.30) return 'averager';
  if (tpm <= 2 && hold < 48) return 'sniper';
  if (sellRatio < 0.15) return 'holder';
  return 'mixed';
}

const wallets = Object.entries(pool)
  .filter(([, w]) => w && w.status !== 'removed')
  .slice(0, LIMIT);

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Wallet activity audit — last ' + DAYS + ' days');
console.log('═══════════════════════════════════════════════════════════════════');
console.log('  Pool size: ' + Object.keys(pool).length);
console.log('  Auditing: ' + wallets.length + ' wallets');
console.log('  Estimated runtime: ' + Math.ceil(wallets.length * 0.5 / 60) + ' minutes');
console.log();

const sinceTs = Math.floor(Date.now() / 1000) - DAYS * 86400;

async function fetchWalletActivity(addr) {
  const url = `https://data-api.polymarket.com/activity?user=${addr.toLowerCase()}&limit=500`;
  try {
    const r = await fetch(url);
    if (!r.ok) return null;
    const events = await r.json();
    return Array.isArray(events) ? events : null;
  } catch { return null; }
}

const results = [];
let processed = 0;
for (const [addr, w] of wallets) {
  processed++;
  if (processed % 25 === 0) {
    console.log(`  Processed ${processed}/${wallets.length}...`);
  }

  const events = await fetchWalletActivity(addr);
  if (!events) {
    results.push({ addr, error: true });
    continue;
  }

  // Filter to last DAYS, BUY only, count unique markets opened
  const recentBuys = events.filter(e =>
    e.type === 'TRADE' && (e.side || '').toUpperCase() === 'BUY' &&
    typeof e.timestamp === 'number' && e.timestamp >= sinceTs
  );

  const uniqueMarketsOpened = new Set(recentBuys.map(e => e.conditionId || e.condition_id || '').filter(Boolean));
  const totalBuys = recentBuys.length;
  const positionsOpened = uniqueMarketsOpened.size;
  const positionsPerDay = positionsOpened / DAYS;
  const buysPerDay = totalBuys / DAYS;

  // Calculate distinct trading days
  const tradeDays = new Set(recentBuys.map(e => Math.floor(e.timestamp / 86400)));

  results.push({
    addr,
    style: classifyStyle(w.stats),
    score: w.score || 0,
    decidedROI: w.stats?.decidedROI,
    winRate: w.stats?.winRate,
    mmScore: w.stats?.mmScore || 0,
    sellRatio: w.stats?.sellRatio,
    dualSideRate: w.stats?.dualSideRate || 0,
    attrMultiplier: w.scoreComponents?.attrMultiplier,
    attrSignals: w.scoreComponents?.attrSignals,
    attrAvgReturn: w.scoreComponents?.attrAvgReturn,
    positionsOpened,
    totalBuys,
    positionsPerDay,
    buysPerDay,
    tradeDays: tradeDays.size,
  });
}

const valid = results.filter(r => !r.error);
console.log('\n  Fetched data for ' + valid.length + ' / ' + wallets.length + ' wallets');
console.log();

// ── Histogram ──────────────────────────────────────────────────────
console.log('  ── Position-opening rate distribution (last ' + DAYS + ' days) ──');
const bands = [
  ['0 (dormant)', r => r.positionsPerDay === 0],
  ['<1/day', r => r.positionsPerDay > 0 && r.positionsPerDay < 1],
  ['1-3/day', r => r.positionsPerDay >= 1 && r.positionsPerDay < 3],
  ['3-10/day', r => r.positionsPerDay >= 3 && r.positionsPerDay < 10],
  ['10-25/day', r => r.positionsPerDay >= 10 && r.positionsPerDay < 25],
  ['25-50/day', r => r.positionsPerDay >= 25 && r.positionsPerDay < 50],
  ['50-100/day', r => r.positionsPerDay >= 50 && r.positionsPerDay < 100],
  ['100+/day', r => r.positionsPerDay >= 100],
];
for (const [label, pred] of bands) {
  const matches = valid.filter(pred);
  if (matches.length === 0) continue;
  const pct = (matches.length / valid.length * 100).toFixed(0);
  const bar = '█'.repeat(Math.round(matches.length / 5));
  console.log('  ' + label.padEnd(15) + String(matches.length).padStart(4) + '  ' + pct.padStart(3) + '%  ' + bar);
}

// ── Top 30 highest-volume ─────────────────────────────────────────
console.log('\n  ── Top 30 highest-volume wallets ──');
valid.sort((a, b) => b.positionsPerDay - a.positionsPerDay);
console.log('  ' + 'Wallet'.padEnd(14) + ' ' + 'Pos/d'.padStart(6) + ' ' + 'Buys/d'.padStart(7) + ' ' + 'Style'.padEnd(10) + ' ' + 'Score'.padStart(6) + ' ' + 'mm'.padStart(3) + ' ' + 'WR'.padStart(5) + ' ' + 'attrSigs'.padStart(8) + ' ' + 'attrRet'.padStart(8));
for (const r of valid.slice(0, 30)) {
  console.log('  ' + r.addr.slice(0, 12).padEnd(14) +
    ' ' + r.positionsPerDay.toFixed(1).padStart(6) +
    ' ' + r.buysPerDay.toFixed(1).padStart(7) +
    ' ' + r.style.padEnd(10) +
    ' ' + r.score.toFixed(1).padStart(6) +
    ' ' + String(r.mmScore).padStart(3) +
    ' ' + (r.winRate != null ? (r.winRate * 100).toFixed(0) + '%' : '—').padStart(5) +
    ' ' + String(r.attrSignals || 0).padStart(8) +
    ' ' + (r.attrAvgReturn != null ? ((r.attrAvgReturn >= 0 ? '+' : '') + (r.attrAvgReturn * 100).toFixed(0) + '%') : '—').padStart(8));
}

// ── Eviction candidates: high volume + bad signal performance ─────
console.log('\n  ── EVICTION CANDIDATES — high volume + negative attribution ──');
const evictionCandidates = valid.filter(r =>
  r.positionsPerDay >= 5
  && (r.attrSignals >= 5 && r.attrAvgReturn != null && r.attrAvgReturn < 0)
);
console.log('  ' + evictionCandidates.length + ' wallets match (≥5 pos/day AND ≥5 signals contributed AND avg signal return negative)');
if (evictionCandidates.length > 0) {
  console.log('  ' + 'Wallet'.padEnd(14) + ' ' + 'Pos/d'.padStart(6) + ' ' + 'Style'.padEnd(10) + ' ' + 'Score'.padStart(6) + ' ' + 'AttrSigs'.padStart(8) + ' ' + 'AttrRet'.padStart(8) + ' ' + 'AttrMult'.padStart(8));
  for (const r of evictionCandidates) {
    console.log('  ' + r.addr.slice(0, 12).padEnd(14) +
      ' ' + r.positionsPerDay.toFixed(1).padStart(6) +
      ' ' + r.style.padEnd(10) +
      ' ' + r.score.toFixed(1).padStart(6) +
      ' ' + String(r.attrSignals).padStart(8) +
      ' ' + ((r.attrAvgReturn >= 0 ? '+' : '') + (r.attrAvgReturn * 100).toFixed(0) + '%').padStart(8) +
      ' ' + (r.attrMultiplier != null ? r.attrMultiplier.toFixed(2) : '—').padStart(8));
  }
}

// ── MM-pattern flags ──────────────────────────────────────────────
console.log('\n  ── HIGH-VOLUME MM-PATTERN wallets (irrespective of attribution) ──');
const mmPattern = valid.filter(r =>
  r.positionsPerDay >= 10
  && (r.style === 'mm-like' || r.style === 'churner' || r.dualSideRate > 0.20 || r.mmScore >= 2)
);
console.log('  ' + mmPattern.length + ' wallets with ≥10 pos/day AND MM/churner pattern');
if (mmPattern.length > 0 && mmPattern.length <= 50) {
  console.log('  ' + 'Wallet'.padEnd(14) + ' ' + 'Pos/d'.padStart(6) + ' ' + 'Style'.padEnd(10) + ' ' + 'mmScore'.padStart(7) + ' ' + 'dualSide'.padStart(8) + ' ' + 'sellRatio'.padStart(9));
  for (const r of mmPattern.slice(0, 30)) {
    console.log('  ' + r.addr.slice(0, 12).padEnd(14) +
      ' ' + r.positionsPerDay.toFixed(1).padStart(6) +
      ' ' + r.style.padEnd(10) +
      ' ' + String(r.mmScore).padStart(7) +
      ' ' + (r.dualSideRate * 100).toFixed(0).padStart(7) + '%' +
      ' ' + (r.sellRatio != null ? r.sellRatio.toFixed(2) : '—').padStart(9));
  }
}

// ── Summary ───────────────────────────────────────────────────────
console.log('\n  ── SUMMARY ──');
const dormant = valid.filter(r => r.positionsPerDay === 0).length;
const heavy = valid.filter(r => r.positionsPerDay >= 25).length;
const extreme = valid.filter(r => r.positionsPerDay >= 100).length;
const totalEvictableByVolume = valid.filter(r => r.positionsPerDay >= 25).length;
console.log('  Dormant in last ' + DAYS + 'd:                ' + dormant);
console.log('  Heavy volume (≥25/day):           ' + heavy);
console.log('  Extreme volume (≥100/day):        ' + extreme);
console.log('  Eviction-by-attribution candidates: ' + evictionCandidates.length);
console.log('  Eviction-by-MM-pattern candidates:  ' + mmPattern.length);
console.log();
