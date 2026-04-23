// Deep segmentation analyzer — slices resolved signals across multiple
// dimensions so we can see exactly where EV comes from or leaks out.
//
// Dimensions:
//   - Entry price band (0-20¢ / 20-40¢ / 40-60¢ / 60-80¢ / 80-100¢)
//   - Market category (esports / crypto / sports / politics / other)
//   - Signal duration (time open: < 24h / 1-3d / 3-7d / > 7d)
//   - Wallet count (within convergence signals)
//   - Confidence bucket (< 50 / 50-65 / 65-80 / 80+)
//
// Usage:
//   node scripts/analyze-signals-deep.mjs                   # all signals
//   node scripts/analyze-signals-deep.mjs --type solo       # just solos
//   node scripts/analyze-signals-deep.mjs --since 2026-04-20

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const args = process.argv.slice(2);
const typeFilter = (() => { const i = args.indexOf('--type'); return i >= 0 ? args[i + 1] : null; })();
const sinceMs = (() => { const i = args.indexOf('--since'); if (i < 0) return null; const d = new Date(args[i + 1]); return isNaN(d.getTime()) ? null : d.getTime(); })();

const data = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const history = data.history || [];

const toMs = (t) => {
  if (!t) return 0;
  if (typeof t === 'string') { const d = new Date(t); return isNaN(d.getTime()) ? 0 : d.getTime(); }
  if (typeof t !== 'number' || !isFinite(t)) return 0;
  return t > 1e11 ? t : t * 1000;
};

const ESPORTS_KEYWORDS = ['dota', 'lol', 'league of legends', 'counter-strike', 'valorant', 'csgo', 'cs:go', 'cs2', 'call of duty', 'rocket league', 'overwatch', 'starcraft', 'hearthstone', 'apex legends', 'fortnite'];
const CRYPTO_KEYWORDS = ['bitcoin up or down', 'btc up or down', 'ethereum up or down', 'eth up or down', 'solana up or down', 'sol up or down'];
const SPORTS_KEYWORDS = ['nba', 'nfl', 'nhl', 'mlb', 'soccer', 'fifa', 'premier league', 'liga', 'bundesliga', 'serie a', 'champions league', 'ufc', 'mma', 'boxing', 'tennis', 'golf', 'pga', 'wimbledon', 'open'];
const POLITICS_KEYWORDS = ['election', 'president', 'senator', 'congress', 'parliament', 'prime minister', 'poll', 'nominee', 'primary', 'governor'];

function categorize(title) {
  const t = (title || '').toLowerCase();
  if (ESPORTS_KEYWORDS.some(k => t.includes(k))) return 'esports';
  if (CRYPTO_KEYWORDS.some(k => t.includes(k))) return 'crypto-updown';
  if (SPORTS_KEYWORDS.some(k => t.includes(k))) return 'sports';
  if (POLITICS_KEYWORDS.some(k => t.includes(k))) return 'politics';
  return 'other';
}

function priceBand(price) {
  const p = (price || 0) * 100;
  if (p <= 0) return 'unknown';
  if (p < 20) return ' 0-20¢';
  if (p < 40) return '20-40¢';
  if (p < 60) return '40-60¢';
  if (p < 80) return '60-80¢';
  return '80-100¢';
}

function durationBucket(openedAt, closedAt) {
  const open = toMs(openedAt);
  const close = toMs(closedAt);
  if (!open || !close) return 'unknown';
  const hours = (close - open) / (3600 * 1000);
  if (hours < 24) return '< 24h';
  if (hours < 72) return '1-3d';
  if (hours < 168) return '3-7d';
  return '> 7d';
}

function confBucket(c) {
  if (typeof c !== 'number') return 'unknown';
  if (c < 50) return '< 50';
  if (c < 65) return '50-65';
  if (c < 80) return '65-80';
  return '80+';
}

function walletBucket(n) {
  if (!n) return 'unknown';
  if (n === 1) return '1 (solo)';
  if (n <= 3) return '2-3';
  if (n <= 5) return '4-5';
  if (n <= 7) return '6-7';
  if (n <= 10) return '8-10';
  return '11+';
}

// Filter signals
let filtered = history.filter(s => s.outcome === 'win' || s.outcome === 'loss');
if (typeFilter) filtered = filtered.filter(s => s.signalType === typeFilter);
if (sinceMs) filtered = filtered.filter(s => toMs(s.closedAt) >= sinceMs);

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log(`  Deep signal analytics  (${filtered.length} resolved, ${typeFilter ? 'type=' + typeFilter : 'all types'}${sinceMs ? `, since ${new Date(sinceMs).toISOString().slice(0, 10)}` : ''})`);
console.log('═══════════════════════════════════════════════════════════════════\n');

function windowStats(signals) {
  const n = signals.length;
  const wins = signals.filter(s => s.outcome === 'win').length;
  const losses = n - wins;
  const wr = n > 0 ? (wins / n * 100).toFixed(1) : '—';
  const returns = signals.filter(s => typeof s.signalReturn === 'number').map(s => s.signalReturn);
  const avgRet = returns.length > 0 ? (returns.reduce((a, b) => a + b, 0) / returns.length).toFixed(1) : '—';
  const winReturns = signals.filter(s => s.outcome === 'win' && typeof s.signalReturn === 'number').map(s => s.signalReturn);
  const avgWin = winReturns.length > 0 ? (winReturns.reduce((a, b) => a + b, 0) / winReturns.length).toFixed(0) : '—';
  return { n, wins, losses, wr, avgRet, avgWin };
}

function printTable(title, grouped) {
  console.log(`  ${title}`);
  console.log('  ' + '─'.repeat(75));
  console.log(`    ${'Bucket'.padEnd(14)}  ${'N'.padStart(4)}  ${'W/L'.padStart(12)}  ${'WR'.padStart(7)}  ${'AvgRet'.padStart(8)}  ${'AvgWin'.padStart(8)}  EV sign`);
  const entries = Object.entries(grouped).sort();
  for (const [bucket, signals] of entries) {
    const s = windowStats(signals);
    if (s.n === 0) continue;
    const evSign = s.avgRet === '—' ? '?' : parseFloat(s.avgRet) > 0 ? '✓' : parseFloat(s.avgRet) > -3 ? '⚠' : '✗';
    console.log(`    ${bucket.padEnd(14)}  ${String(s.n).padStart(4)}  ${`${s.wins}W/${s.losses}L`.padStart(12)}  ${(s.wr + '%').padStart(7)}  ${(s.avgRet + '%').padStart(8)}  ${(s.avgWin + '%').padStart(8)}  ${evSign}`);
  }
  console.log();
}

// Group + print each dimension
const byPrice = {}, byCategory = {}, byDuration = {}, byConf = {}, byWallets = {};
for (const s of filtered) {
  const pb = priceBand(s.avgEntryPrice || s.openMarketPrice);
  const cat = categorize(s.marketTitle);
  const dur = durationBucket(s.openedAt, s.closedAt);
  const cb = confBucket(s.confidence);
  const wb = walletBucket(s.peakWallets || s.walletCount || (s.signalType === 'solo' ? 1 : null));
  (byPrice[pb] = byPrice[pb] || []).push(s);
  (byCategory[cat] = byCategory[cat] || []).push(s);
  (byDuration[dur] = byDuration[dur] || []).push(s);
  (byConf[cb] = byConf[cb] || []).push(s);
  (byWallets[wb] = byWallets[wb] || []).push(s);
}

printTable('By ENTRY PRICE band', byPrice);
printTable('By MARKET CATEGORY', byCategory);
printTable('By SIGNAL DURATION (open → close)', byDuration);
printTable('By WALLET COUNT', byWallets);
printTable('By CONFIDENCE bucket', byConf);

// Cross-cut: type × category
console.log('  TYPE × CATEGORY cross-cut');
console.log('  ' + '─'.repeat(75));
console.log(`    ${'Type'.padEnd(10)} ${'Category'.padEnd(14)}  ${'N'.padStart(4)}  ${'W/L'.padStart(12)}  ${'WR'.padStart(7)}  ${'AvgRet'.padStart(8)}`);
const cross = {};
for (const s of filtered) {
  const t = s.signalType || 'unknown';
  const c = categorize(s.marketTitle);
  const k = `${t}__${c}`;
  (cross[k] = cross[k] || []).push(s);
}
const crossKeys = Object.keys(cross).sort();
for (const k of crossKeys) {
  const [t, c] = k.split('__');
  const s = windowStats(cross[k]);
  if (s.n < 5) continue; // skip tiny buckets
  const evSign = s.avgRet === '—' ? '?' : parseFloat(s.avgRet) > 0 ? '✓' : parseFloat(s.avgRet) > -3 ? '⚠' : '✗';
  console.log(`    ${t.padEnd(10)} ${c.padEnd(14)}  ${String(s.n).padStart(4)}  ${`${s.wins}W/${s.losses}L`.padStart(12)}  ${(s.wr + '%').padStart(7)}  ${(s.avgRet + '%').padStart(8)}  ${evSign}`);
}
console.log();

console.log('  LEGEND: ✓ = positive EV (+0%+),  ⚠ = marginal (-3% to 0%),  ✗ = losing (<-3%)');
console.log();
