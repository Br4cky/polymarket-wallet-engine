// Market sub-topic deep dive — within the broad categories (sports, other,
// esports, crypto), which sub-markets are actually winning?
//
// Sports itself is profitable (+32.5% avg return). But is that mostly NBA?
// Soccer? Tennis? We slice finer to find the gold.
//
// Usage:
//   node scripts/market-deep-dive.mjs
//   node scripts/market-deep-dive.mjs --category sports
//   node scripts/market-deep-dive.mjs --since 2026-04-01

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const args = process.argv.slice(2);
const get = (f, d) => { const i = args.indexOf(f); return i >= 0 && i + 1 < args.length ? args[i + 1] : d; };
const CATEGORY = get('--category', null);
const sinceMs = (() => { const s = get('--since', null); if (!s) return null; const d = new Date(s); return isNaN(d.getTime()) ? null : d.getTime(); })();

const data = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const history = data.history || [];

const toMs = (t) => {
  if (!t) return 0;
  if (typeof t === 'string') { const d = new Date(t); return isNaN(d.getTime()) ? 0 : d.getTime(); }
  if (typeof t !== 'number' || !isFinite(t)) return 0;
  return t > 1e11 ? t : t * 1000;
};

// Sub-topic detection via title keywords. More granular than the broad
// category used in analyze-signals-deep.mjs.
const SUBTOPICS = [
  ['NBA',           ['nba', 'basketball', 'lakers', 'celtics', 'nuggets', 'bucks', 'knicks', 'warriors', 'heat', 'mavs', 'mavericks', 'suns', 'clippers', 'sixers', '76ers']],
  ['NFL',           ['nfl', 'chiefs', 'eagles', 'cowboys', 'patriots', 'packers', 'ravens', '49ers', 'bills', 'bengals']],
  ['MLB',           ['mlb', 'yankees', 'red sox', 'dodgers', 'mets', 'cubs', 'phillies']],
  ['NHL',           ['nhl', 'hockey', 'rangers', 'bruins', 'lightning', 'oilers', 'maple leafs']],
  ['Soccer / EPL',  ['premier league', 'man city', 'manchester', 'chelsea', 'arsenal', 'liverpool', 'tottenham', 'spurs', 'epl']],
  ['Soccer / Other',['bundesliga', 'la liga', 'serie a', 'mls', 'champions league', 'europa']],
  ['Tennis',        ['tennis', 'djokovic', 'nadal', 'alcaraz', 'swiatek', 'madrid open', 'miami open', 'wimbledon', 'us open', 'australian open', 'french open', 'roland']],
  ['UFC / MMA',     ['ufc', 'mma']],
  ['Boxing',        ['boxing']],
  ['Golf / PGA',    ['pga', 'golf', 'masters']],
  ['F1 / Racing',   ['formula 1', 'f1', 'grand prix', 'nascar']],
  ['Esports',       ['dota', 'lol', 'league of legends', 'counter-strike', 'valorant', 'csgo', 'cs:go', 'cs2', 'call of duty', 'rocket league', 'overwatch', 'starcraft']],
  ['Crypto up/down',['bitcoin up or down', 'btc up or down', 'eth up or down', 'solana up or down']],
  ['Crypto other',  ['bitcoin', 'btc', 'ethereum', 'eth', 'solana', 'sol price', 'crypto']],
  ['US Politics',   ['trump', 'biden', 'harris', 'president', 'senate', 'congress', 'republican', 'democrat', 'gop', 'election day', 'electoral']],
  ['Global politics',['election', 'parliament', 'prime minister', 'chancellor']],
  ['Entertainment', ['grammy', 'oscar', 'emmy', 'billboard', 'movie', 'box office', 'netflix', 'album']],
  ['AI / Tech',     ['openai', 'gpt', 'anthropic', 'claude', 'gemini', 'sam altman', 'ai will', 'ai to', 'chatgpt']],
  ['Weather',       ['temperature', 'rain', 'snow', 'weather', 'hurricane']],
];

function subTopic(title) {
  if (!title) return 'Unknown';
  const t = title.toLowerCase();
  for (const [label, keys] of SUBTOPICS) {
    if (keys.some(k => t.includes(k))) return label;
  }
  return 'Other';
}

let resolved = history.filter(s => s.outcome === 'win' || s.outcome === 'loss');
if (sinceMs) resolved = resolved.filter(s => toMs(s.closedAt) >= sinceMs);

function stat(signals) {
  const n = signals.length;
  const wins = signals.filter(s => s.outcome === 'win').length;
  const losses = n - wins;
  const wr = n > 0 ? (wins / n * 100) : 0;
  const rets = signals.filter(s => typeof s.signalReturn === 'number').map(s => s.signalReturn);
  const avgRet = rets.length > 0 ? (rets.reduce((a, b) => a + b, 0) / rets.length) : null;
  const winRets = signals.filter(s => s.outcome === 'win' && typeof s.signalReturn === 'number').map(s => s.signalReturn);
  const avgWin = winRets.length > 0 ? (winRets.reduce((a, b) => a + b, 0) / winRets.length) : null;
  return { n, wins, losses, wr, avgRet, avgWin };
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log(`  Market sub-topic deep dive — ${resolved.length} resolved${sinceMs ? ` since ${new Date(sinceMs).toISOString().slice(0, 10)}` : ''}`);
console.log('═══════════════════════════════════════════════════════════════════\n');

// Group by sub-topic
const byTopic = {};
for (const s of resolved) {
  const t = subTopic(s.marketTitle);
  (byTopic[t] = byTopic[t] || []).push(s);
}

// Sort by avgRet
const topicRows = Object.entries(byTopic)
  .map(([topic, sigs]) => ({ topic, ...stat(sigs) }))
  .filter(r => r.n >= 5) // hide tiny samples
  .sort((a, b) => (b.avgRet ?? -999) - (a.avgRet ?? -999));

console.log(`  ${'Sub-topic'.padEnd(20)}  ${'N'.padStart(4)}  ${'W/L'.padStart(12)}  ${'WR'.padStart(6)}  ${'AvgRet'.padStart(9)}  ${'AvgWin'.padStart(8)}`);
console.log('  ' + '─'.repeat(75));
for (const r of topicRows) {
  const avgRetS = r.avgRet !== null ? (r.avgRet >= 0 ? '+' : '') + r.avgRet.toFixed(1) + '%' : '—';
  const avgWinS = r.avgWin !== null ? r.avgWin.toFixed(0) + '%' : '—';
  const sign = r.avgRet === null ? '' : r.avgRet > 0 ? '  ✓' : r.avgRet < -3 ? '  ✗' : '  ⚠';
  console.log(`  ${r.topic.padEnd(20)}  ${String(r.n).padStart(4)}  ${`${r.wins}W/${r.losses}L`.padStart(12)}  ${(r.wr.toFixed(0) + '%').padStart(6)}  ${avgRetS.padStart(9)}  ${avgWinS.padStart(8)}${sign}`);
}
console.log();

console.log('  Sorted by avg return, high → low. ✓ profitable · ⚠ marginal · ✗ losing.');
console.log('  Next action: categories at top of this list should be prioritized,');
console.log('  bottom entries considered for exclusion in EXCLUDED_KEYWORDS.');
console.log();
