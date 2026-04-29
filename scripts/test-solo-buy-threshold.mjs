// Test the SOLO_MIN_BUY_SIZE threshold by looking at all recent BUYs from
// solo-eligible wallets and computing what would emit at different
// thresholds.
//
// Quality proxy: the contributing wallet's own stats (singleSideROI,
// winRate, decidedROI) — signals from a 90% WR wallet are likely 90% WR
// regardless of bet size, so we use wallet-level quality as a proxy for
// signal-level quality.
//
// Usage:
//   node scripts/test-solo-buy-threshold.mjs              # all solo-eligible
//   node scripts/test-solo-buy-threshold.mjs --limit 50

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const args = process.argv.slice(2);
const get = (f, d) => { const i = args.indexOf(f); return i >= 0 && i + 1 < args.length ? args[i + 1] : d; };
const LIMIT = parseInt(get('--limit', '999'), 10);
const DAYS = parseInt(get('--days', '14'), 10);

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const pool = walletsData.pool || walletsData;

const ALLOWED_CATEGORIES = new Set(['tennis','nba','mma','weather','crypto-updown','crypto-other','mlb','nfl','macro','ai-tech','token-launch','news-event','soccer']);
const SOLO_ALLOWED = new Set(['sniper', 'averager', 'churner']);

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

function classify(q) {
  q = (q || '').toLowerCase();
  if (!q) return 'other';
  if (/dota|lol|league|valorant|cs:?go|cs2|counter-strike|call of duty|rocket league|overwatch|starcraft|hearthstone|apex|fortnite|pubg/.test(q)) return 'esports';
  if (/\blaunch a token|\btoken launch|\btge\b|token on /.test(q)) return 'token-launch';
  if (/bitcoin|btc|ethereum|eth|solana|sol\b|doge|xrp|crypto|coin/.test(q)) {
    if (/reach|above|below|hit|close|\$|\sup\b|\sdown\b|end above|end below/.test(q)) return 'crypto-updown';
    return 'crypto-other';
  }
  if (/nhl|stanley cup| hockey /.test(q)) return 'nhl';
  if (/nba|lakers|celtics|warriors|knicks|nba playoffs/.test(q)) return 'nba';
  if (/nfl|super bowl|touchdown|quarterback/.test(q)) return 'nfl';
  if (/mlb|baseball|world series/.test(q)) return 'mlb';
  if (/epl|premier league|champions league|la liga|bundesliga|serie a|manchester|arsenal|liverpool|chelsea/.test(q)) return 'soccer';
  if (/tennis|wimbledon|us open|french open|atp|wta/.test(q)) return 'tennis';
  if (/ufc|\bmma\b|fight night/.test(q)) return 'mma';
  if (/ pga |golf|masters tournament/.test(q)) return 'golf';
  if (/f1|formula 1|grand prix/.test(q)) return 'f1';
  if (/trump|biden|harris|election|senate|house race|republican|democrat|congressional/.test(q)) return 'politics';
  if (/ai\b|openai|anthropic|gpt|gemini|llm|tech|apple|google|microsoft|nvidia/.test(q)) return 'ai-tech';
  if (/weather|temperature|hurricane|tornado|snow|rainfall/.test(q)) return 'weather';
  if (/fed rate|fomc|inflation|cpi|ppi|gdp|recession|jobs report|unemployment|nonfarm|ipo|earnings|revenue|guidance|\beps\b/.test(q)) return 'macro';
  if (/spacex|starship|mission|nasa|rocket launch|announce|statement|\bsay\b|\bsays\b|tweet|post|comment/.test(q)) return 'news-event';
  if (/\bwill .+ by |\bwill .+ before |\bwill .+ on /.test(q)) return 'news-event';
  return 'other';
}

// Identify solo-eligible wallets (passing all gates EXCEPT buy size)
const soloEligible = [];
for (const [addr, w] of Object.entries(pool)) {
  if (!w || w.status === 'removed') continue;
  const style = classifyStyle(w.stats);
  const score = w.score || 0;
  const wr = w.stats?.recentWinRate || w.stats?.winRate || 0;
  const resolved = w.stats?.resolvedMarkets || 0;
  const safetyOK = !w.stats?.isLikelyMM && !w.stats?.isMeanPickerShape && w.stats?.alphaVerdict !== 'fails';
  const styleOK = SOLO_ALLOWED.has(style);
  const scoreOK = score >= 25;
  const wrOK = wr >= 0.55;
  const resolvedOK = resolved >= 50;
  if (styleOK && scoreOK && wrOK && resolvedOK && safetyOK) {
    soloEligible.push({ addr, w, style });
  }
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  SOLO_MIN_BUY_SIZE threshold test');
console.log('═══════════════════════════════════════════════════════════════════');
console.log('  Solo-eligible wallets in pool: ' + soloEligible.length);
console.log('  Sampling: ' + Math.min(LIMIT, soloEligible.length));
console.log('  Window: last ' + DAYS + ' days');
console.log('  Fetching activity...');
console.log();

const sinceTs = Math.floor(Date.now() / 1000) - DAYS * 86400;

async function fetchActivity(addr) {
  const url = `https://data-api.polymarket.com/activity?user=${addr.toLowerCase()}&limit=500`;
  try {
    const r = await fetch(url);
    if (!r.ok) return null;
    return await r.json();
  } catch { return null; }
}

const allBuys = [];  // each = { addr, style, walletROI, walletWR, buySize, category, conditionId, marketTitle, timestamp }
let processed = 0;

for (const { addr, w, style } of soloEligible.slice(0, LIMIT)) {
  processed++;
  if (processed % 25 === 0) console.log('  Processed ' + processed + '/' + Math.min(LIMIT, soloEligible.length));

  const events = await fetchActivity(addr);
  if (!events) continue;

  const seenMarkets = new Set();
  const buys = events.filter(e => e.type === 'TRADE' && (e.side || '').toUpperCase() === 'BUY' && e.timestamp >= sinceTs);

  for (const b of buys) {
    const cid = b.conditionId || b.condition_id || '';
    if (seenMarkets.has(cid)) continue;
    seenMarkets.add(cid);

    const cat = classify(b.title || '');
    if (!ALLOWED_CATEGORIES.has(cat)) continue;  // category gate

    const buySize = parseFloat(b.size || 0) * parseFloat(b.price || 0);

    allBuys.push({
      addr,
      style,
      walletROI: w.stats?.singleSideROI ?? w.stats?.decidedROI ?? null,
      walletWR: w.stats?.winRate ?? null,
      walletScore: w.score || 0,
      buySize,
      category: cat,
      timestamp: b.timestamp,
    });
  }
}

console.log('\n  Total qualifying solo buys (passing category whitelist + uniqueness): ' + allBuys.length);
console.log();

// ── Buy-size distribution ─────────────────────────────────────────
console.log('  ── Buy-size distribution of solo-eligible buys ──');
const sizeBands = [
  ['<$50', b => b.buySize < 50],
  ['$50-100', b => b.buySize >= 50 && b.buySize < 100],
  ['$100-250', b => b.buySize >= 100 && b.buySize < 250],
  ['$250-500', b => b.buySize >= 250 && b.buySize < 500],
  ['$500-1000', b => b.buySize >= 500 && b.buySize < 1000],
  ['$1000-5000', b => b.buySize >= 1000 && b.buySize < 5000],
  ['$5000+', b => b.buySize >= 5000],
];
for (const [label, pred] of sizeBands) {
  const matches = allBuys.filter(pred);
  if (matches.length === 0) continue;
  const pct = (matches.length / allBuys.length * 100).toFixed(1);
  const bar = '█'.repeat(Math.round(matches.length / 30));
  console.log('  ' + label.padEnd(13) + String(matches.length).padStart(5) + '  ' + pct.padStart(4) + '%  ' + bar);
}

// ── Cumulative emissions per threshold ────────────────────────────
console.log('\n  ── Emissions per threshold (cumulative) ──');
console.log('  ' + 'Threshold'.padEnd(13) + 'Signals'.padStart(8) + 'Per/day'.padStart(9) + '  AvgWalletWR'.padStart(13) + '  AvgWalletROI'.padStart(14));
const thresholds = [0, 50, 100, 250, 500, 1000, 2500];
for (const t of thresholds) {
  const matches = allBuys.filter(b => b.buySize >= t);
  if (matches.length === 0) continue;
  const avgWR = matches.reduce((s, b) => s + (b.walletWR || 0), 0) / matches.length;
  const avgROI = matches.filter(b => b.walletROI != null).reduce((s, b) => s + b.walletROI, 0) / Math.max(1, matches.filter(b => b.walletROI != null).length);
  const perDay = matches.length / DAYS;
  console.log('  ' + ('≥$' + t).padEnd(13) +
    String(matches.length).padStart(8) +
    perDay.toFixed(1).padStart(9) +
    ' ' + ((avgWR * 100).toFixed(0) + '%').padStart(13) +
    ' ' + ((avgROI * 100).toFixed(0) + '%').padStart(14));
}

// ── Quality at each threshold band (incremental, not cumulative) ──
console.log('\n  ── Incremental signals at each band — quality of new admits ──');
console.log('  ' + 'Band'.padEnd(13) + 'Signals'.padStart(8) + '  WalletWR'.padStart(11) + '  WalletROI'.padStart(11) + '  Categories');
const bandsForIncr = [
  ['<$50', b => b.buySize < 50],
  ['$50-100', b => b.buySize >= 50 && b.buySize < 100],
  ['$100-250', b => b.buySize >= 100 && b.buySize < 250],
  ['$250-500', b => b.buySize >= 250 && b.buySize < 500],
  ['≥$500 (current)', b => b.buySize >= 500],
];
for (const [label, pred] of bandsForIncr) {
  const matches = allBuys.filter(pred);
  if (matches.length === 0) {
    console.log('  ' + label.padEnd(13) + '0'.padStart(8));
    continue;
  }
  const avgWR = matches.reduce((s, b) => s + (b.walletWR || 0), 0) / matches.length;
  const avgROI = matches.filter(b => b.walletROI != null).reduce((s, b) => s + b.walletROI, 0) / Math.max(1, matches.filter(b => b.walletROI != null).length);
  const catCounts = {};
  for (const m of matches) catCounts[m.category] = (catCounts[m.category] || 0) + 1;
  const topCats = Object.entries(catCounts).sort((a, b) => b[1] - a[1]).slice(0, 3).map(([c, n]) => c + '(' + n + ')').join(' ');
  console.log('  ' + label.padEnd(13) +
    String(matches.length).padStart(8) +
    ' ' + ((avgWR * 100).toFixed(0) + '%').padStart(10) +
    ' ' + ((avgROI * 100).toFixed(0) + '%').padStart(10) +
    '  ' + topCats);
}

// ── Recommendation ────────────────────────────────────────────────
console.log('\n  ── Recommendation logic ──');
console.log('  - If lower-band buys have strong wallet WR/ROI, lowering threshold safely unlocks signals.');
console.log('  - If they\'re from MM-style bot wallets (low avgROI on those buys), keep threshold.');
console.log('  - Watch the categories — if it\'s mostly crypto-updown bots in lower bands, those');
console.log('    signals will get killed by MIN_HOURS_TO_RESOLUTION anyway, so lowering doesn\'t help.');
console.log();
