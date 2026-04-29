// Bulk-evict bot-pattern wallets that occupy pool slots without
// contributing copyable signals.
//
// Eviction criteria (must meet ALL):
//   1. ≥10 positions opened per day in last 14 days (high volume)
//   2. ≥70% of recent buys on crypto-updown OR <$100 size
//      (not signal-producible — time-gate kills crypto-updown,
//       size threshold kills sub-$100)
//   3. Either:
//      a. zero or <3 signal contributions ever (not contributing), OR
//      b. ≥5 signal contributions but avg return negative (drag)
//
// Wallets passing ALL three are bots/MMs that pollute the pool. They
// can't produce useful signals structurally and they're crowding out
// lower-volume directional traders we'd actually use.
//
// Usage:
//   node scripts/evict-bot-pollution.mjs              # dry-run, report
//   node scripts/evict-bot-pollution.mjs --apply      # commit eviction

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const APPLY = process.argv.includes('--apply');

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const pool = walletsData.pool || walletsData;

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

const DAYS = 14;
const sinceTs = Math.floor(Date.now() / 1000) - DAYS * 86400;

async function fetchActivity(addr) {
  const url = `https://data-api.polymarket.com/activity?user=${addr.toLowerCase()}&limit=500`;
  try {
    const r = await fetch(url);
    if (!r.ok) return null;
    return await r.json();
  } catch { return null; }
}

const activeWallets = Object.entries(pool).filter(([, w]) => w && w.status !== 'removed');

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Bot-pattern eviction — bulk sweep');
console.log('═══════════════════════════════════════════════════════════════════');
console.log('  Active pool wallets to scan: ' + activeWallets.length);
console.log('  Mode: ' + (APPLY ? 'APPLY (will rewrite wallets.json.gz)' : 'DRY-RUN'));
console.log('  Estimated runtime: ~' + Math.ceil(activeWallets.length * 0.5 / 60) + ' min');
console.log();

const evictionCandidates = [];
let processed = 0;

for (const [addr, w] of activeWallets) {
  processed++;
  if (processed % 50 === 0) console.log('  Processed ' + processed + '/' + activeWallets.length);

  const events = await fetchActivity(addr);
  if (!events) continue;

  const buys = events.filter(e => e.type === 'TRADE' && (e.side || '').toUpperCase() === 'BUY' && e.timestamp >= sinceTs);
  if (buys.length === 0) continue;

  // Unique markets per wallet
  const seenMarkets = new Set();
  const uniqueBuys = [];
  for (const b of buys) {
    const cid = b.conditionId || b.condition_id || '';
    if (!cid || seenMarkets.has(cid)) continue;
    seenMarkets.add(cid);
    uniqueBuys.push(b);
  }
  const positionsPerDay = uniqueBuys.length / DAYS;

  // High-volume gate
  if (positionsPerDay < 10) continue;

  // Bot-pattern criteria — % of buys that are crypto-updown OR sub-$100
  const botPatternCount = uniqueBuys.filter(b => {
    const cat = classify(b.title || '');
    const size = parseFloat(b.size || 0) * parseFloat(b.price || 0);
    return cat === 'crypto-updown' || size < 100;
  }).length;
  const botPatternPct = botPatternCount / uniqueBuys.length;

  if (botPatternPct < 0.70) continue;

  // Signal contribution check
  const attrSignals = w.scoreComponents?.attrSignals || 0;
  const attrAvgReturn = w.scoreComponents?.attrAvgReturn;
  const notContributing = attrSignals < 3;
  const negativeContributing = attrSignals >= 5 && attrAvgReturn != null && attrAvgReturn < 0;

  if (!notContributing && !negativeContributing) continue;

  evictionCandidates.push({
    addr,
    score: w.score || 0,
    style: w.stats?.mmScore >= 3 ? 'mm-like' : 'other',
    positionsPerDay: positionsPerDay.toFixed(1),
    botPatternPct: (botPatternPct * 100).toFixed(0) + '%',
    attrSignals,
    attrAvgReturn: attrAvgReturn != null ? ((attrAvgReturn * 100).toFixed(0) + '%') : '—',
    reason: negativeContributing ? 'bot_pattern_negative_attribution' : 'bot_pattern_no_contribution',
  });
}

console.log('\n  ── Eviction candidates: ' + evictionCandidates.length + ' ──');
evictionCandidates.sort((a, b) => parseFloat(b.positionsPerDay) - parseFloat(a.positionsPerDay));

if (evictionCandidates.length === 0) {
  console.log('  No wallets meet bot-pattern + non-contribution criteria.');
  process.exit(0);
}

console.log('  ' + 'Wallet'.padEnd(14) + 'Pos/d'.padStart(7) + 'Pattern%'.padStart(10) + 'Score'.padStart(7) + 'AttrSig'.padStart(8) + 'AttrRet'.padStart(8) + '  Reason');
for (const c of evictionCandidates) {
  console.log('  ' + c.addr.slice(0, 12).padEnd(14) +
    c.positionsPerDay.padStart(7) +
    ' ' + c.botPatternPct.padStart(9) +
    ' ' + c.score.toFixed(1).padStart(6) +
    String(c.attrSignals).padStart(8) +
    c.attrAvgReturn.padStart(8) +
    '  ' + c.reason);
}

console.log();
console.log('  ── Summary ──');
const byReason = {};
for (const c of evictionCandidates) byReason[c.reason] = (byReason[c.reason] || 0) + 1;
for (const [r, n] of Object.entries(byReason)) console.log('  ' + r.padEnd(40) + n);
console.log('  Total to evict: ' + evictionCandidates.length);

if (APPLY) {
  for (const c of evictionCandidates) {
    pool[c.addr].status = 'removed';
    pool[c.addr].removeReason = c.reason;
    pool[c.addr].removeDetail = `${c.positionsPerDay} pos/day, ${c.botPatternPct} crypto-updown/sub-$100, attrSigs=${c.attrSignals} attrRet=${c.attrAvgReturn}`;
    pool[c.addr].removedAt = new Date().toISOString();
  }
  walletsData.pool = pool;
  if (!walletsData.metadata) walletsData.metadata = {};
  walletsData.metadata.lastBotEviction = new Date().toISOString();
  walletsData.metadata.botEvicted = (walletsData.metadata.botEvicted || 0) + evictionCandidates.length;
  fs.writeFileSync(path.join(ROOT, 'data/wallets.json.gz'),
    zlib.gzipSync(Buffer.from(JSON.stringify(walletsData))));
  console.log('\n  ✓ Applied — evicted ' + evictionCandidates.length + ' bot-pattern wallets');
} else {
  console.log('\n  Dry-run. Pass --apply to commit.');
}
console.log();
