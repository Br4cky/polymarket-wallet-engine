// Diagnose why a specific wallet isn't contributing to signals.
// Walks through every recent BUY and shows which gate would have caught
// it under current emission logic.
//
// Usage:
//   node scripts/diagnose-wallet-contribution.mjs <wallet-address>
//   node scripts/diagnose-wallet-contribution.mjs 0x6c9c51ed49
//   node scripts/diagnose-wallet-contribution.mjs 0x490e55d3c2 --days 14
//
// For each BUY in the wallet's recent activity, evaluates:
//   1. Was it on a whitelisted-category market?
//   2. Was the buy size above SOLO_MIN_BUY_SIZE?
//   3. Was the wallet's style allowed for solo (sniper/averager/churner)?
//   4. Did the wallet pass score gate (>=25 or whale bypass)?
//   5. Did the market already resolve / past endDate?
//   6. Was current price reasonable (no stale-follower issue)?
//
// Plus shows the wallet's overall profile + classification.

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const args = process.argv.slice(2);
const targetPrefix = args.find(a => a.startsWith('0x'));
const get = (f, d) => { const i = args.indexOf(f); return i >= 0 && i + 1 < args.length ? args[i + 1] : d; };
const DAYS = parseInt(get('--days', '14'), 10);

if (!targetPrefix) {
  console.log('Usage: node scripts/diagnose-wallet-contribution.mjs <wallet-prefix>');
  process.exit(1);
}

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const pool = walletsData.pool || walletsData;
const fullAddr = Object.keys(pool).find(k => k.toLowerCase().startsWith(targetPrefix.toLowerCase()));
if (!fullAddr) {
  console.log('Wallet not found in pool: ' + targetPrefix);
  process.exit(1);
}
const wallet = pool[fullAddr];

// ── Wallet profile ─────────────────────────────────────────────────
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

const style = classifyStyle(wallet.stats);
const SOLO_ALLOWED = new Set(['sniper', 'averager', 'churner']);
const ALLOWED_CATEGORIES = new Set(['tennis','nba','mma','weather','crypto-updown','crypto-other','mlb','nfl','macro','ai-tech','token-launch','news-event','soccer']);

// Whale bypass criteria
const roi = wallet.stats?.decidedROI ?? wallet.stats?.singleSideROI ?? 0;
const cap = wallet.stats?.decidedCapital ?? wallet.stats?.singleSideCapital ?? 0;
const isWhale = roi >= 0.15 && cap >= 500000
  && (wallet.stats?.mmScore || 0) < 3
  && wallet.stats?.alphaVerdict !== 'fails'
  && wallet.stats?.isMeanPickerShape !== true
  && style !== 'holder';

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Wallet contribution diagnostic');
console.log('═══════════════════════════════════════════════════════════════════');
console.log('  Address:           ' + fullAddr);
console.log('  Score:             ' + (wallet.score || 0).toFixed(1));
console.log('  Style:             ' + style);
console.log('  decidedROI:        ' + (wallet.stats?.decidedROI != null ? (wallet.stats.decidedROI * 100).toFixed(1) + '%' : '—'));
console.log('  singleSideROI:     ' + (wallet.stats?.singleSideROI != null ? (wallet.stats.singleSideROI * 100).toFixed(1) + '%' : '—'));
console.log('  decidedCapital:    $' + (cap ? cap.toLocaleString() : '—'));
console.log('  Win rate:          ' + (wallet.stats?.winRate != null ? (wallet.stats.winRate * 100).toFixed(1) + '%' : '—'));
console.log('  Resolved markets:  ' + (wallet.stats?.resolvedMarkets || 0));
console.log('  mmScore:           ' + (wallet.stats?.mmScore || 0));
console.log('  alphaVerdict:      ' + (wallet.stats?.alphaVerdict || '—'));
console.log('  isMeanPicker:      ' + (wallet.stats?.isMeanPickerShape || false));
console.log('  attrSignals:       ' + (wallet.scoreComponents?.attrSignals || 0));
console.log('  attrAvgReturn:     ' + (wallet.scoreComponents?.attrAvgReturn != null ? ((wallet.scoreComponents.attrAvgReturn >= 0 ? '+' : '') + (wallet.scoreComponents.attrAvgReturn * 100).toFixed(1) + '%') : '—'));

console.log('\n  ── Solo-eligibility check ──');
const styleOK = SOLO_ALLOWED.has(style);
const scoreOK = (wallet.score || 0) >= 25;
const wrOK = (wallet.stats?.recentWinRate || wallet.stats?.winRate || 0) >= 0.55;
const resolvedOK = (wallet.stats?.resolvedMarkets || 0) >= 50;
const safetyOK = !wallet.stats?.isLikelyMM && !wallet.stats?.isMeanPickerShape && wallet.stats?.alphaVerdict !== 'fails';

console.log('  Style allowed:        ' + (styleOK ? '✓' : '✗') + ' (' + style + ' ' + (styleOK ? 'in' : 'NOT in') + ' [sniper, averager, churner])');
console.log('  Whale bypass:         ' + (isWhale ? '✓ (≥15% ROI + ≥$500k cap)' : '✗'));
console.log('  Score ≥ 25:           ' + (scoreOK ? '✓' : '✗') + ' (score: ' + (wallet.score || 0).toFixed(1) + ')');
console.log('  Win rate ≥ 55%:       ' + (wrOK ? '✓' : '✗'));
console.log('  Resolved ≥ 50:        ' + (resolvedOK ? '✓' : '✗') + ' (' + (wallet.stats?.resolvedMarkets || 0) + ')');
console.log('  Safety gates:         ' + (safetyOK ? '✓' : '✗'));

const soloEligible = (styleOK || isWhale) && (scoreOK || isWhale) && wrOK && resolvedOK && safetyOK;
console.log('\n  → Solo-eligible: ' + (soloEligible ? 'YES (this wallet CAN emit solo signals)' : 'NO'));

if (!soloEligible) {
  console.log('  → Reason solo signals not happening: above gate failures');
  process.exit(0);
}

// If solo-eligible, fetch their recent buys and check each
console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Per-buy gate analysis (last ' + DAYS + ' days)');
console.log('═══════════════════════════════════════════════════════════════════');

const sinceTs = Math.floor(Date.now() / 1000) - DAYS * 86400;

async function fetchActivity(addr) {
  const url = `https://data-api.polymarket.com/activity?user=${addr.toLowerCase()}&limit=500`;
  const r = await fetch(url);
  if (!r.ok) return null;
  return await r.json();
}

const events = await fetchActivity(fullAddr);
if (!events) {
  console.log('  Failed to fetch activity from Data API');
  process.exit(1);
}

const buys = events.filter(e => e.type === 'TRADE' && (e.side || '').toUpperCase() === 'BUY' && e.timestamp >= sinceTs);
console.log('  Total recent BUYs: ' + buys.length);
console.log();

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

const SOLO_MIN_BUY_SIZE = 500;
const kills = {
  category_excluded: 0,
  category_other: 0,
  buy_size_too_small: 0,
  duplicate_market: 0,
  would_emit: 0,
};

const seenMarkets = new Set();
const wouldEmitMarkets = [];

for (const b of buys) {
  const title = b.title || '';
  const cat = classify(title);
  const buySize = parseFloat(b.size || 0) * parseFloat(b.price || 0);

  // Whitelist check
  if (!ALLOWED_CATEGORIES.has(cat)) {
    if (cat === 'other') kills.category_other++;
    else kills.category_excluded++;
    continue;
  }

  // Size check (solo)
  if (buySize < SOLO_MIN_BUY_SIZE) {
    kills.buy_size_too_small++;
    continue;
  }

  // Per-wallet uniqueness within window
  const cid = b.conditionId || b.condition_id || '';
  if (seenMarkets.has(cid)) {
    kills.duplicate_market++;
    continue;
  }
  seenMarkets.add(cid);

  kills.would_emit++;
  wouldEmitMarkets.push({
    cat,
    title: title.slice(0, 60),
    buySize,
    price: parseFloat(b.price || 0),
    timestamp: b.timestamp,
    conditionId: cid,
  });
}

console.log('  ── Why each BUY didn\'t emit a solo signal ──');
console.log('  ' + 'Reason'.padEnd(28) + 'Count'.padStart(6));
console.log('  ' + '─'.repeat(38));
for (const [k, v] of Object.entries(kills).sort((a, b) => b[1] - a[1])) {
  if (v > 0) console.log('  ' + k.padEnd(28) + String(v).padStart(6));
}

// Category breakdown of all buys
console.log('\n  ── Category distribution of ALL recent BUYs ──');
const byCat = {};
for (const b of buys) {
  const c = classify(b.title || '');
  byCat[c] = (byCat[c] || 0) + 1;
}
for (const [c, n] of Object.entries(byCat).sort((a, b) => b[1] - a[1])) {
  const inWhitelist = ALLOWED_CATEGORIES.has(c) ? '✓' : '✗';
  console.log('  ' + inWhitelist + ' ' + c.padEnd(14) + ' ' + String(n).padStart(4));
}

// What WOULD have emitted
console.log('\n  ── Buys that WOULD have emitted a solo signal (' + wouldEmitMarkets.length + ') ──');
for (const m of wouldEmitMarkets.slice(0, 15)) {
  const date = new Date(m.timestamp * 1000).toISOString().slice(0, 19);
  console.log('  ' + date + '  $' + m.buySize.toFixed(0).padStart(7) + '  ' + (m.price * 100).toFixed(0) + '¢  ' + m.cat.padEnd(14) + ' ' + m.title);
}
if (wouldEmitMarkets.length > 15) console.log('  ... and ' + (wouldEmitMarkets.length - 15) + ' more');

// Summary
console.log('\n  ── Summary ──');
console.log('  ' + buys.length + ' total recent BUYs');
console.log('  ' + kills.would_emit + ' would have passed solo-emission gates (theoretically)');
console.log('  ' + kills.category_excluded + ' killed by category whitelist');
console.log('  ' + kills.category_other + ' killed by "other" category (unclassified)');
console.log('  ' + kills.buy_size_too_small + ' below $500 buy-size minimum');
console.log('  ' + kills.duplicate_market + ' duplicate market within window');

if (kills.would_emit > 0) {
  console.log('\n  → Wallet should be emitting ' + kills.would_emit + ' solo signals from this 14-day window.');
  console.log('  → If they\'re not actually emitting in the live system, possible reasons:');
  console.log('    - SOLO_MAX_PER_WALLET (3) cap — only 3 active solos at once');
  console.log('    - resolvesTooSoon gate (markets within 4h of end)');
  console.log('    - stale-follower drawdown (price moved against them)');
  console.log('    - market_closed (already settled by scan time)');
} else {
  console.log('\n  → Wallet has no buys that pass the basic solo-emission criteria.');
  console.log('  → They may be trading on markets we exclude or with sub-$500 buys.');
}
console.log();
