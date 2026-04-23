// Simulate what the new category whitelist + widened classifier would
// have done to the 1,446 historical resolved signals. Sanity-checks the
// change before we ship it to production scans.

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

// Import the live classifier + whitelist from the production module.
// This guarantees what we simulate is literally what production will do.
const signalsMod = await import(path.join(ROOT, 'scanner/signals.js'));
const { SIGNAL_THRESHOLDS } = signalsMod;

// Re-derive the same helpers locally (they're not exported). Keep in sync
// with signals.js — if the exports there ever change, inline here.
function isExcludedMarket(title) {
  if (!title || typeof title !== 'string') return false;
  const t = title.toLowerCase();
  return SIGNAL_THRESHOLDS.EXCLUDED_KEYWORDS.some(k => t.includes(k));
}

function classifyMarket(title) {
  const q = (title || '').toLowerCase();
  if (!q) return 'other';
  if (/dota|lol|league of legends|counter-strike|valorant|cs:go|cs2|csgo|call of duty|rocket league|overwatch|starcraft|hearthstone|apex|fortnite|pubg/.test(q)) return 'esports';
  if (/\blaunch a token|\btoken launch|\btge\b|token on /.test(q)) return 'token-launch';
  if (/bitcoin|btc|ethereum|eth|solana|sol\b|doge|xrp|crypto|coin/.test(q)) {
    if (/reach|above|below|hit|close|\$|\sup\b|\sdown\b|end above|end below/.test(q)) return 'crypto-updown';
    return 'crypto-other';
  }
  if (/nhl|stanley cup| hockey /.test(q)) return 'nhl';
  if (/nba|lakers|celtics|warriors|\bknicks\b|\bheat\b|nba playoffs/.test(q)) return 'nba';
  if (/nfl|super bowl|touchdown|quarterback/.test(q)) return 'nfl';
  if (/mlb|baseball|world series/.test(q)) return 'mlb';
  if (/epl|premier league|champions league|la liga|bundesliga|serie a|manchester|arsenal|liverpool|chelsea/.test(q)) return 'soccer';
  if (/tennis|wimbledon|\bus open\b|french open|atp|wta/.test(q)) return 'tennis';
  if (/ufc|\bmma\b|fight night/.test(q)) return 'mma';
  if (/ pga |golf|masters tournament/.test(q)) return 'golf';
  if (/f1\b|formula 1|grand prix/.test(q)) return 'f1';
  if (/trump|biden|harris|election|senate|house race|republican|democrat|congressional/.test(q)) return 'politics';
  if (/ai\b|openai|anthropic|gpt|gemini|\bllm\b|tech|apple|google|microsoft|nvidia/.test(q)) return 'ai-tech';
  if (/weather|temperature|hurricane|tornado|snow|rainfall/.test(q)) return 'weather';
  if (/fed rate|fomc|inflation|cpi|ppi|\bgdp\b|recession|jobs report|unemployment|nonfarm/.test(q)) return 'macro';
  if (/ipo|earnings|revenue|guidance|\beps\b/.test(q)) return 'macro';
  if (/spacex|starship|mission|\bnasa\b|rocket launch/.test(q)) return 'news-event';
  if (/announce|statement|\bsay\b|\bsays\b|tweet|post|comment/.test(q)) return 'news-event';
  if (/\bwill .+ by |\bwill .+ before |\bwill .+ on /.test(q)) return 'news-event';
  return 'other';
}

function isWhitelisted(title) {
  if (isExcludedMarket(title)) return false;
  return SIGNAL_THRESHOLDS.ALLOWED_CATEGORIES.has(classifyMarket(title));
}

// ── Run ──────────────────────────────────────────────────────────────────
const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');

const total = { N: 0, wins: 0, totalRet: 0, retN: 0 };
const kept = { N: 0, wins: 0, totalRet: 0, retN: 0 };
const byCategory = new Map();
let unclassified = 0;

for (const sig of resolved) {
  const cat = classifyMarket(sig.marketTitle || '');
  const allowed = isWhitelisted(sig.marketTitle || '');
  const ret = typeof sig.signalReturn === 'number' ? sig.signalReturn : null;

  total.N++;
  if (sig.outcome === 'win') total.wins++;
  if (ret !== null) { total.totalRet += ret; total.retN++; }

  if (!byCategory.has(cat)) byCategory.set(cat, { N: 0, wins: 0, totalRet: 0, retN: 0, allowed });
  const b = byCategory.get(cat);
  b.N++;
  if (sig.outcome === 'win') b.wins++;
  if (ret !== null) { b.totalRet += ret; b.retN++; }

  if (cat === 'other') unclassified++;

  if (allowed) {
    kept.N++;
    if (sig.outcome === 'win') kept.wins++;
    if (ret !== null) { kept.totalRet += ret; kept.retN++; }
  }
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Whitelist simulation — historical 1,446 resolved signals');
console.log('═══════════════════════════════════════════════════════════════════\n');

console.log(`  BEFORE whitelist:`);
console.log(`    N:           ${total.N}`);
console.log(`    WR:          ${(total.wins / total.N * 100).toFixed(1)}%`);
console.log(`    avg return:  ${(total.totalRet / total.retN).toFixed(1)}%\n`);

console.log(`  AFTER whitelist (${SIGNAL_THRESHOLDS.ALLOWED_CATEGORIES.size} categories):`);
console.log(`    N:           ${kept.N}  (${(kept.N/total.N*100).toFixed(0)}% of original volume)`);
console.log(`    WR:          ${(kept.wins / kept.N * 100).toFixed(1)}%`);
console.log(`    avg return:  ${(kept.totalRet / kept.retN).toFixed(1)}%`);
console.log(`    Δ return:    ${((kept.totalRet / kept.retN) - (total.totalRet / total.retN)).toFixed(1)}pp\n`);

console.log(`  Per-category breakdown (widened classifier):`);
console.log(`  ${'Category'.padEnd(16)}  ${'Allow'.padStart(6)}  ${'N'.padStart(5)}  ${'WR'.padStart(6)}  ${'AvgRet'.padStart(9)}`);
console.log('  ' + '─'.repeat(55));
const rows = [...byCategory.entries()].map(([c, r]) => ({
  cat: c,
  allow: r.allowed,
  N: r.N,
  wr: r.wins / r.N,
  avgRet: r.retN > 0 ? r.totalRet / r.retN : 0,
}));
rows.sort((a, b) => b.avgRet - a.avgRet);
for (const r of rows) {
  const mark = r.allow ? ' ✓' : ' ✗';
  const wr = (r.wr * 100).toFixed(1) + '%';
  const ret = (r.avgRet >= 0 ? '+' : '') + r.avgRet.toFixed(1) + '%';
  console.log(`  ${r.cat.padEnd(16)}  ${mark.padStart(6)}  ${String(r.N).padStart(5)}  ${wr.padStart(6)}  ${ret.padStart(9)}`);
}
console.log(`\n  "other" bucket size: ${unclassified} / ${total.N} = ${(unclassified/total.N*100).toFixed(0)}%`);
console.log('\n═══════════════════════════════════════════════════════════════════\n');
