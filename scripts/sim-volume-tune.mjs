// Project volume impact of:
//   1. Majority-disqualified rule (was: any-disqualified)
//   2. Soccer re-added to whitelist
//
// Replays the 1,475 historical resolved signals and counts how many
// would now pass under the new rules vs the strict ones.

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const pool = walletsData.pool || walletsData;
const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');

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
  if (/fed rate|fomc|inflation|cpi|ppi|gdp|recession|jobs report|unemployment|nonfarm/.test(q)) return 'macro';
  if (/ipo|earnings|revenue|guidance|\beps\b/.test(q)) return 'macro';
  if (/spacex|starship|mission|nasa|rocket launch/.test(q)) return 'news-event';
  if (/announce|statement|\bsay\b|\bsays\b|tweet|post|comment/.test(q)) return 'news-event';
  if (/\bwill .+ by |\bwill .+ before |\bwill .+ on /.test(q)) return 'news-event';
  return 'other';
}

const ALLOWED_OLD = new Set(['tennis', 'nba', 'mma', 'weather', 'crypto-updown', 'crypto-other', 'mlb', 'nfl', 'macro', 'ai-tech', 'token-launch', 'news-event']);
const ALLOWED_NEW = new Set([...ALLOWED_OLD, 'soccer']);
const DISQUALIFIED = new Set(['holder', 'mm-like']);

function contributors(s) {
  const set = new Set();
  if (Array.isArray(s.currentWallets)) s.currentWallets.forEach(w => w && w.address && set.add(String(w.address).toLowerCase()));
  if (s.soloWallet) set.add(String(s.soloWallet).toLowerCase());
  return [...set];
}
function anyDisqualified(s) {
  for (const a of contributors(s)) {
    const w = pool[a];
    if (w && DISQUALIFIED.has(classifyStyle(w.stats))) return true;
  }
  return false;
}
function majorityDisqualified(s) {
  const cs = contributors(s);
  if (cs.length === 0) return false;
  let bad = 0, total = 0;
  for (const a of cs) {
    const w = pool[a];
    if (!w) continue;
    total++;
    if (DISQUALIFIED.has(classifyStyle(w.stats))) bad++;
  }
  return total > 0 && (bad / total) > 0.50;
}

function passWhitelist(s, allowed) {
  return allowed.has(classify(s.marketTitle || ''));
}

function score(filterFn, label) {
  let n = 0, wins = 0, totalRet = 0, retN = 0;
  for (const s of resolved) {
    if (!filterFn(s)) continue;
    n++;
    if (s.outcome === 'win') wins++;
    if (typeof s.signalReturn === 'number') { totalRet += s.signalReturn; retN++; }
  }
  const wr = n > 0 ? wins / n : 0;
  const avgRet = retN > 0 ? totalRet / retN : 0;
  const wrStr = (wr * 100).toFixed(0) + '%';
  const retStr = (avgRet >= 0 ? '+' : '') + avgRet.toFixed(1) + '%';
  console.log('  ' + label.padEnd(50) + 'N=' + String(n).padStart(5) + '  WR=' + wrStr.padStart(4) + '  AvgRet=' + retStr.padStart(7));
  return { n, wr, avgRet };
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Volume tune simulation — over ' + resolved.length + ' historical resolved');
console.log('═══════════════════════════════════════════════════════════════════\n');

console.log('  Baseline reference:');
score(() => true, 'all (baseline)');

console.log('\n  Whitelist comparison (with strict any-disqualified):');
score(s => passWhitelist(s, ALLOWED_OLD) && !anyDisqualified(s), 'OLD whitelist + any-disqualified');
score(s => passWhitelist(s, ALLOWED_NEW) && !anyDisqualified(s), 'NEW whitelist (+soccer) + any-disqualified');

console.log('\n  Disqualification rule comparison (NEW whitelist):');
score(s => passWhitelist(s, ALLOWED_NEW) && !anyDisqualified(s), 'NEW whitelist + any-disqualified (current)');
score(s => passWhitelist(s, ALLOWED_NEW) && !majorityDisqualified(s), 'NEW whitelist + majority-disqualified (NEW)');

console.log('\n  Δ Combined volume change vs current Option-2-strict:');
const oldRule = resolved.filter(s => passWhitelist(s, ALLOWED_OLD) && !anyDisqualified(s)).length;
const newRule = resolved.filter(s => passWhitelist(s, ALLOWED_NEW) && !majorityDisqualified(s)).length;
const lift = (newRule / oldRule - 1) * 100;
console.log('  ' + oldRule + ' → ' + newRule + '  (' + (lift >= 0 ? '+' : '') + lift.toFixed(0) + '%)');

console.log();
