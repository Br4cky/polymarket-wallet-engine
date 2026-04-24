// Simulate the Option 2 composite emission policy against historical
// resolved signals. Uses the classifyWalletStyle + DISQUALIFIED_STYLES
// + SOLO_ALLOWED_STYLES + MICRO_CLUSTER_* rules exactly as coded into
// scanner/signals.js — imports the module directly.
//
// Usage:
//   node scripts/simulate-option2.mjs

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const signalsMod = await import(path.join(ROOT, 'scanner/signals.js'));
const { SIGNAL_THRESHOLDS } = signalsMod;

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const pool = walletsData.pool || walletsData;
const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');

// Mirror the helpers from signals.js
function classifyWalletStyle(stats) {
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
const SOLO_ALLOWED = new Set(['sniper']);
const DISQUALIFIED = new Set(['holder', 'mm-like']);

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
function isExcluded(title) {
  const t = (title || '').toLowerCase();
  return SIGNAL_THRESHOLDS.EXCLUDED_KEYWORDS.some(kw => t.includes(kw));
}
function isWhitelisted(title) {
  return !isExcluded(title) && SIGNAL_THRESHOLDS.ALLOWED_CATEGORIES.has(classify(title));
}
function contributors(s) {
  const set = new Set();
  if (Array.isArray(s.currentWallets)) s.currentWallets.forEach(w => w && w.address && set.add(String(w.address).toLowerCase()));
  if (s.soloWallet) set.add(String(s.soloWallet).toLowerCase());
  return [...set];
}
function hasBad(s) {
  for (const a of contributors(s)) {
    const w = pool[a];
    if (w && DISQUALIFIED.has(classifyWalletStyle(w.stats))) return true;
  }
  return false;
}
function soloStyleAllowed(s) {
  if (s.signalType !== 'solo') return true;  // n/a
  const addr = s.soloWallet ? String(s.soloWallet).toLowerCase() : null;
  if (!addr) return false;
  const w = pool[addr];
  if (!w || !w.stats) return false;
  return SOLO_ALLOWED.has(classifyWalletStyle(w.stats));
}

// Apply Option 2 rules exactly as in signals.js processSignals
function wouldEmit(s) {
  // 1. Category whitelist
  if (!isWhitelisted(s.marketTitle)) return false;

  // 2. Disqualified contributors
  if (hasBad(s)) return false;

  // 3. Path-specific admission
  const wc = s.walletCount || 0;
  const entry = s.avgEntryPrice || 0;

  if (s.signalType === 'solo') {
    // Sniper-only solo
    return soloStyleAllowed(s);
  }

  // Convergence paths
  if (wc >= SIGNAL_THRESHOLDS.CONSENSUS_MIN_WALLETS) return true;  // consensus
  if (wc >= SIGNAL_THRESHOLDS.CLUSTER_MIN_WALLETS
      && wc <= SIGNAL_THRESHOLDS.CLUSTER_MAX_WALLETS) return true;  // cluster
  if (wc >= SIGNAL_THRESHOLDS.MICRO_CLUSTER_MIN_WALLETS
      && wc <= SIGNAL_THRESHOLDS.MICRO_CLUSTER_MAX_WALLETS
      && entry >= SIGNAL_THRESHOLDS.MICRO_CLUSTER_ENTRY_MIN
      && entry < SIGNAL_THRESHOLDS.MICRO_CLUSTER_ENTRY_MAX) return true;  // micro-cluster
  return false;
}

const span = 15.6;
const total = resolved.length;
const kept = resolved.filter(wouldEmit);

// Breakdown
function stats(arr, label) {
  const N = arr.length;
  const wins = arr.filter(s => s.outcome === 'win').length;
  const totalRet = arr.reduce((a, s) => a + (typeof s.signalReturn === 'number' ? s.signalReturn : 0), 0);
  const wr = N > 0 ? wins / N : 0;
  const avgRet = N > 0 ? totalRet / N : 0;
  console.log(`  ${label.padEnd(28)}  N=${String(N).padStart(5)}  /day=${(N/span).toFixed(1).padStart(5)}  WR=${(wr*100).toFixed(0).padStart(3)}%  AvgRet=${((avgRet>=0?'+':'')+avgRet.toFixed(1)+'%').padStart(7)}  Total=${(totalRet>=0?'+':'')+totalRet.toFixed(0)}`);
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Option 2 composite emission policy — simulation vs history');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log(`  Historical window: ${span} days, ${total} resolved signals\n`);

stats(resolved, 'Baseline (all historical)');
stats(kept, 'Option 2 composite');
console.log();

// Break by emission path
const byPath = new Map();
for (const s of kept) {
  let path;
  if (s.signalType === 'solo') path = 'solo (sniper-only)';
  else if ((s.walletCount || 0) >= SIGNAL_THRESHOLDS.CONSENSUS_MIN_WALLETS) path = 'consensus';
  else if ((s.walletCount || 0) >= SIGNAL_THRESHOLDS.CLUSTER_MIN_WALLETS) path = 'cluster';
  else path = 'micro-cluster (fav-resolve)';
  if (!byPath.has(path)) byPath.set(path, []);
  byPath.get(path).push(s);
}
console.log('  ── By emission path ──');
for (const [path, arr] of byPath) stats(arr, path);

console.log(`\n  Volume drop: ${total} → ${kept.length}  (${((kept.length/total)*100).toFixed(0)}% kept, ${((1-kept.length/total)*100).toFixed(0)}% filtered)`);
console.log(`  Projected signals/day: ${(kept.length/span).toFixed(1)}\n`);

console.log('  ── If we ALSO add Option 1 (widen SOLO_ALLOWED to sniper+averager+churner) ──');
const SOLO_WIDE = new Set(['sniper', 'averager', 'churner']);
const wideKept = resolved.filter(s => {
  if (!isWhitelisted(s.marketTitle)) return false;
  if (hasBad(s)) return false;
  if (s.signalType === 'solo') {
    const addr = s.soloWallet ? String(s.soloWallet).toLowerCase() : null;
    const w = addr ? pool[addr] : null;
    if (!w || !w.stats) return false;
    return SOLO_WIDE.has(classifyWalletStyle(w.stats));
  }
  const wc = s.walletCount || 0;
  const entry = s.avgEntryPrice || 0;
  if (wc >= 8) return true;
  if (wc >= 6 && wc <= 7) return true;
  if (wc >= 2 && wc <= 5 && entry >= 0.70 && entry < 0.85) return true;
  return false;
});
stats(wideKept, 'Option 1 (loose)');

console.log('\n  ── If we ALSO add Option 3 (sniper-only + entry 70-85¢ required) ──');
const strictKept = resolved.filter(s => {
  if (!isWhitelisted(s.marketTitle)) return false;
  if (hasBad(s)) return false;
  const entry = s.avgEntryPrice || 0;
  if (entry < 0.70 || entry >= 0.85) return false;  // enforce favorite band on all
  if (s.signalType === 'solo') {
    const addr = s.soloWallet ? String(s.soloWallet).toLowerCase() : null;
    const w = addr ? pool[addr] : null;
    if (!w || !w.stats) return false;
    return classifyWalletStyle(w.stats) === 'sniper';
  }
  const wc = s.walletCount || 0;
  return wc >= 2;
});
stats(strictKept, 'Option 3 (strict)');

console.log('\n═══════════════════════════════════════════════════════════════════\n');
