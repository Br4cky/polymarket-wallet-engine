// Test a range of flexibility approaches against historical data.
// Compares current rigid system vs progressively more flexible alternatives
// to see if any meaningful improvement exists.
//
// Options tested:
//   A. Current (rigid wallet-count-banded)
//   B. Surgical: add 2-3 wallets × 50-70¢ on non-news categories
//   C. Quality-score gate: composite score, emit if above threshold
//   D. Per-category calibrated minimums (each category gets its own gate)
//   E. Pure attribution gate: emit if avg contributor attributionMultiplier high
//   F. Minimal gates: just keep MM/holder/category filters, no walletCount/price
//   G. Solo-prioritized: any wallet score 30+ emits as solo, plus minimal cluster
//   H. Conviction-scored: weighted by walletCount × avgScore × log(size)
//
// Each option gets the same evaluation: N signals, WR, AvgRet, TotalRet.

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');

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

const ALLOWED = new Set(['tennis','nba','mma','weather','crypto-updown','crypto-other','mlb','nfl','macro','ai-tech','token-launch','news-event','soccer']);

function evalOption(filterFn, label) {
  const kept = resolved.filter(s => {
    const cat = classify(s.marketTitle || '');
    if (!ALLOWED.has(cat)) return false;
    return filterFn(s, cat);
  });
  const N = kept.length;
  const wins = kept.filter(s => s.outcome === 'win').length;
  const totalRet = kept.reduce((a, s) => a + (typeof s.signalReturn === 'number' ? s.signalReturn : 0), 0);
  const retN = kept.filter(s => typeof s.signalReturn === 'number').length;
  const wr = N > 0 ? (wins / N * 100) : 0;
  const avgRet = retN > 0 ? totalRet / retN : 0;
  const ret60d = N > 0 ? (N / 30 * (avgRet / 100) * 100).toFixed(0) : 0;  // signals/day × ret per $100
  return { label, N, wr, avgRet, totalRet };
}

// ── Helpers ─────────────────────────────────────────────────────────
const wcOf = s => s.walletCount || 0;
const priceOf = s => s.avgEntryPrice || 0;
const sizeOf = s => s.totalBuySize || 0;
const scoreOf = s => s.avgScore || 0;

// ── Options ─────────────────────────────────────────────────────────
const options = [];

// A. Current rigid
options.push(evalOption((s) => {
  const wc = wcOf(s), p = priceOf(s);
  if (wc >= 8) return true;  // consensus
  if (wc >= 6 && wc <= 7) return true;  // cluster
  if (wc >= 2 && wc <= 5 && p >= 0.70 && p < 0.85) return true;  // micro-cluster
  if (wc === 1) return true;  // solo
  return false;
}, 'A. Current rigid'));

// B. Surgical fix — add 2-3 × 50-70¢ on non-news categories
options.push(evalOption((s, cat) => {
  const wc = wcOf(s), p = priceOf(s);
  if (wc >= 8) return true;
  if (wc >= 6 && wc <= 7) return true;
  if (wc >= 2 && wc <= 5 && p >= 0.70 && p < 0.85) return true;
  if (wc === 1) return true;
  // NEW: 2-3 wallets at 50-70¢, exclude news-event
  if (wc >= 2 && wc <= 3 && p >= 0.50 && p < 0.70 && cat !== 'news-event') return true;
  return false;
}, 'B. Surgical: add 2-3 × 50-70¢ (no news-event)'));

// C. Quality-score gate: emit if composite score above threshold
function qualityScore(s) {
  // Weighted composite — high walletCount + decent score + size + reasonable entry
  const wc = wcOf(s), p = priceOf(s), sz = sizeOf(s), sc = scoreOf(s);
  // Penalize extreme prices
  const pricePenalty = (p < 0.30 || p > 0.90) ? 0.5 : 1.0;
  return (wc * 5 + sc * 0.5 + Math.log10(1 + sz) * 8) * pricePenalty;
}
options.push(evalOption((s) => qualityScore(s) >= 30, 'C1. QualityScore ≥ 30'));
options.push(evalOption((s) => qualityScore(s) >= 40, 'C2. QualityScore ≥ 40'));
options.push(evalOption((s) => qualityScore(s) >= 50, 'C3. QualityScore ≥ 50'));

// D. Per-category calibrated minimums
const CATEGORY_MIN_WC = {
  tennis: 2, nba: 2, weather: 2, mma: 2, soccer: 3, mlb: 3, nfl: 4,
  'crypto-updown': 2, 'crypto-other': 2, macro: 4, 'token-launch': 4,
  'ai-tech': 4, 'news-event': 6,
};
options.push(evalOption((s, cat) => {
  const wc = wcOf(s), p = priceOf(s);
  const min = CATEGORY_MIN_WC[cat] ?? 6;
  if (wc < min) return false;
  // Still apply price safety: avoid 30-50¢ trap unless heavy crowd
  if (p >= 0.30 && p < 0.50 && wc < 6) return false;
  return true;
}, 'D. Per-category min walletCount'));

// E. Solo-prioritized + minimal cluster
options.push(evalOption((s) => {
  const wc = wcOf(s), p = priceOf(s);
  if (wc === 1) return true;  // any solo
  if (wc >= 4) return true;   // any 4+ cluster, no price restriction
  if (wc >= 2 && p >= 0.50 && p < 0.85) return true;  // 2-3 only at moderate prices
  return false;
}, 'E. Solo-priority + min 4 cluster'));

// F. Minimal gates: keep only category whitelist + size floor
options.push(evalOption((s) => {
  return sizeOf(s) >= 500;
}, 'F. Minimal: just $500 size floor'));

// G. Conviction-scored: walletCount × avgScore × log(size)
function convictionScore(s) {
  const wc = wcOf(s), sc = scoreOf(s), sz = sizeOf(s);
  return wc * Math.max(15, sc) * Math.log10(1 + sz);
}
options.push(evalOption((s) => convictionScore(s) >= 200, 'G1. Conviction ≥ 200'));
options.push(evalOption((s) => convictionScore(s) >= 400, 'G2. Conviction ≥ 400'));
options.push(evalOption((s) => convictionScore(s) >= 600, 'G3. Conviction ≥ 600'));

// H. Pure pricing-band quality (no walletCount filter at all)
options.push(evalOption((s) => {
  const p = priceOf(s);
  // Only at proven price bands
  if (p >= 0.70 && p < 0.85) return true;     // favorite (current micro-cluster)
  if (p >= 0.50 && p < 0.70 && wcOf(s) >= 2 && wcOf(s) <= 3) return true;   // mid-fav 2-3
  if (wcOf(s) >= 6) return true;              // heavy crowd, any price
  return false;
}, 'H. Pricing-band priority'));

// I. Drop all gates (sanity check baseline)
options.push(evalOption((s) => true, 'I. Drop all gates (whitelist only)'));

// ── Print results ──────────────────────────────────────────────────
console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Flexibility test — historical replay over ' + resolved.length + ' resolved signals');
console.log('═══════════════════════════════════════════════════════════════════');
console.log();
console.log('  ' + 'Option'.padEnd(50) + 'N'.padStart(5) + 'WR'.padStart(6) + 'AvgRet'.padStart(9) + 'TotalRet'.padStart(11));
console.log('  ' + '─'.repeat(82));

// Sort by total return descending
const sorted = [...options].sort((a, b) => b.totalRet - a.totalRet);
for (const o of options) {
  const wr = o.N > 0 ? o.wr.toFixed(0) + '%' : '—';
  const avgRet = o.N > 0 ? ((o.avgRet >= 0 ? '+' : '') + o.avgRet.toFixed(1) + '%') : '—';
  const totalRet = (o.totalRet >= 0 ? '+' : '') + o.totalRet.toFixed(0);
  // Mark winner
  const isTopVol = o.N >= 100 && o.avgRet >= 5 && o.totalRet >= 1500;
  const marker = isTopVol ? ' ★' : '';
  console.log('  ' + o.label.padEnd(50) + String(o.N).padStart(5) + wr.padStart(6) + avgRet.padStart(9) + totalRet.padStart(11) + marker);
}

console.log();
console.log('  ★ = healthy volume + per-signal return + cumulative return');
console.log();

// ── Headline picks ─────────────────────────────────────────────────
console.log('  ── Best by criterion ──');
const bestTotalRet = sorted[0];
const bestAvgRet = [...options].filter(o => o.N >= 50).sort((a, b) => b.avgRet - a.avgRet)[0];
const mostVolume = [...options].filter(o => o.avgRet > 0).sort((a, b) => b.N - a.N)[0];
console.log(`  Highest total return: ${bestTotalRet.label} → ${bestTotalRet.totalRet.toFixed(0)} (${bestTotalRet.N} signals)`);
console.log(`  Highest per-signal return (≥50 N): ${bestAvgRet.label} → ${bestAvgRet.avgRet.toFixed(1)}% avg`);
console.log(`  Most volume with positive avg: ${mostVolume.label} → ${mostVolume.N} signals at ${mostVolume.avgRet.toFixed(1)}% avg`);
console.log();
