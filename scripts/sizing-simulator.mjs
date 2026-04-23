// Signal position-sizing simulator
// ---------------------------------
// Applies several sizing policies to the resolved signal history and
// reports cumulative P&L per policy. Each "sizing" is a function that
// maps a signal to a bet-size multiplier (flat=1.0, or tilted by
// confidence, score, tier, category, etc.).
//
// P&L per signal = size × signalReturn
// Headline KPI = sum of P&L contribution / total size deployed
// (same units as avg return, but weighted by sizing)

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');

// Category classifier (copy of category-specialization.mjs)
function classify(question) {
  const q = (question || '').toLowerCase();
  if (/dota|lol|league of legends|counter-strike|valorant|cs:go|cs2|csgo|call of duty|rocket league|overwatch|starcraft|hearthstone|apex|fortnite|pubg/.test(q)) return 'esports';
  if (/bitcoin|btc|ethereum|eth|solana|sol\b|doge|xrp|crypto|coin/.test(q)) {
    if (/reach|above|below|hit|close|\$|\sup\b|\sdown\b|end above|end below/.test(q)) return 'crypto-updown';
    return 'crypto-other';
  }
  if (/nba|lakers|celtics|warriors|playoffs/.test(q) && !/nhl/.test(q)) return 'nba';
  if (/nfl|super bowl|touchdown|quarterback/.test(q)) return 'nfl';
  if (/mlb|baseball|world series/.test(q)) return 'mlb';
  if (/nhl|stanley cup| hockey /.test(q)) return 'nhl';
  if (/epl|premier league|champions league|la liga|bundesliga|serie a|manchester|arsenal|liverpool|chelsea/.test(q)) return 'soccer';
  if (/tennis|wimbledon|\bus open\b|french open|atp|wta/.test(q)) return 'tennis';
  if (/ufc|mma|fight/.test(q)) return 'mma';
  if (/ pga |golf|masters tournament/.test(q)) return 'golf';
  if (/f1|formula|grand prix/.test(q)) return 'f1';
  if (/trump|biden|harris|election|senate|house race|republican|democrat|congressional/.test(q)) return 'politics';
  if (/ai\b|openai|anthropic|gpt|gemini|tech|apple|google|microsoft/.test(q)) return 'ai-tech';
  if (/weather|temperature|hurricane|tornado/.test(q)) return 'weather';
  if (/fed rate|inflation|gdp|recession|jobs report|unemployment/.test(q)) return 'macro';
  return 'other';
}

// Sizing policies. Each returns multiplier >= 0.
// Total "budget" per policy = sum of sizes; average return weighted by size.
const POLICIES = {
  'flat  (1× every signal)': (s) => 1,

  'solo only': (s) => s.signalType === 'solo' ? 1 : 0,

  'cluster only': (s) => s.signalType === 'cluster' ? 1 : 0,

  'consensus only': (s) => s.signalType === 'consensus' ? 1 : 0,

  'confidence-weighted (c/50)': (s) => Math.max(0, (s.confidence ?? 50) / 50),

  'confidence > 70 (binary)': (s) => (s.confidence ?? 0) > 70 ? 1 : 0,

  'confidence > 75 (binary)': (s) => (s.confidence ?? 0) > 75 ? 1 : 0,

  'confidence > 80 (binary)': (s) => (s.confidence ?? 0) > 80 ? 1 : 0,

  'avgScore-weighted (s/25)': (s) => Math.max(0, (s.avgScore ?? 0) / 25),

  'avgScore > 30 (binary)': (s) => (s.avgScore ?? 0) > 30 ? 1 : 0,

  'tier=pro': (s) => s.tier === 'pro' ? 1 : 0,

  'tier=elite': (s) => s.tier === 'elite' ? 1 : 0,

  'category-whitelist (tennis+nba+mma+weather+crypto-updown)': (s) => {
    const c = classify(s.marketTitle || '');
    return ['tennis', 'nba', 'mma', 'weather', 'crypto-updown', 'crypto-other'].includes(c) ? 1 : 0;
  },

  'category + confidence>70': (s) => {
    const c = classify(s.marketTitle || '');
    if (!['tennis', 'nba', 'mma', 'weather', 'crypto-updown', 'crypto-other'].includes(c)) return 0;
    return (s.confidence ?? 0) > 70 ? 1 : 0;
  },

  'fat-tail-sizing (square of confidence/50)': (s) => {
    const c = (s.confidence ?? 50) / 50;
    return c * c;
  },

  'entry ≤ 30¢ only': (s) => (s.avgEntryPrice ?? 1) <= 0.30 ? 1 : 0,
  'entry 30-60¢': (s) => { const p = s.avgEntryPrice ?? 0; return p > 0.30 && p <= 0.60 ? 1 : 0; },
  'entry 60-85¢': (s) => { const p = s.avgEntryPrice ?? 0; return p > 0.60 && p <= 0.85 ? 1 : 0; },
  'entry > 85¢ (heavy-favourite)': (s) => (s.avgEntryPrice ?? 0) > 0.85 ? 1 : 0,

  'walletCount ≥ 6': (s) => (s.walletCount ?? 0) >= 6 ? 1 : 0,
  'walletCount ≥ 8': (s) => (s.walletCount ?? 0) >= 8 ? 1 : 0,
};

const results = [];
for (const [name, fn] of Object.entries(POLICIES)) {
  let totalSize = 0, totalPnl = 0, n = 0, wins = 0;
  for (const sig of resolved) {
    const sz = fn(sig);
    if (!sz || sz <= 0) continue;
    const ret = typeof sig.signalReturn === 'number' ? sig.signalReturn : 0;  // percentage
    totalSize += sz;
    totalPnl += sz * ret;
    n++;
    if (sig.outcome === 'win') wins++;
  }
  const weightedReturn = totalSize > 0 ? totalPnl / totalSize : 0;
  const wr = n > 0 ? wins / n : 0;
  results.push({ name, n, wins, wr, weightedReturn, totalSize, totalPnl });
}

results.sort((a, b) => b.weightedReturn - a.weightedReturn);

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Signal position-sizing simulator');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log(`  ${'Policy'.padEnd(55)}  ${'N'.padStart(4)}  ${'WR'.padStart(5)}  ${'WtdRet'.padStart(7)}  ${'TotalPnl'.padStart(9)}`);
console.log('  ' + '─'.repeat(92));
for (const r of results) {
  const wr = (r.wr * 100).toFixed(0) + '%';
  const wret = (r.weightedReturn >= 0 ? '+' : '') + r.weightedReturn.toFixed(1) + '%';
  const pnl = (r.totalPnl >= 0 ? '+' : '') + r.totalPnl.toFixed(0);
  console.log(`  ${r.name.padEnd(55)}  ${String(r.n).padStart(4)}  ${wr.padStart(5)}  ${wret.padStart(7)}  ${pnl.padStart(9)}`);
}

// Headline: delta vs flat
const flat = results.find(r => r.name.includes('flat'));
const best = results[0];
if (flat && best && best.name !== flat.name) {
  const lift = best.weightedReturn - flat.weightedReturn;
  console.log(`\n  Best policy beats flat sizing by ${lift.toFixed(1)}% per dollar deployed`);
  console.log(`  (${best.name})`);
}

console.log('\n═══════════════════════════════════════════════════════════════════\n');
