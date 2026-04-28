// Head-to-head: Current vs Option B (surgical) vs Option E vs B+E hybrid.
// Including category × cell breakdowns so we can see WHERE each option's
// gains and losses come from.

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

const wcOf = s => s.walletCount || 0;
const priceOf = s => s.avgEntryPrice || 0;

// ── Three configs ──────────────────────────────────────────────────
const CONFIGS = {
  A_current: {
    name: 'A. Current rigid',
    filter: (s, cat) => {
      const wc = wcOf(s), p = priceOf(s);
      if (wc >= 8) return 'consensus';
      if (wc >= 6 && wc <= 7) return 'cluster';
      if (wc >= 2 && wc <= 5 && p >= 0.70 && p < 0.85) return 'micro-cluster';
      if (wc === 1) return 'solo';
      return null;
    },
  },
  B_surgical: {
    name: 'B. Surgical (add 2-3 × 50-70¢ no news)',
    filter: (s, cat) => {
      const wc = wcOf(s), p = priceOf(s);
      if (wc >= 8) return 'consensus';
      if (wc >= 6 && wc <= 7) return 'cluster';
      if (wc >= 2 && wc <= 5 && p >= 0.70 && p < 0.85) return 'micro-cluster';
      if (wc >= 2 && wc <= 3 && p >= 0.50 && p < 0.70 && cat !== 'news-event') return 'mid-favorite';
      if (wc === 1) return 'solo';
      return null;
    },
  },
  E_loose: {
    name: 'E. Loose (any 4+ cluster, 2-3 at 50-85¢)',
    filter: (s, cat) => {
      const wc = wcOf(s), p = priceOf(s);
      if (wc === 1) return 'solo';
      if (wc >= 4) return 'cluster';
      if (wc >= 2 && p >= 0.50 && p < 0.85) return 'mid-cluster';
      return null;
    },
  },
  BE_hybrid: {
    name: 'B+E hybrid',
    filter: (s, cat) => {
      const wc = wcOf(s), p = priceOf(s);
      if (wc === 1) return 'solo';
      if (wc >= 8) return 'consensus';
      if (wc >= 4 && wc <= 7) return 'cluster-loose';  // E: 4+ any price
      // 2-3 wallet paths: union of B + E
      if (wc >= 2 && wc <= 3) {
        if (p >= 0.70 && p < 0.85) return 'micro-favorite';   // current
        if (p >= 0.50 && p < 0.70 && cat !== 'news-event') return 'mid-favorite';  // B
        if (p >= 0.85) return 'micro-deep-favorite';  // E extension
      }
      return null;
    },
  },
};

// ── Evaluate each ──────────────────────────────────────────────────
const results = {};
for (const [key, cfg] of Object.entries(CONFIGS)) {
  const emitted = [];
  for (const s of resolved) {
    const cat = classify(s.marketTitle || '');
    if (!ALLOWED.has(cat)) continue;
    const path = cfg.filter(s, cat);
    if (path) emitted.push({ ...s, _emitPath: path, _category: cat });
  }
  const N = emitted.length;
  const wins = emitted.filter(s => s.outcome === 'win').length;
  const totalRet = emitted.reduce((a, s) => a + (typeof s.signalReturn === 'number' ? s.signalReturn : 0), 0);
  const retN = emitted.filter(s => typeof s.signalReturn === 'number').length;
  const avgRet = retN > 0 ? totalRet / retN : 0;
  results[key] = { ...cfg, N, wins, avgRet, totalRet, emitted };
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Head-to-head: Current vs Surgical vs Loose vs B+E Hybrid');
console.log('═══════════════════════════════════════════════════════════════════');
console.log();
console.log('  ' + 'Config'.padEnd(40) + 'N'.padStart(5) + 'WR'.padStart(6) + 'AvgRet'.padStart(9) + 'TotalRet'.padStart(11));
console.log('  ' + '─'.repeat(72));
for (const r of Object.values(results)) {
  const wr = r.N > 0 ? (r.wins/r.N*100).toFixed(0) + '%' : '—';
  const avgRet = r.N > 0 ? ((r.avgRet >= 0 ? '+' : '') + r.avgRet.toFixed(1) + '%') : '—';
  const totalRet = (r.totalRet >= 0 ? '+' : '') + r.totalRet.toFixed(0);
  console.log('  ' + r.name.padEnd(40) + String(r.N).padStart(5) + wr.padStart(6) + avgRet.padStart(9) + totalRet.padStart(11));
}

// ── Where each option diverges from current ─────────────────────────
console.log('\n  ── New signals each option adds vs Current ──');
const aIds = new Set(results.A_current.emitted.map(s => s.signalId));
for (const key of ['B_surgical', 'E_loose', 'BE_hybrid']) {
  const r = results[key];
  const newOnly = r.emitted.filter(s => !aIds.has(s.signalId));
  if (newOnly.length === 0) continue;
  const w = newOnly.filter(s => s.outcome === 'win').length;
  const tr = newOnly.reduce((a, s) => a + (typeof s.signalReturn === 'number' ? s.signalReturn : 0), 0);
  const trN = newOnly.filter(s => typeof s.signalReturn === 'number').length;
  console.log('  ' + r.name + ':');
  console.log('    Adds ' + newOnly.length + ' new signals beyond Current');
  console.log('    WR: ' + (w/newOnly.length*100).toFixed(0) + '%  AvgRet: ' + (trN>0 ? (tr/trN >= 0?'+':'')+(tr/trN).toFixed(1)+'%' : '—') + '  TotalRet: ' + (tr>=0?'+':'')+tr.toFixed(0));
}

// ── Hybrid breakdown by emit path ───────────────────────────────────
console.log('\n  ── B+E Hybrid: per-path performance ──');
const hybridByPath = {};
for (const s of results.BE_hybrid.emitted) {
  if (!hybridByPath[s._emitPath]) hybridByPath[s._emitPath] = [];
  hybridByPath[s._emitPath].push(s);
}
console.log('  ' + 'Path'.padEnd(22) + 'N'.padStart(5) + 'WR'.padStart(6) + 'AvgRet'.padStart(9) + 'TotalRet'.padStart(11));
for (const [p, arr] of Object.entries(hybridByPath).sort((a, b) => b[1].length - a[1].length)) {
  const w = arr.filter(s => s.outcome === 'win').length;
  const tr = arr.reduce((a, s) => a + (typeof s.signalReturn === 'number' ? s.signalReturn : 0), 0);
  const trN = arr.filter(s => typeof s.signalReturn === 'number').length;
  const wr = arr.length > 0 ? (w/arr.length*100).toFixed(0) + '%' : '—';
  const avgRet = trN>0 ? (tr/trN >= 0?'+':'')+(tr/trN).toFixed(1)+'%' : '—';
  console.log('  ' + p.padEnd(22) + String(arr.length).padStart(5) + wr.padStart(6) + avgRet.padStart(9) + ((tr>=0?'+':'')+tr.toFixed(0)).padStart(11));
}

console.log();
console.log('  ── Recommendation logic ──');
const sortedByTotal = Object.values(results).sort((a, b) => b.totalRet - a.totalRet);
const winner = sortedByTotal[0];
console.log('  Highest cumulative return: ' + winner.name + ' → +' + winner.totalRet.toFixed(0) + ' on ' + winner.N + ' signals');
const sortedByQuality = Object.values(results).filter(r => r.N >= 100).sort((a, b) => b.avgRet - a.avgRet);
const qualityWinner = sortedByQuality[0];
console.log('  Highest per-signal return (≥100 N): ' + qualityWinner.name + ' → ' + qualityWinner.avgRet.toFixed(1) + '%');
console.log();
