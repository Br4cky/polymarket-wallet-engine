// Category specialization finder
// --------------------------------
// For every wallet with ≥MIN signals, compute per-category WR and avg return.
// Reports:
//   1) Overall per-category leaderboard (which category has the best signals
//      on average, to prioritise candidate markets)
//   2) Wallet specialists: wallets with concentrated per-category edge
//      (≥5 signals in a single category with ≥60% WR AND ≥30% avg return).
//      These are candidates for per-category sub-pools.
//
// Category inference reuses the same rules as scripts/market-deep-dive.mjs

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const args = process.argv.slice(2);
const get = (f, d) => { const i = args.indexOf(f); return i >= 0 && i + 1 < args.length ? args[i + 1] : d; };
const MIN_TOTAL = parseInt(get('--min', '5'), 10);
const MIN_CATEGORY = parseInt(get('--min-cat', '5'), 10);
const MIN_WR = parseFloat(get('--min-wr', '0.60'));
const MIN_RET = parseFloat(get('--min-ret', '30'));  // percent
const TOP_N = parseInt(get('--top', '25'), 10);

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const pool = walletsData.pool || walletsData;

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

// Load market lookup to classify signals
const marketsRaw = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/markets.json.gz'))).toString());
const marketsArr = Array.isArray(marketsRaw) ? marketsRaw : (marketsRaw.markets || Object.values(marketsRaw));
const marketLookup = new Map();
for (const m of marketsArr) {
  const id = m.condition_id || m.conditionId || m.id || m.marketId;
  if (id) marketLookup.set(String(id).toLowerCase(), m);
}

const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');

// Per-wallet per-category accumulators
const perWallet = new Map();  // addr → Map(category → {signals, wins, totalRet, retN})
const perCategory = new Map(); // category → {signals, wins, totalRet, retN}

function getCategory(sig) {
  if (sig.marketTitle) return classify(sig.marketTitle);
  if (sig.question) return classify(sig.question);
  const cid = sig.conditionId || sig.condition_id || sig.marketId;
  if (cid) {
    const mk = marketLookup.get(String(cid).toLowerCase());
    if (mk) return classify(mk.question || mk.title || mk.marketTitle || '');
  }
  return 'other';
}

for (const sig of resolved) {
  const cat = getCategory(sig);
  const ws = new Set();
  if (Array.isArray(sig.currentWallets)) sig.currentWallets.forEach(w => w && w.address && ws.add(w.address.toLowerCase()));
  if (sig.soloWallet) ws.add(sig.soloWallet.toLowerCase());
  if (Array.isArray(sig.wallets)) sig.wallets.forEach(w => w && w.address && ws.add(w.address.toLowerCase()));
  const ret = typeof sig.signalReturn === 'number' ? sig.signalReturn : null;

  // per-category (overall)
  if (!perCategory.has(cat)) perCategory.set(cat, { signals: 0, wins: 0, totalRet: 0, retN: 0 });
  const pc = perCategory.get(cat);
  pc.signals++;
  if (sig.outcome === 'win') pc.wins++;
  if (ret !== null) { pc.totalRet += ret; pc.retN++; }

  for (const addr of ws) {
    if (!perWallet.has(addr)) perWallet.set(addr, new Map());
    const wm = perWallet.get(addr);
    if (!wm.has(cat)) wm.set(cat, { signals: 0, wins: 0, totalRet: 0, retN: 0 });
    const rec = wm.get(cat);
    rec.signals++;
    if (sig.outcome === 'win') rec.wins++;
    if (ret !== null) { rec.totalRet += ret; rec.retN++; }
  }
}

// Report 1: per-category overall
console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  CATEGORY overall performance (all resolved signals)');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log(`  ${'Category'.padEnd(14)}  ${'N'.padStart(5)}  ${'WR'.padStart(6)}  ${'AvgRet'.padStart(9)}`);
console.log('  ' + '─'.repeat(45));
const catRows = [...perCategory.entries()].map(([c, r]) => ({
  cat: c, signals: r.signals, wr: r.wins / r.signals, avgRet: r.retN > 0 ? r.totalRet / r.retN : 0,
}));
catRows.sort((a, b) => b.avgRet - a.avgRet);
for (const r of catRows) {
  const wr = (r.wr * 100).toFixed(1) + '%';
  const ret = (r.avgRet >= 0 ? '+' : '') + r.avgRet.toFixed(1) + '%';
  console.log(`  ${r.cat.padEnd(14)}  ${String(r.signals).padStart(5)}  ${wr.padStart(6)}  ${ret.padStart(9)}`);
}

// Report 2: per-wallet specialists
console.log('\n═══════════════════════════════════════════════════════════════════');
console.log(`  Per-wallet specialists (≥${MIN_CATEGORY} sigs in category, WR≥${(MIN_WR*100).toFixed(0)}%, ret≥${MIN_RET}%)`);
console.log('═══════════════════════════════════════════════════════════════════\n');

const specialists = [];
for (const [addr, wm] of perWallet) {
  const total = [...wm.values()].reduce((a, r) => a + r.signals, 0);
  if (total < MIN_TOTAL) continue;
  const rows = [...wm.entries()].map(([c, r]) => ({
    cat: c, signals: r.signals, wr: r.wins / r.signals, avgRet: r.retN > 0 ? r.totalRet / r.retN : 0,
    concentration: r.signals / total,
  })).filter(r => r.signals >= MIN_CATEGORY && r.wr >= MIN_WR && r.avgRet >= MIN_RET);
  for (const r of rows) specialists.push({ addr, total, ...r });
}
specialists.sort((a, b) => b.avgRet - a.avgRet);

if (specialists.length === 0) {
  console.log('  No specialists meeting criteria. Try --min-cat 3 or --min-ret 15.\n');
} else {
  console.log(`  ${'Wallet'.padEnd(16)}  ${'Category'.padEnd(14)}  ${'N'.padStart(4)}  ${'Total'.padStart(5)}  ${'Conc'.padStart(6)}  ${'WR'.padStart(6)}  ${'AvgRet'.padStart(9)}  ${'Score'.padStart(6)}`);
  console.log('  ' + '─'.repeat(85));
  for (const s of specialists.slice(0, TOP_N)) {
    const poolW = pool[s.addr];
    const score = poolW && typeof poolW.score === 'number' ? poolW.score.toFixed(1) : '—';
    const conc = (s.concentration * 100).toFixed(0) + '%';
    const wr = (s.wr * 100).toFixed(0) + '%';
    const ret = '+' + s.avgRet.toFixed(1) + '%';
    console.log(`  ${s.addr.slice(0, 14).padEnd(16)}  ${s.cat.padEnd(14)}  ${String(s.signals).padStart(4)}  ${String(s.total).padStart(5)}  ${conc.padStart(6)}  ${wr.padStart(6)}  ${ret.padStart(9)}  ${score.padStart(6)}`);
  }
  console.log();
}

// Report 3: category → top-3 best contributors (for per-category sub-pool)
console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Top contributors by category (candidates for per-category sub-pools)');
console.log('═══════════════════════════════════════════════════════════════════\n');

const byCat = new Map();  // cat → [{addr, signals, wr, avgRet}...]
for (const [addr, wm] of perWallet) {
  for (const [cat, r] of wm) {
    if (r.signals < 3) continue;
    if (!byCat.has(cat)) byCat.set(cat, []);
    byCat.get(cat).push({ addr, signals: r.signals, wr: r.wins / r.signals, avgRet: r.retN > 0 ? r.totalRet / r.retN : 0 });
  }
}
for (const [cat, rows] of byCat) {
  // rank by avgRet * sqrt(signals) to balance size with quality
  rows.sort((a, b) => (b.avgRet * Math.sqrt(b.signals)) - (a.avgRet * Math.sqrt(a.signals)));
  const top = rows.slice(0, 3).filter(r => r.avgRet > 0);
  if (top.length === 0) continue;
  console.log(`  ${cat}`);
  for (const r of top) {
    const poolW = pool[r.addr];
    const score = poolW && typeof poolW.score === 'number' ? poolW.score.toFixed(1) : '—';
    const wr = (r.wr * 100).toFixed(0) + '%';
    const ret = '+' + r.avgRet.toFixed(1) + '%';
    console.log(`    ${r.addr.slice(0, 14)}…  ${String(r.signals).padStart(3)} sigs  WR ${wr.padStart(4)}  Ret ${ret.padStart(7)}  Score ${score}`);
  }
}

console.log('\n═══════════════════════════════════════════════════════════════════\n');
