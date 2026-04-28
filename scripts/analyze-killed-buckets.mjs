// Deep-segment the historical signal buckets that current type_floor
// rejects (2-5 wallets, entry NOT in 70-85¢) to find any sub-segments
// that were actually profitable. If certain category × walletCount ×
// price combinations are net-positive, we could add path-specific
// admissions instead of blanket rejection.
//
// Also looks at solo single-wallet signals at all price bands to
// confirm the solo path is doing its job for non-favorite prices.

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

const ALLOWED_CATEGORIES = new Set([
  'tennis', 'nba', 'mma', 'weather',
  'crypto-updown', 'crypto-other',
  'mlb', 'nfl', 'macro', 'ai-tech',
  'token-launch', 'news-event',
  'soccer',
]);

// Filter to only whitelisted-category resolved signals
const valid = resolved.filter(s => ALLOWED_CATEGORIES.has(classify(s.marketTitle || '')));
console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Killed-bucket deep dive — what would actually be profitable?');
console.log('═══════════════════════════════════════════════════════════════════');
console.log(`  Resolved signals (whitelisted categories): ${valid.length}`);
console.log();

// ── Current system: which historical signals would emit today? ──────
function currentEmits(s) {
  const wc = s.walletCount || 0;
  const entry = s.avgEntryPrice || 0;
  if (wc >= 8) return 'consensus';
  if (wc >= 6 && wc <= 7) return 'cluster';
  if (wc >= 2 && wc <= 5 && entry >= 0.70 && entry < 0.85) return 'micro-cluster';
  if (wc === 1) return 'solo';
  return null;  // type_floor — rejected
}

const emitting = valid.filter(s => currentEmits(s) !== null);
const killed = valid.filter(s => currentEmits(s) === null);
console.log('  ── Current system summary ──');
function summary(arr, label) {
  const w = arr.filter(s => s.outcome === 'win').length;
  const r = arr.reduce((a, s) => a + (typeof s.signalReturn === 'number' ? s.signalReturn : 0), 0);
  const rN = arr.filter(s => typeof s.signalReturn === 'number').length;
  console.log('  ' + label.padEnd(35) + ' N=' + String(arr.length).padStart(5) +
    '  WR=' + (arr.length > 0 ? (w/arr.length*100).toFixed(0) + '%' : '—').padStart(4) +
    '  AvgRet=' + (rN > 0 ? ((r/rN >= 0 ? '+' : '') + (r/rN).toFixed(1) + '%') : '—').padStart(7) +
    '  TotalRet=' + (rN > 0 ? ((r >= 0 ? '+' : '') + r.toFixed(0)) : '0'));
}
summary(emitting, 'Emitted signals (current system)');
summary(killed, 'KILLED at type_floor');
console.log();

// ── Sub-segment killed signals ──────────────────────────────────────
console.log('  ── Killed bucket (2-5 wallets, NOT 70-85¢) by CATEGORY ──');
const killedByCat = {};
for (const s of killed) {
  const c = classify(s.marketTitle || '');
  if (!killedByCat[c]) killedByCat[c] = [];
  killedByCat[c].push(s);
}
const catRows = Object.entries(killedByCat).map(([cat, arr]) => {
  const w = arr.filter(s => s.outcome === 'win').length;
  const r = arr.reduce((a, s) => a + (typeof s.signalReturn === 'number' ? s.signalReturn : 0), 0);
  const rN = arr.filter(s => typeof s.signalReturn === 'number').length;
  return { cat, n: arr.length, wr: w/arr.length, avgRet: rN > 0 ? r/rN : 0, totalRet: r };
});
catRows.sort((a, b) => b.totalRet - a.totalRet);
console.log('  ' + 'Category'.padEnd(14) + ' ' + 'N'.padStart(5) + ' ' + 'WR'.padStart(5) + ' ' + 'AvgRet'.padStart(8) + ' ' + 'TotalRet'.padStart(10));
for (const r of catRows) {
  if (r.n < 5) continue;  // skip tiny
  const positive = r.totalRet > 0 ? '✓' : '✗';
  console.log('  ' + r.cat.padEnd(14) + ' ' + String(r.n).padStart(5) +
    ' ' + (r.wr * 100).toFixed(0).padStart(4) + '% ' +
    ' ' + ((r.avgRet >= 0 ? '+' : '') + r.avgRet.toFixed(1) + '%').padStart(8) +
    ' ' + ((r.totalRet >= 0 ? '+' : '') + r.totalRet.toFixed(0)).padStart(10) +
    '  ' + positive);
}
console.log();

// ── Killed bucket × walletCount × priceBand cross-cut ──────────────
console.log('  ── Killed bucket × walletCount × priceBand ──');
const wcBands = [['1', s => (s.walletCount || 0) === 1],
  ['2-3', s => { const w = s.walletCount || 0; return w >= 2 && w <= 3; }],
  ['4-5', s => { const w = s.walletCount || 0; return w >= 4 && w <= 5; }]];
const priceBands = [['<30¢', p => p < 0.30],
  ['30-50¢', p => p >= 0.30 && p < 0.50],
  ['50-70¢', p => p >= 0.50 && p < 0.70],
  ['≥85¢', p => p >= 0.85]];

console.log('  ' + 'WalletCount'.padEnd(12) + priceBands.map(([l]) => l.padStart(14)).join(''));
for (const [wcLabel, wcPred] of wcBands) {
  const cells = priceBands.map(([_, pPred]) => {
    const arr = killed.filter(s => wcPred(s) && pPred(s.avgEntryPrice || 0));
    if (arr.length === 0) return '—';
    const w = arr.filter(s => s.outcome === 'win').length;
    const r = arr.reduce((a, s) => a + (typeof s.signalReturn === 'number' ? s.signalReturn : 0), 0);
    const rN = arr.filter(s => typeof s.signalReturn === 'number').length;
    const avgRet = rN > 0 ? (r/rN >= 0 ? '+' : '') + (r/rN).toFixed(0) + '%' : '—';
    return `${arr.length}/${avgRet}`;
  });
  console.log('  ' + wcLabel.padEnd(12) + cells.map(c => c.padStart(14)).join(''));
}
console.log('  Cells: count / avgReturn');
console.log();

// ── For categories that look profitable in killed bucket, drill down ──
console.log('  ── Drill-down: which (category, walletCount, priceBand) cells are profitable? ──');
const positiveCells = [];
for (const [cat, arr] of Object.entries(killedByCat)) {
  if (arr.length < 10) continue;
  for (const [wcLabel, wcPred] of wcBands) {
    for (const [pLabel, pPred] of priceBands) {
      const sub = arr.filter(s => wcPred(s) && pPred(s.avgEntryPrice || 0));
      if (sub.length < 10) continue;
      const w = sub.filter(s => s.outcome === 'win').length;
      const r = sub.reduce((a, s) => a + (typeof s.signalReturn === 'number' ? s.signalReturn : 0), 0);
      const rN = sub.filter(s => typeof s.signalReturn === 'number').length;
      const avgRet = rN > 0 ? r/rN : 0;
      if (avgRet > 5 && w/sub.length >= 0.50) {
        positiveCells.push({ cat, wcLabel, pLabel, n: sub.length, wr: w/sub.length, avgRet, totalRet: r });
      }
    }
  }
}
positiveCells.sort((a, b) => b.totalRet - a.totalRet);
if (positiveCells.length === 0) {
  console.log('  No profitable cells found in current type_floor bucket.');
  console.log('  → Confirms current system is correctly rejecting these.');
} else {
  console.log('  Profitable cells worth admitting (avgRet >5%, WR ≥50%):');
  console.log('  ' + 'Cat'.padEnd(14) + 'WC'.padEnd(6) + 'Price'.padEnd(10) + 'N'.padStart(4) + ' WR'.padStart(5) + ' AvgRet'.padStart(8) + ' TotalRet'.padStart(10));
  for (const c of positiveCells) {
    console.log('  ' + c.cat.padEnd(14) + c.wcLabel.padEnd(6) + c.pLabel.padEnd(10) +
      String(c.n).padStart(4) + ' ' + (c.wr * 100).toFixed(0).padStart(4) + '%' +
      ' ' + ((c.avgRet >= 0 ? '+' : '') + c.avgRet.toFixed(1) + '%').padStart(8) +
      ' ' + ((c.totalRet >= 0 ? '+' : '') + c.totalRet.toFixed(0)).padStart(10));
  }
}
console.log();

// ── Summary recommendation ──────────────────────────────────────────
const totalKilledRet = killed.reduce((a, s) => a + (typeof s.signalReturn === 'number' ? s.signalReturn : 0), 0);
const totalKilledWins = killed.filter(s => s.outcome === 'win').length;
console.log('  ── Bottom line ──');
console.log('  If we admitted ALL ' + killed.length + ' currently-killed signals:');
console.log('    Total return added: ' + (totalKilledRet >= 0 ? '+' : '') + totalKilledRet.toFixed(0) + '%');
console.log('    Win rate of killed bucket: ' + (totalKilledWins/killed.length*100).toFixed(0) + '%');
if (totalKilledRet < 0) {
  console.log('  → Killed bucket is NET NEGATIVE in aggregate. Current system is correct to reject.');
  if (positiveCells.length > 0) {
    console.log('  → BUT specific sub-cells ARE profitable. Could selectively admit those for net gain.');
  }
} else {
  console.log('  → Killed bucket is net positive! Current system is over-rejecting.');
}
console.log();
