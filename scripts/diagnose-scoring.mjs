// Diagnose the score-inversion puzzle:
//   Top attributor   0x0ebbb3e9fde5  score 25.0  14 sig  71% WR  +162%
//   Worst in-pool    0x6204a0e099b8  score 31.6  10 sig  20% WR  -62%
//   Worst in-pool    0x0c84d454825e  score 35.6   5 sig  20% WR  -58%
//
// Pulls full scoreComponents + stats and walks through the multiplier chain
// for each, so we can see WHICH term is producing the inverted ranking.

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const pool = walletsData.pool || walletsData;

const prefixes = [
  { tag: 'TOP  (underscored)', addr: '0x0ebbb3e9fde5' },
  { tag: 'WORST in-pool #1', addr: '0x6204a0e099b8' },
  { tag: 'WORST in-pool #2', addr: '0x0c84d454825e' },
];

const allKeys = Object.keys(pool);
const findKey = (pfx) => allKeys.find(k => k.toLowerCase().startsWith(pfx.toLowerCase()));

// Build a signal attribution map for cross-reference
const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');
const attr = new Map();
for (const sig of resolved) {
  const ws = new Set();
  if (Array.isArray(sig.currentWallets)) sig.currentWallets.forEach(w => w && w.address && ws.add(w.address.toLowerCase()));
  if (sig.soloWallet) ws.add(sig.soloWallet.toLowerCase());
  if (Array.isArray(sig.wallets)) sig.wallets.forEach(w => w && w.address && ws.add(w.address.toLowerCase()));
  for (const a of ws) {
    if (!attr.has(a)) attr.set(a, { signals: 0, wins: 0, totalRet: 0, retN: 0, byCat: {} });
    const rec = attr.get(a);
    rec.signals++;
    if (sig.outcome === 'win') rec.wins++;
    if (typeof sig.signalReturn === 'number') { rec.totalRet += sig.signalReturn; rec.retN++; }
  }
}

const fmtPct = (x) => (x >= 0 ? '+' : '') + (x * 100).toFixed(1) + '%';
const fmtNum = (x, dp = 3) => typeof x === 'number' ? x.toFixed(dp) : '—';

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Score-inversion diagnostic');
console.log('═══════════════════════════════════════════════════════════════════');

for (const { tag, addr } of prefixes) {
  const key = findKey(addr);
  console.log(`\n── ${tag}  [${addr}…] ─────────────────────────────`);
  if (!key) { console.log('  NOT FOUND in pool'); continue; }
  const w = pool[key];
  console.log(`  address:    ${key}`);
  console.log(`  score:      ${w.score}`);
  console.log(`  status:     ${w.status || 'active'}`);
  const c = w.scoreComponents || {};
  console.log('\n  ── Score components (how score was built) ──');
  console.log(`    roi  (base decidedROI × 100):     ${fmtNum(c.roi)}`);
  console.log(`    capConf  (capital-confidence):     ${fmtNum(c.capConf)}`);
  console.log(`    sampleConf  (sample-size conf):    ${fmtNum(c.sampleConf)}`);
  console.log(`    recency  (recent activity):        ${fmtNum(c.recency)}`);
  console.log(`    meanPickerPenalty (avg-entry):     ${fmtNum(c.meanPickerPenalty)}`);
  console.log(`    mmPenalty (market-making flag):    ${fmtNum(c.mmPenalty)}  (mmScore=${fmtNum(c.mmScore)})`);
  console.log(`    activityBonus:                     +${fmtNum(c.activityBonus)}`);
  const core = (c.roi ?? 0) * (c.capConf ?? 1) * (c.sampleConf ?? 1) * (c.recency ?? 1) * (c.meanPickerPenalty ?? 1) * (c.mmPenalty ?? 1);
  const reconstructed = core + (c.activityBonus ?? 0);
  console.log(`    ── reconstructed core:             ${fmtNum(core)}`);
  console.log(`    ── + activity bonus:               ${fmtNum(reconstructed)}   (stored score ${w.score})`);
  console.log(`    resolved trades (pool-level):      ${c.resolved ?? '—'}`);

  const s = w.stats || {};
  console.log('\n  ── Key stats ──');
  console.log(`    decidedROI (singleSideROI):        ${fmtNum(s.singleSideROI)}`);
  console.log(`    winRate (pool-level):              ${fmtNum(s.winRate)}  (${s.wins}W/${s.losses}L across ${s.resolvedMarkets} markets)`);
  console.log(`    recentWinRate:                     ${fmtNum(s.recentWinRate)}`);
  console.log(`    singleSideHitRate:                 ${fmtNum(s.singleSideHitRate)}`);
  console.log(`    totalPnl / economicPnl:            $${fmtNum(s.totalPnl, 0)} / $${fmtNum(s.economicPnl, 0)}`);
  console.log(`    avgTradeRoi:                       ${fmtNum(s.avgTradeRoi)}`);
  console.log(`    avgEntryPrice:                     ${fmtNum(s.avgEntryPrice)}`);
  console.log(`    totalTrades / uniqueMarkets:       ${s.totalTrades} / ${s.uniqueMarkets}`);
  console.log(`    avgHoldTimeHours:                  ${fmtNum(s.avgHoldTimeHours, 0)}`);

  const a = attr.get(key.toLowerCase());
  console.log('\n  ── Actual signal history (what we copy) ──');
  if (!a) {
    console.log('    NO signal history');
  } else {
    const wr = a.signals ? a.wins / a.signals : 0;
    const avgRet = a.retN ? a.totalRet / a.retN / 100 : 0;
    console.log(`    signals contributed:               ${a.signals}  (${a.wins}W/${a.signals - a.wins}L)`);
    console.log(`    signal WR:                         ${fmtPct(wr)}`);
    console.log(`    signal avg return:                 ${fmtPct(avgRet)}`);
    console.log(`    inferred attribution multiplier:   ${fmtNum(Math.max(0.2, Math.min(1.5, 1 + avgRet * 2)))}  (pending, next scan)`);
  }
}
console.log('\n═══════════════════════════════════════════════════════════════════\n');
