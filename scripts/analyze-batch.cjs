#!/usr/bin/env node
/**
 * Cross-tab analysis over batch-ledger CSVs (v2 format with pool-derived recency).
 * Usage: node scripts/analyze-batch.cjs out/bottom-25.v2.csv out/sample-50.v2.csv
 */
const fs = require('fs');

function parseCsv(path) {
  const lines = fs.readFileSync(path, 'utf8').trim().split(/\r?\n/);
  const header = lines.shift().split(',');
  return lines.map(line => {
    const cols = line.split(',');
    const row = {};
    header.forEach((h, i) => { row[h] = cols[i]; });
    row._source = path;
    return row;
  });
}

const paths = process.argv.slice(2);
if (!paths.length) { console.error('Usage: analyze-batch.cjs <csv> [csv ...]'); process.exit(1); }
const rows = paths.flatMap(parseCsv).filter(r => !r.error);

const n = (v) => v === '' || v == null ? null : Number(v);

// engineWR and trueWR are stored as 0-100 (percentage), NOT 0-1 ratio.
const wrPct = (r, key = 'engineWR') => n(r[key]);

console.log(`\n=== POOL TRUTH ANALYSIS: ${rows.length} wallets ===\n`);

// 1. Spearman
function spearman(xs, ys) {
  const rank = (arr) => {
    const sorted = arr.map((v, i) => [v, i]).sort((a, b) => a[0] - b[0]);
    const r = Array(arr.length);
    sorted.forEach(([_, i], k) => r[i] = k + 1);
    return r;
  };
  const rx = rank(xs), ry = rank(ys);
  const mean = a => a.reduce((s, v) => s + v, 0) / a.length;
  const mx = mean(rx), my = mean(ry);
  let num = 0, dx = 0, dy = 0;
  for (let i = 0; i < xs.length; i++) {
    num += (rx[i] - mx) * (ry[i] - my);
    dx += (rx[i] - mx) ** 2;
    dy += (ry[i] - my) ** 2;
  }
  return num / Math.sqrt(dx * dy);
}
const valid = rows.filter(r => n(r.engineScore) !== null && n(r.decidedROI) !== null);
const rhoScore = spearman(valid.map(r => n(r.engineScore)), valid.map(r => n(r.decidedROI)));
const rhoWr    = spearman(valid.map(r => wrPct(r)),         valid.map(r => n(r.decidedROI)));
const rhoTP    = spearman(valid.map(r => n(r.engineTotalPnl)), valid.map(r => n(r.decidedROI)));
console.log(`Spearman correlations vs decidedROI:`);
console.log(`  engineScore      : ${rhoScore.toFixed(3)}`);
console.log(`  engineWR         : ${rhoWr.toFixed(3)}`);
console.log(`  engineTotalPnl   : ${rhoTP.toFixed(3)}`);
console.log(`  (1=perfect, 0=noise, -1=inverse)`);

// 2. Score-band breakdown with quartiles
const bandFor = (s) => s >= 90 ? '90+' : s >= 80 ? '80-89' : s >= 75 ? '75-79' : s >= 70 ? '70-74' : '<70';
const bands = {};
for (const r of rows) {
  const s = n(r.engineScore); const d = n(r.decidedROI);
  if (s == null || d == null) continue;
  (bands[bandFor(s)] ??= []).push({ s, d, cap: n(r.decidedCapital), rank: n(r.rank) });
}
console.log('\nScore band -> decidedROI:');
console.log('  band    N    medDecROI    p25 - p75       p90      max');
for (const b of ['90+', '80-89', '75-79', '70-74', '<70']) {
  const arr = bands[b] || [];
  if (!arr.length) { console.log(`  ${b.padEnd(6)} 0`); continue; }
  const ds = arr.map(x => x.d).sort((a, b) => a - b);
  const q = (p) => ds[Math.min(ds.length - 1, Math.floor(ds.length * p))];
  console.log(`  ${b.padEnd(6)}${String(arr.length).padStart(3)}    ${q(0.5).toFixed(1).padStart(6)}%    ${q(0.25).toFixed(1).padStart(5)}% - ${q(0.75).toFixed(1).padStart(5)}%   ${q(0.9).toFixed(1).padStart(6)}%   ${q(0.99).toFixed(1).padStart(6)}%`);
}

// 3. Activity buckets (days since last trade)
console.log('\nActivity (days since last trade):');
const actBuckets = { '0-1d': [], '1-3d': [], '3-7d': [], '7-30d': [], '30d+': [], 'unknown': [] };
for (const r of rows) {
  const d = n(r.daysSinceLastTrade);
  const key = d == null ? 'unknown' : d <= 1 ? '0-1d' : d <= 3 ? '1-3d' : d <= 7 ? '3-7d' : d <= 30 ? '7-30d' : '30d+';
  actBuckets[key].push(r);
}
for (const [k, arr] of Object.entries(actBuckets)) {
  if (!arr.length) continue;
  const ds = arr.map(r => n(r.decidedROI)).filter(v => v != null).sort((a, b) => a - b);
  const med = ds[Math.floor(ds.length / 2)];
  const capMed = arr.map(r => n(r.decidedCapital)).filter(v => v != null).sort((a, b) => a - b);
  const capM = capMed[Math.floor(capMed.length / 2)];
  console.log(`  ${k.padEnd(8)} ${String(arr.length).padStart(3)}  medDecROI=${med != null ? med.toFixed(1) + '%' : 'n/a'.padStart(5)}  medDecCap=$${capM != null ? Math.round(capM).toLocaleString() : 'n/a'}`);
}

// 4. Mean-picker detection — engineWR is 0-100
console.log('\nMean-picker suspects (engineWR >= 95%, decROI < 5%, totalCapital >= $50k):');
const meanPickers = rows.filter(r => wrPct(r) >= 95 && n(r.decidedROI) < 5 && n(r.totalCapital) >= 50000);
meanPickers.sort((a, b) => n(b.totalCapital) - n(a.totalCapital))
  .slice(0, 15)
  .forEach(r => {
    console.log(`  rank=${String(r.rank).padStart(4)} score=${n(r.engineScore).toFixed(1)}  WR=${wrPct(r).toFixed(1)}%  decROI=${n(r.decidedROI).toFixed(2)}%  cap=$${Math.round(n(r.totalCapital)).toLocaleString().padStart(12)}  ${r.address}`);
  });
console.log(`  total mean-picker shapes: ${meanPickers.length}`);

// 5. Real edge candidates, now filterable by activity
console.log('\nReal edge candidates (decROI>=15%, decidedCap>=$10k):');
rows.filter(r => n(r.decidedROI) >= 15 && n(r.decidedCapital) >= 10000)
    .sort((a, b) => n(b.decidedROI) - n(a.decidedROI))
    .forEach(r => {
      const d = n(r.daysSinceLastTrade);
      const tag = d == null ? '[?]' : d <= 3 ? '[ACTIVE]' : d <= 7 ? '[slow]' : d <= 30 ? '[cooling]' : '[DEAD]';
      console.log(`  rank=${String(r.rank).padStart(4)} score=${n(r.engineScore).toFixed(1)}  decROI=${n(r.decidedROI).toFixed(1).padStart(6)}%  decCap=$${Math.round(n(r.decidedCapital)).toLocaleString().padStart(10)}  ${String(r.trueWins).padStart(4)}W/${String(r.trueLosses).padStart(4)}L  lastTrade=${d != null ? d.toFixed(1) + 'd' : '?'}  ${tag}  ${r.address}`);
    });

// 6. High score but inactive (the worst kind of ranked wallet)
console.log('\nHigh score (>=80) but INACTIVE (>=7d since trade):');
rows.filter(r => n(r.engineScore) >= 80 && n(r.daysSinceLastTrade) >= 7)
    .sort((a, b) => n(b.engineScore) - n(a.engineScore))
    .forEach(r => {
      console.log(`  rank=${String(r.rank).padStart(4)} score=${n(r.engineScore).toFixed(1)}  decROI=${n(r.decidedROI).toFixed(1).padStart(6)}%  lastTrade=${n(r.daysSinceLastTrade).toFixed(1)}d  holdHours=${n(r.avgHoldTimeHours)?.toFixed(1) || '?'}  ${r.address}`);
    });

// 7. Tiny-sample noise
console.log('\nTiny-sample noise (decROI>25% but decidedCap<$5k):');
rows.filter(r => n(r.decidedROI) > 25 && n(r.decidedCapital) < 5000)
    .sort((a, b) => n(b.decidedROI) - n(a.decidedROI))
    .forEach(r => console.log(`  rank=${String(r.rank).padStart(4)} decROI=${n(r.decidedROI).toFixed(1).padStart(6)}%  decCap=$${Math.round(n(r.decidedCapital))}  ${r.trueWins}W/${r.trueLosses}L on ${r.truePositions} pos  ${r.address}`));

// 8. Hold time distribution (signal for strategy type)
const holdTimes = rows.map(r => n(r.avgHoldTimeHours)).filter(v => v != null).sort((a, b) => a - b);
if (holdTimes.length) {
  const q = (p) => holdTimes[Math.floor(holdTimes.length * p)];
  console.log(`\nHold-time distribution (hours): p10=${q(0.1).toFixed(1)}  p25=${q(0.25).toFixed(1)}  p50=${q(0.5).toFixed(1)}  p75=${q(0.75).toFixed(1)}  p90=${q(0.9).toFixed(1)}`);
}

// 9. Headline
const realEdgeActive = rows.filter(r => n(r.decidedROI) >= 15 && n(r.decidedCapital) >= 10000 && n(r.daysSinceLastTrade) != null && n(r.daysSinceLastTrade) <= 3).length;
const realEdgeDead   = rows.filter(r => n(r.decidedROI) >= 15 && n(r.decidedCapital) >= 10000 && n(r.daysSinceLastTrade) != null && n(r.daysSinceLastTrade) >= 30).length;
const highScoreDead  = rows.filter(r => n(r.engineScore) >= 80 && n(r.daysSinceLastTrade) != null && n(r.daysSinceLastTrade) >= 30).length;
console.log('\n=== HEADLINE ===');
console.log(`  wallets with real edge + active (<=3d) : ${realEdgeActive}`);
console.log(`  wallets with real edge but dead (>=30d): ${realEdgeDead}`);
console.log(`  wallets scored >=80 but dead (>=30d)   : ${highScoreDead}`);
console.log(`  mean-picker shapes                     : ${meanPickers.length}`);
console.log(`  Spearman(score,decROI)                 : ${rhoScore.toFixed(3)}`);
console.log(`  Spearman(engineWR,decROI)              : ${rhoWr.toFixed(3)}`);
console.log(`  Spearman(engineTotalPnl,decROI)        : ${rhoTP.toFixed(3)}`);
