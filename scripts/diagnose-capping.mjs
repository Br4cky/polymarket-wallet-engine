// Diagnose impact of the 150-cap on Gamma resolution during discovery.
//
// For each pool wallet, look at:
//   - uniqueMarkets (how many unique markets they've traded)
//   - openPositions (current concurrent open holdings)
//   - decidedROI (computed from resolved positions only)
//   - score
//
// Identify:
//   - How many wallets exceed the 150-cap threshold (likely affected)
//   - Does their score behave differently from sub-150 wallets?
//   - Are top wallets disproportionately affected?

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const pool = walletsData.pool || walletsData;

const wallets = [];
for (const [addr, w] of Object.entries(pool)) {
  if (!w || typeof w !== 'object' || w.status === 'removed') continue;
  const s = w.stats || {};
  wallets.push({
    addr,
    score: w.score || 0,
    uniqueMarkets: s.uniqueMarkets || 0,
    openPositions: s.openPositions || s.decidedOpenPositions || 0,
    totalTrades: s.totalTrades || 0,
    decidedROI: s.decidedROI,
    decidedCapital: s.decidedCapital,
    singleSideROI: s.singleSideROI,
    decidedWins: s.decidedWins,
    decidedLosses: s.decidedLosses,
    metricSource: w.scoreComponents?.metricSource,
  });
}

// ── Cap-affected analysis ──────────────────────────────────────────
const CAP = 150;
const overCap = wallets.filter(w => w.openPositions > CAP);
const underCap = wallets.filter(w => w.openPositions > 0 && w.openPositions <= CAP);
const noOpen = wallets.filter(w => w.openPositions === 0);

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Gamma cap (150 open positions/wallet) impact diagnostic');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log('  Pool size: ' + wallets.length + '  active wallets');
console.log('  Wallets > ' + CAP + ' open positions (CAPPED — incomplete data): ' + overCap.length + ' (' + (overCap.length/wallets.length*100).toFixed(0) + '%)');
console.log('  Wallets ≤ ' + CAP + ' open positions (full data):              ' + underCap.length);
console.log('  Wallets with no open positions tracked:                       ' + noOpen.length);

// ── Open-position distribution ──────────────────────────────────────
console.log('\n  ── Open-position distribution ──');
const bands = [
  ['0', w => w.openPositions === 0],
  ['1-25', w => w.openPositions >= 1 && w.openPositions <= 25],
  ['26-75', w => w.openPositions >= 26 && w.openPositions <= 75],
  ['76-150', w => w.openPositions >= 76 && w.openPositions <= 150],
  ['151-500', w => w.openPositions >= 151 && w.openPositions <= 500],
  ['501-1000', w => w.openPositions >= 501 && w.openPositions <= 1000],
  ['1000+', w => w.openPositions > 1000],
];
for (const [label, pred] of bands) {
  const n = wallets.filter(pred).length;
  const pct = (n / wallets.length * 100).toFixed(0);
  const bar = '█'.repeat(Math.round(n / 10));
  console.log('  ' + label.padEnd(10) + ' ' + String(n).padStart(4) + '  ' + pct.padStart(3) + '%  ' + bar);
}

// ── Score and decidedROI by cap status ──────────────────────────────
console.log('\n  ── Score / decidedROI comparison ──');
function summary(arr, label) {
  if (arr.length === 0) { console.log('  ' + label.padEnd(28) + 'EMPTY'); return; }
  const scoreSorted = arr.map(w => w.score).sort((a, b) => a - b);
  const roiSorted = arr.filter(w => w.decidedROI != null).map(w => w.decidedROI).sort((a, b) => a - b);
  const capSorted = arr.filter(w => w.decidedCapital != null).map(w => w.decidedCapital).sort((a, b) => a - b);
  const med = (a) => a.length ? a[Math.floor(a.length / 2)] : null;
  console.log('  ' + label.padEnd(28) +
    'medScore=' + med(scoreSorted).toFixed(1) +
    '  medROI=' + (med(roiSorted) != null ? (med(roiSorted) * 100).toFixed(0) + '%' : '—') +
    '  medCap=$' + (med(capSorted) != null ? (med(capSorted) / 1000).toFixed(0) + 'k' : '—'));
}
summary(overCap, 'CAPPED (>150 open):');
summary(underCap, 'UNCAPPED (1-150 open):');
summary(noOpen, 'NO OPEN tracked:');

// ── Top 20 capped wallets — likely the most affected ───────────────
console.log('\n  ── Top 20 CAPPED wallets (by open-position count) ──');
overCap.sort((a, b) => b.openPositions - a.openPositions);
console.log('  ' + 'Wallet'.padEnd(14) + ' ' + 'OpenPos'.padStart(7) + ' ' + 'UniqMkts'.padStart(8) + ' ' + 'Score'.padStart(6) + ' ' + 'decROI'.padStart(7) + ' ' + 'decCap'.padStart(9) + ' ' + 'D-W/L');
for (const w of overCap.slice(0, 20)) {
  const roi = w.decidedROI != null ? (w.decidedROI * 100).toFixed(0) + '%' : '—';
  const cap = w.decidedCapital != null ? '$' + (w.decidedCapital / 1000).toFixed(0) + 'k' : '—';
  const wl = (w.decidedWins != null ? w.decidedWins : '—') + '/' + (w.decidedLosses != null ? w.decidedLosses : '—');
  console.log('  ' + w.addr.slice(0, 12).padEnd(14) +
    ' ' + String(w.openPositions).padStart(7) +
    ' ' + String(w.uniqueMarkets).padStart(8) +
    ' ' + w.score.toFixed(1).padStart(6) +
    ' ' + roi.padStart(7) +
    ' ' + cap.padStart(9) +
    ' ' + wl);
}

// ── Top 10 wallets by score — are they capped? ─────────────────────
console.log('\n  ── Top 10 by score — cap status ──');
const topByScore = [...wallets].sort((a, b) => b.score - a.score).slice(0, 10);
console.log('  ' + 'Wallet'.padEnd(14) + ' ' + 'Score'.padStart(6) + ' ' + 'OpenPos'.padStart(7) + ' ' + 'CAPPED?'.padStart(8));
for (const w of topByScore) {
  const capped = w.openPositions > CAP ? 'YES' : (w.openPositions > 0 ? 'no' : 'n/a');
  console.log('  ' + w.addr.slice(0, 12).padEnd(14) + ' ' + w.score.toFixed(1).padStart(6) + ' ' + String(w.openPositions).padStart(7) + ' ' + capped.padStart(8));
}

// ── Severity check — do capped wallets have suspiciously low decidedCapital? ──
console.log('\n  ── Severity: capped wallets with disproportionately low decidedCapital ──');
const severe = overCap.filter(w => w.decidedCapital != null && w.decidedCapital < 10000 && w.openPositions > 200);
console.log('  Wallets with >200 open positions but <$10k decidedCapital: ' + severe.length);
console.log('  (Suggests we are dramatically under-counting their resolved capital)');
if (severe.length > 0) {
  severe.sort((a, b) => b.openPositions - a.openPositions);
  for (const w of severe.slice(0, 10)) {
    console.log('    ' + w.addr.slice(0, 12) + '  open=' + w.openPositions + '  decCap=$' + Math.round(w.decidedCapital) + '  score=' + w.score.toFixed(1));
  }
}

console.log('\n═══════════════════════════════════════════════════════════════════\n');
