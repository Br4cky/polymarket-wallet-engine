// Diagnose how many pool wallets are scored via the singleSide fallback
// (because decidedROI/decidedCapital are missing) vs decided metrics.
// Check whether fallback wallets are systematically over/under-scored.

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
  const c = w.scoreComponents || {};
  wallets.push({
    addr,
    score: w.score || 0,
    decidedROI: s.decidedROI,
    decidedCapital: s.decidedCapital,
    decidedWins: s.decidedWins,
    decidedLosses: s.decidedLosses,
    singleSideROI: s.singleSideROI,
    singleSideCapital: s.singleSideCapital,
    metricSource: c.metricSource,
    roiInput: c.roiInput,
    capInput: c.capInput,
    roiPts: c.roi,
    openPositions: s.openPositions || s.decidedOpenPositions || 0,
  });
}

// Buckets
const noDecided = wallets.filter(w => w.decidedROI == null || w.decidedCapital == null);
const hasDecided = wallets.filter(w => w.decidedROI != null && w.decidedCapital != null);
const decidedZero = wallets.filter(w => w.decidedCapital === 0);
const decidedNeg = wallets.filter(w => w.decidedROI != null && w.decidedROI < 0);

const med = (arr) => arr.length ? arr.sort((a, b) => a - b)[Math.floor(arr.length / 2)] : null;

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Decided-metrics fallback diagnostic');
console.log('═══════════════════════════════════════════════════════════════════');
console.log('  Total active wallets: ' + wallets.length);
console.log();

console.log('  ── Decided-metric availability ──');
console.log('  decidedROI populated:        ' + hasDecided.length + ' (' + (hasDecided.length/wallets.length*100).toFixed(0) + '%)');
console.log('  decidedROI MISSING (null):   ' + noDecided.length + ' (' + (noDecided.length/wallets.length*100).toFixed(0) + '%)');
console.log('  decidedCapital == 0:         ' + decidedZero.length);
console.log('  decidedROI negative:         ' + decidedNeg.length);

// metricSource breakdown
console.log('\n  ── Score metricSource (which input scoring used) ──');
const bySource = {};
for (const w of wallets) {
  const k = w.metricSource || 'NOT_SET';
  bySource[k] = (bySource[k] || 0) + 1;
}
for (const [k, v] of Object.entries(bySource)) {
  console.log('  ' + k.padEnd(16) + String(v).padStart(5) + '  (' + (v/wallets.length*100).toFixed(0) + '%)');
}

// Score distribution by metricSource
console.log('\n  ── Score distribution by metricSource ──');
console.log('  ' + 'Source'.padEnd(14) + ' ' + 'N'.padStart(5) + ' ' + 'medScore'.padStart(9) + ' ' + 'medROIin'.padStart(9) + ' ' + 'medCapIn'.padStart(10) + ' ' + 'medRoiPts'.padStart(10));
for (const src of Object.keys(bySource)) {
  const arr = wallets.filter(w => (w.metricSource || 'NOT_SET') === src);
  if (arr.length === 0) continue;
  const scores = arr.map(w => w.score);
  const rois = arr.filter(w => w.roiInput != null).map(w => w.roiInput);
  const caps = arr.filter(w => w.capInput != null).map(w => w.capInput);
  const pts = arr.filter(w => w.roiPts != null).map(w => w.roiPts);
  console.log('  ' + src.padEnd(14) + ' ' + String(arr.length).padStart(5) +
    ' ' + (med(scores.slice()) ?? 0).toFixed(1).padStart(9) +
    ' ' + ((med(rois.slice()) != null) ? (med(rois.slice()) * 100).toFixed(0) + '%' : '—').padStart(9) +
    ' ' + ((med(caps.slice()) != null) ? '$' + (med(caps.slice()) / 1000).toFixed(0) + 'k' : '—').padStart(10) +
    ' ' + ((med(pts.slice()) != null) ? med(pts.slice()).toFixed(1) : '—').padStart(10));
}

// Top 10 by score — are they singleside?
console.log('\n  ── Top 10 by score: which path scored them? ──');
const top10 = [...wallets].sort((a, b) => b.score - a.score).slice(0, 10);
console.log('  ' + 'Wallet'.padEnd(14) + ' ' + 'Score'.padStart(6) + ' ' + 'metricSource'.padEnd(14) + ' ' + 'decROI'.padStart(8) + ' ' + 'ssROI'.padStart(8) + ' ' + 'roiInput'.padStart(9) + ' ' + 'capInput'.padStart(10));
for (const w of top10) {
  const d = w.decidedROI != null ? (w.decidedROI * 100).toFixed(0) + '%' : '—';
  const s = w.singleSideROI != null ? (w.singleSideROI * 100).toFixed(0) + '%' : '—';
  const ri = w.roiInput != null ? (w.roiInput * 100).toFixed(0) + '%' : '—';
  const ci = w.capInput != null ? '$' + (w.capInput / 1000).toFixed(0) + 'k' : '—';
  console.log('  ' + w.addr.slice(0, 12).padEnd(14) + ' ' + w.score.toFixed(1).padStart(6) +
    ' ' + (w.metricSource || 'NOT_SET').padEnd(14) +
    ' ' + d.padStart(8) + ' ' + s.padStart(8) + ' ' + ri.padStart(9) + ' ' + ci.padStart(10));
}

// Is singleside ROI systematically higher than decided ROI? If so, fallback wallets
// are getting INFLATED scores compared to wallets scored on decided.
console.log('\n  ── ROI input distribution: singleSide vs decided ──');
const ssWallets = wallets.filter(w => w.singleSideROI != null && w.decidedROI != null);
if (ssWallets.length > 0) {
  const decidedROIs = ssWallets.map(w => w.decidedROI).sort((a, b) => a - b);
  const ssROIs = ssWallets.map(w => w.singleSideROI).sort((a, b) => a - b);
  const medDec = decidedROIs[Math.floor(decidedROIs.length / 2)];
  const medSS = ssROIs[Math.floor(ssROIs.length / 2)];
  console.log('  Median decidedROI (when populated):       ' + (medDec * 100).toFixed(1) + '%');
  console.log('  Median singleSideROI (same wallets):      ' + (medSS * 100).toFixed(1) + '%');
  console.log('  Median gap (singleSide - decided):        +' + ((medSS - medDec) * 100).toFixed(1) + 'pp');
  console.log('  → If gap is large, fallback wallets get a bonus they don\'t deserve');
}

console.log('\n═══════════════════════════════════════════════════════════════════\n');
