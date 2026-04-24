// Diagnose why Option 2 composite emission is producing few signals.
// Shows:
//   1. Pool wallet style distribution (sniper/averager/churner/mixed/holder/mm-like)
//   2. How many wallets are eligible for each emission path
//   3. Recent signal-history activity — how many convergences did the pool
//      actually produce, and how many did each gate kill?

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const pool = walletsData.pool || walletsData;

function classifyStyle(stats) {
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

const styles = {};
const activeStyles = {};
const byScore = { '<15': 0, '15-24': 0, '25-29': 0, '30-34': 0, '35+': 0 };
let soloEligibleSniper = 0;
let perWalletMinGate = 0;
let eligibleForConvergence = 0;

for (const [addr, w] of Object.entries(pool)) {
  if (!w || typeof w !== 'object') continue;
  const s = classifyStyle(w.stats);
  styles[s] = (styles[s] || 0) + 1;
  if (w.status === 'removed') continue;
  activeStyles[s] = (activeStyles[s] || 0) + 1;

  const sc = w.score || 0;
  if (sc < 15) byScore['<15']++;
  else if (sc < 25) byScore['15-24']++;
  else if (sc < 30) byScore['25-29']++;
  else if (sc < 35) byScore['30-34']++;
  else byScore['35+']++;

  // Solo gate: sniper + score >= 30
  if (sc >= 30 && s === 'sniper') soloEligibleSniper++;

  // Convergence sourcing gate: score >= 15 AND not holder/mm-like
  if (sc >= 15 && s !== 'holder' && s !== 'mm-like') eligibleForConvergence++;

  if (sc >= 15) perWalletMinGate++;
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Emission-gate diagnostic — why so few signals?');
console.log('═══════════════════════════════════════════════════════════════════\n');

console.log('  Pool styles (ALL including removed):');
for (const [k, v] of Object.entries(styles).sort((a, b) => b[1] - a[1])) {
  console.log('    ' + k.padEnd(12) + String(v).padStart(5));
}

console.log('\n  Pool styles (ACTIVE only):');
for (const [k, v] of Object.entries(activeStyles).sort((a, b) => b[1] - a[1])) {
  console.log('    ' + k.padEnd(12) + String(v).padStart(5));
}

console.log('\n  Active score distribution:');
for (const [k, v] of Object.entries(byScore)) {
  console.log('    ' + k.padEnd(8) + String(v).padStart(5));
}

console.log('\n  Gate eligibility:');
console.log('    Solo-eligible (sniper + score >= 30):          ' + soloEligibleSniper);
console.log('    Convergence-sourcing (not holder/mm-like):     ' + eligibleForConvergence);
console.log('    Wallets >= PER_WALLET_MIN_SCORE (15):          ' + perWalletMinGate);

// ── Per-signal emission analysis on recent history ─────────────────────
const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');
const recentCutoff = Date.now() / 1000 - 7 * 86400;
const recent = resolved.filter(s => {
  const ts = s.openedAt ? new Date(s.openedAt).getTime() / 1000 : 0;
  return ts >= recentCutoff;
});

console.log(`\n  Last 7 days of resolved signals: ${recent.length}`);
const byType = {};
for (const s of recent) byType[s.signalType || 'unknown'] = (byType[s.signalType || 'unknown'] || 0) + 1;
for (const [k, v] of Object.entries(byType)) console.log('    ' + k.padEnd(16) + String(v).padStart(4));

// Check active signals too
const active = Object.values(signalsData.active || {});
console.log(`\n  Currently active signals: ${active.length}`);
const byTypeActive = {};
for (const s of active) byTypeActive[s.signalType || 'unknown'] = (byTypeActive[s.signalType || 'unknown'] || 0) + 1;
for (const [k, v] of Object.entries(byTypeActive)) console.log('    ' + k.padEnd(16) + String(v).padStart(4));

// Age of active signals
if (active.length > 0) {
  active.sort((a, b) => (b.openedAt || '').localeCompare(a.openedAt || ''));
  console.log('\n  Newest 5 active signals:');
  active.slice(0, 5).forEach(s => {
    console.log('    type=' + (s.signalType || '?').padEnd(14) + '  opened=' + (s.openedAt || '').slice(0, 19) + '  ' + (s.marketTitle || '').slice(0, 45));
  });
}

console.log('\n═══════════════════════════════════════════════════════════════════\n');
