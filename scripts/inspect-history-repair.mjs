// Inspect the "30 history needing repair" — signals that closed without
// a win/loss outcome and re-trigger the repair pass every scan.

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const history = signalsData.history || [];

const stuck = history.filter(s => s.outcome !== 'win' && s.outcome !== 'loss');

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  History entries needing repair (no win/loss outcome)');
console.log('═══════════════════════════════════════════════════════════════════');
console.log(`  Total: ${stuck.length}`);
console.log();

// Breakdown by closeReason
const byReason = {};
for (const s of stuck) {
  const r = s.closeReason || 'no_reason';
  byReason[r] = (byReason[r] || 0) + 1;
}
console.log('  ── By closeReason ──');
for (const [r, n] of Object.entries(byReason).sort((a, b) => b[1] - a[1])) {
  console.log('  ' + r.padEnd(28) + String(n).padStart(4));
}

// Breakdown by outcome value
const byOutcome = {};
for (const s of stuck) {
  const o = s.outcome === null ? 'null' : (s.outcome === undefined ? 'undefined' : String(s.outcome));
  byOutcome[o] = (byOutcome[o] || 0) + 1;
}
console.log('\n  ── By outcome value ──');
for (const [o, n] of Object.entries(byOutcome).sort((a, b) => b[1] - a[1])) {
  console.log('  ' + o.padEnd(28) + String(n).padStart(4));
}

// Age distribution
const now = Date.now();
console.log('\n  ── Age distribution (since closedAt) ──');
const ageBuckets = [
  ['< 1 day', 0, 86400000],
  ['1-3 days', 86400000, 3 * 86400000],
  ['3-7 days', 3 * 86400000, 7 * 86400000],
  ['1-2 weeks', 7 * 86400000, 14 * 86400000],
  ['> 2 weeks', 14 * 86400000, Infinity],
];
for (const [label, lo, hi] of ageBuckets) {
  const n = stuck.filter(s => {
    if (!s.closedAt) return false;
    const age = now - new Date(s.closedAt).getTime();
    return age >= lo && age < hi;
  }).length;
  console.log('  ' + label.padEnd(14) + String(n).padStart(4));
}

// Detail listing — sorted by age
console.log('\n  ── Detail (oldest first) ──');
stuck.sort((a, b) => (a.closedAt || '').localeCompare(b.closedAt || ''));
console.log('  ' + 'Type'.padEnd(15) + ' ' + 'CloseReason'.padEnd(22) + ' ' + 'ClosedAt'.padEnd(20) + ' ' + 'Title');
for (const s of stuck.slice(0, 30)) {
  console.log('  ' + (s.signalType || '?').padEnd(15) +
    ' ' + (s.closeReason || '—').padEnd(22) +
    ' ' + (s.closedAt || '').slice(0, 19).padEnd(20) +
    ' ' + (s.marketTitle || '').slice(0, 50));
}

console.log('\n═══════════════════════════════════════════════════════════════════\n');
