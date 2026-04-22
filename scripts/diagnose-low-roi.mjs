// Quick diagnostic — why are low-ROI wallets still in the pool?
// Categorises them by the reason the LOW_ROI eviction rule hasn't fired.
import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const file = path.join(ROOT, 'data/wallets.json.gz');

const d = JSON.parse(zlib.gunzipSync(fs.readFileSync(file)).toString());
const pool = d.pool || d;
const active = Object.values(pool).filter(w => w && w.status !== 'removed');
const lowRoi = active.filter(w =>
  w.stats && typeof w.stats.decidedROI === 'number' && w.stats.decidedROI < 0.05
);

console.log('\n═══════════════════════════════════════════════════════════');
console.log('  Low-ROI wallet diagnostic');
console.log('═══════════════════════════════════════════════════════════\n');
console.log(`  Active pool:        ${active.length}`);
console.log(`  Low ROI (< 5%):     ${lowRoi.length}`);
console.log();

const buckets = {
  no_capital: 0,
  no_sample: 0,
  would_evict_but_not_flagged: 0,
  already_flagged: 0,
};

for (const w of lowRoi) {
  const cap = w.stats.decidedCapital || 0;
  const res = (w.stats.decidedWins || 0) + (w.stats.decidedLosses || 0);
  if (cap < 3000) buckets.no_capital++;
  else if (res < 20) buckets.no_sample++;
  else if (w.wouldEvict) buckets.already_flagged++;
  else buckets.would_evict_but_not_flagged++;
}

console.log('  Breakdown of why they\'re still here:');
console.log(`    below capital floor ($3k):          ${buckets.no_capital}`);
console.log(`    below resolved floor (20 markets):   ${buckets.no_sample}`);
console.log(`    would evict but not yet rescored:    ${buckets.would_evict_but_not_flagged}`);
console.log(`    already flagged wouldEvict:           ${buckets.already_flagged}`);
console.log();

const now = Date.now() / 1000;
const unflaggedButQualifies = lowRoi
  .filter(w => !w.wouldEvict
    && (w.stats.decidedCapital || 0) >= 3000
    && ((w.stats.decidedWins || 0) + (w.stats.decidedLosses || 0)) >= 20);

if (unflaggedButQualifies.length > 0) {
  console.log('  Sample of unflagged-but-qualifying wallets (by rescore age):');
  unflaggedButQualifies
    .sort((a, b) => {
      const aT = a.lastScored ? new Date(a.lastScored).getTime() : 0;
      const bT = b.lastScored ? new Date(b.lastScored).getTime() : 0;
      return aT - bT;
    })
    .slice(0, 10)
    .forEach(w => {
      const age = w.lastScored
        ? Math.floor((now - new Date(w.lastScored).getTime() / 1000) / 3600)
        : 'never';
      const roi = (w.stats.decidedROI * 100).toFixed(1);
      const cap = Math.round(w.stats.decidedCapital || 0).toLocaleString();
      console.log(`    ${w.address.slice(0, 14)}...  ROI=${roi}%  cap=$${cap}  last_scored=${age}h ago`);
    });
  console.log();
}

console.log('  Interpretation:');
if (buckets.no_capital > buckets.no_sample + buckets.would_evict_but_not_flagged) {
  console.log('    Most low-ROI wallets are below the $3k capital floor — rule is');
  console.log('    correctly ignoring them as insufficient sample. Either tighten');
  console.log('    LOW_ROI_MIN_CAPITAL or accept them as marginal-but-acceptable.');
} else if (buckets.would_evict_but_not_flagged > 50) {
  console.log('    Many wallets qualify but haven\'t been rescored under the new');
  console.log('    rule yet. Wait 24-48h for rescore rotation to complete, or run');
  console.log('    a one-shot sweep: node scripts/evict-low-roi.mjs --apply');
} else {
  console.log('    System is working — low-ROI wallets are either below thresholds');
  console.log('    or already flagged. No action needed.');
}
console.log();
