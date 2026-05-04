#!/usr/bin/env node
/**
 * One-shot repair for handpicked signals whose `signalReturn` was wrongly
 * recorded as 0 on a winning resolution.
 *
 * Cause: when a handpicked signal was emitted on a wallet's HISTORICAL buy
 * whose market had already resolved on Gamma, `openMarketPrice` got set to
 * the market's CURRENT price (1 for a winner, 0 for a loser). The close-time
 * win-return calc — `(1 / openPrice - 1) * 100` — then collapsed to 0% on
 * every winning record.
 *
 * The forward fix (handpickedSignals.js + signals.js + repair.js) is shipped
 * separately; this script repairs records that are already in history.
 *
 * Usage:  node scripts/repair-handpicked-returns.mjs   [--dry]
 */
import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const SIG_PATH = path.join(ROOT, 'data/signals.json.gz');
const DRY = process.argv.includes('--dry');

const buf = fs.readFileSync(SIG_PATH);
const signals = JSON.parse(zlib.gunzipSync(buf).toString());

let scanned = 0, repaired = 0, skipped = 0;
const detail = [];

for (const h of (signals.history || [])) {
  if (h.signalType !== 'handpicked') continue;
  if (h.outcome !== 'win') continue;
  scanned++;

  const ret = typeof h.signalReturn === 'number' ? h.signalReturn : null;
  if (ret > 0) { skipped++; continue; }  // already correct

  const op = h.avgEntryPrice || 0;
  if (op <= 0 || op >= 1) {
    // Can't recompute without a sane entry price.
    detail.push({ signalId: h.signalId, reason: 'bad_avgEntryPrice', op });
    continue;
  }

  const newRet = +((1 / op - 1) * 100).toFixed(2);
  detail.push({
    signalId: h.signalId,
    title: (h.marketTitle || '').slice(0, 60),
    avgEntryPrice: op,
    oldSignalReturn: ret,
    newSignalReturn: newRet,
  });

  if (!DRY) h.signalReturn = newRet;
  repaired++;
}

console.log(`Handpicked WIN history records scanned: ${scanned}`);
console.log(`  already correct (return > 0): ${skipped}`);
console.log(`  repaired:                     ${repaired}`);
console.log(`  un-repairable (bad data):     ${detail.filter(d => d.reason).length}`);

if (detail.length) {
  console.log('\nDetail:');
  for (const d of detail.slice(0, 50)) console.log('  ' + JSON.stringify(d));
  if (detail.length > 50) console.log(`  … +${detail.length - 50} more`);
}

if (!DRY && repaired > 0) {
  const out = zlib.gzipSync(JSON.stringify(signals));
  fs.writeFileSync(SIG_PATH, out);
  console.log(`\nWrote ${SIG_PATH} (${out.length} bytes)`);
} else if (DRY) {
  console.log('\n--dry mode, no file written');
}
