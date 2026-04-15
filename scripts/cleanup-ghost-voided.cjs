#!/usr/bin/env node
/**
 * One-shot cleanup for "ghost-voided" history entries — signals that were
 * voided first, later had their outcome backfilled to win/loss by
 * repair-outcomes.cjs, but never had their status flipped back from 'voided'
 * to 'closed'. These are silently excluded from WR calcs by the
 * `status === 'voided'` guard in scanner/signals.js.
 *
 * Scans data/signals.json.gz and flips `status` to 'closed' on any entry
 * where outcome is 'win' or 'loss' but status is still 'voided'. Also
 * normalizes the closeReason field (scrubs the legacy misspelled
 * `closedReason`).
 *
 * Idempotent — safe to run multiple times.
 */
const zlib = require('zlib');
const fs = require('fs');
const path = require('path');

const SIGNALS_FILE = process.argv[2] || 'data/signals.json.gz';

function loadGzJSON(p) {
  return JSON.parse(zlib.gunzipSync(fs.readFileSync(p)));
}
function saveGzJSON(p, data) {
  fs.writeFileSync(p, zlib.gzipSync(JSON.stringify(data, null, 2)));
}

function main() {
  if (!fs.existsSync(SIGNALS_FILE)) {
    console.error(`No signals file at ${SIGNALS_FILE}`);
    process.exit(1);
  }

  console.log(`Loading ${SIGNALS_FILE}...`);
  const data = loadGzJSON(SIGNALS_FILE);
  const history = data.history || {};

  let scanned = 0;
  let flipped = 0;
  let legacyFieldScrubbed = 0;
  const examples = [];

  for (const key of Object.keys(history)) {
    const h = history[key];
    scanned++;
    const isWinLoss = h.outcome === 'win' || h.outcome === 'loss';
    const isVoidedStatus = h.status === 'voided';

    if (isWinLoss && isVoidedStatus) {
      h.status = 'closed';
      if (!h.closeReason) h.closeReason = 'resolved';
      flipped++;
      if (examples.length < 10) {
        examples.push({
          signalId: h.signalId,
          outcome: h.outcome,
          resolvedBy: h.resolvedBy,
          signalReturn: h.signalReturn,
        });
      }
    }

    // Scrub legacy misspelled field on any entry
    if (Object.prototype.hasOwnProperty.call(h, 'closedReason')) {
      if (!h.closeReason && h.closedReason) h.closeReason = h.closedReason;
      delete h.closedReason;
      legacyFieldScrubbed++;
    }
  }

  console.log('');
  console.log(`Scanned:              ${scanned}`);
  console.log(`Ghost-voided flipped: ${flipped} (status voided → closed)`);
  console.log(`Legacy field scrubbed: ${legacyFieldScrubbed}`);

  if (flipped === 0 && legacyFieldScrubbed === 0) {
    console.log('\nNothing to do. Exiting without writing.');
    return;
  }

  // Backup before writing
  const backup = SIGNALS_FILE + `.bak.${Date.now()}`;
  fs.copyFileSync(SIGNALS_FILE, backup);
  console.log(`\nBackup saved: ${backup}`);

  saveGzJSON(SIGNALS_FILE, data);
  console.log(`Wrote cleaned signals to ${SIGNALS_FILE}`);

  if (examples.length) {
    console.log('\nExample fixed entries:');
    for (const e of examples) console.log(' ', e);
  }
}

main();
