#!/usr/bin/env node
/**
 * Retrofit existing batch-ledger CSVs with new recency columns from
 * data/wallets.json.gz. Avoids re-running the expensive Goldsky pass.
 *
 * Replaces columns: daysSincePnlChange, pnlDelta, positionsDelta,
 *                   historyWindowDays, historySnapshots
 * With:             daysSinceLastTrade, daysSinceLastScored,
 *                   recentTradesPerDay, avgHoldTimeHours,
 *                   unredeemedWins, worthlessLosses, status
 *
 * Usage: node scripts/retrofit-recency.cjs in.csv [out.csv]
 */
const fs = require('fs');
const { loadPool, recencyFor } = require('./wallet-recency.cjs');

const IN = process.argv[2];
const OUT = process.argv[3] || IN.replace(/\.csv$/, '.v2.csv');
if (!IN) { console.error('Usage: retrofit-recency.cjs in.csv [out.csv]'); process.exit(1); }

const pool = loadPool();
const raw = fs.readFileSync(IN, 'utf8').trim().split(/\r?\n/);
const header = raw.shift().split(',');

const OLD = ['daysSincePnlChange', 'pnlDelta', 'positionsDelta', 'historyWindowDays', 'historySnapshots'];
const NEW = ['daysSinceLastTrade', 'daysSinceLastScored', 'recentTradesPerDay', 'avgHoldTimeHours', 'unredeemedWins', 'worthlessLosses', 'status'];

// Locate column indexes
const oldStart = header.indexOf(OLD[0]);
if (oldStart === -1) { console.error('CSV has no old recency columns; nothing to retrofit'); process.exit(1); }
const before = header.slice(0, oldStart);
const after = header.slice(oldStart + OLD.length);
const newHeader = [...before, ...NEW, ...after];
const addrIdx = header.indexOf('address');

const lines = [newHeader.join(',')];
let hit = 0, miss = 0;
for (const line of raw) {
  const cols = line.split(',');
  const addr = cols[addrIdx];
  const r = recencyFor(pool, addr);
  if (r) hit++; else miss++;
  const newCols = [
    r?.daysSinceLastTrade ?? '',
    r?.daysSinceLastScored ?? '',
    r?.recentTradesPerDay ?? '',
    r?.avgHoldTimeHours ?? '',
    r?.unredeemedWins ?? '',
    r?.worthlessLosses ?? '',
    r?.status ?? '',
  ];
  const keepBefore = cols.slice(0, oldStart);
  const keepAfter = cols.slice(oldStart + OLD.length);
  lines.push([...keepBefore, ...newCols, ...keepAfter].join(','));
}

fs.writeFileSync(OUT, lines.join('\n') + '\n');
console.log(`Wrote ${OUT}  (${hit} with pool recency, ${miss} missed)`);
