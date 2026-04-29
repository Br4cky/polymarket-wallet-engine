// Evict the wallets identified by audit-wallet-activity.mjs as
// high-volume + negative attribution OR high-volume + MM pattern.
//
// Usage:
//   node scripts/evict-from-audit.mjs              # dry-run, report only
//   node scripts/evict-from-audit.mjs --apply      # commit evictions

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const APPLY = process.argv.includes('--apply');

// Identified by full audit (2026-04-28)
const EVICT_LIST = [
  // 3 by attribution drag — high volume + sigs lose money on average
  { addr: '0x5e3040eb55', reason: 'attribution_drag_audit', detail: '12.1 pos/day, 14 sigs @ -29%, attrMult 0.41, mm-like' },
  { addr: '0x6407a638ff', reason: 'attribution_drag_audit', detail: '9.6 pos/day, 6 sigs @ -66%, mm-like (attribution sample below 10-floor)' },
  { addr: '0x6394194aac', reason: 'attribution_drag_audit', detail: '5.1 pos/day, 17 sigs @ -35%, attrMult 0.30' },
  // 10 by MM-pattern — high volume + structural MM indicators
  { addr: '0x1412fce138', reason: 'mm_pattern_audit', detail: '17.4 pos/day, holder, mmScore 2, sellRatio 0.04' },
  { addr: '0x374a925188', reason: 'mm_pattern_audit', detail: '15.3 pos/day, mm-like, mmScore 3' },
  { addr: '0x1e2a00a9bf', reason: 'mm_pattern_audit', detail: '14.6 pos/day, mixed, mmScore 1, dualSide 23%' },
  { addr: '0x5eacc61920', reason: 'mm_pattern_audit', detail: '14.4 pos/day, mixed, dualSide 25%' },
  { addr: '0x5ed388787e', reason: 'mm_pattern_audit', detail: '13.4 pos/day, averager, mmScore 2' },
  { addr: '0x1c657d3750', reason: 'mm_pattern_audit', detail: '13.2 pos/day, mixed, dualSide 21%' },
  { addr: '0x63b0c4136b', reason: 'mm_pattern_audit', detail: '13.1 pos/day, mixed, mmScore 2' },
  { addr: '0x3711f5e678', reason: 'mm_pattern_audit', detail: '12.7 pos/day, mixed, dualSide 22%' },
  { addr: '0x335006f0b1', reason: 'mm_pattern_audit', detail: '12.2 pos/day, mm-like, mmScore 2, dualSide 40%' },
];

const walletsFile = path.join(ROOT, 'data/wallets.json.gz');
const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(walletsFile)).toString());
const pool = walletsData.pool || walletsData;

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Evict-from-audit — bulk eviction of audit-flagged wallets');
console.log('═══════════════════════════════════════════════════════════════════');
console.log('  Mode: ' + (APPLY ? 'APPLY (will rewrite wallets.json.gz)' : 'DRY-RUN'));
console.log('  Candidates: ' + EVICT_LIST.length);
console.log();

let toEvict = 0, alreadyEvicted = 0, notFound = 0;
const evictions = [];

for (const candidate of EVICT_LIST) {
  const fullAddr = Object.keys(pool).find(k => k.toLowerCase().startsWith(candidate.addr.toLowerCase()));
  if (!fullAddr) {
    notFound++;
    console.log('  ✗ ' + candidate.addr + '... NOT FOUND in pool');
    continue;
  }
  const w = pool[fullAddr];
  if (w.status === 'removed') {
    alreadyEvicted++;
    console.log('  - ' + candidate.addr + '... already removed (' + (w.removeReason || 'unknown') + ')');
    continue;
  }
  toEvict++;
  evictions.push({ addr: fullAddr, reason: candidate.reason, detail: candidate.detail, score: w.score });
  console.log('  ✓ ' + candidate.addr + '... (' + candidate.reason + ') score=' + (w.score || 0).toFixed(1));
  console.log('    ' + candidate.detail);
}

console.log();
console.log('  Summary:');
console.log('    Active wallets to evict: ' + toEvict);
console.log('    Already removed:         ' + alreadyEvicted);
console.log('    Not found in pool:       ' + notFound);

if (APPLY && evictions.length > 0) {
  for (const e of evictions) {
    pool[e.addr].status = 'removed';
    pool[e.addr].removeReason = e.reason;
    pool[e.addr].removeDetail = e.detail;
    pool[e.addr].removedAt = new Date().toISOString();
  }
  walletsData.pool = pool;
  if (!walletsData.metadata) walletsData.metadata = {};
  walletsData.metadata.lastAuditEviction = new Date().toISOString();
  walletsData.metadata.auditEvicted = (walletsData.metadata.auditEvicted || 0) + evictions.length;
  fs.writeFileSync(walletsFile, zlib.gzipSync(Buffer.from(JSON.stringify(walletsData))));
  console.log('\n  ✓ Applied — evicted ' + evictions.length + ' wallets, rewrote wallets.json.gz');
} else if (APPLY) {
  console.log('\n  Nothing to evict.');
} else {
  console.log('\n  Dry-run. Pass --apply to commit.');
}
console.log();
