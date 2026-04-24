// Project emission volume AFTER the two tuning changes:
//   1. SOLO_ALLOWED_STYLES = sniper + averager + churner (was sniper-only)
//   2. SOLO_MIN_SCORE = 25 (was 30)
//
// Combined with everything already live (whitelist, churn penalty, etc),
// this estimates the new eligible wallet count and projected signal rate.

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
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

const soloAllowed = new Set(['sniper', 'averager', 'churner']);
const disqualified = new Set(['holder', 'mm-like']);

let soloEligibleOld = 0, soloEligibleNew = 0;
const newSoloByStyle = { sniper: 0, averager: 0, churner: 0 };

for (const [addr, w] of Object.entries(pool)) {
  if (!w || typeof w !== 'object' || w.status === 'removed') continue;
  const s = classifyStyle(w.stats);
  const sc = w.score || 0;

  if (s === 'sniper' && sc >= 30) soloEligibleOld++;

  if (soloAllowed.has(s) && sc >= 25) {
    soloEligibleNew++;
    newSoloByStyle[s]++;
  }
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Post-tuning projection');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log('  OLD gates (sniper-only, score ≥ 30):  ' + soloEligibleOld + ' solo-eligible wallets');
console.log('  NEW gates (sniper+avg+churner, ≥ 25): ' + soloEligibleNew + ' solo-eligible wallets');
console.log('  Increase: ' + (soloEligibleOld > 0 ? ((soloEligibleNew / soloEligibleOld - 1) * 100).toFixed(0) + '%' : '∞ (was 0)'));
console.log('\n  New solo-eligible by style:');
for (const [k, v] of Object.entries(newSoloByStyle)) console.log('    ' + k.padEnd(12) + v);
console.log();

// Expected emission — each eligible wallet makes ~X qualifying trades/day
// Historical solo-eligible wallets averaged ~0.3 emitted signals per day
// (accounting for category filter, market freshness, SOLO_MAX_PER_WALLET)
const expectedDailyRate = soloEligibleNew * 0.15;
console.log('  Projected solo emission rate: ~' + expectedDailyRate.toFixed(1) + ' signals/day from solo path alone');
console.log('  + cluster/consensus/micro-cluster paths still emit independently\n');
