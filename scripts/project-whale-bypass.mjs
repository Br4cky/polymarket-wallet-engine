// Quick projection — how many wallets become solo-eligible with the
// whale-bypass added on top of the existing style+score gates?

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

let normalSoloEligible = 0;
let whaleOnly = 0;
let bothCombined = 0;
const whaleList = [];
const whaleByStyle = { sniper: 0, averager: 0, churner: 0, mixed: 0, holder: 0, 'mm-like': 0 };

for (const [addr, w] of Object.entries(pool)) {
  if (!w || typeof w !== 'object' || w.status === 'removed') continue;
  const s = w.stats || {};
  const style = classifyStyle(s);

  const roi = (typeof s.decidedROI === 'number' ? s.decidedROI
    : (typeof s.singleSideROI === 'number' ? s.singleSideROI : 0));
  const cap = (typeof s.decidedCapital === 'number' ? s.decidedCapital
    : (typeof s.singleSideCapital === 'number' ? s.singleSideCapital : 0));

  const score = w.score || 0;
  const wr = s.recentWinRate || s.winRate || 0;
  const resolved = s.resolvedMarkets || 0;
  const passSafety = (s.mmScore || 0) < 3
    && s.alphaVerdict !== 'fails'
    && s.isMeanPickerShape !== true
    && wr >= 0.55
    && resolved >= 50;
  if (!passSafety) continue;

  const passNormal = score >= 25 && soloAllowed.has(style);
  const passWhale = roi >= 0.15
    && cap >= 500000
    && style !== 'holder';

  if (passNormal) normalSoloEligible++;
  if (passWhale && !passNormal) {
    whaleOnly++;
    whaleByStyle[style] = (whaleByStyle[style] || 0) + 1;
    whaleList.push({ addr, style, score, roi, cap, signals: 0 });
  }
  if (passNormal || passWhale) bothCombined++;
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Whale-solo bypass — eligibility projection');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log('  Solo-eligible via normal gates (score ≥25, sniper/averager/churner):  ' + normalSoloEligible);
console.log('  NEW via whale bypass (ROI≥15%, cap≥$500k, not holder):                ' + whaleOnly);
console.log('  TOTAL after whale bypass:                                              ' + bothCombined);
console.log('\n  New whale-bypass unlocks by style:');
for (const [k, v] of Object.entries(whaleByStyle).sort((a, b) => b[1] - a[1])) {
  if (v > 0) console.log('    ' + k.padEnd(12) + v);
}

console.log('\n  Top 15 newly-unlocked whales (were not solo-eligible before):');
whaleList.sort((a, b) => b.cap - a.cap);
console.log('  ' + 'Wallet'.padEnd(14) + 'Style'.padEnd(10) + 'Score'.padStart(6) + '  ROI'.padStart(6) + '    Capital'.padStart(12));
for (const w of whaleList.slice(0, 15)) {
  console.log('  ' + w.addr.slice(0, 12).padEnd(14) + w.style.padEnd(10) + w.score.toFixed(1).padStart(6) +
    ('  ' + (w.roi * 100).toFixed(0) + '%').padStart(6) + ('  $' + (w.cap / 1000).toFixed(0) + 'k').padStart(12));
}
console.log();
