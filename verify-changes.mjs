#!/usr/bin/env node
/*
 * verify-changes.mjs — sanity-check the recent fixes against current data.
 * Run from repo root: node verify-changes.mjs
 */
import { loadGzJSON } from './scanner/lib.js';

const GREEN = '\x1b[32m', RED = '\x1b[31m', YELLOW = '\x1b[33m', DIM = '\x1b[2m', RESET = '\x1b[0m';
const PASS = `${GREEN}✓${RESET}`, FAIL = `${RED}✗${RESET}`, WARN = `${YELLOW}⚠${RESET}`;

const signals = loadGzJSON('data/signals.json.gz');
const analytics = loadGzJSON('data/analytics.json.gz') || {};
const wallets = loadGzJSON('data/wallets.json.gz');
const pool = wallets?.pool || {};
const active = Object.values(signals.active || {});
const history = signals.history || [];

let fails = 0;

function check(label, ok, detail = '') {
  if (ok) console.log(`${PASS} ${label}${detail ? DIM + ' — ' + detail + RESET : ''}`);
  else { console.log(`${FAIL} ${label}${detail ? ' — ' + detail : ''}`); fails++; }
}
function info(label, detail) { console.log(`  ${DIM}${label}: ${detail}${RESET}`); }

console.log('\n=== 1. Missing-price gate (commit 3b7884d) ===');
const noOpenPrice = active.filter(s => !s.openMarketPrice || s.openMarketPrice === 0);
check('No active signal has openMarketPrice=0',
  noOpenPrice.length === 0,
  `${noOpenPrice.length}/${active.length} active have zero open price`);
if (noOpenPrice.length) {
  for (const s of noOpenPrice.slice(0, 5)) {
    info('  offender', `${s.signalId} scan=${s.openedScan} avgEntry=${s.avgEntryPrice}`);
  }
}

console.log('\n=== 2. Void preservation (commit 90f3041) ===');
const voided = history.filter(h => h.outcome === 'void' || h.status === 'voided');
check('Voided signals remain in history',
  voided.length > 0,
  `${voided.length} voided entries present`);
const resurrected = voided.filter(v => signals.active?.[v.signalId]);
check('No voided signal appears in both active and history',
  resurrected.length === 0,
  resurrected.length ? `${resurrected.length} resurrected` : 'clean');

console.log('\n=== 3. Solo-signal 95¢ filter (commit 60bc0bd) ===');
const soloBreaches = active.filter(s => s.signalType === 'solo' && s.avgEntryPrice > 0.95);
check('No active solo signal has avgEntryPrice > 0.95',
  soloBreaches.length === 0,
  soloBreaches.length ? `${soloBreaches.length} breaches` : 'all solo ≤ 95¢');

console.log('\n=== 4. effectivePnl wiring (commit 6ee9c13) ===');
const poolWithStats = Object.values(pool).filter(w => w.stats);
const withEffective = poolWithStats.filter(w => w.stats.effectivePnl != null);
check('≥1 wallet has stats.effectivePnl populated',
  withEffective.length > 0,
  `${withEffective.length}/${poolWithStats.length} (rest will populate as they re-score)`);

let effectiveCorrect = 0, effectiveWrong = 0;
for (const w of withEffective) {
  const expected = Math.max(w.stats.totalPnl || 0, w.stats.goldskyPnl || w.goldskyPnl || 0);
  if (Math.abs(w.stats.effectivePnl - expected) < 1) effectiveCorrect++;
  else effectiveWrong++;
}
check('effectivePnl = max(totalPnl, goldskyPnl) for all scored wallets',
  effectiveWrong === 0,
  `${effectiveCorrect} correct, ${effectiveWrong} mismatched`);

console.log('\n=== 5. Impact preview: who benefits from effectivePnl ===');
const allWallets = Object.values(pool);
const gains = allWallets.filter(w => {
  const gs = w.goldskyPnl || 0;
  const sp = w.stats?.totalPnl || 0;
  return gs > sp + 1000;
});
info('Wallets where on-chain > sample by >$1k', `${gains.length}/${allWallets.length} (will gain score on re-score)`);
const biggestGain = [...gains].sort((a, b) => (b.goldskyPnl - (b.stats?.totalPnl || 0)) - (a.goldskyPnl - (a.stats?.totalPnl || 0)))[0];
if (biggestGain) {
  info('Biggest benefit',
    `${biggestGain.address.slice(0, 10)} sample=$${(biggestGain.stats?.totalPnl || 0).toFixed(0)} onchain=$${biggestGain.goldskyPnl.toFixed(0)}`);
}

console.log('\n=== 6. Active signal health ===');
const stats = signals.stats || {};
info('Active count', active.length);
info('History count', history.length);
info('Resolved', `${stats.wins || 0}W / ${stats.losses || 0}L = ${stats.winRate || 0}% WR`);
info('Voided (excluded from WR)', voided.length);
const withMktPrice = active.filter(s => s.currentMarketPrice > 0);
info('Active with live price', `${withMktPrice.length}/${active.length}`);

console.log('\n=== 7. Pool health ===');
info('Pool size', Object.keys(pool).length);
const scored = Object.values(pool).filter(w => w.score > 0);
info('With score', scored.length);
const recent = Object.values(pool).filter(w => {
  if (!w.lastScored) return false;
  return (Date.now() - new Date(w.lastScored).getTime()) < 24 * 60 * 60 * 1000;
});
info('Scored in last 24h', recent.length);

console.log('\n' + (fails === 0 ? `${GREEN}All checks passed.${RESET}` : `${RED}${fails} check(s) failed.${RESET}`));
process.exit(fails === 0 ? 0 : 1);
