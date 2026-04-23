// Evict wallets from the pool based on historical signal attribution.
// Catches wallets that pass all quality gates (decidedROI, capital,
// sample size, MM, alpha) but consistently contribute to LOSING signals.
//
// These are the hardest wallets to filter out via gates alone — their
// pool-level metrics look fine, but when they participate in a signal,
// that signal tends to lose. The only way to catch them is by checking
// historical signal outcomes they were part of.
//
// Usage:
//   node scripts/evict-by-attribution.mjs                          # dry-run at defaults
//   node scripts/evict-by-attribution.mjs --min-signals 10         # require ≥10 signals before eviction
//   node scripts/evict-by-attribution.mjs --max-ret -0.03          # max avg return before evicting (-3% default)
//   node scripts/evict-by-attribution.mjs --apply                  # commit evictions

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const args = process.argv.slice(2);
const APPLY = args.includes('--apply');
const VERBOSE = args.includes('--verbose');
const get = (f, d) => { const i = args.indexOf(f); return i >= 0 && i + 1 < args.length ? args[i + 1] : d; };

const MIN_SIGNALS = parseInt(get('--min-signals', '10'), 10);
const MAX_RET = parseFloat(get('--max-ret', '-0.03'));
const MAX_WR = parseFloat(get('--max-wr', '0.50'));   // also require WR ≤ 50% to be extra sure

const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const walletsFile = path.join(ROOT, 'data/wallets.json.gz');
const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(walletsFile)).toString());
const pool = walletsData.pool || walletsData;
const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');

// Build attribution
const attr = new Map();
for (const sig of resolved) {
  const wallets = new Set();
  if (sig.currentWallets && Array.isArray(sig.currentWallets)) sig.currentWallets.forEach(w => w && w.address && wallets.add(w.address.toLowerCase()));
  if (sig.soloWallet) wallets.add(sig.soloWallet.toLowerCase());
  if (sig.wallets && Array.isArray(sig.wallets)) sig.wallets.forEach(w => w && w.address && wallets.add(w.address.toLowerCase()));
  const ret = typeof sig.signalReturn === 'number' ? sig.signalReturn : null;
  for (const addr of wallets) {
    if (!attr.has(addr)) attr.set(addr, { signals: 0, wins: 0, totalRet: 0, retN: 0 });
    const a = attr.get(addr);
    a.signals++;
    if (sig.outcome === 'win') a.wins++;
    if (ret !== null) { a.totalRet += ret; a.retN++; }
  }
}

// Find eviction candidates: in pool, signals ≥ MIN_SIGNALS, avgRet ≤ MAX_RET, WR ≤ MAX_WR
const candidates = [];
for (const [addr, a] of attr) {
  const w = pool[addr];
  if (!w || w.status === 'removed') continue;
  if (a.signals < MIN_SIGNALS) continue;
  const avgRet = a.retN > 0 ? a.totalRet / a.retN : null;
  const wr = a.signals > 0 ? a.wins / a.signals : 0;
  if (avgRet === null || avgRet > MAX_RET / 100) continue;  // MAX_RET is in fraction (-0.03)
  if (wr > MAX_WR) continue;
  candidates.push({ addr, signals: a.signals, wins: a.wins, wr, avgRet, score: w.score });
}

candidates.sort((a, b) => a.avgRet - b.avgRet);

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Evict-by-attribution — kick wallets that drag down portfolio EV');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log(`  Criteria:`);
console.log(`    min signals contributed:  ${MIN_SIGNALS}`);
console.log(`    max avg return:           ${(MAX_RET * 100).toFixed(1)}%`);
console.log(`    max WR:                   ${(MAX_WR * 100).toFixed(0)}%`);
console.log(`  Mode: ${APPLY ? 'APPLY (will rewrite wallets.json.gz)' : 'DRY-RUN'}`);
console.log();
console.log(`  Eviction candidates: ${candidates.length}`);
console.log();

if (candidates.length > 0) {
  console.log(`  ${'Address'.padEnd(14)}  ${'Signals'.padStart(7)}  ${'WR'.padStart(6)}  ${'AvgRet'.padStart(9)}  ${'Score'.padStart(6)}  ${'Impact'.padStart(7)}`);
  console.log('  ' + '─'.repeat(70));
  for (const c of candidates) {
    const impact = (c.signals * c.avgRet).toFixed(0);
    const ret = (c.avgRet >= 0 ? '+' : '') + (c.avgRet * 100).toFixed(1) + '%';
    const wr = (c.wr * 100).toFixed(0) + '%';
    const score = typeof c.score === 'number' ? c.score.toFixed(1) : '—';
    console.log(`  ${c.addr.slice(0, 14)}...  ${String(c.signals).padStart(7)}  ${wr.padStart(6)}  ${ret.padStart(9)}  ${score.padStart(6)}  ${impact.padStart(7)}`);
  }
  console.log();
}

if (APPLY && candidates.length > 0) {
  for (const c of candidates) {
    const w = pool[c.addr];
    w.status = 'removed';
    w.removeReason = 'attribution_drag';
    w.removeDetail = `${c.signals} signals, ${(c.wr * 100).toFixed(0)}% WR, avgRet=${(c.avgRet * 100).toFixed(1)}%, impact=${(c.signals * c.avgRet).toFixed(0)}`;
    w.removedAt = new Date().toISOString();
  }
  walletsData.pool = pool;
  if (!walletsData.metadata) walletsData.metadata = {};
  walletsData.metadata.lastAttributionEvict = new Date().toISOString();
  walletsData.metadata.attributionEvicted = candidates.length;
  fs.writeFileSync(walletsFile, zlib.gzipSync(Buffer.from(JSON.stringify(walletsData))));
  console.log(`  ✓ Applied — evicted ${candidates.length}, rewrote wallets.json.gz\n`);
} else if (APPLY) {
  console.log(`  No candidates — nothing to do.\n`);
} else {
  console.log(`  Dry-run. Pass --apply to commit.\n`);
}
