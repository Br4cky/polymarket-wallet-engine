// Per-wallet signal attribution: which wallets actually produce profitable
// signals, vs which pass all quality gates but consistently contribute to
// losing signals.
//
// For each wallet that's been involved in resolved signals, computes:
//   - Signals contributed to
//   - Win rate on those signals
//   - Avg return on those signals
//   - Current pool score
//   - Attribution EV (signals × avg return)
//
// Usage:
//   node scripts/wallet-attribution.mjs               # all wallets
//   node scripts/wallet-attribution.mjs --top 50      # top 50 by impact
//   node scripts/wallet-attribution.mjs --bottom 30   # worst 30
//   node scripts/wallet-attribution.mjs --min 5       # min signals contributed

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const args = process.argv.slice(2);
const get = (f, d) => { const i = args.indexOf(f); return i >= 0 && i + 1 < args.length ? args[i + 1] : d; };
const TOP = parseInt(get('--top', '0'), 10);
const BOTTOM = parseInt(get('--bottom', '0'), 10);
const MIN_SIGNALS = parseInt(get('--min', '5'), 10);

const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const pool = walletsData.pool || walletsData;
const history = signalsData.history || [];
const resolved = history.filter(s => s.outcome === 'win' || s.outcome === 'loss');

// Build per-wallet attribution
const attribution = new Map();
const noteWallet = (addr) => {
  if (!attribution.has(addr)) {
    attribution.set(addr, { addr, signals: 0, wins: 0, losses: 0, totalReturn: 0, returnCount: 0 });
  }
  return attribution.get(addr);
};

for (const sig of resolved) {
  const wallets = new Set();

  // Extract wallet addresses from various signal shapes
  if (sig.currentWallets && Array.isArray(sig.currentWallets)) {
    sig.currentWallets.forEach(w => w && w.address && wallets.add(w.address.toLowerCase()));
  }
  if (sig.soloWallet) wallets.add(sig.soloWallet.toLowerCase());
  if (sig.wallets && Array.isArray(sig.wallets)) {
    sig.wallets.forEach(w => w && w.address && wallets.add(w.address.toLowerCase()));
  }

  const ret = typeof sig.signalReturn === 'number' ? sig.signalReturn : null;

  for (const addr of wallets) {
    const a = noteWallet(addr);
    a.signals++;
    if (sig.outcome === 'win') a.wins++;
    else a.losses++;
    if (ret !== null) {
      a.totalReturn += ret;
      a.returnCount++;
    }
  }
}

// Enrich with pool data
for (const [addr, a] of attribution) {
  const w = pool[addr];
  a.inPool = w && w.status !== 'removed';
  a.currentScore = w?.score ?? null;
  a.decidedROI = w?.stats?.decidedROI ?? null;
  a.decidedCapital = w?.stats?.decidedCapital ?? null;
  a.resolvedMarkets = (w?.stats?.decidedWins || 0) + (w?.stats?.decidedLosses || 0) || w?.stats?.resolvedMarkets || null;
  a.wr = a.signals > 0 ? a.wins / a.signals : 0;
  a.avgReturn = a.returnCount > 0 ? a.totalReturn / a.returnCount : null;
  a.impact = a.signals * (a.avgReturn || 0);
}

// Filter + sort
let rows = Array.from(attribution.values()).filter(a => a.signals >= MIN_SIGNALS);
rows.sort((a, b) => (b.impact || 0) - (a.impact || 0));

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log(`  Wallet signal attribution — ${rows.length} wallets with ≥${MIN_SIGNALS} signals`);
console.log('═══════════════════════════════════════════════════════════════════\n');

// Summary stats
const inPool = rows.filter(a => a.inPool).length;
const profitable = rows.filter(a => (a.avgReturn || 0) > 0).length;
const losing = rows.filter(a => (a.avgReturn || 0) < 0).length;
const evicted = rows.filter(a => !a.inPool).length;
console.log(`  In pool:       ${inPool}   Evicted:    ${evicted}`);
console.log(`  Profitable:    ${profitable}   Losing:   ${losing}`);

const inPoolLosing = rows.filter(a => a.inPool && (a.avgReturn || 0) < -3).length;
console.log(`  In pool AND losing > -3% avg: ${inPoolLosing}  ← eviction candidates`);
console.log();

function printRow(a, i) {
  const wr = (a.wr * 100).toFixed(0) + '%';
  const ret = a.avgReturn !== null ? (a.avgReturn >= 0 ? '+' : '') + a.avgReturn.toFixed(1) + '%' : '—';
  const score = typeof a.currentScore === 'number' ? a.currentScore.toFixed(1) : '—';
  const poolTag = a.inPool ? '●' : '○';
  const impact = a.impact >= 0 ? '+' + a.impact.toFixed(0) : a.impact.toFixed(0);
  console.log(`  ${String(i + 1).padStart(4)}  ${poolTag} ${a.addr.slice(0, 14)}...  sigs=${String(a.signals).padStart(3)}  ${wr.padStart(4)} WR  ret=${ret.padStart(8)}  impact=${impact.padStart(6)}  score=${score.padStart(5)}`);
}

if (TOP > 0) {
  console.log(`  TOP ${TOP} BY IMPACT (signals × avg return)`);
  console.log('  ' + '─'.repeat(75));
  rows.slice(0, TOP).forEach((a, i) => printRow(a, i));
  console.log();
}

if (BOTTOM > 0) {
  console.log(`  BOTTOM ${BOTTOM} BY IMPACT (biggest drag on portfolio)`);
  console.log('  ' + '─'.repeat(75));
  rows.slice(-BOTTOM).reverse().forEach((a, i) => printRow(a, i));
  console.log();
}

if (TOP === 0 && BOTTOM === 0) {
  console.log(`  TOP 20 BY IMPACT`);
  console.log('  ' + '─'.repeat(75));
  rows.slice(0, 20).forEach((a, i) => printRow(a, i));
  console.log();
  console.log(`  BOTTOM 20 BY IMPACT`);
  console.log('  ' + '─'.repeat(75));
  rows.slice(-20).reverse().forEach((a, i) => printRow(a, i));
  console.log();
  console.log('  Legend: ● = still in pool,  ○ = evicted');
  console.log('  Run with --top N or --bottom N to see more.');
  console.log();
}
