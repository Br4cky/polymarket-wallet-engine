// Market-participant harvest CLI — Stage 3.
//
// Walks the current pool's top-tier wallets, picks markets they WON (by cost
// conviction), fetches all participants on those markets via /trades, and
// ranks participants by appearance frequency.
//
// Output: data/discovered-candidates.json.gz with the top N ranked addresses.
// The next scan cycle can consume this file as a high-priority discovery
// seed (wallets that play in our alphas' winning markets have higher prior
// probability of sharing edge — 14.5% qualifier rate in handpicked-signals
// vs typically 3-5% for raw top-volume leaderboard scans).
//
// Usage:
//   node scripts/harvest-participants.mjs
//   node scripts/harvest-participants.mjs --top 300 --markets-per-source 50
//   node scripts/harvest-participants.mjs --tier-min 70   # only wallets scoreV2 ≥ 70
//   node scripts/harvest-participants.mjs --dry-run       # no file write
//
// Run outside the sandbox (needs data-api.polymarket.com).

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const { discoverCandidates } = await import(path.join(ROOT, 'scanner/participantHarvest.js'));

// ── Args ────────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const has = (flag) => args.includes(flag);
const get = (flag, def) => {
  const i = args.indexOf(flag);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
};

const TOP_N = parseInt(get('--top', '200'), 10);
const MARKETS_PER_SOURCE = parseInt(get('--markets-per-source', '50'), 10);
const TIER_MIN_SCORE = parseFloat(get('--tier-min', '50'));
const DRY_RUN = has('--dry-run');
const VERBOSE = has('--verbose');

// ── Load pool + markets lookup ──────────────────────────────────────────────
function loadGzip(file) {
  if (!fs.existsSync(file)) throw new Error(`missing: ${file}`);
  return JSON.parse(zlib.gunzipSync(fs.readFileSync(file)).toString());
}

const walletsData = loadGzip(path.join(ROOT, 'data/wallets.json.gz'));
const marketsData = loadGzip(path.join(ROOT, 'data/markets.json.gz'));
const pool = walletsData.pool || walletsData;
const marketLookup = new Map();
for (const [tokenId, m] of Object.entries(marketsData)) {
  marketLookup.set(tokenId, { ...m, tokenId });
}

// ── Select top-tier source wallets ──────────────────────────────────────────
const sourceWallets = Object.values(pool)
  .filter(w => w && w.address && w.status !== 'removed')
  .filter(w => typeof w.scoreV2 === 'number' && w.scoreV2 >= TIER_MIN_SCORE)
  .sort((a, b) => b.scoreV2 - a.scoreV2);

if (sourceWallets.length === 0) {
  console.error(`No wallets with scoreV2 ≥ ${TIER_MIN_SCORE}. Try lowering --tier-min.`);
  process.exit(1);
}

console.log(`\n═══════════════════════════════════════════════════════════════════`);
console.log(`  Market-participant harvest`);
console.log(`═══════════════════════════════════════════════════════════════════`);
console.log(`  Source wallets (scoreV2 ≥ ${TIER_MIN_SCORE}): ${sourceWallets.length}`);
console.log(`  Top source: ${sourceWallets[0].address.slice(0, 12)}… (scoreV2=${sourceWallets[0].scoreV2})`);
console.log(`  Target: top ${TOP_N} candidates`);
console.log(`  Markets per source wallet: ${MARKETS_PER_SOURCE}`);
console.log();

// ── Collect winning markets from source wallets ─────────────────────────────
// We use wallet.stats.topMarkets (already stored in the pool) — these are
// the wallet's biggest-$-PnL markets, sorted by absolute PnL. We filter to
// winners only (pnl > 0) and take the top N by conviction ($ cost).
const allMarkets = [];
let withStats = 0;
for (const w of sourceWallets) {
  const topMarkets = w.stats?.topMarkets;
  if (!Array.isArray(topMarkets)) continue;
  withStats++;
  const winners = topMarkets
    .filter(m => m.outcome === 'win' && m.conditionId && m.buySize > 0)
    .slice(0, MARKETS_PER_SOURCE);
  for (const m of winners) {
    allMarkets.push({
      conditionId: m.conditionId,
      sourceWallet: w.address,
      sourceScore: w.scoreV2,
      cost: (m.avgBuyPrice || 0) * (m.buySize || 0),
    });
  }
}

if (allMarkets.length === 0) {
  console.error(`No winning markets found in source wallets' stats.topMarkets.`);
  console.error(`(${withStats}/${sourceWallets.length} wallets had stats.topMarkets populated)`);
  process.exit(1);
}

// Dedupe by conditionId
const uniqueCids = new Set();
const dedupedMarkets = [];
for (const m of allMarkets) {
  if (uniqueCids.has(m.conditionId)) continue;
  uniqueCids.add(m.conditionId);
  dedupedMarkets.push(m);
}
console.log(`  Winning markets to scan: ${dedupedMarkets.length} unique (from ${allMarkets.length} raw)`);
console.log();

// ── Exclude addresses already in the pool ───────────────────────────────────
const excludeSet = new Set();
for (const w of Object.values(pool)) {
  if (w && w.address) excludeSet.add(w.address.toLowerCase());
}

// ── Run harvest ─────────────────────────────────────────────────────────────
const startTs = Date.now();
const result = await discoverCandidates(dedupedMarkets, {
  topN: TOP_N,
  excludeAddresses: excludeSet,
  tradesPerMarket: 500,
  onProgress: (done, total) => {
    if (VERBOSE || done % 10 === 0 || done === total) {
      const eta = done > 0 ? Math.round((total - done) * (Date.now() - startTs) / done / 1000) : '?';
      console.log(`  [${done}/${total}] markets scanned · ${result?.totalUnique || '?'} unique so far · ETA ${eta}s`);
    }
  },
});

const elapsed = ((Date.now() - startTs) / 1000).toFixed(1);
console.log();
console.log(`  Scanned ${result.marketsScanned} markets (${result.totalTrades.toLocaleString()} trades) in ${elapsed}s`);
console.log(`  Unique participants: ${result.totalUnique.toLocaleString()}`);
console.log(`  Returning top ${result.candidates.length} by frequency + volume`);
console.log();

// ── Emit top preview ────────────────────────────────────────────────────────
console.log(`  ${'rank'.padStart(4)}  ${'address'.padEnd(14)}  ${'score'.padStart(8)}  ${'appearances'.padStart(11)}  ${'volume ($)'.padStart(12)}`);
console.log(`  ${'─'.repeat(58)}`);
for (let i = 0; i < Math.min(10, result.candidates.length); i++) {
  const c = result.candidates[i];
  console.log(`  ${String(i + 1).padStart(4)}  ${c.address.slice(0, 12) + '…'}  ${c.score.toFixed(2).padStart(8)}  ${String(c.appearances).padStart(11)}  ${'$' + c.volume.toLocaleString(undefined, {maximumFractionDigits: 0}).padStart(11)}`);
}
if (result.candidates.length > 10) console.log(`  … and ${result.candidates.length - 10} more\n`);

// ── Write output ────────────────────────────────────────────────────────────
if (!DRY_RUN) {
  const out = {
    generatedAt: new Date().toISOString(),
    config: { topN: TOP_N, marketsPerSource: MARKETS_PER_SOURCE, tierMinScore: TIER_MIN_SCORE },
    stats: {
      sourceWallets: sourceWallets.length,
      marketsScanned: result.marketsScanned,
      totalTrades: result.totalTrades,
      uniqueParticipants: result.totalUnique,
    },
    candidates: result.candidates,
  };
  const outFile = path.join(ROOT, 'data/discovered-candidates.json.gz');
  fs.writeFileSync(outFile, zlib.gzipSync(JSON.stringify(out)));
  console.log(`  ✓ Written: ${outFile}`);
  console.log(`    Next scan can consume this as a high-priority discovery seed.\n`);
} else {
  console.log(`  (dry-run — no file written)\n`);
}
