// Add a wallet to the handpicked pool.
//
// Handpicked wallets are manually-curated traders the user has vetted.
// Every BUY from them becomes a signal (signalType='handpicked'). Bypasses
// all scoring/admission gates from the automated scanner.
//
// Usage:
//   node scripts/add-handpicked.mjs 0xabc123...
//   node scripts/add-handpicked.mjs 0xabc123... "NBA underdog hunter, 100% WR on 6 markets"
//   node scripts/add-handpicked.mjs 0xabc... 0xdef... 0xghi...   (multiple)
//
// Stores at data/handpicked-wallets.json.gz. Will be picked up by the
// scanner's fast loop on the next run.

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';
import { fetchAllActivity, analyzeTradeHistory } from '../scanner/dataApi.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const STORE_PATH = path.join(ROOT, 'data/handpicked-wallets.json.gz');

// Parse args. Anything starting with 0x is an address; remainder is notes.
const args = process.argv.slice(2);
const addresses = args.filter(a => /^0x[a-fA-F0-9]{40}$/.test(a)).map(a => a.toLowerCase());
const notes = args.filter(a => !/^0x[a-fA-F0-9]{40}$/.test(a)).join(' ');

if (addresses.length === 0) {
  console.error('Usage: node scripts/add-handpicked.mjs <address> [<address>...] [notes]');
  console.error('Example: node scripts/add-handpicked.mjs 0xbddf61af533ff524d27154e589d2d7a81510c684 "NBA underdog hunter"');
  process.exit(1);
}

// Load existing store
let store = { wallets: [], metadata: { createdAt: new Date().toISOString() } };
if (fs.existsSync(STORE_PATH)) {
  try {
    store = JSON.parse(zlib.gunzipSync(fs.readFileSync(STORE_PATH)).toString());
  } catch (e) {
    console.error('Failed to read existing store:', e.message);
    process.exit(1);
  }
}

const existingAddrs = new Set(store.wallets.map(w => w.address.toLowerCase()));

console.log('═'.repeat(72));
console.log('  Handpicked-wallet add');
console.log('═'.repeat(72));
console.log(`  Adding: ${addresses.length} wallet(s)`);
console.log(`  Existing in pool: ${store.wallets.length}`);
console.log(`  Notes: ${notes || '(none)'}`);
console.log();

let added = 0, skipped = 0;
for (const addr of addresses) {
  if (existingAddrs.has(addr)) {
    console.log(`  ⏭  ${addr.slice(0, 12)}...  already in handpicked pool`);
    skipped++;
    continue;
  }

  process.stdout.write(`  Fetching ${addr.slice(0, 12)}... `);
  let events;
  try {
    events = await fetchAllActivity(addr);
  } catch (e) {
    console.log(`failed: ${e.message}`);
    continue;
  }

  if (!Array.isArray(events) || events.length === 0) {
    console.log('no activity events');
    // Still add — wallet might be brand new with imminent trades
    store.wallets.push({
      address: addr,
      addedAt: new Date().toISOString(),
      notes: notes || null,
      stats: null,
      lastFetched: new Date().toISOString(),
    });
    added++;
    continue;
  }

  const stats = analyzeTradeHistory(events);
  const summary = stats ? {
    totalTrades: stats.totalTrades,
    resolvedMarkets: stats.resolvedMarkets,
    wins: stats.wins,
    losses: stats.losses,
    winRate: +((stats.winRate || 0).toFixed(3)),
    totalPnl: +((stats.totalPnl || 0).toFixed(2)),
    singleSideROI: stats.singleSideROI != null ? +stats.singleSideROI.toFixed(3) : null,
    singleSideCapital: stats.singleSideCapital != null ? +stats.singleSideCapital.toFixed(2) : null,
    avgEntryPrice: stats.avgEntryPrice != null ? +stats.avgEntryPrice.toFixed(3) : null,
    sellRatio: stats.sellRatio != null ? +stats.sellRatio.toFixed(3) : null,
    categoryAlignment: stats.categoryAlignment,
    topCategories: Object.entries(stats.categories || {})
      .sort((a, b) => (b[1].wins + b[1].losses) - (a[1].wins + a[1].losses))
      .slice(0, 5)
      .map(([cat, r]) => ({ cat, wins: r.wins, losses: r.losses, pnl: +(r.pnl || 0).toFixed(2) })),
  } : null;

  store.wallets.push({
    address: addr,
    addedAt: new Date().toISOString(),
    notes: notes || null,
    stats: summary,
    lastFetched: new Date().toISOString(),
  });

  if (summary) {
    console.log(`done — ${summary.wins}W/${summary.losses}L,  ROI ${summary.singleSideROI != null ? (summary.singleSideROI * 100).toFixed(0) + '%' : '?'},  cap $${summary.singleSideCapital},  align ${summary.categoryAlignment}`);
  } else {
    console.log('added (analyzer returned null)');
  }
  added++;
}

if (added > 0) {
  store.metadata.lastUpdatedAt = new Date().toISOString();
  store.metadata.totalWallets = store.wallets.length;
  fs.writeFileSync(STORE_PATH, zlib.gzipSync(Buffer.from(JSON.stringify(store, null, 2))));
}

console.log();
console.log(`  ✓ Added ${added}, skipped ${skipped} (already in pool)`);
console.log(`  Pool now contains ${store.wallets.length} handpicked wallet(s)`);
console.log(`  Store: ${STORE_PATH}`);
