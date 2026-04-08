#!/usr/bin/env node

/**
 * Polymarket Signal Engine v2 — Main Scanner
 *
 * Architecture:
 *   - SLOW LOOP: Wallet discovery via Goldsky + qualification via Data API trade histories
 *   - FAST LOOP: Hourly trade monitoring via Data API → convergence detection → signals
 *
 * Data sources:
 *   - Goldsky PnL subgraph: wallet discovery (finding addresses with positions)
 *   - Polymarket Data API: trade histories, timestamps, entry/exit detection
 *   - Gamma API: market resolution, current prices, metadata
 */

import {
  GOLDSKY_PNL,
  USDC_DIVISOR,
  gqlQuery,
  introspectSchema,
  introspectEntity,
  discoverEntities,
  resolveMarkets,
  loadJSON,
  saveJSON,
  loadGzJSON,
  saveGzJSON,
  initPaperTrading,
  processPaperTrades,
  PAPER_TRADE_CONFIG,
} from './lib.js';

import {
  fetchAllTrades,
  fetchRecentTrades,
  analyzeTradeHistory,
  computeWalletScore,
} from './dataApi.js';

import {
  SIGNAL_THRESHOLDS,
  detectConvergence,
  processSignals,
} from './signals.js';

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.resolve(__dirname, '../data');

// ============================================================================
// Configuration
// ============================================================================

const CONFIG = {
  // Wallet discovery (slow loop)
  MAX_DISCOVERY_WALLETS: 5000,     // Candidates to discover from Goldsky
  TARGET_POOL_SIZE: 500,           // Top N to keep after scoring
  MIN_PNL_DISCOVERY: 500,          // Minimum PnL to even fetch trade history
  MIN_POSITIONS_DISCOVERY: 10,     // Minimum positions on Goldsky to bother checking
  DISCOVERY_INTERVAL_SCANS: 10,    // Run full discovery every N fast-loop scans
  RESCORE_BATCH_SIZE: 50,          // Wallets to rescore per fast loop (background)

  // Fast loop
  FAST_LOOP_INTERVAL_MS: 60 * 60 * 1000, // 60 minutes
  LOOKBACK_HOURS: 2,                       // Check trades from last 2 hours each loop

  // Goldsky pagination
  BATCH_SIZE: 1000,
  MAX_POSITIONS: 200000,
};

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// ============================================================================
// State Management
// ============================================================================

function loadState() {
  const stateFile = path.join(DATA_DIR, 'state.json');
  const defaults = {
    scanCount: 0,
    lastRun: null,
    lastDiscovery: 0,        // Last scan that ran full discovery
    lastFastLoop: null,       // ISO timestamp of last fast loop
    walletPoolVersion: 0,     // Incremented when pool changes
    cursor: null,
    totalPositionsScanned: 0,
  };
  const existing = loadJSON(stateFile);
  return { ...defaults, ...existing };
}

function saveState(state) {
  saveJSON(path.join(DATA_DIR, 'state.json'), state);
}

// ============================================================================
// Wallet Discovery (Slow Loop)
// ============================================================================

/**
 * Discover wallet addresses from Goldsky and qualify them via Data API.
 * This runs periodically (every DISCOVERY_INTERVAL_SCANS fast loops).
 */
async function discoverWallets(state, existingPool) {
  console.log('\n🔍 WALLET DISCOVERY — Finding and qualifying new wallets...');

  // Step 1: Discover entities and fields dynamically (handles schema differences)
  console.log('  Discovering Goldsky schema...');
  let discovered;
  try {
    discovered = await discoverEntities(GOLDSKY_PNL);
  } catch (err) {
    console.error('  Entity discovery failed:', err.message);
    return existingPool;
  }

  if (!discovered || discovered.length === 0) {
    console.error('  No position entities found in Goldsky schema');
    return existingPool;
  }

  const { entity: entityName, fields } = discovered[0];
  console.log(`  Using entity: ${entityName} (user=${fields.user}, pnl=${fields.pnl}, token=${fields.token})`);

  // Step 2: Fetch position summaries from Goldsky (aggregate per wallet)
  console.log('  Fetching wallet positions from Goldsky...');
  const walletSummaries = new Map(); // address → { totalPnl, positionCount, totalBought }

  let cursor = '';
  let totalFetched = 0;

  while (totalFetched < CONFIG.MAX_POSITIONS) {
    const userField = fields.user;
    const pnlField = fields.pnl;
    const boughtField = fields.totalBought;
    const amountField = fields.amount;

    // Build query with only the fields that exist
    const queryFields = ['id', `${userField} { id }`];
    if (pnlField) queryFields.push(pnlField);
    if (boughtField) queryFields.push(boughtField);
    if (amountField) queryFields.push(amountField);

    const query = `{
      ${entityName}s(
        first: ${CONFIG.BATCH_SIZE}
        orderBy: id
        ${cursor ? `where: { id_gt: "${cursor}" }` : ''}
      ) {
        ${queryFields.join('\n        ')}
      }
    }`;

    let data;
    try {
      data = await gqlQuery(GOLDSKY_PNL, query);
    } catch (err) {
      console.error(`  Goldsky query error: ${err.message}`);
      break;
    }

    const items = data?.[`${entityName}s`] || [];
    if (items.length === 0) break;

    for (const item of items) {
      const address = (typeof item[userField] === 'object' ? item[userField]?.id : item[userField]) || '';
      if (!address || address.length < 10) continue;

      const pnl = pnlField ? parseFloat(item[pnlField] || 0) / USDC_DIVISOR : 0;
      const bought = boughtField ? parseFloat(item[boughtField] || 0) / USDC_DIVISOR : 0;

      if (!walletSummaries.has(address)) {
        walletSummaries.set(address, { totalPnl: 0, positionCount: 0, totalBought: 0 });
      }
      const ws = walletSummaries.get(address);
      ws.totalPnl += pnl;
      ws.positionCount++;
      ws.totalBought += bought;
    }

    cursor = items[items.length - 1].id;
    totalFetched += items.length;

    if (totalFetched % 10000 === 0) {
      console.log(`  Scanned ${totalFetched.toLocaleString()} positions, ${walletSummaries.size.toLocaleString()} unique wallets...`);
    }

    if (items.length < CONFIG.BATCH_SIZE) break;
    await new Promise(r => setTimeout(r, 200));
  }

  console.log(`  Found ${walletSummaries.size.toLocaleString()} wallets from ${totalFetched.toLocaleString()} positions`);

  // Step 3: Filter to candidates worth qualifying
  const candidates = [...walletSummaries.entries()]
    .filter(([, ws]) => ws.totalPnl >= CONFIG.MIN_PNL_DISCOVERY && ws.positionCount >= CONFIG.MIN_POSITIONS_DISCOVERY)
    .sort((a, b) => b[1].totalPnl - a[1].totalPnl)
    .slice(0, CONFIG.MAX_DISCOVERY_WALLETS);

  console.log(`  ${candidates.length} candidates pass PnL/position filters`);

  // Step 4: Fetch trade histories from Data API and score
  console.log('  Fetching trade histories from Data API...');
  const pool = { ...existingPool };
  let qualified = 0;
  let processed = 0;

  for (const [address, summary] of candidates) {
    // Skip if already in pool and recently scored
    if (pool[address] && pool[address].lastScored &&
        (Date.now() - new Date(pool[address].lastScored).getTime()) < 7 * 24 * 60 * 60 * 1000) {
      qualified++;
      processed++;
      if (processed % 100 === 0) console.log(`  Processed ${processed}/${candidates.length} (${qualified} qualified)...`);
      continue;
    }

    try {
      const trades = await fetchAllTrades(address, { maxTrades: 5000 });
      if (!trades || trades.length < 10) {
        processed++;
        continue;
      }

      const stats = analyzeTradeHistory(trades);
      if (!stats) {
        processed++;
        continue;
      }

      const score = computeWalletScore(stats);

      pool[address] = {
        address,
        score,
        stats,
        goldskyPnl: summary.totalPnl,
        goldskyPositions: summary.positionCount,
        lastScored: new Date().toISOString(),
        discoveredScan: state.scanCount,
        totalTrades: trades.length,
        status: 'active',
      };

      qualified++;
    } catch (err) {
      // Skip wallets that error out
    }

    processed++;
    if (processed % 50 === 0) {
      console.log(`  Processed ${processed}/${candidates.length} (${qualified} qualified)...`);
    }
  }

  // Step 5: Rank and trim to top N
  const ranked = Object.entries(pool)
    .filter(([, w]) => w.score > 0 && w.status !== 'removed')
    .sort((a, b) => b[1].score - a[1].score);

  const trimmedPool = {};
  let rank = 0;
  for (const [addr, wallet] of ranked) {
    rank++;
    if (rank <= CONFIG.TARGET_POOL_SIZE) {
      wallet.rank = rank;
      trimmedPool[addr] = wallet;
    }
  }

  console.log(`\n  ✅ Wallet pool: ${Object.keys(trimmedPool).length} wallets (from ${qualified} qualified)`);
  const topWallets = ranked.slice(0, 5);
  for (const [addr, w] of topWallets) {
    console.log(`    #${w.rank} ${addr.slice(0, 12)}... score:${w.score} WR:${((w.stats?.recentWinRate || w.stats?.winRate || 0) * 100).toFixed(0)}% PnL:$${(w.stats?.totalPnl || 0).toFixed(0)}`);
  }

  return trimmedPool;
}

// ============================================================================
// Fast Loop — Hourly Trade Monitoring
// ============================================================================

/**
 * The fast loop: check recent trades for all tracked wallets,
 * detect convergence, generate/update signals, process paper trades.
 */
async function fastLoop(state, walletPool, marketLookup) {
  const scanIndex = state.scanCount;
  console.log(`\n⚡ FAST LOOP #${scanIndex} — ${new Date().toISOString()}`);
  console.log(`  Tracked wallets: ${Object.keys(walletPool).length}`);

  // Step 1: Fetch recent trades for all tracked wallets
  const lookbackTs = Math.floor(Date.now() / 1000) - (CONFIG.LOOKBACK_HOURS * 3600);
  const walletAddresses = Object.keys(walletPool);

  console.log(`  Fetching trades since ${new Date(lookbackTs * 1000).toISOString()}...`);
  const recentTrades = await fetchRecentTrades(walletAddresses, lookbackTs, (done, total) => {
    console.log(`  Checked ${done}/${total} wallets...`);
  });

  let totalNewTrades = 0;
  for (const trades of recentTrades.values()) {
    totalNewTrades += trades.length;
  }
  console.log(`  Found ${totalNewTrades} new trades from ${recentTrades.size} active wallets`);

  if (totalNewTrades === 0) {
    console.log('  No new trades — skipping signal processing');
    return;
  }

  // Step 2: Resolve market data for tokens in recent trades
  const tokensToResolve = new Set();
  for (const trades of recentTrades.values()) {
    for (const trade of trades) {
      if (trade.asset && !marketLookup.has(trade.asset)) {
        tokensToResolve.add(trade.asset);
      }
    }
  }

  // Also refresh active signal tokens
  const signalsFile = path.join(DATA_DIR, 'signals.json.gz');
  let existingSignals = loadGzJSON(signalsFile) || { active: {}, history: [], stats: {} };

  for (const signal of Object.values(existingSignals.active || {})) {
    if (signal.tokenId) tokensToResolve.add(signal.tokenId);
  }

  if (tokensToResolve.size > 0) {
    console.log(`  Resolving ${tokensToResolve.size} market tokens via Gamma...`);
    try {
      const resolved = await resolveMarkets(tokensToResolve);
      for (const [id, market] of resolved) {
        marketLookup.set(id, market);
      }
    } catch (err) {
      console.error(`  Gamma resolution error: ${err.message}`);
    }
  }

  // Step 3: Detect convergence — where are multiple wallets buying?
  const walletPoolMap = new Map(Object.entries(walletPool));
  const candidates = detectConvergence(recentTrades, walletPoolMap, marketLookup);
  console.log(`  Convergence candidates: ${candidates.length}`);

  if (candidates.length > 0) {
    const top3 = candidates.slice(0, 3);
    for (const c of top3) {
      console.log(`    ${c.walletCount} wallets → "${c.title}" @ ${(c.avgEntryPrice * 100).toFixed(0)}¢ ($${c.totalBuySize.toFixed(0)})`);
    }
  }

  // Step 4: Generate/update signals
  const updatedSignals = processSignals(
    candidates, existingSignals, recentTrades, walletPoolMap, marketLookup, scanIndex
  );

  console.log(`  Signals: ${updatedSignals.stats.opened} opened, ${updatedSignals.stats.updated} updated, ${updatedSignals.stats.closed} closed`);
  console.log(`  Active: ${updatedSignals.stats.activeCount} | History: ${updatedSignals.stats.historyCount} | WR: ${updatedSignals.stats.winRate}%`);

  // Step 5: Paper trading
  const paperFile = path.join(DATA_DIR, 'paper-trades.json.gz');
  let paperState = loadGzJSON(paperFile) || initPaperTrading();
  paperState = processPaperTrades(updatedSignals, paperState, scanIndex);

  // Step 6: Save everything
  saveGzJSON(signalsFile, updatedSignals);
  saveGzJSON(paperFile, paperState);
  saveGzJSON(path.join(DATA_DIR, 'markets.json.gz'), Object.fromEntries(marketLookup));

  // Step 7: Update analytics
  const analyticsFile = path.join(DATA_DIR, 'analytics.json.gz');
  let analytics = loadGzJSON(analyticsFile) || { trendline: [], walletStats: {} };

  analytics.trendline.push({
    scanIndex,
    timestamp: new Date().toISOString(),
    trackedWallets: Object.keys(walletPool).length,
    activeWalletsThisScan: recentTrades.size,
    newTrades: totalNewTrades,
    convergenceCandidates: candidates.length,
    activeSignals: updatedSignals.stats.activeCount,
    signalsOpened: updatedSignals.stats.opened,
    signalsClosed: updatedSignals.stats.closed,
    totalHistory: updatedSignals.stats.historyCount,
    winRate: updatedSignals.stats.winRate,
  });

  // Keep trendline at a manageable size
  if (analytics.trendline.length > 2000) {
    analytics.trendline.splice(0, analytics.trendline.length - 2000);
  }

  saveGzJSON(analyticsFile, analytics);

  console.log(`  ✅ Fast loop complete`);
}

// ============================================================================
// Main Entry Point
// ============================================================================

async function run() {
  console.log('===========================================');
  console.log('  Polymarket Signal Engine v2');
  console.log(`  Started: ${new Date().toISOString()}`);
  console.log('===========================================');

  const state = loadState();
  state.scanCount++;
  state.lastRun = new Date().toISOString();

  // Load existing data
  const walletFile = path.join(DATA_DIR, 'wallets.json.gz');
  let walletPool = {};
  const existingWallets = loadGzJSON(walletFile);
  if (existingWallets && existingWallets.pool) {
    walletPool = existingWallets.pool;
  } else if (existingWallets && typeof existingWallets === 'object') {
    // Migration: old format might be different
    walletPool = {};
  }

  const marketsFile = path.join(DATA_DIR, 'markets.json.gz');
  const existingMarkets = loadGzJSON(marketsFile) || {};
  const marketLookup = new Map(Object.entries(existingMarkets));

  console.log(`\n📋 State: Scan #${state.scanCount}`);
  console.log(`  Wallet pool: ${Object.keys(walletPool).length}`);
  console.log(`  Known markets: ${marketLookup.size}`);
  console.log(`  Last discovery: scan #${state.lastDiscovery}`);

  // Decide whether to run discovery (slow loop)
  const needsDiscovery = Object.keys(walletPool).length === 0 ||
    (state.scanCount - state.lastDiscovery) >= CONFIG.DISCOVERY_INTERVAL_SCANS;

  if (needsDiscovery) {
    walletPool = await discoverWallets(state, walletPool);
    state.lastDiscovery = state.scanCount;

    // Save wallet pool
    saveGzJSON(walletFile, {
      metadata: {
        totalWallets: Object.keys(walletPool).length,
        lastUpdated: new Date().toISOString(),
        poolVersion: ++state.walletPoolVersion,
      },
      pool: walletPool,
    });
  }

  // Run the fast loop
  if (Object.keys(walletPool).length > 0) {
    await fastLoop(state, walletPool, marketLookup);
  } else {
    console.log('\n⚠ No wallets in pool — run discovery first');
  }

  // Save state
  state.lastFastLoop = new Date().toISOString();
  saveState(state);

  console.log(`\n🏁 Scan #${state.scanCount} complete at ${new Date().toISOString()}`);
}

// Run
run().catch(err => {
  console.error('\n❌ Fatal:', err.message);
  console.error(err.stack);
  process.exit(1);
});
