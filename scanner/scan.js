#!/usr/bin/env node

/**
 * Polymarket Signal Engine - Main Scanner
 * Orchestrates the full pipeline: fetch positions, score wallets, resolve markets, compute analytics
 */

import {
  GOLDSKY_PNL,
  GOLDSKY_POSITIONS,
  discoverEntities,
  fetchPositions,
  analyzePositions,
  computeScore,
  resolveMarkets,
  computeConsensus,
  computeWinPatterns,
  computeActivePositions,
  findBiggestWins,
  loadJSON,
  saveJSON,
} from './lib.js';

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Determine data directory from import.meta.url
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.resolve(__dirname, '../data');

// Configuration
const MAX_POSITIONS = 200000; // per run
const MIN_PNL = 1000;
const MIN_POSITIONS = 20;
const MAX_WALLETS = 2000;
const SCORE_THRESHOLD = 30;

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

/**
 * Main scanner entry point
 */
async function runScan() {
  console.log('\n===========================================');
  console.log('  Polymarket Signal Engine - Scanner');
  console.log(`  Started: ${new Date().toISOString()}`);
  console.log('===========================================\n');

  try {
    // ===== Step 1: Load State =====
    console.log('📋 Loading state...');
    const stateFile = path.join(DATA_DIR, 'state.json');
    let state = loadJSON(stateFile);

    if (!state) {
      state = {
        lastId: '',
        scanCount: 0,
        lastRun: null,
        totalScanned: 0,
      };
    }

    const previousScanCount = state.scanCount;
    state.scanCount++;
    state.lastRun = new Date().toISOString();

    console.log(`  Current scan: #${state.scanCount}`);
    console.log(`  Last cursor: ${state.lastId || '(new)'}`);
    console.log(`  Total scanned so far: ${state.totalScanned}\n`);

    // ===== Step 2: Discover Entities =====
    console.log('🔍 Discovering entities on PnL subgraph...');
    let entities;
    try {
      entities = await discoverEntities(GOLDSKY_PNL);
      console.log(`  Found ${entities.length} position-like entities:`);
      for (const e of entities) {
        console.log(`    - ${e.entity}`);
      }
    } catch (err) {
      console.error('  Error discovering entities:', err.message);
      console.log('  Falling back to known entity (Position)');
      entities = [
        {
          entity: 'Position',
          fields: {
            user: 'user',
            pnl: 'unrealizedPnl',
            token: 'tokenId',
            totalBought: 'totalBought',
            amount: 'amount',
          },
          endpoint: GOLDSKY_PNL,
        },
      ];
    }
    console.log();

    // ===== Step 3: Fetch Positions =====
    console.log('📥 Fetching positions from subgraph...');
    const allPositions = [];
    let totalFetched = 0;

    for (const entityConfig of entities) {
      try {
        console.log(
          `  Fetching ${entityConfig.entity} (from ${entityConfig.lastId || 'start'})...`
        );

        const result = await fetchPositions(
          entityConfig.endpoint,
          entityConfig.entity,
          entityConfig.fields,
          state.lastId,
          MAX_POSITIONS - allPositions.length
        );

        console.log(`    Fetched ${result.items.length} positions`);
        allPositions.push(...result.items);

        if (result.lastId) {
          state.lastId = result.lastId;
        }

        totalFetched += result.items.length;

        if (allPositions.length >= MAX_POSITIONS) {
          console.log('  Reached batch size limit');
          break;
        }
      } catch (err) {
        console.error(`  Error fetching from ${entityConfig.entity}:`, err.message);
      }
    }

    state.totalScanned += totalFetched;
    console.log(`  Total positions fetched this run: ${totalFetched}\n`);

    // ===== Step 4: Load Existing Wallets and Merge =====
    console.log('👥 Loading existing wallet data...');
    const walletsFile = path.join(DATA_DIR, 'wallets.json');
    let walletData = loadJSON(walletsFile);

    if (!walletData) {
      walletData = {
        metadata: { totalWallets: 0, lastUpdated: null, totalScans: 0 },
        wallets: {},
      };
    }

    const wallets = walletData.wallets || {};
    let newWallets = 0;
    let mergedPositions = 0;

    console.log(`  Existing wallets: ${Object.keys(wallets).length}`);

    // Merge new positions by wallet
    for (const pos of allPositions) {
      const { user } = pos;
      if (!user) continue;

      if (!wallets[user]) {
        wallets[user] = {
          score: 0,
          stats: {},
          positions: [],
          firstSeen: state.scanCount,
          lastSeen: state.scanCount,
        };
        newWallets++;
      }

      // Add scan index to position
      const posWithIndex = { ...pos, scanIndex: state.scanCount };

      // Merge by uid to avoid duplicates
      const existing = wallets[user].positions.findIndex((p) => p.uid === pos.uid);
      if (existing >= 0) {
        wallets[user].positions[existing] = posWithIndex;
      } else {
        wallets[user].positions.push(posWithIndex);
      }

      wallets[user].lastSeen = state.scanCount;
      mergedPositions++;
    }

    console.log(`  New wallets: ${newWallets}`);
    console.log(`  Merged positions: ${mergedPositions}\n`);

    // ===== Step 5: Score All Wallets =====
    console.log('⭐ Scoring wallets...');
    const scoredWallets = [];

    for (const [address, wallet] of Object.entries(wallets)) {
      const stats = analyzePositions(wallet.positions || []);
      const score = computeScore(stats);

      wallet.stats = stats;
      wallet.score = score;

      if (stats.resolved >= MIN_POSITIONS) {
        scoredWallets.push({ address, score, stats });
      }
    }

    // Sort and keep top wallets
    scoredWallets.sort((a, b) => b.score - a.score);
    const topWallets = scoredWallets.slice(0, MAX_WALLETS);

    console.log(`  Scored ${scoredWallets.length} wallets with ${MIN_POSITIONS}+ positions`);
    console.log(`  Keeping top ${Math.min(topWallets.length, MAX_WALLETS)} by score`);

    // Update wallet data structure
    const walletsByAddress = new Map();
    for (const wallet of topWallets) {
      walletsByAddress.set(wallet.address, wallets[wallet.address]);
    }

    // Remove wallets not in top list
    for (const address of Object.keys(wallets)) {
      if (!walletsByAddress.has(address)) {
        delete wallets[address];
      }
    }

    const topScore = topWallets.length > 0 ? topWallets[0].score : 0;
    const avgScore = topWallets.length > 0
      ? topWallets.reduce((sum, w) => sum + w.score, 0) / topWallets.length
      : 0;

    console.log(`  Top score: ${topScore.toFixed(1)}`);
    console.log(`  Avg score: ${avgScore.toFixed(1)}\n`);

    // ===== Step 6: Collect Token IDs and Resolve Markets =====
    console.log('🎯 Resolving markets...');
    const tokenIds = new Set();

    for (const wallet of walletsByAddress.values()) {
      if (wallet.positions) {
        for (const pos of wallet.positions) {
          tokenIds.add(pos.tokenId);
        }
      }
    }

    console.log(`  Unique tokens: ${tokenIds.size}`);

    // Load existing market cache
    const marketsFile = path.join(DATA_DIR, 'markets.json');
    let marketLookup = loadJSON(marketsFile) || {};

    // Find tokens that need resolution
    const tokensToResolve = new Set();
    for (const tokenId of tokenIds) {
      if (!marketLookup[tokenId]) {
        tokensToResolve.add(tokenId);
      }
    }

    if (tokensToResolve.size > 0) {
      console.log(`  Fetching ${tokensToResolve.size} new markets from Gamma API...`);
      try {
        const newMarkets = await resolveMarkets(tokensToResolve);
        for (const [tokenId, market] of newMarkets) {
          marketLookup[tokenId] = market;
        }
        console.log(`  Resolved ${newMarkets.size} markets`);
      } catch (err) {
        console.error(`  Error resolving markets: ${err.message}`);
      }
    } else {
      console.log('  All tokens already cached');
    }
    console.log();

    // ===== Step 7: Compute Analytics =====
    console.log('📊 Computing analytics...');
    let consensus = [];
    let winPatterns = {};
    let activePositions = [];
    let biggestWins = [];

    const marketLookupMap = new Map(Object.entries(marketLookup));

    try {
      consensus = computeConsensus(walletsByAddress, marketLookupMap, 3);
      console.log(`  Consensus markets: ${consensus.length}`);
    } catch (err) {
      console.error(`  Error computing consensus: ${err.message}`);
    }

    try {
      winPatterns = computeWinPatterns(walletsByAddress, marketLookupMap);
      console.log(
        `  Top winning markets: ${winPatterns.topWinningMarkets?.length || 0}`
      );
    } catch (err) {
      console.error(`  Error computing win patterns: ${err.message}`);
    }

    try {
      activePositions = computeActivePositions(walletsByAddress, marketLookupMap);
      console.log(`  Markets with active positions: ${activePositions.length}`);
    } catch (err) {
      console.error(`  Error computing active positions: ${err.message}`);
    }

    try {
      biggestWins = findBiggestWins(walletsByAddress, marketLookupMap, 50);
      console.log(`  Top wins found: ${biggestWins.length}`);
    } catch (err) {
      console.error(`  Error finding biggest wins: ${err.message}`);
    }
    console.log();

    // ===== Step 8: Build Leaderboard =====
    console.log('🏆 Building leaderboard...');
    const leaderboard = topWallets.slice(0, 50).map((w, i) => ({
      rank: i + 1,
      address: w.address,
      score: w.score,
      stats: w.stats,
    }));

    console.log(`  Leaderboard size: ${leaderboard.length}\n`);

    // ===== Step 9: Save All Files =====
    console.log('💾 Saving files...');

    // Update metadata
    walletData.metadata = {
      totalWallets: walletsByAddress.size,
      lastUpdated: new Date().toISOString(),
      totalScans: state.scanCount,
    };

    // Save state
    saveJSON(stateFile, state);
    console.log(`  ✓ state.json (scanCount=${state.scanCount})`);

    // Save wallets
    walletData.wallets = wallets;
    saveJSON(walletsFile, walletData);
    console.log(`  ✓ wallets.json (${walletsByAddress.size} wallets)`);

    // Save markets
    saveJSON(marketsFile, marketLookup);
    console.log(`  ✓ markets.json (${Object.keys(marketLookup).length} markets)`);

    // Compute trendline entry
    const summaryStats = {
      totalWallets: walletsByAddress.size,
      avgScore: avgScore,
      totalPnl: topWallets.reduce((sum, w) => sum + (w.stats.totalPnl || 0), 0),
      avgWinRate: topWallets.reduce((sum, w) => sum + (w.stats.wr || 0), 0) / Math.max(1, topWallets.length),
      topScore: topScore,
    };

    // Load existing analytics
    const analyticsFile = path.join(DATA_DIR, 'analytics.json');
    let analytics = loadJSON(analyticsFile);

    if (!analytics) {
      analytics = {
        lastUpdated: null,
        scanCount: 0,
        summary: {},
        leaderboard: [],
        consensus: [],
        activePositions: [],
        winPatterns: {},
        biggestWins: [],
        trendline: [],
      };
    }

    // Update analytics
    analytics.lastUpdated = new Date().toISOString();
    analytics.scanCount = state.scanCount;
    analytics.summary = summaryStats;
    analytics.leaderboard = leaderboard;
    analytics.consensus = consensus.slice(0, 50);
    analytics.activePositions = activePositions.slice(0, 50);
    analytics.winPatterns = winPatterns;
    analytics.biggestWins = biggestWins;

    // Update trendline
    if (!Array.isArray(analytics.trendline)) {
      analytics.trendline = [];
    }

    analytics.trendline.push({
      scanIndex: state.scanCount,
      timestamp: analytics.lastUpdated,
      avgScore: avgScore,
      topScore: topScore,
      walletCount: walletsByAddress.size,
      totalPnl: summaryStats.totalPnl,
    });

    // Keep last 100 trendline entries
    if (analytics.trendline.length > 100) {
      analytics.trendline = analytics.trendline.slice(-100);
    }

    saveJSON(analyticsFile, analytics);
    console.log(`  ✓ analytics.json\n`);

    // ===== Completion =====
    console.log('===========================================');
    console.log('  ✅ Scan Complete');
    console.log(`  Duration: ${Math.round((Date.now() - scanStartTime) / 1000)}s`);
    console.log('  Summary:');
    console.log(`    - Positions fetched: ${totalFetched}`);
    console.log(`    - Wallets tracked: ${walletsByAddress.size}`);
    console.log(`    - Markets resolved: ${Object.keys(marketLookup).length}`);
    console.log(`    - Top score: ${topScore.toFixed(1)}`);
    console.log(`    - Avg score: ${avgScore.toFixed(1)}`);
    console.log('===========================================\n');

    process.exit(0);
  } catch (err) {
    console.error('\n❌ Fatal error:', err.message);
    console.error(err.stack);
    process.exit(1);
  }
}

// Record start time and run
const scanStartTime = Date.now();
runScan();
