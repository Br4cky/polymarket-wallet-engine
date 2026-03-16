#!/usr/bin/env node

/**
 * Polymarket Signal Engine - Main Scanner
 * Processes positions inline as they're fetched (no giant array — no stack overflow).
 * Matches the proven approach from the browser-based screener.
 */

import {
  GOLDSKY_PNL,
  gqlQuery,
  introspectSchema,
  introspectEntity,
  analyzePositions,
  computeScore,
  resolveMarkets,
  computeConsensus,
  computeWinPatterns,
  computeActivePositions,
  findBiggestWins,
  computeResolvedPositions,
  refreshTrackedWallets,
  loadJSON,
  saveJSON,
  loadGzJSON,
  saveGzJSON,
} from './lib.js';

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.resolve(__dirname, '../data');

// Configuration
const MAX_POSITIONS = 200000;
const MIN_PNL = 1000;
const MIN_POSITIONS_STAGE1 = 20;
const MIN_WIN_RATE = 0.50;  // 50% minimum win rate to qualify
const MAX_WALLETS = 2000;
const PROBATION_SCANS = 3;  // Number of scans before a demoted wallet is removed
const USDC_DIVISOR = 1e6;
const BATCH_SIZE = 1000;
const DELAY = 200;

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function runScan() {
  const scanStart = Date.now();

  console.log('\n===========================================');
  console.log('  Polymarket Signal Engine - Scanner');
  console.log(`  Started: ${new Date().toISOString()}`);
  console.log('===========================================\n');

  // ===== Step 1: Load State =====
  console.log('📋 Loading state...');
  const stateFile = path.join(DATA_DIR, 'state.json');
  let state = loadJSON(stateFile) || { lastId: '', scanCount: 0, lastRun: null, totalScanned: 0 };

  state.scanCount++;
  state.lastRun = new Date().toISOString();

  console.log(`  Scan #${state.scanCount}`);
  console.log(`  Cursor: ${state.lastId || '(fresh start)'}`);
  console.log(`  Previously scanned: ${state.totalScanned.toLocaleString()}\n`);

  // ===== Step 2: Discover Entity =====
  console.log('🔍 Discovering entities...');
  const entities = await introspectSchema(GOLDSKY_PNL);
  console.log(`  Available: ${entities.join(', ')}`);

  // Find the plural collection entity (userPositions) — skip singular lookup entities
  const positionEntities = entities.filter(e =>
    /position/i.test(e) && e.endsWith('s') && !e.startsWith('_')
  );
  console.log(`  Position collections: ${positionEntities.join(', ') || 'none'}`);

  if (positionEntities.length === 0) {
    console.error('  ❌ No position collection entities found. Aborting.');
    process.exit(1);
  }

  const entityName = positionEntities[0]; // e.g., "userPositions"

  // Introspect fields — try singular PascalCase type name first
  let typeName = entityName.charAt(0).toUpperCase() + entityName.slice(1);
  if (typeName.endsWith('s')) typeName = typeName.slice(0, -1);
  let fields = await introspectEntity(GOLDSKY_PNL, typeName);
  if (!fields.length) fields = await introspectEntity(GOLDSKY_PNL, entityName);

  console.log(`  ${entityName} fields: ${fields.join(', ')}`);

  const userField = fields.find(f => /user|account|trader|owner/i.test(f));
  const pnlField = fields.find(f => /pnl|profit|realized/i.test(f));
  const tokenField = fields.find(f => /tokenId|token/i.test(f));
  const boughtField = fields.find(f => /totalBought|total_bought|volume/i.test(f));
  const amountField = fields.find(f => /^amount$/i.test(f));

  console.log(`  Mapped: user=${userField}, pnl=${pnlField}, token=${tokenField}, bought=${boughtField}, amount=${amountField}\n`);

  if (!userField) {
    console.error('  ❌ No user field found. Aborting.');
    process.exit(1);
  }

  // ===== Step 3: Fetch & Process Inline =====
  // Process each position as it comes in — no giant array, no stack overflow.
  // This matches the original browser screener approach.
  console.log('📥 Fetching positions (processing inline)...');

  const wallets = {}; // address → { positions: [...], firstSeen, lastSeen }

  // Load existing wallet data to merge with
  const walletsFile = path.join(DATA_DIR, 'wallets.json.gz');
  const existingData = loadGzJSON(walletsFile);
  if (existingData && existingData.wallets) {
    for (const [addr, w] of Object.entries(existingData.wallets)) {
      wallets[addr] = w;
    }
    console.log(`  Loaded ${Object.keys(wallets).length} existing wallets`);
  }

  // Build scanIndex → timestamp map from trendline for backfilling
  const analyticsFile = path.join(DATA_DIR, 'analytics.json');
  const existingAnalytics = loadJSON(analyticsFile) || {};
  const scanTimestampMap = {};
  for (const entry of (existingAnalytics.trendline || [])) {
    if (entry.scanIndex && entry.timestamp) {
      scanTimestampMap[entry.scanIndex] = entry.timestamp;
    }
  }
  scanTimestampMap[state.scanCount] = state.lastRun; // Current scan
  console.log(`  Timestamp map: ${Object.keys(scanTimestampMap).length} scans mapped`);

  // ===== Step 3a: Refresh Tracked Wallets =====
  const trackedCount = Object.keys(wallets).length;
  if (trackedCount > 0) {
    console.log(`\n🔄 Refreshing ${trackedCount} tracked wallets...`);
    const discoveredFields = { user: userField, pnl: pnlField, token: tokenField, totalBought: boughtField, amount: amountField };
    try {
      const refreshResult = await refreshTrackedWallets(
        GOLDSKY_PNL, entityName, discoveredFields, wallets,
        state.scanCount, state.lastRun, DELAY
      );
      console.log(`  ✓ Refreshed: ${refreshResult.refreshed} wallets, ${refreshResult.newPositions} new positions, ${refreshResult.closures} closures\n`);
    } catch (err) {
      console.error(`  ⚠ Refresh error (non-fatal): ${err.message}\n`);
    }
  }

  // ===== Step 3b: Fetch New Positions =====
  let cursor = state.lastId || '';
  let fetched = 0;
  let useNested = false;

  // Build field strings
  let fieldStr = `id ${userField}`;
  if (pnlField) fieldStr += ` ${pnlField}`;
  if (tokenField) fieldStr += ` ${tokenField}`;
  if (boughtField) fieldStr += ` ${boughtField}`;
  if (amountField) fieldStr += ` ${amountField}`;

  let nestedFieldStr = `id ${userField} { id }`;
  if (pnlField) nestedFieldStr += ` ${pnlField}`;
  if (tokenField) nestedFieldStr += ` ${tokenField}`;
  if (boughtField) nestedFieldStr += ` ${boughtField}`;
  if (amountField) nestedFieldStr += ` ${amountField}`;

  while (fetched < MAX_POSITIONS) {
    let batch;
    try {
      if (useNested) throw new Error('use nested');
      const q = `{ ${entityName}(first: ${BATCH_SIZE}, where: { id_gt: "${cursor}" }) { ${fieldStr} } }`;
      const data = await gqlQuery(GOLDSKY_PNL, q);
      batch = data?.[entityName] || [];
    } catch {
      try {
        const q = `{ ${entityName}(first: ${BATCH_SIZE}, where: { id_gt: "${cursor}" }) { ${nestedFieldStr} } }`;
        const data = await gqlQuery(GOLDSKY_PNL, q);
        batch = data?.[entityName] || [];
        useNested = true;
      } catch (err) {
        console.error(`  Query failed: ${err.message}`);
        break;
      }
    }

    if (!batch || batch.length === 0) {
      console.log(`  Exhausted all data at ${fetched.toLocaleString()} positions`);
      break;
    }

    // Process each position inline
    for (const item of batch) {
      const uid = typeof item[userField] === 'object' ? item[userField]?.id : item[userField];
      if (!uid) continue;

      const pnlVal = pnlField ? parseFloat(item[pnlField] || 0) / USDC_DIVISOR : 0;
      const amountVal = amountField ? parseFloat(item[amountField] || 0) / USDC_DIVISOR : 0;

      const pos = {
        uid: item.id,
        pnl: pnlVal,
        tokenId: tokenField ? (item[tokenField] || null) : null,
        totalBought: boughtField ? parseFloat(item[boughtField] || 0) / USDC_DIVISOR : 0,
        amount: amountVal,
        scanIndex: state.scanCount,
        firstSeenTimestamp: state.lastRun, // stamp new positions with current scan time
      };

      if (!wallets[uid]) {
        wallets[uid] = { positions: [], firstSeen: state.scanCount, lastSeen: state.scanCount };
      }

      // Dedupe by uid — preserve original firstSeenTimestamp and resolvedTimestamp on updates
      const existingIdx = wallets[uid].positions.findIndex(p => p.uid === pos.uid);
      if (existingIdx >= 0) {
        const existing = wallets[uid].positions[existingIdx];
        pos.firstSeenTimestamp = existing.firstSeenTimestamp || scanTimestampMap[existing.scanIndex] || pos.firstSeenTimestamp;
        // Detect closure: was open, now closed → stamp resolvedTimestamp
        if ((existing.amount || 0) > 0.01 && amountVal <= 0.01 && Math.abs(pnlVal) > 0.01) {
          pos.resolvedTimestamp = state.lastRun;
        } else {
          pos.resolvedTimestamp = existing.resolvedTimestamp || null;
        }
        wallets[uid].positions[existingIdx] = pos;
      } else {
        // New position — if already resolved when discovered, stamp it
        if (amountVal <= 0.01 && Math.abs(pnlVal) > 0.01) {
          pos.resolvedTimestamp = state.lastRun;
        }
        wallets[uid].positions.push(pos);
      }
      wallets[uid].lastSeen = state.scanCount;
    }

    fetched += batch.length;
    cursor = batch[batch.length - 1].id;

    if (fetched % 10000 === 0) {
      console.log(`  ${fetched.toLocaleString()} positions, ${Object.keys(wallets).length} wallets...`);
    }

    if (batch.length < BATCH_SIZE) {
      console.log(`  Exhausted all data at ${fetched.toLocaleString()} positions`);
      break;
    }

    await sleep(DELAY);
  }

  state.lastId = cursor;
  state.totalScanned += fetched;
  console.log(`\n  ✓ Fetched: ${fetched.toLocaleString()} positions`);
  console.log(`  ✓ Total wallets in DB: ${Object.keys(wallets).length}`);
  console.log(`  ✓ Cumulative scanned: ${state.totalScanned.toLocaleString()}\n`);

  // ===== Step 4: Score All Wallets =====
  console.log('⭐ Scoring wallets...');
  const scoredWallets = [];

  for (const [address, wallet] of Object.entries(wallets)) {
    const stats = analyzePositions(wallet.positions || []);

    // Compute lastActiveTimestamp from positions or scan map
    let lastActiveTs = null;
    for (const p of (wallet.positions || [])) {
      const ts = p.firstSeenTimestamp || scanTimestampMap[p.scanIndex];
      if (ts && (!lastActiveTs || ts > lastActiveTs)) lastActiveTs = ts;
    }
    if (!lastActiveTs) lastActiveTs = scanTimestampMap[wallet.lastSeen] || state.lastRun;

    wallet.lastActiveTimestamp = lastActiveTs;
    stats.lastActiveTimestamp = lastActiveTs;

    // Compute days since active for recency scoring
    const daysSinceActive = (Date.now() - new Date(lastActiveTs).getTime()) / (1000 * 60 * 60 * 24);
    stats.daysSinceActive = Math.round(daysSinceActive);

    const score = computeScore(stats, lastActiveTs);
    wallet.stats = stats;
    wallet.score = score;

    const passesFilters = stats.totalPnl >= MIN_PNL && stats.resolved >= MIN_POSITIONS_STAGE1 && stats.wr >= MIN_WIN_RATE;

    if (passesFilters) {
      // Active and qualifying — clear any probation
      wallet.status = 'active';
      wallet.probationSince = null;
      scoredWallets.push({ address, score, stats, lastActiveTimestamp: lastActiveTs });
    } else if (wallet.status === 'active') {
      // Previously active wallet now failing filters — put on probation
      wallet.status = 'probation';
      wallet.probationSince = wallet.probationSince || state.scanCount;
      console.log(`  ⚠ ${address.slice(0, 10)}... on probation (WR: ${(stats.wr*100).toFixed(1)}%, PnL: $${stats.totalPnl.toFixed(0)})`);
      // Still include in scored wallets so they appear in the dashboard
      scoredWallets.push({ address, score, stats, lastActiveTimestamp: lastActiveTs });
    } else if (wallet.status === 'probation') {
      const scansSinceProbation = state.scanCount - (wallet.probationSince || state.scanCount);
      if (scansSinceProbation >= PROBATION_SCANS) {
        // Exceeded probation period — will be removed
        wallet.status = 'removed';
        console.log(`  ✖ ${address.slice(0, 10)}... removed after ${PROBATION_SCANS} scans of underperformance`);
      } else {
        // Still in probation window — keep tracking
        console.log(`  ⚠ ${address.slice(0, 10)}... probation scan ${scansSinceProbation + 1}/${PROBATION_SCANS}`);
        scoredWallets.push({ address, score, stats, lastActiveTimestamp: lastActiveTs });
      }
    }
  }

  scoredWallets.sort((a, b) => b.score - a.score);
  const topWallets = scoredWallets.slice(0, MAX_WALLETS);

  console.log(`  ${scoredWallets.length} wallets pass filters or on probation`);
  console.log(`  Keeping top ${topWallets.length}`);

  // Build map of tracked wallets — remove only those marked 'removed'
  const topAddresses = new Set(topWallets.map(w => w.address));
  let removedCount = 0;
  for (const address of Object.keys(wallets)) {
    if (wallets[address].status === 'removed') {
      delete wallets[address];
      removedCount++;
    } else if (!topAddresses.has(address) && !wallets[address].status) {
      // New wallet that didn't make the cut — remove
      delete wallets[address];
    }
  }
  if (removedCount > 0) console.log(`  Removed ${removedCount} wallets after probation`);

  const topScore = topWallets[0]?.score || 0;
  const avgScore = topWallets.length > 0
    ? topWallets.reduce((s, w) => s + w.score, 0) / topWallets.length
    : 0;

  console.log(`  Top score: ${topScore.toFixed(1)}, Avg: ${avgScore.toFixed(1)}\n`);

  // ===== Step 5: Resolve Markets =====
  console.log('🎯 Resolving markets...');
  const tokenIds = new Set();
  for (const w of Object.values(wallets)) {
    for (const p of (w.positions || [])) {
      if (p.tokenId) tokenIds.add(p.tokenId);
    }
  }

  const marketsFile = path.join(DATA_DIR, 'markets.json.gz');
  let marketLookup = loadGzJSON(marketsFile) || {};

  const toResolve = new Set();
  for (const id of tokenIds) {
    // Re-resolve if missing entirely OR missing the new groupId/outcome fields
    if (!marketLookup[id] || !marketLookup[id].groupId) toResolve.add(id);
  }

  console.log(`  Unique tokens: ${tokenIds.size}, need resolution: ${toResolve.size}`);

  if (toResolve.size > 0) {
    try {
      // Pass a save callback so markets.json is saved periodically during resolution
      // This preserves progress if the scan is cancelled or times out
      const resolved = await resolveMarkets(toResolve, (partialLookup) => {
        for (const [id, market] of partialLookup) {
          marketLookup[id] = market;
        }
        saveGzJSON(marketsFile, marketLookup);
        console.log(`    💾 Saved markets.json.gz checkpoint (${Object.keys(marketLookup).length} total)`);
      });
      for (const [id, market] of resolved) {
        marketLookup[id] = market;
      }
      console.log(`  Resolved ${resolved.size} markets`);
    } catch (err) {
      console.error(`  Market resolution error: ${err.message}`);
    }
    // Final save after resolution completes
    saveGzJSON(marketsFile, marketLookup);
  }
  console.log();

  // ===== Step 6: Compute Analytics =====
  console.log('📊 Computing analytics...');
  const walletMap = new Map(Object.entries(wallets));
  const marketMap = new Map(Object.entries(marketLookup));

  let consensus = [], winPatterns = {}, activePositions = [], biggestWins = [], resolvedPositions = {};

  try { consensus = computeConsensus(walletMap, marketMap, 3); } catch (e) { console.error(`  Consensus error: ${e.message}`); }
  try { winPatterns = computeWinPatterns(walletMap, marketMap); } catch (e) { console.error(`  Patterns error: ${e.message}`); }
  try { activePositions = computeActivePositions(walletMap, marketMap); } catch (e) { console.error(`  Active positions error: ${e.message}`); }
  try { biggestWins = findBiggestWins(walletMap, marketMap, 50); } catch (e) { console.error(`  Biggest wins error: ${e.message}`); }
  try { resolvedPositions = computeResolvedPositions(walletMap, marketMap, scanTimestampMap); } catch (e) { console.error(`  Resolved positions error: ${e.message}`); }

  console.log(`  Consensus: ${consensus.length}, Active markets: ${activePositions.length}, Top wins: ${biggestWins.length}, Resolved: ${resolvedPositions.positions?.length || 0}\n`);

  // ===== Step 7: Build Leaderboard =====
  const leaderboard = topWallets.map((w, i) => ({
    rank: i + 1,
    address: w.address,
    score: +w.score.toFixed(2),
    stats: w.stats,
    status: wallets[w.address]?.status || 'active',
    lastActiveTimestamp: w.lastActiveTimestamp || null,
  }));

  // ===== Step 8: Save Everything =====
  console.log('💾 Saving...');

  saveJSON(stateFile, state);
  console.log(`  ✓ state.json (scan #${state.scanCount})`);

  saveGzJSON(walletsFile, { metadata: { totalWallets: topAddresses.size, lastUpdated: new Date().toISOString(), totalScans: state.scanCount }, wallets });
  console.log(`  ✓ wallets.json.gz (${topAddresses.size} wallets)`);

  saveGzJSON(marketsFile, marketLookup);
  console.log(`  ✓ markets.json.gz (${Object.keys(marketLookup).length} markets)`);

  // Analytics
  let analytics = loadJSON(analyticsFile) || { trendline: [] };

  analytics.lastUpdated = new Date().toISOString();
  analytics.scanCount = state.scanCount;
  // Compute aggregate activity stats
  const totalPositions = topWallets.reduce((s, w) => s + (w.stats.resolved || 0), 0);
  const totalOpenPositions = topWallets.reduce((s, w) => s + (w.stats.openCount || 0), 0);
  const avgPositionsPerWeek = topWallets.length > 0
    ? +(topWallets.reduce((s, w) => s + (w.stats.positionsPerWeek || 0), 0) / topWallets.length).toFixed(1) : 0;
  const avgTradingDays = topWallets.length > 0
    ? +(topWallets.reduce((s, w) => s + (w.stats.tradingDays || 0), 0) / topWallets.length).toFixed(1) : 0;
  const mostActiveWallets = topWallets
    .map(w => ({ address: w.address, positionsPerWeek: w.stats.positionsPerWeek || 0, tradingDays: w.stats.tradingDays || 0, score: w.score }))
    .sort((a, b) => b.positionsPerWeek - a.positionsPerWeek)
    .slice(0, 10);

  analytics.summary = {
    totalWallets: topAddresses.size,
    avgScore, topScore,
    totalPnl: topWallets.reduce((s, w) => s + (w.stats.totalPnl || 0), 0),
    avgWinRate: topWallets.length > 0 ? topWallets.reduce((s, w) => s + (w.stats.wr || 0), 0) / topWallets.length : 0,
    totalPositions,
    totalOpenPositions,
    avgPositionsPerWeek,
    avgTradingDays,
    mostActiveWallets,
  };
  analytics.leaderboard = leaderboard;
  analytics.consensus = consensus.slice(0, 50);
  analytics.activePositions = activePositions.slice(0, 50);
  analytics.winPatterns = winPatterns;
  analytics.biggestWins = biggestWins;
  analytics.resolvedPositions = resolvedPositions;

  if (!Array.isArray(analytics.trendline)) analytics.trendline = [];
  analytics.trendline.push({ scanIndex: state.scanCount, timestamp: analytics.lastUpdated, avgScore, topScore, walletCount: topAddresses.size, totalPnl: analytics.summary.totalPnl });
  if (analytics.trendline.length > 100) analytics.trendline = analytics.trendline.slice(-100);

  saveJSON(analyticsFile, analytics);
  console.log(`  ✓ analytics.json`);

  // ===== Done =====
  const duration = Math.round((Date.now() - scanStart) / 1000);
  console.log(`\n===========================================`);
  console.log(`  ✅ Scan #${state.scanCount} Complete (${duration}s)`);
  console.log(`    Positions: ${fetched.toLocaleString()}`);
  console.log(`    Wallets tracked: ${topAddresses.size}`);
  console.log(`    Markets: ${Object.keys(marketLookup).length}`);
  console.log(`    Top: ${topScore.toFixed(1)} / Avg: ${avgScore.toFixed(1)}`);
  console.log(`===========================================\n`);
}

runScan().catch(err => {
  console.error('\n❌ Fatal:', err.message);
  console.error(err.stack);
  process.exit(1);
});
