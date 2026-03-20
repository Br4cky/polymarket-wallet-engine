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
const MAX_INACTIVE_DAYS = 90; // Wallets with no activity in 90 days are excluded
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
  const avgPriceField = fields.find(f => /avgPrice|avg_price|averagePrice/i.test(f));

  console.log(`  Mapped: user=${userField}, pnl=${pnlField}, token=${tokenField}, bought=${boughtField}, amount=${amountField}, avgPrice=${avgPriceField}\n`);

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
  const analyticsFile = path.join(DATA_DIR, 'analytics.json.gz');
  const analyticsFilePlain = path.join(DATA_DIR, 'analytics.json');
  const existingAnalytics = loadGzJSON(analyticsFile) || loadJSON(analyticsFilePlain) || {};
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
    const discoveredFields = { user: userField, pnl: pnlField, token: tokenField, totalBought: boughtField, amount: amountField, avgPrice: avgPriceField };
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
  if (avgPriceField) fieldStr += ` ${avgPriceField}`;

  let nestedFieldStr = `id ${userField} { id }`;
  if (pnlField) nestedFieldStr += ` ${pnlField}`;
  if (tokenField) nestedFieldStr += ` ${tokenField}`;
  if (boughtField) nestedFieldStr += ` ${boughtField}`;
  if (amountField) nestedFieldStr += ` ${amountField}`;
  if (avgPriceField) nestedFieldStr += ` ${avgPriceField}`;

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

      // avgPrice from subgraph is in USDC (6 decimals) — divide to get per-share price
      const avgPriceVal = avgPriceField ? parseFloat(item[avgPriceField] || 0) / USDC_DIVISOR : null;

      const pos = {
        uid: item.id,
        pnl: pnlVal,
        tokenId: tokenField ? (item[tokenField] || null) : null,
        totalBought: boughtField ? parseFloat(item[boughtField] || 0) / USDC_DIVISOR : 0,
        amount: amountVal,
        avgPrice: avgPriceVal,
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
        pos.discoveredScan = existing.discoveredScan || existing.scanIndex; // preserve original discovery scan
        // Detect closure: was open, now closed → stamp resolvedTimestamp
        if ((existing.amount || 0) > 0.01 && amountVal <= 0.01 && Math.abs(pnlVal) > 0.01) {
          pos.resolvedTimestamp = state.lastRun;
        } else {
          pos.resolvedTimestamp = existing.resolvedTimestamp || null;
        }
        // Track PnL changes — real activity signal
        if (Math.abs((existing.pnl || 0) - pnlVal) > 0.01) {
          pos.pnlChangedThisScan = true;
        }
        wallets[uid].positions[existingIdx] = pos;
      } else {
        // Brand new position — mark it as discovered this scan
        pos.discoveredScan = state.scanCount;
        pos.isNewThisScan = true;
        // If already resolved when discovered, stamp it but DON'T use current time
        // (we don't know when it actually resolved — leave resolvedTimestamp null)
        if (amountVal <= 0.01 && Math.abs(pnlVal) > 0.01) {
          // Position was already closed when we first saw it — we don't know the real resolve time
          pos.resolvedTimestamp = null;
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
    // Skip tombstoned wallets — they've been removed and shouldn't be re-scored
    if (wallet.status === 'removed') continue;

    const stats = analyzePositions(wallet.positions || []);

    // Compute lastActiveTimestamp from REAL activity signals:
    // 1. The most recent discoveredScan timestamp (when a NEW position appeared)
    // 2. The most recent resolvedTimestamp (when a position actually closed)
    // 3. The most recent PnL/amount change detected during refresh
    // NOT from firstSeenTimestamp which just reflects initial discovery scan time
    let lastActiveTs = wallet.lastActiveTimestamp || null; // preserve existing value

    // Check for positions newly discovered in THIS scan
    let hasNewActivity = false;
    for (const p of (wallet.positions || [])) {
      // New position discovered this scan
      if (p.isNewThisScan) {
        hasNewActivity = true;
        break;
      }
      // Position resolved (closed) this scan
      if (p.resolvedTimestamp === state.lastRun) {
        hasNewActivity = true;
        break;
      }
      // PnL changed this scan (position was updated during refresh)
      if (p.pnlChangedThisScan) {
        hasNewActivity = true;
        break;
      }
    }

    if (hasNewActivity) {
      lastActiveTs = state.lastRun; // Wallet was genuinely active this scan
    }

    // Fallback for wallets that have never had activity tracked
    // (migration: use most recent discoveredScan timestamp if available)
    if (!lastActiveTs) {
      for (const p of (wallet.positions || [])) {
        const ts = p.discoveredScan ? scanTimestampMap[p.discoveredScan] : null;
        if (ts && (!lastActiveTs || ts > lastActiveTs)) lastActiveTs = ts;
      }
    }
    // Ultimate fallback: use the scan when this wallet was first seen
    if (!lastActiveTs) lastActiveTs = scanTimestampMap[wallet.firstSeen] || state.lastRun;

    wallet.lastActiveTimestamp = lastActiveTs;
    stats.lastActiveTimestamp = lastActiveTs;

    const score = computeScore(stats, lastActiveTs);
    wallet.stats = stats;
    wallet.score = score;

    // Filter on REALIZED PnL only — unrealized gains aren't proven performance
    // Also exclude wallets inactive for 90+ days — stale wallets aren't useful for signals
    const daysSinceActive = lastActiveTs ? (Date.now() - new Date(lastActiveTs).getTime()) / (24 * 60 * 60 * 1000) : Infinity;
    const isActive = daysSinceActive <= MAX_INACTIVE_DAYS;
    const passesFilters = isActive && (stats.realizedPnl || stats.totalPnl) >= MIN_PNL && stats.resolved >= MIN_POSITIONS_STAGE1 && stats.wr >= MIN_WIN_RATE;

    if (passesFilters) {
      // Active and qualifying — clear any probation
      wallet.status = 'active';
      wallet.probationSince = null;
      scoredWallets.push({ address, score, stats, lastActiveTimestamp: lastActiveTs });
    } else if (wallet.status === 'active') {
      // Previously active wallet now failing filters — put on probation
      wallet.status = 'probation';
      wallet.probationSince = wallet.probationSince || state.scanCount;
      const reason = !isActive ? `inactive ${Math.floor(daysSinceActive)}d` : `WR: ${(stats.wr*100).toFixed(1)}%, Realized: $${(stats.realizedPnl || stats.totalPnl).toFixed(0)}`;
      console.log(`  ⚠ ${address.slice(0, 10)}... on probation (${reason})`);
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
  const suspWallets = scoredWallets.filter(w => w.stats.suspiciousWinRate);
  if (suspWallets.length > 0) console.log(`  ⚠ ${suspWallets.length} wallets flagged for suspicious 100% win rate`);
  const activeNow = scoredWallets.filter(w => w.stats.newPositionsThisScan > 0);
  console.log(`  📈 ${activeNow.length} wallets had new activity this scan`);

  // Build map of tracked wallets — tombstone removed wallets instead of deleting
  // Keeps a lightweight record so we don't re-discover and re-process them
  const topAddresses = new Set(topWallets.map(w => w.address));
  let removedCount = 0;
  for (const address of Object.keys(wallets)) {
    if (wallets[address].status === 'removed') {
      // Tombstone: keep address and removal metadata, drop positions to save space
      wallets[address] = {
        status: 'removed',
        removedAt: state.scanCount,
        removedTimestamp: new Date().toISOString(),
        previousScore: wallets[address].score || 0,
        positions: [], // clear position data to save space
      };
      removedCount++;
    } else if (!topAddresses.has(address) && !wallets[address].status) {
      // New wallet that didn't make the cut — remove entirely (never qualified)
      delete wallets[address];
    }
  }
  if (removedCount > 0) console.log(`  Removed ${removedCount} wallets after probation (tombstoned)`);

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
    // Re-resolve if missing entirely, missing groupId, or outcome still Unknown
    const m = marketLookup[id];
    if (!m || !m.groupId || m.outcome === 'Unknown') toResolve.add(id);
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
  try { biggestWins = findBiggestWins(walletMap, marketMap, 200); } catch (e) { console.error(`  Biggest wins error: ${e.message}`); }
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

  // Clean up transient per-scan flags before persisting
  // These flags are only meaningful during the current scan for activity detection
  for (const wallet of Object.values(wallets)) {
    for (const pos of (wallet.positions || [])) {
      delete pos.isNewThisScan;
      delete pos.pnlChangedThisScan;
    }
  }

  saveJSON(stateFile, state);
  console.log(`  ✓ state.json (scan #${state.scanCount})`);

  saveGzJSON(walletsFile, { metadata: { totalWallets: topAddresses.size, lastUpdated: new Date().toISOString(), totalScans: state.scanCount }, wallets });
  console.log(`  ✓ wallets.json.gz (${topAddresses.size} wallets)`);

  saveGzJSON(marketsFile, marketLookup);
  console.log(`  ✓ markets.json.gz (${Object.keys(marketLookup).length} markets)`);

  // Analytics
  let analytics = loadGzJSON(analyticsFile) || loadJSON(analyticsFilePlain) || { trendline: [] };

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

  // Count wallets with suspicious win rates and recently active wallets
  const suspiciousCount = topWallets.filter(w => w.stats.suspiciousWinRate).length;
  const activeRecently = topWallets.filter(w => {
    const ts = w.lastActiveTimestamp;
    return ts && (Date.now() - new Date(ts).getTime()) < 7 * 24 * 60 * 60 * 1000;
  }).length;
  const newThisScan = topWallets.filter(w => (w.stats.newPositionsThisScan || 0) > 0).length;

  analytics.summary = {
    totalWallets: topAddresses.size,
    avgScore, topScore,
    totalPnl: topWallets.reduce((s, w) => s + (w.stats.totalPnl || 0), 0),
    realizedPnl: topWallets.reduce((s, w) => s + (w.stats.realizedPnl || 0), 0),
    unrealizedPnl: topWallets.reduce((s, w) => s + (w.stats.unrealizedPnl || 0), 0),
    avgWinRate: topWallets.length > 0 ? topWallets.reduce((s, w) => s + (w.stats.wr || 0), 0) / topWallets.length : 0,
    totalPositions,
    totalOpenPositions,
    avgPositionsPerWeek,
    avgTradingDays,
    mostActiveWallets,
    suspiciousWinRateCount: suspiciousCount,
    activeInLast7Days: activeRecently,
    walletsWithNewActivityThisScan: newThisScan,
  };
  analytics.leaderboard = leaderboard;
  // Store full datasets — frontend can paginate/filter as needed
  // Previous hard caps of 50 were silently discarding data
  analytics.consensus = consensus;
  analytics.activePositions = activePositions;
  analytics.winPatterns = winPatterns;
  analytics.biggestWins = biggestWins;
  analytics.resolvedPositions = resolvedPositions;

  if (!Array.isArray(analytics.trendline)) analytics.trendline = [];
  analytics.trendline.push({ scanIndex: state.scanCount, timestamp: analytics.lastUpdated, avgScore, topScore, walletCount: topAddresses.size, totalPnl: analytics.summary.totalPnl });
  if (analytics.trendline.length > 100) analytics.trendline = analytics.trendline.slice(-100);

  saveGzJSON(analyticsFile, analytics);
  // Remove old uncompressed analytics.json if it exists
  if (fs.existsSync(analyticsFilePlain)) {
    try { fs.unlinkSync(analyticsFilePlain); } catch {}
  }
  console.log(`  ✓ analytics.json.gz`);

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
