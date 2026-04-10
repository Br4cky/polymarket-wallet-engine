/**
 * Signal Engine v2 — Trade-Convergence-Based Signal Generation
 *
 * Instead of detecting position overlap from snapshots, this engine detects
 * when multiple tracked wallets are actively BUYING into the same market
 * within a recent time window. This ensures signals represent live
 * intelligence, not stale holdings.
 *
 * Signal lifecycle:
 *   1. DETECT — fast loop finds multiple wallets recently bought same market
 *   2. OPEN — convergence crosses thresholds → signal created with real timestamps
 *   3. ACTIVE — updated each scan with new trade data, exit monitoring
 *   4. CLOSED — market resolves, wallets exit, or signal expires
 */

import { resolveMarkets, matchesWinningOutcome, loadGzJSON, saveGzJSON } from './lib.js';

// ============================================================================
// Signal Thresholds v2
// ============================================================================

const SIGNAL_THRESHOLDS = {
  // Convergence window — how recent trades must be to count as "active convergence"
  CONVERGENCE_WINDOW_HOURS: 48,     // Trades within last 48 hours count

  // Consensus signals — multiple wallets buying into same market recently
  CONSENSUS_MIN_WALLETS: 8,         // 8+ wallets (lower than v1's 12 because timing-confirmed)
  CONSENSUS_MIN_AVG_SCORE: 60,
  CONSENSUS_MIN_TOTAL_SIZE: 1000,   // $1000+ total buy size across wallets

  // Cluster signals — small group of strong wallets
  CLUSTER_MIN_WALLETS: 3,
  CLUSTER_MAX_WALLETS: 7,
  CLUSTER_MIN_AVG_SCORE: 75,
  CLUSTER_MIN_TOTAL_SIZE: 500,

  // Solo signals — single elite wallet, significant new buy
  SOLO_MIN_SCORE: 80,
  SOLO_MIN_WIN_RATE: 0.80,
  SOLO_MIN_RESOLVED: 50,
  SOLO_MIN_BUY_SIZE: 500,           // $500+ buy in a single market
  SOLO_MAX_PER_WALLET: 3,

  // Lifecycle
  STALE_HOURS: 96,                  // Close signal if no new buys for 96 hours
  MAX_SIGNAL_LIFETIME_HOURS: 600,   // ~25 days max lifetime (safety valve)

  // EV filter — price-based (deferred, but structure ready)
  MIN_ENTRY_PRICE: 0,               // 0 = disabled. Set to e.g. 0.10 to filter
  MAX_ENTRY_PRICE: 1,               // 1 = disabled. Set to e.g. 0.85 to filter
};

// ============================================================================
// Trade Convergence Detection
// ============================================================================

/**
 * Detect markets where multiple tracked wallets have recently bought.
 * This is the core signal source — replaces snapshot-based consensus.
 *
 * @param {Map<string, Array>} recentTrades - wallet → recent trades from Data API
 * @param {Map<string, object>} walletPool - wallet → { score, stats, ... }
 * @param {Map} marketLookup - tokenId → market info (from Gamma)
 * @returns {Array} Array of convergence candidates, sorted by strength
 */
function detectConvergence(recentTrades, walletPool, marketLookup) {
  const now = Math.floor(Date.now() / 1000);
  const windowTs = now - (SIGNAL_THRESHOLDS.CONVERGENCE_WINDOW_HOURS * 3600);

  // Group recent BUY trades by market (conditionId)
  // conditionId is the stable market identifier that groups Yes/No tokens
  const marketBuys = new Map(); // conditionId → { wallets: Map<addr, trades[]>, meta }

  for (const [wallet, trades] of recentTrades) {
    const walletInfo = walletPool.get(wallet);
    if (!walletInfo) continue;

    for (const trade of trades) {
      if (trade.side !== 'BUY') continue;
      if (trade.timestamp < windowTs) continue;

      const cid = trade.conditionId;
      if (!cid) continue;

      if (!marketBuys.has(cid)) {
        marketBuys.set(cid, {
          conditionId: cid,
          title: trade.title || '',
          slug: trade.slug || '',
          eventSlug: trade.eventSlug || '',
          asset: trade.asset || '',
          outcome: trade.outcome || '',
          outcomeIndex: trade.outcomeIndex,
          wallets: new Map(),
        });
      }

      const mb = marketBuys.get(cid);
      if (!mb.wallets.has(wallet)) {
        mb.wallets.set(wallet, {
          address: wallet,
          score: walletInfo.score || 0,
          trades: [],
        });
      }
      mb.wallets.get(wallet).trades.push(trade);
    }
  }

  // Build convergence candidates
  const candidates = [];

  for (const [cid, mb] of marketBuys) {
    const walletCount = mb.wallets.size;
    if (walletCount < SIGNAL_THRESHOLDS.CLUSTER_MIN_WALLETS) continue;

    // Compute aggregate metrics
    let totalSize = 0;
    let totalScoreWeighted = 0;
    let totalScore = 0;
    let avgPrice = 0;
    let priceSum = 0;
    let priceCount = 0;
    let earliestBuy = Infinity;
    let latestBuy = 0;
    const walletDetails = [];

    for (const [addr, wData] of mb.wallets) {
      const walletBuySize = wData.trades.reduce((s, t) => s + (t.size * t.price), 0);
      const walletAvgPrice = wData.trades.reduce((s, t) => s + t.price, 0) / wData.trades.length;
      const walletEarliestBuy = Math.min(...wData.trades.map(t => t.timestamp));
      const walletLatestBuy = Math.max(...wData.trades.map(t => t.timestamp));

      totalSize += walletBuySize;
      totalScore += wData.score;
      totalScoreWeighted += wData.score * walletBuySize; // score weighted by conviction
      priceSum += walletAvgPrice;
      priceCount++;
      earliestBuy = Math.min(earliestBuy, walletEarliestBuy);
      latestBuy = Math.max(latestBuy, walletLatestBuy);

      walletDetails.push({
        address: addr,
        score: wData.score,
        buySize: +walletBuySize.toFixed(2),
        avgPrice: +walletAvgPrice.toFixed(4),
        tradeCount: wData.trades.length,
        firstBuy: walletEarliestBuy,
        lastBuy: walletLatestBuy,
      });
    }

    const avgScore = totalScore / walletCount;
    avgPrice = priceCount > 0 ? priceSum / priceCount : 0;
    const scoreWeightedAvg = totalSize > 0 ? totalScoreWeighted / totalSize : avgScore;

    // Determine direction — which outcome are wallets buying?
    const direction = mb.outcome || 'Unknown';

    candidates.push({
      conditionId: cid,
      title: mb.title,
      slug: mb.slug,
      eventSlug: mb.eventSlug,
      asset: mb.asset,
      direction,
      outcomeIndex: mb.outcomeIndex,

      // Convergence metrics
      walletCount,
      avgScore: +avgScore.toFixed(2),
      scoreWeightedAvg: +scoreWeightedAvg.toFixed(2),
      totalBuySize: +totalSize.toFixed(2),
      avgEntryPrice: +avgPrice.toFixed(4),

      // Timing
      earliestBuy,
      latestBuy,
      convergenceSpanHours: +((latestBuy - earliestBuy) / 3600).toFixed(1),

      // Wallet breakdown
      wallets: walletDetails.sort((a, b) => b.score - a.score),
    });
  }

  // Sort by wallet count × avg score (convergence strength)
  return candidates.sort((a, b) => {
    const strengthA = a.walletCount * a.avgScore;
    const strengthB = b.walletCount * b.avgScore;
    return strengthB - strengthA;
  });
}

// ============================================================================
// Signal Generation
// ============================================================================

/**
 * Process convergence candidates into signals.
 * Opens new signals, updates existing ones, detects exits, and closes stale/resolved.
 *
 * @param {Array} candidates - From detectConvergence()
 * @param {object} existingSignals - { active: {}, history: [], stats: {} }
 * @param {Map<string, Array>} recentTrades - wallet → trades (includes SELL for exit detection)
 * @param {Map<string, object>} walletPool - wallet → { score, stats }
 * @param {Map} marketLookup - tokenId → market info
 * @param {number} scanIndex - Current scan number
 * @returns {object} Updated signals { active, history, stats }
 */
function processSignals(candidates, existingSignals, recentTrades, walletPool, marketLookup, scanIndex) {
  const active = { ...(existingSignals.active || {}) };
  const history = [...(existingSignals.history || [])];
  const now = new Date().toISOString();
  const nowTs = Math.floor(Date.now() / 1000);

  let opened = 0, updated = 0, closed = 0;
  const seenMarkets = new Set();

  // --- Phase 1: Process convergence candidates → open or update signals ---
  for (const candidate of candidates) {
    const cid = candidate.conditionId;
    seenMarkets.add(cid);

    const signalId = `sig_${cid}`;
    const walletCount = candidate.walletCount;
    const avgScore = candidate.avgScore;
    const totalSize = candidate.totalBuySize;

    // Classify signal type
    let signalType, meetsThresholds;

    if (walletCount >= SIGNAL_THRESHOLDS.CONSENSUS_MIN_WALLETS) {
      signalType = 'consensus';
      meetsThresholds = avgScore >= SIGNAL_THRESHOLDS.CONSENSUS_MIN_AVG_SCORE &&
        totalSize >= SIGNAL_THRESHOLDS.CONSENSUS_MIN_TOTAL_SIZE;
    } else if (walletCount >= SIGNAL_THRESHOLDS.CLUSTER_MIN_WALLETS &&
               walletCount <= SIGNAL_THRESHOLDS.CLUSTER_MAX_WALLETS) {
      signalType = 'cluster';
      meetsThresholds = avgScore >= SIGNAL_THRESHOLDS.CLUSTER_MIN_AVG_SCORE &&
        totalSize >= SIGNAL_THRESHOLDS.CLUSTER_MIN_TOTAL_SIZE;
    } else {
      continue; // Below minimum wallet count
    }

    // EV filter (when enabled)
    if (SIGNAL_THRESHOLDS.MIN_ENTRY_PRICE > 0 && candidate.avgEntryPrice < SIGNAL_THRESHOLDS.MIN_ENTRY_PRICE) continue;
    if (SIGNAL_THRESHOLDS.MAX_ENTRY_PRICE < 1 && candidate.avgEntryPrice > SIGNAL_THRESHOLDS.MAX_ENTRY_PRICE) continue;

    if (active[signalId]) {
      // --- UPDATE existing signal ---
      const signal = active[signalId];
      signal.lastUpdatedAt = now;
      signal.lastUpdatedScan = scanIndex;
      signal.lastTradeTs = candidate.latestBuy;

      // Update metrics
      signal.walletCount = walletCount;
      signal.avgScore = avgScore;
      signal.totalBuySize = totalSize;
      signal.avgEntryPrice = candidate.avgEntryPrice;
      signal.signalType = signalType; // Can upgrade cluster → consensus
      signal.conviction = +totalSize.toFixed(2); // Backward compat
      signal.consensusStrength = +(candidate.scoreWeightedAvg / 100 || 0).toFixed(3);

      // Update live market price
      const tokenId = candidate.asset || signal.tokenId;
      const mi = tokenId ? marketLookup.get(tokenId) : null;
      if (mi && mi.currentPrice > 0) {
        signal.currentMarketPrice = +(mi.currentPrice || 0).toFixed(4);
      }

      // Recompute confidence
      signal.confidence = computeConvergenceConfidence(candidate, signalType);
      signal.tier = getSignalTier(signal.confidence);

      // Peak tracking
      signal.peakWallets = Math.max(signal.peakWallets || 0, walletCount);
      signal.peakConfidence = Math.max(signal.peakConfidence || 0, signal.confidence);

      // Wallet snapshot
      signal.currentWallets = candidate.wallets;

      updated++;

    } else if (meetsThresholds) {
      // --- Check market isn't already resolved ---
      const tokenId = candidate.asset || '';
      const mi = tokenId ? marketLookup.get(tokenId) : null;
      if (mi && mi.marketClosed === true) continue;

      // --- OPEN new signal ---
      const confidence = computeConvergenceConfidence(candidate, signalType);
      const currentPrice = mi ? +(mi.currentPrice || 0).toFixed(4) : 0;

      active[signalId] = {
        signalId,
        signalType,
        conditionId: cid,
        tokenId,
        marketTitle: candidate.title,
        slug: candidate.slug,
        eventSlug: candidate.eventSlug,
        groupKey: cid, // Use conditionId as groupKey (stable across tokens)

        // Direction
        direction: candidate.direction,
        outcomeIndex: candidate.outcomeIndex,

        // Timing
        openedAt: now,
        openedScan: scanIndex,
        lastUpdatedAt: now,
        lastUpdatedScan: scanIndex,
        lastTradeTs: candidate.latestBuy,
        earliestBuy: candidate.earliestBuy,
        scansActive: 1,

        // Convergence metrics
        walletCount,
        avgScore,
        totalBuySize: totalSize,
        avgEntryPrice: candidate.avgEntryPrice,
        convergenceSpanHours: candidate.convergenceSpanHours,

        // Backward-compat fields for frontend
        conviction: +totalSize.toFixed(2),
        consensusStrength: +(candidate.scoreWeightedAvg / 100 || 0).toFixed(3),
        avgPnl: 0, // Not applicable in v2 — trades don't carry PnL at open

        // Price at signal open (frozen — never updated)
        openMarketPrice: currentPrice,
        // Live price (updated each scan)
        currentMarketPrice: currentPrice,

        // Confidence
        confidence: +confidence.toFixed(1),
        tier: getSignalTier(confidence),

        // Peak tracking
        peakWallets: walletCount,
        peakConfidence: confidence,

        // Status
        status: 'active',
        outcome: null,
        closedAt: null,
        closedScan: null,
        closeReason: null,

        // Wallet snapshot
        currentWallets: candidate.wallets,
      };

      opened++;
    }
  }

  // --- Phase 1b: Solo signals — single elite wallet, big recent buy ---
  const soloCountByWallet = new Map(); // track max per wallet
  for (const [wallet, trades] of recentTrades) {
    const walletInfo = walletPool.get(wallet);
    if (!walletInfo) continue;
    if ((walletInfo.score || 0) < SIGNAL_THRESHOLDS.SOLO_MIN_SCORE) continue;
    if ((walletInfo.stats?.recentWinRate || walletInfo.stats?.winRate || 0) < SIGNAL_THRESHOLDS.SOLO_MIN_WIN_RATE) continue;
    if ((walletInfo.stats?.resolvedMarkets || 0) < SIGNAL_THRESHOLDS.SOLO_MIN_RESOLVED) continue;

    // Count existing solo signals for this wallet
    const existingSoloCount = Object.values(active).filter(s =>
      s.signalType === 'solo' && s.soloWallet === wallet
    ).length;
    if (existingSoloCount >= SIGNAL_THRESHOLDS.SOLO_MAX_PER_WALLET) continue;

    // Group this wallet's recent buys by market
    const walletMarkets = new Map();
    for (const trade of trades) {
      if (trade.side !== 'BUY') continue;
      const cid = trade.conditionId;
      if (!cid) continue;
      if (seenMarkets.has(cid)) continue; // Already covered by consensus/cluster

      if (!walletMarkets.has(cid)) {
        walletMarkets.set(cid, { trades: [], meta: trade });
      }
      walletMarkets.get(cid).trades.push(trade);
    }

    for (const [cid, data] of walletMarkets) {
      const buySize = data.trades.reduce((s, t) => s + (t.size * t.price), 0);
      if (buySize < SIGNAL_THRESHOLDS.SOLO_MIN_BUY_SIZE) continue;

      const signalId = `sig_solo_${wallet.slice(0, 10)}_${cid.slice(0, 10)}`;
      if (active[signalId]) {
        // Update existing solo signal
        const signal = active[signalId];
        signal.lastUpdatedAt = now;
        signal.lastUpdatedScan = scanIndex;
        signal.lastTradeTs = Math.max(...data.trades.map(t => t.timestamp));
        signal.totalBuySize = +buySize.toFixed(2);

        const tokenId = data.meta.asset || signal.tokenId;
        const mi = tokenId ? marketLookup.get(tokenId) : null;
        if (mi && mi.currentPrice > 0) {
          signal.currentMarketPrice = +(mi.currentPrice || 0).toFixed(4);
        }

        updated++;
      } else {
        // Check market not resolved
        const tokenId = data.meta.asset || '';
        const mi = tokenId ? marketLookup.get(tokenId) : null;
        if (mi && mi.marketClosed === true) continue;

        // Open solo signal
        const avgPrice = data.trades.reduce((s, t) => s + t.price, 0) / data.trades.length;
        const currentPrice = mi ? +(mi.currentPrice || 0).toFixed(4) : 0;
        const confidence = computeSoloConfidence(walletInfo, buySize, avgPrice);

        active[signalId] = {
          signalId,
          signalType: 'solo',
          conditionId: cid,
          tokenId,
          marketTitle: data.meta.title || '',
          slug: data.meta.slug || '',
          eventSlug: data.meta.eventSlug || '',
          groupKey: cid,

          direction: data.meta.outcome || 'Unknown',
          outcomeIndex: data.meta.outcomeIndex,
          soloWallet: wallet,

          openedAt: now,
          openedScan: scanIndex,
          lastUpdatedAt: now,
          lastUpdatedScan: scanIndex,
          lastTradeTs: Math.max(...data.trades.map(t => t.timestamp)),
          scansActive: 1,

          walletCount: 1,
          avgScore: walletInfo.score,
          totalBuySize: +buySize.toFixed(2),
          avgEntryPrice: +avgPrice.toFixed(4),

          openMarketPrice: currentPrice,
          currentMarketPrice: currentPrice,

          confidence: +confidence.toFixed(1),
          tier: getSignalTier(confidence),

          peakWallets: 1,
          peakConfidence: confidence,

          status: 'active',
          outcome: null,
          closedAt: null,
          closedScan: null,
          closeReason: null,

          currentWallets: [{
            address: wallet,
            score: walletInfo.score,
            buySize: +buySize.toFixed(2),
            avgPrice: +avgPrice.toFixed(4),
            tradeCount: data.trades.length,
          }],
        };

        opened++;
      }
    }
  }

  // --- Phase 2: Exit detection — wallets selling positions backing active signals ---
  for (const [signalId, signal] of Object.entries(active)) {
    signal.scansActive = (signal.scansActive || 0) + 1;

    // Refresh live market price from cache (even if not in convergence candidates)
    const refreshTokenId = signal.tokenId;
    const refreshMi = refreshTokenId ? marketLookup.get(refreshTokenId) : null;
    if (refreshMi && refreshMi.currentPrice > 0) {
      signal.currentMarketPrice = +(refreshMi.currentPrice).toFixed(4);
    }

    // Check for sells from backing wallets (needed for exit ratio AND redeem detection later)
    let walletsExited = 0;
    const backingWallets = signal.currentWallets || [];

    for (const w of backingWallets) {
      const walletTrades = recentTrades.get(w.address);
      if (!walletTrades) continue;
      const sells = walletTrades.filter(t =>
        t.conditionId === signal.conditionId && t.side === 'SELL'
      );
      if (sells.length > 0) walletsExited++;
    }

    const exitRatio = backingWallets.length > 0 ? walletsExited / backingWallets.length : 0;
    signal.exitRatio = +exitRatio.toFixed(2);
    signal.walletsExited = walletsExited;

    // A signal has ONLY two terminal states: win or loss.
    // It stays Active while the market is open — regardless of wallet exits,
    // staleness, or age. The only close path is: market has actually resolved
    // on Gamma AND winningOutcome is known → determine direction match.
    const tokenId = signal.tokenId;
    const mi = tokenId ? marketLookup.get(tokenId) : null;

    // Track informational flags on the active signal (for dashboard display only —
    // these do NOT close the signal).
    const lifetimeHours = (nowTs - new Date(signal.openedAt).getTime() / 1000) / 3600;
    signal.lifetimeHours = +lifetimeHours.toFixed(1);
    signal.backersExited = exitRatio > 0.5 && backingWallets.length >= 2;

    // Market resolution: the ONLY way a signal becomes terminal.
    // Requires BOTH: Gamma says the market is closed AND winningOutcome is populated.
    // If marketClosed is true but winningOutcome isn't available yet, we wait —
    // next scan will pick it up.
    const gammaClosed = mi && mi.marketClosed === true;
    const hasWinner = mi && mi.winningOutcome && mi.winningOutcome.length > 0;

    if (gammaClosed && hasWinner) {
      const won = matchesWinningOutcome(signal.direction, signal.direction, mi.winningOutcome);
      const outcome = won ? 'win' : 'loss';
      closeSignal(active, history, signalId, 'resolved', scanIndex, now, outcome);
      const lastEntry = history[history.length - 1];
      if (lastEntry && lastEntry.signalId === signalId) {
        const openPrice = signal.openMarketPrice || signal.avgEntryPrice || 0;
        if (outcome === 'win' && openPrice > 0) {
          lastEntry.signalReturn = +((1 / openPrice - 1) * 100).toFixed(2);
        } else if (outcome === 'loss') {
          lastEntry.signalReturn = -100;
        }
        lastEntry.resolvedBy = 'gamma';
        lastEntry.openMarketPrice = openPrice;
        lastEntry.winningOutcome = mi.winningOutcome;
      }
      closed++;
      continue;
    }
  }

  // --- Phase 2.5: Legacy history repair (safety net for pre-v3 entries) ---
  // In the new model, Phase 2 NEVER writes a null-outcome entry to history —
  // signals only close when Gamma has both marketClosed=true and winningOutcome.
  // This phase exists purely to clean up legacy history entries from before the
  // model simplification. It does three things:
  //   (a) If signalId is already active → drop the stale history duplicate
  //   (b) If market is still open → move it back to active (it was wrongly closed)
  //   (c) If market has resolved and Gamma has winningOutcome → backfill WIN/LOSS
  let repaired = 0;
  let restored = 0;
  let dedupedFromActive = 0;
  for (let i = history.length - 1; i >= 0; i--) {
    const h = history[i];
    if (h.outcome === 'win' || h.outcome === 'loss') continue;
    if (!h.conditionId && !h.tokenId) continue;

    if (h.signalId && active[h.signalId]) {
      history.splice(i, 1);
      dedupedFromActive++;
      continue;
    }

    const hmi = h.tokenId ? marketLookup.get(h.tokenId) : null;
    if (!hmi) continue;

    const marketStillOpen = hmi.marketClosed !== true;
    if (marketStillOpen) {
      const sid = h.signalId;
      if (sid && !active[sid]) {
        h.status = 'active';
        h.outcome = null;
        h.closedAt = null;
        h.closedScan = null;
        delete h.closeReason;
        delete h.closedReason;
        active[sid] = h;
        history.splice(i, 1);
        restored++;
      }
      continue;
    }

    // Market closed — backfill only if Gamma has winningOutcome
    if (hmi.winningOutcome) {
      const won = matchesWinningOutcome(h.direction, h.direction, hmi.winningOutcome);
      h.outcome = won ? 'win' : 'loss';
      h.resolvedBy = 'gamma_repair';
      h.closeReason = 'resolved';
      h.winningOutcome = hmi.winningOutcome;
      const op = h.openMarketPrice || h.avgEntryPrice || 0;
      if (h.outcome === 'win' && op > 0) {
        h.signalReturn = +((1 / op - 1) * 100).toFixed(2);
      } else if (h.outcome === 'loss') {
        h.signalReturn = -100;
      }
      repaired++;
    }
  }
  if (repaired > 0 || restored > 0 || dedupedFromActive > 0) {
    console.log(`  History repair: ${repaired} backfilled with WIN/LOSS, ${restored} restored to active, ${dedupedFromActive} duplicates of active removed`);
  }

  // --- Phase 3: Aggregate stats ---
  const activeSignals = Object.values(active);
  const allHistory = history;
  const wins = allHistory.filter(s => s.outcome === 'win').length;
  const losses = allHistory.filter(s => s.outcome === 'loss').length;
  const totalResolved = wins + losses;

  const stats = {
    activeCount: activeSignals.length,
    historyCount: allHistory.length,
    totalResolved,
    winRate: totalResolved > 0 ? +(wins / totalResolved * 100).toFixed(1) : 0,
    wins,
    losses,
    lastScan: scanIndex,
    lastUpdated: now,
    opened,
    updated,
    closed,
  };

  return { active, history, stats };
}

// ============================================================================
// Confidence Scoring
// ============================================================================

function computeConvergenceConfidence(candidate, signalType) {
  // Wallet count factor (30 pts)
  const minWallets = signalType === 'consensus'
    ? SIGNAL_THRESHOLDS.CONSENSUS_MIN_WALLETS
    : SIGNAL_THRESHOLDS.CLUSTER_MIN_WALLETS;
  const walletFactor = Math.min(1, candidate.walletCount / (minWallets * 2)) * 30;

  // Score factor (25 pts) — average wallet quality
  const scoreFactor = Math.min(1, candidate.avgScore / 90) * 25;

  // Size factor (20 pts) — total $ committed (log scale)
  const sizeFactor = Math.min(1, Math.log10(1 + candidate.totalBuySize) / 4) * 20;

  // Timing factor (15 pts) — tighter convergence = stronger signal
  // All wallets buying within 2 hours = 15pts. Spread over 48 hours = 5pts.
  const spanHours = candidate.convergenceSpanHours || 48;
  const timingFactor = Math.max(0, 1 - spanHours / 72) * 15;

  // Price factor (10 pts) — better EV at lower prices
  const price = candidate.avgEntryPrice || 0.5;
  const priceFactor = price > 0 && price < 1
    ? (1 - price) * 10  // 10¢ = 9pts, 50¢ = 5pts, 90¢ = 1pt
    : 5;

  return Math.min(100, walletFactor + scoreFactor + sizeFactor + timingFactor + priceFactor);
}

function computeSoloConfidence(walletInfo, buySize, avgPrice) {
  // Wallet quality (40 pts)
  const qualityFactor = Math.min(1, (walletInfo.score || 0) / 95) * 40;

  // Position size (30 pts, log scale)
  const sizeFactor = Math.min(1, Math.log10(1 + buySize) / 4) * 30;

  // Price factor (15 pts)
  const price = avgPrice || 0.5;
  const priceFactor = price > 0 && price < 1 ? (1 - price) * 15 : 7.5;

  // Win rate bonus (15 pts)
  const wr = walletInfo.stats?.recentWinRate || walletInfo.stats?.winRate || 0;
  const wrFactor = Math.max(0, (wr - 0.5) * 2) * 15;

  return Math.min(100, qualityFactor + sizeFactor + priceFactor + wrFactor);
}

function getSignalTier(confidence) {
  if (confidence >= 80) return 'elite';
  if (confidence >= 55) return 'pro';
  return 'starter';
}

// ============================================================================
// Signal close helper (mirrors old system for compatibility)
// ============================================================================

function closeSignal(active, history, signalId, reason, scanIndex, timestamp, outcome = null) {
  const signal = active[signalId];
  if (!signal) return;

  signal.status = 'closed';
  signal.closedAt = timestamp;
  signal.closedScan = scanIndex;
  signal.closeReason = reason;
  signal.outcome = outcome;
  signal.duration = scanIndex - (signal.openedScan || scanIndex);

  // Strip wallet snapshot to save space
  delete signal.currentWallets;

  // Prevent duplicate history
  const isDuplicate = history.some(h =>
    h.conditionId === signal.conditionId && h.closeReason === reason && h.outcome === outcome
  );
  if (!isDuplicate) {
    history.push(signal);
  }
  delete active[signalId];

  if (history.length > 500) {
    history.splice(0, history.length - 500);
  }
}

// ============================================================================
// Exports
// ============================================================================

export {
  SIGNAL_THRESHOLDS,
  detectConvergence,
  processSignals,
  getSignalTier,
  closeSignal,
};
