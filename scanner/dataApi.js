/**
 * Polymarket Data API Client
 *
 * Primary data source for wallet trade histories, activity, and real-time monitoring.
 * Endpoint: https://data-api.polymarket.com
 *
 * No authentication required. Rate limits appear generous (~60ms per request).
 */

const DATA_API_BASE = 'https://data-api.polymarket.com';

// Adaptive rate limiting — backs off on errors, speeds up on success
let requestDelay = 50; // ms between requests
const MIN_DELAY = 30;
const MAX_DELAY = 2000;

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Make a rate-limited request to the Data API
 */
async function apiRequest(path, params = {}) {
  const url = new URL(`${DATA_API_BASE}${path}`);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null) url.searchParams.set(k, v);
  }

  const MAX_RETRIES = 3;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      await sleep(requestDelay);
      const response = await fetch(url.toString());

      if (response.status === 429) {
        const backoff = Math.min(5000, 500 * Math.pow(2, attempt));
        requestDelay = Math.min(MAX_DELAY, requestDelay * 2);
        console.warn(`  Data API 429 — backing off ${backoff}ms (delay now ${requestDelay}ms)`);
        await sleep(backoff);
        continue;
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      // Success — speed back up
      requestDelay = Math.max(MIN_DELAY, requestDelay - 5);

      const data = await response.json();
      return data;
    } catch (err) {
      if (attempt === MAX_RETRIES) {
        console.error(`  Data API error after ${MAX_RETRIES} retries: ${err.message}`);
        return null;
      }
      await sleep(500 * (attempt + 1));
    }
  }
  return null;
}

// ============================================================================
// Trade History
// ============================================================================

/**
 * Fetch trades for a wallet address.
 * Returns individual trades with timestamps, prices, sides (BUY/SELL).
 *
 * @param {string} wallet - Wallet address (proxyWallet)
 * @param {object} opts - Options
 * @param {number} opts.limit - Max trades to return (default 500, max 10000)
 * @param {number} opts.offset - Pagination offset
 * @param {number} opts.startTs - Unix timestamp — only trades after this time
 * @param {number} opts.endTs - Unix timestamp — only trades before this time
 * @returns {Promise<Array>} Array of trade objects
 */
async function fetchTrades(wallet, opts = {}) {
  const params = {
    user: wallet,
    limit: opts.limit || 500,
  };
  if (opts.offset) params.offset = opts.offset;
  if (opts.startTs) params.startTs = opts.startTs;
  if (opts.endTs) params.endTs = opts.endTs;

  const data = await apiRequest('/trades', params);
  return Array.isArray(data) ? data : [];
}

/**
 * Fetch ALL trades for a wallet (paginated).
 * Warning: can be many thousands of trades for active wallets.
 *
 * @param {string} wallet - Wallet address
 * @param {object} opts - Options
 * @param {number} opts.maxTrades - Stop after this many trades (default 10000)
 * @param {number} opts.startTs - Only trades after this timestamp
 * @returns {Promise<Array>} All trades, newest first
 */
async function fetchAllTrades(wallet, opts = {}) {
  const maxTrades = opts.maxTrades || 10000;
  const allTrades = [];
  let offset = 0;
  const pageSize = 1000;

  while (allTrades.length < maxTrades) {
    const batch = await fetchTrades(wallet, {
      limit: Math.min(pageSize, maxTrades - allTrades.length),
      offset,
      startTs: opts.startTs,
    });

    if (!batch || batch.length === 0) break;
    allTrades.push(...batch);

    if (batch.length < pageSize) break; // last page
    offset += pageSize;
  }

  return allTrades;
}

/**
 * Fetch recent trades for multiple wallets (since a given timestamp).
 * Designed for the fast loop — check what tracked wallets have done recently.
 *
 * @param {string[]} wallets - Array of wallet addresses
 * @param {number} sinceTs - Unix timestamp — only trades after this
 * @param {Function} onProgress - Optional callback(walletsProcessed, totalWallets)
 * @returns {Promise<Map<string, Array>>} Map of wallet → recent trades
 */
async function fetchRecentTrades(wallets, sinceTs, onProgress) {
  const results = new Map();
  let processed = 0;

  for (const wallet of wallets) {
    const trades = await fetchTrades(wallet, {
      limit: 100, // Most wallets won't have >100 trades in an hour
      startTs: sinceTs,
    });
    if (trades && trades.length > 0) {
      results.set(wallet, trades);
    }
    processed++;
    if (onProgress && processed % 50 === 0) {
      onProgress(processed, wallets.length);
    }
  }

  return results;
}

// ============================================================================
// Activity (Trades + Redeems)
// ============================================================================

/**
 * Fetch activity for a wallet (includes TRADE and REDEEM events).
 * REDEEM events indicate a wallet collected winnings from a resolved market.
 *
 * @param {string} wallet - Wallet address
 * @param {object} opts - Options
 * @param {number} opts.limit - Max records (default 500)
 * @param {number} opts.startTs - Only activity after this timestamp
 * @returns {Promise<Array>} Array of activity objects
 */
async function fetchActivity(wallet, opts = {}) {
  const params = {
    user: wallet,
    limit: opts.limit || 500,
  };
  if (opts.startTs) params.startTs = opts.startTs;

  const data = await apiRequest('/activity', params);
  return Array.isArray(data) ? data : [];
}

// ============================================================================
// Trade Analysis
// ============================================================================

/**
 * Analyze a wallet's trade history to compute real performance stats.
 * This replaces the snapshot-based analyzePositions() from the old system.
 *
 * @param {Array} trades - Full trade history from fetchAllTrades()
 * @param {object} opts - Options
 * @param {number} opts.windowDays - Only consider trades from last N days (default: 90)
 * @returns {object} Computed stats
 */
function analyzeTradeHistory(trades, opts = {}) {
  if (!trades || trades.length === 0) {
    return null;
  }

  const windowDays = opts.windowDays || 90;
  const windowTs = Math.floor(Date.now() / 1000) - (windowDays * 86400);
  const now = Math.floor(Date.now() / 1000);

  // Split into time windows for comparison
  const recentTrades = trades.filter(t => t.timestamp >= windowTs);
  const allTrades = trades;

  // Group trades by market (conditionId) to compute per-market outcomes
  const marketTrades = new Map(); // conditionId → { buys: [], sells: [], redeems: [], meta: {} }

  for (const trade of allTrades) {
    const cid = trade.conditionId;
    if (!cid) continue;

    if (!marketTrades.has(cid)) {
      marketTrades.set(cid, {
        buys: [],
        sells: [],
        title: trade.title || '',
        slug: trade.slug || '',
        eventSlug: trade.eventSlug || '',
        outcome: trade.outcome || '',
        outcomeIndex: trade.outcomeIndex,
      });
    }

    const mt = marketTrades.get(cid);
    if (trade.side === 'BUY') {
      mt.buys.push(trade);
    } else if (trade.side === 'SELL') {
      mt.sells.push(trade);
    }
  }

  // Compute per-market P&L
  // For each market: total bought (size * price), total sold (size * price)
  // If no sells, position is still open
  let resolvedMarkets = 0;
  let wins = 0;
  let losses = 0;
  let totalPnl = 0;
  let openPositions = 0;
  const marketResults = [];
  const categories = new Map(); // category → { wins, losses, pnl }

  // Recent window stats
  let recentResolved = 0;
  let recentWins = 0;
  let recentLosses = 0;
  let recentPnl = 0;

  for (const [cid, mt] of marketTrades) {
    const totalBought = mt.buys.reduce((sum, t) => sum + (t.size * t.price), 0);
    const totalSold = mt.sells.reduce((sum, t) => sum + (t.size * t.price), 0);
    const totalBuySize = mt.buys.reduce((sum, t) => sum + t.size, 0);
    const totalSellSize = mt.sells.reduce((sum, t) => sum + t.size, 0);
    const avgBuyPrice = totalBuySize > 0 ? totalBought / totalBuySize : 0;
    const avgSellPrice = totalSellSize > 0 ? totalSold / totalSellSize : 0;

    const firstBuy = mt.buys.length > 0 ? Math.min(...mt.buys.map(t => t.timestamp)) : 0;
    const lastTrade = Math.max(
      ...mt.buys.map(t => t.timestamp),
      ...mt.sells.map(t => t.timestamp),
      0
    );
    const isRecent = firstBuy >= windowTs;

    // Position fully closed if sell size ≈ buy size
    const positionClosed = totalSellSize >= totalBuySize * 0.95;

    if (positionClosed && totalBuySize > 0) {
      // Resolved market
      const pnl = totalSold - totalBought;
      resolvedMarkets++;
      totalPnl += pnl;

      if (pnl > 0) {
        wins++;
      } else {
        losses++;
      }

      if (isRecent) {
        recentResolved++;
        recentPnl += pnl;
        if (pnl > 0) recentWins++;
        else recentLosses++;
      }

      marketResults.push({
        conditionId: cid,
        title: mt.title,
        outcome: pnl > 0 ? 'win' : 'loss',
        pnl,
        avgBuyPrice,
        avgSellPrice,
        buySize: totalBuySize,
        firstBuy,
        lastTrade,
        holdTime: lastTrade - firstBuy,
      });

      // Category tracking
      const cat = mt.eventSlug?.split('-')[0] || 'unknown';
      if (!categories.has(cat)) categories.set(cat, { wins: 0, losses: 0, pnl: 0 });
      const catStats = categories.get(cat);
      catStats.pnl += pnl;
      if (pnl > 0) catStats.wins++;
      else catStats.losses++;
    } else {
      openPositions++;
    }
  }

  // Compute derived metrics
  const winRate = resolvedMarkets > 0 ? wins / resolvedMarkets : 0;
  const recentWinRate = recentResolved > 0 ? recentWins / recentResolved : 0;
  const avgPnlPerTrade = resolvedMarkets > 0 ? totalPnl / resolvedMarkets : 0;
  const avgHoldTime = marketResults.length > 0
    ? marketResults.reduce((sum, r) => sum + r.holdTime, 0) / marketResults.length
    : 0;

  // Trading frequency
  const firstTradeTs = allTrades.length > 0 ? Math.min(...allTrades.map(t => t.timestamp)) : now;
  const tradingSpanDays = Math.max(1, (now - firstTradeTs) / 86400);
  const tradesPerDay = allTrades.length / tradingSpanDays;
  const marketsPerDay = marketTrades.size / tradingSpanDays;

  // Recent trading frequency
  const recentTradeCount = recentTrades.length;
  const recentTradesPerDay = windowDays > 0 ? recentTradeCount / windowDays : 0;

  // Edge ratio: average win / average loss
  const avgWin = wins > 0
    ? marketResults.filter(r => r.outcome === 'win').reduce((s, r) => s + r.pnl, 0) / wins
    : 0;
  const avgLoss = losses > 0
    ? Math.abs(marketResults.filter(r => r.outcome === 'loss').reduce((s, r) => s + r.pnl, 0) / losses)
    : 1;
  const edgeRatio = avgLoss > 0 ? avgWin / avgLoss : 0;

  return {
    // Core performance
    winRate: +winRate.toFixed(4),
    recentWinRate: +recentWinRate.toFixed(4),
    resolvedMarkets,
    recentResolved,
    wins,
    losses,
    totalPnl: +totalPnl.toFixed(2),
    recentPnl: +recentPnl.toFixed(2),
    avgPnlPerTrade: +avgPnlPerTrade.toFixed(2),
    edgeRatio: +edgeRatio.toFixed(2),

    // Activity
    totalTrades: allTrades.length,
    recentTrades: recentTradeCount,
    uniqueMarkets: marketTrades.size,
    openPositions,
    tradesPerDay: +tradesPerDay.toFixed(2),
    recentTradesPerDay: +recentTradesPerDay.toFixed(2),
    marketsPerDay: +marketsPerDay.toFixed(2),
    tradingSpanDays: Math.round(tradingSpanDays),

    // Timing
    avgHoldTimeHours: +(avgHoldTime / 3600).toFixed(1),
    firstTradeTs,
    lastTradeTs: allTrades.length > 0 ? allTrades[0].timestamp : 0,

    // Breakdown
    categories: Object.fromEntries(categories),
    topMarkets: marketResults
      .sort((a, b) => Math.abs(b.pnl) - Math.abs(a.pnl))
      .slice(0, 10),
  };
}

// ============================================================================
// Wallet Scoring (trade-history-based)
// ============================================================================

/**
 * Compute a wallet's quality score from their trade history stats.
 * Replaces the old snapshot-based computeScore().
 *
 * Score is 0–100, built from:
 *   - Recent win rate (30 pts) — last 90 days, weighted by sample size
 *   - All-time win rate (10 pts) — long-term track record
 *   - Profitability (15 pts) — total PnL on log scale
 *   - Consistency (15 pts) — recent vs all-time performance similarity
 *   - Activity (15 pts) — trading frequency, recency
 *   - Edge quality (15 pts) — avg win / avg loss ratio
 *
 * @param {object} stats - Output from analyzeTradeHistory()
 * @returns {number} Score 0–100
 */
function computeWalletScore(stats) {
  if (!stats || stats.resolvedMarkets === 0) return 0;

  // Recent win rate (30 pts) — most important, but needs sample backing
  // Scale: 50% WR = 0pts, 100% WR = 30pts, with sample size damping
  const recentSampleFactor = Math.min(1, Math.sqrt(stats.recentResolved) / 5); // plateaus at ~25 resolved
  const recentWrScore = Math.max(0, (stats.recentWinRate - 0.5) * 2) * recentSampleFactor * 30;

  // All-time win rate (10 pts) — long-term verification
  const allTimeSampleFactor = Math.min(1, Math.sqrt(stats.resolvedMarkets) / 8); // plateaus at ~64 resolved
  const allTimeWrScore = Math.max(0, (stats.winRate - 0.5) * 2) * allTimeSampleFactor * 10;

  // Profitability (15 pts) — log scale to avoid saturation
  // $100 PnL ≈ 5pts, $1k ≈ 10pts, $10k ≈ 13pts, $100k ≈ 15pts
  const pnlScore = stats.totalPnl > 0
    ? Math.min(1, Math.log10(1 + stats.totalPnl) / 5) * 15
    : 0;

  // Consistency (15 pts) — penalise wallets whose recent performance diverges from all-time
  // If recent WR is close to all-time WR, high consistency. Big drop = low consistency.
  const wrDiff = Math.abs(stats.recentWinRate - stats.winRate);
  const consistencyScore = stats.recentResolved >= 5
    ? Math.max(0, 1 - wrDiff * 3) * 15  // 33% WR difference = 0 pts
    : 7.5; // Not enough recent data — give benefit of doubt

  // Activity (15 pts) — recent trading frequency and recency
  const daysSinceLastTrade = stats.lastTradeTs > 0
    ? (Date.now() / 1000 - stats.lastTradeTs) / 86400
    : 999;
  const recencyFactor = daysSinceLastTrade <= 3 ? 1.0
    : daysSinceLastTrade <= 7 ? 0.9
    : daysSinceLastTrade <= 14 ? 0.75
    : daysSinceLastTrade <= 30 ? 0.5
    : daysSinceLastTrade <= 60 ? 0.25
    : 0;
  const frequencyFactor = Math.min(1, Math.log10(1 + stats.recentTradesPerDay * 10) / 2);
  const activityScore = (recencyFactor * 10 + frequencyFactor * 5);

  // Edge quality (15 pts) — avg win / avg loss ratio
  // edgeRatio of 1.0 = break even, 2.0 = good, 5.0+ = elite
  const edgeScore = stats.edgeRatio > 0
    ? Math.min(1, Math.log2(1 + Math.max(0, stats.edgeRatio - 0.5)) / 3) * 15
    : 0;

  const total = recentWrScore + allTimeWrScore + pnlScore + consistencyScore + activityScore + edgeScore;
  return Math.min(100, Math.round(total * 10) / 10);
}

// ============================================================================
// Exports
// ============================================================================

export {
  DATA_API_BASE,
  fetchTrades,
  fetchAllTrades,
  fetchRecentTrades,
  fetchActivity,
  analyzeTradeHistory,
  computeWalletScore,
};
