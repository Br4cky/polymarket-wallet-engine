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
        let body = '';
        try { body = await response.text(); } catch {}
        throw new Error(`HTTP ${response.status}: ${response.statusText}${body ? ' — ' + body.slice(0, 200) : ''}`);
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
    user: wallet.toLowerCase(),
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
  let truncated = false;

  const maxOffset = 3000; // Data API hard limit on pagination offset

  while (allTrades.length < maxTrades) {
    if (offset >= maxOffset) {
      // Hit the API offset ceiling — wallet has more history we can't see.
      truncated = true;
      break;
    }

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

  // If we hit the user's own maxTrades cap before the API ceiling, also flag.
  if (!truncated && allTrades.length >= maxTrades) truncated = true;
  allTrades.truncated = truncated;
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
    user: wallet.toLowerCase(),
    limit: opts.limit || 500,
  };
  if (opts.offset) params.offset = opts.offset;
  if (opts.startTs) params.startTs = opts.startTs;

  const data = await apiRequest('/activity', params);
  return Array.isArray(data) ? data : [];
}

/**
 * Fetch all activity events for a wallet (TRADE + REDEEM + others).
 * Paginated; capped at the Data API's 3000-offset ceiling.
 *
 * Crucial difference vs fetchAllTrades: REDEEM events are how Polymarket
 * winners actually collect their payouts. Without them, winning positions
 * look "still open" to our analyzer and contribute $0 to computed PnL,
 * which is why internal stats.totalPnl drifts far below Goldsky's lifetime
 * aggregate for any wallet that redeems winners through to resolution.
 *
 * @param {string} wallet - Wallet address
 * @param {object} opts - Options
 * @param {number} opts.maxEvents - Cap total events returned (default 10000)
 * @param {number} opts.startTs - Only events after this timestamp
 * @returns {Promise<Array>} All activity events, plus `truncated` flag as an
 *                           array property when pagination hit a ceiling.
 */
async function fetchAllActivity(wallet, opts = {}) {
  const maxEvents = opts.maxEvents || 10000;
  const allEvents = [];
  let offset = 0;
  const pageSize = 500;
  const maxOffset = 3000;
  let truncated = false;

  while (allEvents.length < maxEvents) {
    if (offset >= maxOffset) {
      truncated = true;
      break;
    }

    const batch = await fetchActivity(wallet, {
      limit: Math.min(pageSize, maxEvents - allEvents.length),
      offset,
      startTs: opts.startTs,
    });

    if (!batch || batch.length === 0) break;
    allEvents.push(...batch);

    if (batch.length < pageSize) break;
    offset += pageSize;
  }

  if (!truncated && allEvents.length >= maxEvents) truncated = true;
  allEvents.truncated = truncated;
  return allEvents;
}

// ============================================================================
// Trade Analysis
// ============================================================================

/**
 * Analyze a wallet's trade/activity history to compute real performance stats.
 *
 * Accepts either:
 *   - A pure trade list from fetchAllTrades() (legacy), OR
 *   - A mixed activity list from fetchAllActivity() including REDEEM events.
 *
 * REDEEM events are normalised into synthetic SELLs so that winning positions
 * which the wallet held through to resolution actually close out. Without this
 * step, redeemed winners stay bucketed as "open" and contribute $0 to PnL,
 * which is why our internal stats.totalPnl used to drift far below Goldsky.
 *
 * @param {Array} events - Full history from fetchAllTrades() or fetchAllActivity()
 * @param {object} opts - Options
 * @param {number} opts.windowDays - Only consider events from last N days (default: 90)
 * @param {Map} opts.marketLookup - Optional Map<conditionId, { marketClosed, winningOutcome }>
 *                                  used to resolve positions the wallet never explicitly closed
 *                                  (winners who didn't redeem, losers whose shares went to $0).
 * @returns {object} Computed stats (includes `tradesTruncated` if pagination capped)
 */
function analyzeTradeHistory(events, opts = {}) {
  if (!events || events.length === 0) {
    return null;
  }

  const windowDays = opts.windowDays || 90;
  const windowTs = Math.floor(Date.now() / 1000) - (windowDays * 86400);
  const now = Math.floor(Date.now() / 1000);
  const marketLookup = opts.marketLookup instanceof Map ? opts.marketLookup : null;

  // Normalise every event into either a BUY, a SELL, or a synthetic SELL from
  // a REDEEM. Anything else (REWARD, SPLIT, MERGE, CONVERSION, …) is ignored.
  // A normalised event always carries: timestamp, conditionId, size, price,
  // side, plus market metadata when present.
  const allEvents = [];
  for (const ev of events) {
    if (!ev || typeof ev !== 'object') continue;
    const type = (ev.type || '').toUpperCase();
    const cid = ev.conditionId;
    if (!cid) continue;

    if (type === 'REDEEM') {
      // Redemption: wallet cashed `size` shares for `usdcSize` USDC payout.
      // Treat as a synthetic SELL at the implied price. Winners redeem
      // at $1/share; losers at $0 (payout usually absent for losers entirely,
      // so the position is just left truncated — we detect via the
      // marketClosed check downstream).
      const size = parseFloat(ev.size || ev.shares || 0) || 0;
      const payout = parseFloat(ev.usdcSize || ev.payout || 0) || 0;
      if (size <= 0) continue;
      const price = payout > 0 ? payout / size : 0;
      allEvents.push({
        timestamp: ev.timestamp,
        conditionId: cid,
        asset: ev.asset || ev.tokenId || '',
        side: 'SELL',
        size,
        price,
        title: ev.title || '',
        slug: ev.slug || '',
        eventSlug: ev.eventSlug || '',
        outcome: ev.outcome || '',
        outcomeIndex: ev.outcomeIndex,
        _fromRedeem: true,
      });
      continue;
    }

    // Plain trade — type may be 'TRADE' or absent (legacy fetchAllTrades shape)
    if (type === 'TRADE' || type === '' || type === undefined) {
      const side = (ev.side || '').toUpperCase();
      if (side !== 'BUY' && side !== 'SELL') continue;
      allEvents.push({
        timestamp: ev.timestamp,
        conditionId: cid,
        asset: ev.asset || ev.tokenId || '',
        side,
        size: parseFloat(ev.size || 0) || 0,
        price: parseFloat(ev.price || 0) || 0,
        title: ev.title || '',
        slug: ev.slug || '',
        eventSlug: ev.eventSlug || '',
        outcome: ev.outcome || '',
        outcomeIndex: ev.outcomeIndex,
      });
    }
    // Other activity types (REWARD, SPLIT, MERGE, CONVERSION) are dropped.
  }

  if (allEvents.length === 0) return null;

  const recentTrades = allEvents.filter(t => t.timestamp >= windowTs);

  // Group trades by market (conditionId) to compute per-market outcomes
  const marketTrades = new Map(); // conditionId → { buys: [], sells: [], redeems: [], meta: {} }

  for (const trade of allEvents) {
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
  // Counters so callers can tell how much of the WR fix actually fired
  let unredeemedWins = 0;
  let worthlessLosses = 0;

  for (const [cid, mt] of marketTrades) {
    const totalBought = mt.buys.reduce((sum, t) => sum + (t.size * t.price), 0);
    const totalSoldRaw = mt.sells.reduce((sum, t) => sum + (t.size * t.price), 0);
    const totalBuySize = mt.buys.reduce((sum, t) => sum + t.size, 0);
    const totalSellSize = mt.sells.reduce((sum, t) => sum + t.size, 0);
    const avgBuyPrice = totalBuySize > 0 ? totalBought / totalBuySize : 0;
    const avgSellPrice = totalSellSize > 0 ? totalSoldRaw / totalSellSize : 0;

    const firstBuy = mt.buys.length > 0 ? Math.min(...mt.buys.map(t => t.timestamp)) : 0;
    const lastTrade = Math.max(
      ...mt.buys.map(t => t.timestamp),
      ...mt.sells.map(t => t.timestamp),
      0
    );
    const isRecent = firstBuy >= windowTs;

    // Position fully closed if sell size ≈ buy size
    let positionClosed = totalSellSize >= totalBuySize * 0.95;
    let totalSold = totalSoldRaw;
    let syntheticCloseKind = null; // 'unredeemed_win' | 'worthless_loss' | null

    // WR fix: if the position still looks open but the market has actually
    // resolved on-chain, close it synthetically. Winners who never bothered
    // to redeem get their unredeemed shares valued at $1; losers' unredeemed
    // shares are worth $0 (so totalSold stays as-is, producing the expected
    // full-stake loss). Without this, losers linger in openPositions forever
    // and inflate WR, since REDEEM events only fire for winners.
    //
    // marketLookup is keyed by tokenId (asset) to match the rest of the
    // scanner. Some conditionIds have multiple tokenIds (one per outcome)
    // but a wallet's trades on a given conditionId will all be on the same
    // side, so looking up by the first trade's asset is sufficient.
    if (!positionClosed && marketLookup && totalBuySize > 0) {
      const firstTrade = mt.buys[0] || mt.sells[0];
      const asset = firstTrade?.asset;
      const info = asset ? marketLookup.get(asset) : null;
      if (info && info.marketClosed === true && info.winningOutcome) {
        const walletOutcome = String(mt.outcome || firstTrade?.outcome || '').toLowerCase().trim();
        const winningOutcome = String(info.winningOutcome).toLowerCase().trim();
        const won = walletOutcome && walletOutcome === winningOutcome;
        const unredeemedSize = Math.max(0, totalBuySize - totalSellSize);
        if (won) {
          totalSold = totalSoldRaw + unredeemedSize * 1.0;
          syntheticCloseKind = 'unredeemed_win';
          unredeemedWins++;
        } else {
          // Losing shares are worthless — no extra payout added.
          syntheticCloseKind = 'worthless_loss';
          worthlessLosses++;
        }
        positionClosed = true;
      }
    }

    if (positionClosed && totalBuySize > 0) {
      // Resolved market
      const pnl = totalSold - totalBought;
      resolvedMarkets++;
      totalPnl += pnl;

      if (pnl > 0) {
        wins++;
      } else if (pnl < 0) {
        losses++;
      }
      // pnl === 0 is break-even: counts as resolved but not win or loss

      if (isRecent) {
        recentResolved++;
        recentPnl += pnl;
        if (pnl > 0) recentWins++;
        else if (pnl < 0) recentLosses++;
      }

      marketResults.push({
        conditionId: cid,
        title: mt.title,
        outcome: pnl > 0 ? 'win' : pnl < 0 ? 'loss' : 'breakeven',
        pnl,
        avgBuyPrice,
        avgSellPrice,
        buySize: totalBuySize,
        firstBuy,
        lastTrade,
        holdTime: lastTrade - firstBuy,
        closeKind: syntheticCloseKind || 'traded',
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
  const firstTradeTs = allEvents.length > 0 ? Math.min(...allEvents.map(t => t.timestamp)) : now;
  const lastTradeTsComputed = allEvents.length > 0 ? Math.max(...allEvents.map(t => t.timestamp)) : now;
  // statsSpanDays: the actual time span covered by the events we analysed.
  // When /activity pagination truncates (tradesTruncated=true), this is the
  // effective recency window — e.g. a span of 45 days means the analyzer only
  // saw the wallet's last ~45 days of trading. Critical for interpreting
  // any stat derived from this sample.
  const statsSpanDays = Math.max(1, Math.ceil((lastTradeTsComputed - firstTradeTs) / 86400));
  const tradingSpanDays = Math.max(1, (now - firstTradeTs) / 86400);
  const tradesPerDay = allEvents.length / tradingSpanDays;
  const marketsPerDay = marketTrades.size / tradingSpanDays;

  // Recent trading frequency — use actual active span, not full window
  const recentTradeCount = recentTrades.length;
  const recentFirstTs = recentTrades.length > 0 ? Math.min(...recentTrades.map(t => t.timestamp)) : now;
  const recentSpanDays = Math.max(1, (now - recentFirstTs) / 86400);
  const recentTradesPerDay = recentTradeCount > 0 ? recentTradeCount / recentSpanDays : 0;

  // Consistency-based frequency — how many distinct weeks had trades, not just total/span.
  // A wallet that traded 150 times on one day then stopped scores low here.
  // A wallet that trades 5x/week every week scores high.
  const activeDays = new Set(allEvents.map(t => Math.floor(t.timestamp / 86400))).size;
  const activeWeeks = new Set(allEvents.map(t => Math.floor(t.timestamp / (86400 * 7)))).size;
  const totalWeeksSpan = Math.max(1, tradingSpanDays / 7);
  // What % of weeks since first trade had at least one trade
  const weeklyConsistency = +(activeWeeks / totalWeeksSpan).toFixed(3);
  // Avg trades per ACTIVE week (not per calendar week)
  const tradesPerActiveWeek = activeWeeks > 0 ? +(allEvents.length / activeWeeks).toFixed(1) : 0;
  // Avg new markets per active week
  const marketsPerActiveWeek = activeWeeks > 0 ? +(marketTrades.size / activeWeeks).toFixed(1) : 0;

  // Same for recent window
  const recentActiveDays = new Set(recentTrades.map(t => Math.floor(t.timestamp / 86400))).size;
  const recentActiveWeeks = new Set(recentTrades.map(t => Math.floor(t.timestamp / (86400 * 7)))).size;
  const recentTotalWeeks = Math.max(1, recentSpanDays / 7);
  const recentWeeklyConsistency = +(recentActiveWeeks / recentTotalWeeks).toFixed(3);
  const recentTradesPerActiveWeek = recentActiveWeeks > 0 ? +(recentTradeCount / recentActiveWeeks).toFixed(1) : 0;

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
    totalTrades: allEvents.length,
    tradesTruncated: events.truncated === true, // Data API 3000-offset cap hit
    recentTrades: recentTradeCount,
    uniqueMarkets: marketTrades.size,
    openPositions,
    tradesPerDay: +tradesPerDay.toFixed(2),
    recentTradesPerDay: +recentTradesPerDay.toFixed(2),
    marketsPerDay: +marketsPerDay.toFixed(2),
    tradingSpanDays: Math.round(tradingSpanDays),
    statsSpanDays,                      // actual days covered by the analysed sample
    unredeemedWins,                     // WR fix: winners closed via marketLookup
    worthlessLosses,                    // WR fix: losers closed via marketLookup

    // Consistency metrics — measures how regularly a wallet trades
    activeDays,                         // distinct days with at least one trade
    activeWeeks,                        // distinct weeks with at least one trade
    weeklyConsistency,                  // ratio: active weeks / total weeks since first trade (0-1)
    tradesPerActiveWeek,                // avg trades in weeks they actually traded
    marketsPerActiveWeek,               // avg new markets in weeks they actually traded
    recentActiveDays,                   // same but for 90-day window
    recentActiveWeeks,
    recentWeeklyConsistency,
    recentTradesPerActiveWeek,

    // Timing
    avgHoldTimeHours: +(avgHoldTime / 3600).toFixed(1),
    firstTradeTs,
    lastTradeTs: allEvents.length > 0 ? Math.max(...allEvents.map(t => t.timestamp)) : 0,

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
  // Falls back to all-time WR if no recent resolved markets (long-duration traders)
  const effectiveRecentWR = stats.recentResolved >= 3 ? stats.recentWinRate : stats.winRate;
  const recentSampleFactor = stats.recentResolved >= 3
    ? Math.min(1, Math.sqrt(stats.recentResolved) / 5) // plateaus at ~25 resolved
    : Math.min(0.6, Math.sqrt(stats.resolvedMarkets) / 10); // damped fallback from all-time
  const recentWrScore = Math.max(0, (effectiveRecentWR - 0.5) * 2) * recentSampleFactor * 30;

  // All-time win rate (10 pts) — long-term verification
  const allTimeSampleFactor = Math.min(1, Math.sqrt(stats.resolvedMarkets) / 8); // plateaus at ~64 resolved
  const allTimeWrScore = Math.max(0, (stats.winRate - 0.5) * 2) * allTimeSampleFactor * 10;

  // Profitability (15 pts) — log scale to avoid saturation.
  // Use effectivePnl = max(sample analyzer, Goldsky on-chain) when present —
  // credits both unredeemed winners (analyzer > Goldsky) and wallets with
  // >3000-event history beyond our sample window (Goldsky > analyzer).
  // Falls back to totalPnl for legacy entries without effectivePnl.
  // $100 PnL ≈ 5pts, $1k ≈ 10pts, $10k ≈ 13pts, $100k ≈ 15pts
  const scorePnl = (stats.effectivePnl != null ? stats.effectivePnl : stats.totalPnl) || 0;
  const pnlScore = scorePnl > 0
    ? Math.min(1, Math.log10(1 + scorePnl) / 5) * 15
    : 0;

  // Consistency (15 pts) — penalise wallets whose recent performance diverges from all-time
  // If recent WR is close to all-time WR, high consistency. Big drop = low consistency.
  const wrDiff = Math.abs(stats.recentWinRate - stats.winRate);
  const consistencyScore = stats.recentResolved >= 5
    ? Math.max(0, 1 - wrDiff * 3) * 15  // 33% WR difference = 0 pts
    : 3; // Not enough recent data — small benefit of doubt, not half marks

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
  fetchAllActivity,
  analyzeTradeHistory,
  computeWalletScore,
};
