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
 * Fetch trades on a specific market (by conditionId).
 * Powers the Stage 3 market-participant harvest — when our top alpha
 * wallets win a market, the other bidders are all interesting candidates.
 *
 * @param {string} conditionId - The market's conditionId
 * @param {object} opts - Options
 * @param {number} opts.limit - Max trades to return (default 500)
 * @param {number} opts.offset - Pagination offset
 * @returns {Promise<Array>} Array of trade objects (newest first)
 */
async function fetchMarketTrades(conditionId, opts = {}) {
  const params = {
    market: conditionId,
    limit: opts.limit || 500,
  };
  if (opts.offset) params.offset = opts.offset;
  const data = await apiRequest('/trades', params);
  return Array.isArray(data) ? data : [];
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
    // Client-side filter — Polymarket's Data API /trades endpoint silently
    // ignores startTs, so we get back up to 100 recent trades regardless of
    // the time window we asked for. For inactive wallets this can span
    // weeks, which previously produced bogus solo signals on markets that
    // had already resolved (see 2026-04-24 diagnosis: wallet 0x1f093262...
    // had April-2 trades emitted as "new" signals on April-24). Filtering
    // at the source prevents every downstream consumer from having to
    // repeat the same age check.
    const filtered = (trades || []).filter(t =>
      typeof t.timestamp === 'number' && t.timestamp >= sinceTs
    );
    if (filtered.length > 0) {
      results.set(wallet, filtered);
    }
    processed++;
    if (onProgress && processed % 50 === 0) {
      onProgress(processed, wallets.length);
    }
  }

  return results;
}

// ============================================================================
// Positions (per-wallet — replaces Goldsky cursor fallback)
// ============================================================================

/**
 * Fetch a wallet's current positions via Polymarket Data API.
 *
 * Replaces the Goldsky per-wallet fallback that consistently timed out
 * because Goldsky's subgraph has no index on the user field. The Data
 * API /positions endpoint works fine per-wallet (no indexing issue) and
 * returns up to ~500 positions per call (apparent cap on Polymarket side).
 *
 * Returned positions are adapted to the shape aggregatePositions expects:
 *   { tokenId, sharesHeld, avgPrice, realizedPnl, totalBought }
 *
 * Used in the rescore loop when walletPositions (from cursor scan) is
 * missing the wallet's address — guarantees every pool wallet gets fresh
 * decided metrics every rescore cycle, regardless of cursor coverage.
 *
 * @param {string} wallet - Wallet address
 * @returns {Promise<Array|null>} Adapted positions or null on failure
 */
async function fetchWalletPositions(wallet) {
  if (!wallet) return null;
  const params = {
    user: wallet.toLowerCase(),
    limit: 2000,  // upper-bound; API may cap lower
  };
  const data = await apiRequest('/positions', params);
  if (!Array.isArray(data)) return null;
  return data.map(p => {
    const sharesHeld = parseFloat(p.size || p.amount || 0);
    const avgPrice = parseFloat(p.avgPrice || p.entry || 0);
    return {
      tokenId: String(p.asset || p.tokenId || ''),
      sharesHeld: isFinite(sharesHeld) ? sharesHeld : 0,
      avgPrice: isFinite(avgPrice) ? avgPrice : 0,
      realizedPnl: parseFloat(p.realizedPnl || 0) || 0,
      totalBought: parseFloat(p.initialValue || (sharesHeld * avgPrice) || 0) || 0,
    };
  }).filter(p => p.tokenId);
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
// Categories the signal engine currently emits on. KEEP IN SYNC with
// signals.js SIGNAL_THRESHOLDS.ALLOWED_CATEGORIES and
// signalAttribution.js CURRENT_ALLOWED_CATEGORIES.
const EMITTABLE_CATEGORIES = new Set([
  'tennis', 'nba', 'mma', 'weather',
  'crypto-other',  // crypto-updown excluded — short windows blocked by duration
  'mlb', 'nfl', 'macro', 'ai-tech',
  'token-launch', 'news-event',
  'soccer',
  'sports-other',  // generic sports catchall — team-vs-team / player props
]);
const EMITTABLE_MIN_HOURS_TO_RESOLUTION = 4;

// Local classifier — duplicated from signals.js to avoid circular import.
function classifyMarketTitleLocal(title) {
  const q = (title || '').toLowerCase();
  if (!q) return 'other';
  if (/dota|lol|league of legends|counter-strike|valorant|cs:?go|cs2|csgo|call of duty|apex|fortnite|pubg/.test(q)) return 'esports';
  if (/\blaunch a token|\btoken launch|\btge\b/.test(q)) return 'token-launch';
  if (/bitcoin|btc|ethereum|eth|solana|sol\b|doge|xrp|crypto|coin/.test(q)) {
    if (/reach|above|below|hit|close|\$|\sup\b|\sdown\b|end above|end below/.test(q)) return 'crypto-updown';
    return 'crypto-other';
  }
  if (/nhl|stanley cup| hockey /.test(q)) return 'nhl';
  if (/nba|lakers|celtics|warriors|knicks|heat|nba playoffs/.test(q)) return 'nba';
  if (/nfl|super bowl|touchdown|quarterback/.test(q)) return 'nfl';
  if (/mlb|baseball|world series/.test(q)) return 'mlb';
  if (/epl|premier league|champions league|la liga|bundesliga|serie a|manchester|arsenal|liverpool|chelsea/.test(q)) return 'soccer';
  if (/tennis|wimbledon|us open|french open|atp|wta/.test(q)) return 'tennis';
  if (/ufc|\bmma\b|fight night/.test(q)) return 'mma';
  if (/ pga |golf|masters tournament/.test(q)) return 'golf';
  if (/f1\b|formula 1|grand prix/.test(q)) return 'f1';
  if (/trump|biden|harris|election|senate|house race|republican|democrat|congressional/.test(q)) return 'politics';
  if (/ai\b|openai|anthropic|gpt|gemini|\bllm\b|tech|apple|google|microsoft|nvidia/.test(q)) return 'ai-tech';
  if (/weather|temperature|hurricane|tornado|snow|rainfall/.test(q)) return 'weather';
  if (/fed rate|fomc|inflation|cpi|ppi|\bgdp\b|recession|jobs report|unemployment|nonfarm/.test(q)) return 'macro';
  if (/ipo|earnings|revenue|guidance|\beps\b/.test(q)) return 'macro';
  if (/spacex|starship|nasa|rocket launch|announce|statement|\bsay\b|\bsays\b|tweet|post|comment/.test(q)) return 'news-event';
  if (/\bwill .+ by |\bwill .+ before |\bwill .+ on /.test(q)) return 'news-event';

  // Generic sports catchall — team-vs-team matchups and player props.
  // Real Polymarket titles often use team/player names without explicit
  // sport keywords ("Giants vs. Phillies", "Tyrese Maxey: Points O/U 24.5",
  // "Valencia vs. Panathinaikos"). Without this, ~80% of legitimate sports
  // markets fell through to 'other' and were treated as blocked.
  if (/\bvs\.|\bvs\b/.test(q)) return 'sports-other';
  if (/o\/u|over\/under|points o\/u|rebounds|assists|moneyline|spread/.test(q)) return 'sports-other';

  return 'other';
}

function analyzeTradeHistory(events, opts = {}) {
  if (!events || events.length === 0) {
    return null;
  }

  const windowDays = opts.windowDays || 90;
  const windowTs = Math.floor(Date.now() / 1000) - (windowDays * 86400);
  const now = Math.floor(Date.now() / 1000);
  const marketLookup = opts.marketLookup instanceof Map ? opts.marketLookup : null;

  // Normalise every event into either a BUY, a SELL, or a synthetic SELL from
  // a REDEEM. MERGE events are also converted to synthetic SELLs (see below).
  // REWARD / MAKER_REBATE / SPLIT / CONVERSION are accumulated into separate
  // counters so downstream classifiers (MM detector, true-economic-PnL) can
  // see the full picture — these were previously silently dropped, which
  // caused whale-01-class wallets ($3.5M in maker rebates over 7 months) to
  // be completely invisible to scoring.
  //
  // A normalised event always carries: timestamp, conditionId, size, price,
  // side, plus market metadata when present.
  const allEvents = [];

  // ── Stage 0 additions: non-trade event accumulators ────────────────────────
  // These power (a) accurate economic PnL and (b) the MM classifier's six
  // signals (merge rate, rebate income, reward income, etc.).
  let mergeCount = 0;
  let mergeUsdcTotal = 0;
  let splitCount = 0;
  let splitUsdcTotal = 0;
  let conversionCount = 0;
  let rewardUsdcTotal = 0;
  let rebateUsdcTotal = 0;
  const mergeMarkets = new Set(); // unique conditionIds that saw ≥1 MERGE
  const splitMarkets = new Set();

  for (const ev of events) {
    if (!ev || typeof ev !== 'object') continue;
    const type = (ev.type || '').toUpperCase();
    const cid = ev.conditionId;

    // REWARD / MAKER_REBATE events don't always carry a conditionId — they're
    // protocol-level USDC distributions. Accumulate them BEFORE the cid gate.
    if (type === 'REWARD') {
      rewardUsdcTotal += parseFloat(ev.usdcSize || ev.size || 0) || 0;
      continue;
    }
    if (type === 'MAKER_REBATE' || type === 'REBATE') {
      // Some Polymarket API variants fold rebates into REWARD; others expose
      // MAKER_REBATE explicitly. Handle both so we never silently miss this.
      // This is the single biggest signal for whale-01-style market makers.
      rebateUsdcTotal += parseFloat(ev.usdcSize || ev.size || 0) || 0;
      continue;
    }

    if (!cid) continue;

    if (type === 'MERGE') {
      // MERGE: wallet combined N YES + N NO shares for N USDC. Convert to a
      // synthetic SELL at the implied per-share price so the position closes
      // out cleanly in the per-market PnL loop. Without this, MERGE-terminated
      // positions looked open forever (inflated openPositions, undercounted
      // resolvedMarkets, hid real PnL on positions that actually resolved
      // via pair merge rather than outcome redemption).
      //
      // Also track per-market counts for the MM classifier — a high merge
      // rate (>10% of markets) is one of whale-01's six MM signals.
      const size = parseFloat(ev.size || ev.shares || 0) || 0;
      const usdcSize = parseFloat(ev.usdcSize || ev.payout || 0) || 0;
      mergeCount++;
      mergeUsdcTotal += usdcSize;
      mergeMarkets.add(cid);
      if (size > 0) {
        const impliedPrice = usdcSize > 0 ? usdcSize / size : 0.5;
        allEvents.push({
          timestamp: ev.timestamp,
          conditionId: cid,
          asset: ev.asset || ev.tokenId || '',
          side: 'SELL',
          size,
          price: impliedPrice,
          title: ev.title || '',
          slug: ev.slug || '',
          eventSlug: ev.eventSlug || '',
          outcome: ev.outcome || '',
          outcomeIndex: ev.outcomeIndex,
          _fromMerge: true,
        });
      }
      continue;
    }

    if (type === 'SPLIT') {
      // SPLIT: wallet deposited N USDC, received N YES + N NO shares. Doesn't
      // directly create PnL (cost basis splits across two positions that enter
      // the TRADE stream separately) but correlates with MM arb plumbing —
      // worth counting for the classifier.
      const size = parseFloat(ev.size || 0) || 0;
      const usdcSize = parseFloat(ev.usdcSize || 0) || 0;
      splitCount++;
      splitUsdcTotal += usdcSize > 0 ? usdcSize : size; // fallback if only size
      splitMarkets.add(cid);
      continue;
    }

    if (type === 'CONVERSION') {
      // CONVERSION: wallet swapped shares of one outcome for another on a
      // multi-outcome market. Cost-basis transformation, not realised PnL.
      conversionCount++;
      continue;
    }

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
    // All non-TRADE event types are now handled above (MERGE / REDEEM into
    // the synthetic-SELL stream, REWARD / MAKER_REBATE / SPLIT / CONVERSION
    // into their respective counters). No silent drops.
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
  // categoryAlignment counters — fraction of resolved trades on
  // markets the engine would emit a signal on TODAY (passes category
  // whitelist + duration gate). 2026-04-30 fix: prevents wallets like
  // 0x022c654f4b (100% BTC up/down 5-min trader) from scoring high
  // based on stats computed across markets we never emit on.
  let totalResolvedTrades = 0;
  let emittableResolvedTrades = 0;
  const marketResults = [];
  const categories = new Map(); // category → { wins, losses, pnl }

  // MM-classifier signals: dual-side markets (wallet bought both YES + NO
  // on same conditionId) and the average sum of their per-outcome avg buy
  // prices. Market-makers typically post limit bids on both sides below $1,
  // so priceSum < 1.01 for dual-side markets is a strong MM tell.
  let dualSideMarkets = 0;
  const dualSidePriceSums = [];
  let totalBuys = 0;
  let totalSells = 0;

  // Recent window stats
  let recentResolved = 0;
  let recentWins = 0;
  let recentLosses = 0;
  let recentPnl = 0;
  // Counters so callers can tell how much of the WR fix actually fired
  let unredeemedWins = 0;
  let worthlessLosses = 0;

  // Exit-style classification — per resolved position, did the wallet hold
  // to resolution (replicable by a follower who copies the BUY) or sell
  // before resolution (a flip — the follower has no exit signal)?
  // Validated on the handpicked pool 2026-05-04: market-makers and
  // scalpers that always SELL_BEFORE produced zero useful follower signals
  // even when nominally profitable. Pool's holdRatio is gated downstream
  // in scan.js.
  const exitStyle = {
    REDEEM: 0,         // had a REDEEM event → held to resolution and claimed payout
    HOLD_TO_ZERO: 0,   // bought losing side, no sell, no redeem → held to worthlessness
    SELL_AFTER: 0,     // sold ≥95% AFTER market endDate
    SELL_BEFORE: 0,    // sold ≥95% BEFORE market endDate (flipper)
    PARTIAL: 0,        // sold some, held some
    UNRESOLVED: 0,     // genuinely still open — excluded from holdRatio
  };
  let holderCapital = 0;
  let flipperCapital = 0;

  // Directional PnL: trade PnL EXCLUDING merge-derived synthetic SELLs.
  // For follower-signal scoring this is the right number — MERGE income
  // is wallet-side arbitrage (combining YES+NO → USDC) that a follower
  // copying just the BUY cannot replicate. Same class as rewards/rebates:
  // belongs to the wallet, doesn't transfer. Used by the admission gate
  // and eviction script in scan.js. `totalPnl` keeps its current
  // include-merges meaning for dashboard / legacy consumers.
  let directionalPnl = 0;

  for (const [cid, mt] of marketTrades) {
    const totalBought = mt.buys.reduce((sum, t) => sum + (t.size * t.price), 0);
    const totalSoldRaw = mt.sells.reduce((sum, t) => sum + (t.size * t.price), 0);
    const totalBuySize = mt.buys.reduce((sum, t) => sum + t.size, 0);
    const totalSellSize = mt.sells.reduce((sum, t) => sum + t.size, 0);
    const avgBuyPrice = totalBuySize > 0 ? totalBought / totalBuySize : 0;
    const avgSellPrice = totalSellSize > 0 ? totalSoldRaw / totalSellSize : 0;

    // Aggregate buy/sell counts + detect dual-side markets (wallet bought
    // both YES and NO on same conditionId). Core MM-classifier inputs.
    totalBuys += mt.buys.length;
    totalSells += mt.sells.length;
    const buyOutcomes = new Map(); // outcome → { cost, size }
    for (const b of mt.buys) {
      const o = String(b.outcome || '').trim().toLowerCase();
      if (!o) continue;
      if (!buyOutcomes.has(o)) buyOutcomes.set(o, { cost: 0, size: 0 });
      const entry = buyOutcomes.get(o);
      entry.cost += b.size * b.price;
      entry.size += b.size;
    }
    const isSingleSide = buyOutcomes.size === 1;
    if (buyOutcomes.size >= 2) {
      dualSideMarkets++;
      // Sum the two outcomes' capital-weighted avg buy prices. On a 2-outcome
      // market, (price_yes + price_no) < 1.0 is a free-arbitrage MM play.
      const avgPrices = [];
      for (const { cost, size } of buyOutcomes.values()) {
        if (size > 0) avgPrices.push(cost / size);
      }
      avgPrices.sort((a, b) => b - a); // top two by price
      if (avgPrices.length >= 2) {
        dualSidePriceSums.push(avgPrices[0] + avgPrices[1]);
      }
    }

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

      // Directional PnL: subtract any MERGE-derived "revenue" from this
      // market's totalSold before computing the follower-replicable PnL.
      // A MERGE pairs N YES + N NO shares back into N USDC — closes the
      // position from the wallet's perspective but isn't a directional
      // outcome a follower could copy.
      const mergeRevenueOnThisMarket = mt.sells
        .filter(s => s._fromMerge)
        .reduce((sum, s) => sum + s.size * s.price, 0);
      directionalPnl += pnl - mergeRevenueOnThisMarket;

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
        isSingleSide,                     // Stage 2: for edge_pp calculation
        buyOutcomeCount: buyOutcomes.size,
      });

      // Category tracking — eventSlug-prefix bucket (kept for diagnostic)
      const cat = mt.eventSlug?.split('-')[0] || 'unknown';
      if (!categories.has(cat)) categories.set(cat, { wins: 0, losses: 0, pnl: 0 });
      const catStats = categories.get(cat);
      catStats.pnl += pnl;
      if (pnl > 0) catStats.wins++;
      else catStats.losses++;

      // Currently-emittable check — used to compute categoryAlignment.
      // The wallet's TRADING UNIVERSE matters: a wallet that trades 100%
      // in markets we'd never emit on (e.g. BTC up/down 5-min) shouldn't
      // be scored as if their stats were emit-relevant. categoryAlignment
      // is the fraction of resolved trades on markets that pass today's
      // category whitelist + duration gate. Used as a multiplier in
      // computeWalletScore to prevent stale-regime wallets from ranking.
      const titleStr = mt.title || '';
      const titleCategory = classifyMarketTitleLocal(titleStr);
      const isAllowedCat = EMITTABLE_CATEGORIES.has(titleCategory);
      // Duration gate: time from first buy to last sell. mt has buys/sells
      // arrays but no aggregate timestamp fields, so derive from the trades.
      // Single-event positions (one buy, no sell) we can't measure → admit.
      const allMtEvents = [...mt.buys, ...mt.sells];
      let passesDuration = true;
      if (allMtEvents.length >= 2) {
        const tsList = allMtEvents.map(e => e.timestamp).filter(t => t > 0);
        if (tsList.length >= 2) {
          const holdHours = (Math.max(...tsList) - Math.min(...tsList)) / 3600;
          passesDuration = holdHours >= EMITTABLE_MIN_HOURS_TO_RESOLUTION;
        }
      }
      if (isAllowedCat && passesDuration) {
        emittableResolvedTrades++;
      }
      totalResolvedTrades++;
    } else {
      openPositions++;
    }

    // ── Exit-style classification ────────────────────────────────────
    // Inspect REAL sells separately from redeem-derived synthetic sells,
    // because a wallet that redeemed is a holder regardless of the
    // synthetic-SELL bookkeeping. We need this distinction here even
    // though the PnL math doesn't.
    {
      if (totalBuySize > 0) {
        const realSells = mt.sells.filter(s => !s._fromRedeem);
        const realSellSize = realSells.reduce((sum, t) => sum + t.size, 0);
        const realSoldFraction = realSellSize / totalBuySize;
        const lastRealSellTs = realSells.length
          ? Math.max(...realSells.map(t => t.timestamp || 0))
          : 0;
        const hasRedeem = mt.sells.some(s => s._fromRedeem);

        // Pull endTs from marketLookup if available (any of the
        // wallet's trades on this market shares the same conditionId,
        // and marketLookup is keyed by tokenId — first asset suffices).
        const firstTradeAsset = (mt.buys[0] || mt.sells[0])?.asset;
        const mInfo = firstTradeAsset && marketLookup ? marketLookup.get(firstTradeAsset) : null;
        const endIso = mInfo?.endDate || mInfo?.end_date_iso || null;
        const endTs = endIso ? new Date(endIso).getTime() / 1000 : 0;
        const marketEnded = mInfo?.marketClosed === true || (endTs > 0 && endTs < (Date.now() / 1000));

        let bucket = null;
        if (hasRedeem) {
          bucket = 'REDEEM';
          holderCapital += totalBought;
        } else if (realSoldFraction >= 0.95) {
          if (endTs > 0 && lastRealSellTs > endTs) {
            bucket = 'SELL_AFTER';
            holderCapital += totalBought;
          } else {
            // Sold out before resolution OR endTs unknown → treat as flip.
            // The unknown-endTs case is conservative on purpose: a wallet
            // that fully exits markets we can't even date is exactly the
            // pattern we want to filter (market-makers / scalpers).
            bucket = 'SELL_BEFORE';
            flipperCapital += totalBought;
          }
        } else if (realSoldFraction >= 0.05) {
          bucket = 'PARTIAL';
          holderCapital += totalBought * (1 - realSoldFraction);
          flipperCapital += totalBought * realSoldFraction;
        } else if (marketEnded) {
          // No real sells, no redeem, market has ended → losing shares
          // held to zero. Still a holder.
          bucket = 'HOLD_TO_ZERO';
          holderCapital += totalBought;
        } else {
          bucket = 'UNRESOLVED';
        }
        exitStyle[bucket]++;
      }
    }
  }

  // categoryAlignment: fraction of resolved trades on markets the engine
  // would emit a signal on TODAY (passes category whitelist + duration gate).
  // Multiplier in computeWalletScore — wallets at 0 alignment get score 0,
  // wallets at 1.0 alignment unaffected.
  const categoryAlignment = totalResolvedTrades > 0
    ? emittableResolvedTrades / totalResolvedTrades
    : 0;

  // holdRatio: fraction of CLASSIFIED positions the wallet held to
  // resolution (REDEEM + HOLD_TO_ZERO + SELL_AFTER + the held portion of
  // PARTIALs) vs flipped early. Markets in UNRESOLVED state are excluded
  // from the denominator — we don't penalise wallets for genuinely still
  // holding active positions. Used by the discovery gate in scan.js.
  // PARTIAL counts as half-flip in the count-weighted ratio.
  const holderCount = exitStyle.REDEEM + exitStyle.HOLD_TO_ZERO + exitStyle.SELL_AFTER + (exitStyle.PARTIAL * 0.5);
  const flipperCount = exitStyle.SELL_BEFORE + (exitStyle.PARTIAL * 0.5);
  const classifiedCount = holderCount + flipperCount;
  const holdRatio = classifiedCount > 0 ? holderCount / classifiedCount : null;
  const totalClassifiedCapital = holderCapital + flipperCapital;
  const holdRatioCapital = totalClassifiedCapital > 0
    ? holderCapital / totalClassifiedCapital
    : null;

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

  // Edge ratio: DOLLAR-based average win / loss (legacy — retained for dashboard
  // and backcompat). See note below: prefer `roiEdgeRatio` for scoring.
  const avgWin = wins > 0
    ? marketResults.filter(r => r.outcome === 'win').reduce((s, r) => s + r.pnl, 0) / wins
    : 0;
  const avgLoss = losses > 0
    ? Math.abs(marketResults.filter(r => r.outcome === 'loss').reduce((s, r) => s + r.pnl, 0) / losses)
    : 0; // no losses yet — avoid bogus $1 fallback that produced huge ratios
  const edgeRatio = avgLoss > 0 ? avgWin / avgLoss : null;

  // ROI-based per-trade metrics. Each resolved market has a known dollar cost
  // (avgBuyPrice * buySize) and known $ PnL, so ROI% = pnl / cost. This lets us
  // distinguish wallets that grind 5% wins on 95¢ scraps (roiEdge ≈ 0.05) from
  // wallets that buy 30¢ underdogs and collect 233% on hits (roiEdge ≈ 2.33).
  // NOTE: buySize is SHARES, not $. Dollar cost is avgBuyPrice * buySize.
  const resolvedWithSize = marketResults.filter(r => r.buySize > 0 && r.avgBuyPrice > 0);
  const marketCost = (r) => r.avgBuyPrice * r.buySize;
  const totalEntryCapital = resolvedWithSize.reduce((s, r) => s + marketCost(r), 0);
  // Capital-weighted avg entry price — reflects where the wallet actually deploys $.
  const avgEntryPrice = totalEntryCapital > 0
    ? resolvedWithSize.reduce((s, r) => s + (r.avgBuyPrice * marketCost(r)), 0) / totalEntryCapital
    : 0;
  const tradeRois = resolvedWithSize.map(r => r.pnl / marketCost(r));
  const avgTradeRoi = tradeRois.length > 0
    ? tradeRois.reduce((s, x) => s + x, 0) / tradeRois.length
    : 0;
  const winRois = resolvedWithSize.filter(r => r.outcome === 'win').map(r => r.pnl / marketCost(r));
  const lossRois = resolvedWithSize.filter(r => r.outcome === 'loss').map(r => Math.abs(r.pnl) / marketCost(r));
  const avgWinRoi = winRois.length > 0 ? winRois.reduce((s, x) => s + x, 0) / winRois.length : 0;
  const avgLossRoi = lossRois.length > 0 ? lossRois.reduce((s, x) => s + x, 0) / lossRois.length : 0;
  // roiEdgeRatio: avg %return on wins / avg %return on losses. Losses on prediction
  // markets are always close to 100% (shares go to $0), so this ratio is roughly
  // equivalent to avgWinRoi — but keeping the ratio form normalises for markets
  // where a loss might be partial (rare). Null when insufficient data to compute.
  const roiEdgeRatio = (avgLossRoi > 0 && winRois.length > 0)
    ? avgWinRoi / avgLossRoi
    : null;

  // ── Stage 2: Single-side alpha test (edge_pp) ─────────────────────────────
  // Measures whether the wallet beat the market's implied probability at
  // entry. Filters to markets where the wallet bought only ONE outcome
  // (single-side). Compares realized hit rate against capital-weighted
  // average entry price. Mean-pickers who buy $0.95 and win 95% score
  // edge_pp ≈ 0 (no edge, market was right). Insight-driven bettors who
  // buy $0.40 and win 55% score edge_pp ≈ +15pp (real alpha).
  //
  // This complements decidedROI: decidedROI answers "did they make money",
  // edge_pp answers "did they outperform the market's pricing". A wallet
  // needs both to qualify as a Tier A directional alpha.
  const singleSideResolved = marketResults.filter(
    r => r.isSingleSide && (r.outcome === 'win' || r.outcome === 'loss')
  );
  let edgePP = null;
  let singleSideHitRate = null;
  let singleSideAvgEntry = null;
  let singleSideWins = 0;
  let singleSideLosses = 0;
  let singleSidePnl = 0;
  let singleSideCapital = 0;
  if (singleSideResolved.length > 0) {
    singleSideWins = singleSideResolved.filter(r => r.outcome === 'win').length;
    singleSideLosses = singleSideResolved.length - singleSideWins;
    singleSidePnl = singleSideResolved.reduce((s, r) => s + r.pnl, 0);
    const capitalByMarket = singleSideResolved.map(r => r.avgBuyPrice * r.buySize);
    singleSideCapital = capitalByMarket.reduce((s, c) => s + c, 0);
    // Capital-weighted average entry price — what implied probability did
    // this wallet's $ actually assume, across all single-side bets?
    if (singleSideCapital > 0) {
      const weightedEntrySum = singleSideResolved.reduce((s, r, i) =>
        s + r.avgBuyPrice * capitalByMarket[i], 0);
      singleSideAvgEntry = weightedEntrySum / singleSideCapital;
    }
    singleSideHitRate = singleSideWins / singleSideResolved.length;
    // edge_pp in percentage points (e.g. +3.5pp, -2.1pp)
    if (singleSideAvgEntry != null) {
      edgePP = +(100 * (singleSideHitRate - singleSideAvgEntry)).toFixed(2);
    }
  }

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
    edgeRatio: edgeRatio != null ? +edgeRatio.toFixed(2) : null,
    // ROI-based per-trade economics (preferred over edgeRatio for scoring)
    avgEntryPrice: +avgEntryPrice.toFixed(4),
    avgTradeRoi: +avgTradeRoi.toFixed(4),
    avgWinRoi: +avgWinRoi.toFixed(4),
    avgLossRoi: +avgLossRoi.toFixed(4),
    roiEdgeRatio: roiEdgeRatio != null ? +roiEdgeRatio.toFixed(3) : null,

    // Activity
    totalTrades: allEvents.length,
    tradesTruncated: events.truncated === true, // Data API 3000-offset cap hit
    recentTrades: recentTradeCount,
    uniqueMarkets: marketTrades.size,
    openPositions,
    // Fraction of resolved trades on currently-emittable markets
    // (passes category whitelist + duration gate). Used as multiplier
    // in computeWalletScore to prevent stale-regime bots scoring high.
    categoryAlignment: +categoryAlignment.toFixed(3),
    emittableResolvedTrades,
    totalResolvedTrades,
    tradesPerDay: +tradesPerDay.toFixed(2),
    recentTradesPerDay: +recentTradesPerDay.toFixed(2),
    marketsPerDay: +marketsPerDay.toFixed(2),
    tradingSpanDays: Math.round(tradingSpanDays),
    statsSpanDays,                      // actual days covered by the analysed sample
    unredeemedWins,                     // WR fix: winners closed via marketLookup

    // Exit-style classification — used by holdRatio gate in scan.js
    exitStyle,                          // { REDEEM, HOLD_TO_ZERO, SELL_AFTER, SELL_BEFORE, PARTIAL, UNRESOLVED }
    holdRatio,                          // count-weighted: holders / classified
    holdRatioCapital,                   // capital-weighted: holder $ / classified $
    classifiedPositions: classifiedCount,

    // Directional PnL — totalPnl with MERGE-derived synthetic SELL revenue
    // subtracted out. The right input for follower-signal admission gates
    // and eviction scripts. MERGE-only "profitable" wallets (MM-style
    // arbitrageurs) score negatively here, which matches what they
    // contribute to a follower copying their BUYs.
    directionalPnl: +directionalPnl.toFixed(2),
    worthlessLosses,                    // WR fix: losers closed via marketLookup

    // ── Event-type breakdown (Stage 0 — MM classifier + true economic PnL) ──
    // These expose non-trade income streams that were previously silently
    // dropped. The MM classifier consumes `mergeRate`, `rebateUsdcTotal`, and
    // `rewardUsdcTotal` as three of its six signals. `economicPnl` replaces
    // naive `totalPnl` as the input to `effectivePnl` downstream.
    mergeCount,
    mergeMarkets: mergeMarkets.size,
    mergeUsdcTotal: +mergeUsdcTotal.toFixed(2),
    mergeRate: marketTrades.size > 0 ? +(mergeCount / marketTrades.size).toFixed(3) : 0,
    splitCount,
    splitMarkets: splitMarkets.size,
    splitUsdcTotal: +splitUsdcTotal.toFixed(2),
    conversionCount,
    rewardUsdcTotal: +rewardUsdcTotal.toFixed(2),
    rebateUsdcTotal: +rebateUsdcTotal.toFixed(2),
    // nonDirectionalIncome: USDC income received without holding directional
    // risk (LP rewards + maker rebates). Whale-01 earned ~$3.5M here over 7
    // months with near-zero directional PnL — the single biggest tell for
    // non-copyable MM wallets.
    nonDirectionalIncome: +(rewardUsdcTotal + rebateUsdcTotal).toFixed(2),
    // economicPnl: total wallet income visible from /activity.
    //   trade PnL (totalPnl — already includes MERGE closures via synthetic SELL)
    //   + non-directional income (rewards + rebates)
    // Use this, not totalPnl, for effectivePnl. Leaves totalPnl untouched so
    // any dashboard/legacy consumer that read the old meaning still works.
    economicPnl: +(totalPnl + rewardUsdcTotal + rebateUsdcTotal).toFixed(2),

    // Buy/sell balance — MM wallets have near-equal B:S counts (closes via
    // MERGE or opposing trades), directional wallets buy much more than sell.
    totalBuys,
    totalSells,
    sellRatio: totalBuys > 0 ? +(totalSells / totalBuys).toFixed(3) : null,

    // Dual-side markets — wallet bought both YES and NO on same conditionId.
    // High rate (>40%) + low price sum (<1.01) is the MM arb signature.
    dualSideMarkets,
    dualSideRate: marketTrades.size > 0 ? +(dualSideMarkets / marketTrades.size).toFixed(3) : 0,
    avgDualSidePriceSum: dualSidePriceSums.length > 0
      ? +(dualSidePriceSums.reduce((a, b) => a + b, 0) / dualSidePriceSums.length).toFixed(4)
      : null,

    // ── Stage 2: Single-side directional alpha test ─────────────────────────
    // edge_pp: percentage-point outperformance vs market's implied probability.
    // Positive = real alpha (wallet predicted better than prices suggested).
    // Zero/negative = mean-picker or worse. Required ≥ 3pp for Tier A.
    singleSideResolved: singleSideResolved.length,
    singleSideWins,
    singleSideLosses,
    singleSideHitRate: singleSideHitRate != null ? +singleSideHitRate.toFixed(4) : null,
    singleSideAvgEntry: singleSideAvgEntry != null ? +singleSideAvgEntry.toFixed(4) : null,
    singleSideCapital: +singleSideCapital.toFixed(2),
    singleSidePnl: +singleSidePnl.toFixed(2),
    singleSideROI: singleSideCapital > 0 ? +(singleSidePnl / singleSideCapital).toFixed(4) : null,
    edgePP, // percentage points. null if no single-side resolved sample.

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
 * Compute a wallet's quality score — SINGLE authoritative scoring function.
 *
 * Keys on decided-truth data (decidedROI + decidedCapital, supplied by
 * positionLedger.aggregatePositions) with a multi-stage penalty pipeline:
 *   - decidedROI    → roi points (exponential saturation to 50 pts at high ROI)
 *   - decidedCapital→ capConf multiplier (sqrt-scaled; $50k → 1.0)
 *   - resolvedMarkets → sampleConf multiplier (linear to 25 resolved)
 *   - lastTradeTs   → recency multiplier (1.0 ≤ 7d, linear decay to 0 at 30d)
 *   - mean-picker   → 0.2× penalty if WR ≥ 95% + decidedROI < 5% + $50k+ cap
 *   - MM classifier → 0.0–0.5× penalty (set by attachMMClassification)
 *   - activity bonus→ additive 0–5 pts on log-scaled trades/day
 *
 * Return shape: { score, reason, components }.
 *   reason: 'ok' | 'no_stats' | 'no_decided_metrics'
 *   score: null if reason !== 'ok' (caller must handle — usually treat as unranked)
 *
 * Callers MUST run attachMMClassification(stats) and attachAlphaEvaluation(stats)
 * BEFORE calling this so stats.mmPenalty + stats.alphaVerdict are populated.
 *
 * Evolution: this replaces a legacy win-rate-weighted formula that had
 * Spearman(score, decidedROI) = -0.152 (inverted on ground truth). Current
 * formula hits ~0.618 on the live pool.
 */
function roiPoints(decidedROI) {
  // 0 at or below 0% ROI; ~15pts at 10%; ~30pts at 25%; ~42pts at 50%;
  // saturates toward 50pts as ROI → ∞. Formula: 50 * (1 - e^(-roi*3)).
  if (decidedROI == null || !isFinite(decidedROI) || decidedROI <= 0) return 0;
  const pts = 50 * (1 - Math.exp(-decidedROI * 3));
  return Math.min(50, pts);
}

function capConfidence(decidedCapital) {
  if (!decidedCapital || decidedCapital <= 0) return 0;
  // sqrt scaling: $5k → 0.32, $20k → 0.63, $50k → 1.0, capped
  return Math.min(1, Math.sqrt(decidedCapital / 50000));
}

function sampleConfidence(resolvedMarkets) {
  if (!resolvedMarkets || resolvedMarkets <= 0) return 0;
  // Linear 0→1 over 0→25 resolved markets
  return Math.min(1, resolvedMarkets / 25);
}

function recencyMultiplier(lastTradeTs) {
  if (!lastTradeTs || lastTradeTs <= 0) return 0;
  const days = (Date.now() / 1000 - lastTradeTs) / 86400;
  if (days <= 7) return 1.0;
  if (days >= 30) return 0;
  return 1 - (days - 7) / 23;
}

// V2 SCORING — signal-EV-relevance composite (2026-04-30 redesign)
//
// Previous formula made decidedROI the magnitude term:
//   roi = roiPoints(decidedROI) ∈ [0, 50]
//   score = roi × multipliers + activityBonus
// The 2026-04-30 wallet-scoring-validity audit measured this against
// the ground truth — wallets with ≥2 emitted signals and a known
// signal-EV. Result over n=32 calibration wallets: r = -0.222. The score
// was negatively correlated with what we want to predict.
//
// Trading ROI measures "did this wallet make money on their own trades?"
// That includes entry, exit timing, sizing, hedging, holding through
// drawdown. Followers can only replicate ENTRY. The other components
// are usually where the alpha lives, so trade-ROI doesn't predict
// signal-emission ROI.
//
// V2 replaces the magnitude with a composite of metrics that DO correlate
// with signal-EV per the audit:
//   avgEntryPrice    r = +0.675   ← strongest predictor
//   resolvedMarkets  r = +0.461   (sample size)
//   sellRatio        r = +0.387   (wallets that close, not hold)
//
// Plus existing safety multipliers (recency, mmPenalty), a style
// multiplier (snipers/averagers full weight, holders/mm zero), and the
// attribution multiplier with widened band — once a wallet has 2+
// emitted signals, its actual measured signal-EV dominates the score.
//
// Variant V4 (best of 8 tested) achieves r = +0.601 vs current r = -0.222.
// See scripts/rescore-pool-v2.mjs for the one-time pool sweep.
const SCORING_V2 = {
  // Favourite-buyer band — wallets buying at higher prices produce
  // signals followers can replicate (the resolution is closer to settled).
  favLo: 0.30, favLoMid: 0.55, favHi: 0.85,
  favBonusLow: 0,      // 0.30-0.55: underdog buyer, no bonus
  favBonusMid: 15,     // 0.55-0.85: mid favorite, mid bonus
  favBonusHigh: 30,    // ≥0.85: heavy favorite, full bonus
  // Sample-size confidence — log-shaped, saturating
  sampleSat: 50, sampleBase: 10, sampleSatHigh: 150, sampleExtra: 5,
  // Sell-ratio bonus — wallets that close positions (not pure holders)
  sellThreshold: 0.20, sellBonus: 5,
  // Attribution band: ≥5 sigs = meaningful signal, [0.0, 2.5] band,
  // sensitivity 3 (so -33% avg → mul 0; +50% avg → mul 2.5).
  // Raised from 2 to 5 after the 2026-04-30 probe found the #1 wallet
  // had only 2 attribution signals — both on BTC-updown 5-min markets
  // we no longer emit on — yet was getting the max 2.5× boost. Min 5
  // requires more emittable signals to accumulate before attribution
  // dominates. Combined with buildAttributionMap's filter to only
  // currently-emittable signals, prevents stale-regime credit.
  attrMin: 5, attrFloor: 0.0, attrCap: 2.5, attrSensitivity: 3,
  // Style multipliers — snipers/averagers replicate well; holders/MM don't
  styles: { sniper: 1.0, averager: 1.0, mixed: 0.7, churner: 0.5, holder: 0.0, 'mm-like': 0.0 },
};

// Style classifier — kept in sync with signals.js classifyWalletStyle.
// Defined inline here to avoid circular import.
function classifyWalletStyleLocal(s) {
  if (!s) return 'unknown';
  if ((s.dualSideRate || 0) > 0.30 || (s.mmScore || 0) >= 3) return 'mm-like';
  const tt = s.totalTrades || 0, um = s.uniqueMarkets || 0;
  const tpm = um > 0 ? tt / um : 0;
  const sr = s.sellRatio ?? 1, hold = s.avgHoldTimeHours || 0;
  if (tpm > 8) return 'churner';
  if (tpm >= 3 && sr > 0.30) return 'averager';
  if (tpm <= 2 && hold < 48) return 'sniper';
  if (sr < 0.15) return 'holder';
  return 'mixed';
}

function computeWalletScore(stats) {
  if (!stats) return { score: null, reason: 'no_stats', components: null };

  // Hard reject mean-picker shape — the historical decidedROI-based
  // formula soft-penalised these (multiplier 0.2). V2 is stricter:
  // mean-pickers can't produce copyable directional signals by definition.
  if (stats.isMeanPickerShape === true) {
    return { score: 0, reason: 'mean_picker', components: { hardReject: 'mean_picker', scoringVersion: 'v2' } };
  }

  // Magnitude — composite of signal-EV predictors
  const aep = stats.avgEntryPrice ?? 0;
  let favoriteBonus = 0;
  if (aep >= SCORING_V2.favLo && aep < SCORING_V2.favLoMid) favoriteBonus = SCORING_V2.favBonusLow;
  else if (aep >= SCORING_V2.favLoMid && aep < SCORING_V2.favHi) favoriteBonus = SCORING_V2.favBonusMid;
  else if (aep >= SCORING_V2.favHi) favoriteBonus = SCORING_V2.favBonusHigh;

  const resolved = stats.decidedWins != null && stats.decidedLosses != null
    ? stats.decidedWins + stats.decidedLosses
    : (stats.singleSideResolved || stats.resolvedMarkets || 0);

  let sampleBonus;
  if (resolved < SCORING_V2.sampleSat) {
    sampleBonus = (resolved / SCORING_V2.sampleSat) * SCORING_V2.sampleBase;
  } else {
    sampleBonus = SCORING_V2.sampleBase + Math.min(
      SCORING_V2.sampleExtra,
      ((resolved - SCORING_V2.sampleSat) / SCORING_V2.sampleSatHigh) * SCORING_V2.sampleExtra,
    );
  }

  const sellRatio = stats.sellRatio ?? 0;
  const sellRatioBonus = sellRatio >= SCORING_V2.sellThreshold ? SCORING_V2.sellBonus : 0;

  const magnitude = favoriteBonus + sampleBonus + sellRatioBonus;

  // Attribution multiplier — direct measurement of signal EV when sample exists
  const attr = stats.signalAttribution;
  const attrMultiplier = (attr && attr.signals >= SCORING_V2.attrMin && typeof attr.avgReturn === 'number')
    ? Math.max(SCORING_V2.attrFloor, Math.min(SCORING_V2.attrCap, 1 + attr.avgReturn * SCORING_V2.attrSensitivity))
    : 1.0;

  const recency = recencyMultiplier(stats.lastTradeTs);
  const mmPenalty = (typeof stats.mmPenalty === 'number') ? stats.mmPenalty : 1.0;
  const style = classifyWalletStyleLocal(stats);
  const styleMul = SCORING_V2.styles[style] ?? 0.5;

  // categoryAlignment — fraction of resolved trades on currently-emittable
  // markets. The 2026-04-30 audit found wallets like 0x022c654f4b scoring
  // 67.5 despite trading 100% in BTC up/down 5-min markets we never emit
  // on. Their pool-wide stats (avgEntryPrice, sellRatio, style) all
  // looked legitimate but every input was from non-emittable markets.
  // Multiplying by alignment makes a 0%-aligned BTC bot score 0,
  // a 100%-aligned wallet unaffected, intermediate wallets penalised
  // proportionally. Default 1.0 only for backwards compat with stats
  // computed before this field existed (will be 0.0 once re-analyzed).
  const categoryAlignment = (typeof stats.categoryAlignment === 'number')
    ? stats.categoryAlignment : 1.0;

  const total = magnitude * attrMultiplier * recency * mmPenalty * styleMul * categoryAlignment;

  // Backwards-compat: report decidedROI source for diagnostic display,
  // even though it no longer drives the score.
  let metricSource = 'v2';
  let roiInput = null, capInput = null;
  if (stats.decidedROI != null && stats.decidedCapital != null) {
    metricSource = 'v2_decided';
    roiInput = stats.decidedROI;
    capInput = stats.decidedCapital;
  } else if (stats.singleSideROI != null && stats.singleSideCapital != null) {
    metricSource = 'v2_singleside';
    roiInput = stats.singleSideROI;
    capInput = stats.singleSideCapital;
  }

  return {
    score: Math.min(100, Math.round(total * 10) / 10),
    reason: 'ok',
    components: {
      // V2 fields — what actually drives the score
      scoringVersion: 'v2',
      magnitude: +magnitude.toFixed(2),
      favoriteBonus,
      sampleBonus: +sampleBonus.toFixed(2),
      sellRatioBonus,
      attrMultiplier: +attrMultiplier.toFixed(3),
      attrSignals: attr ? attr.signals : 0,
      attrAvgReturn: attr ? attr.avgReturn : null,
      recency: +recency.toFixed(3),
      mmPenalty,
      mmScore: stats.mmScore ?? null,
      style,
      styleMul,
      categoryAlignment: +categoryAlignment.toFixed(3),
      emittableResolvedTrades: stats.emittableResolvedTrades ?? null,
      avgEntryPrice: stats.avgEntryPrice != null ? +stats.avgEntryPrice.toFixed(4) : null,
      resolved,
      sellRatio: stats.sellRatio != null ? +stats.sellRatio.toFixed(3) : null,
      // Backwards-compat fields kept so dashboards / scripts that read
      // these keys don't break. roi/capConf/sampleConf are no longer
      // inputs to the score.
      roi: 0, capConf: 1.0, sampleConf: 1.0,
      meanPickerPenalty: stats.isMeanPickerShape === true ? 0 : 1,
      churnPenalty: 1.0,
      activityBonus: 0,
      churnRatio: null,
      metricSource,
      roiInput: roiInput != null ? +roiInput.toFixed(4) : null,
      capInput: capInput != null ? Math.round(capInput) : null,
    },
  };
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
  fetchMarketTrades,
  fetchWalletPositions,
  analyzeTradeHistory,
  computeWalletScore,
};
