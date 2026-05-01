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
  refreshSignalMarkets,
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
  fetchAllActivity,
  fetchRecentTrades,
  fetchWalletPositions,
  analyzeTradeHistory,
  computeWalletScore,
} from './dataApi.js';

import {
  SIGNAL_THRESHOLDS,
  detectConvergence,
  processSignals,
} from './signals.js';

import { aggregatePositions } from './positionLedger.js';
import { attachMMClassification } from './mmClassifier.js';
import { attachAlphaEvaluation, ALPHA_THRESHOLDS } from './alphaTest.js';
import { buildAttributionMap, attachAttribution } from './signalAttribution.js';
import { processHandpickedSignals } from './handpickedSignals.js';

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.resolve(__dirname, '../data');

// ============================================================================
// Configuration
// ============================================================================

const CONFIG = {
  // ── Pool sizing & admission ────────────────────────────────────────────
  TARGET_POOL_SIZE: 1000,          // Top N wallets kept after ranking
  MAX_DISCOVERY_WALLETS: 5000,     // Max candidates fetched per discovery cycle
  RESCORE_BATCH_SIZE: 100,         // Wallets rescored per discovery cycle
  DISCOVERY_INTERVAL_SCANS: 3,     // Run full discovery every N scans

  // Score floor for pool admission. Score scale is practical 0–55; a top
  // decile wallet scores ≥ 25. Pool floor of 5 keeps only wallets that
  // pass the basic "has decided-ROI data + not an MM + not a mean-picker".
  MIN_SCORE_POOL: 5,

  // ── Discovery gates (new wallets, cheap first-look rejects) ─────────────
  // Wallets failing any of these never enter the pool. Mirrors the rescore
  // eviction logic so we don't admit-then-immediately-evict.
  MIN_PNL_DISCOVERY: 500,              // Skip Goldsky wallets below this PnL floor
  MIN_POSITIONS_DISCOVERY: 10,         // Goldsky position count minimum
  MIN_RESOLVED_MARKETS: 20,            // Resolved markets minimum (statistical confidence)
  DISCOVERY_MIN_DECIDED_CAPITAL: 5000, // $5k+ risked on resolved plays
  DISCOVERY_MIN_DECIDED_ROI: 0.10,     // 10%+ ROI on decided capital
  DISCOVERY_MAX_WIN_RATE: 0.98,        // Obvious mean-picker shape (99%+ WR)
  DISCOVERY_MAX_WIN_RATE_MIN_RESOLVED: 25, // Only apply WR cap when sample meaningful
  // Max capital-weighted avg entry price. 0.85 ⇒ wallet's typical trade must
  // have ≥17.6% implied max ROI. Keeps the pool aligned with the signal
  // engine's MIN_OPEN_ROI — scrap-graders can't ride WR into the pool just
  // to produce signals we'd filter out anyway. 0 or 1 = disabled.
  MAX_WALLET_AVG_ENTRY_PRICE: 0.85,

  // ── Per-wallet measurement ──────────────────────────────────────────────
  // Requires Goldsky per-position fetch to compute decidedROI/decidedCapital.
  // Flip off only for debugging — scorer returns null without this data.
  ENABLE_DECIDED_METRICS: true,

  // ── Dormancy / activity ─────────────────────────────────────────────────
  // Single unified dormancy cutoff. A wallet that hasn't traded in this
  // many days is evicted from the pool. Applied at both discovery-gate
  // and rescore-eviction paths.
  DORMANCY_DAYS: 30,

  // ── Eviction rules ──────────────────────────────────────────────────────
  //   'off'    — do nothing
  //   'shadow' — log what would be evicted, don't remove
  //   'live'   — actually remove matching wallets
  // Mean-picker / low-score use strike counters so one fluky rescore can't
  // evict a real wallet. Dormancy and neg-ROI-with-capital are single-shot.
  EVICTION_MODE: 'live',
  MEAN_PICKER_STRIKES_TO_EVICT: 3,
  LOW_SCORE_THRESHOLD: 5,              // on 0–55 scale — score below this is an eviction strike
  LOW_SCORE_STRIKES_TO_EVICT: 3,
  NEG_ROI_CAPITAL_FLOOR: 10000,        // decidedCapital ≥ $10k + ROI<0 = evict
  NEG_ROI_MIN_RESOLVED: 25,            // AND resolved ≥ 25 markets

  // Low-ROI eviction — positive but chronically weak. Complements the
  // neg-ROI rule above. A wallet sitting at 2-4% ROI on meaningful capital
  // is a mean-picker in all but name — low directional edge, just above
  // breakeven. Can't source 75%-hit-rate signals.
  LOW_ROI_THRESHOLD: 0.10,             // 10% ROI floor — matches discovery gate, no drift gap
  LOW_ROI_MIN_CAPITAL: 3000,           // only if they've deployed meaningful $
  LOW_ROI_MIN_RESOLVED: 20,            // only if sample is trustworthy

  // Bot-pattern discovery gate — reject candidates whose recent activity
  // looks like a market-making bot we can't act on:
  //   - >70% of recent buys on crypto-updown markets (5-15min resolution),
  //     OR
  //   - median buy size < $50 (algorithmic micro-bets we can't follow)
  // Without this gate, scripts/evict-bot-pollution sweeps recur weekly
  // because new bot wallets keep getting admitted via discovery.
  // Diagnosed via initial pool audit (2026-04-28): 124 of 988 active
  // wallets (12.6%) matched bot-pattern criteria with zero signal
  // contributions — pool slots wasted.
  BOT_PATTERN_CRYPTO_UPDOWN_PCT: 0.70,
  BOT_PATTERN_MIN_BUY_SIZE: 50,
  // Algorithmic high-frequency floor — sustained trades per active week.
  // Verified separation: human directional traders top out at ~410/active-
  // week (top wallet 0x602785, score 49.9). Confirmed bots in current pool
  // sit at 1,498 / 1,499 / 2,971 / 2,987 — a clean 3× gap. 700 lands in
  // the gap and won't false-positive any legitimate score-30+ trader.
  // Catches single-category algos (NBA bots, tennis arbs) the crypto-
  // updown and small-buy-size rules above can't see.
  BOT_PATTERN_MAX_TRADES_PER_ACTIVE_WEEK: 700,
  BOT_PATTERN_GATE: true,  // set false to disable (admit bots)

  // Fast loop
  FAST_LOOP_INTERVAL_MS: 60 * 60 * 1000, // 60 minutes
  // Trade lookback — bumped 4 → 48 to match signals.js CONVERGENCE_WINDOW_HOURS.
  // Pre-fix, Polymarket's API was ignoring our startTs filter and returning the
  // wallet's last 100 trades (could span weeks). The stale-trade fix on Apr 24
  // made fetchRecentTrades client-side filter to LOOKBACK_HOURS — but with that
  // set to 4, the convergence detector's 48h window was effectively starved
  // because we never gave it more than 4h of data. Result: signals 86k/scan →
  // 1.1k/scan. Bumping to 48 restores the intended convergence-detection window.
  LOOKBACK_HOURS: 48,

  // Goldsky pagination
  BATCH_SIZE: 1000,
  MAX_POSITIONS: 2000000,          // 2M positions — deeper crawl for more wallets
};

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// ============================================================================
// Bot-pattern detection — discovery + rescore gate
// ============================================================================

/**
 * Detect bot-pattern wallets from their trade events. Returns
 *   { isBot: bool, reason: string, cryptoUpdownPct: number, medianBuySize: number,
 *     tradesPerActiveWeek?: number }
 *
 * "Bot" criteria — ANY one triggers eviction:
 *   1. >70% of recent BUYs on crypto-updown markets (5-15min resolution)
 *   2. Median buy size < $50 across recent BUYs
 *   3. Sustained trade frequency > 700 / active week
 *      (catches algos the cat/size rules miss — single-category NBA
 *      bots, tennis arbs, etc. Verified gap: human top traders ≤500;
 *      confirmed bots in pool ≥1500.)
 *
 * Used at discovery (admit-time) and rescore (eviction strike) gates.
 */
function detectBotPattern(events) {
  if (!Array.isArray(events) || events.length === 0) {
    return { isBot: false, reason: 'no_events' };
  }
  const buys = events.filter(e =>
    e.type === 'TRADE' &&
    (e.side || '').toUpperCase() === 'BUY' &&
    e.title
  );
  if (buys.length < 20) {
    return { isBot: false, reason: 'insufficient_sample' };
  }

  // Crypto-updown: titles matching "Bitcoin/ETH/SOL Up or Down" pattern
  const isCryptoUpdown = (title) => {
    const t = (title || '').toLowerCase();
    if (!/bitcoin|btc|ethereum|eth|solana|sol\b|doge|xrp|crypto|coin/.test(t)) return false;
    return /reach|above|below|hit|close|\$|\sup\b|\sdown\b|end above|end below|up or down/.test(t);
  };

  const cryptoUpdownCount = buys.filter(b => isCryptoUpdown(b.title)).length;
  const cryptoUpdownPct = cryptoUpdownCount / buys.length;

  const sizes = buys.map(b => parseFloat(b.size || 0) * parseFloat(b.price || 0)).filter(s => s > 0).sort((a, b) => a - b);
  const medianBuySize = sizes.length > 0 ? sizes[Math.floor(sizes.length / 2)] : 0;

  // Algorithmic-frequency check — uses ALL trades (BUY+SELL), normalised by
  // distinct-week count rather than raw span, so a single tournament-day
  // burst doesn't false-positive (gets divided by 1 week → ~burst-size,
  // typically ≤500). Sustained activity across many weeks stays high.
  const allTrades = events.filter(e => e.type === 'TRADE' && typeof e.timestamp === 'number');
  let tradesPerActiveWeek = 0;
  if (allTrades.length >= 50) {
    const weeks = new Set(allTrades.map(t => Math.floor(t.timestamp / (86400 * 7))));
    if (weeks.size > 0) tradesPerActiveWeek = allTrades.length / weeks.size;
  }

  if (cryptoUpdownPct > CONFIG.BOT_PATTERN_CRYPTO_UPDOWN_PCT) {
    return { isBot: true, reason: 'crypto_updown_dominant', cryptoUpdownPct, medianBuySize, tradesPerActiveWeek };
  }
  if (medianBuySize < CONFIG.BOT_PATTERN_MIN_BUY_SIZE) {
    return { isBot: true, reason: 'median_buy_too_small', cryptoUpdownPct, medianBuySize, tradesPerActiveWeek };
  }
  if (tradesPerActiveWeek > CONFIG.BOT_PATTERN_MAX_TRADES_PER_ACTIVE_WEEK) {
    return { isBot: true, reason: 'algorithmic_high_frequency', cryptoUpdownPct, medianBuySize, tradesPerActiveWeek };
  }
  return { isBot: false, cryptoUpdownPct, medianBuySize, tradesPerActiveWeek };
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
    stuckCursorCount: 0,      // Consecutive discoveries where cursor didn't advance
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
 * Refresh a single wallet's lifetime PnL from Goldsky by aggregating
 * all of its positions. Used during the pool retention re-score loop so we
 * don't hold onto wallets whose lifetime PnL has drifted below the admission
 * floor since they were first discovered.
 *
 * When a marketLookup Map is provided AND CONFIG.ENABLE_DECIDED_METRICS is
 * on, we also pull tokenId/amount/avgPrice for each position so we can
 * classify winners vs losers vs open, and compute the ground-truth metrics
 * the scoring redesign needs (decidedROI, decidedCapital, mean-picker flag,
 * unredeemed / worthless counts). When marketLookup is omitted we fall back
 * to the original aggregate-only query shape.
 *
 * Returns:
 *   { totalPnl, totalBought, positionCount,                    // always
 *     decided: { decidedROI, decidedCapital, decidedPnl,       // if enabled
 *                openCapitalAtRisk, wins, losses, open,
 *                unredeemedWins, worthlessLosses, winRate,
 *                isMeanPickerShape, phantomSkipped } | null }
 *   or null on error.
 */
async function fetchGoldskyWalletPnl(wallet, entityName, fields, { marketLookup = null } = {}) {
  const userField = fields.user;
  const pnlField = fields.pnl;
  const boughtField = fields.totalBought;
  const tokenField = fields.token;
  const amountField = fields.amount;
  if (!pnlField) return null;

  // Only pull per-position detail when enabled AND we have a lookup to
  // classify against. Without a lookup we can't know open vs decided.
  const fullDetail = CONFIG.ENABLE_DECIDED_METRICS
    && marketLookup
    && tokenField && amountField;

  const addr = wallet.toLowerCase();
  let totalPnl = 0;
  let totalBought = 0;
  let positionCount = 0;
  let lastId = '';
  const positions = []; // only populated when fullDetail

  const queryFields = ['id', pnlField];
  if (boughtField) queryFields.push(boughtField);
  if (fullDetail) {
    queryFields.push(tokenField, amountField, 'avgPrice');
  }

  // Page through the wallet's positions (typically <100, so 1-2 queries)
  while (positionCount < 2000) {
    const query = `{
      ${entityName}s(
        first: 1000
        orderBy: id
        where: { ${userField}: "${addr}"${lastId ? `, id_gt: "${lastId}"` : ''} }
      ) {
        ${queryFields.join('\n        ')}
      }
    }`;

    let data;
    try {
      data = await gqlQuery(GOLDSKY_PNL, query);
    } catch (err) {
      return null;
    }

    const items = data?.[`${entityName}s`] || [];
    if (items.length === 0) break;

    for (const item of items) {
      totalPnl += parseFloat(item[pnlField] || 0) / USDC_DIVISOR;
      if (boughtField) totalBought += parseFloat(item[boughtField] || 0) / USDC_DIVISOR;
      positionCount++;

      if (fullDetail) {
        positions.push({
          tokenId: item[tokenField],
          sharesHeld: parseFloat(item[amountField] || 0) / USDC_DIVISOR,
          avgPrice: parseFloat(item.avgPrice || 0) / USDC_DIVISOR,
          realizedPnl: parseFloat(item[pnlField] || 0) / USDC_DIVISOR,
          totalBought: parseFloat(item[boughtField] || 0) / USDC_DIVISOR,
        });
      }
    }

    if (items.length < 1000) break;
    lastId = items[items.length - 1].id;
  }

  let decided = null;
  if (fullDetail && positions.length > 0) {
    const agg = aggregatePositions(positions, marketLookup);
    decided = {
      decidedPnl: agg.decidedPnl,
      decidedCapital: agg.decidedCapital,
      openCapitalAtRisk: agg.openCapitalAtRisk,
      decidedROI: agg.decidedROI,
      wins: agg.wins,
      losses: agg.losses,
      open: agg.open,
      winRate: agg.winRate,
      unredeemedWins: agg.unredeemedWins,
      worthlessLosses: agg.worthlessLosses,
      phantomSkipped: agg.phantomSkipped,
      isMeanPickerShape: agg.isMeanPickerShape,
    };
  }

  return { totalPnl, totalBought, positionCount, decided };
}

/**
 * Fetch all positions for a specific wallet using id_gt on the primary key
 * instead of filtering by user field. Position IDs are formatted as
 * "{wallet_address}-{token_id}", so we can start a cursor at "{addr}-" and
 * page forward until we hit a position belonging to a different wallet.
 *
 * This avoids the timeout caused by the unindexed user field filter in
 * fetchGoldskyWalletPnl. The primary key index makes this fast even for
 * wallets with thousands of positions.
 *
 * Returns { positions, totalPnl, totalBought, positionCount } or null on error.
 */
async function fetchPositionsByIdRange(walletAddr, marketLookup) {
  const addr = walletAddr.toLowerCase();
  const prefix = `${addr}-`;
  const positions = [];
  let totalPnl = 0;
  let totalBought = 0;
  let cursor = `${addr},`; // comma (0x2C) sorts just before dash (0x2D)

  while (positions.length < 2000) {
    const query = `{
      userPositions(
        first: 1000
        orderBy: id
        where: { id_gt: "${cursor}" }
      ) {
        id tokenId amount avgPrice realizedPnl totalBought
      }
    }`;

    let data;
    try {
      data = await gqlQuery(GOLDSKY_PNL, query);
    } catch (err) {
      return null;
    }

    const items = data?.userPositions || [];
    if (items.length === 0) break;

    let foundAny = false;
    for (const item of items) {
      if (!item.id.startsWith(prefix)) {
        // Reached a different wallet's positions — we're done
        return buildResult(positions, totalPnl, totalBought, marketLookup);
      }
      foundAny = true;
      const pnl = parseFloat(item.realizedPnl || 0) / USDC_DIVISOR;
      const bought = parseFloat(item.totalBought || 0) / USDC_DIVISOR;
      totalPnl += pnl;
      totalBought += bought;
      positions.push({
        tokenId: item.tokenId,
        sharesHeld: parseFloat(item.amount || 0) / USDC_DIVISOR,
        avgPrice: parseFloat(item.avgPrice || 0) / USDC_DIVISOR,
        realizedPnl: pnl,
        totalBought: bought,
      });
    }

    if (items.length < 1000) break;
    cursor = items[items.length - 1].id;
    if (!foundAny) break; // all items were from other wallets
  }

  return buildResult(positions, totalPnl, totalBought, marketLookup);
}

function buildResult(positions, totalPnl, totalBought, marketLookup) {
  if (positions.length === 0) return null;
  const agg = aggregatePositions(positions, marketLookup);
  return {
    totalPnl,
    totalBought,
    positionCount: positions.length,
    decided: agg ? {
      decidedPnl: agg.decidedPnl,
      decidedCapital: agg.decidedCapital,
      openCapitalAtRisk: agg.openCapitalAtRisk,
      decidedROI: agg.decidedROI,
      wins: agg.wins,
      losses: agg.losses,
      open: agg.open,
      winRate: agg.winRate,
      unredeemedWins: agg.unredeemedWins,
      worthlessLosses: agg.worthlessLosses,
      phantomSkipped: agg.phantomSkipped,
      isMeanPickerShape: agg.isMeanPickerShape,
    } : null,
  };
}

/**
 * Ensure the global marketLookup has resolution data for all tokens in a
 * wallet's activity events. The fast loop only resolves tokens it sees in
 * recent trades, so older markets (especially worthless losers that nobody
 * trades anymore) may sit in the lookup with only title/slug and no
 * marketClosed/winningOutcome. Without resolution data the WR fix can't
 * fire, leaving losing positions "open" and inflating PnL.
 *
 * This collects unique asset IDs whose lookup entry is missing OR lacks
 * resolution data, resolves them from Gamma, and merges results back into
 * the global lookup so future calls benefit too.
 */
async function ensureMarketsResolved(events, marketLookup) {
  if (!marketLookup || !events || events.length === 0) return;

  // Only resolve tokens where the wallet has a potential open position
  // (bought more than sold on that asset). This avoids wasting Gamma calls
  // on markets the wallet already closed naturally.
  const assetBuys = new Map();  // asset → total buy size
  const assetSells = new Map(); // asset → total sell size
  for (const ev of events) {
    const asset = ev.asset || ev.tokenId || '';
    if (!asset) continue;
    const size = parseFloat(ev.size || 0) || 0;
    const type = (ev.type || '').toUpperCase();
    const side = (ev.side || '').toUpperCase();
    if (type === 'REDEEM' || side === 'SELL') {
      assetSells.set(asset, (assetSells.get(asset) || 0) + size);
    } else if (side === 'BUY') {
      assetBuys.set(asset, (assetBuys.get(asset) || 0) + size);
    }
  }

  const unresolvedTokens = new Set();
  for (const [asset, buySize] of assetBuys) {
    const sellSize = assetSells.get(asset) || 0;
    // Only need resolution for open positions (buy > 95% sold)
    if (sellSize >= buySize * 0.95) continue;
    const existing = marketLookup.get(asset);
    // Re-check any token not confirmed closed. marketClosed:false entries from prior
    // scans may have since resolved — this is where worthless-loser markets hide.
    if (!existing || existing.marketClosed !== true) {
      unresolvedTokens.add(asset);
    }
  }

  if (unresolvedTokens.size === 0) return;

  // Cap per-wallet resolution to avoid scan timeouts. The global lookup
  // persists across scans, so unresolved tokens get picked up next cycle.
  const MAX_RESOLVE_PER_WALLET = 150;
  let tokensToResolve = unresolvedTokens;
  if (unresolvedTokens.size > MAX_RESOLVE_PER_WALLET) {
    const arr = Array.from(unresolvedTokens).slice(0, MAX_RESOLVE_PER_WALLET);
    tokensToResolve = new Set(arr);
    console.log(`    Capping resolution: ${tokensToResolve.size}/${unresolvedTokens.size} open-position tokens`);
  }

  try {
    const resolved = await resolveMarkets(tokensToResolve);
    let newResolutions = 0;
    for (const [id, market] of resolved) {
      marketLookup.set(id, market);
      if (market.marketClosed) newResolutions++;
    }
    if (newResolutions > 0) {
      console.log(`    Resolved ${newResolutions} new markets from ${tokensToResolve.size} unresolved tokens`);
    }
  } catch (err) {
    // Non-fatal — analyzer will run with incomplete lookup
    console.warn(`    Market resolution warning: ${err.message}`);
  }
}

/**
 * Discover wallets from Polymarket's public leaderboard API.
 *
 * Endpoint: data-api.polymarket.com/v1/leaderboard
 * Params: timePeriod (day|week|month|all), orderBy (PNL|VOL),
 *         category (overall|sports|...), limit, offset.
 * Returns: [{ rank, proxyWallet, userName, vol, pnl, ... }]
 *
 * Built as a Goldsky-failure fallback after Goldsky's sgd2684→sgd4477
 * subgraph migration started timing out on cursor pagination
 * (2026-05-01). Polymarket's own leaderboard is more directly aligned
 * with what we want anyway — wallets ranked by actual PnL — and avoids
 * the "scan 2M positions, find 100 wallets" inefficiency of cursor
 * iteration.
 *
 * Returns Map<address, { pnl, vol, userName, sourceQuery }> deduped
 * across multiple (timePeriod × orderBy) queries.
 */
async function discoverFromPolymarketLeaderboard() {
  const candidates = new Map(); // address → { pnl, vol, userName, sourceQuery }
  const PAGE_LIMIT = 100;       // Polymarket's leaderboard endpoint limit
  const HEADERS = {
    'Origin': 'https://polymarket.com',
    'Referer': 'https://polymarket.com/',
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (compatible; PolymarketSignalEngine/1.0)',
  };

  // Pull from multiple windows + orderings to maximize unique candidate count.
  // PNL window catches the trader-side. VOL catches high-volume wallets that
  // might be high-frequency traders or whales we'd otherwise miss.
  const queries = [
    // Overall — top profit + volume across all categories
    { timePeriod: 'month', orderBy: 'PNL', category: 'overall', maxOffset: 500 },
    { timePeriod: 'week',  orderBy: 'PNL', category: 'overall', maxOffset: 500 },
    { timePeriod: 'all',   orderBy: 'PNL', category: 'overall', maxOffset: 500 },
    { timePeriod: 'month', orderBy: 'VOL', category: 'overall', maxOffset: 500 },
    // Per-category for breadth — leaderboard caps each at ~50-100 entries
    { timePeriod: 'month', orderBy: 'PNL', category: 'sports',   maxOffset: 200 },
    { timePeriod: 'month', orderBy: 'PNL', category: 'politics', maxOffset: 200 },
    { timePeriod: 'month', orderBy: 'PNL', category: 'crypto',   maxOffset: 200 },
  ];

  console.log('  Fetching leaderboards from Polymarket data-api...');
  for (const q of queries) {
    const queryTag = `${q.timePeriod}/${q.orderBy}/${q.category}`;
    let added = 0;
    for (let offset = 0; offset <= q.maxOffset; offset += PAGE_LIMIT) {
      const url = `https://data-api.polymarket.com/v1/leaderboard?timePeriod=${q.timePeriod}&orderBy=${q.orderBy}&limit=${PAGE_LIMIT}&offset=${offset}&category=${q.category}`;
      let resp;
      try {
        resp = await fetch(url, { headers: HEADERS });
      } catch (e) {
        console.log(`    ⚠ ${queryTag} offset=${offset} — network error: ${e.message}`);
        break;
      }
      if (!resp.ok) {
        console.log(`    ⚠ ${queryTag} offset=${offset} — HTTP ${resp.status}`);
        break;
      }
      let data;
      try { data = await resp.json(); } catch { break; }
      if (!Array.isArray(data) || data.length === 0) break;

      for (const entry of data) {
        const addr = (entry.proxyWallet || '').toLowerCase();
        if (!/^0x[a-f0-9]{40}$/.test(addr)) continue;
        if (!candidates.has(addr)) {
          candidates.set(addr, {
            pnl: +(entry.pnl || 0),
            vol: +(entry.vol || 0),
            userName: entry.userName || '',
            sourceQuery: queryTag,
          });
          added++;
        } else {
          // Already seen — keep the higher PnL stat across queries
          const existing = candidates.get(addr);
          if ((entry.pnl || 0) > existing.pnl) {
            existing.pnl = +(entry.pnl || 0);
            existing.sourceQuery = queryTag;
          }
        }
      }

      // Page returned fewer than PAGE_LIMIT → end of leaderboard reached
      if (data.length < PAGE_LIMIT) break;

      // Be polite — small delay between paginated fetches
      await new Promise(r => setTimeout(r, 50));
    }
    console.log(`    ${queryTag.padEnd(28)} +${added} unique wallets`);
  }

  console.log(`  ✓ Polymarket leaderboard: ${candidates.size} unique candidates`);
  return candidates;
}

/**
 * Discover wallet addresses from Goldsky and qualify them via Data API.
 * This runs periodically (every DISCOVERY_INTERVAL_SCANS fast loops).
 */
async function discoverWallets(state, existingPool, marketLookup = null, attributionMap = null) {
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

  // Persist the full schema field list to state so we can see what additional
  // fields Goldsky exposes on the position entity that we aren't currently
  // mining (e.g. timestamps, tradeCount, etc.) — discoverEntities logs them
  // but until now we were throwing that information away.
  try {
    const allFields = await introspectEntity(GOLDSKY_PNL,
      entityName.charAt(0).toUpperCase() + entityName.slice(1).replace(/s$/, ''))
      || await introspectEntity(GOLDSKY_PNL, entityName);
    if (allFields && allFields.length > 0) {
      state.goldskySchema = {
        entity: entityName,
        allFields,
        usedFields: fields,
        unusedFields: allFields.filter(f =>
          ![fields.user, fields.pnl, fields.token, fields.totalBought, fields.amount, 'id'].includes(f)),
        introspectedAt: new Date().toISOString(),
      };
    }
  } catch (err) {
    // Non-fatal — introspection is informational
  }

  // Advance a stuck cursor to the next 2-hex-char address prefix bucket.
  // Wallet ids are hex strings like "0x07e78173...-..." — the first two
  // hex chars after "0x" give us 256 evenly-sized buckets. Jumping to the
  // next bucket skips over any "poisoned" region that's timing out in
  // Goldsky's index scan.
  // Example: "0x07e7...-..."  →  "0x08"  (matches everything starting 0x08...)
  //          "0xff...-..."    →  ""      (wrap to beginning)
  function nextPrefixBucket(cur) {
    if (!cur || typeof cur !== 'string' || !cur.startsWith('0x') || cur.length < 4) return '';
    const n = parseInt(cur.slice(2, 4), 16);
    if (Number.isNaN(n)) return '';
    if (n >= 255) return ''; // wrap
    return '0x' + (n + 1).toString(16).padStart(2, '0');
  }

  // Step 2: Fetch position summaries from Goldsky (aggregate per wallet)
  // Resume from last cursor position so each discovery scans NEW positions.
  // If we've been stuck on the same cursor for 2+ cycles, Goldsky is timing
  // out on this position — auto-advance past it before we even start.
  let resumeCursor = state.lastId || '';
  if (resumeCursor && (state.stuckCursorCount || 0) >= 2) {
    const advanced = nextPrefixBucket(resumeCursor);
    console.log(`  ⚠ Cursor has been stuck for ${state.stuckCursorCount} cycles at ${resumeCursor.slice(0, 16)}... — auto-advancing to bucket "${advanced || '(start)'}"`);
    resumeCursor = advanced;
    state.stuckCursorCount = 0;
  }
  console.log(`  Fetching wallet positions from Goldsky...${resumeCursor ? ' (resuming from cursor)' : ' (starting fresh)'}`);
  const walletSummaries = new Map(); // address → { totalPnl, positionCount, totalBought }
  // Per-position detail for decided-metrics (V2 scoring). Populated during the
  // cursor scan so we never need per-wallet Goldsky queries (which timeout
  // because the subgraph has no index on the user field).
  const walletPositions = CONFIG.ENABLE_DECIDED_METRICS ? new Map() : null; // address → position[]

  let cursor = resumeCursor;
  const cursorAtStart = cursor;
  let totalFetched = 0;
  let wrapped = false;
  let bucketAdvancesThisCycle = 0;
  const MAX_BUCKET_ADVANCES = 4; // hard cap — never scan more than 4 buckets in one cycle

  while (totalFetched < CONFIG.MAX_POSITIONS) {
    const userField = fields.user;
    const pnlField = fields.pnl;
    const boughtField = fields.totalBought;
    const amountField = fields.amount;

    // Build query with only the fields that exist
    const tokenField = fields.token;
    const queryFields = ['id', `${userField} { id }`];
    if (pnlField) queryFields.push(pnlField);
    if (boughtField) queryFields.push(boughtField);
    // Per-position detail for decided-metrics — only request extra fields when
    // we're actually going to use them (avoids wasting bandwidth otherwise).
    if (walletPositions && tokenField) queryFields.push(tokenField);
    if (walletPositions && amountField) queryFields.push(amountField);
    if (walletPositions && tokenField) queryFields.push('avgPrice');

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
      console.error(`  Goldsky query error at cursor ${cursor ? cursor.slice(0, 16) + '…' : '(start)'}: ${err.message}`);
      // Advance past the poisoned cursor region instead of breaking silently.
      // Without this, state.lastId would stay pinned at the failing cursor
      // forever and every subsequent discovery would hit the same timeout.
      if (bucketAdvancesThisCycle >= MAX_BUCKET_ADVANCES) {
        console.error(`  Already advanced ${bucketAdvancesThisCycle} buckets this cycle — giving up to avoid burning through address space`);
        break;
      }
      const advanced = nextPrefixBucket(cursor);
      console.log(`  ↪ Advancing cursor to next prefix bucket: "${advanced || '(start)'}"`);
      cursor = advanced;
      bucketAdvancesThisCycle++;
      await new Promise(r => setTimeout(r, 500)); // brief pause before retry
      continue;
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

      // Store per-position detail for aggregatePositions (V2 scoring).
      // Cap at 2000 positions per wallet to bound memory (~400 bytes/pos).
      if (walletPositions && tokenField) {
        if (!walletPositions.has(address)) walletPositions.set(address, []);
        const posArr = walletPositions.get(address);
        if (posArr.length < 2000) {
          posArr.push({
            tokenId: item[tokenField],
            sharesHeld: parseFloat(item[amountField] || 0) / USDC_DIVISOR,
            avgPrice: parseFloat(item.avgPrice || 0) / USDC_DIVISOR,
            realizedPnl: pnl,
            totalBought: bought,
          });
        }
      }
    }

    cursor = items[items.length - 1].id;
    totalFetched += items.length;

    if (totalFetched % 10000 === 0) {
      console.log(`  Scanned ${totalFetched.toLocaleString()} positions, ${walletSummaries.size.toLocaleString()} unique wallets...`);
    }

    if (items.length < CONFIG.BATCH_SIZE) {
      // Reached the end of Goldsky data — wrap around to start next time
      cursor = '';
      wrapped = true;
      break;
    }
    await new Promise(r => setTimeout(r, 200));
  }

  // Save cursor so next discovery resumes where we left off.
  // Track stuck state: if cursor didn't advance at all this cycle, increment
  // the counter. After 2 stuck cycles the next discovery will auto-advance
  // past the poisoned region (see resumeCursor handling above).
  if (cursor === cursorAtStart && !wrapped && totalFetched === 0) {
    state.stuckCursorCount = (state.stuckCursorCount || 0) + 1;
    console.log(`  ⚠ Cursor did not advance this cycle (stuck count: ${state.stuckCursorCount})`);
  } else {
    state.stuckCursorCount = 0;
  }
  state.lastId = cursor;
  console.log(`  Found ${walletSummaries.size.toLocaleString()} wallets from ${totalFetched.toLocaleString()} positions${wrapped ? ' (reached end, will restart next cycle)' : ''}${bucketAdvancesThisCycle > 0 ? ` [${bucketAdvancesThisCycle} bucket advances]` : ''}`);

  // Step 2b: Polymarket leaderboard fallback. When Goldsky returns 0
  // (subgraph migration/timeout) or returns thin results, supplement
  // candidates from Polymarket's own leaderboard API. These come pre-
  // ranked by PnL — exactly what we want — and use proxyWallet
  // addresses our /activity API already works on. See
  // discoverFromPolymarketLeaderboard() docstring for full design.
  let leaderboardAdded = 0;
  try {
    const leaderboardCandidates = await discoverFromPolymarketLeaderboard();
    for (const [addr, info] of leaderboardCandidates) {
      if (walletSummaries.has(addr)) continue;       // already from Goldsky
      if (existingPool[addr]) continue;               // already in pool
      walletSummaries.set(addr, {
        totalPnl: info.pnl,
        positionCount: 0,                             // unknown from leaderboard, qualify pass will fill
        totalBought: 0,
        sourceQuery: info.sourceQuery,
        userName: info.userName,
        leaderboardVol: info.vol,
      });
      leaderboardAdded++;
    }
    console.log(`  Polymarket leaderboard contributed ${leaderboardAdded} new candidates (pre-existing in Goldsky/pool: ${leaderboardCandidates.size - leaderboardAdded})`);
  } catch (err) {
    console.log(`  ⚠ Polymarket leaderboard fetch failed: ${err.message}`);
  }

  // Step 3: Filter to candidates worth qualifying
  // Note: leaderboard-sourced wallets have positionCount=0 (unknown), so we
  // can't filter them on MIN_POSITIONS_DISCOVERY. Trust their PnL ranking
  // and let the per-wallet /activity fetch in Step 4 do the real qualifying.
  const candidates = [...walletSummaries.entries()]
    .filter(([, ws]) => ws.totalPnl >= CONFIG.MIN_PNL_DISCOVERY
      && (ws.positionCount >= CONFIG.MIN_POSITIONS_DISCOVERY || ws.sourceQuery))
    .sort((a, b) => b[1].totalPnl - a[1].totalPnl)
    .slice(0, CONFIG.MAX_DISCOVERY_WALLETS);

  console.log(`  ${candidates.length} candidates pass PnL/position filters (${leaderboardAdded} from leaderboard, ${candidates.length - leaderboardAdded} from Goldsky)`);

  // Free position data for wallets that didn't make the candidate cut — they'll
  // never be used and releasing early keeps peak memory bounded. We keep positions
  // for existing pool members too (they may need decided metrics on rescore).
  if (walletPositions) {
    const keepAddrs = new Set(candidates.map(([addr]) => addr));
    for (const addr of Object.keys(existingPool)) keepAddrs.add(addr);
    for (const addr of walletPositions.keys()) {
      if (!keepAddrs.has(addr)) walletPositions.delete(addr);
    }
  }

  // Step 4: Fetch trade histories from Data API and score
  console.log('  Fetching trade histories from Data API...');
  const pool = { ...existingPool };
  let qualified = 0;
  let processed = 0;

  for (const [address, summary] of candidates) {
    // Skip if already in pool and scored recently.
    //
    // Two-tier cooldown:
    //   - < 24h  (HARD_FLOOR): always skip. Wallets with 50+ resolved markets
    //     barely drift in a day; this cut re-score load by ~95% vs no
    //     cooldown and is what the original time-based gate was there for.
    //   - 24h–7d (activity gate): skip ONLY if the wallet hasn't traded
    //     since its last score. `stats.lastTradeTs > lastScored` means new
    //     activity → stats may have shifted and it's worth re-analyzing.
    //     A dormant wallet's stats literally cannot have moved since it
    //     was last analyzed, so re-scoring it is pure wasted API budget.
    //   - > 7d   (MAX_FLOOR): always re-score. Guards against edge cases
    //     where lastTradeTs is stale or missing, and forces a periodic
    //     refresh so truly inactive wallets still hit the MAX_INACTIVE_DAYS
    //     eviction check on the re-score path.
    const DISCOVERY_RESCORE_COOLDOWN_MS = 24 * 60 * 60 * 1000;
    const DISCOVERY_RESCORE_MAX_COOLDOWN_MS = 7 * 24 * 60 * 60 * 1000;
    if (pool[address] && pool[address].lastScored) {
      const lastScoredMs = new Date(pool[address].lastScored).getTime();
      const ageMs = Date.now() - lastScoredMs;
      const lastTradeMs = (pool[address].stats?.lastTradeTs || 0) * 1000;
      const tradedSinceScored = lastTradeMs > lastScoredMs;
      const withinHardFloor = ageMs < DISCOVERY_RESCORE_COOLDOWN_MS;
      const withinMaxFloor = ageMs < DISCOVERY_RESCORE_MAX_COOLDOWN_MS;
      if (withinHardFloor || (withinMaxFloor && !tradedSinceScored)) {
        // Even though we're skipping the full rescore, populate V2 from
        // cursor positions if this wallet hasn't been V2-scored yet. This
        // is cheap (no API call — pure in-memory aggregation) and prevents
        // the 50% coverage gate from stalling for days while wallets sit
        // in cooldown with no score.
        if (typeof pool[address].score !== 'number' && walletPositions) {
          const cursorPos = walletPositions.get(address);
          if (cursorPos && cursorPos.length > 0) {
            const agg = aggregatePositions(cursorPos, marketLookup);
            if (agg && agg.decidedROI != null && pool[address].stats) {
              pool[address].stats.decidedPnl = agg.decidedPnl;
              pool[address].stats.decidedCapital = agg.decidedCapital;
              pool[address].stats.decidedROI = agg.decidedROI;
              pool[address].stats.decidedWins = agg.wins;
              pool[address].stats.decidedLosses = agg.losses;
              pool[address].stats.decidedWinRate = agg.winRate;
              pool[address].stats.decidedOpenPositions = agg.open;
              pool[address].stats.decidedOpenCapitalAtRisk = agg.openCapitalAtRisk;
              pool[address].stats.decidedUnredeemedWinsPositions = agg.unredeemedWins;
              pool[address].stats.decidedWorthlessLosses = agg.worthlessLosses;
              pool[address].stats.isMeanPickerShape = agg.isMeanPickerShape;
              pool[address].stats.decidedMeasuredAt = new Date().toISOString();
              // Stage 1: attach MM classification before scoring so score
              // can penalise whale-01-class wallets via stats.mmPenalty.
              attachMMClassification(pool[address].stats);
              attachAlphaEvaluation(pool[address].stats);
              attachAttribution(pool[address].stats, attributionMap, address);
              const scored = computeWalletScore(pool[address].stats);
              if (scored && scored.score != null) {
                pool[address].score = scored.score;
                pool[address].scoreComponents = scored.components;
              }
            }
          }
        }
        qualified++;
        processed++;
        if (processed % 100 === 0) console.log(`  Processed ${processed}/${candidates.length} (${qualified} qualified)...`);
        continue;
      }
    }

    try {
      // Use /activity so REDEEM events close out winning positions that would
      // otherwise look "still open" to the analyzer and contribute $0 to PnL.
      const events = await fetchAllActivity(address, { maxEvents: 5000 });
      if (!events || events.length < 10) {
        processed++;
        continue;
      }

      // Discovery uses the global lookup as-is for initial qualification.
      // Per-wallet Gamma resolution is too slow for 1000+ candidates (~9s each).
      // Wallets enter the pool with a rough score and get properly corrected
      // on their first re-score cycle via ensureMarketsResolved (Step 5).
      const stats = analyzeTradeHistory(events, { marketLookup });
      if (!stats) {
        processed++;
        continue;
      }

      // effectivePnl = max(analyzer sample economic PnL, Goldsky on-chain realized).
      // - Sample wins when wallet has unredeemed winners (Goldsky reports $0
      //   until on-chain redemption; analyzer infers via marketLookup).
      // - Goldsky wins when wallet has >3000 activity events (analyzer is
      //   truncated to the most-recent window).
      // Taking max gives us the benefit of both measurement systems.
      //
      // Stage 0: we now use `economicPnl` (= trade PnL + rewards + rebates +
      // MERGE closures) instead of the bare `totalPnl`. The old view silently
      // dropped all non-trade income, which undercounted MM-style wallets
      // like whale-01 by millions. Fallback to totalPnl preserves legacy data.
      stats.goldskyPnl = summary.totalPnl || 0;
      const sampleEconomic = stats.economicPnl != null ? stats.economicPnl : (stats.totalPnl || 0);
      stats.effectivePnl = Math.max(sampleEconomic, stats.goldskyPnl);

      // Require minimum resolved markets — no flukes
      if ((stats.resolvedMarkets || 0) < CONFIG.MIN_RESOLVED_MARKETS) {
        processed++;
        continue;
      }

      // Must have traded recently. Discovery uses the stricter floor than
      // the rescore loop so we don't admit pre-cooled wallets.
      const daysSinceLastTrade = stats.lastTradeTs > 0
        ? (Date.now() / 1000 - stats.lastTradeTs) / 86400
        : Infinity;
      const freshInactiveFloor = CONFIG.DORMANCY_DAYS;
      if (daysSinceLastTrade > freshInactiveFloor) {
        processed++;
        continue;
      }

      // Reject scrap-graders: wallets whose capital-weighted typical entry
      // is above our ROI-floor ceiling. Their WR may look great but the
      // signal engine would filter every trade they make at MIN_OPEN_ROI.
      if (CONFIG.MAX_WALLET_AVG_ENTRY_PRICE > 0 &&
          CONFIG.MAX_WALLET_AVG_ENTRY_PRICE < 1 &&
          stats.avgEntryPrice > 0 &&
          stats.avgEntryPrice > CONFIG.MAX_WALLET_AVG_ENTRY_PRICE) {
        processed++;
        continue;
      }

      // ── Discovery gates on position-centric metrics ─────────────────────
      // Use per-position data captured during the cursor scan to compute
      // decided metrics. This avoids per-wallet Goldsky queries which
      // timeout because the subgraph has no index on the user field.
      let decidedMetrics = null;
      if (walletPositions) {
        const cursorPositions = walletPositions.get(address);
        if (cursorPositions && cursorPositions.length > 0) {
          const agg = aggregatePositions(cursorPositions, marketLookup);
          if (agg) {
            decidedMetrics = {
              decidedPnl: agg.decidedPnl,
              decidedCapital: agg.decidedCapital,
              openCapitalAtRisk: agg.openCapitalAtRisk,
              decidedROI: agg.decidedROI,
              wins: agg.wins,
              losses: agg.losses,
              open: agg.open,
              winRate: agg.winRate,
              unredeemedWins: agg.unredeemedWins,
              worthlessLosses: agg.worthlessLosses,
              isMeanPickerShape: agg.isMeanPickerShape,
            };
          }
        }

        if (decidedMetrics) {
          // Reject obvious mean-picker shape on sight
          if (decidedMetrics.isMeanPickerShape === true) {
            processed++;
            continue;
          }
          // Require meaningful capital-at-risk on resolved bets
          if ((decidedMetrics.decidedCapital || 0) < CONFIG.DISCOVERY_MIN_DECIDED_CAPITAL) {
            processed++;
            continue;
          }
          // Require real edge, not just WR
          if (decidedMetrics.decidedROI == null
              || decidedMetrics.decidedROI < CONFIG.DISCOVERY_MIN_DECIDED_ROI) {
            processed++;
            continue;
          }
          // Hard cap on WR when sample is meaningful — 99%+ WR is almost
          // always a mean-picker, even if other gates passed by a whisker
          const resolvedNow = (decidedMetrics.wins || 0) + (decidedMetrics.losses || 0);
          if (resolvedNow >= CONFIG.DISCOVERY_MAX_WIN_RATE_MIN_RESOLVED
              && decidedMetrics.winRate != null
              && decidedMetrics.winRate > CONFIG.DISCOVERY_MAX_WIN_RATE) {
            processed++;
            continue;
          }
        } else if (stats.singleSideROI != null && stats.singleSideCapital != null) {
          // Fallback path: wallet not in walletPositions cursor — apply the
          // equivalent gates against singleSide* metrics. Keeps admission
          // quality-bar the same whether we got decided or single-side data.
          // See dataApi.js computeWalletScore for how this feeds scoring.
          if (stats.singleSideCapital < CONFIG.DISCOVERY_MIN_DECIDED_CAPITAL) {
            processed++;
            continue;
          }
          if (stats.singleSideROI < CONFIG.DISCOVERY_MIN_DECIDED_ROI) {
            processed++;
            continue;
          }
          const resolvedSS = stats.singleSideResolved || stats.resolvedMarkets || 0;
          if (resolvedSS >= CONFIG.DISCOVERY_MAX_WIN_RATE_MIN_RESOLVED
              && stats.singleSideHitRate != null
              && stats.singleSideHitRate > CONFIG.DISCOVERY_MAX_WIN_RATE) {
            processed++;
            continue;
          }
        }
      }

      // Fold decided metrics onto stats so the wallet admits with full
      // V2 data already populated — no waiting for first rescore.
      if (decidedMetrics) {
        stats.decidedPnl = decidedMetrics.decidedPnl;
        stats.decidedCapital = decidedMetrics.decidedCapital;
        stats.decidedROI = decidedMetrics.decidedROI;
        stats.decidedWins = decidedMetrics.wins;
        stats.decidedLosses = decidedMetrics.losses;
        stats.decidedWinRate = decidedMetrics.winRate;
        stats.decidedOpenPositions = decidedMetrics.open;
        stats.decidedOpenCapitalAtRisk = decidedMetrics.openCapitalAtRisk;
        stats.decidedUnredeemedWinsPositions = decidedMetrics.unredeemedWins;
        stats.decidedWorthlessLosses = decidedMetrics.worthlessLosses;
        stats.isMeanPickerShape = decidedMetrics.isMeanPickerShape;
        stats.decidedMeasuredAt = new Date().toISOString();
      }
      // Compute V2 score too so the wallet is ranker-ready on day zero
      // Stage 1/2: run MM classifier + alpha test before scoring.
      attachMMClassification(stats);
      attachAlphaEvaluation(stats);
      attachAttribution(stats, attributionMap, address);

      // Stage 1/2 hard discovery gates:
      //   1. Likely market-maker (mmScore ≥ 4) — not copy-tradeable.
      //   2. Explicit alpha failure (edge_pp < 1.5pp with enough sample +
      //      capital to trust the signal) — mean-picker shape by another name.
      // Wallets with 'insufficient_sample' or 'insufficient_capital' verdicts
      // are admitted; they can prove themselves later. We don't want to block
      // promising newer wallets — only clear negatives.
      if (stats.isLikelyMM === true) {
        processed++;
        continue;
      }
      if (stats.alphaVerdict === 'fails') {
        processed++;
        continue;
      }
      // Bot-pattern gate — reject if recent activity skews to crypto-updown
      // or sub-$50 micro-bets. Saves pool slot from algorithmic wallets that
      // structurally can't produce copyable signals (kill at MIN_HOURS_TO_RESOLUTION
      // or SOLO_MIN_BUY_SIZE downstream). See detectBotPattern() comments.
      if (CONFIG.BOT_PATTERN_GATE) {
        const bot = detectBotPattern(events);
        if (bot.isBot) {
          processed++;
          continue;
        }
      }

      const scored = computeWalletScore(stats);

      // Note: NO discovery-time score floor. Categories our classifier
      // doesn't recognize (esports without explicit "lol" keyword, niche
      // sports, crypto-other) yield categoryAlignment=0 → score=0, which
      // would reject every candidate. Admit them; let signal-emission
      // gates filter at the right level. Bots are already caught by the
      // bot-pattern check above, mean-pickers by the explicit gate.
      pool[address] = {
        address,
        score: scored.score != null ? scored.score : undefined,
        scoreComponents: scored.components || undefined,
        stats,
        goldskyPnl: summary.totalPnl,
        goldskyPositions: summary.positionCount,
        lastScored: new Date().toISOString(),
        discoveredScan: state.scanCount,
        totalTrades: events.length,
        status: 'active',
      };

      qualified++;
    } catch (err) {
      // Log first few errors per scan so silent failures are visible
      if (processed < 5 || (err && err.message && !/timeout|ECONNRESET|fetch failed/i.test(err.message))) {
        console.log(`    ⚠ qualify err ${address.slice(0, 10)}: ${err?.message || err}`);
      }
    }

    processed++;
    if (processed % 50 === 0) {
      console.log(`  Processed ${processed}/${candidates.length} (${qualified} qualified)...`);
    }
  }

  // Step 5: Re-check existing pool members not in current candidates
  // Catches wallets that have gone inactive or deteriorated since last scored
  const candidateAddrs = new Set(candidates.map(([addr]) => addr));
  let decayed = 0;
  let rescored = 0;
  const staleWallets = Object.entries(pool)
    .filter(([addr, w]) => {
      if (candidateAddrs.has(addr)) return false;
      if (!w.lastScored || w.status === 'removed') return false;
      const daysSinceScored = (Date.now() - new Date(w.lastScored).getTime()) / (24 * 60 * 60 * 1000);
      // Re-check stale pool members weekly. Primary cooldown (24h) handles
      // candidates present in this scan; this catches wallets that have
      // dropped out of the candidate list (inactive / deteriorated).
      return daysSinceScored >= 7;
    })
    .sort((a, b) => {
      // Oldest scored first — prioritise most stale
      return new Date(a[1].lastScored).getTime() - new Date(b[1].lastScored).getTime();
    })
    .slice(0, CONFIG.RESCORE_BATCH_SIZE); // Limit to batch size per discovery

  let pnlDecayed = 0;
  for (const [addr, wallet] of staleWallets) {
    // Quick inactive check (no API call needed)
    const daysSinceLastTrade = wallet.stats?.lastTradeTs > 0
      ? (Date.now() / 1000 - wallet.stats.lastTradeTs) / 86400
      : Infinity;
    if (daysSinceLastTrade > CONFIG.DORMANCY_DAYS) {
      wallet.status = 'removed';
      wallet.removeReason = 'inactive';
      decayed++;
      continue;
    }

    // Refresh lifetime PnL + decided metrics from cursor-scan data when
    // available. Falls back to per-wallet Goldsky query only if the wallet
    // wasn't in this cycle's cursor window. Per-wallet queries often timeout
    // because the subgraph has no index on the user field — the cursor-scan
    // approach avoids that entirely.
    const cursorPos = walletPositions?.get(addr);
    if (cursorPos && cursorPos.length > 0) {
      // Derive PnL + decided from cursor positions (fast, no network call)
      let cursorPnl = 0, cursorBought = 0;
      for (const p of cursorPos) { cursorPnl += p.realizedPnl; cursorBought += p.totalBought; }
      wallet.goldskyPnl = +cursorPnl.toFixed(2);
      wallet.goldskyPositions = cursorPos.length;
      if (wallet.goldskyPnl < CONFIG.MIN_PNL_DISCOVERY) {
        wallet.status = 'removed';
        wallet.removeReason = 'pnl_below_floor';
        pnlDecayed++;
        decayed++;
        continue;
      }
      const agg = aggregatePositions(cursorPos, marketLookup);
      if (agg) {
        wallet.decidedMetrics = {
          decidedPnl: agg.decidedPnl,
          decidedCapital: agg.decidedCapital,
          openCapitalAtRisk: agg.openCapitalAtRisk,
          decidedROI: agg.decidedROI,
          wins: agg.wins,
          losses: agg.losses,
          open: agg.open,
          winRate: agg.winRate,
          unredeemedWins: agg.unredeemedWins,
          worthlessLosses: agg.worthlessLosses,
          isMeanPickerShape: agg.isMeanPickerShape,
          measuredAt: new Date().toISOString(),
        };
      }
    } else {
      // Wallet wasn't in cursor window — fall back to Polymarket Data API
      // /positions per-wallet fetch. This REPLACES the old Goldsky per-wallet
      // query that was silently timing out for ~30% of pool wallets (the
      // subgraph has no index on the user field). The Data API has no such
      // index issue. Fetch is fast (~100ms) and reliable.
      //
      // Diagnosed 2026-04-28: 297 of 1000 pool wallets (30%) had null
      // decidedROI/decidedCapital because the Goldsky fallback always
      // failed silently for them. Switching to Data API fixes this
      // permanently — positions endpoint works for every active wallet.
      try {
        const positions = await fetchWalletPositions(addr);
        if (positions && positions.length > 0) {
          let cursorPnl = 0, cursorBought = 0;
          for (const p of positions) {
            cursorPnl += p.realizedPnl;
            cursorBought += p.totalBought;
          }
          wallet.goldskyPnl = +cursorPnl.toFixed(2);
          wallet.goldskyPositions = positions.length;
          if (wallet.goldskyPnl < CONFIG.MIN_PNL_DISCOVERY) {
            wallet.status = 'removed';
            wallet.removeReason = 'pnl_below_floor';
            pnlDecayed++;
            decayed++;
            continue;
          }
          const agg = aggregatePositions(positions, marketLookup);
          if (agg) {
            wallet.decidedMetrics = {
              decidedPnl: agg.decidedPnl,
              decidedCapital: agg.decidedCapital,
              openCapitalAtRisk: agg.openCapitalAtRisk,
              decidedROI: agg.decidedROI,
              wins: agg.wins,
              losses: agg.losses,
              open: agg.open,
              winRate: agg.winRate,
              unredeemedWins: agg.unredeemedWins,
              worthlessLosses: agg.worthlessLosses,
              isMeanPickerShape: agg.isMeanPickerShape,
              measuredAt: new Date().toISOString(),
              source: 'polymarket_data_api',
            };
          }
        }
      } catch (err) {
        // Non-fatal — leave decidedMetrics as-is
      }
    }

    // Re-score from fresh activity data (trades + redemptions)
    try {
      const events = await fetchAllActivity(addr, { maxEvents: 5000 });
      if (!events || events.length < 10) {
        wallet.status = 'removed';
        wallet.removeReason = 'insufficient_trades';
        decayed++;
        continue;
      }
      await ensureMarketsResolved(events, marketLookup);

      const stats = analyzeTradeHistory(events, { marketLookup });
      if (!stats || (stats.resolvedMarkets || 0) < CONFIG.MIN_RESOLVED_MARKETS) {
        wallet.status = 'removed';
        wallet.removeReason = 'insufficient_resolved';
        decayed++;
        continue;
      }
      // Evict scrap-graders whose typical entry price leaves no ROI headroom
      // for the signal engine's MIN_OPEN_ROI gate to ever accept their plays.
      if (CONFIG.MAX_WALLET_AVG_ENTRY_PRICE > 0 &&
          CONFIG.MAX_WALLET_AVG_ENTRY_PRICE < 1 &&
          stats.avgEntryPrice > 0 &&
          stats.avgEntryPrice > CONFIG.MAX_WALLET_AVG_ENTRY_PRICE) {
        wallet.status = 'removed';
        wallet.removeReason = 'entry_price_too_high';
        decayed++;
        continue;
      }
      // Bot-pattern eviction — wallet drifted into bot behavior since last
      // rescore. Same logic as discovery gate. See detectBotPattern().
      if (CONFIG.BOT_PATTERN_GATE) {
        const bot = detectBotPattern(events);
        if (bot.isBot) {
          wallet.status = 'removed';
          wallet.removeReason = 'bot_pattern_rescore';
          wallet.removeDetail = `${bot.reason}: cryptoUpdownPct=${(bot.cryptoUpdownPct * 100).toFixed(0)}% medianBuy=$${bot.medianBuySize.toFixed(0)} tradesPerActiveWeek=${Math.round(bot.tradesPerActiveWeek || 0)}`;
          decayed++;
          continue;
        }
      }
      // Attach goldskyPnl + effectivePnl so scoring uses the better of the two.
      // Stage 0: economicPnl (analyzer-based trade + rewards + rebates +
      // MERGE closures) is the corrected sample view. Bare totalPnl left in
      // place for dashboard backwards-compatibility.
      stats.goldskyPnl = wallet.goldskyPnl || 0;
      const sampleEconomicRescore = stats.economicPnl != null ? stats.economicPnl : (stats.totalPnl || 0);
      stats.effectivePnl = Math.max(sampleEconomicRescore, stats.goldskyPnl);
      // Fold in the position-centric decided-truth rollup captured during the
      // goldsky refresh above. These are shadow fields today — computeWalletScore
      // doesn't consume them yet — but they're what the redesigned ranker keys on.
      if (wallet.decidedMetrics) {
        stats.decidedPnl = wallet.decidedMetrics.decidedPnl;
        stats.decidedCapital = wallet.decidedMetrics.decidedCapital;
        stats.decidedROI = wallet.decidedMetrics.decidedROI;
        stats.decidedWins = wallet.decidedMetrics.wins;
        stats.decidedLosses = wallet.decidedMetrics.losses;
        stats.decidedWinRate = wallet.decidedMetrics.winRate;
        stats.decidedOpenPositions = wallet.decidedMetrics.open;
        stats.decidedOpenCapitalAtRisk = wallet.decidedMetrics.openCapitalAtRisk;
        stats.decidedUnredeemedWinsPositions = wallet.decidedMetrics.unredeemedWins;
        stats.decidedWorthlessLosses = wallet.decidedMetrics.worthlessLosses;
        stats.isMeanPickerShape = wallet.decidedMetrics.isMeanPickerShape;
        stats.decidedMeasuredAt = wallet.decidedMetrics.measuredAt;
      }
      // Stage 1/2/3: MM classification + alpha evaluation + attribution
      // must run before scoring — their outputs feed into the score formula.
      attachMMClassification(stats);
      attachAlphaEvaluation(stats);
      attachAttribution(stats, attributionMap, addr);
      const scored = computeWalletScore(stats);
      if (scored && scored.score != null) {
        wallet.score = scored.score;
        wallet.scoreComponents = scored.components;
      }
      wallet.stats = stats;
      delete wallet.decidedMetrics;

      // ── Eviction (shadow or live) ─────────────────────────────────────
      // Only runs when we have real decided-metric-driven scores; skips
      // wallets still on aggregate-only data. Strike counters live on the
      // wallet so a single fluky rescore can't evict; pattern must persist.
      if (CONFIG.EVICTION_MODE !== 'off' && scored && scored.score != null) {
        wallet.strikes = wallet.strikes || { meanPicker: 0, lowScore: 0 };
        const evictions = [];

        // Rule 1: mean-picker shape, needs N consecutive strikes
        if (stats.isMeanPickerShape === true) {
          wallet.strikes.meanPicker++;
          if (wallet.strikes.meanPicker >= CONFIG.MEAN_PICKER_STRIKES_TO_EVICT) {
            evictions.push({ reason: 'mean_picker', detail: `${wallet.strikes.meanPicker} strikes, ROI=${(stats.decidedROI * 100).toFixed(1)}% WR=${((stats.decidedWinRate || 0) * 100).toFixed(0)}% cap=$${Math.round(stats.decidedCapital).toLocaleString()}` });
          }
        } else {
          wallet.strikes.meanPicker = 0;
        }

        // Rule 2: low score persisting N cycles
        if (scored.score < CONFIG.LOW_SCORE_THRESHOLD) {
          wallet.strikes.lowScore++;
          if (wallet.strikes.lowScore >= CONFIG.LOW_SCORE_STRIKES_TO_EVICT) {
            evictions.push({ reason: 'low_score', detail: `${wallet.strikes.lowScore} strikes @ score=${scored.score}` });
          }
        } else {
          wallet.strikes.lowScore = 0;
        }

        // Rule 3: money-loser with sample — single-shot, high confidence
        const resolved = (stats.decidedWins || 0) + (stats.decidedLosses || 0);
        if (stats.decidedROI != null && stats.decidedROI < 0
            && (stats.decidedCapital || 0) >= CONFIG.NEG_ROI_CAPITAL_FLOOR
            && resolved >= CONFIG.NEG_ROI_MIN_RESOLVED) {
          evictions.push({ reason: 'neg_roi', detail: `ROI=${(stats.decidedROI * 100).toFixed(1)}% on $${Math.round(stats.decidedCapital).toLocaleString()} across ${resolved} markets` });
        }

        // Rule 3b: low-ROI — positive but chronically weak. Wallet sitting
        // around 2-4% ROI with trustworthy sample is a mean-picker in all
        // but name. Can't source 75%-hit-rate signals.
        if (stats.decidedROI != null
            && stats.decidedROI >= 0
            && stats.decidedROI < CONFIG.LOW_ROI_THRESHOLD
            && (stats.decidedCapital || 0) >= CONFIG.LOW_ROI_MIN_CAPITAL
            && resolved >= CONFIG.LOW_ROI_MIN_RESOLVED) {
          evictions.push({ reason: 'low_roi', detail: `ROI=${(stats.decidedROI * 100).toFixed(1)}% < ${(CONFIG.LOW_ROI_THRESHOLD * 100).toFixed(0)}% floor on $${Math.round(stats.decidedCapital).toLocaleString()} across ${resolved} markets` });
        }

        // Rule 4: dormancy at the tighter pool-maintenance floor (separate
        // from the broader MAX_INACTIVE_DAYS that governs the whole loop).
        const daysSinceLastTrade = stats.lastTradeTs > 0
          ? (Date.now() / 1000 - stats.lastTradeTs) / 86400
          : Infinity;
        if (daysSinceLastTrade > CONFIG.DORMANCY_DAYS) {
          evictions.push({ reason: 'dormant', detail: `${daysSinceLastTrade.toFixed(0)}d since last trade` });
        }

        if (evictions.length > 0) {
          const first = evictions[0];
          if (CONFIG.EVICTION_MODE === 'live') {
            wallet.status = 'removed';
            wallet.removeReason = first.reason;
            wallet.removeDetail = first.detail;
            decayed++;
            console.log(`    ✂ ${addr.slice(0, 10)} evicted: ${first.reason} (${first.detail})`);
            continue;
          } else {
            // Shadow mode — tag the wallet so we can audit without removing
            wallet.wouldEvict = { reason: first.reason, detail: first.detail, flaggedAt: new Date().toISOString() };
            console.log(`    ◇ ${addr.slice(0, 10)} shadow-evict: ${first.reason} (${first.detail})`);
          }
        } else if (wallet.wouldEvict) {
          delete wallet.wouldEvict;
        }
      }

      wallet.lastScored = new Date().toISOString();
      wallet.totalTrades = events.length;
      rescored++;
    } catch (err) {
      // Keep existing score on error
    }
  }
  if (decayed > 0 || rescored > 0) {
    console.log(`  Pool maintenance: ${rescored} re-scored, ${decayed} removed${pnlDecayed > 0 ? ` (${pnlDecayed} below PnL floor)` : ''}`);
  }

  // Step 6: Rank and trim to top N (with minimum score floor)
  //
  // Grace period: after the WR fix ships, scores will recalibrate downward
  // across the whole pool as wallets get re-scored with the honest WR/PnL.
  // If we apply MIN_SCORE_POOL mid-recalibration we risk evicting wallets
  // whose score is TEMPORARILY low only because they haven't been re-scored
  // yet. Detect this by counting how many wallets in the pool still carry
  // pre-fix stats (no `unredeemedWins` field — it was added by the fix).
  //
  // If >20% of the pool is still pre-fix, skip score-based eviction this cycle.
  // Lifetime-PnL-based eviction (via goldskyPnl < MIN_PNL_DISCOVERY in the
  // re-score loop) and inactive-wallet eviction stay active throughout, so
  // nothing garbage sneaks in during the grace window.
  const poolEntries = Object.entries(pool);
  const preFixCount = poolEntries.filter(([, w]) =>
    w.status !== 'removed' && (w.stats && w.stats.unredeemedWins === undefined)
  ).length;
  const activeCount = poolEntries.filter(([, w]) => w.status !== 'removed').length;
  const preFixRatio = activeCount > 0 ? preFixCount / activeCount : 0;
  const graceActive = preFixRatio > 0.2;
  if (graceActive) {
    console.log(`  ⏳ Score-eviction grace period active: ${preFixCount}/${activeCount} wallets still pre-fix (${(preFixRatio * 100).toFixed(0)}%). Skipping MIN_SCORE_POOL filter this cycle.`);
  }

  // Rank by the single authoritative score. Wallets without a score yet
  // (freshly admitted, not yet rescored) get protected from eviction so
  // they have a chance to earn their place on next cycle.
  const activePool = poolEntries.filter(([, w]) => w.status !== 'removed');
  const scoredCount = activePool.filter(([, w]) => typeof w.score === 'number').length;
  const scoreOf = (w) => typeof w.score === 'number' ? w.score : 0;

  console.log(`  Scoring coverage: ${scoredCount}/${activePool.length} (${activePool.length > 0 ? Math.round(100 * scoredCount / activePool.length) : 0}%)`);

  let ranked = Object.entries(pool)
    .filter(([, w]) => w.status !== 'removed' && (graceActive || scoreOf(w) >= CONFIG.MIN_SCORE_POOL))
    .sort((a, b) => scoreOf(b[1]) - scoreOf(a[1]));

  // Safety net: if score filtering wiped >50% of active wallets, something is
  // miscalibrated. Fall back to keeping all non-removed wallets rather than
  // silently collapsing the pool.
  if (!graceActive && ranked.length < activePool.length * 0.5) {
    console.log(`  ⚠ Score filter would keep only ${ranked.length}/${activePool.length} wallets — likely miscalibrated. Bypassing MIN_SCORE filter this cycle.`);
    ranked = Object.entries(pool)
      .filter(([, w]) => w.status !== 'removed')
      .sort((a, b) => scoreOf(b[1]) - scoreOf(a[1]));
  }

  const trimmedPool = {};
  let rank = 0;
  let unscoredProtected = 0;
  for (const [addr, wallet] of ranked) {
    rank++;
    if (rank <= CONFIG.TARGET_POOL_SIZE) {
      wallet.rank = rank;
      trimmedPool[addr] = wallet;
    } else if (typeof wallet.score !== 'number') {
      // Protection: don't evict wallets that haven't been scored yet. They
      // keep their pool spot until they get a score, at which point they
      // compete fairly.
      wallet.rank = rank;
      trimmedPool[addr] = wallet;
      unscoredProtected++;
    }
  }
  if (unscoredProtected > 0) {
    console.log(`  🛡 Unscored protection: ${unscoredProtected} wallets kept in pool pending first score`);
  }

  console.log(`\n  ✅ Wallet pool: ${Object.keys(trimmedPool).length} wallets (from ${qualified} qualified)`);
  const topWallets = ranked.slice(0, 5);
  for (const [addr, w] of topWallets) {
    // Display singleSideROI (per-trade ROI from full /activity event log).
    // decidedROI from /positions snapshot is biased toward worthless
    // leftovers — see frontend/app.js notes (commit 3eaea2d). Fall back to
    // decidedROI only when singleSide unavailable. V2 scoring uses neither
    // as a primary input, so this is purely cosmetic.
    const ssROI = w.stats?.singleSideROI;
    const dROI = w.stats?.decidedROI;
    const roi = ssROI != null
      ? ` ROI:${(ssROI * 100).toFixed(0)}%`
      : (dROI != null ? ` ROI:${(dROI * 100).toFixed(0)}%*` : '');
    console.log(`    #${w.rank} ${addr.slice(0, 12)}... score:${w.score ?? '—'} WR:${((w.stats?.recentWinRate || w.stats?.winRate || 0) * 100).toFixed(0)}% PnL:$${(w.stats?.totalPnl || 0).toFixed(0)}${roi}`);
  }

  // Step 7: Snapshot each wallet's lifetime PnL into the history ledger so we
  // can build equity-curve time series for every tracked wallet. Append-only,
  // capped per-wallet to keep the file bounded.
  try {
    snapshotWalletHistory(trimmedPool, state.scanCount);
  } catch (err) {
    console.error(`  ⚠ Wallet history snapshot failed: ${err.message}`);
  }

  return trimmedPool;
}

/**
 * Append a PnL snapshot row for every wallet in the pool to
 * data/wallet-history.json.gz. One row per discovery cycle per wallet.
 * Used to reconstruct equity curves and detect regime changes.
 */
const WALLET_HISTORY_FILE = path.join(DATA_DIR, 'wallet-history.json.gz');
const WALLET_HISTORY_MAX_ROWS = 180; // ~6 months at once/day

function snapshotWalletHistory(pool, scanCount) {
  const existing = loadGzJSON(WALLET_HISTORY_FILE) || { wallets: {} };
  if (!existing.wallets) existing.wallets = {};

  const ts = new Date().toISOString();
  let added = 0;
  for (const [addr, wallet] of Object.entries(pool)) {
    if (!existing.wallets[addr]) existing.wallets[addr] = [];
    const rows = existing.wallets[addr];

    // Deduplicate: don't snapshot the same scan twice
    if (rows.length && rows[rows.length - 1].scan === scanCount) continue;

    rows.push({
      scan: scanCount,
      ts,
      goldskyPnl: +(wallet.goldskyPnl || 0).toFixed(2),
      goldskyPositions: wallet.goldskyPositions || 0,
      score: +(wallet.score || 0).toFixed(1),
      resolvedMarkets: wallet.stats?.resolvedMarkets || 0,
      winRate: +(wallet.stats?.winRate || 0).toFixed(4),
      samplePnl: +(wallet.stats?.totalPnl || 0).toFixed(2),
      tradesTruncated: wallet.stats?.tradesTruncated === true,
    });

    // Cap to last N rows per wallet
    if (rows.length > WALLET_HISTORY_MAX_ROWS) {
      rows.splice(0, rows.length - WALLET_HISTORY_MAX_ROWS);
    }
    added++;
  }

  saveGzJSON(WALLET_HISTORY_FILE, existing);
  console.log(`  📈 Wallet history: ${added} snapshots appended (${Object.keys(existing.wallets).length} wallets tracked)`);
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
  // Only iterate ACTIVE wallets — Object.keys(walletPool) would include
  // status='removed' entries and waste an /activity API call per evicted
  // wallet (~270 wasted calls/scan post-V2 sweep).
  const activeAddresses = Object.entries(walletPool)
    .filter(([, w]) => w?.status !== 'removed')
    .map(([addr]) => addr);
  console.log(`  Tracked wallets: ${activeAddresses.length}`);

  // Step 1: Fetch recent trades for all tracked wallets
  const lookbackTs = Math.floor(Date.now() / 1000) - (CONFIG.LOOKBACK_HOURS * 3600);
  const walletAddresses = activeAddresses;

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

  // Also refresh active signal tokens (by clob_token_ids first)
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

  // Refresh signal markets via Gamma /events?slug — catches resolved markets
  // that clob_token_ids lookup misses. Include BOTH active signals AND
  // history signals with no outcome, so the repair phase can backfill them.
  // Pass eventSlug + slug through — refreshSignalMarkets needs them to
  // query /events?slug, otherwise every signal gets skipped as "missing slug".
  const activeSignalsList = Object.values(existingSignals.active || {})
    .filter(s => s.conditionId)
    .map(s => ({ tokenId: s.tokenId, conditionId: s.conditionId, eventSlug: s.eventSlug, slug: s.slug }));

  // Terminal closeReasons: signal already finalized as unresolvable. Don't
  // re-fetch every loop — the four-source resolver chase already failed and
  // the market state won't change. Saves up to 30 Gamma /events queries/loop.
  const TERMINAL_CLOSE_REASONS = new Set([
    'voided_no_outcome',         // resolver chase failed; market voided
    'unresolved_void',           // never got a resolution; treated as void
    'past_enddate_no_close',     // endDate passed but no payout fixed
    'market_delisted',           // pulled from Polymarket
    'force_resolve_recovered',   // recovered via fallback; already terminal
    'missing_open_price',        // can't compute return without open price
  ]);

  const historyNeedingRepair = (Array.isArray(existingSignals.history) ? existingSignals.history : Object.values(existingSignals.history || {}))
    .filter(s => s.conditionId
      && s.outcome !== 'win'
      && s.outcome !== 'loss'
      && s.outcome !== 'void'                                // already terminal
      && !TERMINAL_CLOSE_REASONS.has(s.closeReason))         // already-failed chase
    .map(s => ({ tokenId: s.tokenId, conditionId: s.conditionId, eventSlug: s.eventSlug, slug: s.slug }));

  const allSignalsToRefresh = [...activeSignalsList, ...historyNeedingRepair];
  // Deduplicate by conditionId
  const seen = new Set();
  const uniqueSignals = allSignalsToRefresh.filter(s => {
    if (seen.has(s.conditionId)) return false;
    seen.add(s.conditionId);
    return true;
  });

  if (uniqueSignals.length > 0) {
    console.log(`  Refreshing ${uniqueSignals.length} signal markets (${activeSignalsList.length} active, ${historyNeedingRepair.length} history needing repair)`);
    await refreshSignalMarkets(uniqueSignals, marketLookup);
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

  // Force-refresh market data for top candidates BEFORE processSignals runs.
  // resolveMarkets() upstream skips tokens already in the lookup cache, so
  // candidate markets retained stale prices/closed-status. That meant the
  // stale-follower gate in processSignals never fired even when actual
  // Polymarket prices had moved 30¢+ since the wallet's buy.
  // refreshSignalMarkets() force-refreshes via the /events endpoint.
  // Capped at 50 candidates to bound API cost — those are the highest-
  // walletCount ones most likely to actually emit.
  const REFRESH_TOP_N = 50;
  const refreshCandidates = candidates.slice(0, REFRESH_TOP_N).map(c => ({
    tokenId: c.asset,
    conditionId: c.conditionId,
    eventSlug: c.eventSlug,
    slug: c.slug,
  }));
  if (refreshCandidates.length > 0) {
    await refreshSignalMarkets(refreshCandidates, marketLookup);
  }

  // Step 4: Generate/update signals
  const updatedSignals = processSignals(
    candidates, existingSignals, recentTrades, walletPoolMap, marketLookup, scanIndex
  );

  console.log(`  Signals: ${updatedSignals.stats.opened} opened, ${updatedSignals.stats.updated} updated, ${updatedSignals.stats.closed} closed`);
  console.log(`  Active: ${updatedSignals.stats.activeCount} | History: ${updatedSignals.stats.historyCount} | WR: ${updatedSignals.stats.winRate}%`);

  // Step 4b: Handpicked-wallet signals — separate parallel track
  // (manually-curated wallets bypass scoring/admission; every BUY emits
  // a signal). See scanner/handpickedSignals.js for the full design.
  const handpickedFile = path.join(DATA_DIR, 'handpicked-wallets.json.gz');
  if (fs.existsSync(handpickedFile)) {
    try {
      const handpickedStore = loadGzJSON(handpickedFile);
      const handpickedList = handpickedStore?.wallets || [];
      if (handpickedList.length > 0) {
        // Fetch trades for handpicked wallets if not already in recentTrades.
        // Most handpicked wallets won't be in the regular tracked pool, so
        // fetch them as a batch via fetchRecentTrades.
        const missing = handpickedList
          .map(w => w.address.toLowerCase())
          .filter(addr => !recentTrades.has(addr));
        let handpickedTrades = new Map();
        if (missing.length > 0) {
          try {
            handpickedTrades = await fetchRecentTrades(missing, lookbackTs);
          } catch (err) { /* tolerate; skip this scan if API hiccup */ }
        }
        // Merge in any handpicked wallets that ARE already in recentTrades
        for (const w of handpickedList) {
          const addr = w.address.toLowerCase();
          if (recentTrades.has(addr)) handpickedTrades.set(addr, recentTrades.get(addr));
        }

        const hpResult = processHandpickedSignals(
          handpickedTrades, handpickedList, updatedSignals, marketLookup, scanIndex
        );
        // Merge handpicked signals into updatedSignals.active (history/stats unchanged here)
        updatedSignals.active = hpResult.active;
        updatedSignals.stats.activeCount = Object.keys(updatedSignals.active).length;
        console.log(`  Handpicked: ${hpResult.opened} opened, ${hpResult.updated} updated  (pool=${handpickedList.length})`);
        if (hpResult.opened === 0 && hpResult.updated === 0) {
          const reasons = Object.entries(hpResult.kills).filter(([, n]) => n > 0).map(([k, n]) => `${k}=${n}`).join(', ');
          if (reasons) console.log(`    HP kills: ${reasons}`);
        }
      }
    } catch (err) {
      console.log(`  Handpicked: error — ${err.message}`);
    }
  }

  // Step 5: Paper trading
  const paperFile = path.join(DATA_DIR, 'paper-trades.json.gz');
  let paperState = loadGzJSON(paperFile) || initPaperTrading();
  paperState = processPaperTrades(updatedSignals, paperState, scanIndex);

  // Step 6: Save everything
  saveGzJSON(signalsFile, updatedSignals);
  saveGzJSON(paperFile, paperState);
  saveGzJSON(path.join(DATA_DIR, 'markets.json.gz'), Object.fromEntries(marketLookup));

  // Step 7: Build full analytics for frontend
  const analyticsFile = path.join(DATA_DIR, 'analytics.json.gz');
  let analytics = loadGzJSON(analyticsFile) || { trendline: [] };

  // 7a: Trendline
  // trackedWallets counts only ACTIVE wallets — Object.keys(walletPool).length
  // would include status='removed' entries (eviction sets status but doesn't
  // delete the entry, so stats and history can be retained). Pre-fix the chart
  // showed e.g. 828 instead of 557 active, hiding any post-eviction drop.
  analytics.trendline.push({
    scanIndex,
    timestamp: new Date().toISOString(),
    trackedWallets: Object.values(walletPool).filter(w => w?.status !== 'removed').length,
    activeWalletsThisScan: recentTrades.size,
    newTrades: totalNewTrades,
    convergenceCandidates: candidates.length,
    activeSignals: updatedSignals.stats.activeCount,
    signalsOpened: updatedSignals.stats.opened,
    signalsClosed: updatedSignals.stats.closed,
    totalHistory: updatedSignals.stats.historyCount,
    winRate: updatedSignals.stats.winRate,
  });
  if (analytics.trendline.length > 2000) {
    analytics.trendline.splice(0, analytics.trendline.length - 2000);
  }

  // 7b: Leaderboard — wallet list for Dashboard tab.
  const walletList = Object.values(walletPool)
    .filter(w => typeof w.score === 'number' && w.score > 0 && w.status !== 'removed')
    .sort((a, b) => (b.score || 0) - (a.score || 0));

  analytics.leaderboard = walletList.map((w, idx) => ({
    rank: idx + 1,
    address: w.address,
    score: typeof w.score === 'number' ? w.score : null,
    scoreComponents: w.scoreComponents || null,
    // Shadow-eviction flag — populated when a wallet matches an eviction
    // rule but EVICTION_MODE is 'shadow' rather than 'live'.
    wouldEvict: w.wouldEvict || null,
    lastActiveTimestamp: w.lastScored || w.discoveredScan ? new Date().toISOString() : null,
    stats: {
      // totalPnl   = Goldsky on-chain realized (only counts explicitly redeemed/sold positions).
      //              Same number MIN_PNL_DISCOVERY gates against. Misses unredeemed winners.
      // samplePnl  = Analyzer's PnL from /activity events, capped at 3000 events.
      //              Handles unredeemed winners but truncates deep history.
      // effectivePnl = max(onChain, sample) — what scoring actually uses, since both
      //                measurement systems are incomplete in opposite ways.
      totalPnl: w.goldskyPnl || w.stats?.totalPnl || 0,
      samplePnl: w.stats?.totalPnl || 0,
      effectivePnl: w.stats?.effectivePnl || Math.max(w.goldskyPnl || 0, w.stats?.totalPnl || 0),
      realizedPnl: w.goldskyPnl || w.stats?.totalPnl || 0,
      unrealizedPnl: 0,
      wr: w.stats?.winRate || w.stats?.recentWinRate || 0,
      estimatedMarkets: w.stats?.totalMarkets || w.stats?.resolvedMarkets || 0,
      resolved: w.stats?.resolvedMarkets || 0,
      wins: w.stats?.wins || 0,
      losses: w.stats?.losses || 0,
      efficiency: w.stats?.totalPnl && w.stats?.totalVolume
        ? w.stats.totalPnl / w.stats.totalVolume : 0,
      edgeRatio: w.stats?.avgPnlPerWin && w.stats?.avgPnlPerLoss
        ? Math.abs(w.stats.avgPnlPerWin / w.stats.avgPnlPerLoss) : 0,
      totalVolume: w.stats?.totalVolume || 0,
      openCount: 0,
      // Trades/week: use recentTradesPerDay (90-day fixed window) × 7 rather
      // than tradesPerActiveWeek, which divides by a variable activeWeeks that
      // collapses to ~1-2 when the 3000-event API cap truncates history. That
      // bug produced dashboard values like "3000/wk" for wallets with <100
      // resolved markets. The fixed 90-day denominator keeps the metric a
      // stable proxy for "how actively and consistently does this wallet trade".
      positionsPerWeek: +((w.stats?.recentTradesPerDay || 0) * 7).toFixed(1),
      activeWeeks: w.stats?.activeWeeks || 0,
      weeklyConsistency: w.stats?.weeklyConsistency || 0,
      tradingDays: w.stats?.activeDays || 0,
      // Sample-window metadata so the dashboard can explain what the
      // behavioural numbers (wr, samplePnl, wins, losses) actually cover.
      // statsSpanDays: how many days of history the analyzer saw. When
      // tradesTruncated=true, the 3000-offset cap cut off earlier history,
      // so this is the effective recency window — a whale might only have
      // 28 days of sample while a quiet wallet has 300.
      statsSpanDays: w.stats?.statsSpanDays || 0,
      tradesTruncated: w.stats?.tradesTruncated === true,
      // Post-fix transparency: how many open positions were closed by
      // consulting marketLookup (only populated once analyzer has run
      // with the WR fix).
      unredeemedWins: w.stats?.unredeemedWins || 0,
      worthlessLosses: w.stats?.worthlessLosses || 0,
      // Phase 1 ground-truth fields — null until rescored under new pipeline
      decidedROI: w.stats?.decidedROI ?? null,
      decidedCapital: w.stats?.decidedCapital ?? null,
      decidedPnl: w.stats?.decidedPnl ?? null,
      decidedWins: w.stats?.decidedWins ?? null,
      decidedLosses: w.stats?.decidedLosses ?? null,
      decidedWinRate: w.stats?.decidedWinRate ?? null,
      decidedOpenCapitalAtRisk: w.stats?.decidedOpenCapitalAtRisk ?? null,
      isMeanPickerShape: w.stats?.isMeanPickerShape === true,
      decidedMeasuredAt: w.stats?.decidedMeasuredAt || null,
      // singleSide* — per-trade ROI from full /activity event log. The
      // dashboard 'ROI' column displays these as PRIMARY because the
      // /positions snapshot used for decidedROI is biased toward
      // worthless-loss leftovers (winners get redeemed and disappear,
      // losses stay around as worthless shares). Without these in the
      // leaderboard projection, the frontend falls back to decidedROI
      // and shows -100% for ~73% of wallets even when their actual
      // trade-level performance is positive. See commit 3eaea2d.
      singleSideROI: w.stats?.singleSideROI ?? null,
      singleSideCapital: w.stats?.singleSideCapital ?? null,
      singleSideHitRate: w.stats?.singleSideHitRate ?? null,
      singleSideResolved: w.stats?.singleSideResolved ?? null,
    },
  }));

  // 7c: Summary
  const totalWallets = walletList.length;
  const totalPnl = walletList.reduce((s, w) => s + (w.goldskyPnl || w.stats?.totalPnl || 0), 0);
  const totalWins = walletList.reduce((s, w) => s + (w.stats?.wins || 0), 0);
  const totalResolved = walletList.reduce((s, w) => s + (w.stats?.resolvedMarkets || 0), 0);

  const scoredWallets = walletList.filter(w => typeof w.score === 'number');
  analytics.summary = {
    totalWallets,
    totalPnl,
    avgScore: scoredWallets.length > 0
      ? scoredWallets.reduce((s, w) => s + w.score, 0) / scoredWallets.length
      : 0,
    scoringCoverage: totalWallets > 0 ? scoredWallets.length / totalWallets : 0,
    meanPickerCount: walletList.filter(w => w.stats?.isMeanPickerShape === true).length,
    shadowEvictionCount: walletList.filter(w => w.wouldEvict).length,
    winRate: totalResolved > 0 ? totalWins / totalResolved : 0,
    totalWins,
    totalResolved,
  };

  // 7d: Consensus — markets where multiple tracked wallets recently bought
  // Build from convergence candidates (already computed above)
  analytics.consensus = candidates.map(c => ({
    marketTitle: c.title || 'Unknown',
    tokenId: c.conditionId,
    slug: c.slug || '',
    eventSlug: c.eventSlug || '',
    walletCount: c.walletCount,
    avgScore: c.avgScore,
    direction: c.direction || 'mixed',
    consensusStrength: c.avgScore / 100,
    conviction: c.totalBuySize,
    totalBuySize: c.totalBuySize,
    avgEntryPrice: c.avgEntryPrice || 0,
    convergenceSpanHours: c.convergenceSpanHours || 0,
    wallets: (c.wallets || []).map(w => ({
      address: w.address,
      score: w.score || 0,
    })),
  }));

  // 7e: Active Positions — aggregate from recent trades for Portfolio tab
  // Group all recent BUY trades by market
  const marketHolders = new Map(); // conditionId → { title, holders[] }
  for (const [address, trades] of recentTrades) {
    const wallet = walletPool[address];
    if (!wallet) continue;
    for (const trade of trades) {
      if (trade.side !== 'BUY') continue;
      const cid = trade.conditionId || trade.asset;
      if (!marketHolders.has(cid)) {
        const mi = marketLookup.get(trade.asset) || marketLookup.get(cid) || {};
        marketHolders.set(cid, {
          marketTitle: mi.title || trade.title || 'Unknown',
          slug: mi.slug || '',
          tokenId: cid,
          holders: [],
        });
      }
      const entry = marketHolders.get(cid);
      // Avoid duplicate wallet entries per market
      if (!entry.holders.find(h => h.address === address)) {
        entry.holders.push({
          address,
          score: wallet.score,
          shares: parseFloat(trade.size) || 0,
          entryPrice: parseFloat(trade.price) || 0,
          positionValue: (parseFloat(trade.size) || 0) * (parseFloat(trade.price) || 0),
          currentPnl: 0,
        });
      }
    }
  }

  analytics.activePositions = [...marketHolders.values()]
    .filter(m => m.holders.length >= 1)
    .sort((a, b) => b.holders.length - a.holders.length)
    .map(m => ({
      ...m,
      holderCount: m.holders.length,
      totalShares: m.holders.reduce((s, h) => s + h.shares, 0),
      totalValue: m.holders.reduce((s, h) => s + h.positionValue, 0),
      totalPnl: m.holders.reduce((s, h) => s + h.currentPnl, 0),
      avgEntryPrice: m.holders.length > 0
        ? m.holders.reduce((s, h) => s + h.entryPrice, 0) / m.holders.length : 0,
    }));

  // 7f: Win Patterns — aggregate stats from wallet trade histories
  let patternWins = 0, patternLosses = 0, patternTotalPnl = 0;
  const sizeBuckets = {
    small: { wins: 0, count: 0 },   // < $100
    medium: { wins: 0, count: 0 },  // $100-$1000
    large: { wins: 0, count: 0 },   // > $1000
  };

  for (const w of walletList) {
    const s = w.stats || {};
    patternWins += (s.wins || 0);
    patternLosses += (s.losses || 0);
    patternTotalPnl += (s.totalPnl || 0);

    // Approximate size bucket distribution from wallet-level data
    const avgSize = s.totalVolume && s.resolvedMarkets
      ? s.totalVolume / s.resolvedMarkets : 0;
    const resolved = s.resolvedMarkets || 0;
    const wins = s.wins || 0;
    if (avgSize < 100) {
      sizeBuckets.small.count += resolved;
      sizeBuckets.small.wins += wins;
    } else if (avgSize < 1000) {
      sizeBuckets.medium.count += resolved;
      sizeBuckets.medium.wins += wins;
    } else {
      sizeBuckets.large.count += resolved;
      sizeBuckets.large.wins += wins;
    }
  }

  const patternTotal = patternWins + patternLosses;
  analytics.winPatterns = {
    overallStats: {
      winRate: patternTotal > 0 ? patternWins / patternTotal : 0,
      totalTrades: patternTotal,
      totalPnl: patternTotalPnl,
      avgPnl: patternTotal > 0 ? patternTotalPnl / patternTotal : 0,
    },
    sizeBuckets,
    topWinningMarkets: [], // Would require per-market resolution data; placeholder for now
  };

  // 7g: Signals — embed into analytics for frontend
  analytics.signals = {
    active: Object.values(updatedSignals.active || {}),
    history: updatedSignals.history || [],
    stats: updatedSignals.stats || {},
  };
  analytics.scanCount = scanIndex;

  // 7h: Paper trading — embed into analytics for frontend
  analytics.paperTrading = paperState;

  analytics.timestamp = new Date().toISOString();

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

  // One-time migration from legacy dual-score schema (score/scoreV2 split).
  // Pool entries written pre-consolidation may have both fields; the V2
  // field was the authoritative one. Copy it onto `score` and drop V2
  // vocabulary entirely so downstream code sees one clean field.
  let migrated = 0;
  let legacyScoresNuked = 0;
  for (const addr of Object.keys(walletPool)) {
    const w = walletPool[addr];
    if (!w || typeof w !== 'object') continue;
    // scoreV2 was authoritative post-Phase-5; prefer it over legacy score
    if (typeof w.scoreV2 === 'number') {
      w.score = w.scoreV2;
      migrated++;
    }
    if (w.scoreV2Components) {
      w.scoreComponents = w.scoreComponents || w.scoreV2Components;
    }
    if (w.v2Strikes) {
      w.strikes = w.strikes || w.v2Strikes;
    }
    if (w.v2WouldEvict) {
      w.wouldEvict = w.wouldEvict || w.v2WouldEvict;
    }
    delete w.scoreV2;
    delete w.scoreV2Components;
    delete w.v2Strikes;
    delete w.v2WouldEvict;

    // Legacy V1 score cleanup. Unified-formula scores cap at ~55 in
    // practice (50 roi + 5 activity, multiplicatively gated by ≤ 1.0).
    // Any wallet scoring > 60 AND lacking decidedROI in stats is holding
    // a stale V1-formula score that never got overwritten (because
    // computeWalletScore returns null when decidedROI is null, and the
    // rescore path only overwrites score when null != score). Nuke it so
    // the wallet either re-scores correctly on its next rescore cycle
    // (once it gains decidedROI data) or goes unranked until it proves
    // itself. Prevents ghost wallets dominating the top of the pool on
    // outdated scoring.
    if (typeof w.score === 'number' && w.score > 60
        && (!w.stats || w.stats.decidedROI == null)) {
      delete w.score;
      delete w.scoreComponents;
      legacyScoresNuked++;
    }
  }
  if (migrated > 0) {
    console.log(`  📦 Migrated ${migrated} wallets from legacy scoreV2 → score`);
  }
  if (legacyScoresNuked > 0) {
    console.log(`  🧹 Nuked ${legacyScoresNuked} stale legacy V1 scores (will re-rank on next rescore)`);
  }

  const marketsFile = path.join(DATA_DIR, 'markets.json.gz');
  const existingMarkets = loadGzJSON(marketsFile) || {};
  const marketLookup = new Map(Object.entries(existingMarkets));

  // Build attribution map once per scan — used by computeWalletScore to
  // apply the signal-outcome feedback loop. Wallets with ≥ 10 historical
  // signals get their score scaled by their proven signal EV.
  const signalsFileForAttr = path.join(DATA_DIR, 'signals.json.gz');
  const signalsDataForAttr = loadGzJSON(signalsFileForAttr) || {};
  const attributionMap = buildAttributionMap(signalsDataForAttr.history || []);

  // Pool size for logs and discovery decisions = ACTIVE wallets only.
  // Object.keys(walletPool).length includes status='removed' entries which
  // makes pool look bigger than it is and can suppress discovery (e.g. if
  // active=557 but total=828, "below target" check might incorrectly pass).
  const activePoolEntries = Object.entries(walletPool).filter(([, w]) => w?.status !== 'removed');
  const activePoolSize = activePoolEntries.length;
  console.log(`\n📋 State: Scan #${state.scanCount}`);
  console.log(`  Wallet pool: ${activePoolSize} active (${Object.keys(walletPool).length} total in store)`);
  console.log(`  Known markets: ${marketLookup.size}`);
  console.log(`  Attribution map: ${attributionMap.size} wallets with signal history`);
  console.log(`  Last discovery: scan #${state.lastDiscovery}`);

  // Decide whether to run discovery (slow loop)
  // Force discovery if pool is below target (e.g. after state reset)
  const poolSize = activePoolSize;
  const poolEmpty = poolSize === 0;
  const poolBelowTarget = poolSize < CONFIG.TARGET_POOL_SIZE * 0.5;
  const needsDiscovery = poolEmpty ||
    poolBelowTarget ||
    (state.scanCount - state.lastDiscovery) >= CONFIG.DISCOVERY_INTERVAL_SCANS;

  if (needsDiscovery) {
    // Only reset cursor if pool is completely empty (genuine crash)
    // When pool is just below target, let the cursor ADVANCE through new positions
    // so we discover wallets from different parts of the dataset
    if (poolEmpty && state.lastId) {
      console.log('  Pool is EMPTY — resetting Goldsky cursor to start');
      state.lastId = '';
    } else if (poolBelowTarget) {
      console.log(`  Pool below target (${poolSize}/${CONFIG.TARGET_POOL_SIZE}) — forcing discovery but keeping cursor position to scan new wallets`);
    }
    walletPool = await discoverWallets(state, walletPool, marketLookup, attributionMap);
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
