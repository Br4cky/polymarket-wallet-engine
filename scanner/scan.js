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
  analyzeTradeHistory,
  computeWalletScore,
  computeWalletScoreV2,
} from './dataApi.js';

import {
  SIGNAL_THRESHOLDS,
  detectConvergence,
  processSignals,
} from './signals.js';

import { aggregatePositions } from './positionLedger.js';
import { attachMMClassification } from './mmClassifier.js';
import { attachAlphaEvaluation, ALPHA_THRESHOLDS } from './alphaTest.js';

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
  TARGET_POOL_SIZE: 1000,          // Top N to keep after scoring
  MIN_SCORE_POOL: 50,              // Minimum score to enter the pool — filters out noise (legacy 0-100)
  MIN_SCORE_POOL_V2: 25,           // V2 equivalent (V2 scores cap ~55; 25 ≈ same selectivity as 50/100)
  MIN_PNL_DISCOVERY: 500,          // Minimum PnL to even fetch trade history
  MIN_POSITIONS_DISCOVERY: 10,     // Minimum positions on Goldsky to bother checking
  MIN_RESOLVED_MARKETS: 10,        // Minimum resolved markets to enter pool — no flukes
  // Max capital-weighted avg entry price. 0.85 ⇒ wallet's typical trade must
  // have ≥17.6% implied max ROI. Keeps the pool aligned with the signal
  // engine's MIN_OPEN_ROI (15%) — scrap-graders can no longer ride WR into
  // the pool just to produce signals we'd then filter out anyway.
  // 0 or 1 = disabled.
  MAX_WALLET_AVG_ENTRY_PRICE: 0.85,
  MAX_INACTIVE_DAYS: 60,           // Must have traded within last 60 days
  DISCOVERY_INTERVAL_SCANS: 3,     // Run full discovery every N fast-loop scans
  RESCORE_BATCH_SIZE: 100,         // Wallets to rescore per discovery cycle

  // Phase 1 of the scoring redesign: during the rescore loop, fetch the full
  // per-position breakdown from Goldsky (tokenId/amount/avgPrice) and compute
  // decidedROI / decidedCapital / mean-picker flag. Attaches to
  // wallet.stats.decided* without yet changing the ranker — shadow data for
  // Phase 2's computeWalletScoreV2. Flip off to fall back to aggregate-only.
  ENABLE_DECIDED_METRICS: true,

  // Phase 4 eviction: act on the V2 shadow metrics.
  //   'off'    — do nothing (original behaviour pre-Phase-4)
  //   'shadow' — log what would be evicted, don't remove
  //   'live'   — actually remove matching wallets
  // Mean-picker / low-score rules use a strike counter so a single fluky
  // rescore can't evict a real wallet. Dormancy and negROI with meaningful
  // capital are single-shot because they're already high-confidence signals.
  V2_EVICTION_MODE: 'shadow',
  V2_MEAN_PICKER_STRIKES_TO_EVICT: 3,
  V2_LOW_SCORE_THRESHOLD: 15,
  V2_LOW_SCORE_STRIKES_TO_EVICT: 3,
  V2_NEG_ROI_CAPITAL_FLOOR: 10000,   // decidedCapital ≥ $10k + ROI<0 = evict
  V2_NEG_ROI_MIN_RESOLVED: 25,       // AND resolved ≥ 25 markets
  V2_DORMANCY_DAYS: 30,              // no trade in last 30 days = evict

  // Phase 5 ranker promotion: once ≥ V2_MIN_POOL_COVERAGE_PCT of the pool
  // has scoreV2 populated, flip the ranker to sort on scoreV2 instead of
  // legacy score. Until coverage reaches the floor, legacy stays primary.
  USE_SCORE_V2: true,
  V2_MIN_POOL_COVERAGE_PCT: 50,

  // Phase 6 discovery gates: reject candidates at the door on the same
  // shape the rescore loop would evict. Cheaper than admit-then-evict and
  // keeps the pool clean from first contact. Set DISCOVERY_V2_GATE='off'
  // to revert to legacy-only admission.
  DISCOVERY_V2_GATE: 'on',
  DISCOVERY_MAX_INACTIVE_DAYS: 30,           // tighter than MAX_INACTIVE_DAYS
  DISCOVERY_MIN_DECIDED_CAPITAL: 5000,       // must have risked $5k+ on resolved plays
  DISCOVERY_MIN_DECIDED_ROI: 0.08,           // must average 8%+ ROI on resolved capital
  DISCOVERY_MAX_WIN_RATE: 0.98,              // reject obvious mean-picker shape at door
  DISCOVERY_MAX_WIN_RATE_MIN_RESOLVED: 25,   // only apply WR cap when sample is meaningful

  // Fast loop
  FAST_LOOP_INTERVAL_MS: 60 * 60 * 1000, // 60 minutes
  LOOKBACK_HOURS: 4,                       // Check trades from last 4 hours each loop

  // Goldsky pagination
  BATCH_SIZE: 1000,
  MAX_POSITIONS: 2000000,          // 2M positions — deeper crawl for more wallets
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
 * Discover wallet addresses from Goldsky and qualify them via Data API.
 * This runs periodically (every DISCOVERY_INTERVAL_SCANS fast loops).
 */
async function discoverWallets(state, existingPool, marketLookup = null) {
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

  // Step 3: Filter to candidates worth qualifying
  const candidates = [...walletSummaries.entries()]
    .filter(([, ws]) => ws.totalPnl >= CONFIG.MIN_PNL_DISCOVERY && ws.positionCount >= CONFIG.MIN_POSITIONS_DISCOVERY)
    .sort((a, b) => b[1].totalPnl - a[1].totalPnl)
    .slice(0, CONFIG.MAX_DISCOVERY_WALLETS);

  console.log(`  ${candidates.length} candidates pass PnL/position filters`);

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
        // in cooldown with no scoreV2.
        if (typeof pool[address].scoreV2 !== 'number' && walletPositions) {
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
              // Stage 1: attach MM classification before scoring so scoreV2
              // can penalise whale-01-class wallets via stats.mmPenalty.
              attachMMClassification(pool[address].stats);
              attachAlphaEvaluation(pool[address].stats);
              const v2 = computeWalletScoreV2(pool[address].stats);
              if (v2 && v2.score != null) {
                pool[address].scoreV2 = v2.score;
                pool[address].scoreV2Components = v2.components;
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

      const score = computeWalletScore(stats);

      // Require minimum resolved markets — no flukes
      if ((stats.resolvedMarkets || 0) < CONFIG.MIN_RESOLVED_MARKETS) {
        processed++;
        continue;
      }

      // Must have traded recently — Phase 6 tightens this for fresh entries
      // (existing pool members still get the gentler MAX_INACTIVE_DAYS grace
      // in the rescore loop).
      const daysSinceLastTrade = stats.lastTradeTs > 0
        ? (Date.now() / 1000 - stats.lastTradeTs) / 86400
        : Infinity;
      const freshInactiveFloor = CONFIG.DISCOVERY_V2_GATE === 'on'
        ? CONFIG.DISCOVERY_MAX_INACTIVE_DAYS
        : CONFIG.MAX_INACTIVE_DAYS;
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

      // ── Phase 6: V2 discovery gates ───────────────────────────────────
      // Use per-position data captured during the cursor scan to compute
      // decided metrics. This avoids per-wallet Goldsky queries which
      // timeout because the subgraph has no index on the user field.
      let decidedMetrics = null;
      if (CONFIG.DISCOVERY_V2_GATE === 'on' && walletPositions) {
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

      const v2Disc = computeWalletScoreV2(stats);

      pool[address] = {
        address,
        score,
        scoreV2: v2Disc && v2Disc.score != null ? v2Disc.score : undefined,
        scoreV2Components: v2Disc && v2Disc.components ? v2Disc.components : undefined,
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
    if (daysSinceLastTrade > CONFIG.MAX_INACTIVE_DAYS) {
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
      // Wallet wasn't in cursor window — fall back to per-wallet query.
      // This may timeout on the unindexed subgraph; if so goldskyPnl keeps
      // its prior value and decided metrics stay null until the cursor
      // covers this wallet's address range in a future cycle.
      try {
        const fresh = await fetchGoldskyWalletPnl(addr, entityName, fields, { marketLookup });
        if (fresh && fresh.positionCount > 0) {
          wallet.goldskyPnl = +fresh.totalPnl.toFixed(2);
          wallet.goldskyPositions = fresh.positionCount;
          if (wallet.goldskyPnl < CONFIG.MIN_PNL_DISCOVERY) {
            wallet.status = 'removed';
            wallet.removeReason = 'pnl_below_floor';
            pnlDecayed++;
            decayed++;
            continue;
          }
          if (fresh.decided) {
            wallet.decidedMetrics = {
              ...fresh.decided,
              measuredAt: new Date().toISOString(),
            };
          }
        }
      } catch (err) {
        // Non-fatal — keep existing goldskyPnl
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
      wallet.score = computeWalletScore(stats);
      // Stage 1/2: MM classification + alpha evaluation gate V2. Run before scoring.
      attachMMClassification(stats);
      attachAlphaEvaluation(stats);
      const v2 = computeWalletScoreV2(stats);
      if (v2 && v2.score != null) {
        wallet.scoreV2 = v2.score;
        wallet.scoreV2Components = v2.components;
      }
      wallet.stats = stats;
      delete wallet.decidedMetrics;

      // ── Phase 4: V2 eviction (shadow or live) ─────────────────────────
      // Only runs when we have real V2 metrics; skips wallets still on
      // aggregate-only data. Strike counters live on the wallet so a
      // single fluky rescore can't evict; pattern must persist.
      if (CONFIG.V2_EVICTION_MODE !== 'off' && v2 && v2.score != null) {
        wallet.v2Strikes = wallet.v2Strikes || { meanPicker: 0, lowScore: 0 };
        const evictions = [];

        // Rule 1: mean-picker shape, needs N consecutive strikes
        if (stats.isMeanPickerShape === true) {
          wallet.v2Strikes.meanPicker++;
          if (wallet.v2Strikes.meanPicker >= CONFIG.V2_MEAN_PICKER_STRIKES_TO_EVICT) {
            evictions.push({ reason: 'v2_mean_picker', detail: `${wallet.v2Strikes.meanPicker} strikes, ROI=${(stats.decidedROI * 100).toFixed(1)}% WR=${((stats.decidedWinRate || 0) * 100).toFixed(0)}% cap=$${Math.round(stats.decidedCapital).toLocaleString()}` });
          }
        } else {
          wallet.v2Strikes.meanPicker = 0;
        }

        // Rule 2: low V2 score persisting N cycles
        if (v2.score < CONFIG.V2_LOW_SCORE_THRESHOLD) {
          wallet.v2Strikes.lowScore++;
          if (wallet.v2Strikes.lowScore >= CONFIG.V2_LOW_SCORE_STRIKES_TO_EVICT) {
            evictions.push({ reason: 'v2_low_score', detail: `${wallet.v2Strikes.lowScore} strikes @ scoreV2=${v2.score}` });
          }
        } else {
          wallet.v2Strikes.lowScore = 0;
        }

        // Rule 3: money-loser with sample — single-shot, high confidence
        const resolved = (stats.decidedWins || 0) + (stats.decidedLosses || 0);
        if (stats.decidedROI != null && stats.decidedROI < 0
            && (stats.decidedCapital || 0) >= CONFIG.V2_NEG_ROI_CAPITAL_FLOOR
            && resolved >= CONFIG.V2_NEG_ROI_MIN_RESOLVED) {
          evictions.push({ reason: 'v2_neg_roi', detail: `ROI=${(stats.decidedROI * 100).toFixed(1)}% on $${Math.round(stats.decidedCapital).toLocaleString()} across ${resolved} markets` });
        }

        // Rule 4: dormancy at the tighter V2 floor (legacy MAX_INACTIVE_DAYS=60
        // still catches deeper tail upstream — this is an early tighten).
        const daysSinceLastTrade = stats.lastTradeTs > 0
          ? (Date.now() / 1000 - stats.lastTradeTs) / 86400
          : Infinity;
        if (daysSinceLastTrade > CONFIG.V2_DORMANCY_DAYS) {
          evictions.push({ reason: 'v2_dormant', detail: `${daysSinceLastTrade.toFixed(0)}d since last trade` });
        }

        if (evictions.length > 0) {
          const first = evictions[0];
          if (CONFIG.V2_EVICTION_MODE === 'live') {
            wallet.status = 'removed';
            wallet.removeReason = first.reason;
            wallet.removeDetail = first.detail;
            decayed++;
            console.log(`    ✂ ${addr.slice(0, 10)} evicted: ${first.reason} (${first.detail})`);
            continue;
          } else {
            // Shadow mode — tag the wallet so we can audit without removing
            wallet.v2WouldEvict = { reason: first.reason, detail: first.detail, flaggedAt: new Date().toISOString() };
            console.log(`    ◇ ${addr.slice(0, 10)} v2-shadow: ${first.reason} (${first.detail})`);
          }
        } else if (wallet.v2WouldEvict) {
          delete wallet.v2WouldEvict;
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

  // Step 5b: V2 backfill — populate decided metrics + scoreV2 for pool
  // wallets that don't have a V2 score yet. Two data sources:
  //   1. Cursor positions (walletPositions) — for wallets in this cycle's window
  //   2. fetchPositionsByIdRange — for wallets outside the cursor window.
  //      Uses id_gt on the primary key (fast, indexed) instead of the broken
  //      user-field filter that times out.
  // This ensures the entire pool gets V2 in a single discovery cycle rather
  // than waiting weeks for the cursor to wrap around the address space.
  let v2Backfilled = 0;
  let v2BackfillFetched = 0;
  if (CONFIG.ENABLE_DECIDED_METRICS) {
    const needsV2 = Object.entries(pool).filter(
      ([, w]) => w.status !== 'removed' && typeof w.scoreV2 !== 'number'
    );
    if (needsV2.length > 0) {
      console.log(`  V2 backfill: ${needsV2.length} pool wallets need scoring...`);
    }

    for (const [addr, wallet] of needsV2) {
      // Try cursor positions first (free, already in memory)
      let agg = null;
      const cursorPos = walletPositions?.get(addr);
      if (cursorPos && cursorPos.length > 0) {
        agg = aggregatePositions(cursorPos, marketLookup);
      }

      // Fall back to targeted id-range fetch for wallets outside cursor window
      if (!agg || agg.decidedROI == null) {
        try {
          const result = await fetchPositionsByIdRange(addr, marketLookup);
          if (result && result.decided) {
            agg = result.decided;
            // Also refresh goldskyPnl from the full position set
            wallet.goldskyPnl = +result.totalPnl.toFixed(2);
            wallet.goldskyPositions = result.positionCount;
            v2BackfillFetched++;
          }
        } catch (err) {
          // Non-fatal — wallet stays without V2 this cycle
        }
      }

      if (!agg || agg.decidedROI == null) continue;

      // Fold decided metrics onto stats
      if (wallet.stats) {
        wallet.stats.decidedPnl = agg.decidedPnl;
        wallet.stats.decidedCapital = agg.decidedCapital;
        wallet.stats.decidedROI = agg.decidedROI;
        wallet.stats.decidedWins = agg.wins;
        wallet.stats.decidedLosses = agg.losses;
        wallet.stats.decidedWinRate = agg.winRate;
        wallet.stats.decidedOpenPositions = agg.open;
        wallet.stats.decidedOpenCapitalAtRisk = agg.openCapitalAtRisk;
        wallet.stats.decidedUnredeemedWinsPositions = agg.unredeemedWins;
        wallet.stats.decidedWorthlessLosses = agg.worthlessLosses;
        wallet.stats.isMeanPickerShape = agg.isMeanPickerShape;
        wallet.stats.decidedMeasuredAt = new Date().toISOString();
      }

      // Stage 1/2: MM + alpha classification before V2 scoring (backfill path).
      if (wallet.stats) {
        attachMMClassification(wallet.stats);
        attachAlphaEvaluation(wallet.stats);
      }
      const v2 = computeWalletScoreV2(wallet.stats || {});
      if (v2 && v2.score != null) {
        wallet.scoreV2 = v2.score;
        wallet.scoreV2Components = v2.components;
        v2Backfilled++;
      }

      // Progress log every 50 wallets
      if ((v2Backfilled + v2BackfillFetched) % 50 === 0 && v2Backfilled > 0) {
        console.log(`    V2 backfill progress: ${v2Backfilled} scored (${v2BackfillFetched} via id-range fetch)...`);
      }
    }
    if (v2Backfilled > 0) {
      console.log(`  V2 backfill: ${v2Backfilled} pool wallets scored (${v2BackfillFetched} via id-range fetch)`);
    }
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

  // Phase 5: pick the authoritative score. V2 only goes live once a majority
  // of the pool has been rescored under the new pipeline — otherwise recent
  // stragglers (still scoreV2==null) would all collapse to legacy and skew
  // the ranking mid-flip. Under the coverage threshold we stay on legacy.
  const activePool = poolEntries.filter(([, w]) => w.status !== 'removed');
  const v2CoverageCount = activePool.filter(([, w]) => typeof w.scoreV2 === 'number').length;
  const v2Coverage = activePool.length > 0 ? v2CoverageCount / activePool.length : 0;
  const useV2 = CONFIG.USE_SCORE_V2 && v2Coverage >= (CONFIG.V2_MIN_POOL_COVERAGE_PCT / 100);
  const scoreOf = (w) => useV2 && typeof w.scoreV2 === 'number' ? w.scoreV2 : (w.score || 0);

  if (CONFIG.USE_SCORE_V2) {
    console.log(`  V2 ranker coverage: ${v2CoverageCount}/${activePool.length} (${(v2Coverage * 100).toFixed(0)}%) — ${useV2 ? '✓ using scoreV2 as primary' : `↻ below ${CONFIG.V2_MIN_POOL_COVERAGE_PCT}% floor, staying on legacy`}`);
  }

  // Use V2-calibrated floor when V2 is the active ranking score.
  // V2 practical max ~55 vs legacy 100 — applying the legacy floor (50) to V2
  // scores would filter out virtually everything (only perfect wallets survive).
  const effectiveMinScore = useV2 ? CONFIG.MIN_SCORE_POOL_V2 : CONFIG.MIN_SCORE_POOL;

  let ranked = Object.entries(pool)
    .filter(([, w]) => w.status !== 'removed' && (graceActive || scoreOf(w) >= effectiveMinScore))
    .sort((a, b) => scoreOf(b[1]) - scoreOf(a[1]));

  // Safety net: if score filtering wiped >50% of active wallets, something is
  // miscalibrated (e.g. scale mismatch). Fall back to keeping all non-removed
  // wallets rather than silently collapsing the pool.
  if (!graceActive && ranked.length < activePool.length * 0.5) {
    console.log(`  ⚠ Score filter would keep only ${ranked.length}/${activePool.length} wallets — likely miscalibrated. Bypassing MIN_SCORE filter this cycle.`);
    ranked = Object.entries(pool)
      .filter(([, w]) => w.status !== 'removed')
      .sort((a, b) => scoreOf(b[1]) - scoreOf(a[1]));
  }

  const trimmedPool = {};
  let rank = 0;
  let v2Protected = 0;
  for (const [addr, wallet] of ranked) {
    rank++;
    if (rank <= CONFIG.TARGET_POOL_SIZE) {
      wallet.rank = rank;
      trimmedPool[addr] = wallet;
    } else if (CONFIG.USE_SCORE_V2 && typeof wallet.scoreV2 !== 'number') {
      // V2 protection: don't evict wallets that haven't been assessed
      // under the new scoring pipeline yet. They keep their pool spot
      // until they get a scoreV2, at which point they compete fairly.
      // Gate on CONFIG.USE_SCORE_V2 (not useV2) so protection is active
      // during the entire rollout — not just after 50% coverage.
      wallet.rank = rank;
      trimmedPool[addr] = wallet;
      v2Protected++;
    }
  }
  if (v2Protected > 0) {
    console.log(`  🛡 V2 protection: ${v2Protected} wallets kept in pool pending V2 assessment`);
  }

  console.log(`\n  ✅ Wallet pool: ${Object.keys(trimmedPool).length} wallets (from ${qualified} qualified)`);
  const topWallets = ranked.slice(0, 5);
  for (const [addr, w] of topWallets) {
    const shown = useV2 && typeof w.scoreV2 === 'number' ? `v2:${w.scoreV2}` : `score:${w.score}`;
    const roi = w.stats?.decidedROI != null ? ` ROI:${(w.stats.decidedROI * 100).toFixed(0)}%` : '';
    console.log(`    #${w.rank} ${addr.slice(0, 12)}... ${shown} WR:${((w.stats?.recentWinRate || w.stats?.winRate || 0) * 100).toFixed(0)}% PnL:$${(w.stats?.totalPnl || 0).toFixed(0)}${roi}`);
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

  const historyNeedingRepair = (Array.isArray(existingSignals.history) ? existingSignals.history : Object.values(existingSignals.history || {}))
    .filter(s => s.conditionId && s.outcome !== 'win' && s.outcome !== 'loss')
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

  // Step 7: Build full analytics for frontend
  const analyticsFile = path.join(DATA_DIR, 'analytics.json.gz');
  let analytics = loadGzJSON(analyticsFile) || { trendline: [] };

  // 7a: Trendline
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
  if (analytics.trendline.length > 2000) {
    analytics.trendline.splice(0, analytics.trendline.length - 2000);
  }

  // 7b: Leaderboard — wallet list for Dashboard tab.
  // Sort order mirrors the pool ranker: V2 takes priority when present
  // (Phase 5), legacy is the fallback for wallets that haven't been
  // rescored under the new pipeline yet.
  const leaderboardScoreOf = (w) =>
    typeof w.scoreV2 === 'number' ? w.scoreV2 : (w.score || 0);
  const walletList = Object.values(walletPool)
    .filter(w => leaderboardScoreOf(w) > 0 && w.status !== 'removed')
    .sort((a, b) => leaderboardScoreOf(b) - leaderboardScoreOf(a));

  analytics.leaderboard = walletList.map((w, idx) => ({
    rank: idx + 1,
    address: w.address,
    score: w.score,
    // Phase 2 V2 fields — primary ranker post-redesign
    scoreV2: typeof w.scoreV2 === 'number' ? w.scoreV2 : null,
    scoreV2Components: w.scoreV2Components || null,
    // Phase 4 shadow eviction flag — populated when a wallet matches an
    // eviction rule but V2_EVICTION_MODE is 'shadow' rather than 'live'
    v2WouldEvict: w.v2WouldEvict || null,
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
    },
  }));

  // 7c: Summary
  const totalWallets = walletList.length;
  const totalPnl = walletList.reduce((s, w) => s + (w.goldskyPnl || w.stats?.totalPnl || 0), 0);
  const totalWins = walletList.reduce((s, w) => s + (w.stats?.wins || 0), 0);
  const totalResolved = walletList.reduce((s, w) => s + (w.stats?.resolvedMarkets || 0), 0);

  // Summary scores use the same effective-score the ranker uses, with
  // a V2-specific average exposed separately for dashboards that want to
  // track the new scale independently.
  const v2Wallets = walletList.filter(w => typeof w.scoreV2 === 'number');
  analytics.summary = {
    totalWallets,
    totalPnl,
    avgScore: totalWallets > 0 ? walletList.reduce((s, w) => s + leaderboardScoreOf(w), 0) / totalWallets : 0,
    avgScoreV2: v2Wallets.length > 0 ? v2Wallets.reduce((s, w) => s + w.scoreV2, 0) / v2Wallets.length : 0,
    v2Coverage: totalWallets > 0 ? v2Wallets.length / totalWallets : 0,
    meanPickerCount: walletList.filter(w => w.stats?.isMeanPickerShape === true).length,
    shadowEvictionCount: walletList.filter(w => w.v2WouldEvict).length,
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

  const marketsFile = path.join(DATA_DIR, 'markets.json.gz');
  const existingMarkets = loadGzJSON(marketsFile) || {};
  const marketLookup = new Map(Object.entries(existingMarkets));

  console.log(`\n📋 State: Scan #${state.scanCount}`);
  console.log(`  Wallet pool: ${Object.keys(walletPool).length}`);
  console.log(`  Known markets: ${marketLookup.size}`);
  console.log(`  Last discovery: scan #${state.lastDiscovery}`);

  // Decide whether to run discovery (slow loop)
  // Force discovery if pool is below target (e.g. after state reset)
  const poolSize = Object.keys(walletPool).length;
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
    walletPool = await discoverWallets(state, walletPool, marketLookup);
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
