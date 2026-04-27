/*
 * scanner/repair.js — post-scan reconciliation with multi-source resolver.
 *
 * Runs every 30 min via .github/workflows/repair-signals.yml. For every
 * active signal whose underlying market should have resolved by now,
 * chases the outcome through four data sources in priority order:
 *
 *   1. Bulk refresh via Gamma /events (legacy — populates cache)
 *   2. Per-signal fresh Gamma /markets fetch (catches what (1) missed)
 *   3. Gamma /markets by slug (alternate path)
 *   4. Wallet REDEEM events via Data API /activity (positive confirmation
 *      when the wallet has cashed out their winnings)
 *   5. Cached currentPrice extreme (≤0.02 or ≥0.98 = settled)
 *
 * Result: stale signals get resolved with REAL outcomes instead of being
 * voided when our cache lags Polymarket. See active-signal-audit.mjs and
 * scripts/force-resolve-stale.mjs for the diagnostic that motivated this.
 *
 * Does NOT touch discovery, scoring, paper trading, or trendline.
 */

import path from 'path';
import { fileURLToPath } from 'url';
import {
  loadGzJSON, saveGzJSON,
  refreshSignalMarkets, matchesWinningOutcome,
} from './lib.js';
import { closeSignal } from './signals.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_DIR = path.join(__dirname, '..', 'data');

const GAMMA_MARKETS = 'https://gamma-api.polymarket.com/markets';
const POLYMARKET_DATA_API = 'https://data-api.polymarket.com';

// ── Helpers (inlined for repair.js minimal-deps; mirrors force-resolve-stale.mjs) ──

async function gammaByTokenId(tokenId) {
  if (!tokenId) return null;
  try {
    const r = await fetch(`${GAMMA_MARKETS}?clob_token_ids=${tokenId}&limit=1`);
    if (!r.ok) return null;
    const arr = await r.json();
    return Array.isArray(arr) && arr.length > 0 ? arr[0] : null;
  } catch { return null; }
}

async function gammaBySlug(slug) {
  if (!slug) return null;
  try {
    const marketSlug = slug.includes('/') ? slug.split('/').pop() : slug;
    const r = await fetch(`${GAMMA_MARKETS}?slug=${encodeURIComponent(marketSlug)}&limit=1`);
    if (!r.ok) return null;
    const arr = await r.json();
    return Array.isArray(arr) && arr.length > 0 ? arr[0] : null;
  } catch { return null; }
}

function extractWinner(market) {
  if (!market) return null;
  const closed = market.closed === true || market.closed === 'true';
  if (!closed) return null;
  let prices = market.outcomePrices;
  if (typeof prices === 'string') { try { prices = JSON.parse(prices); } catch {} }
  let outcomes = market.outcomes;
  if (typeof outcomes === 'string') { try { outcomes = JSON.parse(outcomes); } catch {} }
  if (Array.isArray(prices) && Array.isArray(outcomes)) {
    for (let i = 0; i < prices.length; i++) {
      if (parseFloat(prices[i]) >= 0.95) return outcomes[i];
    }
  }
  if (Array.isArray(market.tokens)) {
    for (const t of market.tokens) {
      if (parseFloat(t.price || 0) >= 0.95) return t.outcome || null;
    }
  }
  return null;
}

async function walletRedeemOutcome(walletAddr, conditionId, ourDirection) {
  if (!walletAddr || !conditionId) return null;
  try {
    const r = await fetch(`${POLYMARKET_DATA_API}/activity?user=${walletAddr.toLowerCase()}&limit=200`);
    if (!r.ok) return null;
    const events = await r.json();
    if (!Array.isArray(events)) return null;
    for (const e of events) {
      if ((e.type || '').toUpperCase() !== 'REDEEM') continue;
      const cid = e.conditionId || e.condition_id || '';
      if (cid.toLowerCase() !== conditionId.toLowerCase()) continue;
      const size = parseFloat(e.size || e.shares || 0);
      const payout = parseFloat(e.usdcSize || e.payout || 0);
      if (size <= 0) continue;
      const impliedPrice = payout / size;
      if (impliedPrice >= 0.95) return ourDirection;  // wallet redeemed at full price → our side won
      if (impliedPrice < 0.05) return ourDirection === 'Yes' ? 'No' : 'Yes';  // worthless → opposite won
    }
    return null;
  } catch { return null; }
}

/**
 * Multi-source chase for a single signal. Returns { winningOutcome, resolvedBy }
 * when a resolution is found, or null when all sources fail.
 */
async function chaseResolution(sig, marketLookup) {
  const tokenId = sig.tokenId || sig.asset;
  const cid = sig.conditionId;
  const cachedMi = (tokenId && marketLookup.get(String(tokenId)))
    || (cid && marketLookup.get(String(cid))) || null;

  // 1. cached lookup post-bulk-refresh — the existing path
  if (cachedMi && cachedMi.marketClosed === true && cachedMi.winningOutcome) {
    return { winningOutcome: cachedMi.winningOutcome, resolvedBy: 'gamma_repair' };
  }

  // 2. fresh per-signal Gamma fetch by tokenId
  if (tokenId) {
    const fresh = await gammaByTokenId(tokenId);
    const winner = extractWinner(fresh);
    if (winner) return { winningOutcome: winner, resolvedBy: 'gamma_fresh' };
  }

  // 3. Gamma by slug
  if (sig.slug) {
    const fresh = await gammaBySlug(sig.slug);
    const winner = extractWinner(fresh);
    if (winner) return { winningOutcome: winner, resolvedBy: 'gamma_slug' };
  }

  // 4. wallet REDEEM events (only solo signals — clusters lack a single wallet to query)
  if (sig.soloWallet && cid) {
    const winner = await walletRedeemOutcome(sig.soloWallet, cid, sig.direction);
    if (winner) return { winningOutcome: winner, resolvedBy: 'redeem_inferred' };
  }

  // 5. cached currentPrice extreme — token settled at 0 or 1
  if (cachedMi && typeof cachedMi.currentPrice === 'number') {
    if (cachedMi.currentPrice >= 0.98) {
      return { winningOutcome: sig.direction, resolvedBy: 'price_extreme' };
    }
    if (cachedMi.currentPrice <= 0.02) {
      const opposite = sig.direction === 'Yes' ? 'No' : 'Yes';
      return { winningOutcome: opposite, resolvedBy: 'price_extreme' };
    }
  }

  return null;
}

async function main() {
  const signalsFile = path.join(DATA_DIR, 'signals.json.gz');
  const marketsFile = path.join(DATA_DIR, 'markets.json.gz');
  const analyticsFile = path.join(DATA_DIR, 'analytics.json.gz');

  const signals = loadGzJSON(signalsFile);
  const marketsRaw = loadGzJSON(marketsFile) || {};
  const analytics = loadGzJSON(analyticsFile) || {};

  if (!signals || !signals.active) {
    console.error('No signals.json.gz or missing .active — aborting.');
    process.exit(1);
  }

  const active = signals.active;
  const history = signals.history || [];
  const marketLookup = new Map(Object.entries(marketsRaw));

  const activeArr = Object.values(active);
  console.log(`Repair starting: ${activeArr.length} active, ${history.length} history`);

  // Bulk Gamma refresh — populates cache for most active markets.
  const refreshList = activeArr.map(s => ({
    tokenId: s.tokenId,
    conditionId: s.conditionId,
    eventSlug: s.eventSlug,
    slug: s.slug,
  }));
  await refreshSignalMarkets(refreshList, marketLookup);
  saveGzJSON(marketsFile, Object.fromEntries(marketLookup));

  const scanIndex = (analytics.scanCount || signals.stats?.lastScan || 0);
  const now = new Date().toISOString();

  // ── Multi-source resolution chase ─────────────────────────────────
  let closed = 0, wins = 0, losses = 0;
  const resolvedBySource = {};
  const skipReasons = { not_past_enddate: 0, all_sources_failed: 0 };

  for (const signalId of Object.keys(active)) {
    const s = active[signalId];

    // Only chase signals whose market has plausibly resolved.
    // - If endDate is in the future, leave the signal alone (still active).
    // - If endDate is unknown, fall back to age-based: chase if >24h since
    //   last update (Polymarket markets typically resolve within hours of endDate).
    const cachedMi = marketLookup.get(s.tokenId);
    const endMs = cachedMi && cachedMi.endDate ? new Date(cachedMi.endDate).getTime() : 0;
    const ageHours = s.openedAt ? (Date.now() - new Date(s.openedAt).getTime()) / 3600000 : 0;
    const pastEndDate = endMs > 0 && endMs < Date.now();
    const stale = !endMs && ageHours > 24;
    if (!pastEndDate && !stale) { skipReasons.not_past_enddate++; continue; }

    const result = await chaseResolution(s, marketLookup);
    if (!result) { skipReasons.all_sources_failed++; continue; }

    const won = matchesWinningOutcome(s.direction, s.topOutcome, result.winningOutcome);
    const outcome = won ? 'win' : 'loss';

    const op = s.openMarketPrice || s.avgEntryPrice || 0;
    if (won && op > 0) {
      s.signalReturn = +((1 / op - 1) * 100).toFixed(2);
    } else if (!won) {
      s.signalReturn = -100;
    }
    s.winningOutcome = result.winningOutcome;
    s.resolvedBy = result.resolvedBy;

    closeSignal(active, history, signalId, 'resolved', scanIndex, now, outcome);
    closed++;
    if (won) wins++; else losses++;
    resolvedBySource[result.resolvedBy] = (resolvedBySource[result.resolvedBy] || 0) + 1;
  }

  // ── Recompute aggregate stats ─────────────────────────────────────
  const activeSignals = Object.values(active);
  const totalWins = history.filter(s => s.outcome === 'win').length;
  const totalLosses = history.filter(s => s.outcome === 'loss').length;
  const resolved = totalWins + totalLosses;
  const hitRate = resolved > 0 ? +(totalWins / resolved * 100).toFixed(1) : 0;
  const totalHistoryPnl = history.reduce((sum, sig) => sum + (sig.walletPnl || sig.closedPnl || 0), 0);
  const consensusSignals = activeSignals.filter(s => !s.signalType || s.signalType === 'consensus');
  const clusterSignals = activeSignals.filter(s => s.signalType === 'cluster');
  const microClusterSignals = activeSignals.filter(s => s.signalType === 'micro-cluster');
  const soloSignals = activeSignals.filter(s => s.signalType === 'solo');

  const prevStats = signals.stats || {};
  const newStats = {
    ...prevStats,
    activeCount: activeSignals.length,
    consensusCount: consensusSignals.length,
    clusterCount: clusterSignals.length,
    microClusterCount: microClusterSignals.length,
    soloCount: soloSignals.length,
    historyCount: history.length,
    totalResolved: resolved,
    wins: totalWins,
    losses: totalLosses,
    hitRate,
    winRate: hitRate,
    totalPnl: +totalHistoryPnl.toFixed(2),
    avgConfidence: activeSignals.length > 0
      ? +(activeSignals.reduce((s, sig) => s + (sig.confidence || 0), 0) / activeSignals.length).toFixed(1)
      : 0,
    tierBreakdown: {
      elite: activeSignals.filter(s => s.tier === 'elite').length,
      pro: activeSignals.filter(s => s.tier === 'pro').length,
      starter: activeSignals.filter(s => s.tier === 'starter').length,
    },
    typeBreakdown: {
      consensus: consensusSignals.length,
      cluster: clusterSignals.length,
      'micro-cluster': microClusterSignals.length,
      solo: soloSignals.length,
    },
    lastRepair: now,
    lastRepairClosed: closed,
    lastRepairBySource: resolvedBySource,
  };

  signals.stats = newStats;
  saveGzJSON(signalsFile, signals);

  analytics.signals = {
    active: activeSignals,
    history,
    stats: newStats,
  };
  saveGzJSON(analyticsFile, analytics);

  // ── Logging ───────────────────────────────────────────────────────
  console.log(`Repair done: ${closed} closed (${wins}W/${losses}L) | active=${activeSignals.length} history=${history.length} hitRate=${hitRate}%`);
  if (Object.keys(resolvedBySource).length > 0) {
    console.log('  By source:', JSON.stringify(resolvedBySource));
  }
  if (skipReasons.not_past_enddate || skipReasons.all_sources_failed) {
    console.log(`  Skipped: ${skipReasons.not_past_enddate} not-past-endDate, ${skipReasons.all_sources_failed} all-sources-failed`);
  }
}

main().catch(err => {
  console.error('Repair failed:', err);
  process.exit(1);
});
