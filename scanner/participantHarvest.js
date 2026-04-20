/**
 * Market-participant BFS discovery — port of handpicked-signals'
 * discover_market_participants.py.
 *
 * Idea: when our top-tier alpha wallets WIN a trade, the other bidders on
 * that market are all interesting candidates. Some of them share the same
 * sport / market-type / style as our proven alphas and may have their own
 * edge. Ranking them by appearance frequency across many winning markets
 * surfaces the most active participants in our alphas' "neighbourhood".
 *
 * The handpicked-signals implementation yielded a 14.5% qualifier rate on
 * 200-wallet probes — far better than the ~3-5% typical of a raw top-volume
 * leaderboard scan. Why it works: top-volume lists anyone who churns volume,
 * regardless of edge. This harvest is pre-filtered by "plays in the same
 * markets as known winners", which correlates strongly with real edge.
 *
 * Stage 3 of the handpicked-signals → wallet-engine merge.
 *
 * Integration:
 *   - Standalone. Does not depend on or touch the live scan loop.
 *   - Runs against the Data API only (no Goldsky dependency here — callers
 *     provide source wallets + their winning market IDs).
 *   - Writes candidate output to a JSON file; callers (scan.js discovery,
 *     or manual scripts) consume it as a seed list.
 */

import { fetchMarketTrades } from './dataApi.js';

/**
 * Harvest participant addresses from a set of source markets.
 *
 * @param {Array<{conditionId: string, sourceLabel?: string}>} markets - Markets to scan
 * @param {object} opts - Options
 * @param {number} [opts.tradesPerMarket=500] - Cap trades fetched per market
 * @param {Set<string>} [opts.excludeAddresses] - Addresses to skip (existing pool)
 * @param {Function} [opts.onProgress] - Called as (marketsProcessed, total)
 * @returns {Promise<{participants: Map, marketsScanned: number, totalTrades: number}>}
 *   participants: Map<address, { appearances: number, volume: number, firstSeenMarket: string }>
 */
export async function harvestMarketParticipants(markets, opts = {}) {
  const tradesPerMarket = opts.tradesPerMarket || 500;
  const excludeSet = opts.excludeAddresses instanceof Set
    ? opts.excludeAddresses
    : new Set();
  const onProgress = typeof opts.onProgress === 'function' ? opts.onProgress : null;

  const participants = new Map(); // address -> { appearances, volume, firstSeenMarket }
  let marketsScanned = 0;
  let totalTrades = 0;

  // Dedupe markets by conditionId
  const uniqueMarkets = [];
  const seenCids = new Set();
  for (const m of markets) {
    if (!m || !m.conditionId) continue;
    if (seenCids.has(m.conditionId)) continue;
    seenCids.add(m.conditionId);
    uniqueMarkets.push(m);
  }

  for (const market of uniqueMarkets) {
    const trades = await fetchMarketTrades(market.conditionId, { limit: tradesPerMarket });
    marketsScanned++;
    totalTrades += trades.length;

    for (const t of trades) {
      const addr = String(t.proxyWallet || t.wallet || '').toLowerCase();
      if (!addr) continue;
      if (excludeSet.has(addr)) continue;

      if (!participants.has(addr)) {
        participants.set(addr, {
          appearances: 0,
          volume: 0,
          firstSeenMarket: market.conditionId,
        });
      }
      const entry = participants.get(addr);
      entry.appearances++;
      const size = parseFloat(t.size || 0) || 0;
      const price = parseFloat(t.price || 0) || 0;
      entry.volume += size * price;
    }

    if (onProgress) onProgress(marketsScanned, uniqueMarkets.length);
  }

  return { participants, marketsScanned, totalTrades };
}

/**
 * Rank participants by score that balances frequency and volume. This is
 * the "how likely is this wallet to be worth probing" score. Returns the
 * top N as an array sorted descending.
 *
 * @param {Map} participants - Output of harvestMarketParticipants()
 * @param {number} topN - Return at most this many
 * @returns {Array<{address, appearances, volume, score}>}
 */
export function rankParticipants(participants, topN = 200) {
  const entries = [];
  for (const [address, data] of participants) {
    // Combined score: log-scaled volume + linear appearances.
    // Appearance count matters more than any single big trade (a wallet
    // that shows up in many of our alphas' markets is a strong signal);
    // volume is the tiebreaker.
    const score = data.appearances + Math.log10(1 + data.volume);
    entries.push({ address, ...data, score });
  }
  entries.sort((a, b) => b.score - a.score || b.appearances - a.appearances);
  return entries.slice(0, topN);
}

/**
 * Convenience wrapper: harvest + rank + return top N.
 *
 * @param {Array<{conditionId, sourceLabel?}>} markets
 * @param {object} opts
 * @param {number} [opts.topN=200]
 * @returns {Promise<{candidates: Array, marketsScanned, totalTrades, totalUnique}>}
 */
export async function discoverCandidates(markets, opts = {}) {
  const topN = opts.topN || 200;
  const { participants, marketsScanned, totalTrades } =
    await harvestMarketParticipants(markets, opts);
  const candidates = rankParticipants(participants, topN);
  return {
    candidates,
    marketsScanned,
    totalTrades,
    totalUnique: participants.size,
  };
}

/**
 * Given a wallet's Goldsky-fetched positions + marketLookup, return the
 * conditionIds of markets they WON (with most capital conviction). Used
 * as the source-market set for harvestMarketParticipants.
 *
 * @param {Array} positions - Goldsky positions for the wallet
 *                            (tokenId, sharesHeld, avgPrice, realizedPnl, totalBought)
 * @param {Map} marketLookup - tokenId -> market metadata
 * @param {object} [opts]
 * @param {number} [opts.maxMarkets=100] - Cap
 * @param {number} [opts.minCost=50] - Skip markets with cost basis below this
 * @returns {Array<{conditionId, cost, pnl, outcome}>}
 */
export function selectWinningMarkets(positions, marketLookup, opts = {}) {
  const maxMarkets = opts.maxMarkets || 100;
  const minCost = opts.minCost ?? 50;
  const wins = [];

  for (const pos of positions) {
    const cost = pos.totalBought || 0;
    if (cost < minCost) continue;

    const mEntry = marketLookup.get(pos.tokenId);
    if (!mEntry || !mEntry.conditionId) continue;

    // Did this position win?
    let won = false;
    if (pos.realizedPnl > 0.01) {
      // Closed-winner (redeemed or sold at profit)
      won = true;
    } else if (mEntry.marketClosed === true && mEntry.winningOutcome && mEntry.outcome) {
      // Unredeemed winner — market resolved in their favour but they didn't cash out
      const theirs = String(mEntry.outcome).toLowerCase().trim();
      const winning = String(mEntry.winningOutcome).toLowerCase().trim();
      if (theirs && theirs === winning) won = true;
    }
    if (!won) continue;

    wins.push({
      conditionId: mEntry.conditionId,
      cost,
      pnl: pos.realizedPnl || 0,
      outcome: mEntry.outcome || '',
    });
  }

  // Sort by cost (conviction size) descending, take top N
  wins.sort((a, b) => b.cost - a.cost);
  return wins.slice(0, maxMarkets);
}
