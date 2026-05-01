/**
 * Handpicked-wallet signal emission
 *
 * Parallel signal track for manually-curated wallets. Every BUY from a
 * handpicked wallet becomes its own signal — no convergence requirement,
 * no scoring formula, no automated discovery. The user vouches for the
 * wallet manually; we just track their trades.
 *
 * Why a separate track:
 *
 * The automated scanner relies on a scoring formula (V2/V3) calibrated
 * against a small contaminated sample. We've seen it reject objectively
 * elite wallets (e.g., 0xbddf61af with $902k profit and 100% WR on NBA
 * underdogs). Rather than keep tweaking weights against unreliable
 * calibration, this track lets the user inject curated knowledge
 * directly. As handpicked-wallet signals resolve, we accumulate clean
 * ground-truth attribution data — which can later be used to derive a
 * proper scoring formula for the automated scanner.
 *
 * Signal type: 'handpicked'
 *
 * Gates applied (minimal — user already vetted the wallet):
 *   - market_closed: don't emit on already-resolved markets
 *   - resolves_too_soon: 4h floor (followers can't act on faster markets)
 *   - already_active: don't double-emit per (wallet, market) pair
 *   - basic price sanity: must have a current price > 0
 *
 * Gates NOT applied (deliberately minimal):
 *   - categoryAlignment / category whitelist — user trusts the wallet's
 *     market choices; if they bet underdogs in markets we don't normally
 *     touch, we still emit.
 *   - drawdown / stale-follower premium — same rationale.
 *   - V2 score floor — handpicked wallets bypass scoring entirely.
 */

const HANDPICKED_MIN_HOURS_TO_RESOLUTION = 4;

/**
 * Process new buys from handpicked wallets and emit signals.
 *
 * @param {Map<string,Array>} recentTrades  wallet → recent trades from /activity
 * @param {Array<{address,notes}>} handpickedList  wallets the user has vetted
 * @param {object} signals                  existing signals { active, history, stats }
 * @param {Map} marketLookup                tokenId → market info
 * @param {number} scanIndex                current scan number
 * @returns {{ active, history, stats, opened, updated, closed }}
 */
export function processHandpickedSignals(recentTrades, handpickedList, signals, marketLookup, scanIndex) {
  const active = { ...(signals.active || {}) };
  const history = [...(signals.history || [])];
  const now = new Date().toISOString();

  let opened = 0, updated = 0;
  const kills = { market_closed: 0, resolves_too_soon: 0, no_price: 0, already_active: 0, no_wallet_trades: 0 };

  // Index handpicked wallets for fast lookup
  const handpickedAddrs = new Set(handpickedList.map(w => w.address.toLowerCase()));

  for (const [wallet, trades] of recentTrades) {
    if (!handpickedAddrs.has(wallet.toLowerCase())) continue;
    if (!Array.isArray(trades) || trades.length === 0) {
      kills.no_wallet_trades++;
      continue;
    }

    // Each BUY from this wallet → one signal candidate
    for (const trade of trades) {
      if (trade.side !== 'BUY') continue;
      const cid = trade.conditionId;
      if (!cid) continue;

      // signalId scoped to (wallet, market) — one signal per wallet per market
      const signalId = `hp_${cid}_${wallet.slice(2, 10)}`;

      if (active[signalId]) {
        // Already tracking this — refresh metadata only
        const sig = active[signalId];
        sig.lastUpdatedAt = now;
        sig.lastUpdatedScan = scanIndex;
        sig.lastTradeTs = Math.max(sig.lastTradeTs || 0, trade.timestamp);

        // Refresh live price if available
        const tokenId = trade.asset || sig.tokenId;
        const mi = tokenId ? marketLookup.get(tokenId) : null;
        if (mi && mi.currentPrice > 0) sig.currentMarketPrice = +(mi.currentPrice).toFixed(4);

        kills.already_active++;
        updated++;
        continue;
      }

      const tokenId = trade.asset || '';
      const mi = tokenId ? marketLookup.get(tokenId) : null;

      // Gate: market resolved
      if (mi && mi.marketClosed === true) { kills.market_closed++; continue; }

      // Gate: resolves too soon
      if (mi && mi.endDate) {
        const minMs = HANDPICKED_MIN_HOURS_TO_RESOLUTION * 3600 * 1000;
        const msUntil = new Date(mi.endDate).getTime() - Date.now();
        if (msUntil < minMs) { kills.resolves_too_soon++; continue; }
      }

      // Gate: must have a price to track signal-return later
      const currentPrice = mi ? +(mi.currentPrice || 0).toFixed(4) : 0;
      if (!(currentPrice > 0)) { kills.no_price++; continue; }

      // Open a new handpicked signal
      const entryPrice = parseFloat(trade.price) || currentPrice;
      const buySize = (parseFloat(trade.size) || 0) * entryPrice;

      active[signalId] = {
        signalId,
        signalType: 'handpicked',
        conditionId: cid,
        tokenId,
        marketTitle: trade.title || '',
        slug: trade.slug || '',
        eventSlug: trade.eventSlug || '',
        groupKey: cid,

        direction: trade.outcome || 'Unknown',
        outcomeIndex: trade.outcomeIndex,

        // Wallet provenance
        soloWallet: wallet,
        currentWallets: [{ address: wallet, score: 0, buySize: +buySize.toFixed(2) }],
        walletCount: 1,

        // Timing
        openedAt: now,
        openedScan: scanIndex,
        lastUpdatedAt: now,
        lastUpdatedScan: scanIndex,
        lastTradeTs: trade.timestamp,
        scansActive: 1,

        // Pricing
        avgEntryPrice: entryPrice,
        openMarketPrice: currentPrice,
        currentMarketPrice: currentPrice,
        totalBuySize: +buySize.toFixed(2),

        // Confidence — handpicked signals don't have a confidence
        // computation; user-vouched wallets bypass scoring. Set a neutral
        // value so the dashboard tier badge still renders.
        confidence: 50,
        tier: 'handpicked',
        avgScore: 0,

        peakWallets: 1,
        peakConfidence: 50,

        status: 'active',
        outcome: null,
        closedAt: null,
        closedScan: null,
        closeReason: null,
      };
      opened++;
    }
  }

  return {
    active,
    history,
    stats: signals.stats || {},
    opened,
    updated,
    closed: 0,
    kills,
  };
}
