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
 * Gates applied (structural only — user has already vetted the wallet):
 *   - already_active:  anti-dup per (wallet, market) pair
 *   - market_closed:   don't emit signals on settled markets — a signal
 *                      with currentPrice=1 or 0 is recording a wallet's
 *                      historical trade on a resolved market and isn't
 *                      actionable for a follower. Includes the stale-
 *                      Gamma case (acceptingOrders=false). Wallet's
 *                      historical performance is still captured via the
 *                      wallet-stats pipeline.
 *
 * Gates NOT applied (deliberately):
 *   - categoryAlignment / category whitelist
 *   - resolves_too_soon — emit on short-window markets (LCK / fast crypto)
 *   - drawdown / stale-follower premium
 *   - V2 score floor
 *   - no_price — emit anyway with currentPrice=0; repair flow back-fills
 *
 * The user has taken responsibility for wallet quality. We track every
 * trade and let the data speak for itself.
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

      // No quality/timing gates on handpicked — user has already vetted
      // the wallet. The only gates kept are STRUCTURAL:
      //   - already_active: anti-dup per (wallet, market) pair
      //   - market_closed:  don't emit on settled markets. A signal with
      //                     currentPrice=1 (or 0) is recording a trade on
      //                     a market that has resolved — not actionable
      //                     for a follower. We still capture the wallet's
      //                     historical performance via the wallet stats
      //                     pipeline, just don't pollute the live signal
      //                     feed with un-followable noise. Includes the
      //                     stale-Gamma case (acceptingOrders === false
      //                     but marketClosed not yet set).
      const tokenId = trade.asset || '';
      const mi = tokenId ? marketLookup.get(tokenId) : null;
      const currentPrice = mi ? +(mi.currentPrice || 0).toFixed(4) : 0;

      // Skip emission on settled / no-longer-trading markets.
      const marketSettled = mi && (
        mi.marketClosed === true ||
        mi.acceptingOrders === false
      );
      if (marketSettled) {
        kills.market_closed++;
        continue;
      }

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

        // Pricing — `openMarketPrice` MUST reflect what the wallet actually
        // paid, not the market's price at signal-open time. If we open a
        // handpicked signal on a wallet's HISTORICAL buy whose market has
        // already resolved, `currentPrice` will be 1 (winning side) or 0
        // (losing side), which makes the win-return calc — `(1/openPrice - 1)
        // * 100` — collapse to 0 on every winner. Mirroring `entryPrice`
        // here keeps the close-time math anchored to the wallet's actual
        // fill, and matches the close path's existing fallback (which uses
        // avgEntryPrice when openMarketPrice is missing).
        avgEntryPrice: entryPrice,
        openMarketPrice: entryPrice,
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
