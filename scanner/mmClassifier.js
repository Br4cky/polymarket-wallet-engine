/**
 * Market-Maker Classifier — port of handpicked-signals/classify_wallets.py.
 *
 * Six-signal score (0–6) detecting wallets whose "PnL" comes from market-
 * making plumbing rather than directional insight. A high MM score means
 * the wallet's signals are NOT copy-tradeable — copying them would put you
 * into scrap-graded limit orders at near-$1 that can't profit without the
 * maker rebates and LP rewards the original wallet earns.
 *
 * This is the classifier that correctly identified whale-01 (6/6 MM signals,
 * $3.5M PnL almost entirely from rebates + merges) as a non-copyable wallet
 * despite their huge headline PnL and solid 59% WR.
 *
 * Signals (each worth 1 point):
 *   1. sellRatio      < 0.05      — wallet almost never sells (closes via MERGE/REDEEM)
 *   2. dualSideRate   > 0.40      — >40% of markets traded on both YES and NO
 *   3. mergeRate      > 0.10      — uses MERGE for pair-arb on >10% of markets
 *   4. avgPriceSum    < 1.01      — on dual-side markets, avg buy price sum < $1.01
 *                                    (free-arb bids below fair value)
 *   5. rebateUsdcTotal > $100     — earns measurable maker rebates
 *   6. rewardUsdcTotal > $100     — earns measurable LP / order-book rewards
 *
 * Gating:
 *   - Requires at least 25 unique markets traded. Small-sample wallets can
 *     coincidentally hit 3-4 signals out of pure noise; the 25-market floor
 *     kills that failure mode.
 *
 * Score interpretation:
 *   0–1  → almost certainly directional
 *   2    → probably directional (weak MM tells, likely coincidence)
 *   3    → ambiguous — could be a directional wallet with some MM income,
 *          or a soft MM. Apply downweighting but don't evict.
 *   4+   → strong MM signal. Apply heavy scoring penalty; wallet's signals
 *          are not copy-tradeable.
 *   6    → confirmed market-maker (whale-01 class).
 *
 * Integration:
 *   - Called on each wallet's stats during scoring.
 *   - Result attached to stats.mmScore / stats.mmSignals / stats.isLikelyMM.
 *   - computeWalletScore multiplies final score by mmPenalty where
 *     mmPenalty = 0.1 when mmScore >= 4, else 1.0.
 *
 * Stage 1 of the handpicked-signals → wallet-engine merge.
 */

/**
 * Compute MM score for a wallet from its analyzeTradeHistory stats.
 *
 * @param {object} stats - Output from analyzeTradeHistory() with Stage 0 fields.
 *                         Required: uniqueMarkets, sellRatio, dualSideRate,
 *                         mergeRate, avgDualSidePriceSum, rebateUsdcTotal,
 *                         rewardUsdcTotal.
 * @param {object} [opts] - Options
 * @param {number} [opts.minMarkets=25] - Min unique markets before signals count.
 * @returns {object} { score, signals, isLikelyMM, reason }
 *                   score: 0-6
 *                   signals: array of { name, triggered, value, threshold }
 *                   isLikelyMM: boolean (score >= 4)
 *                   reason: explains the result
 */
export function classifyMarketMaker(stats, opts = {}) {
  const minMarkets = opts.minMarkets ?? 25;
  const empty = {
    score: 0,
    signals: [],
    isLikelyMM: false,
    reason: 'no_stats',
  };

  if (!stats || typeof stats !== 'object') return empty;

  const uniqueMarkets = stats.uniqueMarkets || 0;
  if (uniqueMarkets < minMarkets) {
    return {
      score: 0,
      signals: [],
      isLikelyMM: false,
      reason: `insufficient_sample (${uniqueMarkets} < ${minMarkets} markets)`,
    };
  }

  // All six signals, each a single point.
  const signals = [
    {
      name: 'low_sell_ratio',
      value: stats.sellRatio,
      threshold: 0.05,
      triggered: stats.sellRatio != null && stats.sellRatio < 0.05,
      description: 'Closes positions via REDEEM/MERGE instead of selling',
    },
    {
      name: 'high_dual_side_rate',
      value: stats.dualSideRate,
      threshold: 0.40,
      triggered: (stats.dualSideRate || 0) > 0.40,
      description: 'Buys both YES and NO on >40% of markets',
    },
    {
      name: 'high_merge_rate',
      value: stats.mergeRate,
      threshold: 0.10,
      triggered: (stats.mergeRate || 0) > 0.10,
      description: 'Uses MERGE on >10% of markets (pair-arb plumbing)',
    },
    {
      name: 'low_dual_side_price_sum',
      value: stats.avgDualSidePriceSum,
      threshold: 1.01,
      triggered: stats.avgDualSidePriceSum != null && stats.avgDualSidePriceSum < 1.01,
      description: 'Avg sum of YES+NO buy prices < $1.01 (free-arb bids)',
    },
    {
      name: 'maker_rebate_income',
      value: stats.rebateUsdcTotal,
      threshold: 100,
      triggered: (stats.rebateUsdcTotal || 0) > 100,
      description: 'Earns measurable maker rebates (>$100)',
    },
    {
      name: 'lp_reward_income',
      value: stats.rewardUsdcTotal,
      threshold: 100,
      triggered: (stats.rewardUsdcTotal || 0) > 100,
      description: 'Earns measurable LP / order-book rewards (>$100)',
    },
  ];

  const score = signals.filter(s => s.triggered).length;
  const isLikelyMM = score >= 4;

  let reason;
  if (score === 0) reason = 'clean — no MM signals triggered';
  else if (score <= 2) reason = 'weak MM tells — probably directional';
  else if (score === 3) reason = 'ambiguous — downweight but keep';
  else if (score <= 5) reason = 'strong MM pattern — not copy-tradeable';
  else reason = 'confirmed market-maker (whale-01 class)';

  return { score, signals, isLikelyMM, reason };
}

/**
 * Return the scoring-penalty multiplier for a given MM score. Consumed by
 * computeWalletScore via stats.mmPenalty.
 *
 * Calibration:
 *   0–2 → 1.0  (no penalty — directional wallet)
 *   3   → 0.5  (ambiguous — halve, don't kill; discoverable patterns)
 *   4–5 → 0.1  (strong MM — effectively evict)
 *   6   → 0.0  (confirmed MM — eliminate from ranking)
 */
export function mmPenaltyForScore(mmScore) {
  if (mmScore == null || mmScore < 3) return 1.0;
  if (mmScore === 3) return 0.5;
  if (mmScore < 6)   return 0.1;
  return 0.0;
}

/**
 * Convenience: attach mm classification to a stats object in place.
 * Returns the stats object for chaining. Idempotent — safe to call
 * multiple times.
 */
export function attachMMClassification(stats, opts = {}) {
  if (!stats) return stats;
  const result = classifyMarketMaker(stats, opts);
  stats.mmScore = result.score;
  stats.mmSignals = result.signals.map(s => ({
    name: s.name,
    triggered: s.triggered,
    value: s.value,
  }));
  stats.mmTriggered = result.signals.filter(s => s.triggered).map(s => s.name);
  stats.isLikelyMM = result.isLikelyMM;
  stats.mmReason = result.reason;
  stats.mmPenalty = mmPenaltyForScore(result.score);
  return stats;
}
