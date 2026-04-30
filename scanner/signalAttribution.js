/**
 * Signal attribution — feedback loop from signal outcomes back to wallet scoring.
 *
 * Problem this solves: `decidedROI` measures how skilled a wallet is at
 * trading OVERALL — including entry, exit, hedging, and position sizing.
 * When we emit a signal based on that wallet's buy, the signal only
 * captures the ENTRY portion of their strategy. If their alpha is mostly
 * in exit timing or sizing, copying their entries doesn't capture it,
 * and the signal underperforms their headline decidedROI.
 *
 * Historical attribution data (from scripts/wallet-attribution.mjs) showed
 * this gap dramatically: wallets with scores 30-35 producing -50%/-60% avg
 * signal returns, while wallets with scores 23-27 produced +60%/+160%.
 *
 * Fix: measure each wallet's historical SIGNAL PERFORMANCE (not trade
 * performance) and apply it as a multiplier to their score. Wallets whose
 * signals consistently win get boosted. Wallets whose signals consistently
 * lose get de-weighted below the PER_WALLET_MIN_SCORE threshold and stop
 * sourcing future signals — a self-correcting feedback loop.
 *
 * Multiplier is applied only when the wallet has ≥ 10 resolved signals to
 * their name — below that, sample too small, stay at 1.0 (neutral).
 */

/**
 * Build an attribution map from signal history.
 * @param {Array} signalsHistory - The array under signals.history
 * @returns {Map<string, { signals, wins, losses, wr, avgReturn }>}
 *          avgReturn is a DECIMAL fraction (e.g. 0.16 = +16% avg return)
 */
export function buildAttributionMap(signalsHistory) {
  const attr = new Map();
  if (!Array.isArray(signalsHistory)) return attr;

  const resolved = signalsHistory.filter(s => s && (s.outcome === 'win' || s.outcome === 'loss'));
  for (const sig of resolved) {
    const wallets = new Set();
    if (Array.isArray(sig.currentWallets)) {
      sig.currentWallets.forEach(w => w && w.address && wallets.add(w.address.toLowerCase()));
    }
    if (sig.soloWallet) wallets.add(String(sig.soloWallet).toLowerCase());
    if (Array.isArray(sig.wallets)) {
      sig.wallets.forEach(w => w && w.address && wallets.add(w.address.toLowerCase()));
    }

    // signalReturn is stored as PERCENTAGE (e.g. 162.2 means +162.2%).
    // Convert to fraction so downstream math is consistent.
    const retFrac = typeof sig.signalReturn === 'number' ? sig.signalReturn / 100 : null;

    for (const addr of wallets) {
      if (!attr.has(addr)) {
        attr.set(addr, { signals: 0, wins: 0, losses: 0, totalReturn: 0, returnCount: 0 });
      }
      const a = attr.get(addr);
      a.signals++;
      if (sig.outcome === 'win') a.wins++;
      else a.losses++;
      if (retFrac !== null) {
        a.totalReturn += retFrac;
        a.returnCount++;
      }
    }
  }

  // Finalize derived fields
  for (const a of attr.values()) {
    a.avgReturn = a.returnCount > 0 ? a.totalReturn / a.returnCount : 0;
    a.wr = a.signals > 0 ? a.wins / a.signals : 0;
  }
  return attr;
}

/**
 * Compute the score multiplier from a wallet's attribution record.
 *
 * Calibration (post 2026-04-30 audit — widened band, lowered sample floor):
 *   < 3 signals               → 1.0  (neutral — sample insufficient)
 *   avg return = 0%           → 1.0  (break-even)
 *   avg return = +25% / sig   → 1.5
 *   avg return = +50% / sig   → 2.0  (NEW cap — strong signal alpha rewarded)
 *   avg return = -12.5%       → 0.75
 *   avg return = -25%         → 0.50
 *   avg return = -40%         → 0.20
 *   avg return ≤ -50%         → 0.0  (NEW floor — bad emitter fully evicted from sourcing)
 *
 * Formula: multiplier = clamp(1 + avgReturn × 2, 0.0, 2.0)
 *
 * Why widen / lower the sample floor:
 *
 * The 2026-04-30 wallet-scoring-validity audit ran a Pearson correlation
 * across 14 wallets that had both trade-side stats and ≥5 emitted signals
 * with measurable returns. Findings:
 *   - score (current formula) vs signal EV:  r = 0.113   ← uncorrelated
 *   - tradeROI vs signal EV:                 r = -0.502  ← INVERSE
 *   - avgEntryPrice vs signal EV:            r = +0.675  ← strong, but n=14
 *   - sellRatio vs signal EV:                r = +0.387
 *
 * Translation: trade-side metrics (which dominate the score) don't predict
 * signal EV — they actively mispredict it. The only direct measurement of
 * signal EV for each wallet is its own attribution record. So we should
 * (a) start using attribution as soon as we have any sample at all (≥3),
 * and (b) let it dominate score when present. Widening the band means a
 * wallet with proven +50% signal returns gets 2× score, and a wallet with
 * proven -50% signal returns gets 0× score (effectively evicted from
 * signal sourcing). Self-correcting feedback loop, no manual tuning.
 *
 * The trade-side score still matters for wallets with <3 signals — those
 * are the prior. Once 3+ signals accumulate, attribution becomes the
 * posterior and dominates.
 */
export function attributionMultiplier(attribution, opts = {}) {
  // Min signals lowered 5→3. With n=3 at -50% avg return that's almost
  // certainly a real bad emitter (P(3 in a row by chance | true mean ≥ 0)
  // is small at sustained -50% scale). Tighter feedback loop.
  const minSignals = opts.minSignals ?? 3;
  if (!attribution || attribution.signals < minSignals) return 1.0;
  const raw = 1 + attribution.avgReturn * 2;
  return Math.max(0.0, Math.min(2.0, raw));
}

/**
 * Attach attribution-derived fields onto a wallet's stats so
 * computeWalletScore can consume them. Idempotent.
 *
 * @param {object} stats - Wallet stats object (mutated in place)
 * @param {Map} attributionMap - Output of buildAttributionMap
 * @param {string} address - Wallet address (case-insensitive)
 * @returns {object} The stats object for chaining
 */
export function attachAttribution(stats, attributionMap, address) {
  if (!stats || !attributionMap || !address) return stats;
  const attr = attributionMap.get(String(address).toLowerCase());
  if (attr) {
    stats.signalAttribution = {
      signals: attr.signals,
      wins: attr.wins,
      losses: attr.losses,
      wr: +attr.wr.toFixed(3),
      avgReturn: +attr.avgReturn.toFixed(3),
    };
    stats.attributionMultiplier = +attributionMultiplier(attr).toFixed(3);
  } else {
    stats.signalAttribution = null;
    stats.attributionMultiplier = 1.0;
  }
  return stats;
}
