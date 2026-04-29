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
 * Calibration:
 *   < 10 signals             → 1.0  (neutral — not enough sample to judge)
 *   avg return = 0%          → 1.0  (break-even, no boost or penalty)
 *   avg return = +25% per sig → 1.5 (capped boost)
 *   avg return = -12.5%       → 0.75
 *   avg return = -25%         → 0.50
 *   avg return ≤ -40%         → 0.2  (floored — effectively evicted from signal sourcing)
 *
 * Formula: multiplier = clamp(1 + avgReturn × 2, 0.2, 1.5)
 * At avgReturn = +0.162 (the top wallet): 1 + 0.324 = 1.324 (below 1.5 cap)
 *   actually 162% wasn't 0.162 — let me recheck. +162% = 1.62, so 1 + 3.24 = 4.24 → clamped to 1.5.
 * At avgReturn = -0.62 (the worst wallet): 1 - 1.24 = -0.24 → clamped to 0.2.
 */
export function attributionMultiplier(attribution, opts = {}) {
  // Min signals lowered from 10 to 5 (2026-04-28 audit). With 10-floor,
  // wallets like 0x6407a638ff (6 sigs at -66% avg return) escaped any
  // penalty because sample was below threshold. 5-floor still gives
  // statistical credibility while letting clearly-bad signal contributors
  // get penalized faster.
  const minSignals = opts.minSignals ?? 5;
  if (!attribution || attribution.signals < minSignals) return 1.0;
  const raw = 1 + attribution.avgReturn * 2;
  return Math.max(0.2, Math.min(1.5, raw));
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
