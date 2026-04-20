/**
 * Single-side alpha test — port of handpicked-signals/alpha_test.py.
 *
 * Most wallet ranking systems get fooled by mean-pickers: wallets that buy
 * $0.95 tokens and win 95% of the time. On paper they look like sharp bettors
 * (high WR, positive PnL, stable activity) but they have zero predictive edge —
 * the market was already right; they just collected the last 5¢ of spread.
 *
 * The single-side alpha test answers a different question than decidedROI:
 *
 *   decidedROI  = "did this wallet make money?"
 *   edge_pp     = "did this wallet beat the market's implied probability?"
 *
 * For a wallet's single-side bets (markets where they only bought one outcome):
 *
 *   edge_pp = (hit_rate - avg_entry_price) × 100
 *
 * A mean-picker buying $0.95 tokens with 95% WR: edge_pp = (0.95 - 0.95) × 100 = 0
 * An insight-driven bettor buying $0.40 with 55% WR: edge_pp = (0.55 - 0.40) × 100 = +15pp
 *
 * This is the test that caught cases where decidedROI was positive but the
 * wallet had no actual predictive power — they were just riding the spread.
 * decidedROI and edge_pp together catch both failure modes (no money AND no
 * predictive skill).
 *
 * Stage 2 of the handpicked-signals → wallet-engine merge.
 */

// ── Thresholds ─────────────────────────────────────────────────────────────
// These match the handpicked-signals defaults that yielded a 14.5% qualifier
// rate on 200-wallet screens. Tune here if hit rate changes dramatically.

export const ALPHA_THRESHOLDS = {
  // Minimum single-side resolved markets before edge_pp can be trusted.
  // Below this, variance dominates the signal.
  MIN_SINGLE_SIDE_RESOLVED: 50,

  // Edge bands for qualifying wallets:
  //   ≥ 3pp  → meets Tier-A alpha gate
  //   ≥ 1.5pp → partial credit (Tier B-ish)
  //   < 1.5pp → fails alpha gate
  TIER_A_MIN_EDGE_PP: 3.0,
  TIER_B_MIN_EDGE_PP: 1.5,

  // Supporting: wallet must also have positive single-side ROI. An
  // insight-only wallet that breaks even on dollars is still less valuable
  // than one that both predicts + makes money.
  MIN_SINGLE_SIDE_ROI: 0.05,

  // Sample size for the alpha signal to count separately from the MM gate.
  MIN_CAPITAL_FOR_EDGE: 5000, // $5k of single-side capital deployed
};

/**
 * Evaluate a wallet's alpha test result from analyzeTradeHistory stats.
 * Returns a verdict object with the three key fields callers need:
 *   - hasSignificantSample: bool (≥50 single-side resolved markets)
 *   - passesAlphaGate: bool (edge_pp ≥ 3pp AND ROI > 5% AND sample OK)
 *   - verdict: 'tier_a' | 'tier_b' | 'fails' | 'insufficient_sample'
 *
 * @param {object} stats - Output from analyzeTradeHistory (Stage 2+).
 *                         Requires: singleSideResolved, edgePP, singleSideROI,
 *                         singleSideCapital.
 * @param {object} [opts] - Override thresholds (merged onto ALPHA_THRESHOLDS).
 */
export function evaluateAlpha(stats, opts = {}) {
  const T = { ...ALPHA_THRESHOLDS, ...opts };
  const empty = {
    edgePP: null,
    hitRate: null,
    avgEntry: null,
    sampleN: 0,
    hasSignificantSample: false,
    passesAlphaGate: false,
    verdict: 'no_stats',
    reason: '',
  };
  if (!stats || typeof stats !== 'object') return empty;

  const n = stats.singleSideResolved || 0;
  const edgePP = stats.edgePP;
  const roi = stats.singleSideROI;
  const cap = stats.singleSideCapital || 0;

  const out = {
    edgePP,
    hitRate: stats.singleSideHitRate,
    avgEntry: stats.singleSideAvgEntry,
    sampleN: n,
    sampleCapital: cap,
    hasSignificantSample: n >= T.MIN_SINGLE_SIDE_RESOLVED && cap >= T.MIN_CAPITAL_FOR_EDGE,
    passesAlphaGate: false,
    verdict: 'fails',
    reason: '',
  };

  if (n < T.MIN_SINGLE_SIDE_RESOLVED) {
    out.verdict = 'insufficient_sample';
    out.reason = `only ${n} single-side resolved markets (< ${T.MIN_SINGLE_SIDE_RESOLVED})`;
    return out;
  }
  if (cap < T.MIN_CAPITAL_FOR_EDGE) {
    out.verdict = 'insufficient_capital';
    out.reason = `only $${cap.toFixed(0)} single-side capital (< $${T.MIN_CAPITAL_FOR_EDGE})`;
    return out;
  }
  if (edgePP == null) {
    out.verdict = 'no_edge_computed';
    out.reason = 'edgePP is null';
    return out;
  }
  if (edgePP < T.TIER_B_MIN_EDGE_PP) {
    out.verdict = 'fails';
    out.reason = `edge_pp ${edgePP.toFixed(2)}pp < ${T.TIER_B_MIN_EDGE_PP}pp minimum`;
    return out;
  }
  if (roi == null || roi < T.MIN_SINGLE_SIDE_ROI) {
    out.verdict = 'fails';
    out.reason = `single-side ROI ${((roi || 0) * 100).toFixed(1)}% < ${(T.MIN_SINGLE_SIDE_ROI * 100)}% minimum`;
    return out;
  }
  if (edgePP >= T.TIER_A_MIN_EDGE_PP) {
    out.passesAlphaGate = true;
    out.verdict = 'tier_a';
    out.reason = `edge_pp ${edgePP.toFixed(2)}pp, ROI ${(roi * 100).toFixed(1)}%, n=${n} — real directional alpha`;
  } else {
    // Between TIER_B and TIER_A — qualifies as confirmation-tier only.
    out.passesAlphaGate = false;
    out.verdict = 'tier_b';
    out.reason = `edge_pp ${edgePP.toFixed(2)}pp, ROI ${(roi * 100).toFixed(1)}%, n=${n} — weak positive signal`;
  }
  return out;
}

/**
 * Attach alpha evaluation to a stats object in place. Stores:
 *   stats.alphaVerdict   ('tier_a' | 'tier_b' | 'fails' | ...)
 *   stats.alphaPasses    (boolean — passesAlphaGate)
 *   stats.alphaReason    (human-readable rationale)
 * Returns the stats for chaining.
 */
export function attachAlphaEvaluation(stats, opts = {}) {
  if (!stats) return stats;
  const result = evaluateAlpha(stats, opts);
  stats.alphaVerdict = result.verdict;
  stats.alphaPasses = result.passesAlphaGate;
  stats.alphaReason = result.reason;
  return stats;
}
