/**
 * Position-centric ledger primitives shared between the scanner and offline
 * ground-truth scripts.
 *
 * These mirror the logic in scripts/wallet-ledger.cjs but are pure functions
 * with no I/O — callers feed in positions (already pulled from Goldsky) and
 * a marketLookup Map, and get back per-position verdicts + rollups.
 *
 * This is the core input to the new scoring formula (decidedROI /
 * decidedCapital / mean-picker detection). Keeping it separate from scan.js
 * so both the live rescore loop and batch validation scripts can share the
 * same classify code path.
 */

/**
 * Convert our global marketLookup entry (as stored in data/markets.json.gz)
 * into the { closed, tokenWon, currentPrice, question } shape the classifier
 * expects. Returns null if the entry is missing.
 */
export function adaptMarketFromLookup(entry) {
  if (!entry) return null;
  const closed = entry.marketClosed === true;
  let tokenWon = null;
  if (closed && entry.winningOutcome) {
    const thisOutcome = entry.outcome;
    if (thisOutcome) {
      tokenWon = String(thisOutcome).toLowerCase().trim()
        === String(entry.winningOutcome).toLowerCase().trim();
    }
  }
  return {
    closed,
    tokenWon,
    currentPrice: entry.currentPrice ?? null,
    question: entry.question || entry.title || '',
  };
}

/**
 * Classify a single position against its (adapted) market info.
 * Mirrors scripts/wallet-ledger.cjs `classify()` — keep in sync.
 */
export function classifyPosition(pos, market) {
  // Fully closed
  if (pos.sharesHeld < 0.01) {
    if (Math.abs(pos.realizedPnl) < 0.01 && pos.totalBought < 0.01) {
      return { status: 'phantom', outcome: 'phantom', truePnl: 0, decidedPnl: 0 };
    }
    if (pos.realizedPnl > 0.01) {
      return { status: 'closed_win', outcome: 'win', truePnl: pos.realizedPnl, decidedPnl: 0 };
    }
    if (pos.realizedPnl < -0.01) {
      return { status: 'closed_loss', outcome: 'loss', truePnl: pos.realizedPnl, decidedPnl: 0 };
    }
    return { status: 'closed_breakeven', outcome: 'scratch', truePnl: pos.realizedPnl, decidedPnl: 0 };
  }

  // Still holding shares
  if (!market) {
    return { status: 'unknown', outcome: 'unknown', truePnl: pos.realizedPnl, decidedPnl: null };
  }
  if (!market.closed) {
    const mtm = market.currentPrice != null
      ? pos.sharesHeld * market.currentPrice - pos.sharesHeld * pos.avgPrice
      : null;
    return {
      status: 'open',
      outcome: 'pending',
      truePnl: pos.realizedPnl + (mtm || 0),
      decidedPnl: 0,
      mtm,
    };
  }
  if (market.tokenWon === true) {
    const decidedPnl = pos.sharesHeld * (1 - pos.avgPrice);
    return { status: 'unredeemed_win', outcome: 'win', truePnl: pos.realizedPnl + decidedPnl, decidedPnl };
  }
  if (market.tokenWon === false) {
    const decidedPnl = -pos.sharesHeld * pos.avgPrice;
    return { status: 'worthless_loss', outcome: 'loss', truePnl: pos.realizedPnl + decidedPnl, decidedPnl };
  }
  return { status: 'closed_undetermined', outcome: 'unknown', truePnl: pos.realizedPnl, decidedPnl: null };
}

/**
 * Aggregate a set of positions into a decided-truth rollup. This is what
 * the scoring formula keys on — ROI of resolved capital only, with the
 * unredeemed-winner / worthless-loser cases folded in so wallets can't
 * hide by refusing to redeem.
 *
 * Input positions are expected in the same shape fetchPositions produces:
 *   { tokenId, sharesHeld, avgPrice, realizedPnl, totalBought }
 * (all numeric, post-USDC-divisor).
 */
export function aggregatePositions(positions, marketLookup) {
  // Split into two parallel ledgers. Decided (closed_win, closed_loss,
  // closed_breakeven, unredeemed_win, worthless_loss) feeds decidedPnl
  // and decidedCapital. Undecided (open / unknown / closed_undetermined)
  // tracked separately so realizedPnl from partial sells on still-open
  // or unclassifiable positions can't contaminate decidedROI.
  //
  // Pre-fix this function summed realizedPnl across every non-phantom
  // position into one accumulator, then divided by (totalCost minus
  // openCapitalAtRisk). Wins/losses counters and the ROI numerator were
  // computed against different sets — for thin-data wallets this gave
  // absurd values like decidedROI=9841% on $5 cap with 0W/1L.

  let decidedRealized = 0, decidedSettled = 0, decidedTotalBought = 0;
  let undecidedRealized = 0, undecidedCost = 0, openMtm = 0;
  let openCapitalAtRisk = 0;
  let wins = 0, losses = 0, open = 0, scratch = 0;
  let unresolvedMarket = 0, closedUndetermined = 0;
  let unredeemedWins = 0, worthlessLosses = 0;
  let phantomSkipped = 0;

  const DECIDED_STATUSES = new Set([
    'closed_win', 'closed_loss', 'closed_breakeven',
    'unredeemed_win', 'worthless_loss',
  ]);

  for (const pos of positions) {
    const mEntry = marketLookup?.get(pos.tokenId);
    const market = adaptMarketFromLookup(mEntry);
    const verdict = classifyPosition(pos, market);

    if (verdict.status === 'phantom') { phantomSkipped++; continue; }

    if (DECIDED_STATUSES.has(verdict.status)) {
      decidedRealized += pos.realizedPnl;
      if (verdict.decidedPnl != null) decidedSettled += verdict.decidedPnl;
      decidedTotalBought += pos.totalBought;
    } else {
      undecidedRealized += pos.realizedPnl;
      undecidedCost += pos.totalBought;
      if (verdict.outcome === 'pending') openCapitalAtRisk += pos.totalBought;
      if (verdict.status === 'open' && verdict.mtm != null) openMtm += verdict.mtm;
    }

    if (verdict.outcome === 'win') wins++;
    else if (verdict.outcome === 'loss') losses++;
    else if (verdict.outcome === 'pending') open++;
    else if (verdict.outcome === 'scratch') scratch++;
    else if (verdict.status === 'closed_undetermined') closedUndetermined++;
    else if (verdict.status === 'unknown') unresolvedMarket++;

    if (verdict.status === 'unredeemed_win') unredeemedWins++;
    if (verdict.status === 'worthless_loss') worthlessLosses++;
  }

  const resolvedCount = wins + losses;
  const winRate = resolvedCount > 0 ? wins / resolvedCount : null;
  const decidedCapital = decidedTotalBought;
  const decidedPnl = decidedRealized + decidedSettled;
  const decidedROI = decidedCapital > 0 ? decidedPnl / decidedCapital : null;
  const realized = decidedRealized + undecidedRealized;
  const decided = decidedSettled;
  const totalCost = decidedTotalBought + undecidedCost;
  const totalRoi = totalCost > 0 ? (realized + decided + openMtm) / totalCost : null;

  return {
    total: positions.length - phantomSkipped,
    wins, losses, open, scratch,
    closedUndetermined, unresolvedMarket,
    resolvedCount,
    winRate,
    realizedPnl: +realized.toFixed(2),
    decidedUnredeemedPnl: +decided.toFixed(2),
    decidedPnl: +decidedPnl.toFixed(2),
    openMtm: +openMtm.toFixed(2),
    truePnl: +(realized + decided + openMtm).toFixed(2),
    totalCost: +totalCost.toFixed(2),
    decidedCapital: +decidedCapital.toFixed(2),
    openCapitalAtRisk: +openCapitalAtRisk.toFixed(2),
    decidedROI,
    totalRoi,
    unredeemedWins,
    worthlessLosses,
    phantomSkipped,
    // Mean-picker flag: high WR on resolved, very low decided ROI on
    // meaningful capital. The ranker will use this to suppress scores.
    isMeanPickerShape: (
      resolvedCount >= 25 &&
      winRate != null && winRate >= 0.95 &&
      decidedCapital >= 50000 &&
      decidedROI != null && decidedROI < 0.05
    ),
  };
}
