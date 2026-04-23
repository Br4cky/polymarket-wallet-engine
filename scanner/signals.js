/**
 * Signal Engine v2 — Trade-Convergence-Based Signal Generation
 *
 * Instead of detecting position overlap from snapshots, this engine detects
 * when multiple tracked wallets are actively BUYING into the same market
 * within a recent time window. This ensures signals represent live
 * intelligence, not stale holdings.
 *
 * Signal lifecycle:
 *   1. DETECT — fast loop finds multiple wallets recently bought same market
 *   2. OPEN — convergence crosses thresholds → signal created with real timestamps
 *   3. ACTIVE — updated each scan with new trade data, exit monitoring
 *   4. CLOSED — market resolves, wallets exit, or signal expires
 */

import { resolveMarkets, matchesWinningOutcome, loadGzJSON, saveGzJSON } from './lib.js';

// ============================================================================
// Signal Thresholds
// ============================================================================
//
// Scale note: wallet.score runs 0–55 in practice (top decile ≥ 25). All
// score-based thresholds below are calibrated for that range. Do not bump
// to legacy 0–100 values without also rescaling the confidence formulas
// (computeConvergenceConfidence / computeSoloConfidence) — they have a
// bunch of divisors (score / 45, avgScore / 40) that must stay in sync.

const SIGNAL_THRESHOLDS = {
  // Convergence window — how recent trades must be to count as "active convergence"
  CONVERGENCE_WINDOW_HOURS: 48,     // Trades within last 48 hours count

  // Consensus signals — larger group of tracked wallets converging on a market.
  // Historic: 153 resolved, 51.6% WR, +16.5% avg return. Profitable as-is.
  CONSENSUS_MIN_WALLETS: 8,         // 8+ wallets
  CONSENSUS_MIN_AVG_SCORE: 12,      // avg ≥ 12 on 0–55 scale ≈ solid pool median
  CONSENSUS_MIN_TOTAL_SIZE: 1000,   // $1000+ total buy size across wallets

  // Cluster signals — small group of strong wallets.
  // Historic 49.0% WR / -5.8% return was structurally broken. Deep analysis
  // showed 4-5 wallets is the single WORST band (237 signals, -12.6%).
  // 6-7 wallets is marginal (-1.5%). So cluster now = 6-7 wallets only —
  // the 4-5 noise band is eliminated entirely. Plus tight quality gates.
  CLUSTER_MIN_WALLETS: 6,               // was 3 — 4-5 is structurally -EV
  CLUSTER_MAX_WALLETS: 7,               // anything ≥ 8 escalates to consensus
  CLUSTER_MIN_AVG_SCORE: 25,            // top decile on 0–55 scale
  CLUSTER_MIN_TOTAL_SIZE: 750,          // real conviction required
  CLUSTER_MIN_PER_WALLET_SCORE: 18,     // every wallet must be top quartile

  // Per-wallet score floor for consensus + fallback for cluster
  // (cluster has its own CLUSTER_MIN_PER_WALLET_SCORE override above).
  // Raised 10→15 — below this, the wallet doesn't have enough edge
  // signal to meaningfully contribute to any convergence signal.
  PER_WALLET_MIN_SCORE: 15,

  // Solo signals — single top-tier wallet, significant new buy.
  // Historic 60.5% WR / -3.3% return overall, but category-dependent:
  //   solo × sports     : 65.2% WR, +101.7% avg return  ← gold
  //   solo × politics   :   90% WR, +10.6% avg return
  //   solo × crypto-udn :   88.7% WR, +0.5% avg return
  //   solo × other      :   58.0% WR, -7.3% avg return  ← problem
  //   solo × esports    :   43.2% WR, -17.6% avg return ← killed via EXCLUDED
  //
  // Also entry-price-dependent: 20-40¢ underdog solos lost -38%, 60-80¢
  // favorites profited +2.3%, 80-87¢ near-certains profited +8.7%. So
  // underdog entries are actually -EV for solos in our pool — DO NOT cap
  // the upper entry price. Just require a high-score wallet + decent size.
  SOLO_MIN_SCORE: 30,               // was 25 — elite wallets only
  SOLO_MIN_WIN_RATE: 0.55,          // WR secondary to decided edge; 55% = real edge
  SOLO_MIN_RESOLVED: 50,
  SOLO_MIN_BUY_SIZE: 500,           // $500+ buy in a single market
  SOLO_MAX_PER_WALLET: 3,

  // Excluded market keywords — categories with confirmed negative EV from
  // deep analysis of 1,446 resolved signals. Data-driven exclusions only.
  //
  //   Esports:     280 resolved, 43.9% WR, -13.0% avg return   ✗
  //   NHL:          22 resolved, 45.5% WR, -15.9% avg return   ✗
  //   Golf/PGA:     26 resolved, 57.7% WR,  -8.5% avg return   ✗
  //   US Politics:  10 resolved, 50.0% WR, -26.8% avg return   ✗
  //
  // Kept (profitable, do NOT add here):
  //   Crypto up/down: 130 resolved, 71.5% WR, +1.7% avg   ✓
  //   UFC/MMA:         14 resolved, 71.4% WR, +38.7% avg  ✓ best WR
  //   Tennis, MLB, Weather, EPL, NBA — all ✓
  EXCLUDED_KEYWORDS: [
    // Esports (280 signals, -13%)
    'dota', 'lol', 'league of legends', 'counter-strike', 'valorant', 'csgo',
    'cs:go', 'cs2', 'call of duty', 'rocket league', 'overwatch', 'starcraft',
    'hearthstone', 'apex legends', 'fortnite', 'pubg',
    // NHL (22 signals, -16%) — our alphas don't have ice-hockey edge
    'nhl', 'stanley cup', ' hockey ',
    // Golf / PGA (26 signals, -8.5%)
    ' pga ', 'golf', 'masters tournament',
    // US political election markets (10 signals, -27%)
    'trump vs', 'biden vs', 'harris vs', 'republican primary', 'democrat primary',
    'election day', 'electoral college', 'senate race', 'house race',
    'presidential election', 'congressional',
  ],

  // Category whitelist — only markets falling into these classifier buckets
  // are allowed to emit signals. Data-driven from sizing-simulator.mjs:
  // applying this whitelist over 1,446 historical signals lifted weighted
  // return from -2.2% to +17.7% (+20pp). Volume drops meaningfully, but the
  // remaining signals are 71% WR vs 54% baseline.
  //
  // Per-category backtest (avg return, N signals):
  //   tennis        +151%   13   ✓
  //   crypto-other   +41%    3   ✓
  //   nba            +35%   33   ✓
  //   mma            +21%   16   ✓
  //   weather        +20%   44   ✓   (84% WR — most consistent)
  //   crypto-updown   +0%  147   ✓   (huge sample, slightly positive)
  //   mlb/nfl/macro    —    —        (no data — admit as neutral)
  //   other           -3%  821         excluded (mostly un-classified noise)
  //   politics        -7%   17         excluded (residual election markets)
  //   soccer          -7%   46         excluded (ambiguous — EPL+smaller mixed)
  //   esports        -14%  303         already keyword-excluded
  //
  // Unclassified ("other") is excluded on the premise that anything we
  // can't categorise well is more likely to be noise than edge. The
  // classifyMarket() regex is actively being widened — any profitable
  // subtype discovered gets promoted to a named category.
  ALLOWED_CATEGORIES: new Set([
    'tennis', 'nba', 'mma', 'weather',
    'crypto-updown', 'crypto-other',
    // Neutral/small-sample but not negative — admit by default:
    'mlb', 'nfl', 'macro', 'ai-tech',
    // Token launches and conditional-event markets isolated by the
    // widened classifier. These had been dumped into "other" before.
    'token-launch', 'news-event',
  ]),

  // Lifecycle
  STALE_HOURS: 96,                  // Close signal if no new buys for 96 hours
  MAX_SIGNAL_LIFETIME_HOURS: 600,   // ~25 days max lifetime (safety valve)

  // EV filter — implied-ROI based. Enforced at signal OPEN on BOTH the wallet's
  // average fill price AND the live market price (what a follower would pay).
  // Works symmetrically for YES/NO — currentPrice is always the price of the
  // token the wallet bought (and the token a follower would buy), so a buy on
  // the NO side at 10¢ correctly shows as 900% max ROI, not 10¢ = bad.
  //
  // Max ROI on a binary outcome token = (1 / price) - 1. A 15% floor maps to a
  // price ceiling of ~0.8696. What-if on 479 historical signals: dropping the
  // sub-15% cohort (199 signals, cum return −48%) raises avgReturn 6.54%→11.36%
  // while preserving cumulative return — the killed bucket is 98% WR noise of
  // near-resolved scraps where single losses wipe out many tiny wins.
  MIN_ENTRY_PRICE: 0,               // 0 = disabled. Set to e.g. 0.10 to filter
  MIN_WALLET_ROI: 0.15,             // Wallet's fill must have ≥15% max upside. 0 = disabled.
  MIN_OPEN_ROI: 0.15,               // Live price at publish must have ≥15% max upside. 0 = disabled.

  // Stale-follower gate. A follower entering a signal at live price gets
  // a worse entry than the sourcing wallet if the market has moved. Cap
  // the acceptable premium — 0.15 means the follower's price cannot be
  // more than 15% above the wallet's avg entry. Mathematically: if alpha
  // bought at 0.40 and live is now 0.46 (+15%), signal allowed; at 0.47,
  // signal rejected because the follower has already lost 50%+ of the move.
  // 0 = disabled.
  STALE_FOLLOWER_MAX_PREMIUM: 0.15,
};

// Implied-ROI helper: for a binary outcome token priced at p ∈ (0,1),
// max ROI if it resolves TRUE = (1 / p) - 1. E.g. p=0.10 → 900%, p=0.87 → 15%.
function impliedMaxROI(price) {
  if (!(price > 0) || price >= 1) return 0;
  return (1 / price) - 1;
}

// Returns true if a market title matches an excluded keyword (esports,
// short-duration crypto up/down, etc.). Used at signal emission to kill
// categories that are too efficient for wallet alpha to matter.
function isExcludedMarket(title) {
  if (!title || typeof title !== 'string') return false;
  const t = title.toLowerCase();
  return SIGNAL_THRESHOLDS.EXCLUDED_KEYWORDS.some(k => t.includes(k));
}

/**
 * Classify a market title into a category bucket. The classifier is the
 * mechanism by which ALLOWED_CATEGORIES / whitelist gating operates.
 *
 * Widened from the analytics version to capture subtypes that were dumped
 * into "other" (821/1446 signals ≈ 57%): token launches, news events, macro
 * data releases, IPO/earnings, space missions, conditional-calendar markets.
 *
 * Return values must match ALLOWED_CATEGORIES keys exactly (or return
 * 'other' / 'politics' / 'soccer' etc. to be rejected by the whitelist).
 */
function classifyMarket(title) {
  const q = (title || '').toLowerCase();
  if (!q) return 'other';

  // Esports (defensive — also covered by EXCLUDED_KEYWORDS)
  if (/dota|lol|league of legends|counter-strike|valorant|cs:go|cs2|csgo|call of duty|rocket league|overwatch|starcraft|hearthstone|apex|fortnite|pubg/.test(q)) return 'esports';

  // Crypto — directional (up/down, reach-price) vs other (launches, events)
  if (/\blaunch a token|\btoken launch|\btge\b|token on /.test(q)) return 'token-launch';
  if (/bitcoin|btc|ethereum|eth|solana|sol\b|doge|xrp|crypto|coin/.test(q)) {
    if (/reach|above|below|hit|close|\$|\sup\b|\sdown\b|end above|end below/.test(q)) return 'crypto-updown';
    return 'crypto-other';
  }

  // Sports — order matters (NHL before generic hockey, NBA before generic playoffs)
  if (/nhl|stanley cup| hockey /.test(q)) return 'nhl';
  if (/nba|lakers|celtics|warriors|\bknicks\b|\bheat\b|nba playoffs/.test(q)) return 'nba';
  if (/nfl|super bowl|touchdown|quarterback/.test(q)) return 'nfl';
  if (/mlb|baseball|world series/.test(q)) return 'mlb';
  if (/epl|premier league|champions league|la liga|bundesliga|serie a|manchester|arsenal|liverpool|chelsea/.test(q)) return 'soccer';
  if (/tennis|wimbledon|\bus open\b|french open|atp|wta/.test(q)) return 'tennis';
  if (/ufc|\bmma\b|fight night/.test(q)) return 'mma';
  if (/ pga |golf|masters tournament/.test(q)) return 'golf';
  if (/f1\b|formula 1|grand prix/.test(q)) return 'f1';

  // Politics — restricted (election keywords already in EXCLUDED_KEYWORDS)
  if (/trump|biden|harris|election|senate|house race|republican|democrat|congressional/.test(q)) return 'politics';

  // Science / tech / news
  if (/ai\b|openai|anthropic|gpt|gemini|\bllm\b|tech|apple|google|microsoft|nvidia/.test(q)) return 'ai-tech';
  if (/weather|temperature|hurricane|tornado|snow|rainfall/.test(q)) return 'weather';
  if (/fed rate|fomc|inflation|cpi|ppi|\bgdp\b|recession|jobs report|unemployment|nonfarm/.test(q)) return 'macro';
  if (/ipo|earnings|revenue|guidance|\beps\b/.test(q)) return 'macro';
  if (/spacex|starship|mission|\bnasa\b|rocket launch/.test(q)) return 'news-event';
  if (/announce|statement|\bsay\b|\bsays\b|tweet|post|comment/.test(q)) return 'news-event';

  // Conditional calendar markets ("Will X happen by <date>") — sweep into
  // news-event unless otherwise classified. Fat-tail behaviour; some edge.
  if (/\bwill .+ by |\bwill .+ before |\bwill .+ on /.test(q)) return 'news-event';

  return 'other';
}

/**
 * Returns true if a market is allowed under the category whitelist. Never
 * returns true for keyword-excluded markets regardless of category.
 */
function isWhitelistedCategory(title) {
  if (isExcludedMarket(title)) return false;
  const cat = classifyMarket(title);
  return SIGNAL_THRESHOLDS.ALLOWED_CATEGORIES.has(cat);
}

// ============================================================================
// Trade Convergence Detection
// ============================================================================

/**
 * Detect markets where multiple tracked wallets have recently bought.
 * This is the core signal source — replaces snapshot-based consensus.
 *
 * @param {Map<string, Array>} recentTrades - wallet → recent trades from Data API
 * @param {Map<string, object>} walletPool - wallet → { score, stats, ... }
 * @param {Map} marketLookup - tokenId → market info (from Gamma)
 * @returns {Array} Array of convergence candidates, sorted by strength
 */
function detectConvergence(recentTrades, walletPool, marketLookup) {
  const now = Math.floor(Date.now() / 1000);
  const windowTs = now - (SIGNAL_THRESHOLDS.CONVERGENCE_WINDOW_HOURS * 3600);

  // Group recent BUY trades by market (conditionId)
  // conditionId is the stable market identifier that groups Yes/No tokens
  const marketBuys = new Map(); // conditionId → { wallets: Map<addr, trades[]>, meta }

  for (const [wallet, trades] of recentTrades) {
    const walletInfo = walletPool.get(wallet);
    if (!walletInfo) continue;

    // Hard sourcing gates — a wallet contributing to a signal must have
    // proven it's directional alpha, not market-maker, not mean-picker,
    // not known-failed on the alpha test. These are individually checked
    // at eviction time but the rescore loop only runs every 24h. Without
    // this gate, a still-in-pool MM wallet would emit signals until the
    // next rescore catches it. Applying here closes that leak window.
    const stats = walletInfo.stats || {};
    if (stats.isLikelyMM === true) continue;
    if (stats.isMeanPickerShape === true) continue;
    if (stats.alphaVerdict === 'fails') continue;

    // Per-wallet score floor — filters wallets that pass all quality
    // gates but are still too weak to contribute meaningful edge.
    if ((walletInfo.score || 0) < SIGNAL_THRESHOLDS.PER_WALLET_MIN_SCORE) continue;

    for (const trade of trades) {
      if (trade.side !== 'BUY') continue;
      if (trade.timestamp < windowTs) continue;

      const cid = trade.conditionId;
      if (!cid) continue;

      if (!marketBuys.has(cid)) {
        marketBuys.set(cid, {
          conditionId: cid,
          title: trade.title || '',
          slug: trade.slug || '',
          eventSlug: trade.eventSlug || '',
          asset: trade.asset || '',
          outcome: trade.outcome || '',
          outcomeIndex: trade.outcomeIndex,
          wallets: new Map(),
        });
      }

      const mb = marketBuys.get(cid);
      if (!mb.wallets.has(wallet)) {
        mb.wallets.set(wallet, {
          address: wallet,
          score: walletInfo.score || 0,
          trades: [],
        });
      }
      mb.wallets.get(wallet).trades.push(trade);
    }
  }

  // Build convergence candidates
  const candidates = [];

  for (const [cid, mb] of marketBuys) {
    const walletCount = mb.wallets.size;
    if (walletCount < SIGNAL_THRESHOLDS.CLUSTER_MIN_WALLETS) continue;

    // Compute aggregate metrics
    let totalSize = 0;
    let totalScoreWeighted = 0;
    let totalScore = 0;
    let avgPrice = 0;
    let priceSum = 0;
    let priceCount = 0;
    let earliestBuy = Infinity;
    let latestBuy = 0;
    const walletDetails = [];

    for (const [addr, wData] of mb.wallets) {
      const walletBuySize = wData.trades.reduce((s, t) => s + (t.size * t.price), 0);
      const walletAvgPrice = wData.trades.reduce((s, t) => s + t.price, 0) / wData.trades.length;
      const walletEarliestBuy = Math.min(...wData.trades.map(t => t.timestamp));
      const walletLatestBuy = Math.max(...wData.trades.map(t => t.timestamp));

      totalSize += walletBuySize;
      totalScore += wData.score;
      totalScoreWeighted += wData.score * walletBuySize; // score weighted by conviction
      priceSum += walletAvgPrice;
      priceCount++;
      earliestBuy = Math.min(earliestBuy, walletEarliestBuy);
      latestBuy = Math.max(latestBuy, walletLatestBuy);

      walletDetails.push({
        address: addr,
        score: wData.score,
        buySize: +walletBuySize.toFixed(2),
        avgPrice: +walletAvgPrice.toFixed(4),
        tradeCount: wData.trades.length,
        firstBuy: walletEarliestBuy,
        lastBuy: walletLatestBuy,
      });
    }

    const avgScore = totalScore / walletCount;
    avgPrice = priceCount > 0 ? priceSum / priceCount : 0;
    const scoreWeightedAvg = totalSize > 0 ? totalScoreWeighted / totalSize : avgScore;

    // Determine direction — which outcome are wallets buying?
    const direction = mb.outcome || 'Unknown';

    candidates.push({
      conditionId: cid,
      title: mb.title,
      slug: mb.slug,
      eventSlug: mb.eventSlug,
      asset: mb.asset,
      direction,
      outcomeIndex: mb.outcomeIndex,

      // Convergence metrics
      walletCount,
      avgScore: +avgScore.toFixed(2),
      scoreWeightedAvg: +scoreWeightedAvg.toFixed(2),
      totalBuySize: +totalSize.toFixed(2),
      avgEntryPrice: +avgPrice.toFixed(4),

      // Timing
      earliestBuy,
      latestBuy,
      convergenceSpanHours: +((latestBuy - earliestBuy) / 3600).toFixed(1),

      // Wallet breakdown
      wallets: walletDetails.sort((a, b) => b.score - a.score),
    });
  }

  // Sort by wallet count × avg score (convergence strength)
  return candidates.sort((a, b) => {
    const strengthA = a.walletCount * a.avgScore;
    const strengthB = b.walletCount * b.avgScore;
    return strengthB - strengthA;
  });
}

// ============================================================================
// Signal Generation
// ============================================================================

/**
 * Process convergence candidates into signals.
 * Opens new signals, updates existing ones, detects exits, and closes stale/resolved.
 *
 * @param {Array} candidates - From detectConvergence()
 * @param {object} existingSignals - { active: {}, history: [], stats: {} }
 * @param {Map<string, Array>} recentTrades - wallet → trades (includes SELL for exit detection)
 * @param {Map<string, object>} walletPool - wallet → { score, stats }
 * @param {Map} marketLookup - tokenId → market info
 * @param {number} scanIndex - Current scan number
 * @returns {object} Updated signals { active, history, stats }
 */
function processSignals(candidates, existingSignals, recentTrades, walletPool, marketLookup, scanIndex) {
  const active = { ...(existingSignals.active || {}) };
  const history = [...(existingSignals.history || [])];
  const now = new Date().toISOString();
  const nowTs = Math.floor(Date.now() / 1000);

  let opened = 0, updated = 0, closed = 0;
  const seenMarkets = new Set();

  // --- Phase 1: Process convergence candidates → open or update signals ---
  for (const candidate of candidates) {
    const cid = candidate.conditionId;
    seenMarkets.add(cid);

    const signalId = `sig_${cid}`;
    const walletCount = candidate.walletCount;
    const avgScore = candidate.avgScore;
    const totalSize = candidate.totalBuySize;

    // Category gate — whitelist-driven; also runs keyword exclusions.
    // See ALLOWED_CATEGORIES for the data-backed list.
    if (!isWhitelistedCategory(candidate.title)) continue;

    // Classify signal type
    let signalType, meetsThresholds;

    if (walletCount >= SIGNAL_THRESHOLDS.CONSENSUS_MIN_WALLETS) {
      signalType = 'consensus';
      meetsThresholds = avgScore >= SIGNAL_THRESHOLDS.CONSENSUS_MIN_AVG_SCORE &&
        totalSize >= SIGNAL_THRESHOLDS.CONSENSUS_MIN_TOTAL_SIZE;
    } else if (walletCount >= SIGNAL_THRESHOLDS.CLUSTER_MIN_WALLETS &&
               walletCount <= SIGNAL_THRESHOLDS.CLUSTER_MAX_WALLETS) {
      signalType = 'cluster';
      // Cluster-specific extra floor: EVERY wallet in the cluster must meet
      // CLUSTER_MIN_PER_WALLET_SCORE. Catches the case where a cluster's
      // average passes but one marginal wallet is dragging the edge down.
      const allMeetFloor = (candidate.wallets || []).every(w =>
        (w.score || 0) >= SIGNAL_THRESHOLDS.CLUSTER_MIN_PER_WALLET_SCORE
      );
      meetsThresholds = allMeetFloor
        && avgScore >= SIGNAL_THRESHOLDS.CLUSTER_MIN_AVG_SCORE
        && totalSize >= SIGNAL_THRESHOLDS.CLUSTER_MIN_TOTAL_SIZE;
    } else {
      continue; // Below minimum wallet count
    }

    // EV filter — wallet fill price floor (rare) and implied-ROI ceiling.
    if (SIGNAL_THRESHOLDS.MIN_ENTRY_PRICE > 0 && candidate.avgEntryPrice < SIGNAL_THRESHOLDS.MIN_ENTRY_PRICE) continue;
    if (SIGNAL_THRESHOLDS.MIN_WALLET_ROI > 0 &&
        impliedMaxROI(candidate.avgEntryPrice) < SIGNAL_THRESHOLDS.MIN_WALLET_ROI) continue;

    if (active[signalId]) {
      // --- UPDATE existing signal ---
      const signal = active[signalId];
      signal.lastUpdatedAt = now;
      signal.lastUpdatedScan = scanIndex;
      signal.lastTradeTs = candidate.latestBuy;

      // Update metrics
      signal.walletCount = walletCount;
      signal.avgScore = avgScore;
      signal.totalBuySize = totalSize;
      signal.avgEntryPrice = candidate.avgEntryPrice;
      signal.signalType = signalType; // Can upgrade cluster → consensus
      signal.conviction = +totalSize.toFixed(2); // Backward compat
      signal.consensusStrength = +(candidate.scoreWeightedAvg / 100 || 0).toFixed(3);

      // Update live market price
      const tokenId = candidate.asset || signal.tokenId;
      const mi = tokenId ? marketLookup.get(tokenId) : null;
      if (mi && mi.currentPrice > 0) {
        signal.currentMarketPrice = +(mi.currentPrice || 0).toFixed(4);
      }

      // Recompute confidence
      signal.confidence = computeConvergenceConfidence(candidate, signalType);
      signal.tier = getSignalTier(signal.confidence);

      // Peak tracking
      signal.peakWallets = Math.max(signal.peakWallets || 0, walletCount);
      signal.peakConfidence = Math.max(signal.peakConfidence || 0, signal.confidence);

      // Wallet snapshot
      signal.currentWallets = candidate.wallets;

      updated++;

    } else if (meetsThresholds) {
      // --- Check market isn't already resolved ---
      const tokenId = candidate.asset || '';
      const mi = tokenId ? marketLookup.get(tokenId) : null;
      if (mi && mi.marketClosed === true) continue;

      // --- OPEN new signal ---
      const confidence = computeConvergenceConfidence(candidate, signalType);
      const currentPrice = mi ? +(mi.currentPrice || 0).toFixed(4) : 0;

      // Require a valid live price — without it we can't track return,
      // can't run the MIN_OPEN_ROI filter, and the dashboard shows "-".
      if (!(currentPrice > 0)) continue;

      // EV filter on live market price — what a follower would actually pay.
      // Rejects signals where implied max ROI on the signal side is below
      // MIN_OPEN_ROI (e.g. YES at 0.90 = 11.1% ROI, cut at 15% floor).
      if (SIGNAL_THRESHOLDS.MIN_OPEN_ROI > 0 &&
          impliedMaxROI(currentPrice) < SIGNAL_THRESHOLDS.MIN_OPEN_ROI) continue;

      // Stale-follower gate — reject if the live price has already run past
      // the wallet's entry by more than STALE_FOLLOWER_MAX_PREMIUM. If the
      // alpha got in at 30¢ and it's now 50¢, a follower entering at 50¢
      // has already given up 67% of the move. We'd be emitting a signal
      // for a trade already won on paper.
      if (SIGNAL_THRESHOLDS.STALE_FOLLOWER_MAX_PREMIUM > 0
          && candidate.avgEntryPrice > 0
          && currentPrice > candidate.avgEntryPrice * (1 + SIGNAL_THRESHOLDS.STALE_FOLLOWER_MAX_PREMIUM)) {
        continue;
      }

      active[signalId] = {
        signalId,
        signalType,
        conditionId: cid,
        tokenId,
        marketTitle: candidate.title,
        slug: candidate.slug,
        eventSlug: candidate.eventSlug,
        groupKey: cid, // Use conditionId as groupKey (stable across tokens)

        // Direction
        direction: candidate.direction,
        outcomeIndex: candidate.outcomeIndex,

        // Timing
        openedAt: now,
        openedScan: scanIndex,
        lastUpdatedAt: now,
        lastUpdatedScan: scanIndex,
        lastTradeTs: candidate.latestBuy,
        earliestBuy: candidate.earliestBuy,
        scansActive: 1,

        // Convergence metrics
        walletCount,
        avgScore,
        totalBuySize: totalSize,
        avgEntryPrice: candidate.avgEntryPrice,
        convergenceSpanHours: candidate.convergenceSpanHours,

        // Backward-compat fields for frontend
        conviction: +totalSize.toFixed(2),
        consensusStrength: +(candidate.scoreWeightedAvg / 100 || 0).toFixed(3),
        avgPnl: 0, // Not applicable in v2 — trades don't carry PnL at open

        // Price at signal open (frozen — never updated)
        openMarketPrice: currentPrice,
        // Live price (updated each scan)
        currentMarketPrice: currentPrice,

        // Confidence
        confidence: +confidence.toFixed(1),
        tier: getSignalTier(confidence),

        // Peak tracking
        peakWallets: walletCount,
        peakConfidence: confidence,

        // Status
        status: 'active',
        outcome: null,
        closedAt: null,
        closedScan: null,
        closeReason: null,

        // Wallet snapshot
        currentWallets: candidate.wallets,
      };

      opened++;
    }
  }

  // --- Phase 1b: Solo signals — single top-tier wallet, big recent buy ---
  const soloCountByWallet = new Map(); // track max per wallet
  for (const [wallet, trades] of recentTrades) {
    const walletInfo = walletPool.get(wallet);
    if (!walletInfo) continue;

    // Same sourcing gates as consensus/cluster — MM, mean-picker, or
    // alpha-failed wallets cannot source solo signals regardless of score.
    // A wallet hitting SOLO_MIN_SCORE but failing these is almost always a
    // false-positive we'd otherwise have to chase down in post-hoc analysis.
    const stats = walletInfo.stats || {};
    if (stats.isLikelyMM === true) continue;
    if (stats.isMeanPickerShape === true) continue;
    if (stats.alphaVerdict === 'fails') continue;

    if ((walletInfo.score || 0) < SIGNAL_THRESHOLDS.SOLO_MIN_SCORE) continue;
    if ((stats.recentWinRate || stats.winRate || 0) < SIGNAL_THRESHOLDS.SOLO_MIN_WIN_RATE) continue;
    if ((stats.resolvedMarkets || 0) < SIGNAL_THRESHOLDS.SOLO_MIN_RESOLVED) continue;
    // Solo signals prefer proven single-side alpha. Require either a good
    // alpha verdict (tier_a / tier_b) OR genuinely insufficient sample
    // (still learning). Explicit 'fails' already rejected above.
    if (stats.alphaVerdict === 'tier_b' || stats.alphaVerdict === 'tier_a') {
      // best case — fall through
    } else if (stats.alphaVerdict === 'insufficient_sample' || stats.alphaVerdict === 'insufficient_capital') {
      // acceptable — unproven but not disqualified
    } else if (!stats.alphaVerdict) {
      // pre-Stage-2 wallet — allow on the assumption score + WR carry enough signal
    } else {
      continue; // any other verdict (no_edge_computed, etc.) — skip
    }

    // Count existing solo signals for this wallet
    const existingSoloCount = Object.values(active).filter(s =>
      s.signalType === 'solo' && s.soloWallet === wallet
    ).length;
    if (existingSoloCount >= SIGNAL_THRESHOLDS.SOLO_MAX_PER_WALLET) continue;

    // Group this wallet's recent buys by market
    const walletMarkets = new Map();
    for (const trade of trades) {
      if (trade.side !== 'BUY') continue;
      const cid = trade.conditionId;
      if (!cid) continue;
      if (seenMarkets.has(cid)) continue; // Already covered by consensus/cluster

      if (!walletMarkets.has(cid)) {
        walletMarkets.set(cid, { trades: [], meta: trade });
      }
      walletMarkets.get(cid).trades.push(trade);
    }

    for (const [cid, data] of walletMarkets) {
      // Category gate — whitelist + keyword exclusions (see convergence).
      if (!isWhitelistedCategory(data.meta.title)) continue;

      const buySize = data.trades.reduce((s, t) => s + (t.size * t.price), 0);
      if (buySize < SIGNAL_THRESHOLDS.SOLO_MIN_BUY_SIZE) continue;

      // EV filter — wallet's avg fill price floor + implied-ROI ceiling.
      const soloAvgPrice = data.trades.reduce((s, t) => s + t.price, 0) / data.trades.length;
      if (SIGNAL_THRESHOLDS.MIN_ENTRY_PRICE > 0 && soloAvgPrice < SIGNAL_THRESHOLDS.MIN_ENTRY_PRICE) continue;
      if (SIGNAL_THRESHOLDS.MIN_WALLET_ROI > 0 &&
          impliedMaxROI(soloAvgPrice) < SIGNAL_THRESHOLDS.MIN_WALLET_ROI) continue;

      const signalId = `sig_solo_${wallet.slice(0, 10)}_${cid.slice(0, 10)}`;
      if (active[signalId]) {
        // Update existing solo signal
        const signal = active[signalId];
        signal.lastUpdatedAt = now;
        signal.lastUpdatedScan = scanIndex;
        signal.lastTradeTs = Math.max(...data.trades.map(t => t.timestamp));
        signal.totalBuySize = +buySize.toFixed(2);

        const tokenId = data.meta.asset || signal.tokenId;
        const mi = tokenId ? marketLookup.get(tokenId) : null;
        if (mi && mi.currentPrice > 0) {
          signal.currentMarketPrice = +(mi.currentPrice || 0).toFixed(4);
        }

        updated++;
      } else {
        // Check market not resolved
        const tokenId = data.meta.asset || '';
        const mi = tokenId ? marketLookup.get(tokenId) : null;
        if (mi && mi.marketClosed === true) continue;

        // Open solo signal
        const avgPrice = +soloAvgPrice.toFixed(4);
        const currentPrice = mi ? +(mi.currentPrice || 0).toFixed(4) : 0;

        // Require a valid live price — without it we can't track return
        // and the dashboard shows "-".
        if (!(currentPrice > 0)) continue;

        // EV filter on live market price — what a follower would actually pay.
        // Reject if implied max ROI on the signal side is below MIN_OPEN_ROI.
        if (SIGNAL_THRESHOLDS.MIN_OPEN_ROI > 0 &&
            impliedMaxROI(currentPrice) < SIGNAL_THRESHOLDS.MIN_OPEN_ROI) continue;

        // Stale-follower gate (see convergence path for rationale).
        if (SIGNAL_THRESHOLDS.STALE_FOLLOWER_MAX_PREMIUM > 0
            && avgPrice > 0
            && currentPrice > avgPrice * (1 + SIGNAL_THRESHOLDS.STALE_FOLLOWER_MAX_PREMIUM)) {
          continue;
        }

        const confidence = computeSoloConfidence(walletInfo, buySize, avgPrice);

        active[signalId] = {
          signalId,
          signalType: 'solo',
          conditionId: cid,
          tokenId,
          marketTitle: data.meta.title || '',
          slug: data.meta.slug || '',
          eventSlug: data.meta.eventSlug || '',
          groupKey: cid,

          direction: data.meta.outcome || 'Unknown',
          outcomeIndex: data.meta.outcomeIndex,
          soloWallet: wallet,

          openedAt: now,
          openedScan: scanIndex,
          lastUpdatedAt: now,
          lastUpdatedScan: scanIndex,
          lastTradeTs: Math.max(...data.trades.map(t => t.timestamp)),
          scansActive: 1,

          walletCount: 1,
          avgScore: walletInfo.score,
          totalBuySize: +buySize.toFixed(2),
          avgEntryPrice: +avgPrice.toFixed(4),

          openMarketPrice: currentPrice,
          currentMarketPrice: currentPrice,

          confidence: +confidence.toFixed(1),
          tier: getSignalTier(confidence),

          peakWallets: 1,
          peakConfidence: confidence,

          status: 'active',
          outcome: null,
          closedAt: null,
          closedScan: null,
          closeReason: null,

          currentWallets: [{
            address: wallet,
            score: walletInfo.score,
            buySize: +buySize.toFixed(2),
            avgPrice: +avgPrice.toFixed(4),
            tradeCount: data.trades.length,
          }],
        };

        opened++;
      }
    }
  }

  // --- Phase 2: Exit detection — wallets selling positions backing active signals ---
  for (const [signalId, signal] of Object.entries(active)) {
    signal.scansActive = (signal.scansActive || 0) + 1;

    // Refresh live market price from cache (even if not in convergence candidates)
    const refreshTokenId = signal.tokenId;
    const refreshMi = refreshTokenId ? marketLookup.get(refreshTokenId) : null;
    if (refreshMi && refreshMi.currentPrice > 0) {
      signal.currentMarketPrice = +(refreshMi.currentPrice).toFixed(4);
    }

    // Check for sells from backing wallets (needed for exit ratio AND redeem detection later)
    let walletsExited = 0;
    const backingWallets = signal.currentWallets || [];

    for (const w of backingWallets) {
      const walletTrades = recentTrades.get(w.address);
      if (!walletTrades) continue;
      const sells = walletTrades.filter(t =>
        t.conditionId === signal.conditionId && t.side === 'SELL'
      );
      if (sells.length > 0) walletsExited++;
    }

    const exitRatio = backingWallets.length > 0 ? walletsExited / backingWallets.length : 0;
    signal.exitRatio = +exitRatio.toFixed(2);
    signal.walletsExited = walletsExited;

    // A signal has ONLY two terminal states: win or loss.
    // It stays Active while the market is open — regardless of wallet exits,
    // staleness, or age. The only close path is: market has actually resolved
    // on Gamma AND winningOutcome is known → determine direction match.
    const tokenId = signal.tokenId;
    const mi = tokenId ? marketLookup.get(tokenId) : null;

    // Track informational flags on the active signal (for dashboard display only —
    // these do NOT close the signal).
    const lifetimeHours = (nowTs - new Date(signal.openedAt).getTime() / 1000) / 3600;
    signal.lifetimeHours = +lifetimeHours.toFixed(1);
    signal.backersExited = exitRatio > 0.5 && backingWallets.length >= 2;

    // Market resolution: the ONLY way a signal becomes terminal.
    // Requires BOTH: Gamma says the market is closed AND winningOutcome is populated.
    // If marketClosed is true but winningOutcome isn't available yet, we wait —
    // next scan will pick it up.
    const gammaClosed = mi && mi.marketClosed === true;
    const hasWinner = mi && mi.winningOutcome && mi.winningOutcome.length > 0;

    if (gammaClosed && hasWinner) {
      const won = matchesWinningOutcome(signal.direction, signal.direction, mi.winningOutcome);
      const outcome = won ? 'win' : 'loss';
      closeSignal(active, history, signalId, 'resolved', scanIndex, now, outcome);
      const lastEntry = history[history.length - 1];
      if (lastEntry && lastEntry.signalId === signalId) {
        const openPrice = signal.openMarketPrice || signal.avgEntryPrice || 0;
        if (outcome === 'win' && openPrice > 0) {
          lastEntry.signalReturn = +((1 / openPrice - 1) * 100).toFixed(2);
        } else if (outcome === 'loss') {
          lastEntry.signalReturn = -100;
        }
        lastEntry.resolvedBy = 'gamma';
        lastEntry.openMarketPrice = openPrice;
        lastEntry.winningOutcome = mi.winningOutcome;
      }
      closed++;
      continue;
    }
  }

  // --- Phase 2.5: Legacy history repair (safety net for pre-v3 entries) ---
  // In the new model, Phase 2 NEVER writes a null-outcome entry to history —
  // signals only close when Gamma has both marketClosed=true and winningOutcome.
  // This phase exists purely to clean up legacy history entries from before the
  // model simplification. It does three things:
  //   (a) If signalId is already active → drop the stale history duplicate
  //   (b) If market is still open → move it back to active (it was wrongly closed)
  //   (c) If market has resolved and Gamma has winningOutcome → backfill WIN/LOSS
  let repaired = 0;
  let restored = 0;
  let dedupedFromActive = 0;
  for (let i = history.length - 1; i >= 0; i--) {
    const h = history[i];
    if (h.outcome === 'win' || h.outcome === 'loss') continue;
    // Voided signals are terminal — don't restore or backfill them.
    if (h.outcome === 'void' || h.status === 'voided') continue;
    if (!h.conditionId && !h.tokenId) continue;

    if (h.signalId && active[h.signalId]) {
      history.splice(i, 1);
      dedupedFromActive++;
      continue;
    }

    const hmi = h.tokenId ? marketLookup.get(h.tokenId) : null;
    if (!hmi) continue;

    const marketStillOpen = hmi.marketClosed !== true;
    if (marketStillOpen) {
      const sid = h.signalId;
      if (sid && !active[sid]) {
        h.status = 'active';
        h.outcome = null;
        h.closedAt = null;
        h.closedScan = null;
        delete h.closeReason;
        delete h.closedReason;
        active[sid] = h;
        history.splice(i, 1);
        restored++;
      }
      continue;
    }

    // Market closed — backfill only if Gamma has winningOutcome
    if (hmi.winningOutcome) {
      const won = matchesWinningOutcome(h.direction, h.direction, hmi.winningOutcome);
      h.outcome = won ? 'win' : 'loss';
      h.resolvedBy = 'gamma_repair';
      h.closeReason = 'resolved';
      h.winningOutcome = hmi.winningOutcome;
      const op = h.openMarketPrice || h.avgEntryPrice || 0;
      if (h.outcome === 'win' && op > 0) {
        h.signalReturn = +((1 / op - 1) * 100).toFixed(2);
      } else if (h.outcome === 'loss') {
        h.signalReturn = -100;
      }
      repaired++;
    }
  }
  if (repaired > 0 || restored > 0 || dedupedFromActive > 0) {
    console.log(`  History repair: ${repaired} backfilled with WIN/LOSS, ${restored} restored to active, ${dedupedFromActive} duplicates of active removed`);
  }

  // --- Phase 3: Aggregate stats ---
  const activeSignals = Object.values(active);
  const allHistory = history;
  const wins = allHistory.filter(s => s.outcome === 'win').length;
  const losses = allHistory.filter(s => s.outcome === 'loss').length;
  const totalResolved = wins + losses;

  const stats = {
    activeCount: activeSignals.length,
    historyCount: allHistory.length,
    totalResolved,
    winRate: totalResolved > 0 ? +(wins / totalResolved * 100).toFixed(1) : 0,
    wins,
    losses,
    lastScan: scanIndex,
    lastUpdated: now,
    opened,
    updated,
    closed,
  };

  return { active, history, stats };
}

// ============================================================================
// Confidence Scoring
// ============================================================================

// Score-scale constant — the practical max of computeWalletScore on the
// live pool. Elite wallets land around 30-45; top 1% push toward 50.
// If the score formula in dataApi.js ever rescales, update here too.
const MAX_PRACTICAL_SCORE = 45;

function computeConvergenceConfidence(candidate, signalType) {
  // Wallet count factor — bumped 30→40 pts. Sizing-sim backtest showed
  // walletCount alone is strongly predictive (walletCount≥8 = +15.4%,
  // walletCount≥6 = +8.3%). This is the single most reliable confidence
  // ingredient so it earns the biggest share.
  const minWallets = signalType === 'consensus'
    ? SIGNAL_THRESHOLDS.CONSENSUS_MIN_WALLETS
    : SIGNAL_THRESHOLDS.CLUSTER_MIN_WALLETS;
  const walletFactor = Math.min(1, candidate.walletCount / (minWallets * 2)) * 40;

  // Score factor — CUT 25→10 pts. The 25-pt share pushed average-score
  // wallets into elite-tier territory (confidence ≥75), which the
  // sizing-simulator proved is actively anti-predictive: tier=elite
  // weighted return was -14.7% across 364 signals; confidence>80 binary
  // gate was -11.3%. Attribution-weighted scoring will eventually make
  // avgScore reliable again; until then, cap its influence.
  const scoreFactor = Math.min(1, (candidate.avgScore || 0) / MAX_PRACTICAL_SCORE) * 10;

  // Size factor (20 pts) — total $ committed (log scale)
  const sizeFactor = Math.min(1, Math.log10(1 + candidate.totalBuySize) / 4) * 20;

  // Timing factor (15 pts) — tighter convergence = stronger signal
  // All wallets buying within 2 hours = 15pts. Spread over 48 hours = 5pts.
  const spanHours = candidate.convergenceSpanHours || 48;
  const timingFactor = Math.max(0, 1 - spanHours / 72) * 15;

  // Price factor (10 pts) — better EV at lower prices, but entries
  // 30-60¢ showed -5.9% avg return (worst entry band). Keep price-weight
  // small; let category gate do the heavy lifting on EV selection.
  const price = candidate.avgEntryPrice || 0.5;
  const priceFactor = price > 0 && price < 1
    ? (1 - price) * 10  // 10¢ = 9pts, 50¢ = 5pts, 90¢ = 1pt
    : 5;

  // New max = 40 + 10 + 20 + 15 + 10 = 95 (elite threshold=95 → effectively rare)
  return Math.min(100, walletFactor + scoreFactor + sizeFactor + timingFactor + priceFactor);
}

function computeSoloConfidence(walletInfo, buySize, avgPrice) {
  // Wallet quality — CUT 40→20 pts. Same reason as convergence: raw score
  // is anti-predictive in the 25-35 band where most solos originate. Halve
  // its influence until attribution re-calibrates the pool score.
  const qualityFactor = Math.min(1, (walletInfo.score || 0) / MAX_PRACTICAL_SCORE) * 20;

  // Position size (30 pts, log scale) — unchanged
  const sizeFactor = Math.min(1, Math.log10(1 + buySize) / 4) * 30;

  // Price factor (15 pts) — unchanged
  const price = avgPrice || 0.5;
  const priceFactor = price > 0 && price < 1 ? (1 - price) * 15 : 7.5;

  // Decided-ROI bonus — CUT 15→10 pts for the same reason (decidedROI
  // measures trading skill, not signal-emission skill). The remaining
  // 10pts preserves some information without dominating.
  const roi = walletInfo.stats?.decidedROI;
  let edgeFactor;
  if (typeof roi === 'number') {
    edgeFactor = Math.min(1, Math.max(0, roi) / 0.30) * 10;
  } else {
    const wr = walletInfo.stats?.recentWinRate || walletInfo.stats?.winRate || 0;
    edgeFactor = Math.max(0, (wr - 0.5) * 2) * 10;
  }

  // Attribution bonus (15 pts) — wallets with proven positive signal
  // history get explicit confidence credit. Replaces some of what we cut
  // from qualityFactor + edgeFactor above. Neutral (0 pts) when the wallet
  // has no attribution record yet.
  const am = walletInfo.stats?.attributionMultiplier ?? 1.0;
  // attributionMultiplier ∈ [0.2, 1.5]; 1.0 = neutral, map to [-15, +15].
  const attributionFactor = Math.max(-15, Math.min(15, (am - 1.0) * 30));

  return Math.min(100, Math.max(0, qualityFactor + sizeFactor + priceFactor + edgeFactor + attributionFactor));
}

// Confidence → signal tier. Confidence stays on a 0–100 scale regardless
// of wallet-score rescales (the formulas above are the translation layer).
//
// Tier thresholds raised 2026-04-23 after sizing-sim showed tier=elite
// was -14.7% weighted return (the worst cohort by a wide margin).
// Raising elite 75→88 effectively empties the tier with the rebalanced
// confidence formula (max ≈ 95 but rarely hit without all 5 factors
// maxed). New distribution is expected to be ≈70% starter / 25% pro /
// ≤5% elite — and elite now requires an actual 4/5-factor agreement,
// not just a high avgScore inflating the raw confidence.
function getSignalTier(confidence) {
  if (confidence >= 88) return 'elite';
  if (confidence >= 55) return 'pro';
  return 'starter';
}

// ============================================================================
// Signal close helper (mirrors old system for compatibility)
// ============================================================================

function closeSignal(active, history, signalId, reason, scanIndex, timestamp, outcome = null) {
  const signal = active[signalId];
  if (!signal) return;

  signal.status = 'closed';
  signal.closedAt = timestamp;
  signal.closedScan = scanIndex;
  signal.closeReason = reason;
  signal.outcome = outcome;
  signal.duration = scanIndex - (signal.openedScan || scanIndex);

  // Strip wallet snapshot to save space
  delete signal.currentWallets;

  // Prevent duplicate history
  const isDuplicate = history.some(h =>
    h.conditionId === signal.conditionId && h.closeReason === reason && h.outcome === outcome
  );
  if (!isDuplicate) {
    history.push(signal);
  }
  delete active[signalId];

  // History is retained in full — no cap. Every resolved signal is preserved
  // for downstream WR / cohort / return analysis across the engine's lifetime.
}

// ============================================================================
// Exports
// ============================================================================

export {
  SIGNAL_THRESHOLDS,
  detectConvergence,
  processSignals,
  getSignalTier,
  closeSignal,
};
