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
 * Categories currently allowed to emit signals. Mirror of
 * signals.js SIGNAL_THRESHOLDS.ALLOWED_CATEGORIES — kept duplicated
 * here to avoid circular imports. KEEP IN SYNC.
 *
 * Used by buildAttributionMap to filter out historical signals on
 * markets the current code would no longer emit on. Without this,
 * a wallet's attribution multiplier was inflated by signals on
 * categories we've since blocked (e.g. crypto-updown 5-min markets,
 * esports, NHL, golf). The 2026-04-30 audit found the #1 ranked
 * wallet (0x022c654f4b, score 67.5) had both its attribution wins
 * on Bitcoin Up/Down 5-MIN markets — exactly what
 * MIN_HOURS_TO_RESOLUTION=4 now blocks. The wallet was getting the
 * max 2.5× boost from a regime that's structurally closed off.
 */
const CURRENT_ALLOWED_CATEGORIES = new Set([
  'tennis', 'nba', 'mma', 'weather',
  'crypto-updown', 'crypto-other',
  'mlb', 'nfl', 'macro', 'ai-tech',
  'token-launch', 'news-event',
  'soccer',
]);

const MIN_HOURS_TO_RESOLUTION = 4;

function classifyMarketLocal(title) {
  const q = (title || '').toLowerCase();
  if (!q) return 'other';
  if (/dota|lol|league of legends|counter-strike|valorant|cs:?go|cs2|csgo|call of duty|apex|fortnite|pubg/.test(q)) return 'esports';
  if (/\blaunch a token|\btoken launch|\btge\b/.test(q)) return 'token-launch';
  if (/bitcoin|btc|ethereum|eth|solana|sol\b|doge|xrp|crypto|coin/.test(q)) {
    if (/reach|above|below|hit|close|\$|\sup\b|\sdown\b|end above|end below/.test(q)) return 'crypto-updown';
    return 'crypto-other';
  }
  if (/nhl|stanley cup| hockey /.test(q)) return 'nhl';
  if (/nba|lakers|celtics|warriors|knicks|heat|nba playoffs/.test(q)) return 'nba';
  if (/nfl|super bowl|touchdown|quarterback/.test(q)) return 'nfl';
  if (/mlb|baseball|world series/.test(q)) return 'mlb';
  if (/epl|premier league|champions league|la liga|bundesliga|serie a|manchester|arsenal|liverpool|chelsea/.test(q)) return 'soccer';
  if (/tennis|wimbledon|us open|french open|atp|wta/.test(q)) return 'tennis';
  if (/ufc|\bmma\b|fight night/.test(q)) return 'mma';
  if (/ pga |golf|masters tournament/.test(q)) return 'golf';
  if (/f1\b|formula 1|grand prix/.test(q)) return 'f1';
  if (/trump|biden|harris|election|senate|house race|republican|democrat|congressional/.test(q)) return 'politics';
  if (/ai\b|openai|anthropic|gpt|gemini|\bllm\b|tech|apple|google|microsoft|nvidia/.test(q)) return 'ai-tech';
  if (/weather|temperature|hurricane|tornado|snow|rainfall/.test(q)) return 'weather';
  if (/fed rate|fomc|inflation|cpi|ppi|\bgdp\b|recession|jobs report|unemployment|nonfarm/.test(q)) return 'macro';
  if (/ipo|earnings|revenue|guidance|\beps\b/.test(q)) return 'macro';
  if (/spacex|starship|nasa|rocket launch|announce|statement|\bsay\b|\bsays\b|tweet|post|comment/.test(q)) return 'news-event';
  if (/\bwill .+ by |\bwill .+ before |\bwill .+ on /.test(q)) return 'news-event';
  return 'other';
}

/**
 * Returns true if a historical signal would PASS today's emission
 * gates. Used by buildAttributionMap to credit only signals on
 * markets the current code would still emit on.
 *
 * Excludes:
 *   - Markets in categories not in CURRENT_ALLOWED_CATEGORIES
 *     (e.g. esports, golf, nhl, politics, f1)
 *   - Markets that resolved within MIN_HOURS_TO_RESOLUTION hours of
 *     opening (5-min BTC markets, intraday spreads, etc.)
 */
function signalCurrentlyEmittable(sig) {
  if (!sig) return false;
  // Category gate
  const category = classifyMarketLocal(sig.marketTitle);
  if (!CURRENT_ALLOWED_CATEGORIES.has(category)) return false;
  // Time-to-resolution gate. closedAt and openedAt may be ms or s; use Date.parse for ISO.
  const opened = sig.openedAt ? Date.parse(sig.openedAt) : null;
  const closed = sig.closedAt
    ? (typeof sig.closedAt === 'number'
      ? (sig.closedAt > 1e11 ? sig.closedAt : sig.closedAt * 1000)
      : Date.parse(sig.closedAt))
    : null;
  if (opened && closed && isFinite(opened) && isFinite(closed)) {
    const hours = (closed - opened) / (3600 * 1000);
    if (hours < MIN_HOURS_TO_RESOLUTION) return false;
  }
  return true;
}

/**
 * Build an attribution map from signal history.
 * @param {Array} signalsHistory - The array under signals.history
 * @param {object} opts
 * @param {boolean} opts.filterCurrentlyEmittable - When true (default),
 *   only counts historical signals that would pass today's category +
 *   resolution-time gates. Stops attribution credit from accumulating
 *   on markets we no longer emit on (e.g. wallet that won 2 BTC
 *   5-minute signals doesn't get 2.5× boost when those markets are
 *   structurally blocked today).
 * @returns {Map<string, { signals, wins, losses, wr, avgReturn }>}
 *          avgReturn is a DECIMAL fraction (e.g. 0.16 = +16% avg return)
 */
export function buildAttributionMap(signalsHistory, opts = {}) {
  const attr = new Map();
  if (!Array.isArray(signalsHistory)) return attr;
  const filterCurrentlyEmittable = opts.filterCurrentlyEmittable !== false;  // default true

  const resolved = signalsHistory.filter(s =>
    s && (s.outcome === 'win' || s.outcome === 'loss')
    && (!filterCurrentlyEmittable || signalCurrentlyEmittable(s))
  );
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
  // Min signals raised from 3 → 5 after the 2026-04-30 probe revealed
  // the #1-ranked wallet (0x022c654f4b, score 67.5) was getting the max
  // 2.5× boost from only TWO BTC-updown 5-min signals — markets we no
  // longer emit on. Two wins at +62% avg is too thin a sample to
  // justify capping out the multiplier; one streak from a now-blocked
  // category was distorting the leaderboard.
  //
  // 5-signal minimum gives genuinely emittable signals time to
  // accumulate before attribution dominates. Combined with the
  // CURRENT_ALLOWED_CATEGORIES filter in buildAttributionMap, this
  // ensures attribution credit only flows from markets the engine
  // would still emit on today.
  const minSignals = opts.minSignals ?? 5;
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
