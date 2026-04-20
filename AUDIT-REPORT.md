# Polymarket Signal Engine — Sense Check & Logic Audit

**Date:** 1 April 2026
**Scope:** Full system audit of `scanner/lib.js`, `scanner/scan.js`, `frontend/app.js`
**Auditor:** Claude (requested by Charlie)

---

## 1. Core Premise Validation

**Thesis:** "Wallets that have historically made profitable trades on Polymarket will continue to make profitable trades, and when multiple such wallets converge on the same market/direction, that's a tradeable signal."

### What's Sound

The general intuition has merit. Prediction markets do have information asymmetry — some participants (domain experts, insiders, sharp bettors) consistently outperform. Identifying these wallets and watching where they cluster is a defensible signal-seeking strategy. The multi-wallet consensus requirement adds a layer of noise reduction: one wallet could be lucky, twelve wallets converging is harder to dismiss.

### What's Questionable

**The persistence assumption is unproven.** The thesis assumes past performance predicts future performance. In prediction markets specifically, this is less reliable than it sounds. A wallet that crushed it on 2024 US elections might have zero edge on 2026 NBA outcomes. The system treats a wallet's composite score as a single number that applies to all future markets, regardless of category. There's no domain-specific tracking — a wallet with a 90% win rate on crypto markets contributes the same signal weight to a sports market as one that actually knows sports.

**Selection bias is baked in.** The system only discovers wallets that have already been profitable ($1k+ realized PnL, 50%+ WR, 20+ resolved). You cannot observe wallets that *will* be profitable but aren't yet. The system is structurally backward-looking and can only find wallets whose alpha may already be priced in or exhausted.

**The consensus lag is the fatal question.** Because positions have no timestamps from the subgraph, you're observing positions that may have been opened days, weeks, or months ago. By the time 12+ wallets hold positions in a market, the market price almost certainly already reflects their collective information. More on this in Section 6.

### Verdict: Premise is *plausible but untested*. The biggest risk isn't that the logic is wrong, it's that the signal arrives too late to be tradeable.

---

## 2. Data Integrity Check

### 2.1 End-to-End Pipeline Flow

The pipeline flows: **Subgraph fetch → wallet aggregation → scoring → Gamma resolution → consensus computation → signal generation → paper trading**. At each stage I checked for data loss or corruption:

**Position fetch (scan.js lines 183–278):** Cursor-based pagination with `id_gt` works correctly. Positions are deduplicated by `uid` within each wallet — good. The inline processing approach (no giant array) is memory-safe.

**Wallet refresh (lib.js lines 244–394):** Re-queries each tracked wallet's positions via ID range filter (`id_gte: "{address}-", id_lt: "{address}~"`). This is clever and correct for the Polymarket position ID format. PnL changes and closures are detected properly by comparing previous and current values.

**Scoring (lib.js lines 578–627, scan.js lines 287–383):** `analyzePositions()` feeds `computeScore()`. The flow is clean. One issue: `passesFilters` on line 356 of scan.js uses `(stats.realizedPnl || stats.totalPnl) >= MIN_PNL` — the `||` fallback to `totalPnl` means a wallet with $0 realizedPnl but large unrealizedPnl could pass the filter. This contradicts the comment on line 345 ("Filter on REALIZED PnL only"). **Bug: should be `stats.realizedPnl >= MIN_PNL`** without fallback.

**Signal generation (lib.js lines 1487–2149):** Consensus → signal mapping is correct. groupKey is used consistently for deduplication. The three signal types (consensus, cluster, solo) have clearly separated logic paths.

### 2.2 No-Timestamp Problem

This is the most significant data limitation in the entire system. The Goldsky subgraph provides no timestamp fields on positions. The system works around this with:

- `firstSeenTimestamp`: Stamped when the scanner first encounters a position (scan.js line 227)
- `discoveredScan`: Which scan number first found this position
- `resolvedTimestamp`: Stamped when the scanner observes amount going from >0.01 to ≤0.01

**Consequences and edge cases:**

1. **Stale discovery:** If the scanner starts for the first time or catches up from a long gap, it stamps all discovered positions with the current time. Positions that were opened months ago look "new". The system tries to handle this (scan.js lines 255–259: positions discovered already-closed get `resolvedTimestamp = null`), but this means many historical positions have no usable timestamp at all, degrading activity metrics.

2. **Activity detection is scan-relative, not time-relative.** `positionsPerWeek` divides discovered positions by weeks tracked (lib.js line 526). But "weeks tracked" depends on when the scanner first saw the wallet. Two identical wallets scanned at different times get different activity scores. This introduces arbitrary variance.

3. **Position closure timing is approximate.** With scans every 6 hours, a position that closed at hour 1 looks the same as one that closed at hour 5 — both get stamped at the next scan. For fast-moving markets (e.g., live sports), this is a 6-hour blind spot.

4. **The `isNewThisScan` flag is cleared after each scan** (scan.js line 599), which is correct. But `pnlChangedThisScan` is also cleared, meaning if a position's PnL changes incrementally across scans, only the latest change is visible.

### 2.3 Position Closure Detection

Using `amount <= 0.01` as the threshold for "closed" position is reasonable — USDC dust from rounding is expected. However:

**Partial exits are invisible.** If a wallet reduces a 1000-share position to 200 shares, the system sees it as "still open." Only full closure triggers resolution. This means:
- A wallet taking profit on 80% of a position and holding 20% registers zero information until the remaining 20% is also closed.
- A wallet hedging by buying the opposite outcome in a new position would appear as holding *two* open positions, potentially in different directions. The consensus engine would see this wallet as voting for both Yes AND No on the same market, diluting directional signal.

**The `realizedPnl` field reflects on-chain settlement.** If a wallet sells shares on the order book at a profit but the market hasn't resolved, `realizedPnl` still updates (the subgraph tracks this). This is actually fine for scoring purposes. But it means `pnl > 0` on a closed position doesn't necessarily mean "the market resolved in their favour" — it could mean "they sold at a profit before resolution." The system treats both the same way, which conflates smart timing with correct prediction.

**Verdict: Pipeline is mechanically sound but has material blind spots around position timing, partial exits, and the distinction between market resolution and early profit-taking.**

---

## 3. Scoring Formula Audit

### The Formula (lib.js lines 578–627)

| Factor | Weight | Formula |
|---|---|---|
| Win Rate | 25 pts | `wr × sampleFactor × 25` (sampleFactor = `min(1, √resolved / 10)`) |
| Markets | 15 pts | `min(1, estimatedMarkets / 50) × 15` |
| Efficiency | 15 pts | `min(1, log10(1 + eff×100) / 2) × 15` |
| Edge | 10 pts | `min(1, log2(1 + max(0, edge-0.5)) / 3) × 10` |
| Sample Size | 15 pts | `min(1, resolved / 200) × 15` |
| Activity | 20 pts | `min(1, log10(1 + ppw) / 2) × 12 + min(1, tradingDays / 14) × 8` |

Then: `rawScore × recencyMultiplier`

### What's Sound

The use of logarithmic scales for efficiency and edge prevents outliers from dominating. The `sampleFactor` on win rate is a good idea — it dampens win rate for wallets with few resolved positions, preventing a 10-for-10 wallet from outscoring a 150-for-200 one.

The recency multiplier (0.5x after 90 days, 0.75x after 30 days, etc.) is sensible for ensuring the pool stays fresh.

### What's Questionable

**Survivorship bias is real and structural.** The system only scores wallets that already pass the $1k PnL / 20 resolved / 50% WR filters. The scoring formula then ranks *within* this survivor pool. A wallet that had a spectacular run and then started losing would stay in the pool (on probation for 3 scans ≈ 18 hours). This is actually too short — a once-great wallet doesn't become useless in 18 hours. The probation system should probably be measured in days or weeks, not scans.

**The formula identifies "wallets that were profitable" more than "wallets that will be profitable."** There's no forward-looking component. All five factors are backward-looking: past win rate, past markets, past efficiency, past edge, past sample size. Activity (20pts) is the closest thing to a forward-looking signal, but it measures *quantity* of trading, not *quality of recent* trading. A wallet could be extremely active and losing money recently while still having a high activity score.

**Activity at 20pts creates a perverse incentive.** The activity component rewards:
- `log10(1 + positionsPerWeek) / 2 × 12` — positions per week (12pts)
- `tradingDays / 14 × 8` — trading days (8pts)

A wallet trading 100 positions/week gets `log10(101)/2 × 12 ≈ 12pts` on the first sub-component, effectively maxed out. But a wallet with 10 positions/week gets `log10(11)/2 × 12 ≈ 6.3pts`. The difference (5.7pts out of 100) is material. This means a wallet that trades frequently but mediocrely can outscore a more selective but more profitable wallet. At 20% of the total score, activity is arguably overweighted for a system whose purpose is to find *quality* traders, not *active* traders.

**estimatedMarkets = ceil(uniqueTokens / 2) is a rough heuristic.** Polymarket markets have Yes/No token pairs, so dividing by 2 makes sense. But multi-outcome markets (e.g., "Who wins the NBA Finals?" with 30 teams) would have 30 tokens per market, and dividing by 2 would count it as 15 markets. This inflates the Markets score for wallets that trade multi-outcome markets. Given that sports markets dominate Polymarket volume, this could be a material distortion.

### Verdict: Scoring formula is *reasonable but backward-looking by design*. Activity weight should be reconsidered. The `estimatedMarkets` heuristic breaks for multi-outcome markets.

---

## 4. Signal Resolution Logic

### 4.1 The False Win Bug Fix (lib.js ~line 1964)

The recently fixed bug was: when all wallet PnLs were 0 (unredeemed shares), `walletsWon === 0` and `walletsLost === 0`, then `walletsWon >= walletsLost` (0 >= 0) evaluated to `true`, falsely marking signals as wins. The fix at line 2023 now skips resolution if both are 0. **This fix is correct.**

### 4.2 Gamma Resolution Path (lines 1978–1999)

The direction matching logic has several potential issues:

**Partial match is dangerously loose.** Line 1990–1993:
```js
const partialMatch = winner.length > 3 && (
  signalDir.includes(winner) || winner.includes(signalDir) ||
  signalTopOutcome.includes(winner) || winner.includes(signalTopOutcome)
);
```

Edge cases this creates:
- **"Yes" vs "Yesenia"** — if a market has an outcome named "Yesenia" and the signal direction is "yes", `"yesenia".includes("yes")` = true. False match.
- **"No" vs "Novak"** — same problem. However, the `winner.length > 3` guard protects against short strings like "No". But "Yes" is only length 3, so this specific case is actually safe since the guard would fail.
- **Multi-outcome ambiguity:** In a market "Who wins the election?" with outcomes "Trump", "DeSantis", "Haley" — if the winning outcome is "Trump" and the signal direction is "trump", the direct match works fine. But if outcomes had "Trump Jr." and "Trump", `signalDir.includes(winner)` where signalDir is "trump jr." and winner is "trump" would match, which could be wrong if the two are different outcomes.
- **Case sensitivity is handled** — both sides are `.toLowerCase().trim()`. Good.

**Multi-outcome markets more broadly:** The system assumes a signal has a single `direction` and a single `topOutcome`. For a multi-outcome market (30 NBA teams), the consensus direction might be "lakers" based on the highest score-weighted outcome. If the Lakers actually win, the direct match works. But if the signal's `topOutcome` was "Lakers" with 40% consensus and the winning outcome is "Celtics" with 0% consensus, the signal correctly resolves as a loss. This seems fine for most cases.

**The real risk is markets where outcome names overlap** (e.g., "Over 2.5" and "Over 3.5" in the same market group). If the winning outcome is "Over 2.5" and the signal direction is "over 3.5", the partial match `"over 2.5".includes("over 3.5")` fails, and `"over 3.5".includes("over 2.5")` also fails. So this specific case is actually safe. The partial match is more dangerous for substrings like "warriors" matching "golden state warriors" — but that would actually be a *correct* match.

### 4.3 Wallet PnL Fallback (lines 2010–2031)

When Gamma doesn't have resolution data, the system looks at wallet PnLs. The guard at line 2023 (skip if all zero) is correct. But there's a subtlety:

**Majority-vote outcome.** Line 2029: `if (walletsWon >= walletsLost) signalOutcome = 'win'`. This uses the *wallet snapshot* from the signal (`signal.currentWallets`), which was last updated when the signal was last seen in consensus. If wallets updated their positions after the signal was last updated (e.g., the signal went stale and wasn't being refreshed), the PnLs in the snapshot could be stale.

**"Wait for Gamma data" as fallback:** The system skips resolution when PnLs are all zero (line 2023). This is the right call in most cases. The only risk: if Gamma never provides resolution data AND wallets never redeem shares, the signal stays open indefinitely until it goes stale and closes with "stale" outcome (-2% friction). For paper trading, this is a slow bleed rather than a catastrophic error.

### Verdict: Resolution logic is *significantly improved* after the false win fix. The partial match is loose but unlikely to cause many false positives given the `length > 3` guard. The wallet PnL fallback is correctly guarded but relies on potentially stale snapshot data.

---

## 5. Paper Trading Validity

### 5.1 Position Sizing and PnL Model

**Fixed $100 trades** — simplistic but adequate for signal validation. Not meant to model real portfolio management.

**Win PnL formula (lib.js line 2462):**
```js
tradePnl = trade.tradeSize * (1 / entryPrice - 1)
```

This is correct for Polymarket's binary payout mechanic. Buy at $0.65, market resolves Yes, you get $1.00 per share. PnL = $100 × (1/0.65 - 1) = $53.85.

**Loss PnL (line 2464):** `tradePnl = -trade.tradeSize` — lose the full $100. This is correct: if the market resolves against you, your shares are worth $0.

**The asymmetry is real and correct.** On Polymarket, buying at $0.65 means you risk $65 (per $100 notional at $0.65/share, you buy 153.8 shares × $0.65 = $100, shares go to $0 = -$100). Wins pay less than losses cost because the entry price determines the payout ratio. This matches actual mechanics.

### 5.2 What's Missing

**Slippage is not modelled.** The paper trader uses `avgEntryPrice` from the wallets' actual positions. But a real trader entering *after* seeing the signal would face a different (likely worse) price. If 12 wallets have already bought, the price has moved. The paper trader assumes you can enter at the same price the signal wallets did, which is unrealistic.

**Timing lag is not modelled.** With 6-hour scan intervals, a signal detected at scan N might represent positions opened anywhere from 0 to 6+ hours ago. A real trader would need to see the signal, evaluate it, and execute — adding further lag. The paper trader opens at scan time with the wallet's entry price, not the market's current price.

**Liquidity constraints are not modelled.** A $100 paper trade is trivial, but scaling to real money ($1k, $10k) on thin markets would face significant market impact. The paper trader doesn't account for this.

**The `avgEntryPrice` fallback (line 2461) is concerning:**
```js
const entryPrice = closedSignal.avgEntryPrice > 0
  ? Math.max(0.05, Math.min(0.99, closedSignal.avgEntryPrice))
  : Math.max(0.3, Math.min(0.85, (closedSignal.consensusStrength || 0.6)));
```
If `avgEntryPrice` is missing, it falls back to `consensusStrength` as a price proxy. This is a completely different concept — consensus strength (how much wallets agree) has nothing to do with market price. Any signal that goes through this fallback path will have a fabricated entry price, producing meaningless PnL. You should log when this fallback triggers and quantify how often it happens.

### 5.3 Equity Curve Interpretation

The equity curve measures open trade value at *cost basis* (line 2525), not at current market value. This means the equity curve doesn't show unrealized gains or losses — it just shows "cash + amount deployed." This is conservative but understates volatility. A position bought at $0.50 now trading at $0.90 still shows as "$100 deployed" until it resolves.

### Verdict: Paper trading PnL mechanics are *correct for Polymarket's payout structure*. But the model is optimistic because it doesn't account for slippage, timing lag, or the price difference between signal wallets' entry and when a follower could realistically enter. The consensusStrength-as-price fallback should be flagged or removed.

---

## 6. Signal Quality Concerns

### 6.1 The Timestamp Gap is the System's Achilles' Heel

This is the single biggest issue in the entire system. Because positions have no timestamps:

**A wallet holding a position at $0.90 that was entered at $0.30 looks the same as one entered at $0.89.** The system can't distinguish between "early smart money" (entered cheap, huge edge) and "late followers" (entered near current price, minimal edge). The consensus engine counts both wallets equally. A signal showing "12 wallets agree on Yes" might mean "12 wallets bought Yes at $0.90" — where the edge is only 10¢ of upside vs 90¢ of downside. This is a fundamentally bad trade.

**The deferred EV filter (Handoff section 11.4) would partially address this.** Using the Gamma API's `currentPrice` to skip signals where price > $0.85 is a good start. But it still doesn't capture the difference between wallets that entered early vs late. Two wallets both holding at a $0.85 market: one entered at $0.30 (has conviction, huge unrealized gain) and one entered at $0.83 (just got in, minimal conviction). They contribute identically to the signal.

**The `avgEntryPrice` field from the subgraph partially helps** — it's used in paper trading PnL calculations but NOT in signal generation or confidence scoring. Consider incorporating average entry price into signal quality: a consensus of wallets with average entry at $0.40 in a market now at $0.75 is far more informative than one with entries at $0.73.

### 6.2 Solo Signal Reliability

Solo signals come from a single wallet, no matter how impressive their track record. The thresholds are high (score ≥ 75, WR ≥ 85%, 100+ resolved, $50k+ PnL, $500+ position), which filters for truly elite wallets.

**But single-wallet signals have no crowd validation.** A wallet with 90% WR over 200 resolved markets is impressive, but:
- That 90% WR might include many "easy" markets (heavy favourites that everyone got right)
- The current position could be outside their domain of expertise
- Their position size ($500+) might be relatively small for them (low-conviction bet)

Solo signals should be treated with materially more caution than consensus signals. The confidence formula does weight them lower (computeSoloConfidence caps lower in practice), but they still enter the paper trading system at the same $100 size as consensus signals. Consider reducing paper trade sizing for solo signals, or track their performance separately and be prepared to drop the signal type entirely if results are poor.

### 6.3 Signal Freshness vs Position Age

A signal opened on scan #52 might reflect positions that were opened on scan #10. The signal was "new" at scan #52 because that's when enough wallets crossed the threshold, but the underlying positions are old. This creates a gap between "signal detected" and "trade opportunity":

- Smart wallets entered early (cheap prices)
- Price has since moved significantly
- Signal is generated when enough wallets are visible
- A follower entering now faces a much worse price

This is essentially a **front-running problem in reverse**: the signal confirms what the market has likely already priced in.

### 6.4 Market Type Blindness

The system treats all markets identically: crypto, politics, sports, entertainment. But these have fundamentally different characteristics. Sports markets are event-driven with hard resolution dates; crypto markets can be long-running with gradual price discovery; political markets have information landscapes that shift. A wallet's edge in one category doesn't transfer to another. Consider tracking per-category performance and potentially generating category-weighted scores.

### Verdict: Signal quality is *seriously undermined by the lack of position timestamps*. The system generates signals based on position accumulation without knowing when those positions were entered, which means signals may arrive after the trade opportunity has passed. Solo signals add volume but questionable value. The deferred EV filter and a potential entry-price-weighted confidence score would meaningfully improve things.

---

## 7. Summary of Findings

### Sound

- Pipeline mechanics: position fetching, deduplication, wallet lifecycle management, contamination detection
- The false win bug fix and the two resolution guards
- Paper trading payout mechanics (1/entryPrice - 1 for wins, -tradeSize for losses)
- Score-weighted consensus direction (better than raw headcount)
- Adaptive rate limiting on Gamma API calls
- Memory-safe inline processing of positions

### Questionable

- Activity weight at 20% may reward quantity over quality
- `estimatedMarkets = ceil(tokens/2)` breaks for multi-outcome markets
- `(stats.realizedPnl || stats.totalPnl)` filter should be `stats.realizedPnl` only (bug)
- Partial match logic for signal resolution could produce false positives on edge cases
- Wallet PnL snapshot used for fallback resolution may be stale
- Paper trader `consensusStrength` as price fallback produces fabricated PnL
- Solo signals are unvalidated and may not add value
- 3-scan probation period (~18 hours) is very short for wallet lifecycle management

### Potentially Broken

- **No position timestamps means signals have unknown latency.** You cannot determine if a signal represents a current opportunity or a stale accumulation. This is the single highest-risk issue.
- **Paper trading uses wallet entry prices, not follower-achievable prices.** This makes paper trading results optimistic by an unknown margin. If wallets entered at $0.40 and the market is now $0.75, the paper trader records the win at $0.40 entry, but a real follower would have entered at $0.75+.
- **No EV filter means signals fire on markets with terrible risk/reward.** A consensus signal on a market at $0.92 risks $92 to make $8. This is live and happening now (deferred per handoff 11.4).
- **No liquidity filter means signals fire on illiquid markets** where real execution would be impractical (deferred per handoff 11.5).

---

## 8. Recommended Priority Actions

1. **Push the unpushed commit** (handoff 11.1) — the false win fix must go live
2. **Implement the EV filter** (handoff 11.4) — skip signals where current price > 0.85
3. **Implement the liquidity filter** (handoff 11.5) — skip markets below a minimum volume/liquidity threshold
4. **Fix the realizedPnl filter bug** — change `(stats.realizedPnl || stats.totalPnl) >= MIN_PNL` to `stats.realizedPnl >= MIN_PNL`
5. **Add entry price to signal confidence** — weight signals higher when wallets entered at prices significantly below current market price
6. **Log the paper trading price fallback** — quantify how often `consensusStrength` is used as a price proxy and consider removing the fallback
7. **Extend probation period** — 3 scans is too short; consider 12+ scans (3+ days)
8. **Track solo signal performance separately** — be prepared to disable them if they underperform
9. **Fix `estimatedMarkets` for multi-outcome markets** — use `groupId` counts instead of `ceil(tokens/2)`
10. **After running a few post-fix scans, compare win/loss ratio to actual PnL** — if high hit rate still corresponds to negative PnL, the signals are systematically entering too late
