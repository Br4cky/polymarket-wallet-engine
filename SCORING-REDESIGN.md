# Wallet Scoring Redesign

**Date:** 2026-04-16
**Status:** Proposed — awaiting review before implementation
**Evidence base:** 74 wallets deep-audited via position-centric ground-truth ledger (`out/bottom-25.v2.csv`, `out/sample-50.v2.csv`)

---

## 1. Executive summary

The current scoring system does not rank wallets by skill. Across 74 wallets sampled from the full 1000-wallet tracked pool:

- **Spearman(engineScore, decidedROI) = −0.152.** Higher rank slightly predicts *worse* truth.
- Top score band (90+) has median decidedROI of **1.8%**. Bottom band (<70) has **12.9%**. The ranking is **inverted** on truth.
- 5 confirmed mean-pickers (WR ≥95%, decROI <5%, capital ≥$50k) occupy prominent positions totalling **$6.76M of capital** — rank=6 alone is $3.7M at 0.2% decidedROI.
- Of 16 wallets with genuine edge (decROI ≥15% on ≥$10k decided capital), only 6 are actively trading. 5 are dead (30d+ since last trade). The pool is half-full of veterans who won't generate new signals.

The discovery gate mostly works — good wallets do enter the pool (rank=977 has 82.9% decROI on $13.6k decided capital). The **ranker** is what's broken, plus there's no meaningful eviction for dead wallets.

This document proposes a complete replacement of `computeWalletScore`, tightened discovery and eviction gates, and a two-tier tracking system so the signal engine only acts on high-confidence wallets.

---

## 2. What's broken, specifically

### 2.1 The score formula rewards mean-pickers

Current weights in `computeWalletScore` (from `scanner/dataApi.js:650`):

| component | pts | what it measures |
|---|---|---|
| recentWrScore | 30 | recent win rate |
| allTimeWrScore | 10 | lifetime win rate |
| pnlScore | 15 | log-scaled total PnL |
| consistencyScore | 15 | recent WR vs all-time WR divergence |
| activityScore | 15 | recency + frequency |
| edgeScore | 15 | roiEdgeRatio (avg win ROI / avg loss ROI) |

**40 points out of 100 are WR-based.** A mean-picker (buys $0.98 favourite, redeems $1.00) wins 100 positions for an absurd ROI of ~2% per trade, but WR hits 99%. That wallet rides directly to the top of the pool despite essentially zero edge.

The **only** component that would penalise them (`edgeScore`, via `roiEdgeRatio`) is worth 15 pts and gets diluted by everything else.

### 2.2 PnL rewards churn, not skill

`pnlScore` is log-scaled on `effectivePnl = max(analyzer, goldskyPnl)`. A wallet that trades $10M through the book for 0.2% ROI shows $20k PnL and gets high marks. A wallet that trades $50k for 40% ROI shows the same $20k and gets identical marks. **Capital deployed is invisible to the score, so high-turnover low-edge wallets look identical to low-turnover high-edge wallets.**

### 2.3 No minimum-capital gate on decided outcomes

The system gates on `resolvedMarkets ≥ 3` but not on decided *capital*. rank=585 in our sample had decidedROI=532.2% on **$98** of decided capital — literally 5 positions of $20 each. These tiny-sample outliers can game the ranking.

### 2.4 Eviction is too lenient

`MAX_INACTIVE_DAYS = 60`. In our sample, 18 wallets haven't traded in 30+ days and 5 of those haven't in 60+. They're still scored, still tracked, still consuming the pool's 1000 slots. The engine is paying for dead weight.

### 2.5 Consistency score penalises improvement

`consistencyScore` uses `|recentWR − allTimeWR|` — so a wallet that learns and improves (recent WR > all-time) gets **penalised** the same as one that declines. Skill-growth is invisible to the score and actively punished.

### 2.6 The measurement pipeline is partially blind

- `goldskyPnl` is only refreshed for 100 wallets per discovery cycle. With 1714 wallets, each wallet's PnL is only re-measured every ~17 scans — meaning most of what the scorer reads is days-old data.
- The scanner loads `analytics.json.gz.marketLookup` which doesn't exist, so until our recent fix it was hitting Gamma for every position — slow and rate-limited. (Already fixed; kept here for completeness.)

---

## 3. What we have to work with

Per-wallet stats already computed in `scan.js`:

- `winRate`, `recentWinRate`, `resolvedMarkets`, `recentResolved`, `wins`, `losses`
- `totalPnl`, `recentPnl`, `goldskyPnl`, `effectivePnl`, `avgPnlPerTrade`, `roiEdgeRatio`
- `totalTrades`, `recentTrades`, `uniqueMarkets`, `openPositions`
- `tradesPerDay`, `recentTradesPerDay`, `marketsPerDay`
- `tradingSpanDays`, `statsSpanDays`, `avgHoldTimeHours`
- `activeDays`, `activeWeeks`, `weeklyConsistency`
- `firstTradeTs`, `lastTradeTs`
- `unredeemedWins` (dollars), `worthlessLosses` (dollars)

What's **missing** from stats and has to be added:

- `decidedCapital` (dollars) — cost basis of all decided positions
- `decidedPnl` (dollars) — realized + unredeemed-won + worthless-lost, over decided positions only
- `decidedROI = decidedPnl / decidedCapital`
- `decidedPositions` count

These are all computable from the same Goldsky `userPosition` query the scanner already makes — we just need to iterate positions (as `wallet-ledger.cjs` does) instead of taking the aggregate `totalPnl`.

Per-trade timestamps remain **not available** in the Goldsky PnL subgraph. Time-partitioned ROI (last 30d vs all-time) would require either a different data source (raw CTF trade events) or activity-endpoint pagination. **Deferred for v1.**

---

## 4. Design principles

1. **Ground truth is decidedROI, full stop.** Everything else is supporting data. The score must correlate with decidedROI on new batches.
2. **Capital gates are non-negotiable.** A wallet with tiny decided capital has tiny sample size, so its decidedROI is noise — it should not rank above wallets with proven capital-weighted edge.
3. **Recency is a gate, not a reward.** Dead wallets contribute nothing to future signals. Fast-decay recency, not slow.
4. **WR alone is toxic.** Mean-picker patterns must be detectable and penalised.
5. **One score, multiple gates.** Single-number rankings hide too much. Tier assignment (A/B/C) provides a cleaner downstream interface than "is 67.9 good or bad?"
6. **Validate continuously.** Every scoring change must be A/B-tested against a ground-truth batch before deployment.

---

## 5. The new score formula

### 5.1 Inputs

Two new inputs (require Goldsky measurement change, see §9):

- `decidedROI ∈ [−1, ∞)` — decidedPnl / decidedCapital
- `decidedCapital ≥ 0` — dollars of cost basis on resolved markets

Existing inputs reused:

- `resolvedMarkets` — gate on sample size
- `lastTradeTs` — recency
- `recentTradesPerDay` — activity level
- `winRate`, `roiEdgeRatio` — supporting inputs, down-weighted

### 5.2 Formula

```
// Step 1: core truth component (0..50 pts)
// decidedROI capped at 30% to prevent small-sample outliers winning on noise
adjustedROI = clamp(decidedROI, 0, 0.30)
roiScore = adjustedROI / 0.30 * 50          // 0..50 pts

// Step 2: capital-weight the truth signal (0..1 multiplier)
// Hits 1.0 at $50k decided capital
// sqrt chosen over log so small wallets can still earn a meaningful share
capConfidence = min(1, sqrt(decidedCapital / 50000))

// Step 3: sample-size confidence (0..1 multiplier)
// Hits 1.0 at 25 resolved markets
sampleConfidence = min(1, resolvedMarkets / 25)

// Step 4: recency gate (0..1 multiplier)
// 0-7d = 1.0, then linear decay to 0 at 30d
daysSinceTrade = (now - lastTradeTs) / 86400
recencyMultiplier =
  daysSinceTrade <= 7  ? 1.0 :
  daysSinceTrade <= 30 ? (30 - daysSinceTrade) / 23 :
  0

// Step 5: mean-picker penalty (0..1 multiplier)
// If WR is absurdly high AND decidedROI is low, this is a mean-picker
meanPickerFlag = (winRate >= 0.95 && decidedROI < 0.05) ? 0.3 : 1.0

// Step 6: activity bonus (0..10 pts, additive)
// Rewards wallets that are actively producing signals
// Log-scaled so 5 trades/day ≈ 7pts, 20 trades/day ≈ 10pts
activityBonus = recentTradesPerDay > 0
  ? min(10, log10(1 + recentTradesPerDay * 4) * 10)
  : 0

// Step 7: compose
score = (roiScore * capConfidence * sampleConfidence * recencyMultiplier * meanPickerFlag) + activityBonus

// Range: 0..60 pts. Scale to 0..100 for readability.
scaledScore = score * 100 / 60
```

### 5.3 How this fixes the observed failures

| current failure | how new formula addresses it |
|---|---|
| rank=6 mean-picker at 0.2% decROI, $3.7M cap scored 95.5 | `meanPickerFlag=0.3` cuts roiScore to 30% of what it was; `roiScore` itself is ~0.3 pts (0.2%/30% × 50) → new score ≈ 0.3 × 0.3 × 1 × 1 × 1 + activityBonus ≈ **low single digits** |
| rank=977 real edge (82.9% decROI, $13.6k cap) scored 68.3 | `adjustedROI=0.30` → 50 pts; `capConfidence=sqrt(13600/50000)≈0.52`; `recencyMultiplier≈0.17` (19d cooling); score ≈ 50 × 0.52 × 1 × 0.17 = **~4.4 pts**, scaled = **~7**. Still low because it's cooling. If it were active, score would be ~43. |
| rank=6 also cooling → currently unpunished | `recencyMultiplier` forces all stale wallets down regardless of other stats |
| rank=585 noise wallet at 532% decROI on $98 cap | `adjustedROI=0.30` (capped), `capConfidence=sqrt(98/50000)≈0.044` → roiScore contribution ≈ 2.2 pts before other multipliers. Can't climb on tiny-sample noise. |

### 5.4 Expected distribution

Applying this to the 74-wallet sample (capital-weighted + recency-aware + mean-picker-demoted), I'd expect:

- **Top 10–15%** of pool: active wallets with decROI ≥10% on ≥$25k decided capital (genuine edge)
- **Middle 30%**: active wallets with measurable edge below that threshold, or high-edge wallets with smaller capital
- **Bottom 55%**: mean-pickers, cooling wallets, and low-decROI wallets — these are eviction candidates

This is the opposite of the current inversion.

---

## 6. Discovery gate

Any wallet entering the pool must pass **all** of:

| gate | threshold | rationale |
|---|---|---|
| `resolvedMarkets ≥ 10` | up from 3 | tiny samples can't prove skill; increases decidedROI confidence |
| `decidedCapital ≥ $5,000` | new | minimum truth-sample size; kills $98-capital wallets like rank=585 |
| `decidedROI ≥ 0.08` (8%) | replaces current composite | direct truth gate |
| `lastTradeTs within 30 days` | tightened from 60 | can't evaluate skill on wallets that aren't trading |
| `winRate < 0.98` | new | rejects pure mean-pickers at the door |

**Expected effect on current pool:** Running these gates retroactively against the 74-wallet sample would reject 30–40% of it. That matches the intuition that roughly half the tracked pool is useless.

---

## 7. Eviction policy

Wallets in the pool get checked every rescore cycle. Evict on **any**:

| condition | rationale |
|---|---|
| `daysSinceLastTrade > 30` | dead wallets don't produce future signals |
| `decidedROI < 0.03` AND `decidedCapital > $100k` | confirmed mean-picker with real capital deployed |
| `resolvedMarkets` growth < 2 over 30-day window | wallet stopped producing new decided outcomes even if technically "active" |
| `winRate ≥ 0.99` AND `decidedROI < 0.05` | extreme mean-picker (98%+ WR can survive as edge if decROI is real) |
| `score < 15` for 3 consecutive rescores | sustained underperformance |

Grace period: wallets freshly admitted get 7 days before eviction checks apply, so noise in early measurements doesn't flush legitimate wallets.

---

## 8. Two-tier tracking

Currently every wallet in the pool generates signals equally. That's wasteful.

Propose:

| tier | score cutoff | treatment |
|---|---|---|
| **A — Signal** | ≥ 50 | Trades from these wallets produce signals. Weight: full. |
| **B — Confirmation** | 30–50 | Tracked, but only used to confirm Tier A signals (if 2+ Tier A wallets enter a market and 1+ Tier B follows, signal strength increases). No independent signals. |
| **C — Shadow** | 15–30 | Measured and stored but not used for signals. Eligible for promotion if they start winning. |
| **evict** | < 15 | Evicted on next cycle. |

This lets the tracked pool stay large enough to capture long-tail edge while keeping signal-producing wallets tight. Expected ratio: Tier A ~10%, Tier B ~20%, Tier C ~30%, evict ~40% of existing pool.

---

## 9. Implementation plan

### Phase 1 — Measurement upgrade (1–2 days, no production impact)

**Goal:** add `decidedROI`, `decidedCapital`, `decidedPositions` to every wallet's stats.

- Modify `fetchGoldskyWalletPnl` in `scanner/dataApi.js` (or add sibling `fetchGoldskyWalletLedger`) to pull full position list — essentially what `wallet-ledger.cjs` does in `fetchPositions()`.
- Classify each position using the existing `markets.json.gz` lookup (591k entries, fixed last commit). Fallback to Gamma for unresolved lookup.
- Compute aggregate `decidedROI`, `decidedCapital`, `decidedPositions` in the same pass. Attach to `wallet.stats`.
- **Cost:** roughly 1 Goldsky query per wallet per rescore. Current rescore batch = 100 wallets per cycle. With the existing semaphore + backoff we already use in batch-ledger, this is ~30–60s per rescore batch. Not prohibitive.
- Ship behind a feature flag (`CONFIG.ENABLE_DECIDED_METRICS = true`) so we can disable if it blows up.

**Exit criteria:** every wallet in `data/wallets.json.gz` has `stats.decidedROI` etc. populated after one full rescore pass.

### Phase 2 — New score formula, shadow mode (1 day)

**Goal:** compute new score alongside old one without using it.

- Add `computeWalletScoreV2` in `dataApi.js` implementing §5.2.
- Write both old `score` and new `scoreV2` onto each wallet.
- Do not change ranker or signal logic yet.
- Run for ~1 scan cycle to populate scoreV2 across pool.

**Exit criteria:** `scoreV2` present on every wallet; no production behaviour changed.

### Phase 3 — Validation (1 day)

**Goal:** prove scoreV2 actually tracks truth.

- Rerun `batch-ledger.cjs --sample 50` on a fresh random 50-wallet slice.
- Recompute Spearman(scoreV2, decidedROI). Target: ≥ +0.5 (vs current −0.152).
- Recompute score-band breakdown. Top band should have highest median decidedROI, monotonically decreasing.
- Hand-audit the top 10 and bottom 10 by scoreV2 for sanity.

**Abort criteria:** Spearman < 0.3 means the formula needs tuning before Phase 4.

### Phase 4 — Eviction pass (shadow-then-live, 1 day)

**Goal:** mark eviction candidates, review, then evict.

- Run the Phase 6 eviction rules in read-only mode, outputting a CSV of candidates and reasons.
- Human review (you) confirms the eviction list isn't killing anything good.
- Flip to live; evict.
- Expected pool size after: ~550–650 wallets (from 1000).

### Phase 5 — Tier assignment + signal integration (1–2 days)

**Goal:** switch signal generation to tier-aware logic.

- Assign tiers A/B/C on every rescore based on `scoreV2`.
- Update signal generation code to respect tiers (confirmation signals for Tier B, no signals for Tier C).
- Monitor signal volume for 1 week. If volume collapses, we've over-gated — tune tier cutoffs.

### Phase 6 — Discovery gate tightening (1 day)

**Goal:** stop new shit wallets entering the pool.

- Add new gates from §6 to the discovery admission path.
- Run discovery; observe admission rate. Expect: admission rate drops by ~40% because the old gates were letting noise through.
- Let run for 1 week. If pool shrinks too far and can't refill, loosen gates (probably `decidedROI ≥ 0.06` instead of 0.08).

---

## 10. Validation plan

Two standing validation jobs post-deployment:

1. **Weekly ground-truth batch** — `batch-ledger.cjs --sample 75` every Sunday; compute Spearman(scoreV2, decidedROI). Alert if Spearman drops below 0.3 for two consecutive weeks.
2. **Monthly pool audit** — full ground-truth on top 100 wallets by scoreV2. Expect median decidedROI ≥ 15%. If not, the top tier is decaying.

Store both outputs in `out/validation/` with date-stamped filenames so we build a longitudinal record.

---

## 11. Known limitations and deferred work

### Deferred

- **Per-trade timestamps** — required for time-partitioned ROI (last 30d vs lifetime). Goldsky's `userPosition` is aggregate-only. Would require either raw CTF trade-events subgraph or paginated Polymarket `/activity` scraping. Roadmapped for v2.
- **Strategy segmentation** — fast-flippers (p50 hold 11h in our sample) vs position-holders (p90 hold 204h) are very different beasts. Currently both live in the same pool. A proper system would segment them and score within segment, then cross-weight. Out of scope for v1.
- **Market-category edge** — some wallets have massive edge in sports, none in politics. Aggregate decidedROI hides this. `stats.categories` already exists; category-weighted scoring is v2.
- **Hidden unredeemed losses in unknown markets** — our `trueUnknown` coverage rate was 0/67648 positions in the sample (good), but if our `markets.json.gz` goes stale or misses a block of markets, losses could hide as "open". Sanity-check every week.

### Known risks

- **Phase 1 Goldsky cost** — if the extra per-position detail turns out to be too expensive at scale (rate limits, slow range queries on big wallets), we'll need to cache decidedROI at admission and only recompute on a slower cadence than recency.
- **scoreV2 requires decidedROI, which requires Phase 1** — can't skip ahead. If Phase 1 stalls on rate limits, everything else stalls.
- **Discovery gate tightening could starve the pool** — if we evict 40% and admission rate drops 40%, we could shrink to <500 wallets and miss signals. Worth monitoring volume during Phase 6.

---

## 12. Concrete next action

With approval of this plan, the first commit is Phase 1 — adding `decidedROI` / `decidedCapital` to `scanner/dataApi.js` measurement. That's the enabling work; everything else depends on it.

Estimated total time to Phase 5 (all tiers live, gates in place): **5–7 days of focused work**, gated on each phase's validation.
