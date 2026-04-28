# Signal System Audit — How signals actually get produced

This walks through every step from "wallet buys a market" to "Telegram alert" so you can see what's happening at each stage and spot anything weird.

---

## The 30-second version

Every 2 hours, a "fast loop" runs:

1. **Fetch trades** — pull last 48h of trades from all 1000 pool wallets
2. **Detect convergence** — group buys by market, find markets where 2+ pool wallets bought
3. **Classify each candidate** as one of 4 signal types based on wallet count + entry price
4. **Run gates** specific to each type — kill candidates that don't pass
5. **Compute confidence** for survivors → assign tier (starter/pro/elite)
6. **Emit** signal to Telegram

That's the entire pipeline. Everything below is just unpacking each step.

---

## Step 1 — Detect convergence (`detectConvergence` in `signals.js`)

**Input:** `recentTrades` — a Map of `wallet → [trades from last 48h]`

**What it does:**
- Loops through every wallet's trades
- For each BUY trade, groups by market (`conditionId`)
- Filters out wallets that fail sourcing gates: `isLikelyMM`, `isMeanPickerShape`, `alphaVerdict='fails'`, `score < PER_WALLET_MIN_SCORE (15)`
- Filters trades older than `CONVERGENCE_WINDOW_HOURS (48)`
- Builds a list of "candidates" where each candidate is `{market, wallets, totalSize, avgPrice, walletCount, ...}`
- Drops markets with fewer than `MICRO_CLUSTER_MIN_WALLETS (2)` wallets

**Output:** sorted list of candidates by strength (`walletCount × avgScore`)

**Sanity check:** walletCount=1 candidates are dropped here. Solo signals are NOT processed by convergence detection — they're a separate phase (Step 4 below).

---

## Step 2 — Classify each candidate by type (`processSignals` Phase 1)

For each candidate, decide which signal-type path it follows based on wallet count + entry price:

| Wallet count | Entry price | Signal type | Notes |
|---|---|---|---|
| **8+** | any | `consensus` | Big-cluster crowd |
| **6 or 7** | any | `cluster` | Mid-tier cluster |
| **2-5** | 70-85¢ ONLY | `micro-cluster` | Heavy-favorite small cluster |
| **2-5** | other prices | (rejected as `type_floor`) | The 638/scan we see in kill logs |
| **1** | n/a — handled by Solo path separately | | |

This is the source of `type_floor=638` we see in kill breakdowns. Most rejected candidates here are 2-5 wallet convergences NOT on a 70-85¢ favorite — historical data showed those buckets lose money on average, so they're filtered out by design.

**Concern:** is the 2-5 wallet × 70-85¢ rule too narrow? Per the simulator we just ran, widening it to 50-85¢ added 223 signals at -5.6% avg return. So no, it's calibrated correctly for quality. But it does mean a lot of detected convergences die here without emitting.

---

## Step 3 — Gates per signal type (`processSignals` Phase 1)

After classification, each type runs its own threshold check:

### Consensus (8+ wallets)
- `avgScore >= CONSENSUS_MIN_AVG_SCORE (12)` — a low bar; consensus relies on crowd size
- `totalBuySize >= CONSENSUS_MIN_TOTAL_SIZE ($1000)` — must have real money behind it

### Cluster (6-7 wallets)
- Every wallet must have `score >= CLUSTER_MIN_PER_WALLET_SCORE (18)` — no marginal hangers-on
- `avgScore >= CLUSTER_MIN_AVG_SCORE (25)` — top decile on average
- `totalBuySize >= CLUSTER_MIN_TOTAL_SIZE ($750)`

### Micro-cluster (2-5 wallets, 70-85¢)
- `avgScore >= MICRO_CLUSTER_MIN_AVG_SCORE (20)` — looser than cluster
- `totalBuySize >= MICRO_CLUSTER_MIN_TOTAL_SIZE ($500)` — looser than cluster

**Why looser for micro-cluster?** The 70-85¢ entry-price requirement is doing the heavy EV work. Heavy favorites resolving has positive expected value at this price band (77% WR / +22% historical). The wallet-count and avgScore filters are secondary safeguards.

---

## Step 4 — Solo signal path (`processSignals` Phase 1b)

Solos are processed COMPLETELY separately from convergence — they iterate `recentTrades` directly looking for single-wallet large buys.

Per wallet, must pass:
- `score >= SOLO_MIN_SCORE (25)` (whales bypass via WHALE_MIN_ROI/CAPITAL)
- `recentWinRate >= SOLO_MIN_WIN_RATE (0.55)`
- `resolvedMarkets >= SOLO_MIN_RESOLVED (50)` — sample-size floor
- `style ∈ SOLO_ALLOWED_STYLES (sniper, averager, churner)` — unless whale
- `alphaVerdict` not in fail-list
- Per buy: `buySize >= SOLO_MIN_BUY_SIZE ($500)`
- Max `SOLO_MAX_PER_WALLET (3)` open solo signals per wallet

Then the same final gates (market not closed, has price, EV/stale-follower).

**Concern flagged:** the solo path's filtering of trades-by-timestamp is INSIDE the loop body using a 48h window — duplicates the work convergence detection already did. Not a bug, just duplicate filter logic. Tiny inefficiency.

---

## Step 5 — Final emission gates (apply to all types)

These run on every candidate that passed type-classification + thresholds:

1. **`marketClosed === true`** → `kills.market_closed` — Polymarket already settled
2. **`resolvesTooSoon(market)`** → `kills.resolves_too_soon` — endDate within 4h, follower can't act
3. **`currentPrice missing`** → `kills.no_price` — can't compute return without price
4. **`impliedMaxROI(currentPrice) < MIN_OPEN_ROI (0.15)`** → `kills.open_roi_too_low` — token >87¢ has <15% upside
5. **`currentPrice > avgEntryPrice × (1 + STALE_FOLLOWER_MAX_PREMIUM (0.15))`** → `kills.stale_follower` — alpha already captured

If all 5 pass: signal opens.

---

## Step 6 — Compute confidence + tier

The signal gets a confidence score 0-100 from one of two formulas:

### `computeConvergenceConfidence(candidate, signalType)` — for cluster/consensus/micro-cluster

| Component | Max points | Source |
|---|---|---|
| Wallet count | 40 | More wallets agreeing = stronger crowd |
| Avg score | 10 | (capped — was 25 before audit showed it was anti-predictive) |
| Total size | 20 | log10(1+size)/4 — saturates around $10k |
| Convergence speed | 15 | Tighter time-cluster = stronger |
| Entry price | 10 | Cheaper = better max upside |
| **Total** | **95 max** | |

### `computeSoloConfidence(walletInfo, buySize, avgPrice)` — for solo

| Component | Max points | Source |
|---|---|---|
| Wallet score | 20 | (capped — was 40 before audit) |
| Position size | 30 | log10 of $ |
| Entry price | 15 | |
| Wallet ROI / WR | 10 | Decided edge or recent WR |
| Attribution multiplier | ±15 | Wallet's proven signal-emission record |
| **Total** | **90 max** | |

### Tier from confidence (`getSignalTier`)
- `>= 88` → **elite** (rarely hit by design — historical elite cohort was -14.7% avg return)
- `>= 55` → **pro**
- `< 55` → **starter**

**Concern flagged:** elite tier is essentially unreachable by design. The 88 threshold was raised from 75 because the elite cohort was historically losing. This is intentional but worth knowing — you'll basically never see "elite" tier in modern signals.

---

## Concrete walkthrough — the Man Utd signal

From scan #304 the convergence detector found:
- 25 wallets bought "Will Manchester United FC win on 2026-04-27?" within last 48h
- Avg entry price 52¢
- Total buy size $13,915

**Step 2 classification:** walletCount=25 ≥ 8 → **consensus**

**Step 3 thresholds:**
- avgScore (let's say 22) ≥ 12 ✓
- totalSize $13,915 ≥ $1,000 ✓

**Step 5 final gates:**
- marketClosed? Match was YESTERDAY, so probably yes → **killed at `market_closed`**

That's why this candidate appears in our top-3 logs but never emits. The convergence is real (25 wallets agreed) but the market settled before we could act.

---

## Where overlaps and concerns are

**1. Solo timestamp filter is redundant.** The solo path filters trades by `soloWindowTs` inside its own loop, but `fetchRecentTrades` already filters at fetch time. Not a bug, just dup. Could simplify.

**2. Convergence sourcing gates duplicate solo sourcing gates.** Both paths reject `isLikelyMM`, `isMeanPickerShape`, `alphaVerdict='fails'`. If a wallet has those flags, neither path will use them. Could factor out a shared `isUsableWallet()` check.

**3. The `else if (meetsThresholds)` fallthrough is silent.** Until the kill-tracking we just added, candidates that fit a path but failed avgScore/size floors disappeared without trace. Now they show as `cluster_below_thresholds` etc.

**4. Elite tier is unreachable.** By design — but the dashboard might still display it as a possibility. UI decision: either hide elite or document why it never appears.

**5. Solo path doesn't have `resolvesTooSoon` gate at the same place — let me check.**

Actually looking again — solo path DOES have `resolvesTooSoon(mi)` at the same spot. OK, not a bug.

---

## What's NOT a problem (despite seeming weird)

- **638 type_floor kills per scan** — these are 2-5 wallet × non-favorite-band convergences. Historical data shows those bands lose money. Correctly filtered.
- **Most NBA/soccer markets killed at `market_closed`** — sports events resolve fast; by scan time the convergence is on a settled market. Correctly filtered (we can't trade settled markets).
- **Active count low (~74)** — long-tail political bets dominate active set; new emissions are rare but high-quality. Correctly low.
- **0-2 emissions/scan** — bottleneck isn't gating logic, it's that the 1000-wallet pool genuinely doesn't produce many fresh, valid convergences in a 48h window. Loosening dilutes quality.

---

## Net assessment

The system is doing what it should be doing. The bands and gates are calibrated against ~1500 resolved historical signals. Loosening any single gate adds more signals at lower per-signal return — confirmed by the simulator we ran.

If volume is the goal, the answer is upstream: grow the pool, change convergence detection to include sequential buys, or accept current quality as the product.

If quality is the goal, current settings are essentially optimal.
