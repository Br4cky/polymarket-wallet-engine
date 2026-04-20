# Merge Plan — handpicked-signals → wallet-engine

**Branch:** `merge-handpicked-diagnostics`
**Date:** 2026-04-20
**Status:** Stages 0–4 implemented and tested on feature branch. Stage 5 (this doc).

---

## 1. Why this merge

Two parallel efforts converged on the same problem:

- **polymarket-wallet-engine** — the autonomous production signal engine. 1000-wallet pool, GitHub Actions cron every ~80min, Netlify dashboard, 500 resolved signals at 73.7% WR. Architecturally strong but scoring had known weaknesses documented in [SCORING-REDESIGN.md](./SCORING-REDESIGN.md): Spearman(legacyScore, decidedROI) = -0.152 (the ranker was *inverted* on truth), and no mechanism to detect whale-01-class market-makers whose "PnL" came from MAKER_REBATE + LP reward streams rather than directional edge.

- **handpicked-signals** — a smaller Python R&D project on a 29-wallet curated pool. Built specifically to diagnose the failure modes the production engine couldn't catch: a 6-signal MM classifier, a single-side alpha test (`edge_pp = hit_rate − avg_entry_price`), explicit recency gates, and an /activity event ingestion path that captures MERGE/REWARD/MAKER_REBATE events.

The two systems share the same data sources (Polymarket Data API, Goldsky subgraph) but had non-overlapping diagnostic strengths. This merge brings handpicked-signals' diagnostic algorithms into wallet-engine's chassis, preserving the working 73.7% WR signal generation while patching the blind spots that let mean-pickers and market-makers rank at the top.

## 2. The blind spot we were missing

The original wallet-engine's `analyzeTradeHistory` (in `scanner/dataApi.js`) explicitly dropped every non-TRADE/REDEEM event:

> ```
> // Other activity types (REWARD, SPLIT, MERGE, CONVERSION) are dropped.
> ```

And Goldsky's `realizedPnl` is position-redemption-only — it can't see MAKER_REBATE or LP REWARD distributions because they aren't position changes. That combined blind spot meant:

- A wallet earning $3.5M in maker rebates over 7 months would show ~$200–500k of visible PnL (bare trade + redemption).
- The MM classifier couldn't fire because the rebate income was invisible.
- scoreV2's mean-picker flag (WR ≥ 95% AND decidedROI < 5%) only catches the 99% WR scrap-grader failure mode — not the whale-01 failure mode (59% WR, fat PnL, 6/6 MM signals).
- These wallets could rank into Tier A and emit signals that are fundamentally *not copy-tradeable* — the edge comes from rebates and LP yield, not from predicting outcomes.

Stage 0 fixes the measurement; Stages 1–2 make the scoring act on it.

## 3. Stages implemented

### Stage 0 — PnL math fix (foundational)

**Files:** `scanner/dataApi.js`, `scanner/scan.js`, `scripts/test-stage0-pnl.mjs`.

`analyzeTradeHistory` now ingests every event type the `/activity` endpoint returns:

| event type      | handling                                                                                      |
|-----------------|-----------------------------------------------------------------------------------------------|
| `TRADE`         | existing — BUY/SELL into per-market PnL                                                        |
| `REDEEM`        | existing — synthetic SELL at implied payout price                                              |
| `MERGE`         | **new** — synthetic SELL at implied per-share price (usdcSize/size). Closes MERGE-terminated positions so they actually resolve in the per-market loop. Counted for `mergeRate`. |
| `REWARD`        | **new** — accumulated into `rewardUsdcTotal`. No conditionId required (protocol-level distribution). |
| `MAKER_REBATE`  | **new** — accumulated into `rebateUsdcTotal`. Some API variants fold into `REWARD`; handler copes. |
| `SPLIT`         | **new** — counted (`splitCount`); correlates with MM arb plumbing                              |
| `CONVERSION`    | **new** — counted                                                                              |

New fields on stats:
```
mergeCount, mergeRate, mergeMarkets, mergeUsdcTotal
splitCount, splitMarkets, splitUsdcTotal, conversionCount
rewardUsdcTotal, rebateUsdcTotal, nonDirectionalIncome
economicPnl           // totalPnl + rewards + rebates — use this for effectivePnl
totalBuys, totalSells, sellRatio
dualSideMarkets, dualSideRate, avgDualSidePriceSum
singleSideResolved, singleSideWins, singleSideLosses
singleSideHitRate, singleSideAvgEntry, singleSideCapital
singleSidePnl, singleSideROI, edgePP
```

`scan.js` now uses `economicPnl` (fallback to `totalPnl` for backcompat) instead of the bare `totalPnl` when computing `effectivePnl`. The dashboard field names are unchanged — existing consumers still work.

**Verification:** `node scripts/test-stage0-pnl.mjs` — 15-assertion smoke test against a whale-01-shape synthetic fixture. Passes; shows `totalPnl=$55` vs `economicPnl=$2055` on the same events (the dropped income is $2000).

### Stage 1 — MM classifier (`scanner/mmClassifier.js`)

Six-signal classifier, each worth 1 point:

1. `sellRatio < 0.05` — closes via REDEEM/MERGE, not SELL
2. `dualSideRate > 0.40` — buys both YES and NO on >40% of markets
3. `mergeRate > 0.10` — uses MERGE for pair-arb on >10% of markets
4. `avgDualSidePriceSum < 1.01` — free-arb bids below fair value
5. `rebateUsdcTotal > $100`
6. `rewardUsdcTotal > $100`

Gated on `uniqueMarkets ≥ 25` to prevent small-sample false positives.

Score → penalty mapping:
- 0–2 → `mmPenalty = 1.0` (no impact)
- 3 → `mmPenalty = 0.5` (ambiguous; halve but don't evict)
- 4–5 → `mmPenalty = 0.1` (effectively evicted)
- 6 → `mmPenalty = 0.0` (eliminated)

Wired into `computeWalletScoreV2`: the core formula now multiplies by `mmPenalty` alongside the existing `meanPickerPenalty`. These catch *overlapping but distinct* failure modes — mean-picker catches scrap-graders gaming WR; MM catches rebate/LP earners.

**End-to-end impact:** a synthetic whale-01-shape wallet scores scoreV2 = 31.9 before Stage 1, scoreV2 = 0 after. A directional wallet scores identically before and after (24.8). Penalty is surgical.

**Verification:** `node scripts/test-stage1-mm.mjs` — 4 canonical wallet shapes (clean, soft-MM, whale-01, rookie). All assertions pass.

### Stage 2 — Single-side alpha test (`scanner/alphaTest.js`)

For markets where the wallet only bought ONE outcome (single-side bets):

```
edge_pp = (hit_rate − avg_entry_price) × 100
```

A mean-picker buying $0.95 and winning 95% scores `edge_pp ≈ 0` (no predictive power; the market was right). An insight-driven bettor buying $0.40 and winning 55% scores `edge_pp ≈ +15pp` (real alpha).

This complements `decidedROI`:
- `decidedROI` answers *"did this wallet make money?"*
- `edge_pp` answers *"did this wallet beat the market's implied probability?"*

A wallet should pass both to qualify as a Tier A directional alpha — they catch different failure modes (a wallet can make money without predictive edge, or predict well without making money).

Thresholds (`ALPHA_THRESHOLDS` in `alphaTest.js`):
- `MIN_SINGLE_SIDE_RESOLVED = 50`  — min sample
- `MIN_CAPITAL_FOR_EDGE = $5,000`  — min capital-at-risk
- `MIN_SINGLE_SIDE_ROI = 5%`       — positive ROI requirement (alpha + money)
- `TIER_A_MIN_EDGE_PP = 3.0`
- `TIER_B_MIN_EDGE_PP = 1.5`

Verdicts: `tier_a | tier_b | fails | insufficient_sample | insufficient_capital | no_edge_computed`.

`scan.js` discovery path now hard-rejects any candidate whose `alphaVerdict === 'fails'` (alongside the existing mean-picker-shape and isLikelyMM rejections). `insufficient_sample` candidates are still admitted — they haven't disqualified themselves yet.

**Verification:** `node scripts/test-stage2-alpha.mjs` — 4 canonical cases (directional, mean-picker, lucky-small-sample, tier-B). All assertions pass.

### Stage 3 — Market-participant BFS harvest (`scanner/participantHarvest.js`, `scripts/harvest-participants.mjs`)

Replaces the survivorship-biased top-volume leaderboard as the primary discovery source. For each top-tier wallet in the pool: sample their winning markets (biggest conviction wins), fetch all participants from those markets via `/trades?market=<cid>`, rank by appearance frequency × log-volume.

Why it works: top-volume lists churners regardless of edge. This pipeline is pre-filtered to "wallets that play in the same markets our known winners win" — correlates strongly with real edge. Handpicked-signals reported 14.5% qualifier rate on 200-wallet probes vs the usual 3-5% for leaderboard scans.

Module is standalone — does not modify `scan.js` main loop. Writes `data/discovered-candidates.json.gz` which a future scan.js revision can consume as a high-priority discovery seed.

CLI:
```
node scripts/harvest-participants.mjs --top 200 --markets-per-source 50 --tier-min 50
```

### Stage 4 — Cross-validation pipeline (`scripts/cross-validate-scoring.mjs`)

Offline sanity check run against the live pool. Four checks:

1. **MM disagreements at Tier A** — any wallet with scoreV2 ≥ 50 that the MM classifier also flags as `isLikelyMM`. Should be 0; anything else means the penalty isn't applying.
2. **Alpha failures at Tier A** — any Tier A wallet whose `alphaVerdict === 'fails'`. Warning not hard fail — allows for `insufficient_sample` verdicts in the top tier.
3. **Spearman(scoreV2, decidedROI)** across the full pool. Target ≥ 0.5. Current: **0.618** (baseline was -0.152 before any V2 work).
4. **Decile sanity**: top decile by scoreV2 should have higher median decidedROI than bottom decile. Current: top 28.5%, bottom 1.0% — cleanly ordered.

Run regularly (e.g. weekly cron or alongside scan) to catch scoring regressions. Exit code 1 on any hard failure. Persists dated JSON reports to `out/validation/`.

## 4. What's NOT changed

- **scan.js main loop** — cron cadence, fast loop, repair workflow: all untouched
- **Signal generation** (`signals.js`) — consensus/cluster/solo paths identical
- **Frontend / dashboard** — no change
- **data/ schema** — additive (new stats fields only); no breaking renames
- **Legacy `computeWalletScore`** — untouched; only V2 got the penalty wiring
- **Goldsky subgraph flow** — `positionLedger.js` etc. unchanged

This is intentional. The wallet-engine operational chassis works (73.7% WR signals, 1000-wallet pool, self-healing). This merge upgrades the *brain*, not the body.

## 5. Deployment order

Recommended phased rollout to avoid another V2-scale-mismatch-class incident:

1. **Stage 0 only** — ship the PnL fix. Run one scan cycle. Compare `effectivePnl` before/after for a handful of known wallets via `scripts/probe-activity.mjs`. Expect ~0% change for directional wallets, noticeable increase for any MM-flavoured wallets.

2. **Stage 1 shadow** — flip a config flag (recommend adding `CONFIG.MM_PENALTY_ENABLED`) to apply the MM penalty. Watch the pool for unexpected evictions or score collapses. Cross-validate with Stage 4 script.

3. **Stage 2 hard gate** — add the `alphaVerdict === 'fails'` discovery rejection. Monitor admission rate; if it drops >40%, loosen `TIER_B_MIN_EDGE_PP` to 1.0 temporarily.

4. **Stage 3 harvest** — run `harvest-participants.mjs` manually first to produce a candidate list. Compare to leaderboard discovery output. When confident, wire into scan.js as a secondary discovery source.

5. **Stage 4 cron** — add a GHA workflow (`validate.yml`) that runs the cross-validator on every scan. Alert on any hard failure.

## 6. Verification recap

All smoke tests green on feature branch:

```
node scripts/test-stage0-pnl.mjs      # 15 assertions pass
node scripts/test-stage1-mm.mjs       # 4 shapes + end-to-end, all pass
node scripts/test-stage2-alpha.mjs    # 4 canonical cases, all pass
node scripts/cross-validate-scoring.mjs
#   Pool 1000, Spearman=0.618, top/bottom decile 28.5%/1.0%, 0 MM/alpha disagreements
```

## 7. Known risks / deferred

- **MAKER_REBATE event type shape** — inferred from Polymarket's typical schema. If the API returns them under a different type (e.g., folded into REWARD sub-types), the `rebateUsdcTotal` field will be 0 and the MM classifier will see one fewer signal. Probe a known MM wallet in production to verify.
- **Goldsky cost for scaled decidedROI** — the existing cost profile is unchanged (we only added cheap in-process analyzer computations).
- **Stage 3 file consumption** — the harvest script writes a JSON seed file but `scan.js` doesn't yet consume it. Integration is one Phase 6-style change away; deferred to keep this merge focused on scoring correctness.
- **Existing `SCORING-REDESIGN.md`** — the already-shipped V2 work already drove Spearman to 0.618. Stages 1–2 add orthogonal diagnostic layers on top; they don't contradict the existing design.

## 8. Commit checklist (to run from host terminal)

```bash
cd ~/polymarket-wallet-engine
rm -f .git/index.lock

git add \
  scanner/dataApi.js \
  scanner/scan.js \
  scanner/mmClassifier.js \
  scanner/alphaTest.js \
  scanner/participantHarvest.js \
  scripts/test-stage0-pnl.mjs \
  scripts/test-stage1-mm.mjs \
  scripts/test-stage2-alpha.mjs \
  scripts/harvest-participants.mjs \
  scripts/cross-validate-scoring.mjs \
  MERGE-PLAN.md

git commit -m "merge handpicked-signals diagnostics into wallet-engine"
```

Leave on the `merge-handpicked-diagnostics` branch until you're ready to phase in per the deployment order above. Do not merge to `main` wholesale — the GHA scan runs every 80 min and a clean incremental rollout is safer.
