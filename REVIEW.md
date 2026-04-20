# System Review — Unified Pipeline

**Branch:** `merge-handpicked-diagnostics`
**Scope:** Post-consolidation audit of the entire wallet → signal pipeline with unified scoring. Documents decisions made and flags open questions for your review before production rollout.

---

## 1. Pipeline at a glance

```
Goldsky subgraph ──► Discovery (gates)
     │                    │
     │                    ▼
     └──► Per-position ──► analyzeTradeHistory (/activity)
          ledger          │
                          ▼
                    attachMMClassification (dataApi)
                    attachAlphaEvaluation (alphaTest)
                          │
                          ▼
                    computeWalletScore  ◄── one authoritative score
                          │
                          ▼
                      pool[addr].score
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
         Eviction     Ranker      detectConvergence (signals)
         (strikes)  (sort score)    │
                                    ▼
                              processSignals ─► signal tiers
                                                 (elite/pro/starter)
                                                 via confidence 0–100
```

**One score drives three systems:** eviction, ranking, and signal generation.

## 2. Decisions I made during cleanup (sanity-check these)

### 2.1 Score scale — not rescaled, thresholds recalibrated

`computeWalletScore` returns 0–55 in practice, not 0–100. I deliberately did **not** rescale the output because that would force every downstream threshold to migrate. Instead I recalibrated the consumers:

| threshold | old (0–100 scale) | new (0–55 scale) |
|---|---|---|
| `MIN_SCORE_POOL` | 50 | **5** |
| `LOW_SCORE_THRESHOLD` (eviction) | 15 | **5** |
| `SIGNAL_THRESHOLDS.SOLO_MIN_SCORE` | 80 | **25** |
| `SIGNAL_THRESHOLDS.CLUSTER_MIN_AVG_SCORE` | 75 | **18** |
| `SIGNAL_THRESHOLDS.CONSENSUS_MIN_AVG_SCORE` | 60 | **12** |
| confidence: `score / 95` divisor | 95 | **45** (`MAX_PRACTICAL_SCORE` constant in signals.js) |
| confidence: `avgScore / 90` divisor | 90 | **45** (`MAX_PRACTICAL_SCORE`) |
| frontend `scoreClass` high/mid/low cutoffs | 70/40 | **25/10** |

**Why not rescale to 0–100?** Too many downstream thresholds with judgement-baked-in values (strikes, eviction, tier cutoffs). Rescaling would cascade into every file. Picking one constant (`MAX_PRACTICAL_SCORE = 45`) as the single source of truth for the 0–55 → 0–100 confidence translation is simpler and keeps drift confined to one place.

**⚠ Review needed:** You may want to verify these thresholds against the first live scan's output. If all wallets score below `SOLO_MIN_SCORE = 25`, drop to 20; if too many, raise to 28.

### 2.2 Unified dormancy

`MAX_INACTIVE_DAYS: 60` and `DISCOVERY_MAX_INACTIVE_DAYS: 30` and `DORMANCY_DAYS: 30` all became one: **`DORMANCY_DAYS: 30`**. Applied at discovery gate (can't enter if dormant) AND rescore eviction (kicked out if goes dormant).

The old 60-day tail was inherited from pre-decidedROI days when Goldsky was slower to surface new activity. With the current scanner cadence there's no reason for a 60-day grace.

**⚠ Review needed:** 30 days might be aggressive. If the pool shrinks on the next scan because legitimate wallets happen to be between bets, bump to 45.

### 2.3 Signal tier cutoffs (elite / pro / starter)

I **lowered** the tier cutoffs slightly:

| tier | old | new |
|---|---|---|
| elite | confidence ≥ 80 | ≥ 75 |
| pro   | confidence ≥ 55 | ≥ 50 |
| starter | rest | rest |

Reasoning: even with the recalibrated confidence formulas, real signals on the new scale may not hit the original 80 ceiling as often. Slightly looser bands let the elite tier stay populated. This is a marketing-product decision, not a correctness decision — if you want fewer elite signals, raise back to 80.

**⚠ Review needed:** This is pure judgment. Watch one scan's signal output, tune accordingly.

### 2.4 Solo confidence now uses `decidedROI` instead of raw WR

The old `computeSoloConfidence` had `wr > 0.5` contributing up to 15 points of confidence. Post-consolidation, win rate alone is a weak signal (mean-pickers have 95% WR). I replaced it with `decidedROI` scaled to 30% = full 15 points, with a fallback to WR if decidedROI isn't populated.

Directional wallets with 55% WR but 25% decidedROI now beat mean-pickers with 99% WR but 2% decidedROI on solo confidence — aligned with the scoring redesign's whole point.

**⚠ Review needed:** Watch solo signals for a week. If you see too few, drop the saturation point from 30% to 20%.

### 2.5 Dead code in `lib.js` — unexported, body kept

`scanner/lib.js` still has ~1,200 lines of dead signal/scoring code (function bodies for `processSignals`, `computeConsensus`, three `compute*Confidence` variants, `getSignalTier`, `SIGNAL_THRESHOLDS`, `analyzePositions`, `computeScore`, etc. — all shadowed by newer implementations in `signals.js` / `dataApi.js`).

I **unexported** them so nothing outside lib.js can reach them, but **left the function bodies in place** to avoid any risk of accidentally breaking internal calls I didn't trace.

**⚠ Follow-up work:** a focused PR to delete the dead function bodies (lib.js will shrink from ~2750 lines to ~1500). Not urgent, not blocking, but worth doing before the next major refactor to stop confusing future readers.

## 3. Config — every knob in one place

Full `CONFIG` block in `scan.js`, reorganized by purpose:

```js
// ── Pool sizing & admission ──────────────────────────────────────────────
TARGET_POOL_SIZE: 1000,          // Top N wallets kept after ranking
MAX_DISCOVERY_WALLETS: 5000,     // Max candidates per discovery cycle
RESCORE_BATCH_SIZE: 100,         // Wallets rescored per cycle
DISCOVERY_INTERVAL_SCANS: 3,     // Full discovery every N scans
MIN_SCORE_POOL: 5,               // Pool admission floor (0–55 scale)

// ── Discovery gates (new wallets) ────────────────────────────────────────
MIN_PNL_DISCOVERY: 500,          // Goldsky PnL floor before we bother
MIN_POSITIONS_DISCOVERY: 10,     // Goldsky position count minimum
MIN_RESOLVED_MARKETS: 10,        // Resolved markets minimum (no flukes)
DISCOVERY_MIN_DECIDED_CAPITAL: 5000,   // $5k+ risked on resolved plays
DISCOVERY_MIN_DECIDED_ROI: 0.08,       // 8%+ ROI on decided capital
DISCOVERY_MAX_WIN_RATE: 0.98,          // Mean-picker shape reject
DISCOVERY_MAX_WIN_RATE_MIN_RESOLVED: 25,
MAX_WALLET_AVG_ENTRY_PRICE: 0.85,      // Scrap-grader filter

// ── Per-wallet measurement ───────────────────────────────────────────────
ENABLE_DECIDED_METRICS: true,          // Goldsky per-position fetch

// ── Dormancy (unified) ───────────────────────────────────────────────────
DORMANCY_DAYS: 30,                     // single source of truth

// ── Eviction ─────────────────────────────────────────────────────────────
EVICTION_MODE: 'shadow',               // 'off' | 'shadow' | 'live'
MEAN_PICKER_STRIKES_TO_EVICT: 3,
LOW_SCORE_THRESHOLD: 5,                // strike if score below this
LOW_SCORE_STRIKES_TO_EVICT: 3,
NEG_ROI_CAPITAL_FLOOR: 10000,          // decidedCapital ≥ $10k + ROI<0
NEG_ROI_MIN_RESOLVED: 25,
```

No `V2_*` flags, no `MIN_SCORE_POOL_V2`, no `USE_SCORE_V2`, no coverage thresholds. One path.

## 4. What's still on `EVICTION_MODE: 'shadow'`

**Eviction is still in shadow mode.** Wallets that match the eviction rules get tagged `wouldEvict` but stay in the pool. The dashboard shows the count. This is intentional — flip to `'live'` only after:

1. One full scan runs post-consolidation successfully.
2. Shadow-eviction count matches your expectations (not thousands of false positives).
3. Cross-validation still passes.

Then change one line: `EVICTION_MODE: 'shadow'` → `EVICTION_MODE: 'live'`.

## 5. Open questions for your judgment

These aren't code problems — they're product/marketing decisions I can't make for you:

1. **`TARGET_POOL_SIZE: 1000` — right number?** The HANDOFF notes you'd already decided against increasing. Still valid with the tighter admission gates? If the new gates push qualifier count below 1000, the "unscored protection" kicks in and keeps less-qualified wallets to fill the pool.
2. **Tier cutoffs for elite/pro/starter** — what does each tier mean to the end user? If elite = "zero-question, put money on this", cutoff should probably be 80, not 75. I went slightly looser; you tune.
3. **Signal tier labels** — the codebase still uses "elite / pro / starter". If you want to rename for a directional-signals product (e.g. "hammer / strong / watchlist"), it's a single-function change in `signals.js:getSignalTier`.
4. **`SOLO_MAX_PER_WALLET: 3`** — limits one wallet to max 3 concurrent solo signals. Prevents portfolio flooding. Still the right value?
5. **Shadow eviction — are you ready to flip live?** The safety is that shadow mode is free; the risk is the pool silently bloats with wallets that are actually degrading. Recommend: run shadow for one week, if would-evict count is stable and believable, flip live.
6. **Cross-validation should probably run in CI** — I wrote it as a standalone script. A one-line addition to a GHA workflow would catch regressions automatically on every scan. Flagged in MERGE-PLAN.md as Stage 4 deferred wiring.
7. **Paper-trader integration** — I did not touch `initPaperTrading` / `processPaperTrades`. They still reference `wallet.score` which now means the unified score — should work out of the box, but verify that paper-trade sizing calibration still makes sense on the new scale.

## 6. Rollout order (recommended)

If you're ready to merge this to main:

1. **Stage 0 only** (PnL fix) → scan once → verify `effectivePnl` changed as expected on a known-MM wallet.
2. **Stages 0 + 1** (PnL + MM classifier) → scan once → check MM classifier didn't nuke the whole pool.
3. **Stages 0 + 1 + 2** (+ alpha test) → scan once → check alpha-verdict='fails' rejections are sensible.
4. **Full consolidation** (this branch) → scan once → check scores repopulate under the new unified pipeline, shadow-eviction count is believable.
5. **Flip eviction to 'live'** → run for a week → review pool churn.
6. **Stages 3 + 4** (participant harvest + cross-val cron) → productionize discovery and monitoring.

## 7. Verification status

- Syntax: 14/14 JS files clean
- Smoke tests: 3/3 pass (Stage 0 PnL, Stage 1 MM, Stage 2 alpha)
- Cross-validation: ✓ all checks passing
  - Spearman(score, decidedROI) = **0.618** (target ≥ 0.5)
  - Top decile ROI 28.5% vs bottom 1.0%
  - 0 MM disagreements at Tier A
  - 0 alpha failures at Tier A

## 8. If something goes wrong

The feature branch is clean — nothing pushed, main untouched. Rollback is `git checkout main`. The migration in `scan.js` startup only READS `scoreV2` to populate `score` — it doesn't modify main's data format.

To commit from your Mac:

```bash
cd ~/polymarket-wallet-engine
rm -f .git/index.lock
git add -A
git commit -m "consolidation: unified score + recalibrated signal pipeline"
```

Or split into two commits if you want granular history:

```bash
git add scanner/dataApi.js scanner/scan.js scanner/mmClassifier.js scanner/alphaTest.js \
        scanner/participantHarvest.js scripts/ frontend/
git commit -m "merge handpicked-signals diagnostics + consolidate V1/V2 scores"

git add MERGE-PLAN.md REVIEW.md
git commit -m "docs: merge plan + system review"
```

Do not merge to `main` wholesale — phase per §6.
