# Polymarket Signal Engine — Chat Handoff

**Last updated:** 2026-04-14 (after scan #160)
**Repo:** `Br4cky/polymarket-wallet-engine` (main branch)
**Latest commit on HEAD:** `8006ed5 repair: close resolved signals + sync analytics`

---

## 1. Current State (as of scan #160)

### Pool
- **Size:** 1000 wallets (at MAX_POOL_SIZE cap, churn active)
- **Avg score:** 71.4 (was 67.0 at scan #158 — climbing fast)
- **Median score:** 70.6
- **Floor:** score 60 (50–59 band was fully evicted over scans #157–#160)
- **Pool version:** 40

### Score bands
| Band | Count |
|------|-------|
| 90+ | 15 |
| 80–89 | 125 |
| 70–79 | 403 |
| 60–69 | 457 |
| 50–59 | 0 (evicted) |

### Signals
- **Active:** 41
- **History:** 500 (353 W / 126 L → **73.7% WR**, 41 voided excluded)
- **Latest scan opened 15 new signals** (6 consensus, 8 cluster, 27 solo active breakdown)
- **Tiers:** 3 elite, 37 pro, 1 starter
- **Last repair** closed 11 resolved signals cleanly

### effectivePnl rollout
- **268/1000 wallets** have `stats.effectivePnl` populated (scan #160's qualifier batch)
- Remaining wallets populate as they hit their 24h re-score window
- Expected full population by ~2026-04-15 evening (some up to 7 days for stale-pool recheck path)

---

## 2. Problems Fixed This Session

### Commit `8c79b95` — Qualification crash silently killing new wallets
**Symptom:** Scan #153 qualified 0 wallets despite 1000+ candidates.
**Cause:** `scan.js:488` referenced `trades.length` but the variable in scope was `events`. Threw a ReferenceError on every candidate, caught silently in a generic try/catch that swallowed errors.
**Fix:** Renamed to `events.length` + added error logging for future silent failures.

### Commit `3b7884d` — Active signals with no entry price
**Symptom:** ~20 new dashboard signals showing "-" for entry price.
**Cause:** When `marketLookup.get(tokenId)` returned null, signals were still being opened with `openMarketPrice = 0`, breaking dashboard display and return tracking.
**Fix:** Added `if (!(currentPrice > 0)) continue;` gate to both consensus/cluster (Phase 1) and solo (Phase 1b) signal-creation paths.

### Commit `60bc0bd` + `7577900` — Solo signals firing at 98–99¢
**Symptom:** Solo signals opening at 98–99¢ on settlement-scrap markets (terrible EV).
**Cause:** Phase 1b solo loop in `signals.js` had no price gate — `MAX_ENTRY_PRICE` only applied to consensus/cluster paths.
**Fix:** Applied `MIN_ENTRY_PRICE` / `MAX_ENTRY_PRICE` filters in solo path using wallet's avg fill price (`soloAvgPrice`).
**Note:** 9 pre-fix solo signals from scans #88/#97/#136 are grandfathered — they'll resolve naturally.

### Commit `90f3041` — Voided signals being resurrected
**Symptom:** Signals marked `outcome: 'void'` were being restored to active and backfilled.
**Cause:** Phase 2.5 history-repair logic only skipped `win`/`loss` outcomes; voided entries fell through into the restore branch.
**Fix:** Added `if (h.outcome === 'void' || h.status === 'voided') continue;` to the skip condition.

### Commit `b050fb3` — Re-score cooldowns
**Change:** Re-introduced 24h cooldown (`DISCOVERY_RESCORE_COOLDOWN_MS`) for candidates and 7d stale-recheck for pool members not in current candidates.
**Reason:** Was scoring every wallet every scan, wasting compute and Goldsky calls.
**Future note (inline TODO in scan.js):** Upgrade to activity-based cooldown — re-score only wallets where `lastTradeTs > lastScored`.

### Commit `f3125c0` — effectivePnl = max(onChain, sample)
**Problem:** Dashboard showed many wallets with Sample PnL >> Lifetime (on-chain) PnL, which confused ranking.
**Two root causes identified:**
- **Unredeemed winners** (357/977 wallets, ~37%): Goldsky's `realizedPnl` only counts on-chain redemptions. Wallets that won but never redeemed showed $0 on-chain but positive sample PnL.
- **Truncated history** (210/977 wallets, ~21%): Analyzer samples max 3000 activity events, so high-volume wallets have sample PnL missing older wins/losses. On-chain catches these.
**Fix:**
- Added `stats.effectivePnl = Math.max(stats.totalPnl, stats.goldskyPnl)` on both new-qualify and re-score paths.
- Updated `pnlScore` in `dataApi.js` to use `effectivePnl`.
- Updated solo-signal gate in `lib.js` to use `effectivePnl`.
- Added 3-column PnL display on dashboard: **On-chain / Sample / Effective** with tooltips.
**Biggest beneficiary example:** wallet `0x081e1455` — sample PnL $213k, on-chain PnL $1.69M. Previously scored as a $213k wallet, now correctly scored as a $1.69M wallet.

---

## 3. Architecture Quick Reference

### Key files
| File | Responsibility |
|------|----------------|
| `scanner/scan.js` | Main orchestration: fast loop, qualify loop, pool management, leaderboard, state |
| `scanner/signals.js` | Signal creation/update/close logic (consensus, cluster, solo paths + history repair) |
| `scanner/lib.js` | Wallet discovery, `analyzeTradeHistory`, Goldsky queries, paper trading, tier logic |
| `scanner/dataApi.js` | `computeWalletScore`, scoring components (winRate, pnlScore, recencyScore, etc.) |
| `scanner/repair.js` | Background workflow — closes resolved signals, syncs analytics |
| `frontend/app.js` + `frontend/index.html` | Dashboard — reads analytics.json.gz from GitHub raw CDN |

### Signal paths in signals.js
- **Phase 1 — Consensus/cluster:** Groups wallets betting same direction on same market; fires when ≥3 qualifying wallets align.
- **Phase 1b — Solo:** Single high-quality wallet (score ≥ threshold, effective PnL ≥ SOLO_MIN_PNL) making a notable play.
- **Phase 2 — Update/close:** Tracks exits, price movements, resolution for currently active signals.
- **Phase 2.5 — History repair:** Backfills resolution status for signals whose markets have since resolved.

### Data files (all gzipped JSON at `data/`)
- `wallets.json.gz` → `{ metadata, pool: { [address]: { stats, score, goldskyPnl, lastScored, ... } } }`
- `signals.json.gz` → `{ active: {[id]: signal}, history: [...], stats }`
- `analytics.json.gz` → Dashboard-consumed, has leaderboard + stats (CDN-served from raw.githubusercontent.com)
- `markets.json.gz` → `marketLookup` cache (tokenId → market metadata)
- `state.json` → scan counter, cursor, Goldsky schema, totals
- `wallet-history.json.gz` → historical pool composition

### GHA workflows
- **scan.yml** — runs `scanner/scan.js`, 80-min timeout, concurrency group `scanner`, cron scheduled every ~80 min
- **repair.yml** — runs `scanner/repair.js` every :20 and :50 (closes resolved signals, syncs analytics)

### Scoring components (dataApi.js)
- `recentWrScore` (30), `allTimeWrScore` (10), `pnlScore` (15), `consistencyScore` (15), `activityScore` (15), `edgeScore` (15)
- `computeWalletScore(stats)` sums components; returns number 0–100
- **edgeScore** now uses `stats.roiEdgeRatio` (pct-based). `null` → 3pts (neutral on no-loss wallets); ≤0.15 → 0pts (scrap-graders); otherwise `log2(1 + (roiEdgeRatio - 0.15)) / 3 × 15`, capped.

### New per-trade ROI stats (shipped — scoring + qualify gates)
Added to `analyzeTradeHistory` return object:
- `avgEntryPrice` — capital-weighted avg buy price across resolved markets
- `avgTradeRoi` — pnl-weighted avg ROI per resolved market (cost = avgBuyPrice × buySize)
- `avgWinRoi`, `avgLossRoi` — ROI averages partitioned by outcome
- `roiEdgeRatio` — `avgWinRoi / avgLossRoi`; `null` if no losses or no wins
- `edgeRatio` (dollar-denominated) — kept for display; bug fix: fallback now `null` (was `1`, which gave zero-loss wallets bogus huge edgeRatios)

### Score / qualify thresholds (current)
- `MIN_QUALIFY_SCORE`: 60
- `MAX_WALLET_AVG_ENTRY_PRICE`: 0.85 (Option B) — wallets whose capital-weighted avg entry > 85¢ are rejected at qualify and evicted on re-score. Filters scrap-graders even when WR is high.
- `MIN_RESOLVED_MARKETS`: required for qualify
- `MIN_PNL_DISCOVERY`: Goldsky-PnL floor for new candidates
- `SOLO_MIN_PNL`: threshold for solo signal eligibility (uses effectivePnl)

### Signal filters
- `MIN_ENTRY_PRICE`: avg entry price floor (currently 0 = disabled)
- `MIN_WALLET_ROI`: wallet's avg fill must have ≥N% implied max ROI (currently 0.15 → ≤0.8696 price ceiling)
- `MIN_OPEN_ROI`: live market price at publish must have ≥N% implied max ROI (currently 0.15)
- Replaced the old `MAX_ENTRY_PRICE` / `MAX_OPEN_PRICE` 0.95 gates. ROI = (1/price - 1).
- Works symmetrically for YES/NO — a NO buy at 10¢ shows as 900% max ROI and passes cleanly; a YES buy at 90¢ shows as 11.1% and is blocked.

---

## 4. Verification Script

**File:** `verify-changes.mjs` at repo root. Run with `node verify-changes.mjs`.

Checks:
1. No active signal has `openMarketPrice=0` (commit 3b7884d)
2. Voided signals preserved + no resurrection (commit 90f3041)
3. No active solo signal above 95¢ (commit 60bc0bd) — **note: grandfathered pre-fix signals will flag; not a real bug**
4. `effectivePnl` wiring on scored wallets (commit f3125c0) — **note: 0% until re-scoring picks up; ~70% populated within 24h**
5. Impact preview (wallets gaining score from effectivePnl)
6. Active signal health
7. Pool health

Exit code 0 if all pass, 1 otherwise.

---

## 5. Open / Future Tasks

### Short-term watch items
- Monitor leaderboard reshuffle as `effectivePnl` finishes populating (~24h)
- Watch for new solo signals from previously-truncated high-volume wallets (e.g. `0x081e1455`)
- Consider raising `MIN_QUALIFY_SCORE` 60 → 62 once effectivePnl fully populated (tightens floor without needing pool-size bump)

### Decided NOT to do right now
- **Increase `MAX_POOL_SIZE` above 1000** — bottleneck is market efficiency, not coverage. Pool self-upgrading via ranking-based eviction. Revisit if signal volume drops below ~1/day for a week, or if qualify loop consistently produces 400+ wallets/scan.

### Future implementations (prioritized)
1. **Option A — capital-weighted avgTradeRoi scoring component** (deferred from this session). edgeScore already reacts to `roiEdgeRatio`, but a dedicated component that rewards high `avgTradeRoi` (capital-weighted) would more directly order wallets by per-trade $ efficiency. Would also let us downweight the raw WR components so a 95% WR on 5% wins stops outranking a 50% WR on 4x wins.
2. **Activity-based re-score cooldown** — re-score only when `lastTradeTs > lastScored` (currently TODO comment in scan.js)
2. **Goldsky statement-timeout handling** — current discovery caps at ~850k–1M positions vs 2M target
3. **Gamma market-resolution capping** — currently 150 per wallet; may miss valid signals in high-activity markets
4. **Review Elite tier threshold** — sample size too small, noisy
5. **Kill pro/cluster <35¢ cohort** — 0W/14L historical
6. **Kill consensus >50¢ cohort** — poor EV
7. **Add "adjusted WR" to dashboard** — WR excluding void + truncation-adjusted

### Dashboard improvements
- 3-column PnL (On-chain/Sample/Effective) shipped with tooltips
- "Lifetime PnL" renamed "On-chain" to reduce confusion
- colspan updated 9→10

---

## 6. Git Workflow Notes

- Push rejections are common — remote usually has newer scan commits from GHA
- Resolution: `git pull --rebase --autostash origin main`
- For binary conflicts on data files: `git checkout --theirs data/` (trust remote's latest scan data)
- After resolving: `git rebase --continue && git push`

---

## 7. Prompt To Paste Into New Chat

Copy-paste this into your new Cowork chat:

---

> I'm continuing work on the Polymarket Signal Engine (`Br4cky/polymarket-wallet-engine`, main branch). Please read `HANDOFF.md` in the workspace folder first — it has full context on where we are, recent fixes, architecture, and open tasks.
>
> **Quick state summary:**
> - Pool at 1000 wallets (MAX_POOL_SIZE cap), avg score 71.4, floor 60
> - 41 active signals, 73.7% WR on 479 resolved
> - Latest commit `f3125c0` added `effectivePnl = max(onChain, sample)` for scoring; 268/1000 wallets populated so far (rest populate as they hit 24h re-score window)
> - Last scan was #160 at 2026-04-14T21:48Z; repair workflow runs every :20 and :50
>
> **Recent fixes (all shipped):** qualification crash (`8c79b95`), missing-price signals (`3b7884d`), solo 95¢ filter (`60bc0bd`), voided-signal resurrection (`90f3041`), re-score cooldowns (`b050fb3`), effectivePnl scoring (`f3125c0`).
>
> **Verification script:** `node verify-changes.mjs` at repo root — 7 checks, green/red output. Two expected "failures" are covered in HANDOFF.md (grandfathered solo signals + gradual effectivePnl rollout).
>
> **Next things on the list:**
> 1. Watch leaderboard reshuffle as effectivePnl finishes populating (~24h)
> 2. Consider raising `MIN_QUALIFY_SCORE` 60 → 62 once rollout complete
> 3. Future: activity-based re-score cooldown (inline TODO in scan.js)
> 4. Future: tighter EV filter cohorts (kill pro/cluster <35¢, consensus >50¢)
>
> Don't increase `MAX_POOL_SIZE` — already decided against it; see HANDOFF.md section 5 for reasoning.
>
> Please pull latest before any work: `git fetch origin main && git pull --rebase --autostash origin main`. Then ask me what I want to tackle first.

---

*End of handoff.*
