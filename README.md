# Polymarket Signal Engine

Automated system that continuously scans Polymarket's on-chain data to identify consistently profitable traders, detect consensus trades, and surface trading signals.

## Architecture

```
GitHub Actions (every 6hrs)  →  data/ committed to repo  →  GitHub raw CDN
                                                                  ↑
Netlify (static frontend)  ───── fetches JSON on page load ───────┘
```

- **Scanner** runs in GitHub Actions, commits data to repo
- **Frontend** is deployed once to Netlify, only rebuilds when frontend code changes
- **Data** is fetched live from GitHub's raw CDN — no Netlify rebuild on each scan

**Zero server costs. No API keys required. No wasted Netlify build minutes.**

## Quick Start

### 1. Create GitHub Repo

```bash
cd polymarket-signal-engine
git init
git add .
git commit -m "Initial commit"
gh repo create polymarket-signal-engine --public --push
```

### 2. Edit Config

Open `frontend/config.js` and set your GitHub username:

```js
githubUser: 'your-actual-username',
githubRepo: 'polymarket-signal-engine',
```

Commit and push:
```bash
git add frontend/config.js && git commit -m "set github config" && git push
```

### 3. Deploy Frontend to Netlify

- Go to [netlify.com](https://app.netlify.com) → "Add new site" → "Import from Git"
- Select your repo → Deploy
- Netlify only publishes the `frontend/` folder
- The `ignore` rule in `netlify.toml` skips rebuilds when only `data/` changes

### 4. Run First Scan

- Go to your repo → **Actions** → "Scan Polymarket Wallets" → **Run workflow**
- The scanner fetches ~200k positions, scores wallets, resolves market names
- Commits updated JSON to `data/` — your dashboard picks it up automatically
- Runs automatically every 6 hours after that

### 5. Verify

- Check the Actions tab for scan logs
- Visit your Netlify URL — dashboard loads data directly from GitHub

## How Data Flows

1. GitHub Actions runs `scanner/scan.js` every 6 hours
2. Scanner queries Polymarket subgraphs (public, no auth)
3. Scores wallets, computes consensus/patterns/signals
4. Commits `data/*.json` back to the repo
5. Frontend (on Netlify) fetches `data/analytics.json` etc. from `raw.githubusercontent.com`
6. No Netlify rebuild triggered — the `ignore` rule checks if `frontend/` changed

## Project Structure

```
frontend/
  config.js        ← SET YOUR GITHUB USERNAME HERE
  index.html       Dashboard SPA (6 tabs)
  app.js           Rendering logic, Chart.js charts, tables
  style.css        Dark theme, responsive

scanner/
  scan.js          Main scanner (runs in GitHub Actions)
  lib.js           Subgraph queries, scoring, analytics

data/
  state.json       Scan cursor + run metadata
  wallets.json     Wallet registry + positions
  markets.json     TokenId → market name cache
  analytics.json   Pre-computed dashboard data

.github/workflows/
  scan.yml         Every 6 hours + manual trigger

netlify.toml       Publishes frontend/ only, ignores data-only commits
```

## Dashboard Tabs

| Tab | What It Shows |
|-----|--------------|
| **Dashboard** | Leaderboard, score distribution, trendlines, key metrics |
| **Consensus** | Markets where 3+ smart wallets hold positions |
| **Portfolio** | All open positions across tracked wallets |
| **Patterns** | Win rate by position size, top winning markets |
| **Signals** | Biggest recent wins, hot wallets, emerging consensus |
| **Screener** | Manual on-demand browser scan (uses localStorage) |

## Scoring Formula (0-100)

| Component | Weight | Formula |
|-----------|--------|---------|
| Win Rate | 30 pts | `winRate * sqrt(resolved)/10 * 30` |
| Market Diversity | 20 pts | `min(1, markets/50) * 20` |
| Profit Efficiency | 20 pts | `min(1, pnl/volume / 0.10) * 20` |
| Edge Ratio | 15 pts | `min(1, (avgWin/avgLoss - 0.5) / 2.5) * 15` |
| Sample Size | 15 pts | `min(1, resolved/200) * 15` |

## Not Financial Advice

Past performance does not predict future results. This tool is for research and informational purposes only.
