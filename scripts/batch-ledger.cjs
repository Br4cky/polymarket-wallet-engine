#!/usr/bin/env node
/**
 * Batch ground-truth analysis for the tracked wallet pool.
 *
 * Runs the position-centric ledger across a slice of the pool and emits
 * a CSV so we can eyeball: does our current `score` track actual
 * decided-ROI, or are we putting noise wallets in the ranked list?
 *
 * Usage:
 *   node scripts/batch-ledger.cjs                        # top 25 by rank
 *   node scripts/batch-ledger.cjs --top 50               # top 50 by rank
 *   node scripts/batch-ledger.cjs --bottom 25            # bottom 25 by rank
 *   node scripts/batch-ledger.cjs --sample 50            # random sample of 50
 *   node scripts/batch-ledger.cjs --concurrency 3        # parallel lanes (default 2)
 *   node scripts/batch-ledger.cjs --out out/batch.csv    # custom output path
 *
 * Output CSV columns:
 *   rank, address, engineScore, engineWR, engineTotalPnl, engineResolved,
 *   truePositions, trueWins, trueLosses, trueOpen, trueScratch, trueUnknown,
 *   trueWR, realizedPnl, decidedPnl, truePnl, totalCapital, decidedCapital,
 *   openCapitalAtRisk, roi, decidedROI, gammaCalls, error
 */
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { analyzeWallet } = require('./wallet-ledger.cjs');

const args = process.argv.slice(2);
function arg(name, def) {
  const i = args.indexOf(name);
  return i !== -1 && args[i + 1] ? args[i + 1] : def;
}
function flag(name) { return args.includes(name); }

const MODE = flag('--bottom') ? 'bottom' : flag('--sample') ? 'sample' : 'top';
const N = parseInt(arg('--top') || arg('--bottom') || arg('--sample') || '25', 10);
const CONCURRENCY = parseInt(arg('--concurrency', '2'), 10);
const OUT = arg('--out', `out/batch-ledger-${MODE}-${N}-${Date.now()}.csv`);

function loadGz(p) { return JSON.parse(zlib.gunzipSync(fs.readFileSync(p))); }

function pickWallets(pool) {
  const entries = Object.values(pool).filter(w => w.address && typeof w.rank === 'number');
  entries.sort((a, b) => a.rank - b.rank);
  if (MODE === 'top') return entries.slice(0, N);
  if (MODE === 'bottom') return entries.slice(-N).reverse();
  // sample: shuffle + slice
  const shuffled = entries.slice().sort(() => Math.random() - 0.5);
  return shuffled.slice(0, N);
}

function csvEscape(v) {
  if (v == null) return '';
  const s = String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

async function withConcurrency(items, limit, worker) {
  const results = new Array(items.length);
  let cursor = 0;
  const lanes = Array(Math.min(limit, items.length)).fill(0).map(async () => {
    while (true) {
      const i = cursor++;
      if (i >= items.length) return;
      results[i] = await worker(items[i], i);
    }
  });
  await Promise.all(lanes);
  return results;
}

(async function main() {
  const walletsFile = loadGz('data/wallets.json.gz');
  const pool = walletsFile.pool || {};
  const picked = pickWallets(pool);
  console.error(`Running ledger on ${picked.length} wallets (mode=${MODE}, concurrency=${CONCURRENCY})`);

  // Preload marketLookup once
  let marketLookup = new Map();
  try {
    const analytics = loadGz('data/analytics.json.gz');
    for (const [tid, m] of Object.entries(analytics.marketLookup || {})) marketLookup.set(tid, m);
    console.error(`Loaded ${marketLookup.size} markets from analytics.json.gz`);
  } catch {
    console.error('No local marketLookup; will hit Gamma for every still-held position.');
  }

  const rows = await withConcurrency(picked, CONCURRENCY, async (w, idx) => {
    const started = Date.now();
    console.error(`[${idx + 1}/${picked.length}] rank=${w.rank} ${w.address} ...`);
    try {
      const res = await analyzeWallet(w.address.toLowerCase(), { marketLookup, quiet: true });
      const a = res.aggregates;
      const elapsed = ((Date.now() - started) / 1000).toFixed(1);
      console.error(
        `    ${a.total} pos | ${a.wins}W/${a.losses}L | ` +
        `ROI ${a.roi != null ? (a.roi * 100).toFixed(1) + '%' : 'n/a'} | ` +
        `decROI ${a.decidedROI != null ? (a.decidedROI * 100).toFixed(1) + '%' : 'n/a'} | ` +
        `${elapsed}s`
      );
      return { wallet: w, aggregates: a, gammaCalls: res.gammaCalls, error: null };
    } catch (err) {
      console.error(`    FAILED: ${err.message.slice(0, 120)}`);
      return { wallet: w, aggregates: null, gammaCalls: 0, error: err.message.slice(0, 200) };
    }
  });

  // Write CSV
  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  const headers = [
    'rank', 'address', 'engineScore', 'engineWR', 'engineTotalPnl', 'engineResolved',
    'truePositions', 'trueWins', 'trueLosses', 'trueOpen', 'trueScratch', 'trueUnknown',
    'trueWR', 'realizedPnl', 'decidedPnl', 'truePnl',
    'totalCapital', 'decidedCapital', 'openCapitalAtRisk',
    'roi', 'decidedROI', 'gammaCalls', 'error',
  ];
  const lines = [headers.join(',')];
  for (const r of rows) {
    const w = r.wallet;
    const a = r.aggregates;
    const row = [
      w.rank,
      w.address,
      w.score?.toFixed ? w.score.toFixed(2) : w.score,
      w.stats?.winRate != null ? (w.stats.winRate * 100).toFixed(2) : '',
      w.stats?.totalPnl?.toFixed ? w.stats.totalPnl.toFixed(2) : '',
      w.stats?.resolvedMarkets ?? '',
      a?.total ?? '',
      a?.wins ?? '',
      a?.losses ?? '',
      a?.open ?? '',
      a?.scratch ?? '',
      (a ? (a.closedUndetermined + a.unresolvedMarket) : ''),
      a?.winRate != null ? (a.winRate * 100).toFixed(2) : '',
      a?.realizedPnl ?? '',
      a?.decidedUnredeemedPnl ?? '',
      a?.truePnl ?? '',
      a?.totalCapitalDeployed ?? '',
      a?.decidedCapitalDeployed ?? '',
      a?.openCapitalAtRisk ?? '',
      a?.roi != null ? (a.roi * 100).toFixed(2) : '',
      a?.decidedROI != null ? (a.decidedROI * 100).toFixed(2) : '',
      r.gammaCalls,
      r.error || '',
    ].map(csvEscape);
    lines.push(row.join(','));
  }
  fs.writeFileSync(OUT, lines.join('\n'));

  const ok = rows.filter(r => r.aggregates).length;
  const fail = rows.length - ok;
  console.error('');
  console.error(`=== Summary ===`);
  console.error(`Wallets analysed: ${ok} ok / ${fail} failed`);
  console.error(`CSV: ${OUT}`);

  // Quick headline sort — engine score vs decided ROI mismatches
  const good = rows.filter(r => r.aggregates);
  if (good.length > 0) {
    console.error('');
    console.error('Top 10 by decided ROI (resolved-only truth):');
    const byDecROI = good.slice().sort((a, b) => (b.aggregates.decidedROI || -Infinity) - (a.aggregates.decidedROI || -Infinity));
    for (const r of byDecROI.slice(0, 10)) {
      const a = r.aggregates;
      console.error(
        `  rank=${String(r.wallet.rank).padStart(4)} score=${String(r.wallet.score?.toFixed ? r.wallet.score.toFixed(1) : r.wallet.score).padStart(6)}  ` +
        `decROI=${(a.decidedROI * 100).toFixed(1).padStart(7)}%  WR=${(a.winRate * 100).toFixed(1)}%  ` +
        `${a.wins}W/${a.losses}L  cap=$${a.decidedCapitalDeployed}  ${r.wallet.address}`
      );
    }
    console.error('');
    console.error('Bottom 10 by decided ROI (wallets we may be wrongly ranking):');
    for (const r of byDecROI.slice(-10).reverse()) {
      const a = r.aggregates;
      console.error(
        `  rank=${String(r.wallet.rank).padStart(4)} score=${String(r.wallet.score?.toFixed ? r.wallet.score.toFixed(1) : r.wallet.score).padStart(6)}  ` +
        `decROI=${(a.decidedROI * 100).toFixed(1).padStart(7)}%  WR=${(a.winRate * 100).toFixed(1)}%  ` +
        `${a.wins}W/${a.losses}L  cap=$${a.decidedCapitalDeployed}  ${r.wallet.address}`
      );
    }
  }
})().catch(err => {
  console.error('BATCH FAILED:', err);
  process.exit(1);
});
