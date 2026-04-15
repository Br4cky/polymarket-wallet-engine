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
const CONCURRENCY = parseInt(arg('--concurrency', '1'), 10);
const TIMEOUT_SEC = parseInt(arg('--timeout', '240'), 10); // per wallet
const RESUME = arg('--resume'); // path to prior CSV to skip already-done wallets
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

function withTimeout(promise, seconds, label) {
  let t;
  const timer = new Promise((_, rej) => {
    t = setTimeout(() => rej(new Error(`timeout after ${seconds}s (${label})`)), seconds * 1000);
  });
  return Promise.race([promise, timer]).finally(() => clearTimeout(t));
}

function rowToCsv(r) {
  const w = r.wallet;
  const a = r.aggregates;
  return [
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
  ].map(csvEscape).join(',');
}

(async function main() {
  const walletsFile = loadGz('data/wallets.json.gz');
  const pool = walletsFile.pool || {};
  let picked = pickWallets(pool);

  // Resume: drop wallets we've already successfully processed in a
  // prior CSV (any row with a non-empty truePositions value).
  const alreadyDone = new Set();
  if (RESUME && fs.existsSync(RESUME)) {
    const txt = fs.readFileSync(RESUME, 'utf8');
    const rows = txt.split('\n').slice(1).filter(Boolean);
    for (const line of rows) {
      const cells = line.split(',');
      const addr = (cells[1] || '').toLowerCase();
      const truePositions = cells[6];
      if (addr && truePositions && truePositions !== '') alreadyDone.add(addr);
    }
    const before = picked.length;
    picked = picked.filter(w => !alreadyDone.has(w.address.toLowerCase()));
    console.error(`Resuming: skipping ${before - picked.length} already-done wallets from ${RESUME}`);
  }

  console.error(`Running ledger on ${picked.length} wallets (mode=${MODE}, concurrency=${CONCURRENCY}, per-wallet timeout=${TIMEOUT_SEC}s, GQL_MAX_INFLIGHT=${process.env.GQL_MAX_INFLIGHT || '4'})`);

  // Preload marketLookup once
  let marketLookup = new Map();
  try {
    const analytics = loadGz('data/analytics.json.gz');
    for (const [tid, m] of Object.entries(analytics.marketLookup || {})) marketLookup.set(tid, m);
    console.error(`Loaded ${marketLookup.size} markets from analytics.json.gz`);
  } catch {
    console.error('No local marketLookup; will hit Gamma for every still-held position.');
  }

  // Open CSV and stream rows as each wallet finishes, so a crash/hang
  // preserves everything we've already computed.
  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  const headers = [
    'rank', 'address', 'engineScore', 'engineWR', 'engineTotalPnl', 'engineResolved',
    'truePositions', 'trueWins', 'trueLosses', 'trueOpen', 'trueScratch', 'trueUnknown',
    'trueWR', 'realizedPnl', 'decidedPnl', 'truePnl',
    'totalCapital', 'decidedCapital', 'openCapitalAtRisk',
    'roi', 'decidedROI', 'gammaCalls', 'error',
  ];
  const csvStream = fs.createWriteStream(OUT);
  csvStream.write(headers.join(',') + '\n');
  console.error(`CSV: ${OUT}`);

  process.on('SIGINT', () => {
    console.error('\nSIGINT — flushing CSV and exiting');
    csvStream.end(() => process.exit(130));
  });

  const rows = await withConcurrency(picked, CONCURRENCY, async (w, idx) => {
    const started = Date.now();
    console.error(`[${idx + 1}/${picked.length}] rank=${w.rank} ${w.address} ...`);
    let row;
    try {
      const res = await withTimeout(
        analyzeWallet(w.address.toLowerCase(), { marketLookup, quiet: true }),
        TIMEOUT_SEC,
        w.address
      );
      const a = res.aggregates;
      const elapsed = ((Date.now() - started) / 1000).toFixed(1);
      console.error(
        `    ${a.total} pos | ${a.wins}W/${a.losses}L | ` +
        `ROI ${a.roi != null ? (a.roi * 100).toFixed(1) + '%' : 'n/a'} | ` +
        `decROI ${a.decidedROI != null ? (a.decidedROI * 100).toFixed(1) + '%' : 'n/a'} | ` +
        `${elapsed}s`
      );
      row = { wallet: w, aggregates: a, gammaCalls: res.gammaCalls, error: null };
    } catch (err) {
      const elapsed = ((Date.now() - started) / 1000).toFixed(1);
      console.error(`    FAILED after ${elapsed}s: ${err.message.slice(0, 120)}`);
      row = { wallet: w, aggregates: null, gammaCalls: 0, error: err.message.slice(0, 200) };
    }
    csvStream.write(rowToCsv(row) + '\n');
    return row;
  });

  await new Promise(res => csvStream.end(res));

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
