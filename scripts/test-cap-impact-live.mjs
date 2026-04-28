// Live test of the 150-cap's impact on scoring.
//
// For N heavily-capped wallets:
//   1. Fetch their positions live via Polymarket Data API /positions
//      (no cap on this endpoint — returns ALL positions)
//   2. Fetch market metadata for EVERY tokenId in those positions
//      via Gamma /markets (UNCAPPED — no 150 limit)
//   3. Run aggregatePositions(fullPositions, fullMarketLookup)
//   4. Compare the freshly-computed decidedROI / decidedCapital against
//      the cached values in wallets.json.gz
//   5. Report the score-shift estimate per wallet
//
// Usage:
//   node scripts/test-cap-impact-live.mjs                # 5 wallets, default
//   node scripts/test-cap-impact-live.mjs --count 10     # more wallets
//   node scripts/test-cap-impact-live.mjs --batch 25     # tokens per Gamma batch
//
// Runtime: roughly N × (positions / batch) × 100ms
// e.g. 5 wallets × 800 positions / 50-token batch × 100ms ≈ 80 seconds

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';
import { aggregatePositions } from '../scanner/positionLedger.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const args = process.argv.slice(2);
const get = (f, d) => { const i = args.indexOf(f); return i >= 0 && i + 1 < args.length ? args[i + 1] : d; };
const COUNT = parseInt(get('--count', '5'), 10);
const BATCH = parseInt(get('--batch', '8'), 10);  // parallel single-token queries

const GAMMA_MARKETS = 'https://gamma-api.polymarket.com/markets';
const POLYMARKET_DATA_API = 'https://data-api.polymarket.com';

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const pool = walletsData.pool || walletsData;

// Pick top N capped wallets by open-position count
const candidates = [];
for (const [addr, w] of Object.entries(pool)) {
  if (!w || typeof w !== 'object' || w.status === 'removed') continue;
  const op = w.stats?.openPositions || w.stats?.decidedOpenPositions || 0;
  if (op > 200) candidates.push({ addr, openPos: op, score: w.score, stats: w.stats });
}
candidates.sort((a, b) => b.openPos - a.openPos);
const targets = candidates.slice(0, COUNT);

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Live cap-impact test — re-fetch ALL positions/markets, recompute');
console.log('═══════════════════════════════════════════════════════════════════');
console.log('  Testing ' + targets.length + ' heavily-capped wallets (>200 open positions)');
console.log('  Gamma concurrency: ' + BATCH + ' parallel single-token queries');
console.log('  Expected runtime: ~' + (targets.length * 60 * 0.5).toFixed(0) + ' seconds');
console.log();

// ── Helpers ──────────────────────────────────────────────────────────
async function fetchPositions(walletAddr) {
  const url = `${POLYMARKET_DATA_API}/positions?user=${walletAddr.toLowerCase()}&limit=2000`;
  try {
    const r = await fetch(url);
    if (!r.ok) return null;
    const arr = await r.json();
    return Array.isArray(arr) ? arr : null;
  } catch (e) { return null; }
}

// Single-token query — matches the production pattern in lib.js that we know works.
async function fetchMarketSingle(tokenId) {
  const url = `${GAMMA_MARKETS}?clob_token_ids=${tokenId}&limit=1`;
  try {
    const r = await fetch(url);
    if (!r.ok) return null;
    const arr = await r.json();
    return Array.isArray(arr) && arr.length > 0 ? arr[0] : null;
  } catch (e) { return null; }
}

// Concurrent batch — N parallel single-token queries
async function fetchMarketBatchParallel(tokenIds, concurrency = 8) {
  const results = new Map();
  for (let i = 0; i < tokenIds.length; i += concurrency) {
    const slice = tokenIds.slice(i, i + concurrency);
    const fetched = await Promise.all(slice.map(tid => fetchMarketSingle(tid).then(m => [tid, m])));
    for (const [tid, m] of fetched) {
      if (m) results.set(tid, m);
    }
  }
  return results;
}

function adaptGammaMarketToLookupEntry(m) {
  if (!m) return null;
  const closed = m.closed === true || m.closed === 'true';
  const endDate = m.end_date_iso || m.endDate || null;
  let winningOutcome = null;
  if (closed) {
    if (Array.isArray(m.tokens)) {
      for (const t of m.tokens) {
        if (parseFloat(t.price || 0) >= 0.95) { winningOutcome = t.outcome; break; }
      }
    }
    if (!winningOutcome) {
      let prices = m.outcomePrices;
      if (typeof prices === 'string') { try { prices = JSON.parse(prices); } catch {} }
      let outs = m.outcomes;
      if (typeof outs === 'string') { try { outs = JSON.parse(outs); } catch {} }
      if (Array.isArray(prices) && Array.isArray(outs)) {
        for (let i = 0; i < prices.length; i++) {
          if (parseFloat(prices[i]) >= 0.95) { winningOutcome = outs[i]; break; }
        }
      }
    }
  }
  // Pick the currentPrice for "our" token — average if multiple, just use first
  let currentPrice = null;
  if (Array.isArray(m.tokens) && m.tokens.length > 0) {
    currentPrice = parseFloat(m.tokens[0].price || 0);
  } else if (m.outcomePrices) {
    let p = m.outcomePrices;
    if (typeof p === 'string') { try { p = JSON.parse(p); } catch {} }
    if (Array.isArray(p) && p.length > 0) currentPrice = parseFloat(p[0]);
  }
  return {
    title: m.question || m.title || '',
    slug: m.slug || '',
    marketClosed: closed,
    winningOutcome,
    endDate,
    currentPrice,
  };
}

function adaptDataApiPosition(p) {
  // Polymarket Data API position shape may differ — normalize to what
  // aggregatePositions expects: { tokenId, sharesHeld, avgPrice, realizedPnl, totalBought }
  return {
    tokenId: String(p.asset || p.tokenId || ''),
    sharesHeld: parseFloat(p.size || p.amount || 0),
    avgPrice: parseFloat(p.avgPrice || p.entry || 0),
    realizedPnl: parseFloat(p.realizedPnl || 0),
    totalBought: parseFloat(p.initialValue || (parseFloat(p.size || p.amount || 0) * parseFloat(p.avgPrice || p.entry || 0)) || 0),
  };
}

// roiPoints mirrors dataApi.js
function roiPoints(roi) {
  if (roi == null || !isFinite(roi) || roi <= 0) return 0;
  return 50 * (1 - Math.exp(-roi * 3));
}

// ── Process each wallet ──────────────────────────────────────────────
const results = [];

for (let idx = 0; idx < targets.length; idx++) {
  const t = targets[idx];
  console.log(`\n  [${idx + 1}/${targets.length}] ${t.addr.slice(0, 14)}…  current open=${t.openPos}  score=${t.score}`);

  // 1. Fetch positions live
  const startFetch = Date.now();
  const rawPositions = await fetchPositions(t.addr);
  if (!rawPositions || rawPositions.length === 0) {
    console.log(`    ✗ No positions returned from Data API`);
    continue;
  }
  console.log(`    Fetched ${rawPositions.length} positions in ${((Date.now() - startFetch) / 1000).toFixed(1)}s`);

  const positions = rawPositions.map(adaptDataApiPosition).filter(p => p.tokenId);
  const uniqueTokenIds = [...new Set(positions.map(p => p.tokenId))];

  // 2. Fetch market metadata for ALL tokens (per-token, parallel batches)
  const startMarkets = Date.now();
  const rawMarkets = await fetchMarketBatchParallel(uniqueTokenIds, BATCH);
  const marketLookup = new Map();
  for (const [tid, m] of rawMarkets) {
    const entry = adaptGammaMarketToLookupEntry(m);
    if (entry) marketLookup.set(tid, entry);
  }
  console.log(`    Resolved ${marketLookup.size}/${uniqueTokenIds.length} markets in ${((Date.now() - startMarkets) / 1000).toFixed(1)}s`);
  if (marketLookup.size === 0) {
    console.log(`    ✗ No markets resolved — Gamma returning empty. Skipping wallet.`);
    continue;
  }

  // 3. Aggregate with FULL market data
  const freshAgg = aggregatePositions(positions, marketLookup);

  // 4. Compare
  const cached = {
    decidedCapital: t.stats.decidedCapital ?? null,
    decidedPnl: t.stats.decidedPnl ?? null,
    decidedROI: t.stats.decidedROI ?? null,
    decidedWins: t.stats.decidedWins ?? null,
    decidedLosses: t.stats.decidedLosses ?? null,
  };
  const fresh = {
    decidedCapital: freshAgg.decidedCapital,
    decidedPnl: freshAgg.decidedPnl,
    decidedROI: freshAgg.decidedROI,
    decidedWins: freshAgg.wins,
    decidedLosses: freshAgg.losses,
  };

  // Estimate score shift via roiPoints delta
  const oldRoiPts = roiPoints(cached.decidedROI);
  const newRoiPts = roiPoints(fresh.decidedROI);
  const roiPtsDelta = newRoiPts - oldRoiPts;
  // Scale by approximate other-multiplier ratio — score / oldRoiPts
  const ratio = oldRoiPts > 0 ? t.score / oldRoiPts : 1;
  const estNewScore = t.score + roiPtsDelta * ratio;

  console.log(`    Cached:  decidedROI=${cached.decidedROI != null ? (cached.decidedROI * 100).toFixed(1) + '%' : '—'}  cap=$${cached.decidedCapital ? Math.round(cached.decidedCapital) : '—'}  W/L=${cached.decidedWins}/${cached.decidedLosses}`);
  console.log(`    Fresh:   decidedROI=${fresh.decidedROI != null ? (fresh.decidedROI * 100).toFixed(1) + '%' : '—'}  cap=$${fresh.decidedCapital ? Math.round(fresh.decidedCapital) : '—'}  W/L=${fresh.decidedWins}/${fresh.decidedLosses}`);
  console.log(`    Δ ROI: ${fresh.decidedROI != null && cached.decidedROI != null ? (((fresh.decidedROI - cached.decidedROI) * 100).toFixed(1) + 'pp') : '—'}`);
  console.log(`    Estimated score: ${t.score.toFixed(1)} → ${estNewScore.toFixed(1)}  (Δ ${(estNewScore - t.score >= 0 ? '+' : '') + (estNewScore - t.score).toFixed(1)})`);

  results.push({ addr: t.addr, openPos: t.openPos, oldScore: t.score, newScore: estNewScore,
    oldROI: cached.decidedROI, newROI: fresh.decidedROI,
    oldCap: cached.decidedCapital, newCap: fresh.decidedCapital });
}

// ── Summary ──────────────────────────────────────────────────────────
console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  SUMMARY');
console.log('═══════════════════════════════════════════════════════════════════');
console.log(`  ${'Wallet'.padEnd(14)} ${'Open'.padStart(5)} ${'OldROI'.padStart(7)} ${'NewROI'.padStart(7)} ${'OldCap'.padStart(8)} ${'NewCap'.padStart(8)} ${'OldScore'.padStart(8)} ${'NewScore'.padStart(8)} ${'Shift'.padStart(6)}`);
console.log('  ' + '─'.repeat(80));
for (const r of results) {
  const shift = r.newScore - r.oldScore;
  console.log('  ' + r.addr.slice(0, 12).padEnd(14) +
    ' ' + String(r.openPos).padStart(5) +
    ' ' + (r.oldROI != null ? (r.oldROI * 100).toFixed(0) + '%' : '—').padStart(7) +
    ' ' + (r.newROI != null ? (r.newROI * 100).toFixed(0) + '%' : '—').padStart(7) +
    ' ' + ('$' + (r.oldCap ? (r.oldCap / 1000).toFixed(0) : '—') + 'k').padStart(8) +
    ' ' + ('$' + (r.newCap ? (r.newCap / 1000).toFixed(0) : '—') + 'k').padStart(8) +
    ' ' + r.oldScore.toFixed(1).padStart(8) +
    ' ' + r.newScore.toFixed(1).padStart(8) +
    ' ' + ((shift >= 0 ? '+' : '') + shift.toFixed(1)).padStart(6));
}
console.log();
const avgShift = results.reduce((s, r) => s + (r.newScore - r.oldScore), 0) / Math.max(1, results.length);
const bigShifts = results.filter(r => Math.abs(r.newScore - r.oldScore) > 5).length;
console.log(`  Average score shift: ${avgShift >= 0 ? '+' : ''}${avgShift.toFixed(1)} points`);
console.log(`  Wallets with |shift| > 5pts: ${bigShifts}/${results.length}`);
console.log();
console.log('  → If avg shift is small (<2pts), the cap is NOT materially distorting scores.');
console.log('  → If many wallets shift >5pts, cap should be raised or removed.\n');
