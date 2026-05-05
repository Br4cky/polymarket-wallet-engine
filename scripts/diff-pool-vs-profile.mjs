#!/usr/bin/env node
/**
 * Run the CLOB-resolved profile-matching diff against every active wallet in
 * the pool. Validates that the lifetime PnL numbers stored in
 * data/wallets.json.gz (post full-pool-rescore) match what an independent
 * recomputation produces. Use this to:
 *
 *   1. Audit any wallet whose dashboard PnL "looks wrong" — the diff
 *      number is the ground truth (it's what the public profile shows).
 *   2. Spot wallets where the cached stats.totalPnl drifts meaningfully
 *      from the recomputation, which would indicate a bug somewhere.
 *
 * Output:
 *   - per-wallet line: address, cached PnL, real PnL, delta, WR, ROI
 *   - sorted worst-real-PnL first so the negative outliers surface
 *   - summary footer with pool aggregates and any large divergences
 *
 * Runtime: ~15-25 min for ~280 active wallets at 120ms CLOB throttle.
 *
 * Usage:  node scripts/diff-pool-vs-profile.mjs   [--top N]   [--addr 0x…]
 */
import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const { fetchAllActivity } = await import(path.join(ROOT, 'scanner/dataApi.js'));

// CLI flags
const argv = process.argv.slice(2);
let limit = null;
let onlyAddr = null;
for (let i = 0; i < argv.length; i++) {
  if (argv[i] === '--top' && argv[i+1]) { limit = parseInt(argv[++i]); }
  else if (argv[i] === '--addr' && argv[i+1]) { onlyAddr = argv[++i].toLowerCase(); }
}

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const pool = walletsData.pool || walletsData;

let entries = Object.entries(pool).filter(([, w]) => w?.status !== 'removed');
if (onlyAddr) entries = entries.filter(([a]) => a.toLowerCase() === onlyAddr);
if (limit) entries = entries.slice(0, limit);

console.log(`Loaded ${entries.length} active wallets (of ${Object.keys(pool).length} total)\n`);

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function fetchClobMarket(cid, attempt = 0) {
  try {
    const res = await fetch(`https://clob.polymarket.com/markets/${cid}`);
    if (res.status === 429 || res.status >= 500) {
      if (attempt < 3) { await sleep(500 * (attempt + 1)); return fetchClobMarket(cid, attempt + 1); }
      return null;
    }
    if (!res.ok) return null;
    return await res.json();
  } catch {
    if (attempt < 2) { await sleep(500); return fetchClobMarket(cid, attempt + 1); }
    return null;
  }
}

/**
 * Mirror the diff-wallet-vs-profile logic: build per-market ledger from
 * /activity, resolve every market via CLOB, classify each position
 * (sold_out / redeemed / synthetic_win / synthetic_loss / open), sum PnL.
 */
async function diffWallet(addr) {
  const events = await fetchAllActivity(addr, { maxEvents: 10000 });
  if (!Array.isArray(events) || events.length === 0) return null;

  const conditionIds = [...new Set(events.map(e => e.conditionId).filter(Boolean))];
  const lookup = new Map();
  let clobFails = 0;
  for (const cid of conditionIds) {
    const m = await fetchClobMarket(cid);
    if (!m) { clobFails++; await sleep(120); continue; }
    let winningOutcome = null;
    if (Array.isArray(m.tokens)) {
      for (const tok of m.tokens) {
        if (tok.winner === true) { winningOutcome = tok.outcome || null; break; }
      }
      if (!winningOutcome) for (const tok of m.tokens) {
        if (parseFloat(tok.price || 0) >= 0.95) { winningOutcome = tok.outcome || null; break; }
      }
    }
    lookup.set(cid, { closed: m.closed === true, winningOutcome });
    await sleep(120);
  }

  // Per-market ledger
  const ledger = new Map();
  for (const e of events) {
    const cid = e.conditionId;
    if (!cid) continue;
    if (!ledger.has(cid)) ledger.set(cid, { buys: [], sells: [], redeems: [], outcome: e.outcome || '' });
    const m = ledger.get(cid);
    if (e.type === 'TRADE' && e.side === 'BUY') m.buys.push(e);
    else if (e.type === 'TRADE' && e.side === 'SELL') m.sells.push(e);
    else if (e.type === 'REDEEM') m.redeems.push(e);
  }

  let wins = 0, losses = 0, openCount = 0;
  let totalWagered = 0, totalReturned = 0, totalPnl = 0;
  for (const [cid, m] of ledger) {
    const totalBought = m.buys.reduce((s, t) => s + t.size * t.price, 0);
    const totalSold   = m.sells.reduce((s, t) => s + t.size * t.price, 0);
    const totalBuySize  = m.buys.reduce((s, t) => s + t.size, 0);
    const totalSellSize = m.sells.reduce((s, t) => s + t.size, 0);
    const redeemSize    = m.redeems.reduce((s, t) => s + (t.size || 0), 0);
    const redeemUsdc    = m.redeems.reduce((s, t) => s + (t.usdcSize || t.payout || 0), 0);
    if (totalBuySize === 0) continue;

    const closedShares = totalSellSize + redeemSize;
    const sellsClose = closedShares >= totalBuySize * 0.95;
    const g = lookup.get(cid) || {};

    let pnl, returned, won = null;
    if (sellsClose) {
      returned = totalSold + redeemUsdc;
      pnl = returned - totalBought;
      won = pnl > 0;
    } else if (g.closed && g.winningOutcome) {
      const walletWon = String(m.outcome || '').toLowerCase().trim() === String(g.winningOutcome).toLowerCase().trim();
      const unredeemed = Math.max(0, totalBuySize - totalSellSize - redeemSize);
      returned = totalSold + redeemUsdc + (walletWon ? unredeemed * 1.0 : 0);
      pnl = returned - totalBought;
      won = walletWon;
    } else {
      openCount++;
      continue;
    }

    totalPnl += pnl;
    totalWagered += totalBought;
    totalReturned += returned;
    if (won) wins++;
    else losses++;
  }

  const resolved = wins + losses;
  return {
    events: events.length,
    markets: ledger.size,
    open: openCount,
    resolved,
    wins, losses,
    wr: resolved > 0 ? wins / resolved : null,
    totalPnl,
    totalWagered,
    totalReturned,
    roi: totalWagered > 0 ? (totalReturned - totalWagered) / totalWagered : null,
    clobFails,
  };
}

const rows = [];
for (let i = 0; i < entries.length; i++) {
  const [addr, w] = entries[i];
  const cachedPnl = w.stats?.totalPnl ?? null;
  process.stdout.write(`[${i+1}/${entries.length}] ${addr.slice(0, 12)}…  `);
  try {
    const real = await diffWallet(addr);
    if (!real) { console.log('NO_EVENTS'); continue; }
    const delta = cachedPnl != null ? real.totalPnl - cachedPnl : null;
    rows.push({ addr, cachedPnl, ...real, delta });
    const wrStr = real.wr == null ? '—' : (real.wr * 100).toFixed(0) + '%';
    const roiStr = real.roi == null ? '—' : (real.roi * 100).toFixed(0) + '%';
    console.log(`real=$${real.totalPnl.toFixed(0).padStart(8)}  cached=$${(cachedPnl||0).toFixed(0).padStart(8)}  Δ=${(delta||0).toFixed(0).padStart(7)}  ${real.wins}W/${real.losses}L  WR=${wrStr}  ROI=${roiStr}`);
  } catch (e) {
    console.log(`FAILED: ${e.message}`);
  }
}

// ── Report ─────────────────────────────────────────────────────────────
console.log('\n');
console.log('═'.repeat(140));
console.log('POOL-WIDE PROFILE DIFF SUMMARY');
console.log('═'.repeat(140));

const w = (s, n) => String(s == null ? '' : s).slice(0, n).padEnd(n);
const wr = (s, n) => String(s == null ? '' : s).slice(0, n).padStart(n);

// Sort: worst real PnL first — what the user asked to see
rows.sort((a, b) => a.totalPnl - b.totalPnl);

console.log('\n── 25 worst-PnL wallets (real, CLOB-resolved) ──');
console.log(
  w('addr', 14) +
  wr('mkts', 6) + ' ' +
  wr('W', 5) + ' ' +
  wr('L', 5) + ' ' +
  wr('WR', 7) + ' ' +
  wr('wagered$', 12) +
  wr('real PnL', 12) +
  wr('cached PnL', 12) +
  wr('Δ', 10) +
  wr('ROI', 8)
);
console.log('-'.repeat(140));
for (const r of rows.slice(0, 25)) {
  const wrStr = r.wr == null ? '—' : (r.wr * 100).toFixed(0) + '%';
  const roiStr = r.roi == null ? '—' : (r.roi * 100).toFixed(0) + '%';
  console.log(
    w(r.addr.slice(0, 12), 14) +
    wr(r.markets, 6) + ' ' +
    wr(r.wins, 5) + ' ' +
    wr(r.losses, 5) + ' ' +
    wr(wrStr, 7) +
    wr('$' + r.totalWagered.toFixed(0), 12) +
    wr('$' + r.totalPnl.toFixed(0), 12) +
    wr('$' + (r.cachedPnl||0).toFixed(0), 12) +
    wr((r.delta||0).toFixed(0), 10) +
    wr(roiStr, 8)
  );
}

// Aggregate
const totalReal = rows.reduce((s, r) => s + r.totalPnl, 0);
const totalCached = rows.reduce((s, r) => s + (r.cachedPnl || 0), 0);
const totalWagered = rows.reduce((s, r) => s + r.totalWagered, 0);
const negativeReal = rows.filter(r => r.totalPnl < 0).length;
console.log('-'.repeat(140));
console.log(`\n── Aggregate ──`);
console.log(`  Wallets analysed:           ${rows.length}`);
console.log(`  Negative real PnL:          ${negativeReal}  (${(negativeReal/rows.length*100).toFixed(1)}%)`);
console.log(`  Pool real PnL (sum):        $${totalReal.toFixed(0)}`);
console.log(`  Pool cached PnL (sum):      $${totalCached.toFixed(0)}`);
console.log(`  Total wagered (sum):        $${totalWagered.toFixed(0)}`);
console.log(`  Pool ROI (real):            ${(totalReal / totalWagered * 100).toFixed(2)}%`);

// Largest discrepancies between cached and real — bug-finder
const withDelta = rows.filter(r => r.cachedPnl != null);
withDelta.sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));
console.log('\n── 15 largest cached-vs-real discrepancies (bug detector) ──');
for (const r of withDelta.slice(0, 15)) {
  const dir = r.delta > 0 ? 'real > cached' : 'real < cached';
  console.log(`  ${r.addr.slice(0, 12)}  cached=$${(r.cachedPnl||0).toFixed(0).padStart(8)}  real=$${r.totalPnl.toFixed(0).padStart(8)}  Δ=$${r.delta.toFixed(0).padStart(8)}  (${dir})`);
}
console.log();
