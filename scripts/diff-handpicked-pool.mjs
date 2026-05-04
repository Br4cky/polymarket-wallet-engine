#!/usr/bin/env node
/**
 * Run the CLOB-resolved position analysis on every handpicked wallet and
 * compare to the cached stats currently sitting in handpicked-wallets.json.gz.
 *
 * The point: validate that the "Gamma misses niche markets → analyzer hides
 * abandoned losers → WR / PnL inflated" finding from the ieuei31 diff holds
 * across the whole handpicked pool. If it does, every wallet's TRUE WR will
 * be lower (often dramatically) than what we have cached.
 *
 * For each wallet, prints one summary line:
 *   - cached  W/L, WR, PnL  (what handpicked-wallets.json.gz currently has)
 *   - real    W/L, WR, PnL  (recomputed with CLOB market resolution)
 *   - delta   how many losses were hidden
 *
 * Usage:  node scripts/diff-handpicked-pool.mjs
 */
import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const { fetchAllActivity } = await import(path.join(ROOT, 'scanner/dataApi.js'));

const HP_PATH = path.join(ROOT, 'data/handpicked-wallets.json.gz');
const handpicked = JSON.parse(zlib.gunzipSync(fs.readFileSync(HP_PATH)).toString());
const wallets = Array.isArray(handpicked) ? handpicked : (handpicked.wallets || handpicked.list || []);

console.log(`Loaded ${wallets.length} handpicked wallets\n`);

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

async function analyseWallet(addr) {
  const events = await fetchAllActivity(addr, { maxEvents: 10000 });
  const conditionIds = [...new Set(events.map(e => e.conditionId).filter(Boolean))];

  // Resolve every market via CLOB
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
      if (!winningOutcome) {
        for (const tok of m.tokens) {
          if (parseFloat(tok.price || 0) >= 0.95) { winningOutcome = tok.outcome || null; break; }
        }
      }
    }
    lookup.set(cid, { closed: m.closed === true, winningOutcome, title: m.question || '' });
    await sleep(120);
  }

  // Build per-market ledger
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

  // Score
  let wins = 0, losses = 0, openCount = 0, totalPnl = 0, totalWagered = 0, totalReturned = 0;
  let bigWins = 0;   // wins > $1000 PnL (sizing edge)
  let dustLosses = 0; // losses < $50 PnL

  for (const [cid, m] of ledger) {
    const totalBought = m.buys.reduce((s, t) => s + t.size * t.price, 0);
    const totalSold   = m.sells.reduce((s, t) => s + t.size * t.price, 0);
    const totalBuySize  = m.buys.reduce((s, t) => s + t.size, 0);
    const totalSellSize = m.sells.reduce((s, t) => s + t.size, 0);
    const redeemSize    = m.redeems.reduce((s, t) => s + (t.size || 0), 0);
    const redeemUsdc    = m.redeems.reduce((s, t) => s + (t.usdcSize || t.payout || 0), 0);
    const closedShares = totalSellSize + redeemSize;
    const sellsClose = closedShares >= totalBuySize * 0.95;
    const g = lookup.get(cid) || {};

    let pnl, returned, isWin;
    if (sellsClose && totalBuySize > 0) {
      returned = totalSold + redeemUsdc;
      pnl = returned - totalBought;
      isWin = pnl > 0;
    } else if (g.closed && g.winningOutcome) {
      const won = String(m.outcome || '').toLowerCase().trim() === String(g.winningOutcome).toLowerCase().trim();
      const unredeemed = Math.max(0, totalBuySize - totalSellSize - redeemSize);
      returned = totalSold + redeemUsdc + (won ? unredeemed * 1.0 : 0);
      pnl = returned - totalBought;
      isWin = pnl > 0;
    } else {
      // Genuinely still open — exclude from W/L but track wagered
      openCount++;
      continue;
    }

    totalPnl += pnl;
    totalWagered += totalBought;
    totalReturned += returned;
    if (pnl > 0) { wins++; if (pnl > 1000) bigWins++; }
    else if (pnl < 0) { losses++; if (Math.abs(pnl) < 50) dustLosses++; }
  }

  const resolved = wins + losses;
  return {
    events: events.length,
    markets: ledger.size,
    clobFails,
    wins, losses, openCount,
    wr: resolved > 0 ? wins / resolved : null,
    totalPnl,
    totalWagered,
    totalReturned,
    roi: totalWagered > 0 ? (totalReturned - totalWagered) / totalWagered : null,
    bigWins, dustLosses,
  };
}

const rows = [];
for (let i = 0; i < wallets.length; i++) {
  const w = wallets[i];
  const addr = w.address;
  const cached = w.stats || {};
  console.log(`[${i+1}/${wallets.length}] ${addr.slice(0, 10)}…  fetching…`);
  try {
    const real = await analyseWallet(addr.toLowerCase());
    rows.push({ addr, notes: w.notes || '', cached, real });
    console.log(`  done: ${real.markets} markets, ${real.wins}W/${real.losses}L, $${real.totalPnl.toFixed(0)} PnL, ${real.openCount} still open`);
  } catch (e) {
    console.log(`  FAILED: ${e.message}`);
    rows.push({ addr, notes: w.notes || '', cached, real: null, error: e.message });
  }
}

// ── Side-by-side report ────────────────────────────────────────────────
const w = (s, n) => String(s == null ? '' : s).slice(0, n).padEnd(n);
const wr = (s, n) => String(s == null ? '' : s).slice(0, n).padStart(n);

console.log('\n');
console.log('='.repeat(150));
console.log('HANDPICKED POOL — CACHED vs CLOB-RESOLVED (real)');
console.log('='.repeat(150));
console.log(
  w('wallet', 12) +
  wr('mkts', 6) + ' ' +
  '   │ ' +
  w('CACHED  W/L', 12) + wr('WR', 7) + wr('PnL', 11) + '  │ ' +
  w('REAL    W/L', 12) + wr('WR', 7) + wr('PnL', 11) + wr('ROI', 8) + '  │ ' +
  wr('hidden L', 9) + ' ' +
  w('  notes', 30)
);
console.log('-'.repeat(150));
for (const row of rows) {
  const c = row.cached || {};
  const r = row.real;
  if (!r) {
    console.log(w(row.addr.slice(0, 10), 12) + '  ERROR: ' + row.error);
    continue;
  }
  const cWR = c.winRate != null ? (c.winRate * 100).toFixed(0) + '%' : '—';
  const cPnL = c.totalPnl != null ? '$' + c.totalPnl.toFixed(0) : '—';
  const rWR = r.wr != null ? (r.wr * 100).toFixed(0) + '%' : '—';
  const rPnL = '$' + r.totalPnl.toFixed(0);
  const rROI = r.roi != null ? (r.roi * 100).toFixed(0) + '%' : '—';
  const hiddenLosses = r.losses - (c.losses || 0);
  console.log(
    w(row.addr.slice(0, 10), 12) +
    wr(r.markets, 6) + ' ' +
    '   │ ' +
    w(`${c.wins ?? '—'}W/${c.losses ?? '—'}L`, 12) +
    wr(cWR, 7) +
    wr(cPnL, 11) + '  │ ' +
    w(`${r.wins}W/${r.losses}L`, 12) +
    wr(rWR, 7) +
    wr(rPnL, 11) +
    wr(rROI, 8) + '  │ ' +
    wr('+' + hiddenLosses, 9) + ' ' +
    w('  ' + (row.notes || ''), 30)
  );
}
console.log('-'.repeat(150));

// Pool totals
const totalCachedPnl = rows.filter(r => r.real).reduce((s, r) => s + (r.cached?.totalPnl || 0), 0);
const totalRealPnl = rows.filter(r => r.real).reduce((s, r) => s + r.real.totalPnl, 0);
const totalCachedW = rows.filter(r => r.real).reduce((s, r) => s + (r.cached?.wins || 0), 0);
const totalCachedL = rows.filter(r => r.real).reduce((s, r) => s + (r.cached?.losses || 0), 0);
const totalRealW = rows.filter(r => r.real).reduce((s, r) => s + r.real.wins, 0);
const totalRealL = rows.filter(r => r.real).reduce((s, r) => s + r.real.losses, 0);
console.log(
  w('POOL TOTAL', 12) + wr('', 7) +
  '   │ ' +
  w(`${totalCachedW}W/${totalCachedL}L`, 12) +
  wr(totalCachedW + totalCachedL > 0 ? (totalCachedW / (totalCachedW + totalCachedL) * 100).toFixed(0) + '%' : '—', 7) +
  wr('$' + totalCachedPnl.toFixed(0), 11) + '  │ ' +
  w(`${totalRealW}W/${totalRealL}L`, 12) +
  wr(totalRealW + totalRealL > 0 ? (totalRealW / (totalRealW + totalRealL) * 100).toFixed(0) + '%' : '—', 7) +
  wr('$' + totalRealPnl.toFixed(0), 11) +
  wr('', 8) + '  │ ' +
  wr('+' + (totalRealL - totalCachedL), 9)
);
console.log('='.repeat(150));
console.log('\nReading: "hidden L" = losses our cached stats missed because the market wasn\'t resolved in our marketLookup.');
console.log('A wallet with high "hidden L" had its WR / PnL inflated by Gamma\'s niche-market gap.');
