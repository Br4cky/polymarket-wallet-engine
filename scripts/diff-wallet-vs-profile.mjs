#!/usr/bin/env node
/**
 * Diff our analyzer's view of a wallet against its public Polymarket profile.
 *
 * Usage:  node scripts/diff-wallet-vs-profile.mjs <0x...>
 *
 * Pulls /activity for the wallet, looks up every market on Gamma, and prints
 * per-market: cost / sold / PnL / our_status / gamma_status / outcome / title.
 * The point: produce a row-per-market view directly comparable to the profile.
 */
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const { fetchAllActivity } = await import(path.join(ROOT, 'scanner/dataApi.js'));

const wallet = (process.argv[2] || '').toLowerCase();
if (!/^0x[0-9a-f]{40}$/.test(wallet)) {
  console.error('Usage: diff-wallet-vs-profile.mjs <0x...>');
  process.exit(1);
}

console.log(`\nWallet: ${wallet}\n`);

// ── Pull activity ──────────────────────────────────────────────────────
console.log('Fetching /activity (lifetime)…');
const events = await fetchAllActivity(wallet, { maxEvents: 10000 });
console.log(`  ${events.length} events  (truncated=${events.truncated})`);

const types = {};
for (const e of events) types[e.type || 'unknown'] = (types[e.type || 'unknown'] || 0) + 1;
console.log('  by type:', types);

if (events.length) {
  const ts = events.map(e => e.timestamp).filter(Boolean);
  const first = new Date(Math.min(...ts) * 1000).toISOString().slice(0, 10);
  const last = new Date(Math.max(...ts) * 1000).toISOString().slice(0, 10);
  console.log(`  date range: ${first} → ${last}\n`);
}

// ── Resolve every market via CLOB ──────────────────────────────────────
// We use CLOB (clob.polymarket.com/markets/<conditionId>) instead of Gamma
// because Gamma's index excludes niche markets (e.g. LoL / esports). For an
// esports specialist wallet, every Gamma lookup returned []. CLOB has them
// all, with `closed`, `accepting_orders`, and a tokens[] array carrying
// each outcome's resolved price (1.0 for the winner, 0 for the loser).
const conditionIds = [...new Set(events.map(e => e.conditionId).filter(Boolean))];
console.log(`Resolving ${conditionIds.length} markets via CLOB…`);

async function fetchClobMarket(cid, attempt = 0) {
  try {
    const res = await fetch(`https://clob.polymarket.com/markets/${cid}`);
    if (res.status === 429 || res.status >= 500) {
      if (attempt < 3) {
        await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
        return fetchClobMarket(cid, attempt + 1);
      }
      return null;
    }
    if (!res.ok) return null;
    return await res.json();
  } catch {
    if (attempt < 2) {
      await new Promise(r => setTimeout(r, 500));
      return fetchClobMarket(cid, attempt + 1);
    }
    return null;
  }
}
const sleep = ms => new Promise(r => setTimeout(r, ms));

const gamma = new Map(); // conditionId → { closed, winningOutcome, endDate, title }
let i = 0;
let clobFails = 0;
for (const cid of conditionIds) {
  const m = await fetchClobMarket(cid);
  if (!m) { clobFails++; }
  await sleep(120);
  if (m) {
    const closedFlag = m.closed === true;
    const endIso = m.end_date_iso || m.endDate || null;
    const endMs = endIso ? new Date(endIso).getTime() : 0;
    const pastEnd = endMs > 0 && endMs < Date.now();
    // tokens: [{ token_id, outcome, price, winner }]. Resolved markets
    // carry either an explicit `winner: true` or a price ≥ 0.95 on the
    // winning side.
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
    gamma.set(cid, {
      closed: closedFlag,
      pastEnd,
      winningOutcome,
      endDate: endIso,
      title: m.question || '',
    });
  }
  if (++i % 5 === 0) process.stdout.write(`  ${i}/${conditionIds.length}\r`);
}
console.log(`  resolved ${gamma.size}/${conditionIds.length} markets via CLOB  (failures=${clobFails})         `);

// ── Build per-market ledger from events ────────────────────────────────
const ledger = new Map();
for (const e of events) {
  const cid = e.conditionId;
  if (!cid) continue;
  if (!ledger.has(cid)) ledger.set(cid, { buys: [], sells: [], redeems: [], title: e.title || '', outcome: e.outcome || '' });
  const m = ledger.get(cid);
  if (e.type === 'TRADE' && e.side === 'BUY') m.buys.push(e);
  else if (e.type === 'TRADE' && e.side === 'SELL') m.sells.push(e);
  else if (e.type === 'REDEEM') m.redeems.push(e);
}

// ── Per-market summary ─────────────────────────────────────────────────
const rows = [];
for (const [cid, m] of ledger) {
  const totalBought = m.buys.reduce((s, t) => s + t.size * t.price, 0);
  const totalSold   = m.sells.reduce((s, t) => s + t.size * t.price, 0);
  const totalBuySize  = m.buys.reduce((s, t) => s + t.size, 0);
  const totalSellSize = m.sells.reduce((s, t) => s + t.size, 0);
  const redeemSize    = m.redeems.reduce((s, t) => s + (t.size || 0), 0);
  const redeemUsdc    = m.redeems.reduce((s, t) => s + (t.usdcSize || t.payout || 0), 0);
  const avgBuyPrice = totalBuySize > 0 ? totalBought / totalBuySize : 0;

  const g = gamma.get(cid) || {};
  // Position is "closed" when sells + redeems together cover ≥95% of buys.
  // The previous version ignored REDEEMs, which made every redeemed-winner
  // look open (and threw the EdgeX position into a phantom -$15k loss).
  const closedShares = totalSellSize + redeemSize;
  const sellsClose = closedShares >= totalBuySize * 0.95;
  const gammaClosed = g.closed === true;

  // Reconstruct what the analyzer SHOULD see:
  let status, pnl, syntheticKind = null, walletWon = null;
  if (sellsClose && totalBuySize > 0) {
    // Realised PnL = (sell revenue + redeem payout) − cost
    pnl = (totalSold + redeemUsdc) - totalBought;
    status = redeemSize > 0 ? 'redeemed' : 'sold_out';
    if (redeemSize > 0) walletWon = redeemUsdc > 0;
  } else if (gammaClosed && g.winningOutcome) {
    const walletOutcome = String(m.outcome || '').toLowerCase().trim();
    const winning = String(g.winningOutcome || '').toLowerCase().trim();
    walletWon = walletOutcome && walletOutcome === winning;
    const unredeemed = Math.max(0, totalBuySize - totalSellSize - redeemSize);
    if (walletWon) {
      pnl = (totalSold + redeemUsdc + unredeemed * 1.0) - totalBought;
      status = 'synthetic_win';
      syntheticKind = 'unredeemed_win';
    } else {
      pnl = (totalSold + redeemUsdc) - totalBought;
      status = 'synthetic_loss';
      syntheticKind = 'worthless_loss';
    }
  } else {
    status = 'open';
    pnl = (totalSold + redeemUsdc) - totalBought; // realised slice only
  }

  // What does the wallet ACTUALLY hold? buy size minus sell size minus redeems.
  const heldShares = Math.max(0, totalBuySize - totalSellSize - redeemSize);

  rows.push({
    cid,
    title: g.title || m.title,
    direction: m.outcome,
    status,
    syntheticKind,
    walletWon,
    pnl,
    cost: totalBought,
    sold: totalSold,
    buySize: totalBuySize,
    sellSize: totalSellSize,
    redeemSize,
    redeemUsdc,
    heldShares,
    gammaClosed,
    pastEnd: g.pastEnd === true,
    winningOutcome: g.winningOutcome,
    avgBuyPrice,
  });
}

rows.sort((a, b) => (b.pnl || 0) - (a.pnl || 0));

// ── Headline ───────────────────────────────────────────────────────────
const resolved = rows.filter(r => r.status !== 'open');
const open = rows.filter(r => r.status === 'open');
const wins = resolved.filter(r => r.pnl > 0).length;
const losses = resolved.filter(r => r.pnl < 0).length;
const totalPnl = resolved.reduce((s, r) => s + r.pnl, 0);
console.log('\n=== HEADLINE (this script\'s recompute) ===');
console.log(`Markets touched:  ${rows.length}`);
console.log(`  resolved:        ${resolved.length}  (${wins}W / ${losses}L  WR ${resolved.length ? (wins/resolved.length*100).toFixed(1) : '—'}%)`);
console.log(`     sold_out:       ${rows.filter(r => r.status === 'sold_out').length}`);
console.log(`     redeemed:       ${rows.filter(r => r.status === 'redeemed').length}`);
console.log(`     synthetic_win:  ${rows.filter(r => r.status === 'synthetic_win').length}`);
console.log(`     synthetic_loss: ${rows.filter(r => r.status === 'synthetic_loss').length}`);
console.log(`  open:            ${open.length}`);
console.log(`Total PnL:        $${totalPnl.toFixed(2)}`);

// ── Per-market table ───────────────────────────────────────────────────
// "Returned" = sells + redemptions + (if synthetic-closed and won) value of
// unredeemed shares × $1. ROI is profit / wagered, expressed as a percent.
console.log('\n=== POSITION TABLE (sorted by PnL) ===');
const w = (s, n) => String(s == null ? '' : s).slice(0, n).padEnd(n);
const wr = (s, n) => String(s == null ? '' : s).slice(0, n).padStart(n);
console.log(
  w('result', 7) + ' ' +
  wr('wagered', 10) + ' ' +
  wr('returned', 10) + ' ' +
  wr('P/L', 10) + ' ' +
  wr('ROI', 8) + ' ' +
  w('side', 18) + ' ' +
  w('title', 65)
);
console.log('-'.repeat(140));
for (const r of rows) {
  // Returned = realised cash in (sold + redeemed) plus, for synthetic
  // wins, the $1-per-share value of the unredeemed winning side.
  let returned = (r.sold || 0) + (r.redeemUsdc || 0);
  if (r.status === 'synthetic_win') {
    returned += Math.max(0, (r.buySize || 0) - (r.sellSize || 0) - (r.redeemSize || 0)) * 1;
  }
  const wagered = r.cost || 0;
  const pnl = r.pnl || 0;
  const roi = wagered > 0 ? (pnl / wagered) * 100 : 0;
  const result = pnl > 0 ? 'WIN' : pnl < 0 ? 'LOSS' : '—';
  console.log(
    w(result, 7) + ' ' +
    wr('$' + wagered.toFixed(0), 10) + ' ' +
    wr('$' + returned.toFixed(0), 10) + ' ' +
    wr((pnl >= 0 ? '+' : '') + '$' + pnl.toFixed(0), 10) + ' ' +
    wr((roi >= 0 ? '+' : '') + roi.toFixed(0) + '%', 8) + ' ' +
    w(r.direction || '', 18) + ' ' +
    w(r.title || '', 65)
  );
}
console.log('-'.repeat(140));

// Totals
const totalWagered = rows.reduce((s, r) => s + (r.cost || 0), 0);
const totalReturned = rows.reduce((s, r) => {
  let ret = (r.sold || 0) + (r.redeemUsdc || 0);
  if (r.status === 'synthetic_win') ret += Math.max(0, (r.buySize||0) - (r.sellSize||0) - (r.redeemSize||0));
  return s + ret;
}, 0);
const totalRoi = totalWagered > 0 ? ((totalReturned - totalWagered) / totalWagered) * 100 : 0;
console.log(
  w('TOTAL', 7) + ' ' +
  wr('$' + totalWagered.toFixed(0), 10) + ' ' +
  wr('$' + totalReturned.toFixed(0), 10) + ' ' +
  wr((totalPnl >= 0 ? '+' : '') + '$' + totalPnl.toFixed(0), 10) + ' ' +
  wr((totalRoi >= 0 ? '+' : '') + totalRoi.toFixed(1) + '%', 8)
);

// ── Why are "open" rows still open in our view? ────────────────────────
console.log('\n=== "OPEN" ROWS — why each is still open ===');
for (const r of open) {
  const reasons = [];
  if (!r.gammaClosed) {
    if (r.pastEnd && !r.winningOutcome) reasons.push('gamma:past_end_no_winner');
    else reasons.push('gamma:not_closed');
  }
  if (r.heldShares > 0) reasons.push(`holds=${r.heldShares.toFixed(0)}`);
  if (r.redeemSize > 0) reasons.push(`redeemed=${r.redeemSize.toFixed(0)}`);
  console.log(`  ${reasons.join('  ')}  [${r.direction}]  ${(r.title||'').slice(0,90)}`);
}

console.log('\nDone. The status column tells you which path our analyzer used.');
console.log('Compare row-by-row to the profile. If the profile shows a market we are missing, conditionId is in the row.');
