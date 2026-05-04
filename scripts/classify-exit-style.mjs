#!/usr/bin/env node
/**
 * Classify each handpicked wallet by exit style.
 *
 * Reasoning: we emit follower signals on BUYs. A follower buys and holds
 * to resolution. If the source wallet's edge comes from selling before
 * resolution (flipping), the follower can't replicate that exit timing
 * and the signal is noise. Only buy-and-hold-to-resolution wallets are
 * useful alpha sources — even if a flipper is profitable, copying their
 * BUYs without their SELLs will not reproduce their PnL.
 *
 * Per resolved position we classify the wallet's exit:
 *   - REDEEM         had a REDEEM event → held to resolution. ✅ holder
 *   - HOLD_TO_ZERO   bought losing side, no sell, no redeem; the shares
 *                    became worthless at resolution. ✅ holder
 *   - SELL_AFTER     sold ≥95% AFTER market endDate. ✅ holder (rare)
 *   - SELL_BEFORE    sold ≥95% BEFORE market endDate. ❌ flipper
 *   - PARTIAL        sold some, held the rest. Counted as half-flip.
 *
 * Output: per wallet, the holdRatio (HOLD-eligible / total resolved).
 * Wallets with holdRatio < 0.5 are flippers and should be evicted from
 * the signal pool regardless of their profitability.
 *
 * Usage:  node scripts/classify-exit-style.mjs
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

async function classifyWallet(addr) {
  const events = await fetchAllActivity(addr, { maxEvents: 10000 });
  const conditionIds = [...new Set(events.map(e => e.conditionId).filter(Boolean))];

  // CLOB lookup — need endDate per market to know when "before resolution" was
  const lookup = new Map();
  for (const cid of conditionIds) {
    const m = await fetchClobMarket(cid);
    if (m) {
      const endIso = m.end_date_iso || m.endDate || null;
      const endTs = endIso ? new Date(endIso).getTime() / 1000 : 0;
      let winningOutcome = null;
      if (Array.isArray(m.tokens)) {
        for (const tok of m.tokens) {
          if (tok.winner === true) { winningOutcome = tok.outcome || null; break; }
        }
        if (!winningOutcome) for (const tok of m.tokens) {
          if (parseFloat(tok.price || 0) >= 0.95) { winningOutcome = tok.outcome || null; break; }
        }
      }
      lookup.set(cid, { closed: m.closed === true, endTs, winningOutcome });
    }
    await sleep(120);
  }

  // Per-market classification
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

  const counts = { REDEEM: 0, HOLD_TO_ZERO: 0, SELL_AFTER: 0, SELL_BEFORE: 0, PARTIAL: 0, UNRESOLVED: 0 };
  // Also track $ wagered per bucket to weight by capital
  const capitalByBucket = { holder: 0, flipper: 0 };

  for (const [cid, m] of ledger) {
    const g = lookup.get(cid) || {};
    const nowTs = Date.now() / 1000;
    const marketEnded = g.endTs > 0 && g.endTs < nowTs;
    const marketResolved = g.closed === true || marketEnded;

    const buySize = m.buys.reduce((s, t) => s + t.size, 0);
    const sellSize = m.sells.reduce((s, t) => s + t.size, 0);
    const redeemSize = m.redeems.reduce((s, t) => s + (t.size || 0), 0);
    const wagered = m.buys.reduce((s, t) => s + t.size * t.price, 0);
    if (buySize === 0) continue;

    const lastSellTs = m.sells.length ? Math.max(...m.sells.map(t => t.timestamp || 0)) : 0;
    const soldFraction = sellSize / buySize;
    const redeemed = redeemSize > 0 || m.redeems.length > 0;

    let bucket;
    if (redeemed) {
      // Redeem implies wallet held to resolution and claimed payout — definitionally a holder.
      bucket = 'REDEEM';
      capitalByBucket.holder += wagered;
    } else if (soldFraction >= 0.95) {
      // Fully sold out. Classify by exit timing relative to market end.
      // No CLOB-closed gate here: a wallet that sells out of an
      // active market is the canonical flipper pattern — the previous
      // version hid these by skipping markets without resolution data.
      if (g.endTs > 0 && lastSellTs > g.endTs) {
        bucket = 'SELL_AFTER';
        capitalByBucket.holder += wagered;
      } else {
        // endTs unknown OR sold before market end → treat as flip.
        bucket = 'SELL_BEFORE';
        capitalByBucket.flipper += wagered;
      }
    } else if (soldFraction >= 0.05) {
      // Sold some, held some.
      bucket = 'PARTIAL';
      capitalByBucket.holder += wagered * (1 - soldFraction);
      capitalByBucket.flipper += wagered * soldFraction;
    } else if (marketResolved) {
      // No sells, no redeem, market has ended → losing shares held to zero.
      bucket = 'HOLD_TO_ZERO';
      capitalByBucket.holder += wagered;
    } else {
      // No sells, market still active — wallet is genuinely still holding.
      // Don't reward or penalise; exclude from the ratio.
      counts.UNRESOLVED++;
      continue;
    }
    counts[bucket]++;
  }

  const totalResolved = counts.REDEEM + counts.HOLD_TO_ZERO + counts.SELL_AFTER + counts.SELL_BEFORE + counts.PARTIAL;
  const holderCount = counts.REDEEM + counts.HOLD_TO_ZERO + counts.SELL_AFTER;
  const holdRatio = totalResolved > 0 ? holderCount / totalResolved : null;
  const totalCapital = capitalByBucket.holder + capitalByBucket.flipper;
  const capitalHoldRatio = totalCapital > 0 ? capitalByBucket.holder / totalCapital : null;

  return { counts, totalResolved, holdRatio, capitalHoldRatio, capitalByBucket };
}

const rows = [];
for (let i = 0; i < wallets.length; i++) {
  const w = wallets[i];
  const addr = w.address;
  console.log(`[${i+1}/${wallets.length}] ${addr.slice(0, 10)}…  classifying…`);
  try {
    const r = await classifyWallet(addr.toLowerCase());
    rows.push({ addr, notes: w.notes || '', ...r });
    const hr = r.holdRatio == null ? '—' : (r.holdRatio * 100).toFixed(0) + '%';
    console.log(`  holdRatio=${hr}  REDEEM=${r.counts.REDEEM} HOLD0=${r.counts.HOLD_TO_ZERO} SELL_A=${r.counts.SELL_AFTER} SELL_B=${r.counts.SELL_BEFORE} PARTIAL=${r.counts.PARTIAL}`);
  } catch (e) {
    console.log(`  FAILED: ${e.message}`);
  }
}

// ── Report ─────────────────────────────────────────────────────────────
const w = (s, n) => String(s == null ? '' : s).slice(0, n).padEnd(n);
const wr = (s, n) => String(s == null ? '' : s).slice(0, n).padStart(n);

console.log('\n');
console.log('='.repeat(140));
console.log('HANDPICKED POOL — EXIT STYLE CLASSIFICATION');
console.log('='.repeat(140));
console.log(
  w('wallet', 12) +
  wr('resolved', 9) +
  wr('REDEEM', 8) +
  wr('HOLD0', 7) +
  wr('S_AFTER', 9) +
  wr('S_BEFORE', 9) +
  wr('PART', 6) +
  '   ' +
  wr('hold%', 7) +
  wr('hold$%', 8) +
  '   ' +
  w('verdict', 12) +
  '   notes'
);
console.log('-'.repeat(140));

const verdict = r => {
  if (r.totalResolved < 5) return 'TOO_FEW';
  const ratio = r.holdRatio || 0;
  if (ratio >= 0.7) return 'HOLDER ✓';
  if (ratio >= 0.4) return 'MIXED';
  return 'FLIPPER ✗';
};

// Sort: holders first, flippers last
rows.sort((a, b) => (b.holdRatio || 0) - (a.holdRatio || 0));

for (const r of rows) {
  const hr = r.holdRatio == null ? '—' : (r.holdRatio * 100).toFixed(0) + '%';
  const chr = r.capitalHoldRatio == null ? '—' : (r.capitalHoldRatio * 100).toFixed(0) + '%';
  console.log(
    w(r.addr.slice(0, 10), 12) +
    wr(r.totalResolved, 9) +
    wr(r.counts.REDEEM, 8) +
    wr(r.counts.HOLD_TO_ZERO, 7) +
    wr(r.counts.SELL_AFTER, 9) +
    wr(r.counts.SELL_BEFORE, 9) +
    wr(r.counts.PARTIAL, 6) +
    '   ' +
    wr(hr, 7) +
    wr(chr, 8) +
    '   ' +
    w(verdict(r), 12) +
    '   ' + (r.notes || '').slice(0, 40)
  );
}
console.log('-'.repeat(140));

console.log('\nLegend:');
console.log('  REDEEM      — wallet held to resolution and redeemed payout (buy-and-hold) ✓');
console.log('  HOLD0       — wallet held losing shares to worthlessness (still buy-and-hold) ✓');
console.log('  S_AFTER     — sold after market resolved (effectively a holder) ✓');
console.log('  S_BEFORE    — sold before market resolved (FLIP — follower can\'t replicate) ✗');
console.log('  PART        — partial sell + partial hold');
console.log('  hold%       — fraction of resolved positions where wallet held to resolution (by count)');
console.log('  hold$%      — same, capital-weighted (more honest — big positions count more)');
console.log('  verdict     — HOLDER if holdRatio ≥ 70%; FLIPPER if < 40%; MIXED in between');
