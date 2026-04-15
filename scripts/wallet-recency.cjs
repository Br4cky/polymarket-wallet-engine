/**
 * Recency + activity signals pulled from data/wallets.json.gz (the live
 * tracked pool), which is the authoritative per-wallet state the scanner
 * maintains.
 *
 * Why not wallet-history.json.gz?
 *   The scanner snapshots history for every wallet every scan, but only
 *   re-queries Goldsky for RESCORE_BATCH_SIZE (100) wallets per discovery
 *   cycle. With ~1714 wallets in the pool, each wallet's goldskyPnl is
 *   only refreshed every ~17 scans. The snapshot rows in between carry
 *   the stale cached value, so delta-of-goldskyPnl across the ~4.6 day
 *   history window is zero for essentially every wallet — that's a
 *   measurement artifact, not dormancy.
 *
 *   The pool file (wallets.json.gz) has the wallet's current state as
 *   known at its last rescoring. lastTradeTs is an actual timestamp
 *   (not a delta of stale values), so even if our measurement is a few
 *   scans old, "days since last trade" is directly interpretable.
 *
 * Exposed:
 *   loadPool()                -> Map<addrLower, walletRecord>
 *   recencyFor(pool, addr)    -> { daysSinceLastTrade, daysSinceLastScored,
 *                                  recentTradesPerDay, tradingSpanDays,
 *                                  avgHoldTimeHours, unredeemedWins,
 *                                  worthlessLosses, status, rank,
 *                                  lastTradeTs, lastScored }
 */
const fs = require('fs');
const zlib = require('zlib');

const MS_PER_DAY = 86_400_000;
const SEC_PER_DAY = 86_400;

function loadPool(path = 'data/wallets.json.gz') {
  const raw = JSON.parse(zlib.gunzipSync(fs.readFileSync(path)));
  const pool = raw.pool || raw;
  const map = new Map();
  for (const [addr, w] of Object.entries(pool)) {
    map.set(addr.toLowerCase(), w);
  }
  return map;
}

function recencyFor(pool, addr) {
  const w = pool.get(addr.toLowerCase());
  if (!w) return null;
  const s = w.stats || {};
  const nowSec = Date.now() / 1000;

  const daysSinceLastTrade = s.lastTradeTs
    ? +((nowSec - s.lastTradeTs) / SEC_PER_DAY).toFixed(2)
    : null;

  const daysSinceLastScored = w.lastScored
    ? +((Date.now() - new Date(w.lastScored).getTime()) / MS_PER_DAY).toFixed(2)
    : null;

  return {
    daysSinceLastTrade,
    daysSinceLastScored,
    recentTradesPerDay: s.recentTradesPerDay ?? null,
    tradingSpanDays: s.tradingSpanDays ?? null,
    avgHoldTimeHours: s.avgHoldTimeHours ?? null,
    unredeemedWins: s.unredeemedWins ?? null,
    worthlessLosses: s.worthlessLosses ?? null,
    status: w.status ?? null,
    rank: w.rank ?? null,
    lastTradeTs: s.lastTradeTs ?? null,
    lastScored: w.lastScored ?? null,
  };
}

module.exports = { loadPool, recencyFor };
