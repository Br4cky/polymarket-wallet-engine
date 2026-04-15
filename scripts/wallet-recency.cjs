/**
 * Recency signal derived from data/wallet-history.json.gz.
 *
 * Goldsky userPosition is aggregate-only (no per-trade timestamps), so we
 * can't compute time-partitioned ROI from positions alone. As a proxy, we
 * mine the per-scan history snapshots to answer:
 *   - when did goldskyPnl last move?
 *   - how much has it moved over the tracked window?
 *   - is position count growing, flat, or shrinking?
 *
 * A wallet whose pnl has been frozen for many days with no new positions
 * is effectively dormant — regardless of lifetime ROI.
 *
 * Exposed:
 *   loadHistory()                         -> Map<addrLower, snapshots[]>
 *   recencyFor(history, addr)             -> { daysSinceChange, pnlDelta,
 *                                              positionsDelta, windowDays,
 *                                              snapshotCount, lastTs }
 */
const fs = require('fs');
const zlib = require('zlib');

const MS_PER_DAY = 86_400_000;

function loadHistory(path = 'data/wallet-history.json.gz') {
  const raw = JSON.parse(zlib.gunzipSync(fs.readFileSync(path)));
  const src = raw.wallets || raw;
  const map = new Map();
  for (const [addr, snaps] of Object.entries(src)) {
    if (!Array.isArray(snaps) || snaps.length === 0) continue;
    // Ensure chronological order.
    const sorted = snaps.slice().sort((a, b) => new Date(a.ts) - new Date(b.ts));
    map.set(addr.toLowerCase(), sorted);
  }
  return map;
}

function recencyFor(history, addr) {
  const snaps = history.get(addr.toLowerCase());
  if (!snaps || snaps.length === 0) return null;

  const first = snaps[0];
  const last = snaps[snaps.length - 1];
  const lastTs = new Date(last.ts);
  const firstTs = new Date(first.ts);
  const windowDays = (lastTs - firstTs) / MS_PER_DAY;

  // Find the most recent snapshot where goldskyPnl differed from the latest.
  let lastChangeTs = firstTs;
  for (let i = snaps.length - 2; i >= 0; i--) {
    if (snaps[i].goldskyPnl !== last.goldskyPnl) {
      // Change happened between snaps[i] and snaps[i+1] — use the later one.
      lastChangeTs = new Date(snaps[i + 1].ts);
      break;
    }
    if (i === 0) lastChangeTs = firstTs; // no change found in window
  }
  // If only one snapshot, daysSinceChange is 0 (can't tell).
  if (snaps.length === 1) lastChangeTs = lastTs;

  const daysSinceChange = (lastTs - lastChangeTs) / MS_PER_DAY;
  const pnlDelta = (last.goldskyPnl || 0) - (first.goldskyPnl || 0);
  const positionsDelta = (last.goldskyPositions || 0) - (first.goldskyPositions || 0);

  return {
    daysSinceChange: +daysSinceChange.toFixed(2),
    pnlDelta: +pnlDelta.toFixed(2),
    positionsDelta,
    windowDays: +windowDays.toFixed(2),
    snapshotCount: snaps.length,
    lastTs: last.ts,
    lastPnl: last.goldskyPnl,
    lastPositions: last.goldskyPositions,
  };
}

module.exports = { loadHistory, recencyFor };
