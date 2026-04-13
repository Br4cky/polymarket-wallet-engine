/*
 * scanner/repair.js — lightweight post-scan reconciliation.
 *
 * Runs between full scans to catch signals whose markets resolved during
 * Gamma's eventual-consistency window. For every active signal:
 *   1. Re-fetch its event via Gamma /events (returns closed markets too)
 *   2. If marketClosed + winningOutcome → close the signal with win/loss
 *   3. Regenerate aggregate stats
 *   4. Sync analytics.json.gz so the dashboard reflects the new state
 *
 * Does NOT touch discovery, scoring, paper trading, or trendline — only
 * closes stuck resolved signals and republishes analytics.signals.
 */

import path from 'path';
import { fileURLToPath } from 'url';
import {
  loadGzJSON, saveGzJSON,
  refreshSignalMarkets, matchesWinningOutcome,
} from './lib.js';
import { closeSignal } from './signals.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_DIR = path.join(__dirname, '..', 'data');

async function main() {
  const signalsFile = path.join(DATA_DIR, 'signals.json.gz');
  const marketsFile = path.join(DATA_DIR, 'markets.json.gz');
  const analyticsFile = path.join(DATA_DIR, 'analytics.json.gz');

  const signals = loadGzJSON(signalsFile);
  const marketsRaw = loadGzJSON(marketsFile) || {};
  const analytics = loadGzJSON(analyticsFile) || {};

  if (!signals || !signals.active) {
    console.error('No signals.json.gz or missing .active — aborting.');
    process.exit(1);
  }

  const active = signals.active;
  const history = signals.history || [];
  const marketLookup = new Map(Object.entries(marketsRaw));

  const activeArr = Object.values(active);
  console.log(`Repair starting: ${activeArr.length} active, ${history.length} history`);

  // Refresh markets for active signals via /events endpoint (includes closed).
  const refreshList = activeArr.map(s => ({
    tokenId: s.tokenId,
    conditionId: s.conditionId,
    eventSlug: s.eventSlug,
    slug: s.slug,
  }));
  await refreshSignalMarkets(refreshList, marketLookup);

  // Persist refreshed market cache.
  saveGzJSON(marketsFile, Object.fromEntries(marketLookup));

  // Derive a scanIndex for book-keeping. Use analytics.scanCount if present.
  const scanIndex = (analytics.scanCount || signals.stats?.lastScan || 0);
  const now = new Date().toISOString();

  let closedCount = 0;
  let winsClosed = 0;
  let lossesClosed = 0;

  for (const signalId of Object.keys(active)) {
    const s = active[signalId];
    const mi = marketLookup.get(s.tokenId);
    if (!mi) continue;
    const marketClosed = mi.marketClosed === true;
    const hasWinner = mi.winningOutcome && mi.winningOutcome.length > 0;
    if (!(marketClosed && hasWinner)) continue;

    const won = matchesWinningOutcome(s.direction, s.topOutcome, mi.winningOutcome);
    const outcome = won ? 'win' : 'loss';

    // Mirror the PnL math processSignals uses for gamma_repair closures.
    const op = s.openMarketPrice || s.avgEntryPrice || 0;
    if (won && op > 0) {
      s.signalReturn = +((1 / op - 1) * 100).toFixed(2);
    } else if (!won) {
      s.signalReturn = -100;
    }
    s.winningOutcome = mi.winningOutcome;
    s.resolvedBy = 'gamma_repair';

    closeSignal(active, history, signalId, 'resolved', scanIndex, now, outcome);
    closedCount++;
    if (won) winsClosed++; else lossesClosed++;
  }

  // Recompute aggregate stats (mirrors scanner/lib.js:2281-2324).
  const activeSignals = Object.values(active);
  const wins = history.filter(s => s.outcome === 'win').length;
  const losses = history.filter(s => s.outcome === 'loss').length;
  const resolved = wins + losses;
  const hitRate = resolved > 0 ? +(wins / resolved * 100).toFixed(1) : 0;
  const totalHistoryPnl = history.reduce((sum, sig) => sum + (sig.walletPnl || sig.closedPnl || 0), 0);
  const consensusSignals = activeSignals.filter(s => !s.signalType || s.signalType === 'consensus');
  const clusterSignals = activeSignals.filter(s => s.signalType === 'cluster');
  const soloSignals = activeSignals.filter(s => s.signalType === 'solo');

  const prevStats = signals.stats || {};
  const newStats = {
    ...prevStats,
    activeCount: activeSignals.length,
    consensusCount: consensusSignals.length,
    clusterCount: clusterSignals.length,
    soloCount: soloSignals.length,
    historyCount: history.length,
    totalResolved: resolved,
    wins,
    losses,
    hitRate,
    winRate: hitRate,
    totalPnl: +totalHistoryPnl.toFixed(2),
    avgConfidence: activeSignals.length > 0
      ? +(activeSignals.reduce((s, sig) => s + (sig.confidence || 0), 0) / activeSignals.length).toFixed(1)
      : 0,
    tierBreakdown: {
      elite: activeSignals.filter(s => s.tier === 'elite').length,
      pro: activeSignals.filter(s => s.tier === 'pro').length,
      starter: activeSignals.filter(s => s.tier === 'starter').length,
    },
    typeBreakdown: {
      consensus: consensusSignals.length,
      cluster: clusterSignals.length,
      solo: soloSignals.length,
    },
    // Repair-induced close is additive to this-scan tallies — leave those alone.
    lastRepair: now,
    lastRepairClosed: closedCount,
  };

  signals.stats = newStats;
  saveGzJSON(signalsFile, signals);

  // Sync analytics.signals so the dashboard sees the new state.
  analytics.signals = {
    active: activeSignals,
    history,
    stats: newStats,
  };
  saveGzJSON(analyticsFile, analytics);

  console.log(`Repair done: ${closedCount} closed (${winsClosed}W/${lossesClosed}L) | active=${activeSignals.length} history=${history.length} hitRate=${hitRate}%`);
}

main().catch(err => {
  console.error('Repair failed:', err);
  process.exit(1);
});
