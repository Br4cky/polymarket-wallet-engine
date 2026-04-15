#!/usr/bin/env node
/**
 * One-time repair: retroactively resolve history signals that have no WIN/LOSS outcome.
 * Queries Gamma by condition_id to get the actual market result.
 */
const zlib = require('zlib');
const fs = require('fs');

const GAMMA_MARKETS = 'https://gamma-api.polymarket.com/markets';
const SIGNALS_FILE = 'data/signals.json.gz';

function loadGzJSON(path) {
  return JSON.parse(zlib.gunzipSync(fs.readFileSync(path)));
}
function saveGzJSON(path, data) {
  fs.writeFileSync(path, zlib.gzipSync(JSON.stringify(data, null, 2)));
}

async function queryGamma(conditionId) {
  const url = `${GAMMA_MARKETS}?condition_id=${conditionId}&limit=10`;
  const res = await fetch(url);
  if (!res.ok) return null;
  const markets = await res.json();
  if (!Array.isArray(markets) || markets.length === 0) return null;
  return markets[0];
}

function determineWinningOutcome(market) {
  // Method 1: tokens array — price >= 0.95 means that outcome won
  if (market.tokens && Array.isArray(market.tokens)) {
    for (const token of market.tokens) {
      const price = parseFloat(token.price || 0);
      if (price >= 0.95) return token.outcome || null;
    }
  }
  // Method 2: outcomePrices array
  let parsedPrices = market.outcomePrices;
  if (typeof parsedPrices === 'string') try { parsedPrices = JSON.parse(parsedPrices); } catch(e) { parsedPrices = null; }
  let parsedOutcomes = market.outcomes;
  if (typeof parsedOutcomes === 'string') try { parsedOutcomes = JSON.parse(parsedOutcomes); } catch(e) { parsedOutcomes = null; }
  if (Array.isArray(parsedPrices) && Array.isArray(parsedOutcomes)) {
    for (let i = 0; i < parsedPrices.length; i++) {
      if (parseFloat(parsedPrices[i] || 0) >= 0.95 && parsedOutcomes[i]) {
        return parsedOutcomes[i];
      }
    }
  }
  // Method 3: winner field
  if (market.winner) return market.winner;
  return null;
}

function determineOutcome(signal, market, winningOutcome) {
  // If we have a winning outcome, check if it matches signal direction
  if (winningOutcome) {
    const dir = (signal.direction || '').toLowerCase().trim();
    const winner = winningOutcome.toLowerCase().trim();
    if (dir === winner) return { outcome: 'win', resolvedBy: 'gamma_repair' };
    return { outcome: 'loss', resolvedBy: 'gamma_repair' };
  }

  // Fallback: use token prices from the market
  const tokenId = signal.tokenId;
  let clobIds = market.clobTokenIds;
  if (typeof clobIds === 'string') try { clobIds = JSON.parse(clobIds); } catch(e) { clobIds = null; }
  let outcomePrices = market.outcomePrices;
  if (typeof outcomePrices === 'string') try { outcomePrices = JSON.parse(outcomePrices); } catch(e) { outcomePrices = null; }

  if (Array.isArray(clobIds) && Array.isArray(outcomePrices)) {
    const idx = clobIds.indexOf(tokenId);
    if (idx !== -1) {
      const price = parseFloat(outcomePrices[idx] || 0);
      if (price >= 0.95) return { outcome: 'win', resolvedBy: 'price_repair' };
      if (price <= 0.05) return { outcome: 'loss', resolvedBy: 'price_repair' };
    }
  }

  // Check if market is closed but we can't determine winner
  const closed = market.closed === true || market.closed === 'true';
  const notAccepting = !(market.accepting_orders === true || market.acceptingOrders === true
    || market.accepting_orders === 'true' || market.acceptingOrders === 'true');

  if (closed || notAccepting) {
    // Use current token price direction
    if (Array.isArray(clobIds) && Array.isArray(outcomePrices)) {
      const idx = clobIds.indexOf(tokenId);
      if (idx !== -1) {
        const price = parseFloat(outcomePrices[idx] || 0);
        if (price > 0.5) return { outcome: 'win', resolvedBy: 'price_infer_repair' };
        return { outcome: 'loss', resolvedBy: 'price_infer_repair' };
      }
    }
  }

  return null; // Can't determine
}

async function main() {
  console.log('Loading signals...');
  const data = loadGzJSON(SIGNALS_FILE);
  const historyKeys = Object.keys(data.history || {});

  let repaired = 0;
  let notClosed = 0;
  let failed = 0;
  let alreadyResolved = 0;

  for (const key of historyKeys) {
    const signal = data.history[key];
    if (signal.outcome === 'win' || signal.outcome === 'loss') {
      alreadyResolved++;
      continue;
    }
    if (!signal.conditionId) {
      console.log(`  SKIP ${signal.signalId} — no conditionId`);
      failed++;
      continue;
    }

    const market = await queryGamma(signal.conditionId);
    if (!market) {
      console.log(`  SKIP ${signal.signalId} — Gamma returned nothing for ${signal.conditionId.slice(0, 20)}...`);
      failed++;
      continue;
    }

    const closed = market.closed === true || market.closed === 'true';
    const notAccepting = !(market.accepting_orders === true || market.acceptingOrders === true
      || market.accepting_orders === 'true' || market.acceptingOrders === 'true');

    if (!closed && !notAccepting) {
      console.log(`  SKIP ${signal.signalId} — market still open: ${(market.question || '').slice(0, 50)}`);
      notClosed++;
      continue;
    }

    const winningOutcome = determineWinningOutcome(market);
    const result = determineOutcome(signal, market, winningOutcome);

    if (!result) {
      console.log(`  SKIP ${signal.signalId} — can't determine outcome for: ${(market.question || '').slice(0, 50)}`);
      failed++;
      continue;
    }

    // Repair the signal.
    // IMPORTANT: flip status back to 'closed' too. Historic bug here was that
    // voided-then-repaired entries kept status='voided', producing "ghost"
    // signals with outcome='win' but still excluded from WR calcs by the
    // `status === 'voided'` guard. Also use the canonical `closeReason`
    // field (not the misspelled `closedReason`).
    const oldOutcome = signal.outcome;
    const oldStatus = signal.status;
    signal.outcome = result.outcome;
    signal.resolvedBy = result.resolvedBy;
    signal.closeReason = 'resolved';
    signal.status = 'closed';
    delete signal.closedReason; // scrub legacy misspelled field if present

    // Compute return
    const openPrice = signal.openMarketPrice || signal.avgEntryPrice || 0;
    if (result.outcome === 'win' && openPrice > 0) {
      signal.signalReturn = +((1 / openPrice - 1) * 100).toFixed(2);
    } else if (result.outcome === 'loss') {
      signal.signalReturn = -100;
    }

    console.log(`  REPAIRED ${signal.signalId}: ${oldOutcome}/${oldStatus} → ${result.outcome}/closed (${result.resolvedBy}) | dir=${signal.direction} winner=${winningOutcome || 'inferred'} | ${(market.question || '').slice(0, 50)}`);
    repaired++;

    await new Promise(r => setTimeout(r, 150)); // Rate limit
  }

  console.log(`\n=== REPAIR SUMMARY ===`);
  console.log(`Already resolved: ${alreadyResolved}`);
  console.log(`Repaired: ${repaired}`);
  console.log(`Still open: ${notClosed}`);
  console.log(`Failed/unknown: ${failed}`);
  console.log(`Total history: ${historyKeys.length}`);

  // Save
  if (repaired > 0) {
    saveGzJSON(SIGNALS_FILE, data);
    console.log(`\nSaved ${SIGNALS_FILE} with ${repaired} repaired signals.`);
  } else {
    console.log('\nNo repairs needed.');
  }
}

main().catch(console.error);
