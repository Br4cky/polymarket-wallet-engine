#!/usr/bin/env node
/**
 * PROTOTYPE: Position-centric wallet ledger.
 *
 * Given a wallet address, enumerate every position it has ever held via
 * Goldsky's userPosition subgraph (lifetime-complete, not capped at 3000
 * events like the /activity API). For each position, reconcile against our
 * marketLookup to determine:
 *   - resolved win / loss / still open
 *   - realized PnL (Goldsky's direct number)
 *   - unrealized-but-decided PnL (winner but unredeemed, or loser sitting on
 *     worthless tokens — Goldsky reports $0 for these)
 *   - true lifetime PnL = realized + decided
 *
 * Output: per-position ledger + aggregates. No scanner side effects, no
 * state writes. This is for eyeballing a wallet end-to-end.
 *
 * Usage:
 *   node scripts/wallet-ledger.cjs 0x1234...
 *   node scripts/wallet-ledger.cjs 0x1234... --json     # machine-readable
 *   node scripts/wallet-ledger.cjs 0x1234... --verbose  # per-position detail
 */
const zlib = require('zlib');
const fs = require('fs');

const GOLDSKY_PNL = 'https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/pnl-subgraph/0.0.14/gn';
const GAMMA_MARKETS = 'https://gamma-api.polymarket.com/markets';
const USDC_DIVISOR = 1e6;

const args = process.argv.slice(2);
const address = args.find(a => /^0x[0-9a-fA-F]{40}$/.test(a))?.toLowerCase();
const wantJson = args.includes('--json');
const verbose = args.includes('--verbose') || args.includes('-v');

if (!address) {
  console.error('Usage: node scripts/wallet-ledger.cjs <0xaddress> [--json] [--verbose]');
  process.exit(1);
}

function loadGzJSON(p) {
  return JSON.parse(zlib.gunzipSync(fs.readFileSync(p)));
}

async function gql(query) {
  const res = await fetch(GOLDSKY_PNL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) throw new Error(`Goldsky ${res.status}`);
  const data = await res.json();
  if (data.errors) throw new Error(`GraphQL: ${JSON.stringify(data.errors)}`);
  return data.data;
}

async function fetchPositions(addr) {
  const positions = [];
  let lastId = '';
  while (positions.length < 5000) {
    const query = `{
      userPositions(
        first: 1000
        orderBy: id
        where: { user: "${addr}"${lastId ? `, id_gt: "${lastId}"` : ''} }
      ) {
        id
        tokenId
        amount
        avgPrice
        realizedPnl
        totalBought
      }
    }`;
    const data = await gql(query);
    const items = data?.userPositions || [];
    if (items.length === 0) break;
    for (const it of items) {
      positions.push({
        id: it.id,
        tokenId: it.tokenId,
        sharesHeld: parseFloat(it.amount || 0) / USDC_DIVISOR,
        avgPrice: parseFloat(it.avgPrice || 0) / USDC_DIVISOR,
        realizedPnl: parseFloat(it.realizedPnl || 0) / USDC_DIVISOR,
        totalBought: parseFloat(it.totalBought || 0) / USDC_DIVISOR,
      });
    }
    if (items.length < 1000) break;
    lastId = items[items.length - 1].id;
  }
  return positions;
}

async function gammaLookup(tokenId) {
  try {
    const res = await fetch(`${GAMMA_MARKETS}?clob_token_ids=${tokenId}&limit=1`);
    if (!res.ok) return null;
    const arr = await res.json();
    if (!Array.isArray(arr) || arr.length === 0) return null;
    const m = arr[0];
    let tokens = m.clobTokenIds;
    if (typeof tokens === 'string') { try { tokens = JSON.parse(tokens); } catch { tokens = null; } }
    let outcomes = m.outcomes;
    if (typeof outcomes === 'string') { try { outcomes = JSON.parse(outcomes); } catch { outcomes = null; } }
    let prices = m.outcomePrices;
    if (typeof prices === 'string') { try { prices = JSON.parse(prices); } catch { prices = null; } }
    const closed = m.closed === true || m.closed === 'true';
    let idx = -1;
    if (Array.isArray(tokens)) idx = tokens.indexOf(tokenId);
    let tokenWon = null;
    let currentPrice = null;
    if (idx !== -1 && Array.isArray(prices)) {
      const p = parseFloat(prices[idx] || 0);
      currentPrice = p;
      if (closed) {
        if (p >= 0.99) tokenWon = true;
        else if (p <= 0.01) tokenWon = false;
      }
    }
    return {
      closed,
      tokenWon,
      currentPrice,
      question: m.question || '',
    };
  } catch {
    return null;
  }
}

function classify(pos, market) {
  // Fully closed: realizedPnl is the whole story.
  if (pos.sharesHeld < 0.01) {
    return {
      status: 'closed',
      outcome: pos.realizedPnl > 0 ? 'win' : (pos.realizedPnl < 0 ? 'loss' : 'scratch'),
      truePnl: pos.realizedPnl,
      decidedPnl: 0,
    };
  }

  // Still holding shares. Need market info.
  if (!market) {
    return {
      status: 'unknown',
      outcome: 'unknown',
      truePnl: pos.realizedPnl,
      decidedPnl: null,
      note: 'market not in lookup',
    };
  }

  if (!market.closed) {
    // Open position, mark-to-market at current price if we have it
    const mtm = market.currentPrice != null
      ? pos.sharesHeld * market.currentPrice - pos.sharesHeld * pos.avgPrice
      : null;
    return {
      status: 'open',
      outcome: 'pending',
      truePnl: pos.realizedPnl + (mtm || 0),
      decidedPnl: 0,
      mtm,
      currentPrice: market.currentPrice,
    };
  }

  // Market closed & still holding shares — decided outcome, unredeemed.
  if (market.tokenWon === true) {
    // Winner. Each share is worth $1 at redemption.
    const decidedPnl = pos.sharesHeld * (1 - pos.avgPrice);
    return {
      status: 'unredeemed_win',
      outcome: 'win',
      truePnl: pos.realizedPnl + decidedPnl,
      decidedPnl,
    };
  }
  if (market.tokenWon === false) {
    // Loser. Tokens are worth $0; they paid avgPrice per share and got nothing.
    const decidedPnl = -pos.sharesHeld * pos.avgPrice;
    return {
      status: 'worthless_loss',
      outcome: 'loss',
      truePnl: pos.realizedPnl + decidedPnl,
      decidedPnl,
    };
  }
  // Closed market but can't determine winner (ambiguous price)
  return {
    status: 'closed_undetermined',
    outcome: 'unknown',
    truePnl: pos.realizedPnl,
    decidedPnl: null,
    note: 'market closed but winner undetermined',
  };
}

(async function main() {
  console.error(`Fetching Goldsky positions for ${address}...`);
  const positions = await fetchPositions(address);
  console.error(`Got ${positions.length} positions from subgraph.`);

  // Load our marketLookup if available (saves Gamma API calls)
  let marketLookup = new Map();
  try {
    const analytics = loadGzJSON('data/analytics.json.gz');
    for (const [tid, m] of Object.entries(analytics.marketLookup || {})) {
      marketLookup.set(tid, m);
    }
    console.error(`Loaded ${marketLookup.size} markets from analytics.json.gz`);
  } catch {
    console.error('No local marketLookup; will hit Gamma for every position.');
  }

  const enriched = [];
  let gammaCalls = 0;
  for (const pos of positions) {
    let market = null;
    const fromLookup = marketLookup.get(pos.tokenId);
    if (fromLookup) {
      // Adapt our lookup format to the {closed, tokenWon, currentPrice, question} shape
      const closed = fromLookup.marketClosed === true;
      let tokenWon = null;
      if (closed && fromLookup.winningOutcome) {
        // Winning side as string; compare against our token's outcome label
        const thisOutcome = fromLookup.outcome;
        if (thisOutcome && fromLookup.winningOutcome) {
          tokenWon = thisOutcome.toLowerCase().trim() === fromLookup.winningOutcome.toLowerCase().trim();
        }
      }
      market = {
        closed,
        tokenWon,
        currentPrice: fromLookup.currentPrice ?? null,
        question: fromLookup.question || fromLookup.title || '',
      };
    }
    // If lookup was missing OR had no resolution data and position is still held, hit Gamma
    if ((!market || (market.closed && market.tokenWon === null)) && pos.sharesHeld >= 0.01) {
      market = await gammaLookup(pos.tokenId) || market;
      gammaCalls++;
      if (gammaCalls % 5 === 0) await new Promise(r => setTimeout(r, 200));
    }
    const verdict = classify(pos, market);
    enriched.push({ ...pos, market, ...verdict });
  }

  // Aggregate
  let realized = 0, decided = 0, openMtm = 0;
  let wins = 0, losses = 0, open = 0, unknown = 0;
  let totalCost = 0;
  for (const e of enriched) {
    realized += e.realizedPnl;
    if (e.decidedPnl != null) decided += e.decidedPnl;
    if (e.status === 'open' && e.mtm != null) openMtm += e.mtm;
    if (e.outcome === 'win') wins++;
    else if (e.outcome === 'loss') losses++;
    else if (e.outcome === 'pending') open++;
    else unknown++;
    totalCost += e.totalBought;
  }
  const truePnl = realized + decided;
  const resolvedCount = wins + losses;
  const wr = resolvedCount > 0 ? wins / resolvedCount : null;
  const roi = totalCost > 0 ? truePnl / totalCost : null;

  if (wantJson) {
    console.log(JSON.stringify({
      address,
      positions: enriched.length,
      aggregates: {
        wins, losses, open, unknown,
        resolvedCount,
        winRate: wr,
        realizedPnl: +realized.toFixed(2),
        decidedUnredeemedPnl: +decided.toFixed(2),
        openMtm: +openMtm.toFixed(2),
        truePnl: +truePnl.toFixed(2),
        totalCapitalDeployed: +totalCost.toFixed(2),
        roi,
      },
      positionsDetail: verbose ? enriched : undefined,
    }, null, 2));
    return;
  }

  console.log('');
  console.log(`=== Ground-truth ledger for ${address} ===`);
  console.log('');
  console.log(`Lifetime positions:            ${enriched.length}`);
  console.log(`  - wins:                      ${wins}`);
  console.log(`  - losses:                    ${losses}`);
  console.log(`  - open (unresolved):         ${open}`);
  console.log(`  - unknown:                   ${unknown}`);
  console.log('');
  console.log(`Win rate (resolved only):      ${wr != null ? (wr * 100).toFixed(2) + '%' : 'n/a'}`);
  console.log(`Total capital deployed:        $${totalCost.toFixed(2)}`);
  console.log('');
  console.log(`Realized PnL (Goldsky):        $${realized.toFixed(2)}`);
  console.log(`Unredeemed decided PnL:        $${decided.toFixed(2)}  (wins never redeemed + worthless losers)`);
  console.log(`  --------`);
  console.log(`TRUE lifetime PnL:             $${truePnl.toFixed(2)}`);
  console.log(`ROI (truePnl / capital):       ${roi != null ? (roi * 100).toFixed(2) + '%' : 'n/a'}`);
  console.log(`Open positions mark-to-market: $${openMtm.toFixed(2)}`);
  console.log('');
  console.log(`(${gammaCalls} Gamma calls used for missing resolutions)`);

  if (verbose) {
    console.log('\n--- Per-position detail ---');
    const sorted = enriched.slice().sort((a, b) => Math.abs(b.truePnl || 0) - Math.abs(a.truePnl || 0));
    for (const p of sorted.slice(0, 30)) {
      const q = (p.market?.question || '').slice(0, 60);
      console.log(
        `  ${p.status.padEnd(20)} pnl=$${(p.truePnl || 0).toFixed(2).padStart(10)}  ` +
        `cost=$${p.totalBought.toFixed(2).padStart(10)}  shares=${p.sharesHeld.toFixed(2)}  ` +
        `avg=$${p.avgPrice.toFixed(3)}  ${q}`
      );
    }
    if (sorted.length > 30) console.log(`  ... and ${sorted.length - 30} more`);
  }
})().catch(err => {
  console.error('FAILED:', err);
  process.exit(1);
});
