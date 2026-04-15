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

// CLI args are only required when this file is the entrypoint — when
// required as a module (e.g. from batch-ledger.cjs) we skip validation
// so the require() doesn't blow up.
const args = process.argv.slice(2);
const address = args.find(a => /^0x[0-9a-fA-F]{40}$/.test(a))?.toLowerCase();
const wantJson = args.includes('--json');
const verbose = args.includes('--verbose') || args.includes('-v');

if (require.main === module && !address) {
  console.error('Usage: node scripts/wallet-ledger.cjs <0xaddress> [--json] [--verbose]');
  process.exit(1);
}

function loadGzJSON(p) {
  return JSON.parse(zlib.gunzipSync(fs.readFileSync(p)));
}

// Global concurrency limiter for Goldsky queries. Even with batch
// concurrency=2, the recursive parallel sharding inside a single
// fetchPositionsWithRange can fire 10 (or 100 at 2 levels deep) queries
// at once, trivially blowing past Goldsky's rate limit. This semaphore
// caps ALL in-flight gql() calls across the process.
const GQL_MAX_INFLIGHT = parseInt(process.env.GQL_MAX_INFLIGHT || '4', 10);
let gqlInflight = 0;
const gqlQueue = [];
function gqlAcquire() {
  return new Promise(resolve => {
    if (gqlInflight < GQL_MAX_INFLIGHT) {
      gqlInflight++;
      resolve();
    } else {
      gqlQueue.push(resolve);
    }
  });
}
function gqlRelease() {
  const next = gqlQueue.shift();
  if (next) next();
  else gqlInflight--;
}

async function gql(query, { retries = 5 } = {}) {
  await gqlAcquire();
  try {
    let lastErr;
    for (let attempt = 0; attempt < retries; attempt++) {
      try {
        const res = await fetch(GOLDSKY_PNL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ query }),
        });
        if (res.status === 429) {
          // Rate limited — back off with large increasing waits. Honour
          // Retry-After if Goldsky sends it.
          const retryAfter = parseInt(res.headers.get('retry-after') || '0', 10);
          const waitMs = Math.max(retryAfter * 1000, 2000 * Math.pow(2, attempt)); // 2s, 4s, 8s, 16s, 32s
          if (attempt < retries - 1) {
            await new Promise(r => setTimeout(r, waitMs));
            lastErr = new Error(`Goldsky 429 (waited ${waitMs}ms)`);
            continue;
          }
          throw new Error('Goldsky 429');
        }
        if (!res.ok) throw new Error(`Goldsky ${res.status}`);
        const data = await res.json();
        if (data.errors) {
          const msg = JSON.stringify(data.errors);
          if (/timeout/i.test(msg) && attempt < retries - 1) {
            await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
            lastErr = new Error(`GraphQL: ${msg}`);
            continue;
          }
          throw new Error(`GraphQL: ${msg}`);
        }
        return data.data;
      } catch (err) {
        lastErr = err;
        if (attempt < retries - 1 && /timeout|ECONNRESET|fetch failed|Goldsky 5/i.test(err.message)) {
          await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
          continue;
        }
        throw err;
      }
    }
    throw lastErr;
  } finally {
    gqlRelease();
  }
}

// Goldsky's user_position table isn't indexed on `user`, so filtering by
// `where: { user: "0x..." }` scans the whole table and times out. But the
// `id` field IS indexed (primary key) and follows the format
// `{user}-{tokenId}`, so we can filter by id_starts_with — which uses the
// primary-key index and returns instantly. Fall back to id-range bounds
// if the subgraph doesn't support _starts_with.
async function fetchPositions(addr) {
  const positions = [];
  let lastId = '';
  // Use two approaches in order: id_starts_with (cleanest if supported),
  // then id_gte/id_lt range bounds (universally supported).
  const tryStartsWith = true;
  while (positions.length < 10000) {
    const whereClauses = [];
    if (tryStartsWith) {
      whereClauses.push(`id_starts_with: "${addr}"`);
    } else {
      // ID range: everything lexicographically between addr and addr+"~".
      // "~" (0x7E) is greater than all hex chars and '-' (0x2D), so it
      // serves as an inclusive upper bound for any id that begins with addr.
      whereClauses.push(`id_gte: "${addr}"`, `id_lt: "${addr}~"`);
    }
    if (lastId) whereClauses.push(`id_gt: "${lastId}"`);
    const query = `{
      userPositions(
        first: 50
        orderBy: id
        where: { ${whereClauses.join(', ')} }
      ) {
        id
        tokenId
        amount
        avgPrice
        realizedPnl
        totalBought
      }
    }`;
    let data;
    try {
      data = await gql(query);
    } catch (err) {
      // If starts_with isn't supported, swap to range bounds and retry the batch
      if (tryStartsWith && /starts_with|Unknown argument|Cannot query/i.test(err.message)) {
        console.error('  id_starts_with not supported, falling back to id range');
        return fetchPositionsWithRange(addr);
      }
      throw err;
    }
    const items = data?.userPositions || [];
    if (items.length === 0) break;
    for (const it of items) {
      // Defensive: skip any rows where the id prefix doesn't match our address
      // (shouldn't happen with starts_with, but guards against lexicographic
      // surprises on the range path).
      if (!it.id.toLowerCase().startsWith(addr)) continue;
      const pos = {
        id: it.id,
        tokenId: it.tokenId,
        sharesHeld: parseFloat(it.amount || 0) / USDC_DIVISOR,
        avgPrice: parseFloat(it.avgPrice || 0) / USDC_DIVISOR,
        realizedPnl: parseFloat(it.realizedPnl || 0) / USDC_DIVISOR,
        totalBought: parseFloat(it.totalBought || 0) / USDC_DIVISOR,
      };
      // Skip phantom subgraph records (all-zero rows from intermediate sync
      // states). See fetchPositionsWithRange for full explanation.
      const isPhantom = pos.sharesHeld < 0.01
        && Math.abs(pos.realizedPnl) < 0.01
        && pos.totalBought < 0.01
        && pos.avgPrice < 0.001;
      if (isPhantom) continue;
      positions.push(pos);
    }
    if (items.length < 50) break;
    lastId = items[items.length - 1].id;
  }
  return positions;
}

// Fallback: id range bounds. Same result, more portable.
// For whales with thousands of positions even batch=50 can time out,
// so we shrink to batch=20 and auto-shard by first char of tokenId
// (0-9, since tokenIds are decimal) when a range still times out.
async function fetchPositionsWithRange(addr, rangeLo = addr, rangeHi = `${addr}~`, batch = 20) {
  const positions = [];
  let lastId = rangeLo;
  while (positions.length < 10000) {
    const query = `{
      userPositions(
        first: ${batch}
        orderBy: id
        where: { id_gt: "${lastId}", id_lt: "${rangeHi}" }
      ) {
        id tokenId amount avgPrice realizedPnl totalBought
      }
    }`;
    let data;
    try {
      // Only 1 retry here — if the range is too big, retrying the same
      // query won't help, we need to shard. Fail fast to get there.
      data = await gql(query, { retries: 1 });
    } catch (err) {
      // Range still timing out — shard this range into 10 sub-ranges on
      // the next char (tokenIds are decimal, so 0-9) and fire them in
      // parallel. Give up if we're already 3 levels deep.
      const suffix = rangeLo.slice(addr.length + 1); // chars after "{addr}-"
      if (/timeout/i.test(err.message) && suffix.length < 3) {
        console.error(`  range ${rangeLo.slice(0, 48)}.. timed out, sharding 10 sub-ranges in parallel`);
        const shardPromises = [];
        for (const d of '0123456789') {
          const lo = `${addr}-${suffix}${d}`;
          const hi = d === '9' ? rangeHi : `${addr}-${suffix}${String.fromCharCode(d.charCodeAt(0) + 1)}`;
          shardPromises.push(
            fetchPositionsWithRange(addr, lo, hi, batch)
              .then(p => { console.error(`    shard ${suffix}${d}: ${p.length} positions`); return p; })
              .catch(e => { console.error(`    shard ${suffix}${d} failed: ${e.message.slice(0, 80)}`); return []; })
          );
        }
        const shardResults = await Promise.all(shardPromises);
        return [...positions, ...shardResults.flat()];
      }
      throw err;
    }
    const items = data?.userPositions || [];
    if (items.length === 0) break;
    for (const it of items) {
      if (!it.id.toLowerCase().startsWith(addr)) continue;
      const pos = {
        id: it.id,
        tokenId: it.tokenId,
        sharesHeld: parseFloat(it.amount || 0) / USDC_DIVISOR,
        avgPrice: parseFloat(it.avgPrice || 0) / USDC_DIVISOR,
        realizedPnl: parseFloat(it.realizedPnl || 0) / USDC_DIVISOR,
        totalBought: parseFloat(it.totalBought || 0) / USDC_DIVISOR,
      };
      // Skip phantom subgraph records: rows where every numeric field is
      // zero. These aren't real positions — they show up when the subgraph
      // creates an entry during an intermediate sync state and then zeroes
      // it out. Counting them pollutes win/loss tallies and ROI math.
      const isPhantom = pos.sharesHeld < 0.01
        && Math.abs(pos.realizedPnl) < 0.01
        && pos.totalBought < 0.01
        && pos.avgPrice < 0.001;
      if (isPhantom) continue;
      positions.push(pos);
    }
    if (items.length < batch) break;
    lastId = items[items.length - 1].id;
  }
  return positions;
}

// Gamma lookup for a single tokenId. Tries two URL shapes because Gamma
// has historically been picky about which parameter name it accepts, and
// the token arrays inside the response can be JSON-encoded strings.
async function gammaLookup(tokenId) {
  const urls = [
    `${GAMMA_MARKETS}?clob_token_ids=${tokenId}&limit=1`,
    `${GAMMA_MARKETS}?clobTokenIds=${tokenId}&limit=1`,
  ];
  for (const url of urls) {
    try {
      const res = await fetch(url);
      if (!res.ok) continue;
      const arr = await res.json();
      if (!Array.isArray(arr) || arr.length === 0) continue;
      const m = arr[0];
      let tokens = m.clobTokenIds;
      if (typeof tokens === 'string') { try { tokens = JSON.parse(tokens); } catch { tokens = null; } }
      let outcomes = m.outcomes;
      if (typeof outcomes === 'string') { try { outcomes = JSON.parse(outcomes); } catch { outcomes = null; } }
      let prices = m.outcomePrices;
      if (typeof prices === 'string') { try { prices = JSON.parse(prices); } catch { prices = null; } }
      const closed = m.closed === true || m.closed === 'true';
      let idx = -1;
      if (Array.isArray(tokens)) {
        idx = tokens.findIndex(t => String(t) === String(tokenId));
      }
      let tokenWon = null;
      let currentPrice = null;
      if (idx !== -1 && Array.isArray(prices)) {
        const p = parseFloat(prices[idx] || 0);
        currentPrice = p;
        if (closed) {
          // Be more permissive — some closed markets settle at 0.97-0.99
          // or 0.01-0.03 rather than exactly 1/0.
          if (p >= 0.95) tokenWon = true;
          else if (p <= 0.05) tokenWon = false;
        }
      }
      // If market is closed but we couldn't determine tokenWon from price,
      // try the `winner` field on the market itself + our outcome position.
      if (closed && tokenWon === null && m.winner && Array.isArray(outcomes) && idx !== -1) {
        const ourOutcome = outcomes[idx];
        if (ourOutcome) {
          tokenWon = String(ourOutcome).toLowerCase().trim() === String(m.winner).toLowerCase().trim();
        }
      }
      return {
        closed,
        tokenWon,
        currentPrice,
        question: m.question || '',
      };
    } catch {
      continue;
    }
  }
  return null;
}

function classify(pos, market) {
  // Fully closed: realizedPnl is the whole story.
  if (pos.sharesHeld < 0.01) {
    // Distinguish three sub-cases:
    //   realizedPnl > 0  → closed win (sold/redeemed for profit)
    //   realizedPnl < 0  → closed loss (sold at a loss, ate the cost)
    //   realizedPnl ≈ 0 AND totalBought ≈ 0  → not a real position, skip
    //     (but we already filtered these as phantoms upstream)
    //   realizedPnl ≈ 0 AND totalBought > 0  → closed at breakeven, rare
    //     but possible when someone buys and sells at the same price
    if (Math.abs(pos.realizedPnl) < 0.01 && pos.totalBought < 0.01) {
      return {
        status: 'phantom',
        outcome: 'phantom',
        truePnl: 0,
        decidedPnl: 0,
      };
    }
    if (pos.realizedPnl > 0.01) {
      return { status: 'closed_win', outcome: 'win', truePnl: pos.realizedPnl, decidedPnl: 0 };
    }
    if (pos.realizedPnl < -0.01) {
      return { status: 'closed_loss', outcome: 'loss', truePnl: pos.realizedPnl, decidedPnl: 0 };
    }
    // Breakeven — traded in and out at same price, or scaled out at cost.
    return {
      status: 'closed_breakeven',
      outcome: 'scratch',
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

// Core analyzer — exported so batch runners can reuse it without
// reimplementing the reconcile/classify pipeline. Pass in a preloaded
// marketLookup Map to avoid reloading analytics.json.gz per wallet.
async function analyzeWallet(addr, { marketLookup = new Map(), quiet = false } = {}) {
  const log = quiet ? () => {} : (msg) => console.error(msg);
  log(`Fetching Goldsky positions for ${addr}...`);
  const positions = await fetchPositions(addr);
  log(`Got ${positions.length} positions from subgraph.`);

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
  let wins = 0, losses = 0, open = 0, scratch = 0, unresolvedMarket = 0, closedUndetermined = 0;
  let totalCost = 0;
  let openCapitalAtRisk = 0;
  for (const e of enriched) {
    realized += e.realizedPnl;
    if (e.decidedPnl != null) decided += e.decidedPnl;
    if (e.status === 'open' && e.mtm != null) openMtm += e.mtm;
    if (e.outcome === 'win') wins++;
    else if (e.outcome === 'loss') losses++;
    else if (e.outcome === 'pending') { open++; openCapitalAtRisk += e.totalBought; }
    else if (e.outcome === 'scratch') scratch++;
    else if (e.status === 'closed_undetermined') closedUndetermined++;
    else if (e.status === 'unknown') unresolvedMarket++;
    // 'phantom' status intentionally not counted — they're subgraph noise.
    totalCost += e.totalBought;
  }
  const truePnl = realized + decided;
  const resolvedCount = wins + losses;
  const wr = resolvedCount > 0 ? wins / resolvedCount : null;
  const roi = totalCost > 0 ? truePnl / totalCost : null;
  // decidedROI = pure resolved ROI (excludes open MtM), which is what
  // scoring should key on — open positions are still "pending" truth.
  const decidedCapital = totalCost - openCapitalAtRisk;
  const decidedROI = decidedCapital > 0 ? (realized + decided) / decidedCapital : null;

  return {
    address: addr,
    positions: enriched,
    aggregates: {
      total: enriched.length,
      wins, losses, open, scratch, closedUndetermined, unresolvedMarket,
      resolvedCount,
      winRate: wr,
      realizedPnl: +realized.toFixed(2),
      decidedUnredeemedPnl: +decided.toFixed(2),
      openMtm: +openMtm.toFixed(2),
      openCapitalAtRisk: +openCapitalAtRisk.toFixed(2),
      truePnl: +truePnl.toFixed(2),
      totalCapitalDeployed: +totalCost.toFixed(2),
      decidedCapitalDeployed: +decidedCapital.toFixed(2),
      roi,
      decidedROI,
    },
    gammaCalls,
  };
}

module.exports = { analyzeWallet, fetchPositions, gammaLookup, classify };

// CLI entry point — only runs when invoked directly
if (require.main === module) (async function main() {
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

  const result = await analyzeWallet(address, { marketLookup });
  const { aggregates: a, positions: enriched, gammaCalls } = result;
  const {
    wins, losses, open, scratch, closedUndetermined, unresolvedMarket,
    winRate: wr, realizedPnl: realized, decidedUnredeemedPnl: decided,
    openMtm, openCapitalAtRisk, truePnl, totalCapitalDeployed: totalCost,
    roi, decidedROI,
  } = a;

  if (wantJson) {
    console.log(JSON.stringify({
      address,
      positions: enriched.length,
      aggregates: a,
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
  console.log(`  - open (unresolved):         ${open}  ($${openCapitalAtRisk.toFixed(2)} at risk)`);
  console.log(`  - scratch (breakeven):       ${scratch}`);
  console.log(`  - closed, winner undetermined: ${closedUndetermined}`);
  console.log(`  - market not found:          ${unresolvedMarket}`);
  console.log('');
  console.log(`Win rate (resolved only):      ${wr != null ? (wr * 100).toFixed(2) + '%' : 'n/a'}  (${wins}W / ${losses}L)`);
  console.log(`Total capital deployed:        $${totalCost.toFixed(2)}`);
  console.log('');
  console.log(`Realized PnL (Goldsky):        $${realized.toFixed(2)}`);
  console.log(`Unredeemed decided PnL:        $${decided.toFixed(2)}  (wins never redeemed + worthless losers)`);
  console.log(`  --------`);
  console.log(`TRUE lifetime PnL:             $${truePnl.toFixed(2)}`);
  console.log(`ROI (truePnl / total capital): ${roi != null ? (roi * 100).toFixed(2) + '%' : 'n/a'}`);
  console.log(`Decided ROI (resolved only):   ${decidedROI != null ? (decidedROI * 100).toFixed(2) + '%' : 'n/a'}`);
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
