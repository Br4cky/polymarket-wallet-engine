/**
 * Polymarket Signal Engine - Core Library
 * Provides GraphQL queries, position fetching, scoring, and analytics
 */

import fs from 'fs';
import path from 'path';
import zlib from 'zlib';

const GOLDSKY_PNL = 'https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/pnl-subgraph/0.0.14/gn';
// GOLDSKY_POSITIONS endpoint removed — unused, all position data comes from PNL subgraph
const GAMMA_MARKETS = 'https://gamma-api.polymarket.com/markets';
const USDC_DIVISOR = 1e6;

// ============================================================================
// GraphQL Utilities
// ============================================================================

/**
 * Execute a GraphQL query against a subgraph endpoint
 * @param {string} endpoint - The GraphQL endpoint URL
 * @param {string} query - The GraphQL query string
 * @returns {Promise<any>} The data field from the response
 * @throws {Error} If the request fails or returns GraphQL errors
 */
async function gqlQuery(endpoint, query) {
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query }),
  });

  if (!response.ok) {
    throw new Error(`GraphQL request failed: ${response.status} ${response.statusText}`);
  }

  const json = await response.json();
  if (json.errors) {
    throw new Error(`GraphQL errors: ${JSON.stringify(json.errors)}`);
  }

  return json.data;
}

/**
 * Introspect the schema to get all queryable entity names
 * Uses queryType fields (same approach as the browser screener)
 * @param {string} endpoint - The GraphQL endpoint URL
 * @returns {Promise<string[]>} Array of entity names
 */
async function introspectSchema(endpoint) {
  const data = await gqlQuery(endpoint, `{ __schema { queryType { fields { name } } } }`);
  return data.__schema.queryType.fields
    .filter(f => !f.name.startsWith('_'))
    .map(f => f.name);
}

/**
 * Introspect an entity to get its field names
 * @param {string} endpoint - The GraphQL endpoint URL
 * @param {string} entityName - The entity name to introspect
 * @returns {Promise<string[]>} Array of field names
 */
async function introspectEntity(endpoint, entityName) {
  const query = `
    {
      __type(name: "${entityName}") {
        fields {
          name
        }
      }
    }
  `;

  const data = await gqlQuery(endpoint, query);
  if (!data.__type) return [];

  return data.__type.fields.map((f) => f.name);
}

/**
 * Discover position-like entities and their relevant fields
 * @param {string} endpoint - The GraphQL endpoint URL
 * @returns {Promise<Array>} Array of {entity, fields: {user, pnl, token, totalBought, amount}, endpoint}
 */
async function discoverEntities(endpoint) {
  const entities = await introspectSchema(endpoint);
  console.log(`  Available entities: ${entities.join(', ')}`);

  // Filter for position-like entities, excluding user-like entities
  // (same fix as browser screener: userPositions contains "user" but is a position entity)
  const positionLike = entities.filter(e =>
    /position|trade|order|fill/i.test(e) && !e.endsWith('s_') && !e.startsWith('_')
  );

  const discovered = [];

  for (const entity of positionLike) {
    try {
      // Try to introspect the type - try PascalCase singular form first
      let typeName = entity.charAt(0).toUpperCase() + entity.slice(1);
      if (typeName.endsWith('s')) typeName = typeName.slice(0, -1);
      let fields = await introspectEntity(endpoint, typeName);
      if (!fields.length) fields = await introspectEntity(endpoint, entity);
      if (!fields.length) continue;

      console.log(`  ${entity} fields: ${fields.join(', ')}`);

      // Find relevant fields by pattern matching
      const userField = fields.find(f => /user|account|trader|owner|maker/i.test(f));
      const pnlField = fields.find(f => /pnl|profit|realized/i.test(f));
      const tokenField = fields.find(f => /tokenId|token/i.test(f));
      const totalBoughtField = fields.find(f => /totalBought|total_bought|volume/i.test(f));
      const amountField = fields.find(f => /^amount$/i.test(f));

      if (!userField) { console.log(`    Skipping ${entity}: no user field`); continue; }

      discovered.push({
        entity,
        fields: {
          user: userField,
          pnl: pnlField,
          token: tokenField,
          totalBought: totalBoughtField,
          amount: amountField,
        },
        endpoint,
      });
      console.log(`    ✓ Using ${entity} (user=${userField}, pnl=${pnlField}, token=${tokenField})`);
    } catch (e) {
      console.log(`    Skipping ${entity}: ${e.message}`);
    }
  }

  return discovered;
}

/**
 * Fetch positions from a subgraph with pagination
 * Batches are fetched in groups of 1000 using id_gt pagination
 * @param {string} endpoint - The GraphQL endpoint
 * @param {string} entity - The entity name
 * @param {object} fields - {user, pnl, token, totalBought, amount} field mappings
 * @param {string} lastId - Starting cursor (empty string for first fetch)
 * @param {number} maxBatch - Maximum number of items to fetch across all batches
 * @returns {Promise<{items: Array, lastId: string}>} Fetched items and cursor
 */
async function fetchPositions(endpoint, entity, fields, lastId = '', maxBatch = 200000) {
  const items = [];
  let cursor = lastId;
  const batchSize = 1000;
  let useNested = false;

  // Build field string for flat query
  let fieldStr = `id ${fields.user}`;
  if (fields.pnl) fieldStr += ` ${fields.pnl}`;
  if (fields.token) fieldStr += ` ${fields.token}`;
  if (fields.totalBought) fieldStr += ` ${fields.totalBought}`;
  if (fields.amount) fieldStr += ` ${fields.amount}`;

  // Build field string for nested user query (user { id })
  let nestedFieldStr = `id ${fields.user} { id }`;
  if (fields.pnl) nestedFieldStr += ` ${fields.pnl}`;
  if (fields.token) nestedFieldStr += ` ${fields.token}`;
  if (fields.totalBought) nestedFieldStr += ` ${fields.totalBought}`;
  if (fields.amount) nestedFieldStr += ` ${fields.amount}`;

  // Use the entity name as-is for the query (the subgraph uses plural entity names directly)
  const entityName = entity;

  while (items.length < maxBatch) {
    const remaining = maxBatch - items.length;
    const fetchSize = Math.min(batchSize, remaining);

    let batch;
    try {
      if (useNested) throw new Error('use nested');
      const query = `{ ${entityName}(first: ${fetchSize}, where: { id_gt: "${cursor}" }) { ${fieldStr} } }`;
      const data = await gqlQuery(endpoint, query);
      batch = data?.[entityName] || [];
    } catch {
      try {
        const query = `{ ${entityName}(first: ${fetchSize}, where: { id_gt: "${cursor}" }) { ${nestedFieldStr} } }`;
        const data = await gqlQuery(endpoint, query);
        batch = data?.[entityName] || [];
        useNested = true;
      } catch (err) {
        console.error(`  Error fetching from ${entityName}:`, err.message);
        break;
      }
    }

    if (!batch || batch.length === 0) break;

    // Normalize and process batch items
    for (const item of batch) {
      const uid = typeof item[fields.user] === 'object' ? item[fields.user]?.id : item[fields.user];
      if (!uid) continue;

      const normalized = {
        uid: item.id,
        user: uid,
        pnl: fields.pnl ? parseFloat(item[fields.pnl] || 0) / USDC_DIVISOR : 0,
        tokenId: fields.token ? (item[fields.token] || null) : null,
        totalBought: fields.totalBought ? parseFloat(item[fields.totalBought] || 0) / USDC_DIVISOR : 0,
        amount: fields.amount ? parseFloat(item[fields.amount] || 0) / USDC_DIVISOR : 0,
      };

      if (normalized.user) {
        items.push(normalized);
      }
    }

    cursor = batch[batch.length - 1]?.id || cursor;

    if (items.length % 10000 < batchSize) {
      console.log(`    ${items.length.toLocaleString()} positions fetched...`);
    }

    if (batch.length < batchSize) {
      console.log(`    Exhausted all data at ${items.length.toLocaleString()} positions`);
      break;
    }

    // Add delay to avoid rate limiting
    await new Promise((resolve) => setTimeout(resolve, 200));
  }

  return { items, lastId: cursor };
}

/**
 * Refresh positions for tracked wallets by re-querying the subgraph.
 * Detects position closures (amount going to 0), PnL changes, and new positions.
 * @param {string} endpoint - The GraphQL endpoint
 * @param {string} entityName - The entity name (e.g. "userPositions")
 * @param {object} fields - {user, pnl, token, totalBought, amount} field mappings
 * @param {Object} wallets - wallets map (address → {positions, ...}) — mutated in place
 * @param {number} scanIndex - Current scan index
 * @param {string} scanTimestamp - Current scan ISO timestamp
 * @param {number} [delay=200] - Delay between batches in ms
 * @returns {Promise<{refreshed: number, newPositions: number, closures: number}>}
 */
async function refreshTrackedWallets(endpoint, entityName, fields, wallets, scanIndex, scanTimestamp, delay = 200) {
  // Skip tombstoned/removed/contaminated wallets — don't waste API calls on them
  const addresses = Object.keys(wallets).filter(addr => {
    const status = wallets[addr].status;
    return status !== 'removed' && status !== 'contaminated';
  });
  let totalRefreshed = 0;
  let totalNew = 0;
  let totalClosures = 0;

  // Build field strings (same logic as fetchPositions)
  let fieldStr = `id ${fields.user}`;
  if (fields.pnl) fieldStr += ` ${fields.pnl}`;
  if (fields.token) fieldStr += ` ${fields.token}`;
  if (fields.totalBought) fieldStr += ` ${fields.totalBought}`;
  if (fields.amount) fieldStr += ` ${fields.amount}`;
  if (fields.avgPrice) fieldStr += ` ${fields.avgPrice}`;

  let nestedFieldStr = `id ${fields.user} { id }`;
  if (fields.pnl) nestedFieldStr += ` ${fields.pnl}`;
  if (fields.token) nestedFieldStr += ` ${fields.token}`;
  if (fields.totalBought) nestedFieldStr += ` ${fields.totalBought}`;
  if (fields.amount) nestedFieldStr += ` ${fields.amount}`;
  if (fields.avgPrice) nestedFieldStr += ` ${fields.avgPrice}`;

  let useNested = false;

  for (let i = 0; i < addresses.length; i++) {
    const address = addresses[i];
    const wallet = wallets[address];

    // Query all positions for this wallet using id range filter
    // Position IDs are formatted as "{address}-{tokenId}"
    const idPrefix = address.toLowerCase();
    let allPositions = [];
    let cursor = '';

    // Paginate through all positions for this wallet
    while (true) {
      let batch = [];
      const whereClause = cursor
        ? `id_gt: "${cursor}", id_gte: "${idPrefix}-", id_lt: "${idPrefix}~"`
        : `id_gte: "${idPrefix}-", id_lt: "${idPrefix}~"`;

      // Retry once on timeout/error before giving up
      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          if (useNested) throw new Error('use nested');
          const q = `{ ${entityName}(first: 1000, where: { ${whereClause} }) { ${fieldStr} } }`;
          const data = await gqlQuery(endpoint, q);
          batch = data?.[entityName] || [];
          break; // success
        } catch {
          try {
            const q = `{ ${entityName}(first: 1000, where: { ${whereClause} }) { ${nestedFieldStr} } }`;
            const data = await gqlQuery(endpoint, q);
            batch = data?.[entityName] || [];
            useNested = true;
            break; // success
          } catch (err) {
            if (attempt === 0) {
              // First failure — wait and retry
              await new Promise(resolve => setTimeout(resolve, 2000));
              continue;
            }
            console.error(`    Error refreshing ${address.slice(0, 10)}...: ${err.message}`);
            batch = [];
            break;
          }
        }
      }

      if (!batch || batch.length === 0) break;
      allPositions = allPositions.concat(batch);
      cursor = batch[batch.length - 1]?.id || '';
      if (batch.length < 1000) break;
    }

    // Build a map of existing positions by uid for fast lookup
    const existingByUid = new Map();
    for (const p of (wallet.positions || [])) {
      existingByUid.set(p.uid, p);
    }

    // Process fresh positions from subgraph
    for (const item of allPositions) {
      const uid = item.id;
      const pnl = fields.pnl ? parseFloat(item[fields.pnl] || 0) / USDC_DIVISOR : 0;
      const tokenId = fields.token ? (item[fields.token] || null) : null;
      const totalBought = fields.totalBought ? parseFloat(item[fields.totalBought] || 0) / USDC_DIVISOR : 0;
      const amount = fields.amount ? parseFloat(item[fields.amount] || 0) / USDC_DIVISOR : 0;
      const avgPrice = fields.avgPrice ? parseFloat(item[fields.avgPrice] || 0) / USDC_DIVISOR : null;

      const existing = existingByUid.get(uid);

      if (existing) {
        // Track closures: was open (amount > 0.01), now closed (amount ≈ 0)
        const wasOpen = (existing.amount || 0) > 0.01;
        const nowClosed = amount <= 0.01;
        if (wasOpen && nowClosed) {
          totalClosures++;
          // Stamp when the position actually resolved
          existing.resolvedTimestamp = scanTimestamp;
        }

        // Track PnL changes as real activity signals
        if (Math.abs((existing.pnl || 0) - pnl) > 0.01) {
          existing.pnlChangedThisScan = true;
        }

        // Update with fresh data, preserve original firstSeenTimestamp and discoveredScan
        existing.pnl = pnl;
        existing.totalBought = totalBought;
        existing.amount = amount;
        existing.avgPrice = avgPrice;
        existing.scanIndex = scanIndex;
        // Don't overwrite firstSeenTimestamp or discoveredScan — keep originals
      } else {
        // Brand new position for this tracked wallet
        const newPos = {
          uid,
          pnl,
          tokenId,
          totalBought,
          amount,
          avgPrice,
          scanIndex,
          firstSeenTimestamp: scanTimestamp,
          discoveredScan: scanIndex, // Track which scan first found this position
          isNewThisScan: true,       // Flag for activity detection
        };
        // If discovered already resolved, DON'T stamp resolvedTimestamp
        // (we don't know when it actually resolved — just that it was already closed)
        // Only stamp resolvedTimestamp when we actually SEE a position go from open → closed
        wallet.positions.push(newPos);
        totalNew++;
      }
    }

    wallet.lastSeen = scanIndex;
    totalRefreshed++;

    if ((i + 1) % 10 === 0 || i === addresses.length - 1) {
      console.log(`    Refreshed ${i + 1}/${addresses.length} wallets (${totalNew} new, ${totalClosures} closures)...`);
    }

    await new Promise((resolve) => setTimeout(resolve, delay));
  }

  return { refreshed: totalRefreshed, newPositions: totalNew, closures: totalClosures };
}

// ============================================================================
// Scoring Functions (matching existing screener formulas)
// ============================================================================

/**
 * Analyze positions to compute statistical metrics
 * @param {Array} positions - Array of {pnl, tokenId, totalBought, amount, scanIndex}
 * @returns {object} Statistics object
 */
function analyzePositions(positions, marketLookup = null) {
  if (!positions || positions.length === 0) {
    return {
      wins: 0,
      losses: 0,
      resolved: 0,
      wr: 0,
      avgW: 0,
      avgL: 0,
      totalPnl: 0,
      realizedPnl: 0,
      unrealizedPnl: 0,
      totalVolume: 0,
      uniqueTokens: 0,
      estimatedMarkets: 0,
      efficiency: 0,
      edgeRatio: 0,
      openCount: 0,
      maxScanIndex: 0,
      tradingDays: 0,
      positionsPerWeek: 0,
      newPositionsThisScan: 0,
      suspiciousWinRate: false,
      openProfitable: 0,
      openLosing: 0,
    };
  }

  // --- Position classification ---
  // A position is "resolved" when we can determine the outcome:
  //   1. amount ≤ 0.01 (wallet redeemed/sold) — use PnL to determine win/loss
  //   2. amount > 0.01 BUT market is closed (marketClosed === true) — wallet holds
  //      unredeemed shares. Use winningOutcome to determine if they picked correctly.
  //      Unredeemed losing shares are worthless; unredeemed winning shares have value.
  //      Without this check, unredeemed losses hide as "open" positions and inflate WR.
  // A position is "open" only if amount > 0.01 AND the market is NOT closed.
  let wins = 0;
  let losses = 0;
  let winSum = 0;   // total $ won across winning resolved positions
  let lossSum = 0;  // total $ lost across losing resolved positions

  let totalPnl = 0;
  let realizedPnl = 0;   // resolved positions (redeemed + unredeemed on closed markets)
  let unrealizedPnl = 0; // genuinely open positions on live markets
  let totalVolume = 0;
  let openCount = 0;
  let openProfitable = 0;
  let openLosing = 0;
  let maxScanIndex = 0;

  const uniqueTokens = new Set();

  for (const pos of positions) {
    const { pnl, tokenId, totalBought, amount, scanIndex } = pos;

    // Skip dust-level positions — sub-$1 bets are noise, not signal
    if (totalBought < 1) continue;

    totalPnl += pnl;
    totalVolume += totalBought;
    uniqueTokens.add(tokenId);

    if (scanIndex > maxScanIndex) maxScanIndex = scanIndex;

    const hasAmount = (amount || 0) > 0.01;

    // Look up market resolution data
    let marketInfo = null;
    if (marketLookup && tokenId) {
      marketInfo = typeof marketLookup.get === 'function'
        ? marketLookup.get(tokenId)
        : marketLookup[tokenId];
    }
    const marketClosed = marketInfo && marketInfo.marketClosed === true;

    if (!hasAmount && totalBought > 0.01) {
      // Case 1: Wallet redeemed/sold — position amount is ~0
      // Use PnL directly to determine win/loss
      realizedPnl += pnl;
      if (pnl > 0) {
        wins++;
        winSum += pnl;
      } else if (pnl < 0) {
        losses++;
        lossSum += -pnl;
      }
    } else if (hasAmount && marketClosed) {
      // Case 2: Wallet still holds shares but market is resolved
      // Determine outcome by checking if wallet's side matches the winner
      const walletOutcome = (marketInfo.outcome || '').toLowerCase().trim();
      const winner = (marketInfo.winningOutcome || '').toLowerCase().trim();

      if (winner && walletOutcome) {
        // Check if wallet picked the winning side
        const isWin = walletOutcome === winner ||
          (walletOutcome.length >= 4 && winner.length >= 4 && (
            winner.includes(walletOutcome) || walletOutcome.includes(winner)
          ));

        if (isWin) {
          // Unredeemed winning shares — worth $1 each minus entry cost
          const entryPrice = pos.avgPrice || (amount > 0 ? Math.min(1, totalBought / amount) : 0.5);
          const impliedPnl = amount * (1 - entryPrice);
          wins++;
          winSum += Math.max(0, impliedPnl);
          realizedPnl += Math.max(0, impliedPnl);
        } else {
          // Unredeemed losing shares — worthless
          losses++;
          lossSum += totalBought; // Lost what they paid
          realizedPnl -= totalBought;
        }
      } else if (pnl < -0.01) {
        // Market closed but no winningOutcome data — use PnL as fallback
        losses++;
        lossSum += -pnl;
        realizedPnl += pnl;
      } else if (pnl > 0.01) {
        wins++;
        winSum += pnl;
        realizedPnl += pnl;
      }
    } else if (hasAmount) {
      // Case 3: Genuinely open position on a live market
      openCount++;
      unrealizedPnl += pnl;
      if (pnl > 0.01) openProfitable++;
      else if (pnl < -0.01) openLosing++;
    }
  }

  const resolved = wins + losses;
  const wr = resolved > 0 ? wins / resolved : 0;
  const avgW = wins > 0 ? winSum / wins : 0;
  const avgL = losses > 0 ? lossSum / losses : 0;
  const uniqueTokenCount = uniqueTokens.size;
  // Count unique markets using groupId when market data is available.
  // This handles multi-outcome markets correctly (e.g. 30-team NBA market = 1 market, not 15).
  // Falls back to ceil(tokens/2) when no market lookup is provided.
  let estimatedMarkets;
  if (marketLookup) {
    const uniqueGroups = new Set();
    for (const tokenId of uniqueTokens) {
      const mi = typeof marketLookup.get === 'function' ? marketLookup.get(tokenId) : marketLookup[tokenId];
      const groupId = mi?.groupId || tokenId;
      uniqueGroups.add(groupId);
    }
    estimatedMarkets = Math.max(1, uniqueGroups.size);
  } else {
    estimatedMarkets = Math.max(1, Math.ceil(uniqueTokenCount / 2));
  }

  // Efficiency: PnL per dollar traded (same as original screener)
  const efficiency = totalVolume > 0 ? totalPnl / totalVolume : 0;

  // Edge ratio: average win / average loss — capped at 10 to avoid absurd values
  const edgeRatio = avgL > 0 ? Math.min(10, avgW / avgL) : (avgW > 0 ? 10 : 0);

  // Activity rate metrics — count unique scanIndexes (each scan = 6h interval)
  // This measures how many distinct scans found NEW positions, not total portfolio size
  const scanIndexes = new Set();
  const uniqueDays = new Set();
  let earliestTs = null;
  let latestTs = null;
  let newPositionsThisScan = 0;
  for (const pos of positions) {
    if (pos.discoveredScan) {
      scanIndexes.add(pos.discoveredScan);
    }
    if (pos.firstSeenTimestamp) {
      const day = pos.firstSeenTimestamp.slice(0, 10); // YYYY-MM-DD
      uniqueDays.add(day);
      if (!earliestTs || pos.firstSeenTimestamp < earliestTs) earliestTs = pos.firstSeenTimestamp;
      if (!latestTs || pos.firstSeenTimestamp > latestTs) latestTs = pos.firstSeenTimestamp;
    }
    if (pos.isNewThisScan) newPositionsThisScan++;
  }
  const tradingDays = uniqueDays.size;
  // Use actual scan span for weeks tracked — each scan is ~6 hours apart
  const scansActive = scanIndexes.size || 1;
  const weeksTracked = earliestTs && latestTs
    ? Math.max(1, (new Date(latestTs) - new Date(earliestTs)) / (7 * 24 * 60 * 60 * 1000))
    : 1;
  // positionsPerWeek: only count positions that have discoveredScan set (real new entries)
  // divided by weeks actually tracked, not total portfolio size / 1
  const discoveredPositions = positions.filter(p => p.discoveredScan).length;
  const positionsPerWeek = weeksTracked > 0.5
    ? +(discoveredPositions / weeksTracked).toFixed(1)
    : +(discoveredPositions).toFixed(1);

  // Flag suspiciously perfect win rates — market makers / arbitrageurs
  // Market maker detection: 100% WR with zero losses needs a large sample to be suspicious
  // A wallet going 10/10 could be a skilled trader; 50/50 with 0 losses is almost certainly automated
  const suspiciousWinRate = (wr >= 0.99 && losses === 0 && resolved >= 50);

  // Flag bot-like activity — wallets trading at inhuman frequency
  // 500/week = ~70/day which is aggressive but possible for active traders
  // 1000/week = ~140/day which is almost certainly automated
  const isBotLike = (positionsPerWeek > 1000);

  // Combined contamination flag
  const isContaminated = suspiciousWinRate || isBotLike;

  return {
    wins,
    losses,
    resolved,
    wr,
    avgW,
    avgL,
    totalPnl,
    realizedPnl,
    unrealizedPnl,
    totalVolume,
    uniqueTokens: uniqueTokenCount,
    estimatedMarkets,
    efficiency,
    edgeRatio,
    openCount,
    openProfitable,
    openLosing,
    maxScanIndex,
    tradingDays,
    positionsPerWeek,
    newPositionsThisScan,
    suspiciousWinRate,
    isBotLike,
    isContaminated,
  };
}

/**
 * Compute a composite score from 0-100
 * Weights: WR (25) + Markets (15) + Efficiency (15) + Edge (10) + Sample (15) + Activity (20)
 * Then applies recency multiplier and suspicion penalty.
 * @param {object} stats - Statistics from analyzePositions
 * @param {string} [lastActiveTimestamp] - ISO timestamp of last real activity
 * @returns {number} Score 0-100
 */
function computeScore(stats, lastActiveTimestamp) {
  const { resolved, wr } = stats;
  const sampleFactor = resolved > 0 ? Math.min(1, Math.sqrt(resolved) / 10) : 0;

  // Win rate component (25 pts): wr * sampleFactor * 25
  const wrScore = wr * sampleFactor * 25;
  // Market diversity (15 pts): min(1, estimatedMarkets/50) * 15
  const estimatedMarkets = stats.estimatedMarkets || Math.max(1, Math.ceil((stats.uniqueTokens || 0) / 2));
  const marketScore = Math.min(1, estimatedMarkets / 50) * 15;
  // Profit efficiency (15 pts): use log scale to avoid saturation
  // Old cap was 10% which 51% of wallets hit. Now use log scale: log10(1 + eff*100) / 2
  const rawEff = Math.max(0, stats.efficiency || 0);
  const efficiencyScore = Math.min(1, Math.log10(1 + rawEff * 100) / 2) * 15;
  // Edge ratio (10 pts): use log scale to avoid saturation at 3.0
  // Old: min(1, (edge-0.5)/2.5). Now: log2(1+max(0,edge-0.5)) / 3
  const rawEdge = Math.max(0, (stats.edgeRatio || 0) - 0.5);
  const edgeScore = Math.min(1, Math.log2(1 + rawEdge) / 3) * 10;
  // Sample size (15 pts): min(1, resolved/200) * 15
  const sampleScore = Math.min(1, resolved / 200) * 15;
  // Activity component (20 pts) — rewards wallets that actively trade
  // Based on positions per week and trading days, with diminishing returns
  const ppw = stats.positionsPerWeek || 0;
  const activityScore = Math.min(1, Math.log10(1 + ppw) / 2) * 12 +
    Math.min(1, (stats.tradingDays || 0) / 14) * 8;

  let rawScore = wrScore + marketScore + efficiencyScore + edgeScore + sampleScore + activityScore;

  // Recency multiplier — penalise stale wallets
  let recencyMultiplier = 1.0;
  if (lastActiveTimestamp) {
    const daysSince = (Date.now() - new Date(lastActiveTimestamp).getTime()) / (1000 * 60 * 60 * 24);
    if (daysSince > 90) recencyMultiplier = 0.5;
    else if (daysSince > 30) recencyMultiplier = 0.75;
    else if (daysSince > 14) recencyMultiplier = 0.85;
    else if (daysSince > 7) recencyMultiplier = 0.9;
    stats.recencyMultiplier = recencyMultiplier;
    stats.daysSinceActive = Math.round(daysSince);
  }

  // Contaminated wallets (bots/MMs) are excluded entirely in scan.js
  // Set contaminationType for downstream use but no score penalty needed —
  // they'll be purged before scoring matters
  if (stats.isContaminated) {
    if (stats.suspiciousWinRate && stats.isBotLike) stats.contaminationType = 'mm_bot';
    else if (stats.suspiciousWinRate) stats.contaminationType = 'market_maker';
    else if (stats.isBotLike) stats.contaminationType = 'bot';
  }

  return rawScore * recencyMultiplier;
}

// ============================================================================
// Market Resolution
// ============================================================================

/**
 * Resolve market data from Gamma API
 * @param {Set} tokenIds - Set of token IDs to resolve
 * @param {Function} [onCheckpoint] - Optional callback(lookup) called every ~5000 tokens to save progress
 * @returns {Promise<Map>} Map of tokenId → {title, slug, category, image}
 */
async function resolveMarkets(tokenIds, onCheckpoint) {
  if (tokenIds.size === 0) return new Map();

  const lookup = new Map();
  const idsSet = new Set(tokenIds); // for fast has() checks
  const ids = Array.from(tokenIds);
  const CONCURRENCY = 5; // parallel requests — conservative to avoid 429s
  let queried = 0;
  let errors = 0;
  let delay = 100; // adaptive delay in ms — increases on 429, decreases on success

  /**
   * Fetch a single token from Gamma API with retry on 429
   */
  async function fetchOne(tokenId) {
    if (lookup.has(tokenId)) return;

    const MAX_RETRIES = 3;
    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      try {
        const url = `${GAMMA_MARKETS}?clob_token_ids=${tokenId}&limit=1`;
        const response = await fetch(url);

        if (response.status === 429) {
          // Rate limited — back off exponentially
          const backoff = Math.min(5000, 500 * Math.pow(2, attempt));
          delay = Math.min(500, delay + 50); // slow down future batches too
          if (attempt < MAX_RETRIES) {
            await new Promise(resolve => setTimeout(resolve, backoff));
            continue;
          }
          errors++;
          if (errors <= 3 || errors % 200 === 0) {
            console.error(`    Gamma 429 rate limit (${errors} total errors)`);
          }
          return;
        }

        if (!response.ok) {
          errors++;
          if (errors <= 3 || errors % 200 === 0) {
            console.error(`    Gamma API error (${errors} total): ${response.status}`);
          }
          return;
        }

        // Success — gradually speed back up
        delay = Math.max(60, delay - 5);

        const markets = await response.json();

        if (Array.isArray(markets) && markets.length > 0) {
          const market = markets[0];
          const eventSlug = market.events?.[0]?.slug || '';
          const marketSlug = market.slug || '';
          const fullSlug = eventSlug && marketSlug ? `${eventSlug}/${marketSlug}` : eventSlug || marketSlug;
          // Use condition_id as the grouping key for Yes/No token pairs
          const groupId = market.condition_id || market.id || tokenId;

          // Market resolution fields from Gamma API
          const marketClosed = market.closed === true || market.closed === 'true';
          const marketActive = market.active === true || market.active === 'true';
          const endDate = market.end_date_iso || market.endDate || null;
          const acceptingOrders = market.accepting_orders === true || market.acceptingOrders === true;

          // Determine winning outcome from Gamma data
          // When a market resolves, the winning token's price = 1.00 and loser = 0.00
          let winningOutcome = null;
          if (marketClosed) {
            // Method 1: Check tokens array for a price near 1.0
            if (market.tokens && Array.isArray(market.tokens)) {
              for (const token of market.tokens) {
                const price = parseFloat(token.price || 0);
                if (price >= 0.95) {
                  winningOutcome = token.outcome || null;
                  break;
                }
              }
            }

            // Method 2: Check outcomePrices + outcomes arrays (more reliable for resolved markets)
            // Gamma returns outcomePrices as '["1","0"]' and outcomes as '["Yes","No"]'
            if (!winningOutcome) {
              let parsedPrices = market.outcomePrices;
              if (typeof parsedPrices === 'string') {
                try { parsedPrices = JSON.parse(parsedPrices); } catch(e) { parsedPrices = null; }
              }
              let parsedOutcomes = market.outcomes;
              if (typeof parsedOutcomes === 'string') {
                try { parsedOutcomes = JSON.parse(parsedOutcomes); } catch(e) { parsedOutcomes = null; }
              }
              if (Array.isArray(parsedPrices) && Array.isArray(parsedOutcomes)) {
                for (let pi = 0; pi < parsedPrices.length; pi++) {
                  const price = parseFloat(parsedPrices[pi] || 0);
                  if (price >= 0.95 && parsedOutcomes[pi]) {
                    winningOutcome = parsedOutcomes[pi];
                    break;
                  }
                }
              }
            }

            // Method 3: Check Gamma's resolved_by or winner field directly
            if (!winningOutcome && market.winner) {
              winningOutcome = market.winner;
            }
          }

          // Volume and liquidity
          const volume = parseFloat(market.volume || 0);
          const liquidity = parseFloat(market.liquidity || 0);
          const volume24hr = parseFloat(market.volume24hr || market.volume_24hr || 0);

          const commonFields = {
            title: market.title || market.question || `Market ${tokenId.slice(0, 8)}...`,
            slug: fullSlug,
            category: market.category || '',
            image: market.image || '',
            groupId,
            // Resolution status
            marketClosed,
            marketActive,
            endDate,
            acceptingOrders,
            winningOutcome,
            // Market depth
            volume,
            liquidity,
            volume24hr,
          };

          // Parse stringified JSON arrays from Gamma API
          // clobTokenIds and outcomes come as strings like '["id1","id2"]'
          let clobIds = market.clobTokenIds;
          if (typeof clobIds === 'string') {
            try { clobIds = JSON.parse(clobIds); } catch(e) { clobIds = null; }
          }
          let outcomesList = market.outcomes;
          if (typeof outcomesList === 'string') {
            try { outcomesList = JSON.parse(outcomesList); } catch(e) { outcomesList = null; }
          }

          // Build per-token info with outcome and current price from tokens array
          if (market.tokens && Array.isArray(market.tokens)) {
            for (const token of market.tokens) {
              const tid = token.token_id || token.tokenId;
              if (tid) {
                lookup.set(tid, {
                  ...commonFields,
                  outcome: token.outcome || 'Unknown',
                  currentPrice: parseFloat(token.price || 0),
                });
              }
            }
          }

          // Parse outcomePrices for current market prices per outcome
          // Gamma returns these as a JSON string like '["0.65","0.35"]' matching clobTokenIds order
          let outcomePrices = market.outcomePrices;
          if (typeof outcomePrices === 'string') {
            try { outcomePrices = JSON.parse(outcomePrices); } catch(e) { outcomePrices = null; }
          }

          // Use parsed clobTokenIds + outcomes to map tokens to Yes/No
          // This is the most reliable source — Gamma returns them in order [Yes, No]
          if (Array.isArray(clobIds)) {
            for (let ci = 0; ci < clobIds.length; ci++) {
              const tid = clobIds[ci];
              const existing = lookup.get(tid);
              // Determine outcome: prefer outcomes array, fallback to positional
              const outcomeValue = (Array.isArray(outcomesList) && outcomesList[ci])
                ? outcomesList[ci]
                : (ci === 0 ? 'Yes' : 'No');
              // Current price from outcomePrices array, or from tokens array if already set
              const price = (Array.isArray(outcomePrices) && outcomePrices[ci])
                ? parseFloat(outcomePrices[ci])
                : (existing?.currentPrice || 0);
              // Override if not set OR if outcome is still Unknown
              if (!existing || existing.outcome === 'Unknown') {
                lookup.set(tid, {
                  ...(existing || {}),
                  ...commonFields,
                  outcome: outcomeValue,
                  currentPrice: price,
                });
              } else if (price > 0) {
                // Update price even if outcome is already known
                existing.currentPrice = price;
              }
            }
          }

          // Final fallback: if the requested tokenId still isn't in lookup
          if (!lookup.has(tokenId)) {
            lookup.set(tokenId, {
              ...commonFields,
              outcome: 'Unknown',
            });
          }
        }
        return; // success, no retry needed
      } catch (err) {
        if (attempt === MAX_RETRIES) {
          errors++;
          if (errors <= 3) console.error(`    Error fetching market:`, err.message);
        }
      }
    }
  }

  // Process in concurrent batches
  for (let i = 0; i < ids.length; i += CONCURRENCY) {
    const batch = ids.slice(i, i + CONCURRENCY).filter(id => !lookup.has(id));

    if (batch.length > 0) {
      await Promise.all(batch.map(id => fetchOne(id)));
    }

    queried = Math.min(i + CONCURRENCY, ids.length);

    if (queried % 500 === 0 || queried >= ids.length) {
      console.log(`    Gamma progress: ${queried}/${ids.length} queried, ${lookup.size} resolved, ${errors} errors, delay=${delay}ms`);
    }

    // Checkpoint save every 5000 tokens to preserve progress
    if (onCheckpoint && queried % 5000 === 0 && queried > 0) {
      try { onCheckpoint(lookup); } catch (e) { /* non-fatal */ }
    }

    // Adaptive delay between batches
    await new Promise(resolve => setTimeout(resolve, delay));
  }

  return lookup;
}

// ============================================================================
// Analytics Functions
// ============================================================================

/**
 * Compute consensus markets from top wallets
 * Identifies markets where multiple wallets hold active positions
 * @param {Map} walletData - Map of address → {positions, score, stats}
 * @param {Map} marketLookup - Map of tokenId → market info
 * @param {number} minWallets - Minimum wallets to include market
 * @returns {Array} Consensus markets sorted by conviction
 */
function computeConsensus(walletData, marketLookup, minWallets = 3) {
  // Group by market (using groupId to combine all outcome tokens into one entry)
  const marketMap = new Map();

  for (const [address, wallet] of walletData) {
    if (!wallet.positions) continue;

    for (const pos of wallet.positions) {
      if (pos.amount <= 0.01) continue; // Only active positions

      const tokenId = pos.tokenId;
      const marketInfo = marketLookup.get(tokenId) || {};

      // Skip positions on markets already known to be closed/resolved.
      // On Polymarket, losing shares remain in wallets with non-zero amounts
      // even after market resolution — they're worthless but never redeemed.
      if (marketInfo.marketClosed === true) continue;

      // Use groupId to combine all outcome sides; fall back to tokenId if no groupId
      const groupKey = marketInfo.groupId || tokenId;
      const outcome = marketInfo.outcome || 'Unknown';

      if (!marketMap.has(groupKey)) {
        marketMap.set(groupKey, {
          groupId: groupKey,
          tokenId, // keep one tokenId for reference
          wallets: [],
          pnlSum: 0,
          outcomeCounts: {}, // Track ALL outcomes, not just Yes/No
        });
      }

      const market = marketMap.get(groupKey);
      market.wallets.push({
        address,
        score: wallet.score,
        pnl: pos.pnl,
        outcome,
      });
      market.pnlSum += pos.pnl;

      // Count every outcome type (Yes, No, Lakers, Pistons, Over 2.5, etc.)
      if (outcome && outcome !== 'Unknown') {
        market.outcomeCounts[outcome] = (market.outcomeCounts[outcome] || 0) + 1;
      }
    }
  }

  // Filter and compute metrics
  const consensus = [];
  for (const [groupKey, market] of marketMap) {
    if (market.wallets.length < minWallets) continue;

    // Use market info from any token in the group
    const marketInfo = marketLookup.get(market.tokenId) || {
      title: `Market ${market.tokenId}`,
      slug: market.tokenId,
    };

    const avgScore = market.wallets.reduce((sum, w) => sum + w.score, 0) / market.wallets.length;
    const avgPnl = market.pnlSum / market.wallets.length;

    // --- Score-weighted consensus direction ---
    // Instead of just counting heads, weight each wallet's vote by their score.
    // This means 3 elite wallets on Yes outweigh 5 mediocre wallets on No.
    const scoreByOutcome = {};
    const countByOutcome = {};
    let totalScoreWeight = 0;

    for (const w of market.wallets) {
      const oc = w.outcome || 'Unknown';
      if (oc === 'Unknown') continue;
      scoreByOutcome[oc] = (scoreByOutcome[oc] || 0) + (w.score || 0);
      countByOutcome[oc] = (countByOutcome[oc] || 0) + 1;
      totalScoreWeight += (w.score || 0);
    }

    // Sort outcomes by score weight (not headcount)
    const outcomesByWeight = Object.entries(scoreByOutcome)
      .sort((a, b) => b[1] - a[1]);

    let direction = 'mixed';
    let topOutcome = null;
    let topOutcomeScore = 0;
    let topCount = 0;
    let totalVotes = Object.values(countByOutcome).reduce((s, c) => s + c, 0);

    // Consensus strength: what % of total score weight does the top outcome hold
    let consensusStrength = 0;

    if (outcomesByWeight.length > 0) {
      topOutcome = outcomesByWeight[0][0];
      topOutcomeScore = outcomesByWeight[0][1];
      topCount = countByOutcome[topOutcome] || 0;
      consensusStrength = totalScoreWeight > 0 ? topOutcomeScore / totalScoreWeight : 0;

      // Direction is set if:
      // 1. Only one outcome exists, OR
      // 2. Top outcome holds >60% of total score weight
      if (outcomesByWeight.length === 1) {
        direction = topOutcome.toLowerCase();
        consensusStrength = 1.0;
      } else if (consensusStrength > 0.6) {
        direction = topOutcome.toLowerCase();
      }
    }

    // Conviction now uses score-weighted direction strength
    // Strong consensus: all wallets agree → conviction = walletCount × avgScore
    // Weak consensus: split wallets → conviction is reduced by how split they are
    const conviction = market.wallets.length * avgScore * consensusStrength;

    // Backwards-compatible yesCount/noCount + new generic fields
    const yesCount = market.outcomeCounts['Yes'] || 0;
    const noCount = market.outcomeCounts['No'] || 0;

    consensus.push({
      groupKey: market.groupId,  // The canonical key used for signal deduplication
      marketTitle: marketInfo.title,
      slug: marketInfo.slug || market.tokenId,
      tokenId: market.tokenId,
      walletCount: market.wallets.length,
      yesCount,
      noCount,
      direction,
      topOutcome: topOutcome || 'Unknown',
      topOutcomeCount: topCount,
      topOutcomeScore: +topOutcomeScore.toFixed(1),
      totalOutcomeVotes: totalVotes,
      consensusStrength: +consensusStrength.toFixed(3),  // 0.0 to 1.0
      outcomeCounts: market.outcomeCounts,
      wallets: market.wallets,
      avgScore,
      avgPnl,
      conviction: +conviction.toFixed(2),
    });
  }

  // Sort by conviction descending
  return consensus.sort((a, b) => b.conviction - a.conviction);
}

/**
 * Analyze winning patterns across top wallets
 * @param {Map} walletData - Map of address → {positions, score, stats}
 * @param {Map} marketLookup - Map of tokenId → market info
 * @returns {object} Pattern analysis
 */
function computeWinPatterns(walletData, marketLookup) {
  const marketWins = new Map();
  const sizeBuckets = {
    small: { count: 0, wins: 0, totalPnl: 0, avgPnl: 0 },
    medium: { count: 0, wins: 0, totalPnl: 0, avgPnl: 0 },
    large: { count: 0, wins: 0, totalPnl: 0, avgPnl: 0 },
  };

  let overallWins = 0;
  let overallTrades = 0;
  let overallPnl = 0;

  // Analyze only CLOSED positions (amount ≈ 0) — these are resolved predictions
  // Open positions are excluded because the prediction outcome isn't decided yet
  for (const [address, wallet] of walletData) {
    if (!wallet.positions) continue;

    for (const pos of wallet.positions) {
      const { pnl, tokenId, totalBought } = pos;

      // Skip open positions — prediction not yet resolved
      if ((pos.amount || 0) > 0.01) continue;
      // Skip positions with no meaningful activity
      if (Math.abs(pnl) < 0.01 && (totalBought || 0) < 0.01) continue;

      overallTrades++;
      overallPnl += pnl;
      if (pnl > 0) overallWins++;

      // Market-level analysis
      if (!marketWins.has(tokenId)) {
        marketWins.set(tokenId, {
          wins: 0,
          total: 0,
          pnlSum: 0,
          market: marketLookup.get(tokenId) || { title: `Market ${tokenId}` },
        });
      }

      const mw = marketWins.get(tokenId);
      mw.total++;
      mw.pnlSum += pnl;
      if (pnl > 0) mw.wins++;

      // Size bucket analysis
      let bucket;
      if (totalBought < 100) bucket = 'small';
      else if (totalBought < 1000) bucket = 'medium';
      else bucket = 'large';

      sizeBuckets[bucket].count++;
      sizeBuckets[bucket].totalPnl += pnl;
      if (pnl > 0) sizeBuckets[bucket].wins++;
    }
  }

  // Compute bucket averages
  for (const bucket of Object.values(sizeBuckets)) {
    if (bucket.count > 0) {
      bucket.avgPnl = bucket.totalPnl / bucket.count;
    }
  }

  // Top winning markets
  const topWinningMarkets = Array.from(marketWins.values())
    .sort((a, b) => (b.wins / b.total || 0) - (a.wins / a.total || 0))
    .slice(0, 100)
    .map((m) => ({
      title: m.market.title,
      slug: m.market.slug,
      winRate: m.total > 0 ? m.wins / m.total : 0,
      totalTrades: m.total,
      totalPnl: m.pnlSum,
      avgPnl: m.total > 0 ? m.pnlSum / m.total : 0,
    }));

  const overallStats = {
    totalTrades: overallTrades,
    totalWins: overallWins,
    winRate: overallTrades > 0 ? overallWins / overallTrades : 0,
    totalPnl: overallPnl,
    avgPnl: overallTrades > 0 ? overallPnl / overallTrades : 0,
  };

  return {
    topWinningMarkets,
    sizeBuckets,
    overallStats,
  };
}

/**
 * Extract active positions across all wallets
 * @param {Map} walletData - Map of address → {positions, score, stats}
 * @param {Map} marketLookup - Map of tokenId → market info
 * @returns {Array} Active positions grouped by market
 */
function computeActivePositions(walletData, marketLookup) {
  const marketHoldings = new Map();

  for (const [address, wallet] of walletData) {
    if (!wallet.positions) continue;

    for (const pos of wallet.positions) {
      if (pos.amount <= 0.01) continue; // Only active

      const tokenId = pos.tokenId;
      const mInfo = marketLookup.get(tokenId) || {};
      if (mInfo.marketClosed === true) continue; // Skip resolved markets

      if (!marketHoldings.has(tokenId)) {
        marketHoldings.set(tokenId, {
          tokenId,
          market: mInfo.title ? mInfo : { title: `Market ${tokenId}` },
          holders: [],
          totalShares: 0,
          totalValue: 0,
          totalPnl: 0,
        });
      }

      const market = marketHoldings.get(tokenId);
      // Note: totalBought includes all USDC ever spent (even on shares since sold),
      // so totalBought/amount overstates entry price if partial sells occurred.
      // Use avgPrice from subgraph if available, otherwise approximate with a cap at $1.
      const entryPrice = pos.avgPrice || (pos.amount > 0 ? Math.min(1, pos.totalBought / pos.amount) : 0);

      // Estimated dollar value of this position (shares * entry price)
      const positionValue = pos.amount * entryPrice;

      market.holders.push({
        address,
        shares: pos.amount,
        entryPrice,
        positionValue,
        currentPnl: pos.pnl,
        walletScore: wallet.score,
      });
      market.totalShares += pos.amount;
      market.totalValue += positionValue;
      market.totalPnl += pos.pnl;
    }
  }

  // Convert to sorted array
  const active = Array.from(marketHoldings.values())
    .sort((a, b) => b.holders.length - a.holders.length)
    .map((m) => ({
      marketTitle: m.market.title,
      slug: m.market.slug || m.tokenId,
      tokenId: m.tokenId,
      holderCount: m.holders.length,
      totalShares: m.totalShares,
      totalValue: +m.totalValue.toFixed(2),
      totalPnl: +m.totalPnl.toFixed(2),
      avgEntryPrice: m.holders.length > 0
        ? +(m.holders.reduce((s, h) => s + h.entryPrice, 0) / m.holders.length).toFixed(4)
        : 0,
      holders: m.holders,
    }));

  return active;
}

// ============================================================================
// File I/O Helpers
// ============================================================================

function loadJSON(filepath) {
  try {
    const data = fs.readFileSync(filepath, 'utf8');
    return JSON.parse(data);
  } catch (err) {
    return null;
  }
}

/**
 * Write data to a JSON file with 2-space indent
 * @param {string} filepath - Path to JSON file
 * @param {any} data - Data to write
 */
function saveJSON(filepath, data) {
  fs.writeFileSync(filepath, JSON.stringify(data, null, 2) + '\n', 'utf8');
}

/**
 * Load and parse a gzipped JSON file, return null if missing
 * Falls back to uncompressed .json if .gz doesn't exist
 * @param {string} filepath - Path to .gz file
 * @returns {any} Parsed JSON or null
 */
function loadGzJSON(filepath) {
  try {
    const compressed = fs.readFileSync(filepath);
    const decompressed = zlib.gunzipSync(compressed);
    return JSON.parse(decompressed.toString('utf8'));
  } catch (err) {
    // Fall back to plain JSON (without .gz extension)
    const plainPath = filepath.replace(/\.gz$/, '');
    try {
      const data = fs.readFileSync(plainPath, 'utf8');
      return JSON.parse(data);
    } catch {
      return null;
    }
  }
}

/**
 * Write data to a gzipped JSON file (compact, no indentation for smaller size)
 * @param {string} filepath - Path to .gz file
 * @param {any} data - Data to write
 */
function saveGzJSON(filepath, data) {
  try {
    const json = JSON.stringify(data);
    const compressed = zlib.gzipSync(json, { level: 9 });
    fs.writeFileSync(filepath, compressed);
  } catch (err) {
    if (!(err instanceof RangeError)) throw err;
    // Data too large for single JSON.stringify — serialize in chunks via Buffers
    // to bypass V8's ~512MB string length limit
    const chunks = [];
    if (typeof data === 'object' && data !== null && !Array.isArray(data)) {
      chunks.push(Buffer.from('{'));
      const keys = Object.keys(data);
      for (let i = 0; i < keys.length; i++) {
        if (i > 0) chunks.push(Buffer.from(','));
        chunks.push(Buffer.from(JSON.stringify(keys[i]) + ':'));
        const val = data[keys[i]];
        try {
          chunks.push(Buffer.from(JSON.stringify(val)));
        } catch (e2) {
          // Value itself too large (e.g. the wallets object) — go one level deeper
          if (typeof val === 'object' && val !== null && !Array.isArray(val)) {
            chunks.push(Buffer.from('{'));
            const vkeys = Object.keys(val);
            for (let j = 0; j < vkeys.length; j++) {
              if (j > 0) chunks.push(Buffer.from(','));
              chunks.push(Buffer.from(JSON.stringify(vkeys[j]) + ':' + JSON.stringify(val[vkeys[j]])));
            }
            chunks.push(Buffer.from('}'));
          } else {
            throw e2;
          }
        }
      }
      chunks.push(Buffer.from('}'));
    } else {
      throw err;
    }
    const jsonBuffer = Buffer.concat(chunks);
    const compressed = zlib.gzipSync(jsonBuffer, { level: 9 });
    fs.writeFileSync(filepath, compressed);
  }
}

// ============================================================================
// Signal Engine
// ============================================================================

/**
 * Signal thresholds — tune these as we gather performance data
 */
const SIGNAL_THRESHOLDS = {
  // Consensus signals — multiple wallets agree
  // Calibrated against scan #52: 1424 wallets, targeting ~50-70 consensus signals
  MIN_WALLETS: 12,        // 12+ wallets = real convergence (was 4 — too easy with 1400 wallets)
  MIN_AVG_SCORE: 60,      // Avg score 60+ = genuinely skilled pool (was 40)
  MIN_CONVICTION: 500,    // Raw conviction 500+ = strong signal (was 100)
  CONSENSUS_RATIO: 0.6,   // 60%+ score-weighted must agree on direction
  STALE_SCANS: 16,        // Close signal if no wallet activity for 16 scans (~4 days)

  // Cluster signals — small group of strong wallets (3-11), bridges gap between solo and consensus
  // Higher per-wallet quality than consensus since fewer wallets to validate
  CLUSTER_MIN_WALLETS: 3,        // At least 3 wallets agreeing
  CLUSTER_MAX_WALLETS: 11,       // Below consensus threshold (consensus starts at 12)
  CLUSTER_MIN_AVG_SCORE: 75,     // Higher bar than consensus (60) — fewer wallets need to be better (was 70, produced 474)
  CLUSTER_MIN_CONVICTION: 250,   // Lower absolute conviction (fewer wallets) but score-weighted
  CLUSTER_STALE_SCANS: 14,       // Between solo (12) and consensus (16)

  // Solo signals — single high-performing wallet, no consensus required
  // Calibrated: 95 wallets were generating 4100 signals (43 each). Target ~20-40 solo signals.
  SOLO_MIN_SCORE: 75,            // Top-tier wallet only (was 55)
  SOLO_MIN_WIN_RATE: 0.85,       // 85%+ win rate — truly elite (was 0.72)
  SOLO_MIN_RESOLVED: 100,        // 100+ resolved = deep track record (was 50)
  SOLO_MIN_PNL: 50000,           // $50k+ realized PnL (was $10k)
  SOLO_MIN_POSITION_VALUE: 500,  // $500+ position = real conviction (was $100)
  SOLO_MAX_PER_WALLET: 3,        // Max 3 solo signals per wallet (prevents portfolio flooding)
  SOLO_STALE_SCANS: 12,          // Solo signals go stale faster (12 scans ≈ 3 days)
};

/**
 * Signal confidence tiers — determines subscription tier visibility
 */
function getSignalTier(confidence) {
  if (confidence >= 80) return 'elite';    // High conviction, many wallets, strong scores
  if (confidence >= 55) return 'pro';      // Solid signal, good backing
  return 'starter';                        // Emerging signal, worth watching
}

/**
 * Generate, update, and close signals based on current consensus data.
 *
 * Signal lifecycle:
 *   1. OPEN — consensus crosses thresholds → new signal created
 *   2. ACTIVE — updated each scan with latest wallet count, scores, direction
 *   3. CLOSED — market resolves OR wallets exit OR goes stale → outcome recorded
 *
 * @param {Array} consensus - Current consensus data from computeConsensus()
 * @param {object} existingSignals - Previous signals keyed by signalId {active: {}, history: []}
 * @param {Map} walletData - Clean wallet map
 * @param {Map} marketLookup - Market lookup map
 * @param {number} scanIndex - Current scan number
 * @returns {object} Updated signals {active: {}, history: [], stats: {}}
 */
function processSignals(consensus, existingSignals, walletData, marketLookup, scanIndex) {
  const active = { ...(existingSignals.active || {}) };
  const history = [...(existingSignals.history || [])];
  const now = new Date().toISOString();

  // Track what consensus entries we see this scan
  const seenGroups = new Set();

  let opened = 0, updated = 0, closed = 0;

  // --- Phase 1: Process current consensus → open or update signals ---
  for (const entry of consensus) {
    // Use the canonical groupKey from computeConsensus (groupId || tokenId)
    // This is the stable market identifier that doesn't change between scans
    const groupKey = entry.groupKey || entry.slug || entry.tokenId;
    if (!groupKey) continue;
    seenGroups.add(groupKey);

    const walletCount = entry.walletCount || 0;
    const avgScore = entry.avgScore || 0;
    const conviction = entry.conviction || walletCount * avgScore;
    const direction = entry.direction || 'mixed';

    // Check if this consensus entry meets signal thresholds
    const meetsThresholds =
      walletCount >= SIGNAL_THRESHOLDS.MIN_WALLETS &&
      avgScore >= SIGNAL_THRESHOLDS.MIN_AVG_SCORE &&
      conviction >= SIGNAL_THRESHOLDS.MIN_CONVICTION &&
      direction !== 'mixed';

    const signalId = `sig_${groupKey}`;

    if (active[signalId]) {
      // --- UPDATE existing signal ---
      const signal = active[signalId];
      signal.lastUpdatedScan = scanIndex;
      signal.lastUpdatedAt = now;
      signal.scansSinceUpdate = 0;

      // Track direction changes
      if (direction !== signal.direction && direction !== 'mixed') {
        signal.directionChanges = (signal.directionChanges || 0) + 1;
        signal.previousDirection = signal.direction;
        signal.direction = direction;
      }

      // Update metrics
      signal.walletCount = walletCount;
      signal.avgScore = +avgScore.toFixed(2);
      signal.conviction = +conviction.toFixed(2);
      signal.topOutcome = entry.topOutcome || signal.topOutcome;
      signal.outcomeCounts = entry.outcomeCounts || signal.outcomeCounts;
      signal.avgPnl = +(entry.avgPnl || 0).toFixed(2);
      signal.avgEntryPrice = +(entry.avgEntryPrice || 0).toFixed(4);
      signal.consensusStrength = +(entry.consensusStrength || 0).toFixed(3);

      // Recompute confidence
      signal.confidence = computeSignalConfidence(signal);
      signal.tier = getSignalTier(signal.confidence);

      // Track peak values
      signal.peakWallets = Math.max(signal.peakWallets || 0, walletCount);
      signal.peakConviction = Math.max(signal.peakConviction || 0, conviction);
      signal.peakConfidence = Math.max(signal.peakConfidence || 0, signal.confidence);

      // Store wallet snapshot
      signal.currentWallets = (entry.wallets || []).map(w => ({
        address: w.address,
        score: w.score,
        outcome: w.outcome,
        pnl: w.pnl,
      }));

      updated++;

    } else if (meetsThresholds) {
      // --- Guard: skip markets that are already resolved ---
      // Without this, Phase 2 would immediately close a signal we just opened,
      // producing a 0-duration open→close that pollutes the track record.
      const openTokenId = entry.tokenId || '';
      const openMarketInfo = openTokenId ? marketLookup.get(openTokenId) : null;
      if (openMarketInfo && openMarketInfo.marketClosed === true) {
        continue; // Market already settled — no point opening a signal
      }

      // --- OPEN new signal ---
      const consensusStr = entry.consensusStrength || 0;
      const confidence = computeSignalConfidence({
        walletCount, avgScore, conviction, direction, consensusStrength: consensusStr,
      });

      active[signalId] = {
        signalId,
        marketTitle: entry.marketTitle || 'Unknown',
        slug: entry.slug || '',
        tokenId: entry.tokenId || '',
        groupKey,

        // Timing
        openedAt: now,
        openedScan: scanIndex,
        lastUpdatedAt: now,
        lastUpdatedScan: scanIndex,
        scansSinceUpdate: 0,
        scansActive: 1,

        // Direction
        direction,
        topOutcome: entry.topOutcome || direction,
        outcomeCounts: entry.outcomeCounts || {},
        directionChanges: 0,

        // Metrics at open
        walletCount,
        avgScore: +avgScore.toFixed(2),
        conviction: +conviction.toFixed(2),
        consensusStrength: +consensusStr.toFixed(3),
        avgPnl: +(entry.avgPnl || 0).toFixed(2),
        avgEntryPrice: +(entry.avgEntryPrice || 0).toFixed(4),

        // Market price at signal open — locked, never updated (from Gamma outcomePrices)
        openMarketPrice: +(openMarketInfo && openMarketInfo.currentPrice || 0).toFixed(4),

        // Confidence & tier
        confidence: +confidence.toFixed(1),
        tier: getSignalTier(confidence),

        // Peak tracking
        peakWallets: walletCount,
        peakConviction: conviction,
        peakConfidence: confidence,

        // Status
        status: 'active',
        outcome: null,
        closedAt: null,
        closedScan: null,
        closeReason: null,

        // Wallet snapshot
        currentWallets: (entry.wallets || []).map(w => ({
          address: w.address,
          score: w.score,
          outcome: w.outcome,
          pnl: w.pnl,
        })),
      };

      opened++;
    }
  }

  // --- Phase 1a: Cluster signals — small groups of strong wallets (3-11) ---
  let clusterOpened = 0, clusterUpdated = 0;
  for (const entry of consensus) {
    const groupKey = entry.groupKey || entry.slug || entry.tokenId;
    if (!groupKey) continue;

    const walletCount = entry.walletCount || 0;
    const avgScore = entry.avgScore || 0;
    const conviction = entry.conviction || walletCount * avgScore;
    const direction = entry.direction || 'mixed';
    const signalId = `sig_${groupKey}`;

    // Skip if already handled as a consensus signal (12+)
    if (active[signalId] && active[signalId].signalType !== 'cluster') continue;

    // Cluster range: 3-11 wallets
    if (walletCount < SIGNAL_THRESHOLDS.CLUSTER_MIN_WALLETS ||
        walletCount > SIGNAL_THRESHOLDS.CLUSTER_MAX_WALLETS) continue;

    const meetsClusterThresholds =
      avgScore >= SIGNAL_THRESHOLDS.CLUSTER_MIN_AVG_SCORE &&
      conviction >= SIGNAL_THRESHOLDS.CLUSTER_MIN_CONVICTION &&
      direction !== 'mixed';

    if (active[signalId]) {
      // Update existing cluster signal
      const signal = active[signalId];
      signal.lastUpdatedScan = scanIndex;
      signal.lastUpdatedAt = now;
      signal.scansSinceUpdate = 0;

      if (direction !== signal.direction && direction !== 'mixed') {
        signal.directionChanges = (signal.directionChanges || 0) + 1;
        signal.previousDirection = signal.direction;
        signal.direction = direction;
      }

      signal.walletCount = walletCount;
      signal.avgScore = +avgScore.toFixed(2);
      signal.conviction = +conviction.toFixed(2);
      signal.topOutcome = entry.topOutcome || signal.topOutcome;
      signal.outcomeCounts = entry.outcomeCounts || {};
      signal.avgPnl = +(entry.avgPnl || 0).toFixed(2);
      signal.avgEntryPrice = +(entry.avgEntryPrice || 0).toFixed(4);
      signal.consensusStrength = +(entry.consensusStrength || 0).toFixed(3);

      // If it grew past cluster range into consensus territory, upgrade type
      if (walletCount >= SIGNAL_THRESHOLDS.MIN_WALLETS) {
        signal.signalType = 'consensus';
        signal.confidence = computeSignalConfidence(signal);
      } else {
        signal.confidence = computeClusterConfidence(signal);
      }
      signal.tier = getSignalTier(signal.confidence);

      signal.peakWallets = Math.max(signal.peakWallets || 0, walletCount);
      signal.peakConviction = Math.max(signal.peakConviction || 0, conviction);
      signal.peakConfidence = Math.max(signal.peakConfidence || 0, signal.confidence);

      signal.currentWallets = (entry.wallets || []).map(w => ({
        address: w.address, score: w.score, outcome: w.outcome, pnl: w.pnl,
      }));

      clusterUpdated++;

    } else if (meetsClusterThresholds) {
      // Guard: skip already-resolved markets
      const clOpenTokenId = entry.tokenId || '';
      const clOpenMarketInfo = clOpenTokenId ? marketLookup.get(clOpenTokenId) : null;
      if (clOpenMarketInfo && clOpenMarketInfo.marketClosed === true) {
        continue;
      }

      // Open new cluster signal
      const consensusStr = entry.consensusStrength || 0;
      const confidence = computeClusterConfidence({
        walletCount, avgScore, conviction, direction, consensusStrength: consensusStr,
      });

      active[signalId] = {
        signalId,
        signalType: 'cluster',
        marketTitle: entry.marketTitle || 'Unknown',
        slug: entry.slug || '',
        tokenId: entry.tokenId || '',
        groupKey,
        openedAt: now,
        openedScan: scanIndex,
        lastUpdatedAt: now,
        lastUpdatedScan: scanIndex,
        scansSinceUpdate: 0,
        scansActive: 1,
        direction,
        topOutcome: entry.topOutcome || direction,
        outcomeCounts: entry.outcomeCounts || {},
        directionChanges: 0,
        walletCount,
        avgScore: +avgScore.toFixed(2),
        conviction: +conviction.toFixed(2),
        consensusStrength: +consensusStr.toFixed(3),
        avgPnl: +(entry.avgPnl || 0).toFixed(2),
        avgEntryPrice: +(entry.avgEntryPrice || 0).toFixed(4),
        openMarketPrice: +(clOpenMarketInfo && clOpenMarketInfo.currentPrice || 0).toFixed(4),
        confidence: +confidence.toFixed(1),
        tier: getSignalTier(confidence),
        peakWallets: walletCount,
        peakConviction: conviction,
        peakConfidence: confidence,
        status: 'active',
        outcome: null,
        closedAt: null,
        closedScan: null,
        closeReason: null,
        currentWallets: (entry.wallets || []).map(w => ({
          address: w.address, score: w.score, outcome: w.outcome, pnl: w.pnl,
        })),
      };

      clusterOpened++;
    }
  }

  // --- Phase 1b: Solo signals — high-performing individual wallets ---
  let soloOpened = 0;
  const soloCountPerWallet = new Map(); // Track per-wallet solo signal count
  // Pre-count existing solo signals per wallet
  for (const [sid, sig] of Object.entries(active)) {
    if (sig.signalType === 'solo' && sig.soloWallet) {
      soloCountPerWallet.set(sig.soloWallet, (soloCountPerWallet.get(sig.soloWallet) || 0) + 1);
    }
  }

  for (const [address, wallet] of walletData) {
    const stats = wallet.stats;
    if (!stats || !wallet.positions) continue;

    // Wallet must meet elite solo thresholds
    const qualifiesForSolo =
      (wallet.score || 0) >= SIGNAL_THRESHOLDS.SOLO_MIN_SCORE &&
      (stats.wr || 0) >= SIGNAL_THRESHOLDS.SOLO_MIN_WIN_RATE &&
      (stats.resolved || 0) >= SIGNAL_THRESHOLDS.SOLO_MIN_RESOLVED &&
      (stats.realizedPnl || stats.totalPnl || 0) >= SIGNAL_THRESHOLDS.SOLO_MIN_PNL;

    if (!qualifiesForSolo) continue;

    // Per-wallet cap — only top N positions become signals
    const maxPerWallet = SIGNAL_THRESHOLDS.SOLO_MAX_PER_WALLET || 3;
    const currentCount = soloCountPerWallet.get(address) || 0;
    if (currentCount >= maxPerWallet) continue;

    // Sort positions by value (highest first) so we pick the best ones
    const activePositions = wallet.positions
      .filter(p => p.amount > 0.01)
      .map(p => {
        const ep = p.avgPrice || (p.amount > 0 ? Math.min(1, p.totalBought / p.amount) : 0);
        return { ...p, _value: p.amount * ep };
      })
      .sort((a, b) => b._value - a._value);

    // Check each active position for solo signal potential
    for (const pos of activePositions) {
      if ((soloCountPerWallet.get(address) || 0) >= maxPerWallet) break;

      const tokenId = pos.tokenId;
      const marketInfo = marketLookup.get(tokenId) || {};
      // Use same groupKey derivation as computeConsensus for consistency
      const groupKey = marketInfo.groupId || tokenId;
      const outcome = marketInfo.outcome || 'Unknown';
      if (outcome === 'Unknown') continue;

      // Skip already-resolved markets
      if (marketInfo.marketClosed === true) continue;

      // Skip if this market already has a consensus signal (using canonical groupKey)
      const consensusSignalId = `sig_${groupKey}`;
      if (active[consensusSignalId]) continue;
      if (seenGroups.has(groupKey)) continue; // Already covered by consensus

      // Check position value
      const entryPrice = pos.avgPrice || (pos.amount > 0 ? Math.min(1, pos.totalBought / pos.amount) : 0);
      const positionValue = pos.amount * entryPrice;
      if (positionValue < SIGNAL_THRESHOLDS.SOLO_MIN_POSITION_VALUE) continue;

      const soloSignalId = `solo_${groupKey}_${address.slice(0, 10)}`;

      if (active[soloSignalId]) {
        // Update existing solo signal
        const signal = active[soloSignalId];
        signal.lastUpdatedScan = scanIndex;
        signal.lastUpdatedAt = now;
        signal.scansSinceUpdate = 0;
        signal.positionValue = +positionValue.toFixed(2);
        signal.avgEntryPrice = +(pos.avgPrice || entryPrice).toFixed(4);
        signal.currentPnl = +(pos.pnl || 0).toFixed(2);
        signal.walletScore = +(wallet.score || 0).toFixed(2);
        signal.confidence = computeSoloConfidence(wallet, pos, positionValue);
        signal.tier = getSignalTier(signal.confidence);
        signal.peakConfidence = Math.max(signal.peakConfidence || 0, signal.confidence);
        signal.currentWallets = [{
          address,
          score: wallet.score || 0,
          outcome,
          pnl: pos.pnl || 0,
        }];
        updated++;
      } else {
        // Open new solo signal
        const confidence = computeSoloConfidence(wallet, pos, positionValue);

        active[soloSignalId] = {
          signalId: soloSignalId,
          signalType: 'solo',
          marketTitle: marketInfo.title || `Market ${tokenId}`,
          slug: marketInfo.slug || '',
          tokenId,
          groupKey,

          // Timing
          openedAt: now,
          openedScan: scanIndex,
          lastUpdatedAt: now,
          lastUpdatedScan: scanIndex,
          scansSinceUpdate: 0,
          scansActive: 1,

          // Direction — solo signal is whatever the wallet holds
          direction: outcome.toLowerCase(),
          topOutcome: outcome,
          outcomeCounts: { [outcome]: 1 },
          directionChanges: 0,

          // Metrics
          walletCount: 1,
          avgScore: +(wallet.score || 0).toFixed(2),
          conviction: +(wallet.score || 0).toFixed(2), // Solo = just the wallet score
          avgPnl: +(pos.pnl || 0).toFixed(2),

          // Solo-specific fields
          soloWallet: address,
          walletScore: +(wallet.score || 0).toFixed(2),
          walletWinRate: +(stats.wr || 0).toFixed(3),
          walletResolved: stats.resolved || 0,
          walletPnl: +(stats.realizedPnl || stats.totalPnl || 0).toFixed(2),
          positionValue: +positionValue.toFixed(2),
          positionShares: +pos.amount.toFixed(2),
          entryPrice: +entryPrice.toFixed(4),
          avgEntryPrice: +entryPrice.toFixed(4),
          currentPnl: +(pos.pnl || 0).toFixed(2),
          openMarketPrice: +(marketInfo.currentPrice || 0).toFixed(4),

          // Confidence & tier
          confidence: +confidence.toFixed(1),
          tier: getSignalTier(confidence),

          // Peak tracking
          peakWallets: 1,
          peakConviction: wallet.score || 0,
          peakConfidence: confidence,

          // Status
          status: 'active',
          outcome: null,
          closedAt: null,
          closedScan: null,
          closeReason: null,

          // Wallet snapshot
          currentWallets: [{
            address,
            score: wallet.score || 0,
            outcome,
            pnl: pos.pnl || 0,
          }],
        };

        soloOpened++;
        opened++;
        soloCountPerWallet.set(address, (soloCountPerWallet.get(address) || 0) + 1);
      }
    }
  }

  // --- Phase 2: Check existing signals not in current consensus → stale/close ---
  for (const [signalId, signal] of Object.entries(active)) {
    const groupKey = signal.groupKey;
    signal.scansActive = (signal.scansActive || 0) + 1;

    // Determine stale threshold based on signal type
    const staleThreshold = signal.signalType === 'solo'
      ? SIGNAL_THRESHOLDS.SOLO_STALE_SCANS
      : signal.signalType === 'cluster'
        ? SIGNAL_THRESHOLDS.CLUSTER_STALE_SCANS
        : SIGNAL_THRESHOLDS.STALE_SCANS;

    const isGroupSignal = !signal.signalType || signal.signalType === 'consensus' || signal.signalType === 'cluster';

    if (isGroupSignal && !seenGroups.has(groupKey)) {
      // Consensus/cluster signal's market no longer in consensus — wallets may have exited
      signal.scansSinceUpdate = (signal.scansSinceUpdate || 0) + 1;

      if (signal.scansSinceUpdate >= staleThreshold) {
        closeSignal(active, history, signalId, 'stale', scanIndex, now);
        closed++;
        continue;
      }
    } else if (signal.signalType === 'solo') {
      // Solo signal: check if the wallet still holds the position
      const wallet = walletData.get(signal.soloWallet);
      if (!wallet || !wallet.positions) {
        signal.scansSinceUpdate = (signal.scansSinceUpdate || 0) + 1;
      } else {
        const stillHolding = wallet.positions.some(p => {
          const mi = marketLookup.get(p.tokenId) || {};
          const gk = mi.groupId || mi.slug || p.tokenId;
          return gk === groupKey && p.amount > 0.01;
        });
        if (!stillHolding) {
          signal.scansSinceUpdate = (signal.scansSinceUpdate || 0) + 1;
        }
      }

      if (signal.scansSinceUpdate >= staleThreshold) {
        closeSignal(active, history, signalId, 'stale', scanIndex, now);
        closed++;
        continue;
      }

      // Check if solo signal should be upgraded to consensus
      // If a consensus signal now covers this market, close the solo one
      const consensusId = `sig_${signal.groupKey || signal.slug || signal.tokenId}`;
      if (active[consensusId] && active[consensusId] !== signal) {
        closeSignal(active, history, signalId, 'upgraded_to_consensus', scanIndex, now);
        closed++;
        continue;
      }
    }

    // --- Check if market resolved ---
    // Method 1: Gamma API market status (most reliable — works even if wallets haven't redeemed)
    // Look up any token in this signal's market to check if Gamma says it's closed
    const signalTokenId = signal.tokenId;
    const marketInfo = signalTokenId ? marketLookup.get(signalTokenId) : null;
    const gammaResolved = marketInfo && marketInfo.marketClosed === true;

    // Method 2: Wallet position amounts (fallback — all wallets' positions closed)
    const walletResolved = (signal.currentWallets && signal.currentWallets.length > 0)
      ? checkIfMarketResolved(signal, walletData, marketLookup)
      : false;

    if (gammaResolved || walletResolved) {
      // Determine signal outcome
      let signalOutcome;
      let totalPnl = 0;
      let walletsWon = 0;
      let walletsLost = 0;

      // Gamma-resolved with no winningOutcome means market closed but we can't determine the winner.
      // Only resolve if we have a clear winner OR wallet PnLs can tell us.
      if (gammaResolved && !marketInfo.winningOutcome && !walletResolved) {
        // Market closed per Gamma but no winner info and wallets haven't cleared — skip for now
        continue;
      }

      if (gammaResolved && marketInfo.winningOutcome) {
        // Gamma tells us which outcome won — check if signal's wallets were on the right side
        const winner = marketInfo.winningOutcome.toLowerCase().trim();
        const signalDir = (signal.direction || '').toLowerCase().trim();
        const signalTopOutcome = (signal.topOutcome || '').toLowerCase().trim();

        // Match signal direction against winning outcome.
        // Use exact match on full strings to avoid partial-match false positives.
        // Direct: "celtics" === "celtics", "yes" === "yes"
        const dirMatch = signalDir === winner || signalTopOutcome === winner;

        // Partial match for multi-word outcomes where names are subsets
        // e.g. "trail blazers" vs "blazers", or "golden state warriors" vs "warriors"
        // Guard: both sides must be 4+ chars to avoid "yes"/"no" matching substrings
        let partialMatch = false;
        if (!dirMatch && winner.length >= 4 && signalDir.length >= 4) {
          // Only allow partial match if one string fully contains the other
          // AND the match covers a substantial portion of the shorter string
          const shorter = signalDir.length <= winner.length ? signalDir : winner;
          const longer = signalDir.length <= winner.length ? winner : signalDir;
          if (longer.includes(shorter) && shorter.length >= longer.length * 0.35) {
            partialMatch = true;
          }
        }
        if (!dirMatch && !partialMatch && winner.length >= 4 && signalTopOutcome.length >= 4) {
          const shorter = signalTopOutcome.length <= winner.length ? signalTopOutcome : winner;
          const longer = signalTopOutcome.length <= winner.length ? winner : signalTopOutcome;
          if (longer.includes(shorter) && shorter.length >= longer.length * 0.35) {
            partialMatch = true;
          }
        }

        if (dirMatch || partialMatch) {
          signalOutcome = 'win';
        } else {
          signalOutcome = 'loss';
        }

        // Still compute per-wallet PnL for tracking
        if (signal.currentWallets) {
          for (const w of signal.currentWallets) {
            const pnl = w.pnl || 0;
            totalPnl += pnl;
            if (pnl > 0) walletsWon++;
            else if (pnl < 0) walletsLost++;
          }
        }
      } else {
        // Fallback: determine outcome from LIVE wallet positions (not stale snapshot).
        // signal.currentWallets may contain stale PnLs from when the signal was last updated.
        // Read fresh PnL from the actual wallet data instead.
        let liveWon = 0;
        let liveLost = 0;
        let livePnl = 0;

        for (const sw of (signal.currentWallets || [])) {
          const wallet = walletData.get(sw.address);
          if (!wallet || !wallet.positions) continue;

          // Find the wallet's position in this signal's market
          const signalGroupKey = signal.groupKey;
          const pos = wallet.positions.find(p => {
            const mi = marketLookup.get(p.tokenId);
            const gk = mi?.groupId || p.tokenId;
            return gk === signalGroupKey;
          });

          if (pos) {
            const pnl = pos.pnl || 0;
            livePnl += pnl;
            if (pnl > 0.01) liveWon++;
            else if (pnl < -0.01) liveLost++;
          }
        }

        walletsWon = liveWon;
        walletsLost = liveLost;
        totalPnl = livePnl;

        // Need at least one wallet with a non-zero PnL to determine outcome.
        // If all PnLs are 0 (unredeemed shares), we can't tell who won — skip resolution.
        if (walletsWon === 0 && walletsLost === 0) {
          // Can't determine outcome — don't close the signal yet, wait for Gamma data or PnL updates
          continue;
        }

        // Binary outcome only — we called a direction, it was right or wrong
        if (walletsWon >= walletsLost) signalOutcome = 'win';
        else signalOutcome = 'loss';
      }

      closeSignal(active, history, signalId, 'resolved', scanIndex, now, signalOutcome, totalPnl);
      // Store per-wallet breakdown in history for detailed track record
      const lastHistoryEntry = history[history.length - 1];
      if (lastHistoryEntry && lastHistoryEntry.signalId === signalId) {
        lastHistoryEntry.walletsWon = walletsWon;
        lastHistoryEntry.walletsLost = walletsLost;
        lastHistoryEntry.walletHitRate = walletsWon + walletsLost > 0
          ? +((walletsWon / (walletsWon + walletsLost)) * 100).toFixed(1)
          : 0;
        lastHistoryEntry.resolvedBy = gammaResolved ? 'gamma' : 'wallet_positions';
        if (marketInfo?.winningOutcome) lastHistoryEntry.winningOutcome = marketInfo.winningOutcome;
      }
      closed++;
    }
  }

  // --- Phase 2b: Deduplicate — no two active signals for the same market ---
  // Build a map of groupKey → signals, keep highest-confidence if conflicts found
  const groupToSignals = new Map();
  for (const [signalId, signal] of Object.entries(active)) {
    const gk = signal.groupKey;
    if (!gk) continue;
    if (!groupToSignals.has(gk)) groupToSignals.set(gk, []);
    groupToSignals.get(gk).push({ signalId, signal });
  }
  for (const [gk, signals] of groupToSignals) {
    if (signals.length <= 1) continue;
    // Multiple signals for same market — keep the one with highest confidence
    signals.sort((a, b) => (b.signal.confidence || 0) - (a.signal.confidence || 0));
    for (let i = 1; i < signals.length; i++) {
      const dup = signals[i];
      closeSignal(active, history, dup.signalId, 'deduplicated', scanIndex, now);
      closed++;
    }
  }

  // --- Phase 2c: Fix historical push outcomes using Gamma data ---
  // Pushes occurred when wallet PnLs were all 0 (unredeemed). Now that we have
  // winningOutcome from Gamma, retroactively correct push → win/loss.
  let pushesFixed = 0;
  for (const hist of history) {
    if (hist.outcome !== 'push') continue;
    // Look up market data for this signal's token
    const histToken = hist.tokenId;
    const histMarket = histToken ? marketLookup.get(histToken) : null;
    if (!histMarket || !histMarket.winningOutcome) continue;

    const winner = histMarket.winningOutcome.toLowerCase().trim();
    const dir = (hist.direction || '').toLowerCase().trim();
    const topOut = (hist.topOutcome || '').toLowerCase().trim();

    const dirMatch = dir === winner || topOut === winner;
    // Tightened partial match: both strings must be 4+ chars and shorter must
    // cover at least 40% of longer string to prevent spurious substring hits
    let partialMatch = false;
    if (!dirMatch && winner.length >= 4 && dir.length >= 4) {
      const shorter = dir.length <= winner.length ? dir : winner;
      const longer = dir.length <= winner.length ? winner : dir;
      if (longer.includes(shorter) && shorter.length >= longer.length * 0.35) partialMatch = true;
    }
    if (!dirMatch && !partialMatch && winner.length >= 4 && topOut.length >= 4) {
      const shorter = topOut.length <= winner.length ? topOut : winner;
      const longer = topOut.length <= winner.length ? winner : topOut;
      if (longer.includes(shorter) && shorter.length >= longer.length * 0.35) partialMatch = true;
    }

    if (dirMatch || partialMatch) {
      hist.outcome = 'win';
    } else {
      hist.outcome = 'loss';
    }
    hist.resolvedBy = 'gamma_retrofix';
    hist.winningOutcome = histMarket.winningOutcome;
    pushesFixed++;
  }
  if (pushesFixed > 0) {
    console.log(`  Fixed ${pushesFixed} historical push outcomes using Gamma data`);
  }

  // --- Phase 3: Compute aggregate stats ---
  const activeSignals = Object.values(active);
  const allHistory = history;

  const wins = allHistory.filter(s => s.outcome === 'win').length;
  const losses = allHistory.filter(s => s.outcome === 'loss').length;
  const resolved = wins + losses;
  const hitRate = resolved > 0 ? +(wins / resolved * 100).toFixed(1) : 0;
  const totalHistoryPnl = allHistory.reduce((s, sig) => s + (sig.walletPnl || sig.closedPnl || 0), 0);

  const consensusSignals = activeSignals.filter(s => !s.signalType || s.signalType === 'consensus');
  const clusterSignals = activeSignals.filter(s => s.signalType === 'cluster');
  const soloSignals = activeSignals.filter(s => s.signalType === 'solo');

  const stats = {
    activeCount: activeSignals.length,
    consensusCount: consensusSignals.length,
    clusterCount: clusterSignals.length,
    soloCount: soloSignals.length,
    historyCount: allHistory.length,
    totalResolved: resolved,
    wins,
    losses,
    hitRate,
    totalPnl: +totalHistoryPnl.toFixed(2),
    avgConfidence: activeSignals.length > 0
      ? +(activeSignals.reduce((s, sig) => s + sig.confidence, 0) / activeSignals.length).toFixed(1)
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
    openedThisScan: opened,
    clusterOpenedThisScan: clusterOpened,
    soloOpenedThisScan: soloOpened,
    updatedThisScan: updated,
    closedThisScan: closed,
  };

  return { active, history, stats };
}

/**
 * Compute signal confidence score (0-100)
 * Calibrated so minimum-qualifying signals score ~30-40 (starter),
 * strong signals score ~55-75 (pro), and only exceptional signals hit 80+ (elite).
 *
 * Target distribution: ~50-60% starter, ~25-35% pro, ~10-15% elite
 */
function computeSignalConfidence(signal) {
  const wc = signal.walletCount || 0;
  const as = signal.avgScore || 0;
  const conv = signal.conviction || 0;
  const cs = signal.consensusStrength || 0;

  // Wallet count factor (25 pts): 12 wallets (min) → ~8pts, 30 → ~16pts, 80+ → ~23pts
  // Steeper curve: need 50+ wallets for full points
  const walletFactor = Math.min(1, Math.pow(wc / 80, 0.7)) * 25;

  // Avg score factor (25 pts): 60 (min) → ~10pts, 75 → ~17pts, 90+ → ~24pts
  // Shifted reference to 95 so minimum threshold gets ~40% of points
  const scoreFactor = Math.min(1, Math.pow(Math.max(0, as - 40) / 55, 1.3)) * 25;

  // Conviction factor (20 pts): 500 (min) → ~5pts, 2000 → ~11pts, 10000+ → ~18pts
  // Much steeper: need log10(10001)≈4.0 / 5.0 = 80% for near-max
  const convictionFactor = Math.min(1, Math.pow(Math.log10(1 + conv) / 5.0, 1.5)) * 20;

  // Consensus strength factor (20 pts): 0.6 (min) → ~6pts, 0.8 → ~12pts, 0.95+ → ~19pts
  // Quadratic curve so median consensus (~0.7-0.8) doesn't max out
  const alignmentFactor = Math.pow(Math.max(0, cs - 0.4) / 0.6, 1.8) * 20;

  // Stability factor (10 pts): penalize signals that flip direction
  const changes = signal.directionChanges || 0;
  const stabilityFactor = Math.max(0, 1 - changes * 0.3) * 10;

  return Math.min(100, Math.round(walletFactor + scoreFactor + convictionFactor + alignmentFactor + stabilityFactor));
}

/**
 * Compute confidence for cluster signals (0-100)
 * Cluster = 3-11 wallets — a small group of strong traders converging on a market.
 * Weighs per-wallet quality more heavily than full consensus since the crowd is smaller.
 *
 * Calibrated: 3 wallets avg70 → ~35, 6 wallets avg78 → ~55, 10 wallets avg85 → ~75
 * Target: ~45-55% starter, ~30-40% pro, ~10-15% elite
 */
function computeClusterConfidence(signal) {
  const wc = signal.walletCount || 0;
  const as = signal.avgScore || 0;
  const conv = signal.conviction || 0;
  const cs = signal.consensusStrength || 0;

  // Wallet count factor (20 pts): 3 → ~5pts, 6 → ~12pts, 10+ → ~19pts
  // Steeper per-wallet curve since the range is narrow (3-11)
  const walletFactor = Math.min(1, Math.pow((wc - 2) / 9, 0.8)) * 20;

  // Avg score factor (30 pts): higher weight than consensus — quality over quantity
  // 70 (min) → ~10pts, 80 → ~19pts, 90+ → ~28pts
  const scoreFactor = Math.min(1, Math.pow(Math.max(0, as - 50) / 45, 1.4)) * 30;

  // Conviction factor (20 pts): same as consensus but scaled for smaller groups
  // 250 (min) → ~4pts, 1000 → ~10pts, 5000+ → ~17pts
  const convictionFactor = Math.min(1, Math.pow(Math.log10(1 + conv) / 4.5, 1.5)) * 20;

  // Alignment factor (20 pts): even more important with small groups
  const alignmentFactor = Math.pow(Math.max(0, cs - 0.4) / 0.6, 1.8) * 20;

  // Stability factor (10 pts)
  const changes = signal.directionChanges || 0;
  const stabilityFactor = Math.max(0, 1 - changes * 0.3) * 10;

  return Math.min(100, Math.round(walletFactor + scoreFactor + convictionFactor + alignmentFactor + stabilityFactor));
}

/**
 * Compute confidence for solo signals (0-100)
 * Different weighting: heavily relies on wallet track record since there's no crowd validation.
 *
 * Calibrated so minimum-qualifying solos score ~25-40 (starter),
 * strong solos ~55-75 (pro), only legendary wallets with huge positions hit 80+ (elite).
 *
 * Target: ~55-65% starter, ~25-30% pro, ~5-10% elite
 */
function computeSoloConfidence(wallet, position, positionValue) {
  const stats = wallet.stats || {};
  const score = wallet.score || 0;

  // Wallet score factor (35 pts): 75 (min) → ~10pts, 85 → ~20pts, 95+ → ~33pts
  // Reference shifted to 98 with steeper curve — need truly elite wallets for full points
  const scoreFactor = Math.min(1, Math.pow(Math.max(0, score - 60) / 38, 1.5)) * 35;

  // Win rate factor (25 pts): 0.85 (min) → ~5pts, 0.90 → ~12pts, 0.95+ → ~23pts
  // Steep curve above 0.80 baseline — the last few % of WR are what separate great from elite
  const wr = stats.wr || 0;
  const wrFactor = Math.min(1, Math.pow(Math.max(0, wr - 0.80) / 0.18, 2.0)) * 25;

  // Track record depth (20 pts): 100 (min) → ~5pts, 300 → ~11pts, 1000+ → ~18pts
  // Steeper log curve — need hundreds of resolved markets for full credit
  const resolved = stats.resolved || 0;
  const depthFactor = Math.min(1, Math.pow(Math.log10(1 + resolved) / 3.5, 1.8)) * 20;

  // Position size factor (20 pts): $500 (min) → ~4pts, $2000 → ~10pts, $10000+ → ~18pts
  // Need serious capital deployed for full points
  const sizeFactor = Math.min(1, Math.pow(Math.log10(1 + positionValue) / 4.5, 1.5)) * 20;

  return Math.min(100, Math.max(0, Math.round(scoreFactor + wrFactor + depthFactor + sizeFactor)));
}

/**
 * Close a signal and move it to history
 */
function closeSignal(active, history, signalId, reason, scanIndex, timestamp, outcome = null, pnl = 0) {
  const signal = active[signalId];
  if (!signal) return;

  signal.status = 'closed';
  signal.closedAt = timestamp;
  signal.closedScan = scanIndex;
  signal.closeReason = reason;
  signal.outcome = outcome;
  // walletPnl: raw sum of backing wallets' PnL (informational only — NOT the signal's win/loss result)
  signal.walletPnl = +pnl.toFixed(2);
  // closedPnl kept as 0 for backward compatibility — paper trader uses its own PnL calculation
  signal.closedPnl = 0;
  signal.duration = scanIndex - (signal.openedScan || scanIndex);

  // Strip wallet snapshot to save space in history
  delete signal.currentWallets;

  // Prevent duplicate history entries — don't re-add a signal that was already closed for this market
  const groupKey = signal.groupKey;
  const isDuplicate = groupKey && history.some(h =>
    h.groupKey === groupKey && h.closeReason === reason && h.outcome === outcome
  );
  if (!isDuplicate) {
    history.push(signal);
  }
  delete active[signalId];

  // Keep history at a manageable size (last 500 signals)
  if (history.length > 500) {
    history.splice(0, history.length - 500);
  }
}

/**
 * Check if a signal's market has resolved by checking if all backing wallets
 * now have ~0 shares (meaning positions closed/resolved)
 */
function checkIfMarketResolved(signal, walletData, marketLookup) {
  if (!signal.currentWallets || signal.currentWallets.length === 0) return false;

  let resolvedCount = 0;
  let checkedCount = 0;

  for (const sw of signal.currentWallets) {
    const wallet = walletData.get(sw.address);
    if (!wallet || !wallet.positions) continue;

    // Find this wallet's position in the signal's market using canonical groupKey
    const signalGroupKey = signal.groupKey;
    const pos = wallet.positions.find(p => {
      const mi = marketLookup.get(p.tokenId);
      const gk = mi?.groupId || p.tokenId;
      return gk === signalGroupKey;
    });

    if (pos) {
      checkedCount++;
      if (pos.amount <= 0.01) resolvedCount++; // Position closed
    }
  }

  // Consider resolved if we checked at least 2 wallets and all positions are closed
  return checkedCount >= 2 && resolvedCount === checkedCount;
}

// ============================================================================
// Paper Trading Engine
// ============================================================================

/**
 * Paper trader config
 */
const PAPER_TRADE_CONFIG = {
  STARTING_BALANCE: 10000,   // $10k per portfolio
  TRADE_SIZE: 100,           // $100 per trade
  MAX_OPEN_TRADES: 25,       // Cap per portfolio — only highest confidence signals
  PORTFOLIOS: ['combined', 'elite', 'pro', 'starter'],
};

/**
 * Initialize a fresh paper trading state
 */
function initPaperTrading() {
  const portfolios = {};
  for (const name of PAPER_TRADE_CONFIG.PORTFOLIOS) {
    portfolios[name] = {
      balance: PAPER_TRADE_CONFIG.STARTING_BALANCE,
      startingBalance: PAPER_TRADE_CONFIG.STARTING_BALANCE,
      openTrades: {},     // keyed by signalId
      closedTrades: [],   // history of closed trades
      equity: [],         // equity curve snapshots [{scan, timestamp, equity, openTradeValue}]
      stats: {
        totalTrades: 0,
        wins: 0,
        losses: 0,
        totalPnl: 0,
        biggestWin: 0,
        biggestLoss: 0,
        winStreak: 0,
        lossStreak: 0,
        currentStreak: 0,     // positive = wins, negative = losses
        maxDrawdown: 0,
        peakEquity: PAPER_TRADE_CONFIG.STARTING_BALANCE,
      },
    };
  }
  return { portfolios, createdAt: new Date().toISOString(), version: 1 };
}

/**
 * Process paper trades based on current signal state.
 *
 * Called each scan AFTER processSignals(). Logic:
 *   1. Check for newly opened signals → open paper trades
 *   2. Check for closed signals (in history) → close paper trades + compute PnL
 *   3. Snapshot equity curve
 *
 * @param {object} signals - Current signal state {active, history, stats}
 * @param {object} paperState - Existing paper trading state
 * @param {number} scanIndex - Current scan number
 * @returns {object} Updated paper trading state
 */
function processPaperTrades(signals, paperState, scanIndex) {
  if (!paperState || !paperState.portfolios) {
    paperState = initPaperTrading();
  }
  const now = new Date().toISOString();
  const portfolios = paperState.portfolios;
  const tradeSize = PAPER_TRADE_CONFIG.TRADE_SIZE;

  // On first run, allow all existing active signals to open trades
  const isFirstRun = !paperState.lastProcessedScan;
  paperState.lastProcessedScan = scanIndex;

  // Build set of signals that closed this scan (they're now in history)
  const closedThisScan = new Map();
  for (const hist of (signals.history || [])) {
    if (hist.closedScan === scanIndex) {
      closedThisScan.set(hist.signalId, hist);
    }
  }

  // --- Step 1: Open trades for newly active signals ---
  // Sort candidates by confidence (highest first) so we fill slots with the best signals
  const maxOpen = PAPER_TRADE_CONFIG.MAX_OPEN_TRADES || 25;
  const candidates = Object.entries(signals.active || {})
    .filter(([, sig]) => sig.openedScan === scanIndex || isFirstRun)
    .sort((a, b) => (b[1].confidence || 0) - (a[1].confidence || 0));

  for (const [signalId, signal] of candidates) {
    // Determine which portfolios this signal belongs to
    const tier = signal.tier || 'starter';
    const targetPortfolios = ['combined'];
    if (tier === 'elite') targetPortfolios.push('elite');
    else if (tier === 'pro') targetPortfolios.push('pro');
    else targetPortfolios.push('starter');

    for (const pName of targetPortfolios) {
      const portfolio = portfolios[pName];
      if (!portfolio) continue;

      // Skip if already have a trade for this signal
      if (portfolio.openTrades[signalId]) continue;

      // Skip if at max open trades capacity
      if (Object.keys(portfolio.openTrades).length >= maxOpen) continue;

      // Skip if not enough balance
      if (portfolio.balance < tradeSize) continue;

      // Open the trade
      portfolio.balance -= tradeSize;
      portfolio.openTrades[signalId] = {
        signalId,
        marketTitle: signal.marketTitle || 'Unknown',
        direction: signal.direction || 'unknown',
        tier: signal.tier || 'starter',
        signalType: signal.signalType || 'consensus',
        confidence: signal.confidence || 0,
        avgEntryPrice: signal.avgEntryPrice || 0,
        openMarketPrice: signal.openMarketPrice || 0,
        tradeSize,
        openedAt: now,
        openedScan: scanIndex,
        signalOpenedScan: signal.openedScan,
      };
      portfolio.stats.totalTrades++;
    }
  }

  // --- Step 2: Close trades for signals that closed this scan ---
  for (const [signalId, closedSignal] of closedThisScan) {
    for (const pName of PAPER_TRADE_CONFIG.PORTFOLIOS) {
      const portfolio = portfolios[pName];
      if (!portfolio) continue;

      const trade = portfolio.openTrades[signalId];
      if (!trade) continue;

      // Compute paper PnL based on signal outcome
      let tradePnl = 0;
      const outcome = closedSignal.outcome;
      const closeReason = closedSignal.closeReason;

      if (closeReason === 'resolved' && outcome === 'win') {
        // Signal was right — payout based on real avg entry price from wallets' positions
        // Polymarket payout: buy at entryPrice, win pays $1.00 per share
        // ROI = (1/entryPrice - 1) × tradeSize
        // Use real avgEntryPrice from signal or from the trade's stored entry price.
        // If no real entry price is available, use a conservative default (0.65)
        // to avoid fabricating PnL from unrelated metrics like consensusStrength.
        const realPrice = closedSignal.avgEntryPrice > 0
          ? closedSignal.avgEntryPrice
          : (trade.avgEntryPrice > 0 ? trade.avgEntryPrice : 0);
        const entryPrice = realPrice > 0
          ? Math.max(0.05, Math.min(0.99, realPrice))
          : 0.65; // Conservative default: roughly Polymarket's median entry
        if (realPrice <= 0) {
          console.log(`  ⚠ Paper trade ${signalId} in ${pName}: no real entry price, using default 0.65`);
        }
        tradePnl = trade.tradeSize * (1 / entryPrice - 1);
      } else if (closeReason === 'resolved' && outcome === 'loss') {
        // Signal was wrong — lose the trade amount
        tradePnl = -trade.tradeSize;
      } else if (closeReason === 'stale' || closeReason === 'deduplicated' || closeReason === 'upgraded_to_consensus') {
        // Signal closed without resolution — return capital with small friction cost
        tradePnl = -trade.tradeSize * 0.02; // 2% slippage/friction for exit
      } else {
        // Unknown outcome — small loss for friction
        tradePnl = -trade.tradeSize * 0.02;
      }

      tradePnl = +tradePnl.toFixed(2);

      // Return capital + PnL to balance
      portfolio.balance += trade.tradeSize + tradePnl;
      portfolio.balance = +portfolio.balance.toFixed(2);

      // Record closed trade
      const closedTrade = {
        ...trade,
        closedAt: now,
        closedScan: scanIndex,
        outcome: outcome || 'unknown',
        closeReason,
        pnl: tradePnl,
        returnPct: +((tradePnl / trade.tradeSize) * 100).toFixed(2),
        duration: scanIndex - trade.openedScan,
      };
      portfolio.closedTrades.push(closedTrade);

      // Update stats
      const stats = portfolio.stats;
      stats.totalPnl = +(stats.totalPnl + tradePnl).toFixed(2);

      if (tradePnl > 0) {
        stats.wins++;
        stats.biggestWin = Math.max(stats.biggestWin, tradePnl);
        stats.currentStreak = stats.currentStreak > 0 ? stats.currentStreak + 1 : 1;
        stats.winStreak = Math.max(stats.winStreak, stats.currentStreak);
      } else if (tradePnl < 0) {
        stats.losses++;
        stats.biggestLoss = Math.min(stats.biggestLoss, tradePnl);
        stats.currentStreak = stats.currentStreak < 0 ? stats.currentStreak - 1 : -1;
        stats.lossStreak = Math.max(stats.lossStreak, Math.abs(stats.currentStreak));
      }

      // Remove from open trades
      delete portfolio.openTrades[signalId];

      // Keep closed trades list manageable
      if (portfolio.closedTrades.length > 500) {
        portfolio.closedTrades.splice(0, portfolio.closedTrades.length - 500);
      }
    }
  }

  // --- Step 3: Snapshot equity curve ---
  for (const pName of PAPER_TRADE_CONFIG.PORTFOLIOS) {
    const portfolio = portfolios[pName];
    if (!portfolio) continue;

    // Equity = cash balance + value of open trades (at cost basis)
    const openTradeValue = Object.values(portfolio.openTrades).reduce((s, t) => s + t.tradeSize, 0);
    const equity = +(portfolio.balance + openTradeValue).toFixed(2);

    portfolio.equity.push({
      scan: scanIndex,
      timestamp: now,
      equity,
      balance: portfolio.balance,
      openTrades: Object.keys(portfolio.openTrades).length,
      openTradeValue,
    });

    // Track peak equity and drawdown
    const stats = portfolio.stats;
    if (equity > stats.peakEquity) stats.peakEquity = equity;
    const drawdown = stats.peakEquity > 0 ? +((1 - equity / stats.peakEquity) * 100).toFixed(2) : 0;
    if (drawdown > stats.maxDrawdown) stats.maxDrawdown = drawdown;

    // Keep equity curve manageable (last 200 snapshots)
    if (portfolio.equity.length > 200) {
      portfolio.equity.splice(0, portfolio.equity.length - 200);
    }
  }

  return paperState;
}

// ============================================================================
// Exports
// ============================================================================

export {
  GOLDSKY_PNL,
  GAMMA_MARKETS,
  USDC_DIVISOR,
  gqlQuery,
  introspectSchema,
  introspectEntity,
  discoverEntities,
  fetchPositions,
  analyzePositions,
  computeScore,
  resolveMarkets,
  computeConsensus,
  computeWinPatterns,
  computeActivePositions,
  processSignals,
  SIGNAL_THRESHOLDS,
  refreshTrackedWallets,
  initPaperTrading,
  processPaperTrades,
  PAPER_TRADE_CONFIG,
  loadJSON,
  saveJSON,
  loadGzJSON,
  saveGzJSON,
};
