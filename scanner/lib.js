/**
 * Polymarket Signal Engine - Core Library
 * Provides GraphQL queries, position fetching, scoring, and analytics
 */

import fs from 'fs';
import path from 'path';

const GOLDSKY_PNL = 'https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/pnl-subgraph/0.0.14/gn';
const GOLDSKY_POSITIONS = 'https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/positions-subgraph/0.0.7/gn';
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

// ============================================================================
// Scoring Functions (matching existing screener formulas)
// ============================================================================

/**
 * Analyze positions to compute statistical metrics
 * @param {Array} positions - Array of {pnl, tokenId, totalBought, amount, scanIndex}
 * @returns {object} Statistics object
 */
function analyzePositions(positions) {
  if (!positions || positions.length === 0) {
    return {
      wins: 0,
      losses: 0,
      resolved: 0,
      wr: 0,
      avgW: 0,
      avgL: 0,
      totalPnl: 0,
      totalVolume: 0,
      uniqueTokens: 0,
      estimatedMarkets: 0,
      efficiency: 0,
      edgeRatio: 0,
      openCount: 0,
      maxScanIndex: 0,
    };
  }

  let wins = 0;
  let losses = 0;
  let winSum = 0;
  let lossSum = 0;
  let totalPnl = 0;
  let totalVolume = 0;
  let openCount = 0;
  let maxScanIndex = 0;

  const uniqueTokens = new Set();

  for (const pos of positions) {
    const { pnl, tokenId, totalBought, amount, scanIndex } = pos;

    totalPnl += pnl;
    totalVolume += totalBought;
    uniqueTokens.add(tokenId);

    if (scanIndex > maxScanIndex) maxScanIndex = scanIndex;

    if (totalBought > 0.01) {
      if (pnl > 0) {
        wins++;
        winSum += pnl;
      } else if (pnl < 0) {
        losses++;
        lossSum += -pnl;
      }
    }

    if (amount > 0.01) openCount++;
  }

  const resolved = wins + losses;
  const wr = resolved > 0 ? wins / resolved : 0;
  const avgW = wins > 0 ? winSum / wins : 0;
  const avgL = losses > 0 ? lossSum / losses : 0;
  const uniqueTokenCount = uniqueTokens.size;
  const estimatedMarkets = Math.max(1, Math.ceil(uniqueTokenCount / 2));

  // Efficiency: PnL per dollar traded (same as original screener)
  const efficiency = totalVolume > 0 ? totalPnl / totalVolume : 0;

  // Edge ratio: average win / average loss (same as original screener)
  const edgeRatio = avgL > 0 ? avgW / avgL : (avgW > 0 ? 10 : 0);

  // Activity rate metrics — count unique days positions were first seen
  const uniqueDays = new Set();
  let earliestTs = null;
  let latestTs = null;
  for (const pos of positions) {
    if (pos.firstSeenTimestamp) {
      const day = pos.firstSeenTimestamp.slice(0, 10); // YYYY-MM-DD
      uniqueDays.add(day);
      if (!earliestTs || pos.firstSeenTimestamp < earliestTs) earliestTs = pos.firstSeenTimestamp;
      if (!latestTs || pos.firstSeenTimestamp > latestTs) latestTs = pos.firstSeenTimestamp;
    }
  }
  const tradingDays = uniqueDays.size;
  const weeksTracked = earliestTs && latestTs
    ? Math.max(1, (new Date(latestTs) - new Date(earliestTs)) / (7 * 24 * 60 * 60 * 1000))
    : 1;
  const positionsPerWeek = +(positions.length / weeksTracked).toFixed(1);

  return {
    wins,
    losses,
    resolved,
    wr,
    avgW,
    avgL,
    totalPnl,
    totalVolume,
    uniqueTokens: uniqueTokenCount,
    estimatedMarkets,
    efficiency,
    edgeRatio,
    openCount,
    maxScanIndex,
    tradingDays,
    positionsPerWeek,
  };
}

/**
 * Compute a composite score from 0-100
 * Weights: WR (30) + Markets (20) + Efficiency (20) + Edge (15) + Sample size (15)
 * Then applies a recency multiplier based on how recently the wallet was active.
 * @param {object} stats - Statistics from analyzePositions
 * @param {string} [lastActiveTimestamp] - ISO timestamp of last activity
 * @returns {number} Score 0-100
 */
function computeScore(stats, lastActiveTimestamp) {
  const { resolved, wr } = stats;
  const sampleFactor = resolved > 0 ? Math.min(1, Math.sqrt(resolved) / 10) : 0;

  // Exact same formula as the browser screener:
  // Win rate component (30 pts): wr * sampleFactor * 30
  const wrScore = wr * sampleFactor * 30;
  // Market diversity (20 pts): min(1, estimatedMarkets/50) * 20
  const estimatedMarkets = stats.estimatedMarkets || Math.max(1, Math.ceil((stats.uniqueTokens || 0) / 2));
  const marketScore = Math.min(1, estimatedMarkets / 50) * 20;
  // Profit efficiency (20 pts): min(1, max(0, efficiency)/0.10) * 20
  const efficiencyScore = Math.min(1, Math.max(0, stats.efficiency || 0) / 0.10) * 20;
  // Edge ratio (15 pts): min(1, max(0, edgeRatio-0.5)/2.5) * 15
  const edgeScore = Math.min(1, Math.max(0, (stats.edgeRatio || 0) - 0.5) / 2.5) * 15;
  // Sample size (15 pts): min(1, resolved/200) * 15
  const sampleScore = Math.min(1, resolved / 200) * 15;

  let rawScore = wrScore + marketScore + efficiencyScore + edgeScore + sampleScore;

  // Recency multiplier — penalise stale wallets
  let recencyMultiplier = 1.0;
  if (lastActiveTimestamp) {
    const daysSince = (Date.now() - new Date(lastActiveTimestamp).getTime()) / (1000 * 60 * 60 * 24);
    if (daysSince > 90) recencyMultiplier = 0.5;
    else if (daysSince > 30) recencyMultiplier = 0.75;
    else if (daysSince > 7) recencyMultiplier = 0.9;
    stats.recencyMultiplier = recencyMultiplier;
  }

  return rawScore * recencyMultiplier;
}

// ============================================================================
// Market Resolution
// ============================================================================

/**
 * Resolve market data from Gamma API
 * @param {Set} tokenIds - Set of token IDs to resolve
 * @returns {Promise<Map>} Map of tokenId → {title, slug, category, image}
 */
async function resolveMarkets(tokenIds) {
  if (tokenIds.size === 0) return new Map();

  const lookup = new Map();
  const batchSize = 100;
  const ids = Array.from(tokenIds);

  for (let i = 0; i < ids.length; i += batchSize) {
    const batch = ids.slice(i, i + batchSize);
    const tokenIdQuery = batch.map((id) => `"${id}"`).join(',');

    try {
      const url = `${GAMMA_MARKETS}?limit=100&offset=${i}`;
      const response = await fetch(url);

      if (!response.ok) {
        console.error(`Gamma API error: ${response.status}`);
        continue;
      }

      const markets = await response.json();

      if (Array.isArray(markets)) {
        for (const market of markets) {
          if (market.tokens && Array.isArray(market.tokens)) {
            for (const token of market.tokens) {
              if (batch.includes(token.token_id)) {
                lookup.set(token.token_id, {
                  title: market.title,
                  slug: market.slug,
                  category: market.category,
                  image: market.image,
                });
              }
            }
          }
        }
      }
    } catch (err) {
      console.error(`Error fetching markets batch:`, err.message);
    }

    // Add delay to avoid rate limiting
    await new Promise((resolve) => setTimeout(resolve, 200));
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
  const marketMap = new Map();

  // Group positions by market
  for (const [address, wallet] of walletData) {
    if (!wallet.positions) continue;

    for (const pos of wallet.positions) {
      if (pos.amount <= 0.01) continue; // Only active positions

      const tokenId = pos.tokenId;
      if (!marketMap.has(tokenId)) {
        marketMap.set(tokenId, {
          tokenId,
          wallets: [],
          pnlSum: 0,
        });
      }

      const market = marketMap.get(tokenId);
      market.wallets.push({
        address,
        score: wallet.score,
        pnl: pos.pnl,
      });
      market.pnlSum += pos.pnl;
    }
  }

  // Filter and compute metrics
  const consensus = [];
  for (const [tokenId, market] of marketMap) {
    if (market.wallets.length < minWallets) continue;

    const marketInfo = marketLookup.get(tokenId) || {
      title: `Market ${tokenId}`,
      slug: tokenId,
    };

    const avgScore = market.wallets.reduce((sum, w) => sum + w.score, 0) / market.wallets.length;
    const avgPnl = market.pnlSum / market.wallets.length;
    const conviction = market.wallets.length * avgScore;

    consensus.push({
      marketTitle: marketInfo.title,
      slug: marketInfo.slug || tokenId,
      tokenId,
      walletCount: market.wallets.length,
      wallets: market.wallets,
      avgScore,
      avgPnl,
      conviction,
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

  // Analyze all resolved positions (skip open/unresolved ones with ~$0 PnL)
  for (const [address, wallet] of walletData) {
    if (!wallet.positions) continue;

    for (const pos of wallet.positions) {
      const { pnl, tokenId, totalBought } = pos;

      // Skip unresolved positions — only count positions with meaningful PnL
      if (Math.abs(pnl) < 0.01 && (totalBought || 0) < 0.01) continue;
      // Also skip open positions (still holding shares)
      if ((pos.amount || 0) > 0.01 && Math.abs(pnl) < 0.01) continue;

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
    .slice(0, 20)
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
      if (!marketHoldings.has(tokenId)) {
        marketHoldings.set(tokenId, {
          tokenId,
          market: marketLookup.get(tokenId) || { title: `Market ${tokenId}` },
          holders: [],
          totalShares: 0,
        });
      }

      const market = marketHoldings.get(tokenId);
      const entryPrice = pos.amount > 0 ? pos.totalBought / pos.amount : 0;

      market.holders.push({
        address,
        shares: pos.amount,
        entryPrice,
        currentPnl: pos.pnl,
        walletScore: wallet.score,
      });
      market.totalShares += pos.amount;
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
      holders: m.holders,
    }));

  return active;
}

/**
 * Find the biggest individual winning positions
 * @param {Map} walletData - Map of address → {positions, score, stats}
 * @param {Map} marketLookup - Map of tokenId → market info
 * @param {number} topN - Number of top wins to return
 * @returns {Array} Top N positions by PnL
 */
function findBiggestWins(walletData, marketLookup, topN = 50) {
  const allWins = [];

  for (const [address, wallet] of walletData) {
    if (!wallet.positions) continue;

    for (const pos of wallet.positions) {
      if (pos.pnl <= 0) continue; // Only winning positions

      const market = marketLookup.get(pos.tokenId) || {
        title: `Market ${pos.tokenId}`,
      };
      const roi = pos.totalBought > 0 ? (pos.pnl / pos.totalBought) * 100 : 0;

      allWins.push({
        rank: 0,
        address,
        walletScore: wallet.score,
        marketTitle: market.title,
        slug: market.slug || pos.tokenId,
        tokenId: pos.tokenId,
        pnl: pos.pnl,
        totalBought: pos.totalBought,
        roi,
      });
    }
  }

  // Sort by PnL descending and take top N
  return allWins
    .sort((a, b) => b.pnl - a.pnl)
    .slice(0, topN)
    .map((w, i) => ({ ...w, rank: i + 1 }));
}

// ============================================================================
// File I/O Helpers
// ============================================================================

/**
 * Load and parse a JSON file, return null if missing
 * @param {string} filepath - Path to JSON file
 * @returns {any} Parsed JSON or null
 */
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

// ============================================================================
// Exports
// ============================================================================

export {
  GOLDSKY_PNL,
  GOLDSKY_POSITIONS,
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
  findBiggestWins,
  loadJSON,
  saveJSON,
};
