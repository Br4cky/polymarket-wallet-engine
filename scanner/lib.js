/**
 * Polymarket Signal Engine - Core Library
 * Provides GraphQL queries, position fetching, scoring, and analytics
 */

import fs from 'fs';
import path from 'path';
import zlib from 'zlib';

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
  const addresses = Object.keys(wallets);
  let totalRefreshed = 0;
  let totalNew = 0;
  let totalClosures = 0;

  // Build field strings (same logic as fetchPositions)
  let fieldStr = `id ${fields.user}`;
  if (fields.pnl) fieldStr += ` ${fields.pnl}`;
  if (fields.token) fieldStr += ` ${fields.token}`;
  if (fields.totalBought) fieldStr += ` ${fields.totalBought}`;
  if (fields.amount) fieldStr += ` ${fields.amount}`;

  let nestedFieldStr = `id ${fields.user} { id }`;
  if (fields.pnl) nestedFieldStr += ` ${fields.pnl}`;
  if (fields.token) nestedFieldStr += ` ${fields.token}`;
  if (fields.totalBought) nestedFieldStr += ` ${fields.totalBought}`;
  if (fields.amount) nestedFieldStr += ` ${fields.amount}`;

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
      let batch;
      const whereClause = cursor
        ? `id_gt: "${cursor}", id_gte: "${idPrefix}-", id_lt: "${idPrefix}~"`
        : `id_gte: "${idPrefix}-", id_lt: "${idPrefix}~"`;

      try {
        if (useNested) throw new Error('use nested');
        const q = `{ ${entityName}(first: 1000, where: { ${whereClause} }) { ${fieldStr} } }`;
        const data = await gqlQuery(endpoint, q);
        batch = data?.[entityName] || [];
      } catch {
        try {
          const q = `{ ${entityName}(first: 1000, where: { ${whereClause} }) { ${nestedFieldStr} } }`;
          const data = await gqlQuery(endpoint, q);
          batch = data?.[entityName] || [];
          useNested = true;
        } catch (err) {
          console.error(`    Error refreshing ${address.slice(0, 10)}...: ${err.message}`);
          batch = [];
          break;
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
      tradingDays: 0,
      positionsPerWeek: 0,
      newPositionsThisScan: 0,
      suspiciousWinRate: false,
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
  let openLosses = 0; // Track positions that are open AND currently losing

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

    if (amount > 0.01) {
      openCount++;
      // Track open positions with negative PnL (unrealized losses)
      if (pnl < -0.01) openLosses++;
    }
  }

  const resolved = wins + losses;
  const wr = resolved > 0 ? wins / resolved : 0;
  const avgW = wins > 0 ? winSum / wins : 0;
  const avgL = losses > 0 ? lossSum / losses : 0;
  const uniqueTokenCount = uniqueTokens.size;
  const estimatedMarkets = Math.max(1, Math.ceil(uniqueTokenCount / 2));

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

  // Flag suspiciously perfect win rates — 100% WR with hiding losses in open positions
  const suspiciousWinRate = (wr >= 0.99 && losses === 0 && resolved >= 20 && openLosses >= 3);

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
    openLosses,
    maxScanIndex,
    tradingDays,
    positionsPerWeek,
    newPositionsThisScan,
    suspiciousWinRate,
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

  // Suspicious win rate penalty — wallets with 100% WR but hiding losses in open positions
  if (stats.suspiciousWinRate) {
    rawScore *= 0.7; // 30% penalty
    stats.suspiciousPenalty = true;
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

          // Build per-token info with outcome
          if (market.tokens && Array.isArray(market.tokens)) {
            for (const token of market.tokens) {
              lookup.set(token.token_id, {
                title: market.title || market.question || `Market ${tokenId.slice(0, 8)}...`,
                slug: fullSlug,
                category: market.category || '',
                image: market.image || '',
                groupId,
                outcome: token.outcome || 'Unknown',
              });
            }
          }
          if (market.clobTokenIds && Array.isArray(market.clobTokenIds)) {
            for (let ci = 0; ci < market.clobTokenIds.length; ci++) {
              const tid = market.clobTokenIds[ci];
              if (!lookup.has(tid)) {
                lookup.set(tid, {
                  title: market.title || market.question || `Market ${tokenId.slice(0, 8)}...`,
                  slug: fullSlug,
                  category: market.category || '',
                  image: market.image || '',
                  groupId,
                  outcome: ci === 0 ? 'Yes' : 'No',
                });
              }
            }
          }
          if (!lookup.has(tokenId)) {
            lookup.set(tokenId, {
              title: market.title || market.question || `Market ${tokenId.slice(0, 8)}...`,
              slug: fullSlug,
              category: market.category || '',
              image: market.image || '',
              groupId,
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
  // Group by market (using groupId to combine Yes/No tokens into one entry)
  const marketMap = new Map();

  for (const [address, wallet] of walletData) {
    if (!wallet.positions) continue;

    for (const pos of wallet.positions) {
      if (pos.amount <= 0.01) continue; // Only active positions

      const tokenId = pos.tokenId;
      const marketInfo = marketLookup.get(tokenId) || {};
      // Use groupId to combine Yes/No sides; fall back to tokenId if no groupId
      const groupKey = marketInfo.groupId || tokenId;
      const outcome = marketInfo.outcome || 'Unknown';

      if (!marketMap.has(groupKey)) {
        marketMap.set(groupKey, {
          groupId: groupKey,
          tokenId, // keep one tokenId for reference
          wallets: [],
          pnlSum: 0,
          yesCount: 0,
          noCount: 0,
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

      if (outcome === 'Yes') market.yesCount++;
      else if (outcome === 'No') market.noCount++;
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
    const conviction = market.wallets.length * avgScore;

    // Determine consensus direction
    let direction = 'mixed';
    if (market.yesCount > 0 && market.noCount === 0) direction = 'yes';
    else if (market.noCount > 0 && market.yesCount === 0) direction = 'no';

    consensus.push({
      marketTitle: marketInfo.title,
      slug: marketInfo.slug || market.tokenId,
      tokenId: market.tokenId,
      walletCount: market.wallets.length,
      yesCount: market.yesCount,
      noCount: market.noCount,
      direction,
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
/**
 * Compute resolved positions dataset for the signals tab.
 * Builds a list of all resolved positions (closed, non-zero PnL) with
 * per-period stats (today, 7d, 30d, 90d, all-time).
 * @param {Map} walletData - Map of address → {positions, score, stats}
 * @param {Map} marketLookup - Map of tokenId → {title, slug, ...}
 * @param {Object} scanTimestampMap - Map of scanIndex → ISO timestamp
 * @returns {Object} { positions: [...], periodStats: {...} }
 */
function computeResolvedPositions(walletData, marketLookup, scanTimestampMap) {
  const now = Date.now();
  const DAY = 24 * 60 * 60 * 1000;
  const cutoffs = {
    today: now - 1 * DAY,
    week: now - 7 * DAY,
    month: now - 30 * DAY,
    quarter: now - 90 * DAY,
  };

  const allResolved = [];

  for (const [address, wallet] of walletData) {
    if (!wallet.positions) continue;
    const walletScore = wallet.score || 0;

    for (const pos of wallet.positions) {
      // Only resolved positions: has non-trivial PnL and not holding shares
      if (Math.abs(pos.pnl || 0) < 0.01) continue;
      if ((pos.amount || 0) > 0.01) continue; // still open

      // Get resolved timestamp — when the position actually closed
      // Only positions where we WITNESSED the closure (open → closed between scans)
      // have a valid resolvedTimestamp. Positions discovered already-closed have null.
      const ts = pos.resolvedTimestamp || null;
      const tsMs = ts ? new Date(ts).getTime() : null;

      const marketInfo = marketLookup.get(pos.tokenId) || {};

      allResolved.push({
        address,
        walletScore: +walletScore.toFixed(1),
        marketTitle: marketInfo.title || `Market ${(pos.tokenId || '').slice(0, 8)}...`,
        slug: marketInfo.slug || '',
        tokenId: pos.tokenId,
        pnl: +(pos.pnl || 0).toFixed(2),
        totalBought: +(pos.totalBought || 0).toFixed(2),
        roi: pos.totalBought > 0.01 ? +((pos.pnl / pos.totalBought)).toFixed(4) : 0,
        timestamp: ts,
        timestampMs: tsMs,
      });
    }
  }

  // Sort by PnL descending (biggest wins first)
  allResolved.sort((a, b) => b.pnl - a.pnl);

  // Compute period stats
  function periodStats(positions) {
    let wins = 0, losses = 0, totalPnl = 0, winPnl = 0, lossPnl = 0;
    for (const p of positions) {
      totalPnl += p.pnl;
      if (p.pnl > 0) { wins++; winPnl += p.pnl; }
      else { losses++; lossPnl += Math.abs(p.pnl); }
    }
    const total = wins + losses;
    return {
      total,
      wins,
      losses,
      winRate: total > 0 ? +(wins / total).toFixed(4) : 0,
      totalPnl: +totalPnl.toFixed(2),
      avgWin: wins > 0 ? +(winPnl / wins).toFixed(2) : 0,
      avgLoss: losses > 0 ? +(lossPnl / losses).toFixed(2) : 0,
    };
  }

  // Filter by period based on timestamp
  const withTs = allResolved.filter(p => p.timestampMs);
  const periods = {
    today: periodStats(withTs.filter(p => p.timestampMs >= cutoffs.today)),
    week: periodStats(withTs.filter(p => p.timestampMs >= cutoffs.week)),
    month: periodStats(withTs.filter(p => p.timestampMs >= cutoffs.month)),
    quarter: periodStats(withTs.filter(p => p.timestampMs >= cutoffs.quarter)),
    allTime: periodStats(allResolved),
  };

  return {
    // Top 500 resolved positions for the table
    positions: allResolved.slice(0, 500).map(p => {
      const { timestampMs, ...rest } = p;
      return rest;
    }),
    periodStats: periods,
  };
}

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
  const json = JSON.stringify(data);
  const compressed = zlib.gzipSync(json, { level: 9 });
  fs.writeFileSync(filepath, compressed);
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
  computeResolvedPositions,
  refreshTrackedWallets,
  loadJSON,
  saveJSON,
  loadGzJSON,
  saveGzJSON,
};
