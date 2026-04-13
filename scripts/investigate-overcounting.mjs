#!/usr/bin/env node
// Investigate "pure overcounting" wallets — sample PnL >> Goldsky lifetime PnL,
// yet NOT truncated and 0 unredeemed wins.
//
// Diagnoses:
//   1. Re-runs the analyzer on fresh /activity data with marketLookup
//   2. Compares per-market PnL breakdown vs Goldsky per-position data
//   3. Checks for USDC value anomalies, price > 1.0, duplicate events
//   4. Prints actionable summary of what's inflating the number
//
// Usage:
//   node scripts/investigate-overcounting.mjs                  # auto-pick worst pure overcounter
//   node scripts/investigate-overcounting.mjs 0xABC...         # probe specific wallet
//   node scripts/investigate-overcounting.mjs --top5           # probe top 5 overcounters
//
// Run OUTSIDE the sandbox (needs data-api.polymarket.com + gamma-api.polymarket.com).

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const { fetchAllActivity, analyzeTradeHistory } =
  await import(path.join(ROOT, 'scanner/dataApi.js'));

const { discoverEntities, gqlQuery, USDC_DIVISOR } =
  await import(path.join(ROOT, 'scanner/lib.js'));

const DATA_API = 'https://data-api.polymarket.com';
const GAMMA = 'https://gamma-api.polymarket.com';
const GOLDSKY_PNL = 'https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/polymarket-pnl/0.0.1/gn';

function loadPool() {
  const file = path.join(ROOT, 'data/wallets.json.gz');
  if (!fs.existsSync(file)) return null;
  const raw = fs.readFileSync(file);
  const data = JSON.parse(zlib.gunzipSync(raw).toString());
  return data.pool || data;
}

// ─── Goldsky per-position PnL for a single wallet ───
async function fetchGoldskyPositions(wallet) {
  // Discover the schema first (same logic scan.js uses)
  const entities = await discoverEntities(GOLDSKY_PNL);
  if (!entities || entities.length === 0) throw new Error('No Goldsky entities found');
  const { entity: entityName, fields } = entities[0];

  const addr = wallet.toLowerCase();
  const positions = [];
  let lastId = '';

  const queryFields = ['id'];
  if (fields.pnl) queryFields.push(fields.pnl);
  if (fields.totalBought) queryFields.push(fields.totalBought);
  if (fields.token) queryFields.push(fields.token);
  if (fields.amount) queryFields.push(fields.amount);
  // Try to get outcome/conditionId if available
  queryFields.push('conditionId');

  while (positions.length < 2000) {
    const query = `{
      ${entityName}s(
        first: 1000
        orderBy: id
        where: { ${fields.user}: "${addr}"${lastId ? `, id_gt: "${lastId}"` : ''} }
      ) {
        ${queryFields.join('\n        ')}
      }
    }`;

    const data = await gqlQuery(GOLDSKY_PNL, query);
    const items = data?.[`${entityName}s`] || [];
    if (items.length === 0) break;

    for (const item of items) {
      positions.push({
        id: item.id,
        pnl: fields.pnl ? parseFloat(item[fields.pnl] || 0) / USDC_DIVISOR : 0,
        totalBought: fields.totalBought ? parseFloat(item[fields.totalBought] || 0) / USDC_DIVISOR : 0,
        token: item[fields.token] || '',
        amount: fields.amount ? parseFloat(item[fields.amount] || 0) / USDC_DIVISOR : 0,
        conditionId: item.conditionId || '',
      });
    }
    if (items.length < 1000) break;
    lastId = items[items.length - 1].id;
  }

  return { positions, entityName, fields };
}

// ─── Market lookup from Gamma ───
async function buildMarketLookup(events) {
  const lookup = new Map();
  const slugs = new Set();
  for (const ev of events) {
    if (ev.eventSlug) slugs.add(ev.eventSlug);
    else if (ev.slug) slugs.add(ev.slug);
  }
  if (slugs.size === 0) return lookup;
  console.log(`  Building marketLookup from ${slugs.size} slugs...`);
  let resolved = 0;
  for (const slug of slugs) {
    try {
      const r = await fetch(`${GAMMA}/events?slug=${encodeURIComponent(slug)}`);
      if (!r.ok) continue;
      const arr = await r.json();
      const event = Array.isArray(arr) ? arr[0] : arr;
      if (!event?.markets) continue;
      for (const market of event.markets) {
        const marketClosed = market.closed === true || market.closed === 'true';
        let winningOutcome = null;
        if (marketClosed) {
          let parsedPrices = market.outcomePrices;
          let parsedOutcomes = market.outcomes;
          try { if (typeof parsedPrices === 'string') parsedPrices = JSON.parse(parsedPrices); } catch {}
          try { if (typeof parsedOutcomes === 'string') parsedOutcomes = JSON.parse(parsedOutcomes); } catch {}
          if (Array.isArray(parsedPrices) && Array.isArray(parsedOutcomes)) {
            for (let i = 0; i < parsedPrices.length; i++) {
              if (parseFloat(parsedPrices[i] || 0) >= 0.95 && parsedOutcomes[i]) {
                winningOutcome = parsedOutcomes[i]; break;
              }
            }
          }
          if (!winningOutcome && Array.isArray(market.tokens)) {
            for (const t of market.tokens) {
              if (parseFloat(t.price || 0) >= 0.95) { winningOutcome = t.outcome; break; }
            }
          }
          if (!winningOutcome && market.winner) winningOutcome = market.winner;
        }
        const tokenIds = [];
        let clobIds = market.clobTokenIds || market.clob_token_ids;
        try { if (typeof clobIds === 'string') clobIds = JSON.parse(clobIds); } catch {}
        if (Array.isArray(clobIds)) tokenIds.push(...clobIds);
        if (Array.isArray(market.tokens)) {
          for (const t of market.tokens) if (t.token_id || t.tokenId) tokenIds.push(t.token_id || t.tokenId);
        }
        const info = { marketClosed, winningOutcome, conditionId: market.conditionId || '' };
        for (const tid of tokenIds) { if (tid) lookup.set(String(tid), info); }
        if (marketClosed && winningOutcome) resolved++;
      }
    } catch {}
    await new Promise(r => setTimeout(r, 40));
  }
  console.log(`  Lookup: ${lookup.size} tokenIds, ${resolved} resolved markets`);
  return lookup;
}

// ─── Manual per-market PnL from raw events (independent of analyzer) ───
function manualPnlBreakdown(events) {
  const markets = new Map();
  for (const ev of events) {
    const cid = ev.conditionId;
    if (!cid) continue;
    if (!markets.has(cid)) markets.set(cid, []);
    markets.get(cid).push(ev);
  }

  const results = [];
  for (const [cid, evts] of markets) {
    let totalBought = 0, totalSold = 0, totalRedeemed = 0;
    let sharesBought = 0, sharesSold = 0, sharesRedeemed = 0;
    let outcome = '', asset = '', title = '';
    let dupeCheck = new Set();
    let dupes = 0;

    for (const ev of evts) {
      // Duplicate detection: same timestamp + type + size
      const sig = `${ev.timestamp}-${ev.type}-${ev.side}-${ev.size}`;
      if (dupeCheck.has(sig)) { dupes++; continue; }
      dupeCheck.add(sig);

      const type = (ev.type || '').toUpperCase();
      const side = (ev.side || '').toUpperCase();
      const size = parseFloat(ev.size || ev.shares || 0) || 0;
      const price = parseFloat(ev.price || 0) || 0;
      const usdcSize = parseFloat(ev.usdcSize || 0) || 0;
      if (!outcome && ev.outcome) outcome = ev.outcome;
      if (!asset && ev.asset) asset = ev.asset;
      if (!title && ev.title) title = ev.title;

      if (type === 'REDEEM') {
        totalRedeemed += usdcSize || 0;
        sharesRedeemed += size;
      } else if (side === 'BUY') {
        totalBought += usdcSize || (size * price);
        sharesBought += size;
      } else if (side === 'SELL') {
        totalSold += usdcSize || (size * price);
        sharesSold += size;
      }
    }

    const pnl = (totalSold + totalRedeemed) - totalBought;
    const netShares = sharesBought - sharesSold - sharesRedeemed;
    const closed = netShares < (sharesBought * 0.05);

    results.push({
      cid, outcome, asset, title: (title || '').slice(0, 40),
      totalBought, totalSold, totalRedeemed, pnl,
      sharesBought, sharesSold, sharesRedeemed, netShares,
      closed, dupes, eventCount: evts.length,
    });
  }
  return results;
}

// ─── MAIN ───
async function main() {
  const pool = loadPool();
  if (!pool) { console.error('No pool at data/wallets.json.gz'); process.exit(1); }

  const args = process.argv.slice(2);
  const doTop5 = args.includes('--top5');
  const positional = args.filter(a => !a.startsWith('--'));

  // Collect wallets to investigate
  let targets = [];
  if (positional.length > 0) {
    for (const a of positional) {
      const addr = a.toLowerCase();
      const entry = pool[addr] || Object.values(pool).find(w => w.address?.toLowerCase() === addr);
      if (!entry) { console.error(`Wallet ${addr} not in pool`); continue; }
      targets.push(entry);
    }
  } else {
    // Find pure overcounters
    const overcounters = [];
    for (const w of Object.values(pool)) {
      if (!w.stats || !w.goldskyPnl || w.goldskyPnl <= 0) continue;
      if (w.stats.tradesTruncated) continue;
      if ((w.stats.unredeemedWins || 0) > 0) continue;
      const ratio = (w.stats.totalPnl || 0) / w.goldskyPnl;
      if (ratio > 1.5) overcounters.push({ entry: w, ratio });
    }
    overcounters.sort((a, b) => b.ratio - a.ratio);
    targets = (doTop5 ? overcounters.slice(0, 5) : overcounters.slice(0, 1)).map(o => o.entry);
    console.log(`Found ${overcounters.length} pure overcounters. Investigating ${targets.length}.`);
  }

  if (targets.length === 0) { console.error('No targets'); process.exit(1); }

  for (const entry of targets) {
    const addr = entry.address.toLowerCase();
    const goldsky = Number(entry.goldskyPnl || 0);
    const storedSample = Number(entry.stats?.totalPnl || 0);
    const ratio = goldsky > 0 ? (storedSample / goldsky).toFixed(1) : '?';

    console.log('\n' + '═'.repeat(70));
    console.log(`  WALLET: ${addr}`);
    console.log(`  Goldsky PnL:    $${goldsky.toFixed(2)}`);
    console.log(`  Stored sample:  $${storedSample.toFixed(2)}  (${ratio}x)`);
    console.log(`  statsSpanDays:  ${entry.stats?.statsSpanDays || '?'}`);
    console.log(`  truncated:      ${entry.stats?.tradesTruncated}`);
    console.log(`  unredeemedWins: ${entry.stats?.unredeemedWins || 0}`);
    console.log(`  worthlessLoss:  ${entry.stats?.worthlessLosses || 0}`);
    console.log(`  resolved:       ${entry.stats?.resolvedMarkets || 0}`);
    console.log(`  openPositions:  ${entry.stats?.openPositions || 0}`);
    console.log('═'.repeat(70));

    // ── Step 1: Fetch Goldsky per-position data ──
    console.log('\n── Step 1: Goldsky per-position PnL ──');
    let gsPositions;
    try {
      const gs = await fetchGoldskyPositions(addr);
      gsPositions = gs.positions;
      const gsTotalPnl = gsPositions.reduce((s, p) => s + p.pnl, 0);
      const gsTotalBought = gsPositions.reduce((s, p) => s + p.totalBought, 0);
      const gsWins = gsPositions.filter(p => p.pnl > 0).length;
      const gsLosses = gsPositions.filter(p => p.pnl < 0).length;
      const gsZero = gsPositions.filter(p => p.pnl === 0).length;
      console.log(`  Positions: ${gsPositions.length}`);
      console.log(`  Total PnL: $${gsTotalPnl.toFixed(2)}`);
      console.log(`  Total bought: $${gsTotalBought.toFixed(2)}`);
      console.log(`  Winners/Losers/Zero: ${gsWins}/${gsLosses}/${gsZero}`);

      // Top/bottom Goldsky positions
      gsPositions.sort((a, b) => b.pnl - a.pnl);
      console.log('\n  Top 5 Goldsky positions:');
      for (const p of gsPositions.slice(0, 5)) {
        console.log(`    PnL: $${p.pnl.toFixed(2).padStart(10)}  bought: $${p.totalBought.toFixed(2).padStart(8)}  token: ${p.token?.slice(0, 12) || '?'}  id: ${p.id?.slice(0, 20)}`);
      }
      console.log('  Bottom 5 Goldsky positions:');
      const bottom = [...gsPositions].sort((a, b) => a.pnl - b.pnl).slice(0, 5);
      for (const p of bottom) {
        console.log(`    PnL: $${p.pnl.toFixed(2).padStart(10)}  bought: $${p.totalBought.toFixed(2).padStart(8)}  token: ${p.token?.slice(0, 12) || '?'}  id: ${p.id?.slice(0, 20)}`);
      }
    } catch (err) {
      console.error(`  Goldsky fetch failed: ${err.message}`);
      gsPositions = [];
    }

    // ── Step 2: Fetch Data API /activity ──
    console.log('\n── Step 2: Data API /activity ──');
    const events = await fetchAllActivity(addr, { maxEvents: 5000 });
    console.log(`  Events: ${events.length} (truncated: ${events.truncated})`);

    if (events.length === 0) {
      console.log('  ⚠ No events — skipping rest of analysis');
      continue;
    }

    // Event type breakdown
    const typeCounts = {};
    for (const ev of events) {
      const t = (ev.type || 'TRADE').toUpperCase();
      typeCounts[t] = (typeCounts[t] || 0) + 1;
    }
    console.log(`  Event types: ${JSON.stringify(typeCounts)}`);

    // ── Step 3: Run analyzer with marketLookup ──
    console.log('\n── Step 3: Analyzer (with marketLookup) ──');
    const marketLookup = await buildMarketLookup(events);
    const stats = analyzeTradeHistory(events, { marketLookup });
    if (!stats) { console.log('  ⚠ Analyzer returned null'); continue; }

    console.log(`  totalPnl:         $${stats.totalPnl.toFixed(2)}`);
    console.log(`  resolvedMarkets:  ${stats.resolvedMarkets}`);
    console.log(`  wins/losses:      ${stats.wins}/${stats.losses}`);
    console.log(`  winRate:          ${(stats.winRate * 100).toFixed(1)}%`);
    console.log(`  openPositions:    ${stats.openPositions}`);
    console.log(`  tradesTruncated:  ${stats.tradesTruncated}`);
    console.log(`  statsSpanDays:    ${stats.statsSpanDays}`);
    console.log(`  unredeemedWins:   ${stats.unredeemedWins}`);
    console.log(`  worthlessLosses:  ${stats.worthlessLosses}`);

    // ── Step 4: Manual per-market PnL breakdown ──
    console.log('\n── Step 4: Manual per-market PnL ──');
    const manual = manualPnlBreakdown(events);
    const manualTotal = manual.reduce((s, m) => s + m.pnl, 0);
    const closedManual = manual.filter(m => m.closed);
    const openManual = manual.filter(m => !m.closed);
    const closedPnl = closedManual.reduce((s, m) => s + m.pnl, 0);
    const openPnl = openManual.reduce((s, m) => s + m.pnl, 0);
    const totalDupes = manual.reduce((s, m) => s + m.dupes, 0);
    console.log(`  Total markets:    ${manual.length}`);
    console.log(`  Total PnL:        $${manualTotal.toFixed(2)}`);
    console.log(`  Closed markets:   ${closedManual.length} (PnL: $${closedPnl.toFixed(2)})`);
    console.log(`  Open markets:     ${openManual.length} (unrealized PnL: $${openPnl.toFixed(2)})`);
    console.log(`  Duplicate events: ${totalDupes}`);

    // Top 10 manual markets
    manual.sort((a, b) => b.pnl - a.pnl);
    console.log('\n  Top 10 markets (manual PnL):');
    for (const m of manual.slice(0, 10)) {
      console.log(`    ${m.cid?.slice(0, 12)}...  PnL: $${m.pnl.toFixed(2).padStart(10)}  bought: $${m.totalBought.toFixed(2).padStart(8)}  sold: $${m.totalSold.toFixed(2).padStart(8)}  redeemed: $${m.totalRedeemed.toFixed(2).padStart(8)}  net: ${m.netShares.toFixed(1).padStart(6)}  ${m.closed ? 'CLOSED' : 'OPEN'}  ${m.title}`);
    }

    // ── Step 5: Cross-reference Goldsky vs Manual ──
    console.log('\n── Step 5: Goldsky vs Data API cross-reference ──');
    // Build a map of Goldsky PnL by token for comparison
    const gsMap = new Map();
    for (const p of gsPositions) {
      if (p.token) gsMap.set(p.token, p);
    }

    // For each manual market, find the matching Goldsky position and compare
    let matched = 0, unmatched = 0, bigGaps = [];
    for (const m of manual) {
      const gsPos = gsMap.get(m.asset);
      if (!gsPos) { unmatched++; continue; }
      matched++;
      const gap = m.pnl - gsPos.pnl;
      if (Math.abs(gap) > 10) {
        bigGaps.push({
          cid: m.cid,
          title: m.title,
          manualPnl: m.pnl,
          goldskyPnl: gsPos.pnl,
          gap,
          manualBought: m.totalBought,
          goldskyBought: gsPos.totalBought,
          netShares: m.netShares,
          closed: m.closed,
        });
      }
    }
    console.log(`  Matched positions: ${matched} / ${manual.length}`);
    console.log(`  Unmatched (no Goldsky position): ${unmatched}`);
    bigGaps.sort((a, b) => b.gap - a.gap);
    console.log(`  Markets with >$10 PnL gap: ${bigGaps.length}`);

    if (bigGaps.length > 0) {
      console.log('\n  Top 15 gap markets (manual PnL - Goldsky PnL):');
      for (const g of bigGaps.slice(0, 15)) {
        console.log(`    ${g.cid?.slice(0, 12)}...  manual: $${g.manualPnl.toFixed(2).padStart(10)}  goldsky: $${g.goldskyPnl.toFixed(2).padStart(10)}  GAP: $${g.gap.toFixed(2).padStart(10)}  manBought: $${g.manualBought.toFixed(2).padStart(8)}  gsBought: $${g.goldskyBought.toFixed(2).padStart(8)}  net: ${g.netShares.toFixed(1)}  ${g.closed ? 'CLOSED' : 'OPEN'}  ${g.title}`);
      }
      console.log('  Bottom 5 gap markets (Goldsky higher than manual):');
      const bottomGaps = [...bigGaps].sort((a, b) => a.gap - b.gap).slice(0, 5);
      for (const g of bottomGaps) {
        console.log(`    ${g.cid?.slice(0, 12)}...  manual: $${g.manualPnl.toFixed(2).padStart(10)}  goldsky: $${g.goldskyPnl.toFixed(2).padStart(10)}  GAP: $${g.gap.toFixed(2).padStart(10)}  manBought: $${g.manualBought.toFixed(2).padStart(8)}  gsBought: $${g.goldskyBought.toFixed(2).padStart(8)}  net: ${g.netShares.toFixed(1)}  ${g.closed ? 'CLOSED' : 'OPEN'}  ${g.title}`);
      }
    }

    // ── Step 6: Sanity checks ──
    console.log('\n── Step 6: Sanity checks ──');

    // Check for price > 1.0 (binary markets should have 0-1 prices)
    const pricesOver1 = events.filter(e => parseFloat(e.price || 0) > 1.01);
    console.log(`  Events with price > 1.01: ${pricesOver1.length}`);
    if (pricesOver1.length > 0) {
      console.log('  ⚠ FOUND! Sample:');
      for (const ev of pricesOver1.slice(0, 5)) {
        console.log(`    type=${ev.type} side=${ev.side} price=${ev.price} size=${ev.size} usdcSize=${ev.usdcSize} asset=${ev.asset?.slice(0, 12)}`);
      }
    }

    // Check if usdcSize and size*price agree
    let mismatchCount = 0;
    let maxMismatch = 0;
    for (const ev of events) {
      if (!ev.usdcSize || !ev.price || !ev.size) continue;
      const fromUsdc = parseFloat(ev.usdcSize);
      const fromSP = parseFloat(ev.size) * parseFloat(ev.price);
      if (fromUsdc > 0 && fromSP > 0) {
        const diff = Math.abs(fromUsdc - fromSP);
        if (diff > 0.01 * fromUsdc && diff > 0.5) { // >1% and >$0.50
          mismatchCount++;
          maxMismatch = Math.max(maxMismatch, diff);
        }
      }
    }
    console.log(`  usdcSize vs size*price mismatches (>1% & >$0.50): ${mismatchCount}`);
    if (mismatchCount > 0) console.log(`  Max mismatch: $${maxMismatch.toFixed(2)}`);

    // Check for very large individual trades
    const largeTrades = events.filter(e => parseFloat(e.usdcSize || 0) > 10000);
    console.log(`  Events with usdcSize > $10,000: ${largeTrades.length}`);

    // Check if analyzer uses size*price or usdcSize
    console.log('\n  NOTE: Analyzer uses size*price for PnL (not usdcSize).');
    console.log('  If usdcSize != size*price systematically, that\'s the overcounting root cause.');

    // ── Step 7: Diagnosis summary ──
    console.log('\n' + '─'.repeat(70));
    console.log('  DIAGNOSIS SUMMARY');
    console.log('─'.repeat(70));
    console.log(`  Goldsky lifetime PnL:  $${goldsky.toFixed(2)}`);
    console.log(`  Analyzer PnL (fresh):  $${stats.totalPnl.toFixed(2)}`);
    console.log(`  Manual PnL:            $${manualTotal.toFixed(2)}`);
    console.log(`  Goldsky positions:     ${gsPositions.length}`);
    console.log(`  Data API markets:      ${manual.length}`);
    console.log(`  Matched:               ${matched}`);
    console.log(`  Markets with big gaps: ${bigGaps.length}`);
    console.log(`  Total gap PnL:         $${bigGaps.reduce((s, g) => s + g.gap, 0).toFixed(2)}`);
    console.log(`  Duplicate events:      ${totalDupes}`);
    console.log(`  Prices > 1.01:         ${pricesOver1.length}`);
    console.log(`  USDC mismatches:       ${mismatchCount}`);

    const totalGap = stats.totalPnl - goldsky;
    const gapFromBigMarkets = bigGaps.reduce((s, g) => s + g.gap, 0);
    if (gapFromBigMarkets > totalGap * 0.5) {
      console.log(`\n  ⚠ ${(gapFromBigMarkets / totalGap * 100).toFixed(0)}% of the overcounting comes from per-market PnL gaps vs Goldsky.`);
      console.log('    Root cause is likely in how the analyzer computes PnL vs how Goldsky does.');
    }
    if (mismatchCount > 10) {
      console.log(`\n  ⚠ ${mismatchCount} events have size*price != usdcSize. The analyzer uses size*price.`);
      console.log('    If Goldsky uses actual USDC transferred (matching usdcSize), this explains the gap.');
    }
    if (pricesOver1.length > 0) {
      console.log(`\n  ⚠ ${pricesOver1.length} events have price > 1.01 — possible non-binary market or data error.`);
    }
    if (totalDupes > 0) {
      console.log(`\n  ⚠ ${totalDupes} duplicate events detected — may be inflating PnL.`);
    }
  }
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
