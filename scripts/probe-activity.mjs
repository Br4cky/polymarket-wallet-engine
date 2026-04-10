// Pre-flight test for the /activity switchover.
//
// Verifies that:
//   1. The Data API /activity endpoint is reachable and returns events
//   2. We can see REDEEM event(s) in the feed
//   3. We know the exact field names REDEEM events carry (size, usdcSize, etc.)
//   4. Routing activity through the new analyzeTradeHistory() produces a
//      totalPnl that's closer to Goldsky's lifetime PnL than routing raw trades
//      (which was the whole point of the switch — redemptions were invisible).
//
// Usage:
//   node scripts/probe-activity.mjs                         # auto-pick a whale (worst offender — biggest goldsky vs stats gap)
//   node scripts/probe-activity.mjs --median                # pick the median pool wallet instead
//   node scripts/probe-activity.mjs 0xABC...                # probe a specific wallet
//
// Flags:
//   --median         Pick the median pool wallet (by goldskyPnl) instead of the worst offender.
//                    Useful for verifying the WR fix on a "normal" wallet — whales often look
//                    fine because their redemption history dwarfs their worthless losers.
//   --no-lookup      Skip building the marketLookup (faster, but WR fix won't fire).
//
// Run this OUTSIDE the sandbox (which blocks data-api.polymarket.com).

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

// Dynamic import so we exercise the exact code path scan.js uses.
const { fetchAllActivity, fetchAllTrades, analyzeTradeHistory } =
  await import(path.join(ROOT, 'scanner/dataApi.js'));

const DATA_API = 'https://data-api.polymarket.com';

function loadPool() {
  const file = path.join(ROOT, 'data/wallets.json.gz');
  if (!fs.existsSync(file)) return null;
  const raw = fs.readFileSync(file);
  const data = JSON.parse(zlib.gunzipSync(raw).toString());
  return data.pool || data;
}

function pickWhale(pool) {
  // Prefer the wallet with the biggest gap between goldskyPnl and stats.totalPnl.
  // Those are the ones where redemptions are being missed, so they're the best
  // test case — if /activity is wired up properly, the internal number should
  // jump up toward the Goldsky number.
  const entries = Object.values(pool).filter(w => w && w.address && w.goldskyPnl > 0);
  entries.sort((a, b) => {
    const gapA = (a.goldskyPnl || 0) - (a.stats?.totalPnl || 0);
    const gapB = (b.goldskyPnl || 0) - (b.stats?.totalPnl || 0);
    return gapB - gapA;
  });
  return entries[0] || null;
}

function pickMedian(pool) {
  // Median wallet by goldskyPnl. A "normal" pool member — useful for showing
  // that the WR fix doesn't just help whales. Normal wallets often show the
  // most dramatic WR distortion because they have fewer redeemed winners
  // padding out the sample.
  const entries = Object.values(pool).filter(w => w && w.address && w.goldskyPnl > 0);
  if (entries.length === 0) return null;
  entries.sort((a, b) => (a.goldskyPnl || 0) - (b.goldskyPnl || 0));
  return entries[Math.floor(entries.length / 2)] || null;
}

// Build a tokenId-keyed market lookup from Gamma API. We collect unique
// eventSlugs from the events (Polymarket events group related markets), then
// hit /events?slug=... for each, and read `closed` + `winningOutcome` per market.
// Returns Map<tokenId, { marketClosed, winningOutcome, conditionId }>.
async function buildMarketLookup(events) {
  const GAMMA = 'https://gamma-api.polymarket.com';
  const lookup = new Map();

  // Collect unique eventSlugs. Some events may carry only `slug` — treat both.
  const slugs = new Set();
  for (const ev of events) {
    if (ev.eventSlug) slugs.add(ev.eventSlug);
    else if (ev.slug) slugs.add(ev.slug);
  }
  if (slugs.size === 0) {
    console.log('  ⚠ No eventSlugs found in events — cannot build market lookup');
    return lookup;
  }
  console.log(`  Fetching Gamma metadata for ${slugs.size} unique event slugs...`);

  let fetched = 0;
  let resolved = 0;
  for (const slug of slugs) {
    try {
      const r = await fetch(`${GAMMA}/events?slug=${encodeURIComponent(slug)}`);
      if (!r.ok) continue;
      const arr = await r.json();
      const event = Array.isArray(arr) ? arr[0] : arr;
      if (!event || !Array.isArray(event.markets)) continue;

      for (const market of event.markets) {
        const marketClosed = market.closed === true || market.closed === 'true';
        let winningOutcome = null;

        if (marketClosed) {
          // Try outcomePrices + outcomes arrays (most reliable for resolved markets)
          let parsedPrices = market.outcomePrices;
          let parsedOutcomes = market.outcomes;
          try { if (typeof parsedPrices === 'string') parsedPrices = JSON.parse(parsedPrices); } catch {}
          try { if (typeof parsedOutcomes === 'string') parsedOutcomes = JSON.parse(parsedOutcomes); } catch {}
          if (Array.isArray(parsedPrices) && Array.isArray(parsedOutcomes)) {
            for (let i = 0; i < parsedPrices.length; i++) {
              if (parseFloat(parsedPrices[i] || 0) >= 0.95 && parsedOutcomes[i]) {
                winningOutcome = parsedOutcomes[i];
                break;
              }
            }
          }
          // Fallback: tokens array
          if (!winningOutcome && Array.isArray(market.tokens)) {
            for (const t of market.tokens) {
              if (parseFloat(t.price || 0) >= 0.95) { winningOutcome = t.outcome || null; break; }
            }
          }
          // Fallback: explicit winner field
          if (!winningOutcome && market.winner) winningOutcome = market.winner;
        }

        // Key the lookup by every tokenId this market exposes, so the analyzer
        // can find it via firstTrade.asset regardless of which side the wallet held.
        const tokenIds = [];
        let clobIds = market.clobTokenIds || market.clob_token_ids;
        try { if (typeof clobIds === 'string') clobIds = JSON.parse(clobIds); } catch {}
        if (Array.isArray(clobIds)) tokenIds.push(...clobIds);
        if (Array.isArray(market.tokens)) {
          for (const t of market.tokens) if (t.token_id || t.tokenId) tokenIds.push(t.token_id || t.tokenId);
        }

        const info = {
          marketClosed,
          winningOutcome,
          conditionId: market.conditionId || market.condition_id || '',
        };
        for (const tid of tokenIds) {
          if (tid) lookup.set(String(tid), info);
        }
        if (marketClosed && winningOutcome) resolved++;
      }
    } catch {}
    fetched++;
    if (fetched % 50 === 0) console.log(`    ...${fetched}/${slugs.size} (${lookup.size} tokens mapped, ${resolved} resolved)`);
    await new Promise(r => setTimeout(r, 50)); // gentle on Gamma
  }
  console.log(`  Lookup built: ${lookup.size} tokenIds, ${resolved} resolved markets`);
  return lookup;
}

function sectionHeader(label) {
  console.log('\n' + '─'.repeat(70));
  console.log('  ' + label);
  console.log('─'.repeat(70));
}

async function rawProbeActivity(wallet) {
  // Direct HTTP probe so we can see the raw field shape before our normaliser
  // touches it. This is what tells us whether REDEEM events use 'size'+'usdcSize'
  // or 'amount'+'payout' (the two plausible schemas we coded for).
  const url = `${DATA_API}/activity?user=${wallet.toLowerCase()}&limit=50`;
  console.log(`\nRaw probe: ${url}`);
  const r = await fetch(url);
  console.log(`Status: ${r.status}`);
  if (!r.ok) {
    console.log(`Body: ${(await r.text()).slice(0, 300)}`);
    return null;
  }
  const j = await r.json();
  if (!Array.isArray(j)) {
    console.log(`Unexpected response shape: ${JSON.stringify(j).slice(0, 300)}`);
    return null;
  }
  console.log(`Returned: ${j.length} events`);
  return j;
}

function inspectEventTypes(events) {
  const byType = new Map();
  for (const ev of events) {
    const t = (ev.type || '(missing)').toUpperCase();
    if (!byType.has(t)) byType.set(t, { count: 0, sample: ev });
    byType.get(t).count++;
  }
  console.log('\nEvent type breakdown:');
  for (const [type, { count, sample }] of byType) {
    console.log(`  ${type.padEnd(15)} ${count}`);
  }
  return byType;
}

function printSample(label, obj) {
  if (!obj) return;
  console.log(`\n${label}:`);
  const keys = Object.keys(obj).sort();
  console.log(`  Keys (${keys.length}): ${keys.join(', ')}`);
  for (const k of keys) {
    let v = obj[k];
    if (typeof v === 'string' && v.length > 60) v = v.slice(0, 60) + '…';
    if (typeof v === 'object' && v !== null) v = JSON.stringify(v).slice(0, 80);
    console.log(`    ${k.padEnd(20)} ${v}`);
  }
}

function checkRedeemFieldCoverage(redeemSample) {
  if (!redeemSample) return;
  console.log('\nREDEEM field coverage vs our normaliser:');
  const fields = [
    ['size',     redeemSample.size],
    ['shares',   redeemSample.shares],
    ['usdcSize', redeemSample.usdcSize],
    ['payout',   redeemSample.payout],
    ['amount',   redeemSample.amount],
  ];
  for (const [name, val] of fields) {
    const has = val !== undefined && val !== null;
    console.log(`  ${has ? '✓' : '·'} ${name.padEnd(10)} ${has ? String(val) : '(absent)'}`);
  }
  const size = parseFloat(redeemSample.size || redeemSample.shares || 0) || 0;
  const payout = parseFloat(redeemSample.usdcSize || redeemSample.payout || 0) || 0;
  const ok = size > 0;
  console.log(`\n  Our analyzer would ${ok ? 'ACCEPT' : 'REJECT'} this REDEEM`);
  if (ok) {
    const impliedPrice = payout > 0 ? payout / size : 0;
    console.log(`    synthetic sell price = ${impliedPrice.toFixed(4)} (payout $${payout.toFixed(2)} / ${size} shares)`);
    if (payout === 0) {
      console.log(`    ⚠ payout is 0 — this REDEEM will contribute nothing.`);
      console.log(`    If all REDEEMs look like this, the field names are wrong and we need to adjust.`);
    }
  }
}

async function main() {
  const pool = loadPool();
  if (!pool) {
    console.error('No wallet pool found at data/wallets.json.gz — run a discovery cycle first.');
    process.exit(1);
  }

  // Parse flags + positional wallet arg
  const args = process.argv.slice(2);
  const useMedian = args.includes('--median');
  const noLookup = args.includes('--no-lookup');
  const positional = args.filter(a => !a.startsWith('--'));

  let walletAddr = positional[0];
  let walletEntry;
  if (walletAddr) {
    walletAddr = walletAddr.toLowerCase();
    walletEntry = pool[walletAddr] || Object.values(pool).find(w => w.address?.toLowerCase() === walletAddr);
    if (!walletEntry) {
      console.error(`Wallet ${walletAddr} not found in pool. Known wallets: ${Object.keys(pool).length}`);
      process.exit(1);
    }
  } else {
    walletEntry = useMedian ? pickMedian(pool) : pickWhale(pool);
    if (!walletEntry) {
      console.error(`Could not pick a ${useMedian ? 'median' : 'whale'} — pool appears empty or missing goldskyPnl.`);
      process.exit(1);
    }
    walletAddr = walletEntry.address.toLowerCase();
    console.log(`  (Picked ${useMedian ? 'median' : 'worst-offender'} wallet from pool)`);
  }

  sectionHeader(`Probing wallet ${walletAddr}`);
  console.log(`  goldskyPnl:       $${Number(walletEntry.goldskyPnl || 0).toFixed(0)}`);
  console.log(`  stats.totalPnl:   $${Number(walletEntry.stats?.totalPnl || 0).toFixed(0)}   (from old /trades analyzer)`);
  console.log(`  score:            ${walletEntry.score}`);
  console.log(`  goldskyPositions: ${walletEntry.goldskyPositions}`);
  const gap = (walletEntry.goldskyPnl || 0) - (walletEntry.stats?.totalPnl || 0);
  console.log(`  gap to close:     $${gap.toFixed(0)}   (redemptions currently missing)`);

  sectionHeader('Step 1 — Raw /activity probe (verify field shape)');
  const rawEvents = await rawProbeActivity(walletAddr);
  if (!rawEvents || rawEvents.length === 0) {
    console.error('\nNo events returned. Either the wallet has no activity, or the endpoint is blocked.');
    process.exit(1);
  }
  const byType = inspectEventTypes(rawEvents);
  const tradeSample = byType.get('TRADE')?.sample || rawEvents.find(e => e.side);
  const redeemSample = byType.get('REDEEM')?.sample;
  printSample('Sample TRADE event', tradeSample);
  printSample('Sample REDEEM event', redeemSample);
  checkRedeemFieldCoverage(redeemSample);

  sectionHeader('Step 2 — Full fetch via fetchAllActivity() + fetchAllTrades()');
  console.log('(Paginated up to 5000 events each; may take ~30s)');

  const t0 = Date.now();
  const activity = await fetchAllActivity(walletAddr, { maxEvents: 5000 });
  const tActivity = Date.now() - t0;
  console.log(`  fetchAllActivity: ${activity.length} events in ${tActivity}ms (truncated=${activity.truncated})`);

  const t1 = Date.now();
  const trades = await fetchAllTrades(walletAddr, { maxTrades: 5000 });
  const tTrades = Date.now() - t1;
  console.log(`  fetchAllTrades:   ${trades.length} trades in ${tTrades}ms (truncated=${trades.truncated})`);

  const redeemCount = activity.filter(e => (e.type || '').toUpperCase() === 'REDEEM').length;
  console.log(`  REDEEM events in activity feed: ${redeemCount}`);

  sectionHeader('Step 3a — analyzeTradeHistory() on both feeds (no marketLookup)');
  const statsFromTrades = analyzeTradeHistory(trades);
  const statsFromActivity = analyzeTradeHistory(activity);

  const fmt = n => (n >= 0 ? '+' : '') + Number(n || 0).toFixed(0);
  console.log(`\n                         /trades (old)    /activity (new)   Δ`);
  console.log(`  totalPnl              $${(fmt(statsFromTrades?.totalPnl)+'').padEnd(14)}  $${(fmt(statsFromActivity?.totalPnl)+'').padEnd(14)}  $${fmt((statsFromActivity?.totalPnl || 0) - (statsFromTrades?.totalPnl || 0))}`);
  console.log(`  resolvedMarkets       ${(statsFromTrades?.resolvedMarkets+'').padEnd(15)} ${(statsFromActivity?.resolvedMarkets+'').padEnd(15)} ${(statsFromActivity?.resolvedMarkets || 0) - (statsFromTrades?.resolvedMarkets || 0)}`);
  console.log(`  wins                  ${(statsFromTrades?.wins+'').padEnd(15)} ${(statsFromActivity?.wins+'').padEnd(15)} ${(statsFromActivity?.wins || 0) - (statsFromTrades?.wins || 0)}`);
  console.log(`  losses                ${(statsFromTrades?.losses+'').padEnd(15)} ${(statsFromActivity?.losses+'').padEnd(15)} ${(statsFromActivity?.losses || 0) - (statsFromTrades?.losses || 0)}`);
  console.log(`  winRate               ${((statsFromTrades?.winRate || 0) * 100).toFixed(1).padEnd(15)}% ${((statsFromActivity?.winRate || 0) * 100).toFixed(1).padEnd(15)}%`);
  console.log(`  openPositions         ${(statsFromTrades?.openPositions+'').padEnd(15)} ${(statsFromActivity?.openPositions+'').padEnd(15)} ${(statsFromActivity?.openPositions || 0) - (statsFromTrades?.openPositions || 0)}`);
  console.log(`  tradesTruncated       ${(statsFromTrades?.tradesTruncated+'').padEnd(15)} ${(statsFromActivity?.tradesTruncated+'').padEnd(15)}`);
  console.log(`  statsSpanDays         ${(statsFromTrades?.statsSpanDays+'').padEnd(15)} ${(statsFromActivity?.statsSpanDays+'').padEnd(15)}`);

  // Step 3b — same run but with the marketLookup that catches worthless losers
  // and unredeemed winners. This is the WR fix we're verifying.
  let statsWithLookup = null;
  if (!noLookup) {
    sectionHeader('Step 3b — analyzeTradeHistory(/activity) WITH marketLookup');
    const marketLookup = await buildMarketLookup(activity);
    statsWithLookup = analyzeTradeHistory(activity, { marketLookup });
    console.log(`\n                         /activity bare    /activity+lookup   Δ`);
    console.log(`  totalPnl              $${(fmt(statsFromActivity?.totalPnl)+'').padEnd(14)}  $${(fmt(statsWithLookup?.totalPnl)+'').padEnd(14)}  $${fmt((statsWithLookup?.totalPnl || 0) - (statsFromActivity?.totalPnl || 0))}`);
    console.log(`  resolvedMarkets       ${(statsFromActivity?.resolvedMarkets+'').padEnd(15)} ${(statsWithLookup?.resolvedMarkets+'').padEnd(15)} ${(statsWithLookup?.resolvedMarkets || 0) - (statsFromActivity?.resolvedMarkets || 0)}`);
    console.log(`  wins                  ${(statsFromActivity?.wins+'').padEnd(15)} ${(statsWithLookup?.wins+'').padEnd(15)} ${(statsWithLookup?.wins || 0) - (statsFromActivity?.wins || 0)}`);
    console.log(`  losses                ${(statsFromActivity?.losses+'').padEnd(15)} ${(statsWithLookup?.losses+'').padEnd(15)} ${(statsWithLookup?.losses || 0) - (statsFromActivity?.losses || 0)}`);
    console.log(`  winRate               ${((statsFromActivity?.winRate || 0) * 100).toFixed(1).padEnd(15)}% ${((statsWithLookup?.winRate || 0) * 100).toFixed(1).padEnd(15)}%`);
    console.log(`  openPositions         ${(statsFromActivity?.openPositions+'').padEnd(15)} ${(statsWithLookup?.openPositions+'').padEnd(15)} ${(statsWithLookup?.openPositions || 0) - (statsFromActivity?.openPositions || 0)}`);
    console.log(`  unredeemedWins        -                ${(statsWithLookup?.unredeemedWins+'').padEnd(15)}`);
    console.log(`  worthlessLosses       -                ${(statsWithLookup?.worthlessLosses+'').padEnd(15)}`);
  }

  sectionHeader('Verdict');
  const goldsky = Number(walletEntry.goldskyPnl || 0);
  const oldPnl = Number(statsFromTrades?.totalPnl || 0);
  const newPnl = Number(statsFromActivity?.totalPnl || 0);
  const lookupPnl = Number(statsWithLookup?.totalPnl || 0);
  const gapOld = Math.abs(goldsky - oldPnl);
  const gapNew = Math.abs(goldsky - newPnl);
  const gapLookup = Math.abs(goldsky - lookupPnl);

  console.log(`  Goldsky lifetime PnL:         $${goldsky.toFixed(0)}`);
  console.log(`  Old analyzer (/trades):       $${oldPnl.toFixed(0)}    (gap: $${gapOld.toFixed(0)})`);
  console.log(`  New analyzer (/activity):     $${newPnl.toFixed(0)}    (gap: $${gapNew.toFixed(0)})`);
  if (statsWithLookup) {
    console.log(`  New analyzer (+marketLookup): $${lookupPnl.toFixed(0)}    (gap: $${gapLookup.toFixed(0)})`);
    const wrBare = ((statsFromActivity?.winRate || 0) * 100).toFixed(1);
    const wrLookup = ((statsWithLookup?.winRate || 0) * 100).toFixed(1);
    console.log(`  WR bare /activity:  ${wrBare}% (${statsFromActivity?.wins}W / ${statsFromActivity?.losses}L)`);
    console.log(`  WR with lookup:     ${wrLookup}% (${statsWithLookup?.wins}W / ${statsWithLookup?.losses}L)`);
    if (statsWithLookup.worthlessLosses > 0 || statsWithLookup.unredeemedWins > 0) {
      console.log(`  ✅ WR fix fired: +${statsWithLookup.unredeemedWins} unredeemed wins, +${statsWithLookup.worthlessLosses} worthless losses closed`);
    } else {
      console.log(`  · WR fix didn't fire — no open positions resolved via marketLookup`);
      console.log(`    (either all positions closed naturally, or markets aren't in the lookup)`);
    }
  }
  console.log();

  // Final verdict uses the BEST available number (lookup result if we ran it,
  // otherwise the bare /activity number) and compares against Goldsky. The old
  // logic was comparing pre-lookup numbers and printing a stale gap.
  const bestPnl = statsWithLookup ? lookupPnl : newPnl;
  const bestGap = statsWithLookup ? gapLookup : gapNew;
  const statsTruncated = statsFromActivity?.tradesTruncated === true;
  const spanDays = statsFromActivity?.statsSpanDays || 0;

  if (redeemCount === 0) {
    console.log('  ⚠ No REDEEMs in the activity feed for this wallet — try a wallet that has');
    console.log('    held positions through to resolution. Results are inconclusive.');
  } else if (bestPnl > oldPnl * 1.2 || bestPnl > oldPnl + 100 || bestGap < gapOld * 0.5) {
    console.log('  ✅ New analyzer captures materially more PnL — REDEEM handling is working.');
    if (bestGap < Math.max(100, Math.abs(goldsky) * 0.1)) {
      console.log(`  ✅ Final gap to Goldsky is only $${bestGap.toFixed(0)} — analyzer now matches lifetime PnL.`);
    } else if (statsTruncated) {
      console.log(`  ⚠ Final gap to Goldsky is $${bestGap.toFixed(0)} BUT tradesTruncated=true (sample only covers ${spanDays} days).`);
      console.log(`    This is expected: the analyzer is measuring a ${spanDays}-day recency window,`);
      console.log('    while Goldsky reports lifetime. These two numbers are SUPPOSED to diverge');
      console.log('    on heavy traders — the analyzer is the recent-behavior profile, Goldsky is the admission gate.');
    } else {
      console.log(`  ⚠ Final gap to Goldsky is $${bestGap.toFixed(0)} without truncation — unexpected. Investigate.`);
    }
  } else {
    console.log('  ❌ New analyzer produced similar or lower PnL than the old one.');
    console.log('     Likely the REDEEM field names are different from what we coded for.');
    console.log('     Check the "Sample REDEEM event" output above and adjust the normaliser');
    console.log('     in scanner/dataApi.js:analyzeTradeHistory (look for usdcSize/payout/size).');
  }
}

main().catch(err => {
  console.error('\nFATAL:', err);
  process.exit(1);
});
