// Smart force-resolver for stale active signals.
//
// Aggressively chases down resolutions for signals stuck in active where
// the underlying market has finished but our cache hasn't caught up.
// Uses multiple data sources in priority order:
//
//   1. Fresh Gamma /markets fetch (bypass cached state)
//   2. Polymarket /markets/{slug} as alternate Gamma endpoint
//   3. Wallet REDEEM events for that conditionId via Data API /activity
//      — REDEEMs only happen on resolved markets and the redeem price
//      tells us the outcome
//   4. Cached currentPrice extremes (0.0 or 1.0 = settled)
//
// Only voids as last resort if all four sources return nothing.
//
// Usage:
//   node scripts/force-resolve-stale.mjs           # dry-run, report
//   node scripts/force-resolve-stale.mjs --apply   # commit resolutions

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const APPLY = process.argv.includes('--apply');
// --recover: also retry recently-voided signals (closeReason in
// voided_no_outcome / past_enddate_no_close / market_delisted) — moves
// them BACK to active as ephemeral records, runs the resolver, then
// either resolves with outcome or re-voids if still unresolvable.
const RECOVER = process.argv.includes('--recover');

const GAMMA_MARKETS = 'https://gamma-api.polymarket.com/markets';

const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const marketsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/markets.json.gz'))).toString());

const marketLookup = new Map();
for (const [tid, m] of Object.entries(marketsData)) {
  if (!m) continue;
  marketLookup.set(String(tid), m);
  if (m.conditionId) marketLookup.set(String(m.conditionId), m);
}

const active = signalsData.active || {};
const history = signalsData.history || [];
const now = Date.now();

// ── Identify stale signals (same logic as audit) ──────────────────────
const RECOVERABLE_VOID_REASONS = new Set([
  'voided_no_outcome',
  'past_enddate_no_close',
  'market_delisted',
  'stale_audit',
  'unresolved_void',
]);

const stale = [];

// Active stale signals
for (const sig of Object.values(active)) {
  const tid = sig.tokenId || sig.asset;
  const cid = sig.conditionId;
  const mi = (tid && marketLookup.get(String(tid))) || (cid && marketLookup.get(String(cid))) || null;
  const ageHours = sig.openedAt ? (now - new Date(sig.openedAt).getTime()) / 3600000 : 0;
  if (ageHours < 48) continue;

  let bucket = null;
  if (!mi) bucket = 'NO-MARKET';
  else if (mi.marketClosed === true && !mi.winningOutcome) bucket = 'NO-WINNER';
  else if (mi.marketClosed !== true && mi.endDate && new Date(mi.endDate).getTime() < now) bucket = 'PAST-ENDDATE';

  if (bucket) stale.push({ sig, mi, bucket, ageHours, source: 'active' });
}

// Recover-mode: also re-process recently voided signals from history
let recoverCandidates = [];
if (RECOVER) {
  recoverCandidates = history.filter(s => RECOVERABLE_VOID_REASONS.has(s.closeReason || ''));
  console.log('  --recover: found ' + recoverCandidates.length + ' previously-voided signals to retry');
  for (const sig of recoverCandidates) {
    const tid = sig.tokenId || sig.asset;
    const cid = sig.conditionId;
    const mi = (tid && marketLookup.get(String(tid))) || (cid && marketLookup.get(String(cid))) || null;
    stale.push({ sig, mi, bucket: 'RECOVER', ageHours: 0, source: 'history' });
  }
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Smart force-resolver for stale signals');
console.log('═══════════════════════════════════════════════════════════════════');
console.log('  Mode: ' + (APPLY ? 'APPLY' : 'DRY-RUN'));
console.log('  Stale signals to chase: ' + stale.length + '\n');

if (stale.length === 0) {
  console.log('  Nothing stale — exiting.\n');
  process.exit(0);
}

// ── Resolution sources ───────────────────────────────────────────────
async function gammaByTokenId(tokenId) {
  try {
    const url = `${GAMMA_MARKETS}?clob_token_ids=${tokenId}&limit=1`;
    const r = await fetch(url);
    if (!r.ok) return null;
    const arr = await r.json();
    if (!Array.isArray(arr) || arr.length === 0) return null;
    return arr[0];
  } catch { return null; }
}

async function gammaBySlug(slug) {
  if (!slug) return null;
  try {
    // Slugs in our data are sometimes "event-slug/market-slug"; query market portion
    const marketSlug = slug.includes('/') ? slug.split('/').pop() : slug;
    const url = `${GAMMA_MARKETS}?slug=${encodeURIComponent(marketSlug)}&limit=1`;
    const r = await fetch(url);
    if (!r.ok) return null;
    const arr = await r.json();
    if (!Array.isArray(arr) || arr.length === 0) return null;
    return arr[0];
  } catch { return null; }
}

function extractWinner(market) {
  if (!market) return null;
  const closed = market.closed === true || market.closed === 'true';
  if (!closed) return null;

  // Try outcomes/outcomePrices arrays
  let prices = market.outcomePrices;
  if (typeof prices === 'string') { try { prices = JSON.parse(prices); } catch {} }
  let outcomes = market.outcomes;
  if (typeof outcomes === 'string') { try { outcomes = JSON.parse(outcomes); } catch {} }
  if (Array.isArray(prices) && Array.isArray(outcomes)) {
    for (let i = 0; i < prices.length; i++) {
      if (parseFloat(prices[i]) >= 0.95) return outcomes[i];
    }
  }
  // Fallback: tokens array
  if (Array.isArray(market.tokens)) {
    for (const t of market.tokens) {
      if (parseFloat(t.price || 0) >= 0.95) return t.outcome || null;
    }
  }
  return null;
}

async function walletRedeemOutcome(walletAddr, conditionId) {
  if (!walletAddr || !conditionId) return null;
  try {
    const url = `https://data-api.polymarket.com/activity?user=${walletAddr.toLowerCase()}&limit=200`;
    const r = await fetch(url);
    if (!r.ok) return null;
    const events = await r.json();
    if (!Array.isArray(events)) return null;
    // Find a REDEEM for this conditionId
    for (const e of events) {
      const t = (e.type || '').toUpperCase();
      if (t !== 'REDEEM') continue;
      const cid = e.conditionId || e.condition_id || '';
      if (cid.toLowerCase() !== conditionId.toLowerCase()) continue;
      // size = shares redeemed; usdcSize = payout; price = payout/size
      const size = parseFloat(e.size || e.shares || 0);
      const payout = parseFloat(e.usdcSize || e.payout || 0);
      if (size <= 0) continue;
      const impliedPrice = payout / size;
      return { redeemed: true, impliedPrice, payout, size, outcome: e.outcome || null };
    }
    return null;
  } catch { return null; }
}

// ── Process each stale signal ──────────────────────────────────────────
const resolutions = [];
let i = 0;
for (const item of stale) {
  i++;
  const { sig, mi, bucket } = item;
  const prefix = `  [${i}/${stale.length}] ${sig.signalType || '?'} | ${(sig.marketTitle || '').slice(0, 50)}`;
  console.log(prefix);

  let outcome = null;
  let signalReturn = null;
  let resolvedBy = null;
  let winningOutcome = null;

  // Source 1: Fresh Gamma by tokenId
  const tokenId = sig.tokenId || sig.asset;
  if (tokenId) {
    const fresh = await gammaByTokenId(tokenId);
    const winner = extractWinner(fresh);
    if (winner) {
      winningOutcome = winner;
      resolvedBy = 'gamma_fresh';
    }
  }

  // Source 2: Gamma by slug
  if (!winningOutcome && sig.slug) {
    const fresh = await gammaBySlug(sig.slug);
    const winner = extractWinner(fresh);
    if (winner) {
      winningOutcome = winner;
      resolvedBy = 'gamma_slug';
    }
  }

  // Source 3: REDEEM events (only if soloWallet known)
  if (!winningOutcome && sig.soloWallet && sig.conditionId) {
    const redeem = await walletRedeemOutcome(sig.soloWallet, sig.conditionId);
    if (redeem && redeem.impliedPrice >= 0.95) {
      // Wallet got near-full payout → their bet won → their direction won
      winningOutcome = sig.direction;
      resolvedBy = 'redeem_inferred_win';
    } else if (redeem && redeem.impliedPrice < 0.05) {
      // Wallet got near-zero payout → their bet lost → opposite direction won
      winningOutcome = sig.direction === 'Yes' ? 'No' : 'Yes';
      resolvedBy = 'redeem_inferred_loss';
    }
  }

  // Source 4: cached currentPrice extreme (settled)
  if (!winningOutcome && mi && (mi.currentPrice <= 0.02 || mi.currentPrice >= 0.98)) {
    if (mi.currentPrice >= 0.98) {
      winningOutcome = sig.direction;  // our side was YES on the winning token
      resolvedBy = 'price_extreme';
    } else {
      winningOutcome = sig.direction === 'Yes' ? 'No' : 'Yes';
      resolvedBy = 'price_extreme';
    }
  }

  // Compute outcome
  if (winningOutcome) {
    const ourSide = String(sig.direction || '').toLowerCase().trim();
    const winSide = String(winningOutcome).toLowerCase().trim();
    outcome = ourSide === winSide ? 'win' : 'loss';
    const open = sig.openMarketPrice || sig.avgEntryPrice || 0;
    if (outcome === 'win' && open > 0) {
      signalReturn = +((1 - open) / open * 100).toFixed(1);
    } else if (outcome === 'loss') {
      signalReturn = -100;
    }
    console.log(`        → ${resolvedBy.toUpperCase()}: outcome=${outcome} ret=${signalReturn != null ? signalReturn + '%' : '—'}`);
    resolutions.push({ signalId: sig.signalId, outcome, signalReturn, resolvedBy, winningOutcome });
  } else {
    console.log(`        → UNRESOLVED (all 4 sources empty)`);
    resolutions.push({ signalId: sig.signalId, outcome: null, signalReturn: null, resolvedBy: 'unresolved' });
  }
}

// ── Apply ──────────────────────────────────────────────────────────────
const resolved = resolutions.filter(r => r.outcome != null);
const unresolved = resolutions.filter(r => r.outcome == null);
const wins = resolved.filter(r => r.outcome === 'win').length;

console.log('\n  ── Summary ──');
console.log('  Resolved with outcome: ' + resolved.length + ' (' + wins + ' wins / ' + (resolved.length - wins) + ' losses)');
console.log('  Unresolved (all sources failed): ' + unresolved.length);

if (APPLY) {
  let updatedActive = 0, updatedHistory = 0;
  for (let idx = 0; idx < resolutions.length; idx++) {
    const r = resolutions[idx];
    const item = stale[idx];
    if (item.source === 'history') {
      // Update existing history entry in place — we found a real outcome
      const histIdx = history.findIndex(s => s.signalId === r.signalId);
      if (histIdx === -1) continue;
      if (r.outcome != null) {
        history[histIdx].outcome = r.outcome;
        history[histIdx].signalReturn = r.signalReturn;
        history[histIdx].resolvedBy = r.resolvedBy;
        history[histIdx].closeReason = 'force_resolve_recovered';
        if (r.winningOutcome) history[histIdx].winningOutcome = r.winningOutcome;
        updatedHistory++;
      }
      // If still unresolved, leave as-is (already voided)
    } else {
      // Active stale → close it
      const sig = active[r.signalId];
      if (!sig) continue;
      sig.status = 'closed';
      sig.closedAt = new Date().toISOString();
      sig.closeReason = r.outcome ? 'force_resolve' : 'unresolved_void';
      sig.outcome = r.outcome;
      sig.signalReturn = r.signalReturn;
      sig.resolvedBy = r.resolvedBy;
      if (r.winningOutcome) sig.winningOutcome = r.winningOutcome;
      delete sig.currentWallets;
      history.push(sig);
      delete active[r.signalId];
      updatedActive++;
    }
  }
  signalsData.active = active;
  signalsData.history = history;
  fs.writeFileSync(path.join(ROOT, 'data/signals.json.gz'), zlib.gzipSync(Buffer.from(JSON.stringify(signalsData))));
  console.log('  ✓ Wrote ' + updatedActive + ' active closures + ' + updatedHistory + ' history recoveries to data/signals.json.gz\n');
} else {
  console.log('  Dry-run. Pass --apply to commit.\n');
}
