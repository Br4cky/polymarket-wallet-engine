// Active signal health audit
// ───────────────────────────
// Cross-references every active signal against the market lookup to
// classify them by health:
//
//   HEALTHY-OPEN        — market open, normal active signal (long hold)
//   STALE-MARKET-CLOSED — market.marketClosed === true, signal still active
//                          (closure failed somehow)
//   STALE-NO-WINNER     — market closed but winningOutcome missing
//                          (Gamma eventual consistency — repair will catch)
//   STALE-PAST-ENDDATE  — market endDate has passed but marketClosed not
//                          yet flipped (Gamma indexer lag)
//   STALE-NO-MARKET     — tokenId not in marketLookup at all (delisted?)
//   STALE-NO-PRICE      — market open but currentPrice missing
//
// For STALE-* categories, output a force-close script.
//
// Usage:
//   node scripts/active-signal-audit.mjs              # report only
//   node scripts/active-signal-audit.mjs --apply      # force-close stale signals

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const args = process.argv.slice(2);
const APPLY = args.includes('--apply');

const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const marketsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/markets.json.gz'))).toString());

// markets.json.gz is an object KEYED BY tokenId — the long-number string
// keys ARE the tokenIds. Build a lookup that uses the key directly, and
// also index by conditionId as fallback for signals that reference cid.
const marketLookup = new Map();
if (Array.isArray(marketsData)) {
  for (const m of marketsData) {
    if (!m) continue;
    const tids = m.clobTokenIds || (m.tokenId ? [m.tokenId] : []);
    for (const t of tids) marketLookup.set(String(t), m);
    if (m.conditionId || m.condition_id) marketLookup.set(String(m.conditionId || m.condition_id), m);
  }
} else {
  for (const [tokenId, m] of Object.entries(marketsData)) {
    if (!m) continue;
    marketLookup.set(String(tokenId), m);
    if (m.conditionId || m.condition_id) marketLookup.set(String(m.conditionId || m.condition_id), m);
  }
}

const active = signalsData.active || {};
const activeArr = Object.values(active);
const now = Date.now();

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Active signal health audit');
console.log('═══════════════════════════════════════════════════════════════════');
console.log('  Total active signals: ' + activeArr.length);
console.log('  Mode: ' + (APPLY ? 'APPLY (will close stale signals)' : 'DRY-RUN'));
console.log();

const buckets = {
  'HEALTHY-OPEN': [],
  'STALE-MARKET-CLOSED': [],
  'STALE-NO-WINNER': [],
  'STALE-PAST-ENDDATE': [],
  'STALE-NO-MARKET': [],
  'STALE-NO-PRICE': [],
};

for (const sig of activeArr) {
  const tid = sig.tokenId || sig.asset;
  const cid = sig.conditionId;
  const mi = (tid && marketLookup.get(String(tid))) || (cid && marketLookup.get(String(cid))) || null;

  const ageHours = sig.openedAt ? (now - new Date(sig.openedAt).getTime()) / 3600000 : 0;
  const sinceUpdateHours = sig.lastUpdatedAt ? (now - new Date(sig.lastUpdatedAt).getTime()) / 3600000 : ageHours;

  const record = {
    signalId: sig.signalId,
    type: sig.signalType,
    title: sig.marketTitle || '',
    slug: sig.slug || '',
    openedAt: sig.openedAt,
    ageHours: ageHours,
    sinceUpdateHours: sinceUpdateHours,
    walletCount: sig.walletCount || 1,
    confidence: sig.confidence,
    direction: sig.direction,
    avgEntryPrice: sig.avgEntryPrice,
    currentPrice: sig.currentMarketPrice,
    mi,
  };

  if (!mi) {
    buckets['STALE-NO-MARKET'].push(record);
    continue;
  }
  if (mi.marketClosed === true) {
    if (mi.winningOutcome) {
      buckets['STALE-MARKET-CLOSED'].push(record);
    } else {
      buckets['STALE-NO-WINNER'].push(record);
    }
    continue;
  }
  if (mi.endDate) {
    const endMs = new Date(mi.endDate).getTime();
    if (isFinite(endMs) && endMs < now) {
      buckets['STALE-PAST-ENDDATE'].push(record);
      continue;
    }
  }
  if (!mi.currentPrice && mi.currentPrice !== 0) {
    buckets['STALE-NO-PRICE'].push(record);
    continue;
  }
  buckets['HEALTHY-OPEN'].push(record);
}

// ── Summary ─────────────────────────────────────────────────────────
console.log('  ── HEALTH BREAKDOWN ──');
for (const [k, arr] of Object.entries(buckets)) {
  const pct = activeArr.length > 0 ? (arr.length / activeArr.length * 100).toFixed(0) + '%' : '—';
  console.log('  ' + k.padEnd(22) + String(arr.length).padStart(4) + '  ' + pct.padStart(4));
}

// ── Detail per stale category ───────────────────────────────────────
const STALE_KEYS = ['STALE-MARKET-CLOSED', 'STALE-NO-WINNER', 'STALE-PAST-ENDDATE', 'STALE-NO-MARKET'];
for (const k of STALE_KEYS) {
  const arr = buckets[k];
  if (arr.length === 0) continue;
  console.log('\n  ── ' + k + ' (' + arr.length + ') ──');
  arr.sort((a, b) => b.ageHours - a.ageHours);
  console.log('  ' + 'Type'.padEnd(14) + ' ' + 'Age(d)'.padStart(7) + ' ' + 'Title');
  for (const r of arr.slice(0, 20)) {
    console.log('  ' + (r.type || '?').padEnd(14) + ' ' + (r.ageHours / 24).toFixed(1).padStart(7) + ' ' + r.title.slice(0, 60));
  }
  if (arr.length > 20) console.log('  ... and ' + (arr.length - 20) + ' more');
}

// ── HEALTHY-OPEN distribution ───────────────────────────────────────
console.log('\n  ── HEALTHY-OPEN age distribution ──');
const healthy = buckets['HEALTHY-OPEN'];
const ageBuckets = [
  ['< 24h', h => h < 24],
  ['1-3 days', h => h >= 24 && h < 72],
  ['3-7 days', h => h >= 72 && h < 168],
  ['1-2 weeks', h => h >= 168 && h < 336],
  ['2-4 weeks', h => h >= 336 && h < 672],
  ['> 4 weeks', h => h >= 672],
];
for (const [label, pred] of ageBuckets) {
  const count = healthy.filter(r => pred(r.ageHours)).length;
  console.log('  ' + label.padEnd(12) + String(count).padStart(4));
}

// Long-term holds detail
const longHolds = healthy.filter(r => r.ageHours >= 168).sort((a, b) => b.ageHours - a.ageHours);
if (longHolds.length > 0) {
  console.log('\n  ── HEALTHY-OPEN longer than 1 week (' + longHolds.length + ') ──');
  console.log('  ' + 'Type'.padEnd(14) + ' ' + 'Age(d)'.padStart(7) + ' ' + 'Price'.padStart(6) + ' ' + 'Title');
  for (const r of longHolds.slice(0, 15)) {
    const px = r.currentPrice != null ? r.currentPrice.toFixed(3) : '—';
    console.log('  ' + (r.type || '?').padEnd(14) + ' ' + (r.ageHours / 24).toFixed(1).padStart(7) + ' ' + px.padStart(6) + ' ' + r.title.slice(0, 55));
  }
}

// ── Force-close stale signals ──────────────────────────────────────
const toClose = [
  ...buckets['STALE-MARKET-CLOSED'],
  ...buckets['STALE-NO-WINNER'],
  ...buckets['STALE-PAST-ENDDATE'],
  ...buckets['STALE-NO-MARKET'],
].filter(r => r.ageHours > 48);  // only close stale signals older than 48h

console.log('\n  ── FORCE-CLOSE candidates (stale + age > 48h): ' + toClose.length + ' ──');

if (APPLY && toClose.length > 0) {
  const history = signalsData.history || [];
  let closed = 0;
  for (const r of toClose) {
    const sig = active[r.signalId];
    if (!sig) continue;
    const mi = r.mi;
    let outcome = null;
    let closeReason = 'stale_audit';
    let signalReturn = null;

    if (mi && mi.marketClosed && mi.winningOutcome) {
      // Real resolution — we know the outcome
      const winningSide = String(mi.winningOutcome).toLowerCase().trim();
      const ourSide = String(sig.direction).toLowerCase().trim();
      outcome = winningSide === ourSide ? 'win' : 'loss';
      closeReason = 'gamma_repair';
      // Compute signalReturn from open price vs terminal value (1 if win, 0 if loss)
      const open = sig.openMarketPrice || sig.avgEntryPrice || 0;
      if (open > 0) {
        signalReturn = outcome === 'win' ? +((1 - open) / open * 100).toFixed(1) : -100;
      }
    } else if (mi && mi.marketClosed && !mi.winningOutcome) {
      // Closed but no winner — void
      outcome = null;
      closeReason = 'voided_no_outcome';
    } else if (!mi) {
      // Market gone from lookup
      outcome = null;
      closeReason = 'market_delisted';
    } else {
      // Past endDate but not yet marked closed — void
      outcome = null;
      closeReason = 'past_enddate_no_close';
    }

    sig.status = 'closed';
    sig.closedAt = new Date().toISOString();
    sig.closeReason = closeReason;
    sig.outcome = outcome;
    sig.signalReturn = signalReturn;
    delete sig.currentWallets;
    history.push(sig);
    delete active[r.signalId];
    closed++;
  }
  signalsData.active = active;
  signalsData.history = history;
  fs.writeFileSync(path.join(ROOT, 'data/signals.json.gz'), zlib.gzipSync(Buffer.from(JSON.stringify(signalsData))));
  console.log('  ✓ Closed ' + closed + ' stale signals; rewrote data/signals.json.gz');
} else if (toClose.length > 0) {
  console.log('  Run with --apply to force-close these.');
  for (const r of toClose.slice(0, 10)) {
    console.log('  → ' + r.signalId + '  (' + (r.ageHours / 24).toFixed(1) + 'd old)  ' + r.title.slice(0, 50));
  }
  if (toClose.length > 10) console.log('  ... and ' + (toClose.length - 10) + ' more');
}

console.log('\n═══════════════════════════════════════════════════════════════════\n');
