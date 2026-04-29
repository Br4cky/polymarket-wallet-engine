// Look up a specific market in our signals + trade data and report
// who bet on what, what we emitted, what happened.
//
// Usage:
//   node scripts/lookup-market.mjs <slug-or-keyword>
//   node scripts/lookup-market.mjs "lakers"
//   node scripts/lookup-market.mjs "nba-lal-hou-2026-04-26"

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const search = process.argv[2] || '';
if (!search) {
  console.log('Usage: node scripts/lookup-market.mjs <keyword>');
  process.exit(1);
}
const term = search.toLowerCase();

const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());
const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const pool = walletsData.pool || walletsData;

// Find matching signals (active + history)
const all = [
  ...Object.values(signalsData.active || {}),
  ...(signalsData.history || []),
];

const matched = all.filter(s => {
  const t = (s.marketTitle || '').toLowerCase();
  const sl = (s.slug || '').toLowerCase();
  const es = (s.eventSlug || '').toLowerCase();
  return t.includes(term) || sl.includes(term) || es.includes(term);
});

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Market lookup: "' + search + '"');
console.log('═══════════════════════════════════════════════════════════════════');
console.log('  Matching signals (active + history): ' + matched.length);
console.log();

if (matched.length === 0) {
  console.log('  No signals found matching that term.');
  console.log('  This means we never emitted a signal on this market —');
  console.log('  either no convergence reached our gates, or it was killed at one.');
  process.exit(0);
}

for (const sig of matched) {
  console.log('  ── ' + (sig.signalType || '?') + ' signal ──');
  console.log('    ID:               ' + sig.signalId);
  console.log('    Title:            ' + sig.marketTitle);
  console.log('    Slug:             ' + sig.slug);
  console.log('    Event slug:       ' + sig.eventSlug);
  console.log('    Direction:        ' + sig.direction);
  console.log('    Status:           ' + (sig.status || (sig.closedAt ? 'closed' : 'unknown')));
  console.log('    Wallet count:     ' + (sig.walletCount || (sig.soloWallet ? 1 : '—')));
  console.log('    Avg entry price:  ' + (sig.avgEntryPrice != null ? (sig.avgEntryPrice * 100).toFixed(0) + '¢' : '—'));
  console.log('    Open market price:' + (sig.openMarketPrice != null ? (sig.openMarketPrice * 100).toFixed(0) + '¢' : '—'));
  console.log('    Current price:    ' + (sig.currentMarketPrice != null ? (sig.currentMarketPrice * 100).toFixed(0) + '¢' : '—'));
  console.log('    Total buy size:   ' + (sig.totalBuySize != null ? '$' + sig.totalBuySize.toFixed(0) : '—'));
  console.log('    Confidence:       ' + (sig.confidence != null ? sig.confidence.toFixed(0) + ' (' + (sig.tier || '?') + ')' : '—'));
  console.log('    Opened at:        ' + (sig.openedAt || '—'));
  console.log('    Closed at:        ' + (sig.closedAt || '—'));
  console.log('    Outcome:          ' + (sig.outcome || '—'));
  console.log('    Close reason:     ' + (sig.closeReason || '—'));
  console.log('    Winning outcome:  ' + (sig.winningOutcome || '—'));
  console.log('    Signal return:    ' + (sig.signalReturn != null ? (sig.signalReturn >= 0 ? '+' : '') + sig.signalReturn.toFixed(1) + '%' : '—'));
  console.log('    Resolved by:      ' + (sig.resolvedBy || '—'));

  // Wallet breakdown
  const wallets = sig.currentWallets || (sig.soloWallet ? [{ address: sig.soloWallet }] : []);
  if (Array.isArray(wallets) && wallets.length > 0) {
    console.log('    Wallets contributing (' + wallets.length + '):');
    wallets.sort((a, b) => (b.score || 0) - (a.score || 0));
    for (const w of wallets) {
      const addr = (w.address || '').toLowerCase();
      const poolEntry = pool[addr];
      const poolScore = poolEntry?.score != null ? poolEntry.score.toFixed(1) : '—';
      const status = poolEntry?.status || (poolEntry ? 'active' : 'NOT IN POOL');
      console.log('      ' + (w.address || '').slice(0, 14) + '…  ' +
        'sigScore=' + (w.score != null ? w.score.toFixed(1) : '—').padEnd(5) +
        '  poolScore=' + poolScore.padEnd(5) +
        '  size=$' + (w.buySize != null ? w.buySize.toFixed(0) : '—').padEnd(7) +
        '  entry=' + (w.avgPrice != null ? (w.avgPrice * 100).toFixed(0) + '¢' : '—').padEnd(5) +
        '  status=' + status);
    }
  }
  console.log();
}
