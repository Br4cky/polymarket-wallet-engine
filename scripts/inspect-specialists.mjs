// Inspect rejected specialist candidates — show per-wallet:
//   1) Full signal history (WR, avg return, per-category breakdown, timeline)
//   2) Broader trade profile (decidedROI, MM score, alpha verdict, key stats)
//   3) Rejection rationale in plain English
//   4) Synthetic-score estimate (what they'd get under attribution-bypass)
//
// Side-by-side visibility lets us make per-wallet admit decisions instead
// of a blanket policy call.
//
// Usage:
//   node scripts/inspect-specialists.mjs                             # auto-find rejected candidates
//   node scripts/inspect-specialists.mjs 0x56772c 0x06a0402 0x4248f  # inspect specific prefixes
//   node scripts/inspect-specialists.mjs --min-signals 5 --min-ret 10 --min-wr 0.60

import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const { fetchAllActivity, analyzeTradeHistory, computeWalletScore } =
  await import(path.join(ROOT, 'scanner/dataApi.js'));
const { attachMMClassification } = await import(path.join(ROOT, 'scanner/mmClassifier.js'));
const { attachAlphaEvaluation } = await import(path.join(ROOT, 'scanner/alphaTest.js'));
const { buildAttributionMap, attachAttribution } = await import(path.join(ROOT, 'scanner/signalAttribution.js'));

const args = process.argv.slice(2);
const get = (f, d) => { const i = args.indexOf(f); return i >= 0 && i + 1 < args.length ? args[i + 1] : d; };
const MIN_SIGNALS = parseInt(get('--min-signals', '5'), 10);
const MIN_RET = parseFloat(get('--min-ret', '10'));
const MIN_WR = parseFloat(get('--min-wr', '0.60'));
const explicitPrefixes = args.filter(a => a.startsWith('0x'));

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/wallets.json.gz'))).toString());
const pool = walletsData.pool || walletsData;
const signalsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'data/signals.json.gz'))).toString());

const attrMap = buildAttributionMap(signalsData.history || []);
const resolved = (signalsData.history || []).filter(s => s.outcome === 'win' || s.outcome === 'loss');

// Build per-wallet category + timeline side tables for presentation
const perWalletCat = new Map();  // addr → Map(cat → {signals, wins, totalRet, retN})
const perWalletTimeline = new Map(); // addr → [{date, outcome, ret, category}]

function classifyMarket(title) {
  const q = (title || '').toLowerCase();
  if (!q) return 'other';
  if (/dota|lol|league|valorant|cs:?go|cs2|counter-strike|call of duty|rocket league|overwatch|starcraft|hearthstone|apex|fortnite|pubg/.test(q)) return 'esports';
  if (/\blaunch a token|\btoken launch|\btge\b|token on /.test(q)) return 'token-launch';
  if (/bitcoin|btc|ethereum|eth|solana|sol\b|doge|xrp|crypto|coin/.test(q)) {
    if (/reach|above|below|hit|close|\$|\sup\b|\sdown\b|end above|end below/.test(q)) return 'crypto-updown';
    return 'crypto-other';
  }
  if (/nhl|stanley cup| hockey /.test(q)) return 'nhl';
  if (/nba|lakers|celtics|warriors|knicks|nba playoffs/.test(q)) return 'nba';
  if (/nfl|super bowl|touchdown|quarterback/.test(q)) return 'nfl';
  if (/mlb|baseball|world series/.test(q)) return 'mlb';
  if (/epl|premier league|champions league|la liga|bundesliga|serie a|manchester|arsenal|liverpool|chelsea/.test(q)) return 'soccer';
  if (/tennis|wimbledon|\bus open\b|french open|atp|wta/.test(q)) return 'tennis';
  if (/ufc|\bmma\b|fight night/.test(q)) return 'mma';
  if (/ pga |golf|masters tournament/.test(q)) return 'golf';
  if (/f1\b|formula 1|grand prix/.test(q)) return 'f1';
  if (/trump|biden|harris|election|senate|house race|republican|democrat|congressional/.test(q)) return 'politics';
  if (/ai\b|openai|anthropic|gpt|gemini|llm|tech|apple|google|microsoft|nvidia/.test(q)) return 'ai-tech';
  if (/weather|temperature|hurricane|tornado|snow|rainfall/.test(q)) return 'weather';
  if (/fed rate|fomc|inflation|cpi|ppi|gdp|recession|jobs report|unemployment|nonfarm/.test(q)) return 'macro';
  if (/ipo|earnings|revenue|guidance|\beps\b/.test(q)) return 'macro';
  if (/spacex|starship|mission|nasa|rocket launch/.test(q)) return 'news-event';
  if (/announce|statement|\bsay\b|\bsays\b|tweet|post|comment/.test(q)) return 'news-event';
  if (/\bwill .+ by |\bwill .+ before |\bwill .+ on /.test(q)) return 'news-event';
  return 'other';
}

for (const sig of resolved) {
  const cat = classifyMarket(sig.marketTitle || '');
  const ws = new Set();
  if (Array.isArray(sig.currentWallets)) sig.currentWallets.forEach(w => w && w.address && ws.add(w.address.toLowerCase()));
  if (sig.soloWallet) ws.add(sig.soloWallet.toLowerCase());
  const ret = typeof sig.signalReturn === 'number' ? sig.signalReturn : null;

  for (const addr of ws) {
    if (!perWalletCat.has(addr)) perWalletCat.set(addr, new Map());
    const m = perWalletCat.get(addr);
    if (!m.has(cat)) m.set(cat, { signals: 0, wins: 0, totalRet: 0, retN: 0 });
    const r = m.get(cat);
    r.signals++;
    if (sig.outcome === 'win') r.wins++;
    if (ret !== null) { r.totalRet += ret; r.retN++; }

    if (!perWalletTimeline.has(addr)) perWalletTimeline.set(addr, []);
    perWalletTimeline.get(addr).push({
      at: sig.closedAt || sig.openedAt || '',
      outcome: sig.outcome,
      ret,
      cat,
      title: (sig.marketTitle || '').slice(0, 60),
    });
  }
}

// ── Pick candidates ──────────────────────────────────────────────────────
let candidates;
if (explicitPrefixes.length > 0) {
  candidates = [];
  for (const pfx of explicitPrefixes) {
    const addr = [...attrMap.keys()].find(a => a.toLowerCase().startsWith(pfx.toLowerCase()));
    if (addr) {
      const a = attrMap.get(addr);
      candidates.push({ addr, ...a, retPct: a.avgReturn * 100 });
    } else {
      console.log(`(no match for prefix ${pfx})`);
    }
  }
} else {
  candidates = [];
  for (const [addr, a] of attrMap) {
    if (a.signals < MIN_SIGNALS) continue;
    const retPct = a.avgReturn * 100;
    if (retPct < MIN_RET) continue;
    if (a.wr < MIN_WR) continue;
    const existing = pool[addr];
    if (existing && existing.status !== 'removed') continue;
    candidates.push({ addr, ...a, retPct });
  }
  candidates.sort((a, b) => b.retPct - a.retPct);
}

console.log('\n═══════════════════════════════════════════════════════════════════');
console.log('  Specialist inspection — full profile per rejected candidate');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log(`  Candidates to inspect: ${candidates.length}\n`);

// ── Inspect each ─────────────────────────────────────────────────────────
const summary = [];
for (let i = 0; i < candidates.length; i++) {
  const c = candidates[i];
  const shortAddr = c.addr.slice(0, 10);
  console.log(`\n──────────────────────────────────────────────────────────`);
  console.log(`  ${i+1}/${candidates.length}  ${c.addr}`);
  console.log(`──────────────────────────────────────────────────────────`);

  // SIGNAL SIDE
  console.log(`  ── Signal-emission record ──`);
  console.log(`    signals:           ${c.signals}  (${c.wins}W/${c.signals - c.wins}L)`);
  console.log(`    WR:                ${(c.wr * 100).toFixed(1)}%`);
  console.log(`    avg return:        ${c.retPct >= 0 ? '+' : ''}${c.retPct.toFixed(1)}%`);
  const catMap = perWalletCat.get(c.addr);
  if (catMap) {
    console.log(`    category breakdown:`);
    const rows = [...catMap.entries()].map(([cat, r]) => ({
      cat, N: r.signals, wr: r.wins / r.signals, avgRet: r.retN > 0 ? r.totalRet / r.retN : 0,
    }));
    rows.sort((a, b) => b.N - a.N);
    for (const r of rows) {
      const wr = (r.wr * 100).toFixed(0) + '%';
      const ret = (r.avgRet >= 0 ? '+' : '') + r.avgRet.toFixed(1) + '%';
      console.log(`      ${r.cat.padEnd(15)} N=${String(r.N).padStart(3)}  WR ${wr.padStart(4)}  Ret ${ret.padStart(7)}`);
    }
  }
  const tl = (perWalletTimeline.get(c.addr) || []).sort((a, b) => (a.at || '').localeCompare(b.at || ''));
  if (tl.length > 0) {
    console.log(`    first signal: ${(tl[0].at || '').slice(0, 10)}  last signal: ${(tl[tl.length-1].at || '').slice(0, 10)}`);
  }

  // POOL STATUS
  const existing = pool[c.addr];
  console.log(`    pool status:       ${existing ? (existing.status || 'active') + (existing.removeReason ? ' (' + existing.removeReason + ')' : '') : 'never added'}`);

  // DISCOVERY PROFILE via Data API
  console.log(`\n  ── Discovery pipeline (live Data API fetch) ──`);
  let stats = null, scoreResult = null;
  try {
    const events = await fetchAllActivity(c.addr, { pageLimit: 500, maxPages: 20 });
    if (!events || events.length === 0) {
      console.log(`    no activity returned — check address / API`);
    } else {
      console.log(`    events fetched:    ${events.length}`);
      stats = analyzeTradeHistory(events);
      attachMMClassification(stats);
      attachAlphaEvaluation(stats);
      attachAttribution(stats, attrMap, c.addr);
      scoreResult = computeWalletScore(stats);

      console.log(`    total trades:      ${stats.totalTrades}`);
      console.log(`    unique markets:    ${stats.uniqueMarkets}`);
      console.log(`    win rate:          ${((stats.winRate ?? 0) * 100).toFixed(1)}%  (${stats.wins || 0}W/${stats.losses || 0}L across ${stats.resolvedMarkets || 0} resolved)`);
      console.log(`    decidedROI:        ${stats.decidedROI != null ? stats.decidedROI.toFixed(3) : 'null'}  (decidedWins ${stats.decidedWins ?? '—'} / decidedLosses ${stats.decidedLosses ?? '—'})`);
      console.log(`    decidedCapital:    ${stats.decidedCapital != null ? '$' + stats.decidedCapital.toFixed(0) : 'null'}`);
      console.log(`    singleSideCapital: ${stats.singleSideCapital != null ? '$' + stats.singleSideCapital.toFixed(0) : '—'}`);
      console.log(`    singleSideROI:     ${stats.singleSideROI != null ? stats.singleSideROI.toFixed(3) : '—'}`);
      console.log(`    sellRatio:         ${stats.sellRatio != null ? stats.sellRatio.toFixed(3) : '—'}  (MM tell if < 0.05)`);
      console.log(`    dualSideRate:      ${stats.dualSideRate != null ? stats.dualSideRate.toFixed(3) : '—'}  (MM tell if > 0.40)`);
      console.log(`    mergeRate:         ${stats.mergeRate != null ? stats.mergeRate.toFixed(3) : '—'}`);
      console.log(`    rebate income:     $${(stats.rebateUsdcTotal || 0).toFixed(0)}  reward income: $${(stats.rewardUsdcTotal || 0).toFixed(0)}`);
      console.log(`    mmScore:           ${stats.mmScore}/6  ${stats.isLikelyMM ? '← MM DETECTED' : ''}`);
      console.log(`    alphaVerdict:      ${stats.alphaVerdict || '—'}`);
      console.log(`    score:             ${scoreResult?.score ?? 'null'}  reason=${scoreResult?.reason || 'null'}`);
    }
  } catch (err) {
    console.log(`    ERROR — ${err.message}`);
  }

  // RECOMMENDATION
  console.log(`\n  ── Assessment ──`);
  let verdict = '';
  let reason = '';
  if (!stats) {
    verdict = 'UNKNOWN';
    reason = 'Data API call failed — retry needed.';
  } else if (stats.isLikelyMM) {
    verdict = 'REJECT';
    reason = 'MM classifier triggered — not copy-tradeable despite signal history. Signal history is likely a quirk of our filter (saw big buys, missed the exit/merge pattern).';
  } else if (stats.dualSideRate > 0.30 || stats.sellRatio < 0.10) {
    verdict = 'REJECT';
    reason = 'Heavy dual-side / low-sell pattern — borderline MM even if not flagged. Their entries don\'t translate to directional exposure.';
  } else if ((stats.uniqueMarkets || 0) < 10 || (stats.totalTrades || 0) < 20) {
    verdict = 'WAIT';
    reason = 'Insufficient trade history for stable scoring (too few markets/trades). Signals may be high-noise. Check back in a month.';
  } else if (stats.alphaVerdict === 'fails') {
    verdict = 'BORDERLINE';
    reason = 'Alpha test explicitly fails despite positive signal history. Likely a narrow specialist whose alpha is category-concentrated — the single-side directional test doesn\'t see it. Worth admitting WITH an attribution-based synthetic score.';
  } else if (scoreResult?.reason === 'no_decided_metrics') {
    verdict = 'BORDERLINE';
    reason = 'Wallet doesn\'t generate decidedROI (likely trades dual-side or tiny capital per side). Signal history is strong — admit only if we trust attribution more than trade-alpha.';
  } else if ((scoreResult?.score ?? 0) >= 10) {
    verdict = 'ADMIT';
    reason = 'Passes gates — backfill script would admit already on re-run.';
  } else {
    verdict = 'BORDERLINE';
    reason = `Score ${scoreResult?.score ?? '—'} below admit threshold but signal history is strong.`;
  }

  console.log(`    verdict: ${verdict}`);
  console.log(`    reason:  ${reason}`);

  // Synthetic score estimate (attribution-bypass)
  const synthetic = Math.min(35, 15 + c.signals * 0.4 + Math.max(0, c.retPct) * 0.3);
  console.log(`    synthetic score (if we bypass decided-metrics):  ${synthetic.toFixed(1)}`);

  summary.push({
    addr: c.addr.slice(0, 12),
    signals: c.signals,
    wr: c.wr,
    ret: c.retPct,
    markets: stats?.uniqueMarkets ?? null,
    dual: stats?.dualSideRate ?? null,
    mm: stats?.mmScore ?? null,
    alpha: stats?.alphaVerdict ?? null,
    score: scoreResult?.score ?? null,
    scoreReason: scoreResult?.reason ?? null,
    verdict,
    synthetic: +synthetic.toFixed(1),
  });
}

// ── Final table ──────────────────────────────────────────────────────────
console.log('\n\n═══════════════════════════════════════════════════════════════════');
console.log('  SUMMARY');
console.log('═══════════════════════════════════════════════════════════════════\n');
console.log(`  ${'Wallet'.padEnd(14)}  ${'Sigs'.padStart(4)}  ${'WR'.padStart(4)}  ${'Ret'.padStart(7)}  ${'Mkts'.padStart(4)}  ${'Dual'.padStart(5)}  ${'MM'.padStart(3)}  ${'Alpha'.padEnd(10)}  ${'Score'.padStart(5)}  ${'Synth'.padStart(5)}  Verdict`);
console.log('  ' + '─'.repeat(92));
for (const s of summary) {
  const wr = (s.wr * 100).toFixed(0) + '%';
  const ret = (s.ret >= 0 ? '+' : '') + s.ret.toFixed(0) + '%';
  const dual = s.dual != null ? s.dual.toFixed(2) : '—';
  const mm = s.mm != null ? String(s.mm) : '—';
  const alpha = (s.alpha || '—').slice(0, 10);
  const sc = s.score != null ? s.score.toFixed(0) : '—';
  console.log(`  ${s.addr.padEnd(14)}  ${String(s.signals).padStart(4)}  ${wr.padStart(4)}  ${ret.padStart(7)}  ${String(s.markets ?? '—').padStart(4)}  ${dual.padStart(5)}  ${mm.padStart(3)}  ${alpha.padEnd(10)}  ${sc.padStart(5)}  ${String(s.synthetic).padStart(5)}  ${s.verdict}`);
}
console.log();
