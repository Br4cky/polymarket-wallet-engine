// Usage: node scripts/probe-market.mjs <event-slug>
// Example: node scripts/probe-market.mjs bitcoin-up-or-down-april-10-2026-10am-et

const slug = process.argv[2] || 'bitcoin-up-or-down-april-10-2026-10am-et';

const GAMMA = 'https://gamma-api.polymarket.com';
const CLOB = 'https://clob.polymarket.com';

function short(v, n = 200) {
  if (v == null) return String(v);
  const s = typeof v === 'string' ? v : JSON.stringify(v);
  return s.length > n ? s.slice(0, n) + '…' : s;
}

async function hit(label, url) {
  console.log(`\n── ${label} ──`);
  console.log(`URL: ${url}`);
  try {
    const r = await fetch(url);
    console.log(`Status: ${r.status}`);
    if (!r.ok) { console.log(`Body: ${(await r.text()).slice(0, 300)}`); return null; }
    const j = await r.json();
    const count = Array.isArray(j) ? j.length : (j ? 1 : 0);
    console.log(`Array length: ${count}`);
    return j;
  } catch (e) {
    console.log(`ERROR: ${e.message}`);
    return null;
  }
}

function summariseMarket(m, label = 'market') {
  if (!m) return;
  console.log(`\n  [${label}]`);
  console.log(`    question:       ${short(m.question || m.title)}`);
  console.log(`    slug:           ${m.slug}`);
  console.log(`    conditionId:    ${m.conditionId || m.condition_id}`);
  console.log(`    closed:         ${m.closed}`);
  console.log(`    active:         ${m.active}`);
  console.log(`    accepting:      ${m.accepting_orders ?? m.acceptingOrders}`);
  console.log(`    endDate:        ${m.endDate || m.end_date_iso}`);
  console.log(`    winner:         ${m.winner}`);
  console.log(`    outcomes:       ${short(m.outcomes)}`);
  console.log(`    outcomePrices:  ${short(m.outcomePrices)}`);
  console.log(`    clobTokenIds:   ${short(m.clobTokenIds)}`);
  if (m.tokens) console.log(`    tokens:         ${short(m.tokens, 300)}`);
  // Any field that smells like "winning outcome"
  const winKeys = Object.keys(m).filter(k => /win|resolv|settle/i.test(k));
  if (winKeys.length) {
    console.log(`    resolution-ish keys: ${winKeys.join(', ')}`);
    winKeys.forEach(k => console.log(`      ${k}: ${short(m[k])}`));
  }
}

(async () => {
  console.log(`Probing slug: ${slug}\n`);

  // 1. /events?slug=...
  const events = await hit('Gamma /events?slug=', `${GAMMA}/events?slug=${slug}`);
  let event = null;
  if (Array.isArray(events) && events.length > 0) {
    event = events[0];
    console.log(`  event.id:       ${event.id}`);
    console.log(`  event.title:    ${short(event.title)}`);
    console.log(`  event.slug:     ${event.slug}`);
    console.log(`  event.closed:   ${event.closed}`);
    console.log(`  markets count:  ${event.markets?.length ?? 0}`);
    if (Array.isArray(event.markets)) {
      event.markets.forEach((m, i) => summariseMarket(m, `event.markets[${i}]`));
    }
  }

  // Grab a condition id and clob token id from the event for further probes
  let condId = null, clobId = null, marketSlug = null;
  if (event && Array.isArray(event.markets) && event.markets.length) {
    const m0 = event.markets[0];
    condId = m0.conditionId || m0.condition_id;
    marketSlug = m0.slug;
    let ct = m0.clobTokenIds;
    if (typeof ct === 'string') try { ct = JSON.parse(ct); } catch(e) {}
    if (Array.isArray(ct) && ct.length) clobId = ct[0];
  }

  // 2. /markets?slug=...
  if (marketSlug) {
    const byMarketSlug = await hit('Gamma /markets?slug=<marketSlug>', `${GAMMA}/markets?slug=${marketSlug}`);
    if (Array.isArray(byMarketSlug) && byMarketSlug[0]) summariseMarket(byMarketSlug[0], 'by market slug');
  }

  // 3. /markets?clob_token_ids=...
  if (clobId) {
    const byClob = await hit('Gamma /markets?clob_token_ids=', `${GAMMA}/markets?clob_token_ids=${clobId}&limit=1`);
    if (Array.isArray(byClob) && byClob[0]) summariseMarket(byClob[0], 'by clob_token_ids');
  }

  // 4. /markets?conditionIds=... (camelCase, maybe supported)
  if (condId) {
    const byCondCamel = await hit('Gamma /markets?conditionIds=<camel>', `${GAMMA}/markets?conditionIds=${condId}`);
    if (Array.isArray(byCondCamel) && byCondCamel[0]) summariseMarket(byCondCamel[0], 'by conditionIds camelCase');
  }

  // 5. /markets?condition_ids=... (plural snake_case)
  if (condId) {
    const byCondPlural = await hit('Gamma /markets?condition_ids=<plural snake>', `${GAMMA}/markets?condition_ids=${condId}`);
    if (Array.isArray(byCondPlural) && byCondPlural[0]) summariseMarket(byCondPlural[0], 'by condition_ids plural');
  }

  // 6. /markets?condition_id=... (singular snake_case — the one we've been using)
  if (condId) {
    const byCondSingle = await hit('Gamma /markets?condition_id=<singular snake>', `${GAMMA}/markets?condition_id=${condId}`);
    if (Array.isArray(byCondSingle) && byCondSingle[0]) {
      const first = byCondSingle[0];
      const matches = (first.conditionId || first.condition_id || '').toLowerCase() === condId.toLowerCase();
      console.log(`  returned market matches requested condId? ${matches}`);
      summariseMarket(first, 'by condition_id singular');
    }
  }

  // 7. CLOB API by condition id
  if (condId) {
    await hit('CLOB /markets/<conditionId>', `${CLOB}/markets/${condId}`);
  }

  // 8. CLOB API by clob token id (via /market?token_id=)
  if (clobId) {
    await hit('CLOB /markets?token_id=', `${CLOB}/markets?token_id=${clobId}`);
  }

  console.log('\nDone.');
})();
