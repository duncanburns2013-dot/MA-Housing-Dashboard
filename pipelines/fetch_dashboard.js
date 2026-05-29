// fetch_dashboard.js — pulls MLSPIN closed + active for MA and produces
// data/processed/dashboard.json consumed by the Squarespace embed.
//
// Strategy:
//   • Pull closed sales for the last ~24 months (covers current + prior year
//     so we always have a YoY comparison).
//   • Pull active listings statewide (for months-of-supply).
//   • For each {region × property type × year}, compute:
//       avg ClosePrice, avg sold $/sqft, avg DaysOnMarket, count, supply.
//   • For Greater Newburyport, also compute current-year YTD per-town stats.
//   • Compute affordability inputs (avg price by region → income required).
//
// We DO NOT try to pull 2014-2024 from MLSPIN — that history isn't reliably
// retained on the feed. Historical baselines live in the embed itself; this
// script only refreshes the live years and current-snapshot panels.
//
// Usage:
//   1. Save your BRIDGE_TOKEN to .env (copy from .env.example).
//   2. node pipelines/fetch_dashboard.js
//
// Output:
//   data/processed/dashboard.json   — public, consumed by embed
//   data/raw/bridge_*.json          — raw pulls, gitignored

const fs = require('fs');
const path = require('path');
const https = require('https');
const { buildMarketShare } = require('./market_share');

const ROOT = path.join(__dirname, '..');
const RAW = path.join(ROOT, 'data', 'raw');
const OUT = path.join(ROOT, 'data', 'processed');
fs.mkdirSync(RAW, { recursive: true });
fs.mkdirSync(OUT, { recursive: true });

// ---------- region definitions ----------
// Boston in MLSPIN is filed under neighborhood names as City, not just "Boston".
const BOSTON_NEIGHBORHOODS = [
  'Boston', 'Allston', 'Back Bay', 'Bay Village', 'Beacon Hill', 'Brighton',
  'Charlestown', 'Chinatown', 'Dorchester', 'East Boston', 'Fenway',
  'Hyde Park', 'Jamaica Plain', 'Mattapan', 'Mission Hill', 'North End',
  'Roslindale', 'Roxbury', 'South Boston', 'South End', 'West End',
  'West Roxbury', 'Leather District'
];
const BOSTON_SET = new Set(BOSTON_NEIGHBORHOODS.map(s => s.toUpperCase()));

// All 34 Essex County towns. We list them rather than relying on
// CountyOrParish, which is inconsistently populated on the MLSPIN feed.
const ESSEX_TOWNS = [
  'Amesbury', 'Andover', 'Beverly', 'Boxford', 'Danvers', 'Essex',
  'Georgetown', 'Gloucester', 'Groveland', 'Hamilton', 'Haverhill',
  'Ipswich', 'Lawrence', 'Lynn', 'Lynnfield', 'Manchester', 'Manchester-by-the-Sea',
  'Marblehead', 'Merrimac', 'Methuen', 'Middleton', 'Nahant', 'Newbury',
  'Newburyport', 'North Andover', 'Peabody', 'Rockport', 'Rowley', 'Salem',
  'Salisbury', 'Saugus', 'Swampscott', 'Topsfield', 'Wenham', 'West Newbury'
];
const ESSEX_SET = new Set(ESSEX_TOWNS.map(s => s.toUpperCase()));

const GN_TOWNS = ['Newburyport', 'West Newbury', 'Newbury', 'Rowley', 'Salisbury', 'Amesbury'];

// "& beyond" — the 4 surrounding Essex County towns Bentley's is expanding into.
// Same per-town breakdown as GN_TOWNS but emitted under a separate `beyond` key.
const BEYOND_TOWNS = ['Ipswich', 'Georgetown', 'Groveland', 'Merrimac'];

// ---------- property type buckets ----------
// MLSPIN PropertySubType values vary slightly; match on common patterns.
function bucketPT(rec) {
  const pt = (rec.PropertyType || '').trim();
  const sub = (rec.PropertySubType || '').trim();
  if (pt === 'Residential Income') return 'mf';
  if (pt !== 'Residential') return null;
  if (/condom/i.test(sub)) return 'condo';
  if (/single family/i.test(sub)) return 'sf';
  // Default Residential → assume SF if subtype is missing
  if (!sub) return 'sf';
  return null;
}

// ---------- env loader ----------
function loadEnv() {
  const file = path.join(ROOT, '.env');
  if (!fs.existsSync(file)) {
    console.error('Missing .env (copy from .env.example).');
    process.exit(1);
  }
  const env = {};
  for (const line of fs.readFileSync(file, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/);
    if (m) env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
  return env;
}

// ---------- HTTP with retry ----------
function get(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'MA-Housing-Dashboard/1.0' } }, (res) => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        if (res.statusCode >= 400) return reject(new Error(`HTTP ${res.statusCode}: ${body.slice(0, 300)}`));
        try { resolve(JSON.parse(body)); } catch (e) { reject(e); }
      });
    }).on('error', reject);
  });
}
async function getRetry(url, n = 4) {
  let last;
  for (let i = 0; i < n; i++) {
    try { return await get(url); }
    catch (e) {
      last = e;
      const wait = 1000 * Math.pow(2, i);
      console.warn(`  retry ${i + 1}/${n} after ${wait}ms: ${e.message}`);
      await new Promise(r => setTimeout(r, wait));
    }
  }
  throw last;
}

// ---------- MLSPIN keyset pager ----------
async function pageMLS({ token, dataset, filter, select, label, cap = 200000 }) {
  const all = [];
  let lastKey = null;
  let pages = 0;
  if (!select.split(',').includes('ListingKey')) select = 'ListingKey,' + select;
  while (all.length < cap) {
    const f = lastKey ? `${filter} and ListingKey gt '${lastKey}'` : filter;
    const u = new URL(`https://api.bridgedataoutput.com/api/v2/OData/${dataset}/Property`);
    u.searchParams.set('access_token', token);
    u.searchParams.set('$filter', f);
    u.searchParams.set('$select', select);
    u.searchParams.set('$orderby', 'ListingKey asc');
    u.searchParams.set('$top', '200');
    const json = await getRetry(u.toString());
    const batch = json.value || json.bundle || [];
    if (!batch.length) break;
    all.push(...batch);
    pages++;
    process.stdout.write(`  ${label}: page ${pages}, total ${all.length}\r`);
    if (batch.length < 200) break;
    lastKey = batch[batch.length - 1].ListingKey;
  }
  process.stdout.write('\n');
  return all;
}

// ---------- region matchers ----------
function inRegion(rec, region) {
  const c = (rec.City || '').toUpperCase();
  if (!c) return false;
  switch (region) {
    case 'ma':     return true;                       // already filtered to MA
    case 'boston': return BOSTON_SET.has(c);
    case 'essex':  return ESSEX_SET.has(c);
    case 'nbpt':   return c === 'NEWBURYPORT';
    default:       return false;
  }
}

// ---------- aggregator ----------
function avg(arr) {
  if (!arr.length) return null;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}
function nums(recs, field) {
  return recs.map(r => r[field]).filter(v => v != null && !Number.isNaN(Number(v)) && Number(v) > 0).map(Number);
}

// Compute price/sqft/dom/count for an arbitrary record set.
function aggregateRecords(recs) {
  if (!recs.length) return null;
  const prices = nums(recs, 'ClosePrice');
  const sqftPrices = nums(recs, 'MLSPIN_SOLD_PRICE_PER_SQFT');
  const doms = nums(recs, 'MLSPIN_MARKET_TIME');
  return {
    price: prices.length ? Math.round(avg(prices)) : null,
    sqft:  sqftPrices.length ? Math.round(avg(sqftPrices)) : null,
    dom:   doms.length ? Math.round(avg(doms)) : null,
    units: recs.length
  };
}

// ---------- main ----------
(async () => {
  const env = loadEnv();
  const token = env.BRIDGE_TOKEN;
  const dataset = env.BRIDGE_DATASET || 'mlspin';
  if (!token || token === 'your_server_access_token_here') {
    console.error('BRIDGE_TOKEN missing in .env'); process.exit(1);
  }

  const now = new Date();
  const currentYear = now.getUTCFullYear();
  const priorYear = currentYear - 1;
  const since = `${priorYear}-01-01`;

  const propTypeFilter = `(PropertyType eq 'Residential' or PropertyType eq 'Residential Income')`;
  const closedSelect = [
    'City', 'PropertyType', 'PropertySubType',
    'ClosePrice', 'CloseDate',
    'MLSPIN_MARKET_TIME', 'MLSPIN_SOLD_PRICE_PER_SQFT',
    'LivingArea'
  ].join(',');
  const activeSelect = [
    'City', 'PropertyType', 'PropertySubType',
    'ListPrice', 'LivingArea'
  ].join(',');

  // --no-pull: skip the network pull and re-aggregate the cached raw data.
  // Useful while iterating on the aggregation logic.
  const skipPull = process.argv.includes('--no-pull');
  let closed, active;
  if (skipPull && fs.existsSync(path.join(RAW, 'bridge_closed.json')) && fs.existsSync(path.join(RAW, 'bridge_active.json'))) {
    console.log('--no-pull: loading cached raw data');
    closed = JSON.parse(fs.readFileSync(path.join(RAW, 'bridge_closed.json')));
    active = JSON.parse(fs.readFileSync(path.join(RAW, 'bridge_active.json')));
    console.log(`  loaded ${closed.length} closed + ${active.length} active`);
  } else {
    console.log(`Pulling MLSPIN closed sales for all MA since ${since}...`);
    closed = await pageMLS({
      token, dataset,
      filter: `StandardStatus eq 'Closed' and StateOrProvince eq 'MA' and ${propTypeFilter} and CloseDate ge ${since}`,
      select: closedSelect, label: 'closed'
    });
    fs.writeFileSync(path.join(RAW, 'bridge_closed.json'), JSON.stringify(closed));

    console.log(`\nPulling MLSPIN active listings for all MA...`);
    active = await pageMLS({
      token, dataset,
      filter: `StandardStatus eq 'Active' and StateOrProvince eq 'MA' and ${propTypeFilter}`,
      select: activeSelect, label: 'active'
    });
    fs.writeFileSync(path.join(RAW, 'bridge_active.json'), JSON.stringify(active));
  }

  // ---- attach year + PT bucket to every closed record once ----
  const enriched = [];
  for (const r of closed) {
    const pt = bucketPT(r);
    if (!pt) continue;
    const cd = r.CloseDate;
    if (!cd) continue;
    const yr = parseInt(cd.slice(0, 4), 10);
    if (!yr) continue;
    r.__pt = pt;
    r.__yr = yr;
    enriched.push(r);
  }
  // Active records — just bucket them
  const activeEnriched = [];
  for (const r of active) {
    const pt = bucketPT(r);
    if (!pt) continue;
    r.__pt = pt;
    activeEnriched.push(r);
  }

  // ---- region × pt × year aggregates ----
  // Prior year uses calendar-year (Jan 1 - Dec 31). Current year uses
  // trailing-12-month — comparable to a full-year snapshot for the chart,
  // and matches the convention the original hand-curated embed used.
  const regions = ['ma', 'boston', 'essex', 'nbpt'];
  const ptList = ['sf', 'condo', 'mf'];
  const cutoffT12 = new Date(now.getTime() - 365 * 86400e3).toISOString().slice(0, 10);

  const regional = {};
  for (const region of regions) {
    regional[region] = {};
    for (const pt of ptList) {
      regional[region][pt] = {};
      // Prior year: calendar year
      const priorRecs = enriched.filter(r => r.__pt === pt && r.__yr === priorYear && inRegion(r, region));
      regional[region][pt][priorYear] = aggregateRecords(priorRecs);
      // Current year: trailing 12 months from today (more useful than YTD partial year)
      const t12Recs = enriched.filter(r => r.__pt === pt && r.CloseDate >= cutoffT12 && inRegion(r, region));
      regional[region][pt][currentYear] = aggregateRecords(t12Recs);
      // months of supply (current snapshot): active count / (trailing-12mo monthly avg)
      const activeRecs = activeEnriched.filter(r => r.__pt === pt && inRegion(r, region));
      const monthlyRate = t12Recs.length / 12;
      regional[region][pt].supply = monthlyRate > 0
        ? +(activeRecs.length / monthlyRate).toFixed(2)
        : null;
      regional[region][pt].active = activeRecs.length;
      regional[region][pt].trailing12 = t12Recs.length;
    }
  }

  // ---- Greater Newburyport per-town breakdown ----
  // Same convention: prior year = calendar, current = trailing-12.
  const gnTowns = {};
  for (const t of GN_TOWNS) {
    const tu = t.toUpperCase();
    gnTowns[t] = {};
    const prior = enriched.filter(r =>
      r.__pt === 'sf' && r.__yr === priorYear && (r.City || '').toUpperCase() === tu);
    const t12 = enriched.filter(r =>
      r.__pt === 'sf' && r.CloseDate >= cutoffT12 && (r.City || '').toUpperCase() === tu);
    gnTowns[t][priorYear] = aggregateRecords(prior);
    gnTowns[t][currentYear] = aggregateRecords(t12);
  }

  // ---- GN regional summary (single-family weighted across 6 towns, trailing-12) ----
  const gnAll = enriched.filter(r =>
    r.__pt === 'sf' && r.CloseDate >= cutoffT12 &&
    GN_TOWNS.map(s => s.toUpperCase()).includes((r.City || '').toUpperCase()));
  const gnSummary = aggregateRecords(gnAll);
  const gnActive = activeEnriched.filter(r =>
    r.__pt === 'sf' &&
    GN_TOWNS.map(s => s.toUpperCase()).includes((r.City || '').toUpperCase()));
  gnSummary.activeListings = gnActive.length;

  // ---- "& beyond" per-town breakdown (Ipswich, Georgetown, Groveland, Merrimac) ----
  // Same shape as gnTowns. Lets the page show the expansion-frontier towns.
  const beyondTowns = {};
  for (const t of BEYOND_TOWNS) {
    const tu = t.toUpperCase();
    beyondTowns[t] = {};
    const prior = enriched.filter(r =>
      r.__pt === 'sf' && r.__yr === priorYear && (r.City || '').toUpperCase() === tu);
    const t12 = enriched.filter(r =>
      r.__pt === 'sf' && r.CloseDate >= cutoffT12 && (r.City || '').toUpperCase() === tu);
    beyondTowns[t][priorYear] = aggregateRecords(prior);
    beyondTowns[t][currentYear] = aggregateRecords(t12);
  }
  const beyondAll = enriched.filter(r =>
    r.__pt === 'sf' && r.CloseDate >= cutoffT12 &&
    BEYOND_TOWNS.map(s => s.toUpperCase()).includes((r.City || '').toUpperCase()));
  const beyondSummary = aggregateRecords(beyondAll) || { price: null, sqft: null, dom: null, units: 0 };
  const beyondActive = activeEnriched.filter(r =>
    r.__pt === 'sf' &&
    BEYOND_TOWNS.map(s => s.toUpperCase()).includes((r.City || '').toUpperCase()));
  beyondSummary.activeListings = beyondActive.length;

  // ---- affordability (uses MA median household income — ACS) ----
  // Income required at 28% DTI for {avg price × 80% LTV × 6.5% × 30yr} +
  // taxes/insurance approx. We embed the formula in the page; here we just
  // surface current avg prices.
  // MA median household income — published annually by ACS. Hard-coded for now;
  // could be wired to fetch_fred.js or census later.
  const MA_MEDIAN_INCOME = 104800;
  function incomeNeeded(price) {
    if (!price) return null;
    const down = price * 0.20;
    const loan = price - down;
    const monthlyRate = 0.065 / 12;
    const n = 360;
    const pi = loan * monthlyRate * Math.pow(1 + monthlyRate, n) / (Math.pow(1 + monthlyRate, n) - 1);
    const taxes = price * 0.012 / 12;     // ~1.2% MA effective property tax
    const insurance = price * 0.004 / 12; // ~0.4% homeowner's
    const monthly = pi + taxes + insurance;
    const yearly = monthly * 12;
    const income = yearly / 0.28;
    return { down: Math.round(down), monthly: Math.round(pi), monthlyAll: Math.round(monthly), income: Math.round(income) };
  }
  const affordability = {
    medianIncome: MA_MEDIAN_INCOME,
    markets: {
      ma: { price: regional.ma.sf[currentYear].price, ...incomeNeeded(regional.ma.sf[currentYear].price) },
      boston: { price: regional.boston.sf[currentYear].price, ...incomeNeeded(regional.boston.sf[currentYear].price) },
      essex: { price: regional.essex.sf[currentYear].price, ...incomeNeeded(regional.essex.sf[currentYear].price) },
      nbpt: { price: regional.nbpt.sf[currentYear].price, ...incomeNeeded(regional.nbpt.sf[currentYear].price) }
    }
  };

  // ---- brokerage market share (office-level + firm-level cuts, all 10 towns) ----
  // Self-contained module: makes its own MLS pulls (Office lookup + closed sales
  // per-town filter), so it doesn't depend on the `enriched` / `closed` arrays
  // above. Runs in parallel with the rest of the aggregation here.
  console.log('\nBuilding brokerage market share...');
  let marketShare = null;
  try {
    marketShare = await buildMarketShare(token);
  } catch (e) {
    console.warn(`[fetch_dashboard] market share build failed: ${e.message}`);
    marketShare = { error: e.message, generated: new Date().toISOString() };
  }

  // ---- output ----
  const out = {
    meta: {
      generated: new Date().toISOString(),
      source: 'MLSPIN',
      currentYear,
      priorYear,
      closed_records: enriched.length,
      active_records: activeEnriched.length
    },
    regional,
    greaterNewburyport: {
      towns: gnTowns,
      summary: gnSummary
    },
    beyond: {
      towns: beyondTowns,
      summary: beyondSummary
    },
    affordability,
    marketShare
  };

  fs.writeFileSync(path.join(OUT, 'dashboard.json'), JSON.stringify(out, null, 2));
  console.log(`\nWrote dashboard.json — ${enriched.length} closed + ${activeEnriched.length} active`);

  // ---- summary print ----
  console.log('\n  REGIONAL — Single Family avg sale price');
  const years = [priorYear, currentYear];
  console.log('  Region          ' + years.map(y => String(y).padStart(10)).join('') + '   supply');
  for (const region of regions) {
    const row = years.map(yr => {
      const v = regional[region].sf[yr]?.price;
      return (v ? '$' + Math.round(v/1000) + 'K' : '—').padStart(10);
    }).join('');
    const s = regional[region].sf.supply;
    console.log(`  ${region.padEnd(15)} ${row}    ${s != null ? s.toFixed(2) + 'mo' : '—'}`);
  }
  console.log('\n  GREATER NEWBURYPORT — SF current-year avg');
  for (const t of GN_TOWNS) {
    const v = gnTowns[t][currentYear]?.price;
    const n = gnTowns[t][currentYear]?.units || 0;
    console.log(`  ${t.padEnd(16)} ${v ? '$' + Math.round(v/1000) + 'K' : '—'} (n=${n})`);
  }
  console.log('\n  & BEYOND — SF current-year avg');
  for (const t of BEYOND_TOWNS) {
    const v = beyondTowns[t][currentYear]?.price;
    const n = beyondTowns[t][currentYear]?.units || 0;
    console.log(`  ${t.padEnd(16)} ${v ? '$' + Math.round(v/1000) + 'K' : '—'} (n=${n})`);
  }
  if (marketShare && marketShare.officeLevel) {
    const gn = marketShare.officeLevel.greaterNewburyport;
    if (gn && gn.byVolume && gn.byVolume[0]) {
      console.log(`\n  MARKET SHARE leader (GN, by volume): ${gn.byVolume[0].displayName} — ${gn.byVolume[0].volumePct}%`);
    }
  }
})().catch(e => { console.error('FAILED:', e.message); process.exit(1); });
