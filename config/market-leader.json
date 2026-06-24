// fetch_market_leader.js — Greater Newburyport market-leadership snapshot.
// Pulls trailing-12-month closed sales for the 6 GN towns from MLSPIN,
// rolls up brokerage market share, town volume, sale-to-list ratio, and
// town competitiveness. Combines with config/market-leader.json (off-market
// adjustment + non-MLSPIN constants like digital reach) and writes
// data/processed/market-leader.json.
//
// Usage:
//   node pipelines/fetch_market_leader.js
//   node pipelines/fetch_market_leader.js --no-pull   (reuse cached raw)
//
// 2026-06 CHANGE — rate-limit fix:
//   This script used to pull the ENTIRE statewide MLSPIN Office directory
//   (~23k offices, ~115 paged requests) every run just to map office IDs to
//   names. Running right after fetch_dashboard.js (which already pulls tens of
//   thousands of records), that duplicate scan blew past the Bridge request
//   quota and the run died with HTTP 429. We now resolve names only for the
//   offices that actually appear in the GN closed sales (~150), via batched
//   OfficeMlsId filters (~4 requests). getRetry is also 429-aware.

const fs = require('fs');
const path = require('path');
const https = require('https');

const ROOT = path.join(__dirname, '..');
const RAW = path.join(ROOT, 'data', 'raw');
const OUT = path.join(ROOT, 'data', 'processed');
const CFG = path.join(ROOT, 'config', 'market-leader.json');
fs.mkdirSync(RAW, { recursive: true });
fs.mkdirSync(OUT, { recursive: true });

const GN_TOWNS = ['Newburyport', 'West Newbury', 'Newbury', 'Rowley', 'Salisbury', 'Amesbury'];
const GN_UC = new Set(GN_TOWNS.map(s => s.toUpperCase()));

// How many OfficeMlsId values to OR together per Office request.
const OFFICE_ID_CHUNK = 40;

// ---------- env loader ----------
function loadEnv() {
  const file = path.join(ROOT, '.env');
  if (!fs.existsSync(file)) { console.error('Missing .env'); process.exit(1); }
  const env = {};
  for (const line of fs.readFileSync(file, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/);
    if (m) env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
  return env;
}

// ---------- HTTP ----------
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

// Parse the reset timestamp out of a Bridge 429 body, e.g.
//   "...Your limit will reset on Mon Jun 22 2026 12:52:36 GMT+0000 (...)"
function parseResetMs(msg) {
  const m = /reset on (.+?)(?:"|\}|$)/i.exec(msg || '');
  if (!m) return null;
  const t = Date.parse(m[1].trim());
  return Number.isNaN(t) ? null : t;
}

// 429-aware retry. On a rate-limit, wait until the quota resets (clamped).
async function getRetry(url, n = 5) {
  let last;
  for (let i = 0; i < n; i++) {
    try { return await get(url); }
    catch (e) {
      last = e;
      if (i === n - 1) break;
      let wait = 1000 * Math.pow(2, i);
      if (/HTTP 429/.test(e.message)) {
        const resetMs = parseResetMs(e.message);
        const until = resetMs ? (resetMs - Date.now() + 2000) : 60000;
        wait = Math.min(Math.max(until, 5000), 120000); // clamp 5s..120s
        console.warn(`  rate limited (429); waiting ${Math.round(wait / 1000)}s before retry ${i + 1}/${n}`);
      }
      await new Promise(r => setTimeout(r, wait));
    }
  }
  throw last;
}

// ---------- keyset pager (Property by ListingKey) ----------
async function pageMLS({ token, dataset, filter, select, label, cap = 50000 }) {
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

// ---------- targeted Office name lookup (by OfficeMlsId) ----------
// MLSPIN Property only exposes ListOfficeMlsId (no name). Resolve names for
// just the offices that appear in our closed sales, in OR'd batches, instead
// of scanning the whole statewide Office directory.
async function fetchOfficesByIds({ token, dataset, ids, label = 'offices' }) {
  const out = [];
  const unique = [...new Set((ids || []).filter(Boolean))];
  let done = 0;
  for (let i = 0; i < unique.length; i += OFFICE_ID_CHUNK) {
    const slice = unique.slice(i, i + OFFICE_ID_CHUNK);
    const clause = slice.map(id => `OfficeMlsId eq '${String(id).replace(/'/g, "''")}'`).join(' or ');
    const u = new URL(`https://api.bridgedataoutput.com/api/v2/OData/${dataset}/Office`);
    u.searchParams.set('access_token', token);
    u.searchParams.set('$select', 'OfficeKey,OfficeMlsId,OfficeName,MainOfficeKey');
    u.searchParams.set('$top', '200');
    u.searchParams.set('$filter', `(${clause})`);
    let json;
    try { json = await getRetry(u.toString()); }
    catch (e) { console.warn(`  ${label}: chunk ${Math.floor(i / OFFICE_ID_CHUNK)} failed: ${e.message}`); continue; }
    const batch = json.value || [];
    out.push(...batch);
    done += slice.length;
    process.stdout.write(`  ${label}: ${out.length} names for ${done}/${unique.length} ids\r`);
  }
  process.stdout.write('\n');
  return out;
}

// ---------- brokerage normalization ----------
function normalize(s) {
  if (!s) return '';
  return s.toUpperCase()
    .replace(/[‘’“”]/g, '')  // smart quotes
    .replace(/[,'.&\/]/g, ' ')
    .replace(/\s+/g, ' ').trim();
}

function classifyBrokerage(officeName, cfg) {
  const norm = normalize(officeName);
  if (!norm) return null;
  // your firm first
  for (const kw of cfg.yourFirm.matchKeywords) {
    if (norm.includes(kw.toUpperCase())) return cfg.yourFirm.displayName;
  }
  // then canonical brands
  for (const brand of cfg.brandCanonicals) {
    for (const kw of brand.matchKeywords) {
      if (norm.includes(kw.toUpperCase())) return brand.displayName;
    }
  }
  // unmatched: bucket under its raw display name (title-case the normalized form)
  return officeName.trim();
}

// ---------- main ----------
(async () => {
  const env = loadEnv();
  const token = env.BRIDGE_TOKEN;
  const dataset = env.BRIDGE_DATASET || 'mlspin';
  if (!token || token === 'your_server_access_token_here') {
    console.error('BRIDGE_TOKEN missing in .env'); process.exit(1);
  }
  const cfg = JSON.parse(fs.readFileSync(CFG, 'utf8'));

  const now = new Date();
  const since = new Date(now.getTime() - 365 * 86400e3).toISOString().slice(0, 10);
  const labelYear = now.getUTCFullYear();

  const propTypeFilter = `(PropertyType eq 'Residential' or PropertyType eq 'Residential Income')`;
  const cityClause = GN_TOWNS.map(t => `City eq '${t}'`).join(' or ');
  const fullFilter = `StandardStatus eq 'Closed' and StateOrProvince eq 'MA' and ${propTypeFilter} and CloseDate ge ${since} and (${cityClause})`;

  const select = [
    'City', 'PropertyType', 'PropertySubType',
    'ClosePrice', 'CloseDate', 'OriginalListPrice', 'ListPrice',
    'MLSPIN_MARKET_TIME', 'LivingArea',
    'ListOfficeMlsId'
  ].join(',');

  const skipPull = process.argv.includes('--no-pull');
  const closedCache = path.join(RAW, 'mlspin_gn_closed.json');
  const officeCache = path.join(RAW, 'mlspin_offices.json');
  let closed, offices;
  if (skipPull && fs.existsSync(closedCache) && fs.existsSync(officeCache)) {
    console.log('--no-pull: loading cached raw data');
    closed = JSON.parse(fs.readFileSync(closedCache));
    offices = JSON.parse(fs.readFileSync(officeCache));
    console.log(`  loaded ${closed.length} closed + ${offices.length} offices`);
  } else {
    console.log(`Pulling GN trailing-12mo closed sales since ${since}...`);
    closed = await pageMLS({
      token, dataset, filter: fullFilter, select, label: 'gn closed', cap: 20000
    });
    fs.writeFileSync(closedCache, JSON.stringify(closed));

    console.log(`\nResolving office names for offices that appear in GN sales...`);
    const officeIds = [...new Set(closed.map(r => r.ListOfficeMlsId).filter(Boolean))];
    offices = await fetchOfficesByIds({ token, dataset, ids: officeIds, label: 'offices' });
    fs.writeFileSync(officeCache, JSON.stringify(offices));
  }

  // Build OfficeMlsId → OfficeName map
  const officeIdToName = new Map();
  for (const o of offices) {
    if (o.OfficeMlsId && o.OfficeName) officeIdToName.set(o.OfficeMlsId, o.OfficeName);
  }
  // Attach resolved name to each closed record (use raw ID if not found)
  for (const r of closed) {
    r.__officeName = officeIdToName.get(r.ListOfficeMlsId) || `[${r.ListOfficeMlsId || 'UNKNOWN'}]`;
  }

  // ---- brokerage market share ----
  const byBrokerage = new Map();
  for (const r of closed) {
    const brand = classifyBrokerage(r.__officeName, cfg);
    if (!brand) continue;
    if (!byBrokerage.has(brand)) byBrokerage.set(brand, { volume: 0, units: 0 });
    const b = byBrokerage.get(brand);
    b.volume += Number(r.ClosePrice) || 0;
    b.units += 1;
  }
  // Apply off-market boost to your firm. If yourTownVolumeOverride is set,
  // use (sum-of-overrides - MLSPIN-volume) as the boost so the brokerage
  // chart matches the town chart. Else use the flat offMarketVolume value.
  const yourFirm = cfg.yourFirm.displayName;
  let firmBoost = cfg.offMarketVolumeLast12Months || 0;
  if (cfg.yourTownVolumeOverride && typeof cfg.yourTownVolumeOverride === 'object') {
    const overrideSum = Object.values(cfg.yourTownVolumeOverride).reduce((s, v) => s + (Number(v) || 0), 0);
    const mlspinFirmVol = byBrokerage.get(yourFirm)?.volume || 0;
    firmBoost = Math.max(0, overrideSum - mlspinFirmVol);
  }
  if (byBrokerage.has(yourFirm) && firmBoost > 0) {
    byBrokerage.get(yourFirm).volume += firmBoost;
    byBrokerage.get(yourFirm).offMarketAdded = firmBoost;
  }
  const totalVolume = [...byBrokerage.values()].reduce((s, b) => s + b.volume, 0);
  const brokerages = [...byBrokerage.entries()]
    .map(([name, b]) => ({
      name,
      volume: Math.round(b.volume),
      units: b.units,
      share: +((b.volume / totalVolume) * 100).toFixed(2),
      offMarketAdded: b.offMarketAdded || 0
    }))
    .sort((a, b) => b.volume - a.volume);

  const yourBrokerage = brokerages.find(b => b.name === yourFirm);
  const secondPlace = brokerages.find(b => b.name !== yourFirm);
  const leadOverSecond = yourBrokerage && secondPlace
    ? +(((yourBrokerage.share / secondPlace.share) - 1) * 100).toFixed(0)
    : null;

  // ---- town volume: two views ----
  // (a) Bentley's slice per town — what the existing page shows.
  // (b) Total market per town — for context / competitiveness scoring.
  const yourFirmName = cfg.yourFirm.displayName;
  const yourByTown = new Map();
  const marketByTown = new Map();
  for (const r of closed) {
    const t = (r.City || '').trim();
    const canonical = GN_TOWNS.find(gn => gn.toUpperCase() === t.toUpperCase());
    if (!canonical) continue;
    const price = Number(r.ClosePrice) || 0;
    const brand = classifyBrokerage(r.__officeName, cfg);
    // Market totals
    if (!marketByTown.has(canonical)) marketByTown.set(canonical, { volume: 0, units: 0 });
    const m = marketByTown.get(canonical);
    m.volume += price;
    m.units += 1;
    // Your-firm slice
    if (brand === yourFirmName) {
      if (!yourByTown.has(canonical)) yourByTown.set(canonical, { volume: 0, units: 0 });
      const y = yourByTown.get(canonical);
      y.volume += price;
      y.units += 1;
    }
  }
  const yourMlspinTotal = [...yourByTown.values()].reduce((s, t) => s + t.volume, 0);
  const offMarket = cfg.offMarketVolumeLast12Months || 0;
  // Two modes: (a) per-town override wins; else (b) proportional redistribution.
  if (cfg.yourTownVolumeOverride && typeof cfg.yourTownVolumeOverride === 'object') {
    for (const town of GN_TOWNS) {
      const overrideVal = cfg.yourTownVolumeOverride[town];
      if (overrideVal != null) {
        if (!yourByTown.has(town)) yourByTown.set(town, { volume: 0, units: 0 });
        yourByTown.get(town).volume = overrideVal;
      }
    }
  } else if (offMarket > 0 && yourMlspinTotal > 0) {
    for (const t of yourByTown.values()) {
      t.offMarketAdd = offMarket * (t.volume / yourMlspinTotal);
      t.volume += t.offMarketAdd;
    }
  }
  const yourTotalVolume = [...yourByTown.values()].reduce((s, t) => s + t.volume, 0);
  const yourTowns = GN_TOWNS
    .map(name => {
      const t = yourByTown.get(name) || { volume: 0, units: 0 };
      return {
        name,
        volume: Math.round(t.volume),
        units: t.units,
        share: yourTotalVolume > 0 ? +((t.volume / yourTotalVolume) * 100).toFixed(1) : 0
      };
    })
    .sort((a, b) => b.volume - a.volume);
  const marketTotalVolume = [...marketByTown.values()].reduce((s, t) => s + t.volume, 0);
  const marketTowns = GN_TOWNS
    .map(name => {
      const t = marketByTown.get(name) || { volume: 0, units: 0 };
      return {
        name,
        volume: Math.round(t.volume),
        units: t.units,
        share: marketTotalVolume > 0 ? +((t.volume / marketTotalVolume) * 100).toFixed(1) : 0
      };
    })
    .sort((a, b) => b.volume - a.volume);

  // ---- sale-to-list ratio ----
  const ratios = closed
    .filter(r => r.ClosePrice && r.OriginalListPrice && r.OriginalListPrice > 0)
    .map(r => r.ClosePrice / r.OriginalListPrice);
  const localSL = ratios.length
    ? +((ratios.reduce((a, b) => a + b, 0) / ratios.length) * 100).toFixed(0)
    : null;

  // ---- competitiveness score per town ----
  // Composite: rank-based percentile of three signals:
  //   • lower DOM → hotter
  //   • higher S/L ratio → hotter
  //   • higher units (proxy for buyer interest given total inventory) → hotter
  // Normalize each to 0..10, average, round to 1 decimal.
  function townMetrics(townName) {
    const recs = closed.filter(r => (r.City || '').toUpperCase() === townName.toUpperCase());
    const doms = recs.map(r => r.MLSPIN_MARKET_TIME).filter(v => v != null);
    const sls = recs.filter(r => r.ClosePrice && r.OriginalListPrice && r.OriginalListPrice > 0)
      .map(r => r.ClosePrice / r.OriginalListPrice);
    return {
      avgDom: doms.length ? doms.reduce((a, b) => a + b, 0) / doms.length : null,
      avgSL: sls.length ? sls.reduce((a, b) => a + b, 0) / sls.length : null,
      units: recs.length
    };
  }
  const metrics = Object.fromEntries(GN_TOWNS.map(t => [t, townMetrics(t)]));
  function normalize0to10(values, invert = false) {
    const valid = values.filter(v => v != null);
    if (!valid.length) return values.map(() => 5);
    const min = Math.min(...valid), max = Math.max(...valid);
    const range = max - min || 1;
    return values.map(v => v == null ? 5
      : invert ? 10 * (1 - (v - min) / range) : 10 * ((v - min) / range));
  }
  const domVals = GN_TOWNS.map(t => metrics[t].avgDom);
  const slVals  = GN_TOWNS.map(t => metrics[t].avgSL);
  const unitVals = GN_TOWNS.map(t => metrics[t].units);
  const domScores  = normalize0to10(domVals, true);    // lower DOM → higher score
  const slScores   = normalize0to10(slVals, false);    // higher S/L → higher score
  const unitScores = normalize0to10(unitVals, false);  // more units → higher score
  const competitiveness = GN_TOWNS.map((t, i) => {
    const composite = (domScores[i] + slScores[i] + unitScores[i]) / 3;
    let category = 'cool';
    if (composite >= 7.5) category = 'hot';
    else if (composite >= 6.5) category = 'comp';
    return { name: t, score: +composite.toFixed(1), category, avgDom: Math.round(metrics[t].avgDom || 0), units: metrics[t].units };
  }).sort((a, b) => b.score - a.score);

  // ---- output ----
  const out = {
    meta: {
      generated: new Date().toISOString(),
      source: 'MLSPIN',
      windowStart: since,
      windowEnd: now.toISOString().slice(0, 10),
      labelYear,
      totalSales: closed.length,
      marketTotalVolume: Math.round(marketTotalVolume),
      yourTotalVolume: Math.round(yourTotalVolume),
      yourMlspinOnlyVolume: Math.round(yourMlspinTotal),
      offMarketVolume: offMarket
    },
    yourFirm: {
      ...yourBrokerage,
      leadOverSecond,
      yearsAtNumber1: cfg.yearsAtNumber1
    },
    brokerages: brokerages.slice(0, 10),
    yourTowns,
    marketTowns,
    saleToList: {
      local: localSL,
      national: cfg.usNationalSaleToList,
      exampleHomePrice: cfg.exampleHomePrice,
      exampleDifference: localSL && cfg.usNationalSaleToList
        ? Math.round(cfg.exampleHomePrice * ((localSL - cfg.usNationalSaleToList) / 100))
        : null
    },
    competitiveness,
    digitalReach: cfg.digitalReach,
    activeBuyersByPrice: cfg.activeBuyersByPrice
  };

  fs.writeFileSync(path.join(OUT, 'market-leader.json'), JSON.stringify(out, null, 2));
  console.log(`\nWrote market-leader.json — ${closed.length} closed in GN over last 365 days`);
  console.log(`\n  BROKERAGE MARKET SHARE`);
  for (const b of brokerages.slice(0, 10)) {
    console.log(`  ${b.name.padEnd(28)}  $${(b.volume/1e6).toFixed(1)}M  ${b.share.toFixed(2)}%  (${b.units} sales)`);
  }
  console.log(`\n  YOUR FIRM — VOLUME BY TOWN (incl off-market $${(offMarket/1e6).toFixed(1)}M)`);
  for (const t of yourTowns) {
    console.log(`  ${t.name.padEnd(18)}  $${(t.volume/1e6).toFixed(1)}M  ${t.share}%  (${t.units} MLSPIN sales)`);
  }
  console.log(`\n  TOTAL MARKET — VOLUME BY TOWN`);
  for (const t of marketTowns) {
    console.log(`  ${t.name.padEnd(18)}  $${(t.volume/1e6).toFixed(1)}M  ${t.share}%  (${t.units} sales)`);
  }
  console.log(`\n  SALE-TO-LIST: ${localSL}% local vs ${cfg.usNationalSaleToList}% national`);
  console.log(`\n  TOWN COMPETITIVENESS`);
  for (const c of competitiveness) {
    console.log(`  ${c.name.padEnd(18)}  ${c.score}  ${c.category}  (DOM ${c.avgDom}, n=${c.units})`);
  }
})().catch(e => { console.error('FAILED:', e.message); process.exit(1); });
