// pipelines/market_share.js
// =============================================================================
// Brokerage market-share aggregation for the MA Housing Dashboard.
//
// Pulls trailing-12-month closed residential sales for the 10-town territory
// (6 Greater Newburyport core + 4 "& beyond"), groups them two ways, and emits
// ranked share by sides (listing count) and dollar volume — per region.
//
// Two cuts of the same data are produced:
//
//   1. marketShare.officeLevel — every MLS office is its own row. Drives the
//      detailed table on the Market Data page (KW Newburyport ≠ KW Topsfield).
//      EXCEPT: Bentley's two MLS IDs (legacy AN2888 + RE/MAX-era AN8279) are
//      the same physical office and merge to one row, "RE/MAX Bentley's".
//
//   2. marketShare.firmLevel — brand-consolidated. All Keller Williams branches
//      roll up to "Keller Williams", all Lamacchia offices to "Lamacchia Realty",
//      all Coldwell Banker branches to "Coldwell Banker", etc. Drives the
//      brokerage bar chart on the Market Leader sales-presentation page.
//
// Exports: buildMarketShare(token) → Promise<object>
//
// 2026-06 CHANGE — rate-limit fix:
//   Previously this fetched the ENTIRE statewide MA Office directory
//   (~23k offices, ~115 paged requests) on every run, which combined with
//   fetch_market_leader.js's duplicate office scan pushed the pipeline over
//   the Bridge request quota (HTTP 429). We now resolve office names only for
//   the offices that actually appear in the territory's closed sales (~180),
//   via batched OfficeMlsId filters (~5 requests). getRetry is also 429-aware.
// =============================================================================

const https = require('https');

const BRIDGE_BASE = 'https://api.bridgedataoutput.com/api/v2/OData/mlspin';

// Bentley's two MLS IDs are the same physical office — legacy team era + RE/MAX era.
const BENTLEY_IDS = ['AN2888', 'AN8279'];
const BENTLEY_CANONICAL_ID = 'AN8279';
const BENTLEY_DISPLAY_NAME = "RE/MAX Bentley's";

// Region definitions (must stay in sync with fetch_dashboard.js)
const GN_CORE_TOWNS = ['Newburyport', 'West Newbury', 'Newbury', 'Rowley', 'Salisbury', 'Amesbury'];
const BEYOND_TOWNS  = ['Ipswich', 'Georgetown', 'Groveland', 'Merrimac'];
const TERRITORY     = [...GN_CORE_TOWNS, ...BEYOND_TOWNS];

// How many OfficeMlsId values to OR together per Office request. Keeps the
// query string comfortably short while still resolving ~180 offices in a
// handful of calls.
const OFFICE_ID_CHUNK = 40;

// -----------------------------------------------------------------------------
// HTTP / pagination
// -----------------------------------------------------------------------------

function get(url) {
  return new Promise((resolve, reject) => {
    https.get(url, res => {
      let body = '';
      res.on('data', c => (body += c));
      res.on('end', () => {
        if (res.statusCode >= 400) {
          return reject(new Error(`HTTP ${res.statusCode}: ${body.slice(0, 300)}`));
        }
        try { resolve(JSON.parse(body)); }
        catch (e) { reject(new Error(`JSON parse: ${e.message}`)); }
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

// 429-aware retry. On a rate-limit response we wait until the quota resets
// (clamped to a sane window) instead of burning the normal short backoffs.
async function getRetry(url, n = 5) {
  let lastErr;
  for (let i = 0; i < n; i++) {
    try { return await get(url); }
    catch (e) {
      lastErr = e;
      if (i === n - 1) break;
      let wait = 500 * Math.pow(2, i);
      if (/HTTP 429/.test(e.message)) {
        const resetMs = parseResetMs(e.message);
        const until = resetMs ? (resetMs - Date.now() + 2000) : 60000;
        wait = Math.min(Math.max(until, 5000), 120000); // clamp 5s..120s
        console.warn(`[market_share] rate limited (429); waiting ${Math.round(wait / 1000)}s before retry ${i + 1}/${n}`);
      }
      await new Promise(r => setTimeout(r, wait));
    }
  }
  throw lastErr;
}

async function pageAll(initialUrl, token) {
  const all = [];
  let url = initialUrl;
  let page = 0;
  while (url && page < 80) {
    const resp = await getRetry(url);
    if (resp.value) all.push(...resp.value);
    url = resp['@odata.nextLink'];
    if (url && !url.includes('access_token=')) url += '&access_token=' + token;
    page++;
  }
  return all;
}

// -----------------------------------------------------------------------------
// Name cleanup + firm grouping
// -----------------------------------------------------------------------------

// Coldwell Banker registers branches with name tails like " - 74 State St.".
// Strip those so the office-level display reads cleanly.
function cleanOfficeName(name) {
  if (!name) return 'Unknown';
  let n = name.trim();
  n = n.replace(/\s*-\s*\d+\s+[A-Za-z][\w\s.&'-]*$/, '');
  n = n.replace(/\s+/g, ' ').trim();
  return n;
}

// Firm-level grouping: map an office's raw name to a consolidated firm name
// for the Market Leader page chart. Bentley check MUST come before RE/MAX.
function firmGroup(officeName) {
  if (!officeName) return 'Unknown';
  const n = officeName.trim();
  if (/Bentley/i.test(n))                                  return "RE/MAX Bentley's";
  if (/Keller Williams/i.test(n))                          return 'Keller Williams';
  if (/Lamacchia/i.test(n))                                return 'Lamacchia Realty';
  if (/William Raveis|Raveis/i.test(n))                    return 'William Raveis';
  if (/Coldwell Banker/i.test(n))                          return 'Coldwell Banker';
  if (/Gibson Sotheby/i.test(n))                           return "Gibson Sotheby's Int'l Realty";
  if (/Compass/i.test(n))                                  return 'Compass';
  if (/Churchill/i.test(n))                                return 'Churchill Properties';
  if (/J\.?\s*Barrett/i.test(n))                           return 'J. Barrett & Company';
  if (/Realty One Group|Realty ONE/i.test(n))              return 'Realty One Group Nest';
  if (/\beXp\b/i.test(n))                                  return 'eXp Realty';
  if (/Engel\s*(&|and)?\s*V[oö]lkers/i.test(n))            return 'Engel & Volkers';
  if (/Sagan Harborside/i.test(n))                         return 'Sagan Harborside Sotheby';
  if (/Real Broker/i.test(n))                              return 'Real Broker';
  if (/Stone Ridge/i.test(n))                              return 'Stone Ridge Properties';
  if (/Cameron Real Estate|Cameron Prestige/i.test(n))     return 'Cameron Real Estate Group';
  if (/Redfin/i.test(n))                                   return 'Redfin';
  if (/Century 21/i.test(n))                               return 'Century 21';
  if (/Berkshire Hathaway/i.test(n))                       return 'Berkshire Hathaway';
  if (/Re\/Max|RE\/MAX/i.test(n))                          return 'RE/MAX (other offices)';
  return n;  // keep as-is if no match — small offices stay distinct
}

// -----------------------------------------------------------------------------
// Data fetching
// -----------------------------------------------------------------------------

// Resolve office names for a specific set of OfficeMlsIds. Batches the IDs into
// OR'd $filter chunks so we make ~5 requests instead of scanning all ~23k MA
// offices. Offices that don't resolve (discontinued/inactive, not in the Office
// resource) simply won't appear in the map; callers already fall back to the
// raw MLS id for those.
async function fetchOfficesByIds(token, ids) {
  const map = new Map();
  const unique = [...new Set((ids || []).filter(Boolean))];
  const select = 'OfficeKey,OfficeMlsId,OfficeName,OfficeCity,OfficeStateOrProvince';
  for (let i = 0; i < unique.length; i += OFFICE_ID_CHUNK) {
    const slice = unique.slice(i, i + OFFICE_ID_CHUNK);
    const clause = slice.map(id => `OfficeMlsId eq '${String(id).replace(/'/g, "''")}'`).join(' or ');
    const url = `${BRIDGE_BASE}/Office?access_token=${token}` +
                `&$select=${encodeURIComponent(select)}` +
                `&$top=200` +
                `&$filter=${encodeURIComponent(`(${clause})`)}`;
    let resp;
    try { resp = await getRetry(url); }
    catch (e) {
      console.warn(`[market_share] office chunk ${Math.floor(i / OFFICE_ID_CHUNK)} failed: ${e.message}`);
      continue;
    }
    for (const o of (resp.value || [])) {
      if (o.OfficeMlsId) {
        map.set(o.OfficeMlsId, {
          officeName: o.OfficeName || '',
          officeCity: o.OfficeCity || ''
        });
      }
    }
  }
  return map;
}

async function fetchClosedSales(token, monthsBack = 12) {
  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - monthsBack);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  const select = 'ClosePrice,CloseDate,City,ListOfficeMlsId,PropertyType,PropertySubType';
  const all = [];

  for (const city of TERRITORY) {
    const filter = `City eq '${city.replace(/'/g, "''")}' and StandardStatus eq 'Closed' and PropertyType eq 'Residential' and CloseDate ge ${cutoffStr}`;
    const url = `${BRIDGE_BASE}/Property?access_token=${token}&$select=${select}&$top=200&$filter=${encodeURIComponent(filter)}`;
    try {
      const recs = await pageAll(url, token);
      all.push(...recs);
    } catch (e) {
      console.warn(`[market_share] Property fetch failed for ${city}: ${e.message}`);
    }
  }
  return all;
}

// -----------------------------------------------------------------------------
// Aggregation — OFFICE level (every MLS office its own row)
// -----------------------------------------------------------------------------

function aggregateOfficeLevel(records, officeLookup) {
  const grouped = new Map();
  let totalSides = 0;
  let totalVolume = 0;

  for (const rec of records) {
    let officeId = rec.ListOfficeMlsId;
    if (!officeId) continue;
    // Merge Bentley's two IDs into one canonical entry
    if (BENTLEY_IDS.includes(officeId)) officeId = BENTLEY_CANONICAL_ID;

    if (!grouped.has(officeId)) {
      const isBentleys = officeId === BENTLEY_CANONICAL_ID;
      const lookup = officeLookup.get(officeId) || {};
      const rawName = lookup.officeName && lookup.officeName.trim() ? lookup.officeName : null;
      // Fall back to MLS ID when no name is registered (rare — discontinued/inactive offices)
      const officeName = isBentleys
        ? BENTLEY_DISPLAY_NAME
        : (rawName ? cleanOfficeName(rawName) : `Office ${officeId}`);
      const officeCity = lookup.officeCity || '';
      const displayName = officeCity ? `${officeName} — ${officeCity}` : officeName;

      grouped.set(officeId, {
        officeId,
        officeName,
        officeCity,
        displayName,
        sides: 0,
        volume: 0,
        isBentleys
      });
    }

    const entry = grouped.get(officeId);
    entry.sides += 1;
    entry.volume += Number(rec.ClosePrice) || 0;
    totalSides += 1;
    totalVolume += Number(rec.ClosePrice) || 0;
  }

  const offices = Array.from(grouped.values()).map(o => ({
    ...o,
    sidesPct:  totalSides  > 0 ? +((o.sides  / totalSides ) * 100).toFixed(2) : 0,
    volumePct: totalVolume > 0 ? +((o.volume / totalVolume) * 100).toFixed(2) : 0
  }));

  return {
    totalSides,
    totalVolume,
    officeCount: offices.length,
    bySides:  [...offices].sort((a, b) => b.sides  - a.sides),
    byVolume: [...offices].sort((a, b) => b.volume - a.volume)
  };
}

// -----------------------------------------------------------------------------
// Aggregation — FIRM level (brand-merged, for Market Leader page)
// -----------------------------------------------------------------------------

function aggregateFirmLevel(records, officeLookup) {
  const grouped = new Map();
  let totalSides = 0;
  let totalVolume = 0;

  for (const rec of records) {
    let officeId = rec.ListOfficeMlsId;
    if (!officeId) continue;
    if (BENTLEY_IDS.includes(officeId)) officeId = BENTLEY_CANONICAL_ID;

    const lookup = officeLookup.get(officeId) || {};
    const rawName = lookup.officeName && lookup.officeName.trim() ? lookup.officeName : null;
    const officeName = officeId === BENTLEY_CANONICAL_ID
      ? BENTLEY_DISPLAY_NAME
      : (rawName || `Office ${officeId}`);
    const firmName = firmGroup(officeName);

    if (!grouped.has(firmName)) {
      grouped.set(firmName, {
        firmName,
        displayName: firmName,
        name: firmName,           // alias for the Market Leader page (expects b.name)
        sides: 0,
        volume: 0,
        officeIds: new Set(),
        isBentleys: firmName === BENTLEY_DISPLAY_NAME
      });
    }

    const entry = grouped.get(firmName);
    entry.sides += 1;
    entry.volume += Number(rec.ClosePrice) || 0;
    entry.officeIds.add(officeId);
    totalSides += 1;
    totalVolume += Number(rec.ClosePrice) || 0;
  }

  const firms = Array.from(grouped.values()).map(f => ({
    firmName: f.firmName,
    displayName: f.displayName,
    name: f.name,
    sides: f.sides,
    volume: f.volume,
    officeCount: f.officeIds.size,
    isBentleys: f.isBentleys,
    sidesPct:  totalSides  > 0 ? +((f.sides  / totalSides ) * 100).toFixed(2) : 0,
    volumePct: totalVolume > 0 ? +((f.volume / totalVolume) * 100).toFixed(2) : 0,
    // alias for the Market Leader page (expects b.share — using volume share as default)
    share: totalVolume > 0 ? +((f.volume / totalVolume) * 100).toFixed(2) : 0
  }));

  return {
    totalSides,
    totalVolume,
    firmCount: firms.length,
    bySides:  [...firms].sort((a, b) => b.sides  - a.sides),
    byVolume: [...firms].sort((a, b) => b.volume - a.volume)
  };
}

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------

async function buildMarketShare(token) {
  console.log('[market_share] Building...');

  // Pull the closed sales FIRST so we know exactly which offices we need names
  // for — then resolve only those (instead of scanning the whole state).
  console.log('[market_share] Fetching 12mo closed sales for the territory...');
  const closed = await fetchClosedSales(token, 12);
  console.log(`[market_share]   ${closed.length} closed records`);

  const officeIds = closed.map(r => r.ListOfficeMlsId).filter(Boolean);
  officeIds.push(...BENTLEY_IDS); // ensure Bentley always resolvable
  console.log('[market_share] Resolving office names for offices that appear in the data...');
  const officeLookup = await fetchOfficesByIds(token, officeIds);
  console.log(`[market_share]   ${officeLookup.size} offices indexed`);

  const slices = {
    newburyport:        closed.filter(r => r.City === 'Newburyport'),
    greaterNewburyport: closed.filter(r => GN_CORE_TOWNS.includes(r.City)),
    beyond:             closed.filter(r => BEYOND_TOWNS.includes(r.City)),
    territory:          closed
  };

  const officeLevel = {};
  const firmLevel = {};
  for (const k of Object.keys(slices)) {
    officeLevel[k] = aggregateOfficeLevel(slices[k], officeLookup);
    firmLevel[k]   = aggregateFirmLevel(slices[k], officeLookup);
  }

  const out = {
    window:    '12 months trailing',
    generated: new Date().toISOString(),
    coreTowns:  GN_CORE_TOWNS,
    beyondTowns: BEYOND_TOWNS,
    officeLevel,
    firmLevel
  };

  console.log(`[market_share] Done.`);
  console.log(`[market_share]   GN office-level: ${officeLevel.greaterNewburyport.officeCount} offices`);
  console.log(`[market_share]   GN firm-level:   ${firmLevel.greaterNewburyport.firmCount} firms`);
  return out;
}

module.exports = { buildMarketShare, firmGroup };
