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

// Cities where competitor offices register but that aren't in the territory.
// Needed for the Office lookup so we get clean display names for KW Andover, etc.
const NEIGHBORING_REGISTRATION_CITIES = [
  'Andover', 'North Andover', 'Boxford', 'Haverhill', 'Topsfield',
  'Boston', 'Beverly', 'Marblehead', 'Manchester', 'Lynnfield', 'Wenham',
  'Hamilton', 'Essex', 'Rockport', 'Gloucester'
];

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

async function getRetry(url, n = 4) {
  let lastErr;
  for (let i = 0; i < n; i++) {
    try { return await get(url); }
    catch (e) {
      lastErr = e;
      await new Promise(r => setTimeout(r, 500 * Math.pow(2, i)));
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

async function fetchOfficeLookup(token) {
  const cities = [...TERRITORY, ...NEIGHBORING_REGISTRATION_CITIES];
  const map = new Map();

  for (const city of cities) {
    const filter = `OfficeCity eq '${city.replace(/'/g, "''")}' and OfficeStateOrProvince eq 'MA'`;
    const select = 'OfficeMlsId,OfficeName,OfficeCity';
    const url = `${BRIDGE_BASE}/Office?access_token=${token}&$select=${select}&$top=200&$filter=${encodeURIComponent(filter)}`;
    try {
      const offices = await pageAll(url, token);
      for (const o of offices) {
        if (o.OfficeMlsId) {
          map.set(o.OfficeMlsId, {
            officeName: o.OfficeName || 'Unknown',
            officeCity: o.OfficeCity || ''
          });
        }
      }
    } catch (e) {
      console.warn(`[market_share] Office lookup failed for ${city}: ${e.message}`);
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
      const officeName = isBentleys ? BENTLEY_DISPLAY_NAME : cleanOfficeName(lookup.officeName);
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
    const officeName = officeId === BENTLEY_CANONICAL_ID
      ? BENTLEY_DISPLAY_NAME
      : (lookup.officeName || 'Unknown');
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

  console.log('[market_share] Fetching office lookup...');
  const officeLookup = await fetchOfficeLookup(token);
  console.log(`[market_share]   ${officeLookup.size} offices indexed`);

  console.log('[market_share] Fetching 12mo closed sales for the territory...');
  const closed = await fetchClosedSales(token, 12);
  console.log(`[market_share]   ${closed.length} closed records`);

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
