// fetch_fub.js — pulls aggregate Follow Up Boss CRM stats.
//
// Writes data/processed/fub-stats.json — counts only, NEVER individual PII.
// The Market Leader page can consume this to show real-time database stats
// (Hot Prospects count, Buyers/Sellers split, IDX activity, etc.).
//
// Auth: HTTP Basic with the API key as the username and an empty password.
//
// Usage:
//   1. Save your FUB_API_KEY to .env (copy from .env.example).
//   2. node pipelines/fetch_fub.js
//
// Output:
//   data/processed/fub-stats.json   — public, consumed by embeds

const fs = require('fs');
const path = require('path');
const https = require('https');

const ROOT = path.join(__dirname, '..');
const OUT = path.join(ROOT, 'data', 'processed');
fs.mkdirSync(OUT, { recursive: true });

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

// ---------- HTTP ----------
function fubGet(pathStr, apiKey) {
  return new Promise((resolve, reject) => {
    const auth = Buffer.from(`${apiKey}:`).toString('base64');
    const opts = {
      hostname: 'api.followupboss.com',
      path: pathStr,
      method: 'GET',
      headers: {
        Authorization: `Basic ${auth}`,
        'X-System': 'Bentleys-Dashboard',
        'X-System-Key': apiKey,
        'User-Agent': 'MA-Housing-Dashboard/1.0'
      }
    };
    https.request(opts, res => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        if (res.statusCode >= 400) return reject(new Error(`FUB ${pathStr} → HTTP ${res.statusCode}: ${body.slice(0, 200)}`));
        try { resolve(JSON.parse(body)); } catch (e) { reject(e); }
      });
    }).on('error', reject).end();
  });
}

async function fubGetRetry(pathStr, apiKey, n = 4) {
  let last;
  for (let i = 0; i < n; i++) {
    try { return await fubGet(pathStr, apiKey); }
    catch (e) {
      last = e;
      const wait = 1000 * Math.pow(2, i);
      console.warn(`  retry ${i + 1}/${n} after ${wait}ms: ${e.message}`);
      await new Promise(r => setTimeout(r, wait));
    }
  }
  throw last;
}

// ---------- main ----------
(async () => {
  const env = loadEnv();
  const apiKey = env.FUB_API_KEY;
  if (!apiKey || apiKey === 'fka_your_fub_api_key_here') {
    console.error('FUB_API_KEY missing in .env (or workflow secret).');
    process.exit(1);
  }

  console.log('Pulling FUB identity + total count...');
  const identity = await fubGetRetry('/v1/identity', apiKey);
  const totalPing = await fubGetRetry('/v1/people?limit=1', apiKey);
  const total = (totalPing._metadata && totalPing._metadata.total) || 0;

  // Pull stages, then count people in each via stageId filter on /people.
  console.log('Pulling stages + per-stage counts...');
  const stagesResp = await fubGetRetry('/v1/stages', apiKey);
  const stagesList = stagesResp.stages || [];
  const byStage = {};
  for (const s of stagesList) {
    try {
      const r = await fubGetRetry(`/v1/people?limit=1&stageId=${s.id}`, apiKey);
      byStage[s.name] = (r._metadata && r._metadata.total) || 0;
    } catch (e) {
      console.warn(`  stage "${s.name}" lookup failed: ${e.message}`);
    }
  }

  // Pull smart lists, then count people in each via smartListId filter on /people.
  console.log('Pulling smart lists + per-list counts...');
  const smartListsResp = await fubGetRetry('/v1/smartLists', apiKey);
  const smartLists = [];
  for (const s of (smartListsResp.smartLists || [])) {
    let count = 0;
    try {
      const r = await fubGetRetry(`/v1/people?limit=1&smartListId=${s.id}`, apiKey);
      count = (r._metadata && r._metadata.total) || 0;
    } catch (e) {
      console.warn(`  smartList "${s.name}" lookup failed: ${e.message}`);
    }
    smartLists.push({ id: s.id, name: s.name, peopleCount: count });
  }

  // Recent activity — people updated in the last 7 / 30 days.
  console.log('Pulling recent activity counts...');
  function nDaysAgo(n) { const d = new Date(Date.now() - n * 86400e3); return d.toISOString(); }
  const activity = {};
  for (const [label, days] of [['last7days', 7], ['last30days', 30]]) {
    try {
      const resp = await fubGetRetry(`/v1/people?limit=1&updatedAfter=${encodeURIComponent(nDaysAgo(days))}`, apiKey);
      activity[label] = (resp._metadata && resp._metadata.total) || 0;
    } catch (e) {
      console.warn(`  activity "${label}" lookup failed: ${e.message}`);
    }
  }

  // ---- output ----
  const out = {
    meta: {
      generated: new Date().toISOString(),
      source: 'Follow Up Boss',
      account: identity.account && identity.account.name ? identity.account.name : null
    },
    totals: {
      people: total
    },
    byStage,
    activity,
    smartLists
  };

  fs.writeFileSync(path.join(OUT, 'fub-stats.json'), JSON.stringify(out, null, 2));
  console.log(`\nWrote fub-stats.json — ${total.toLocaleString()} people total`);
  console.log('\n  STAGES');
  for (const [k, v] of Object.entries(byStage)) console.log(`  ${k.padEnd(20)} ${v.toLocaleString()}`);
  console.log('\n  ACTIVITY');
  for (const [k, v] of Object.entries(activity)) console.log(`  ${k.padEnd(20)} ${v.toLocaleString()}`);
  console.log('\n  SMART LISTS');
  smartLists.sort((a, b) => b.peopleCount - a.peopleCount).forEach(s => {
    console.log(`  [${String(s.id).padStart(3)}] ${s.name.padEnd(20)} ${s.peopleCount.toLocaleString()}`);
  });
})().catch(e => { console.error('FAILED:', e.message); process.exit(1); });
