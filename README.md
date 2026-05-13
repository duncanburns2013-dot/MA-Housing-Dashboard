# MA Housing Market Dashboard

Live-data dashboard for the bentleysrealestate.com Massachusetts Housing
Market Report page. Pulls MLSPIN sales,
aggregates by region × property type × year, and serves a single
`dashboard.json` consumed by a Squarespace code block.

## How it works

```
GitHub Action (weekly)
    │
    ▼
pipelines/fetch_dashboard.js
    │   pulls closed sales (24mo) + active listings from MLSPIN
    │   aggregates by region (MA / Boston / Essex / Newburyport / GN towns)
    │   computes price, $/sqft, DOM, count, supply, affordability
    ▼
data/processed/dashboard.json   ← committed back to main
    │
    ▼
GitHub Pages serves dashboard.json
    │
    ▼
embed.html (in Squarespace) fetches dashboard.json
    │   overlays live years onto frozen 2014–2024 baseline
    ▼
Rendered page
```

The historical 2014–2024 numbers are baked into `embed.html` and never
change — MLSPIN doesn't reliably retain that far back via this feed.
Only the current year and prior year (live), the Greater Newburyport
town breakdown, the regional summary, and the affordability inputs are
refreshed.

## Local setup

```bash
cp .env.example .env
# edit .env and paste your BRIDGE_TOKEN
node pipelines/fetch_dashboard.js
```

This writes `data/processed/dashboard.json` (committed) and
`data/raw/bridge_*.json` (gitignored).

## Publishing

1. Push this repo to GitHub.
2. Settings → Pages → deploy from `main` branch / root.
3. Settings → Secrets and variables → Actions → add `BRIDGE_TOKEN`.
4. Update `DATA_URL` near the top of `embed.html` to your
   `https://<user>.github.io/<repo>/data/processed/dashboard.json`.
5. Paste `embed.html` into a Squarespace code block on the
   massachusetts-housing-market-data page. Display Source = OFF.

The Action runs every Sunday at 07:00 UTC. Trigger it manually anytime
from the Actions tab → "Refresh dashboard data" → "Run workflow".

## Adding more dashboards

This pattern (script → JSON → GH Pages → fetch in embed) works for any
of the 30+ MA civic dashboards. To add another:

1. Add another script in `pipelines/`.
2. Add another output file under `data/processed/`.
3. Update the workflow to commit it.
4. Build the new embed to fetch from its URL.

Keep one repo per topic, or consolidate into a single
"ma-civic-data" repo with multiple endpoints.
