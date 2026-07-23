# Thrive Causemetics — Launch Dashboard

Static launch-performance dashboard hosted on GitHub Pages, with an automated
data-refresh pipeline from Snowflake `DAASITY_DB`.

## Pages

| File | Purpose |
|---|---|
| `index.html` | Launch Intelligence Hub — cards for live / pending / upcoming launches |
| `launch.html?id=<launch-id>` | Per-launch dashboard (8 tabs: Overview, Traffic Sources, Landing Page, PDP Health, Cross-Sells & Pairings, Category Customers, Upcoming Launches, Data Guide) |
| `data.js` | **Generated** data file consumed by both pages (`window.DASHBOARD_DATA`) |

## How data flows

```
Snowflake DAASITY_DB ──> scripts/refresh_data.py ──> data.js ──> GitHub Pages
        (read-only SELECTs)      (daily GitHub Action)     (commit + push)
```

- `config/launches.json` is the launch registry: id, name, launch date,
  category, and SKUs per launch. **To add a launch, edit only this file.**
- `scripts/refresh_data.py` queries Snowflake and rewrites `data.js`.
- `.github/workflows/refresh-data.yml` runs it daily at 10:30 UTC (and on
  manual dispatch), committing `data.js` when it changes.
- Until the Snowflake secrets are configured, the workflow skips itself with a
  warning and the committed static snapshot (data cutoff **Jun 23, 2026**)
  keeps serving.

## Launch lifecycle (date-driven)

- A launch **appears automatically** once its launch date passes and the
  refresh returns data for its SKUs.
- A launch with no SKUs configured (or launched after the current data cutoff)
  shows as **"Live · data pending"**.
- A launch is **removed 122 days after its launch date**
  (`retention_days` in `config/launches.json`).

## Activating the automation

Add these repository secrets (Settings → Secrets and variables → Actions):

| Secret | Required | Notes |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | yes | e.g. `abc12345.us-east-1` |
| `SNOWFLAKE_USER` | yes | service account recommended |
| `SNOWFLAKE_PASSWORD` | yes* | *or `SNOWFLAKE_PRIVATE_KEY_B64` (base64 PEM, key-pair auth) |
| `SNOWFLAKE_WAREHOUSE` | yes | |
| `SNOWFLAKE_ROLE` | no | read-only role recommended |
| `SNOWFLAKE_DATABASE` | no | defaults to `DAASITY_DB` |

**Review & dry-run first:** all queries are read-only SELECTs, but they hit
production data. Column names in the `COLS` dict were verified against
`DAASITY_DB.INFORMATION_SCHEMA` on 2026-07-23; if a schema changes, fix them
there in one place (`python scripts/refresh_data.py --dry-run` prints every
query). Category membership is matched via the `category_type_patterns`
ILIKE patterns in `config/launches.json` against `UOS.PRODUCTS.PRODUCT_TYPE`.
Note the refresh methodology is documented in the script docstring and may
differ from the old manual snapshot by a low single-digit percent (USD
conversion, first-order-based new/returning split, plan window).

## Data rules

- Data cutoff is always **yesterday** relative to the refresh run (never
  today, never a future date). Override with the `DATA_CUTOFF` env var for
  backfills.
- GA4 traffic lags ~2 days behind the sales cutoff.
- All revenue figures are **net of refunds** (non-refunded line items:
  `SUM((price − discount/qty) × qty)`).
- **Category Customers** (per-launch tab): launch buyers split into
  *new-to-category* (no prior purchase in the launch's category before launch
  date, full-history lookback) vs *existing category customers* — i.e., how
  many net-new category customers each launch drives.

## Manual refresh (fallback)

```bash
export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=... SNOWFLAKE_WAREHOUSE=...
python scripts/refresh_data.py          # rewrites data.js
git add data.js && git commit -m "chore: refresh dashboard data" && git push
```
