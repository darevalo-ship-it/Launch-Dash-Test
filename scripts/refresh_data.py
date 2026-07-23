#!/usr/bin/env python3
"""
Regenerate data.js for the Thrive Causemetics Launch Dashboard from Snowflake.

Reads config/launches.json, runs read-only SELECTs against DAASITY_DB, and
rewrites data.js in the repo root with the same structure the dashboard
already consumes (window.DASHBOARD_DATA).

REVIEW & DRY-RUN FIRST
----------------------
Every query is a read-only SELECT, but they run against production data
(Shopify + NetSuite via Daasity, GA4, Recharge). Before the first live run:
  1. `python scripts/refresh_data.py --dry-run` prints every query without
     connecting, so column names can be validated against the schema.
  2. Column names marked VERIFY in COLS below are assumptions taken from the
     dashboard's documented metric definitions — confirm them against
     DAASITY_DB's INFORMATION_SCHEMA and adjust COLS in one place.
  3. category_product_types values in config/launches.json must match real
     product-type values in the data — verify before trusting the
     "New to Category" numbers.

Credentials come from environment variables (set as GitHub Actions secrets):
  SNOWFLAKE_ACCOUNT     e.g. abc12345.us-east-1
  SNOWFLAKE_USER
  SNOWFLAKE_PASSWORD    (or SNOWFLAKE_PRIVATE_KEY_B64 for key-pair auth)
  SNOWFLAKE_WAREHOUSE
  SNOWFLAKE_ROLE        optional
  SNOWFLAKE_DATABASE    optional, default DAASITY_DB
  DATA_CUTOFF           optional YYYY-MM-DD override; default = yesterday.
                        Never set to today or a future date — the last full
                        day of data is always yesterday.
"""

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config" / "launches.json"
OUT_PATH = ROOT / "data.js"

GA4_LAG_DAYS = 2  # GA4 sync lag: traffic data trails the sales cutoff

# ---------------------------------------------------------------------------
# Column-name assumptions, centralized so fixes happen in one place.
# VERIFY each against DAASITY_DB INFORMATION_SCHEMA on the first dry run.
# ---------------------------------------------------------------------------
COLS = {
    "orders": {
        "table": "UOS.ORDERS",
        "order_id": "ORDER_ID",          # VERIFY
        "customer_id": "CUSTOMER_ID",    # VERIFY
        "order_date": "ORDER_DATE",      # VERIFY (date or timestamp)
    },
    "lines": {
        "table": "UOS.ORDER_LINE_ITEMS",
        "order_id": "ORDER_ID",          # VERIFY
        "sku": "SKU",                    # VERIFY
        "qty": "QUANTITY",               # VERIFY
        "price": "PRICE",                # VERIFY (unit price)
        "discount": "DISCOUNT",          # VERIFY (line-level discount)
        "refund_flag": "REFUND_FLAG",    # confirmed used by current snapshot
        "product_type": "PRODUCT_TYPE",  # VERIFY — used for category matching
    },
    "traffic": {
        "table": "GA4_API.BASE_TRAFFIC",
        "date": "CREATED_ON",            # YYYYMMDD text (confirmed)
        "channel": "SESSION_DEFAULT_CHANNEL_GROUPING",  # VERIFY
        "sessions": "SESSIONS",          # VERIFY
        "transactions": "TRANSACTIONS",  # VERIFY
        "revenue": "PURCHASE_REVENUE",   # VERIFY
        "engaged_sessions": "ENGAGED_SESSIONS",  # VERIFY
    },
    "landing": {
        "table": "GA4_BQ_STG.BASE_LANDING_PAGE_STG2",
        "date": "CREATED_ON",            # VERIFY format
        "page": "LANDING_PAGE",          # VERIFY
        "pageviews": "PAGEVIEWS",        # VERIFY
        "sessions": "SESSIONS",          # VERIFY
        "transactions": "TRANSACTIONS",  # VERIFY
        "revenue": "PURCHASE_REVENUE",   # VERIFY
        "engagement_rate": "ENGAGEMENT_RATE",  # VERIFY
    },
    "pdp": {
        "table": "UTS.PRODUCT_PAGE",
        "date": "CREATED_ON",            # VERIFY
        "sku": "SKU",                    # VERIFY
        "views": "PRODUCT_DETAIL_VIEWS", # VERIFY
        "atc": "ADD_TO_CARTS",           # VERIFY
        "checkouts": "CHECKOUTS",        # VERIFY
        "purchases": "PURCHASES",        # VERIFY
        "revenue": "PURCHASE_REVENUE",   # VERIFY
    },
    "affinity": {
        "table": "DRP.PRODUCT_AFFINITY_SETS",
        "sku_a": "SKU_A",                # VERIFY
        "sku_b": "SKU_B",                # VERIFY
        "product_b": "PRODUCT_TITLE_B",  # VERIFY
        "pairs": "PAIR_COUNT",           # VERIFY
    },
    "subs": {
        "table": "USS.ORDER_LINE_ITEMS",  # Recharge; no order dates — all-time
        "order_id": "ORDER_ID",          # VERIFY
        "sku": "SKU",                    # VERIFY
        "qty": "QUANTITY",               # VERIFY
    },
    "plan": {
        "table": "GSHEETS.SKU_LAUNCH_DAY_FORECAST",
        "sku": "SKU",                    # VERIFY
        "units": "PLAN_UNITS",           # VERIFY
    },
}

# Net revenue per the dashboard's documented definition:
# SUM((price - discount/qty) * qty) on non-refunded line items.
NET_SALES_EXPR = (
    "SUM(({p}.{price} - {p}.{discount} / NULLIF({p}.{qty}, 0)) * {p}.{qty})"
)


def sku_list_sql(skus):
    """Render a validated SKU list for an IN clause (SKUs come from our own
    config file, but keep them strictly alphanumeric as defense in depth)."""
    for s in skus:
        if not s.replace("-", "").isalnum():
            raise ValueError(f"Suspicious SKU rejected: {s!r}")
    return ", ".join(f"'{s}'" for s in skus)


def str_list_sql(values):
    for v in values:
        if "'" in v:
            raise ValueError(f"Suspicious value rejected: {v!r}")
    return ", ".join(f"'{v}'" for v in values)


# ---------------------------------------------------------------------------
# Query builders — each returns (sql, params). All filters use bound params
# for dates; SKU/category lists are validated and inlined.
# ---------------------------------------------------------------------------

def q_launch_lines_cte(skus):
    o, li = COLS["orders"], COLS["lines"]
    return f"""
WITH launch_lines AS (
  SELECT li.{li['sku']} AS SKU, li.{li['qty']} AS QTY,
         (li.{li['price']} - li.{li['discount']} / NULLIF(li.{li['qty']}, 0)) * li.{li['qty']} AS NET_LINE,
         o.{o['order_id']} AS ORDER_ID, o.{o['customer_id']} AS CUSTOMER_ID,
         CAST(o.{o['order_date']} AS DATE) AS ORDER_DAY
  FROM {li['table']} li
  JOIN {o['table']} o ON o.{o['order_id']} = li.{li['order_id']}
  WHERE li.{li['sku']} IN ({sku_list_sql(skus)})
    AND CAST(o.{o['order_date']} AS DATE) BETWEEN %(launch_date)s AND %(cutoff)s
    AND COALESCE(li.{li['refund_flag']}, FALSE) = FALSE
)"""


def q_summary(skus):
    o = COLS["orders"]
    sql = q_launch_lines_cte(skus) + f""",
first_orders AS (
  SELECT {o['customer_id']} AS CUSTOMER_ID, MIN(CAST({o['order_date']} AS DATE)) AS FIRST_ORDER_DAY
  FROM {o['table']}
  GROUP BY 1
)
SELECT
  SUM(ll.NET_LINE)                                    AS NET_SALES,
  SUM(ll.QTY)                                         AS UNITS,
  COUNT(DISTINCT ll.ORDER_ID)                         AS ORDERS,
  COUNT(DISTINCT ll.CUSTOMER_ID)                      AS TOTAL_CUSTOMERS,
  COUNT(DISTINCT CASE WHEN fo.FIRST_ORDER_DAY >= %(launch_date)s THEN ll.CUSTOMER_ID END) AS NEW_CUSTOMERS
FROM launch_lines ll
LEFT JOIN first_orders fo ON fo.CUSTOMER_ID = ll.CUSTOMER_ID
"""
    return sql


def q_by_variant(skus):
    o = COLS["orders"]
    return q_launch_lines_cte(skus) + f""",
first_orders AS (
  SELECT {o['customer_id']} AS CUSTOMER_ID, MIN(CAST({o['order_date']} AS DATE)) AS FIRST_ORDER_DAY
  FROM {o['table']}
  GROUP BY 1
)
SELECT ll.SKU,
       SUM(ll.NET_LINE)               AS NET_SALES,
       SUM(ll.QTY)                    AS UNITS,
       COUNT(DISTINCT ll.ORDER_ID)    AS ORDERS,
       COUNT(DISTINCT CASE WHEN fo.FIRST_ORDER_DAY >= %(launch_date)s THEN ll.CUSTOMER_ID END) AS NEW_CUSTOMERS,
       COUNT(DISTINCT CASE WHEN fo.FIRST_ORDER_DAY <  %(launch_date)s THEN ll.CUSTOMER_ID END) AS RET_CUSTOMERS
FROM launch_lines ll
LEFT JOIN first_orders fo ON fo.CUSTOMER_ID = ll.CUSTOMER_ID
GROUP BY 1 ORDER BY NET_SALES DESC
"""


def q_daily(skus):
    return q_launch_lines_cte(skus) + """
SELECT ORDER_DAY AS D, SUM(QTY) AS UNITS, SUM(NET_LINE) AS NET_SALES
FROM launch_lines GROUP BY 1 ORDER BY 1
"""


def q_category_customers(skus, cat_types, cat_col):
    """New-to-category vs existing-category customers for a launch.

    Launch buyers = distinct customers with a non-refunded order containing a
    launch SKU between launch date and cutoff. 'Existing' = same customer had
    any earlier non-refunded order containing a product in the launch's
    category (full history lookback before launch date)."""
    o, li = COLS["orders"], COLS["lines"]
    return q_launch_lines_cte(skus) + f""",
launch_buyers AS (
  SELECT DISTINCT CUSTOMER_ID FROM launch_lines
),
prior_category_buyers AS (
  SELECT DISTINCT o.{o['customer_id']} AS CUSTOMER_ID
  FROM {li['table']} li
  JOIN {o['table']} o ON o.{o['order_id']} = li.{li['order_id']}
  WHERE li.{cat_col} IN ({str_list_sql(cat_types)})
    AND CAST(o.{o['order_date']} AS DATE) < %(launch_date)s
    AND COALESCE(li.{li['refund_flag']}, FALSE) = FALSE
)
SELECT COUNT(*)                            AS TOTAL,
       COUNT(p.CUSTOMER_ID)                AS EXISTING_CATEGORY,
       COUNT(*) - COUNT(p.CUSTOMER_ID)     AS NEW_TO_CATEGORY
FROM launch_buyers b
LEFT JOIN prior_category_buyers p ON p.CUSTOMER_ID = b.CUSTOMER_ID
"""


def q_category_by_variant(skus, cat_types, cat_col):
    o, li = COLS["orders"], COLS["lines"]
    return q_launch_lines_cte(skus) + f""",
buyer_variants AS (
  SELECT DISTINCT SKU, CUSTOMER_ID FROM launch_lines
),
prior_category_buyers AS (
  SELECT DISTINCT o.{o['customer_id']} AS CUSTOMER_ID
  FROM {li['table']} li
  JOIN {o['table']} o ON o.{o['order_id']} = li.{li['order_id']}
  WHERE li.{cat_col} IN ({str_list_sql(cat_types)})
    AND CAST(o.{o['order_date']} AS DATE) < %(launch_date)s
    AND COALESCE(li.{li['refund_flag']}, FALSE) = FALSE
)
SELECT bv.SKU,
       COUNT(*) - COUNT(p.CUSTOMER_ID) AS NEW_TO_CATEGORY,
       COUNT(p.CUSTOMER_ID)            AS EXISTING_CATEGORY
FROM buyer_variants bv
LEFT JOIN prior_category_buyers p ON p.CUSTOMER_ID = bv.CUSTOMER_ID
GROUP BY 1
"""


def q_pdp(skus):
    c = COLS["pdp"]
    return f"""
SELECT {c['sku']} AS SKU,
       SUM({c['views']})     AS PDP_VIEWS,
       SUM({c['atc']})       AS ATC,
       SUM({c['checkouts']}) AS CKTS,
       SUM({c['purchases']}) AS PURCH,
       SUM({c['revenue']})   AS REV
FROM {c['table']}
WHERE {c['sku']} IN ({sku_list_sql(skus)})
  AND CAST({c['date']} AS DATE) BETWEEN %(launch_date)s AND %(cutoff)s
GROUP BY 1 ORDER BY PDP_VIEWS DESC
"""


def q_cross_sell(skus):
    c = COLS["affinity"]
    return f"""
SELECT {c['product_b']} AS PRODUCT, {c['sku_b']} AS SKU, SUM({c['pairs']}) AS PAIRS
FROM {c['table']}
WHERE {c['sku_a']} IN ({sku_list_sql(skus)})
  AND {c['sku_b']} NOT IN ({sku_list_sql(skus)})
GROUP BY 1, 2 ORDER BY PAIRS DESC LIMIT 10
"""


def q_subs(skus):
    c = COLS["subs"]
    return f"""
SELECT COUNT(DISTINCT {c['order_id']}) AS SUB_ORDERS, SUM({c['qty']}) AS SUB_UNITS
FROM {c['table']}
WHERE {c['sku']} IN ({sku_list_sql(skus)})
"""


def q_plan(skus):
    c = COLS["plan"]
    return f"""
SELECT {c['sku']} AS SKU, SUM({c['units']}) AS PLAN_UNITS
FROM {c['table']}
WHERE {c['sku']} IN ({sku_list_sql(skus)})
GROUP BY 1
"""


def q_traffic_by_channel():
    c = COLS["traffic"]
    # CREATED_ON is YYYYMMDD text — compare on TO_DATE(...)
    return f"""
SELECT {c['channel']} AS CH,
       SUM({c['sessions']})      AS SESSIONS,
       SUM({c['transactions']})  AS TXNS,
       SUM({c['revenue']})       AS REV,
       SUM({c['engaged_sessions']}) AS ENGAGED
FROM {c['table']}
WHERE TO_DATE({c['date']}, 'YYYYMMDD') BETWEEN %(traffic_start)s AND %(ga_cutoff)s
GROUP BY 1 ORDER BY SESSIONS DESC
"""


def q_traffic_monthly():
    c = COLS["traffic"]
    return f"""
SELECT TO_CHAR(TO_DATE({c['date']}, 'YYYYMMDD'), 'Mon YYYY') AS MONTH,
       MIN(TO_DATE({c['date']}, 'YYYYMMDD'))                  AS MONTH_START,
       {c['channel']} AS CH,
       SUM({c['sessions']}) AS SESSIONS
FROM {c['table']}
WHERE TO_DATE({c['date']}, 'YYYYMMDD') BETWEEN %(traffic_start)s AND %(ga_cutoff)s
GROUP BY 1, 3 ORDER BY MONTH_START
"""


def q_landing(all_skus_and_slugs):
    c = COLS["landing"]
    like = " OR ".join(
        f"{c['page']} ILIKE '%{s}%'" for s in all_skus_and_slugs
    )
    return f"""
SELECT {c['page']} AS PAGE,
       SUM({c['pageviews']})    AS PAGEVIEWS,
       SUM({c['sessions']})     AS SESSIONS,
       SUM({c['transactions']}) AS TXNS,
       SUM({c['revenue']})      AS REV,
       AVG({c['engagement_rate']}) AS ENG
FROM {c['table']}
WHERE ({like})
  AND CAST({c['date']} AS DATE) BETWEEN %(traffic_start)s AND %(ga_cutoff)s
GROUP BY 1 ORDER BY SESSIONS DESC LIMIT 10
"""


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def connect():
    import snowflake.connector  # imported lazily so --dry-run needs no deps

    kwargs = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "DAASITY_DB"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )
    pk_b64 = os.environ.get("SNOWFLAKE_PRIVATE_KEY_B64")
    if pk_b64:
        import base64
        from cryptography.hazmat.primitives import serialization

        key = serialization.load_pem_private_key(base64.b64decode(pk_b64), password=None)
        kwargs["private_key"] = key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    else:
        kwargs["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    return snowflake.connector.connect(**{k: v for k, v in kwargs.items() if v})


def rows(cur, sql, params):
    cur.execute(sql, params)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def f2(v):
    return round(float(v or 0), 2)


def build_launch(cur, lc, cutoff, cat_col):
    launch_date = lc["launch_date"]
    skus = [s["sku"] for s in lc["skus"]]
    sku_meta = {s["sku"]: s for s in lc["skus"]}
    params = {"launch_date": launch_date, "cutoff": cutoff}

    summary_row = rows(cur, q_summary(skus), params)[0]
    variant_rows = rows(cur, q_by_variant(skus), params)
    daily_rows = rows(cur, q_daily(skus), params)
    pdp_rows = rows(cur, q_pdp(skus), params)
    cross_rows = rows(cur, q_cross_sell(skus), {})
    subs_row = rows(cur, q_subs(skus), {})[0]
    plan_rows = {r["SKU"]: int(r["PLAN_UNITS"] or 0) for r in rows(cur, q_plan(skus), {})}

    cat_types = lc.get("category_product_types") or []
    cc = None
    if cat_types:
        cc_row = rows(cur, q_category_customers(skus, cat_types, cat_col), params)[0]
        cc_var = rows(cur, q_category_by_variant(skus, cat_types, cat_col), params)
        cc = {
            "category": lc.get("category"),
            "total": int(cc_row["TOTAL"] or 0),
            "existingCategory": int(cc_row["EXISTING_CATEGORY"] or 0),
            "newToCategory": int(cc_row["NEW_TO_CATEGORY"] or 0),
            "byVariant": [
                {
                    "sku": r["SKU"],
                    "name": sku_meta.get(r["SKU"], {}).get("name", r["SKU"]),
                    "newToCategory": int(r["NEW_TO_CATEGORY"] or 0),
                    "existingCategory": int(r["EXISTING_CATEGORY"] or 0),
                }
                for r in cc_var
            ],
        }

    total_units = int(summary_row["UNITS"] or 0)
    total_orders = int(summary_row["ORDERS"] or 0)
    total_cust = int(summary_row["TOTAL_CUSTOMERS"] or 0)
    new_cust = int(summary_row["NEW_CUSTOMERS"] or 0)
    ret_cust = total_cust - new_cust
    net_sales = f2(summary_row["NET_SALES"])
    plan_total = sum(plan_rows.values()) or None

    total_pdp_views = sum(int(r["PDP_VIEWS"] or 0) for r in pdp_rows)
    total_atc = sum(int(r["ATC"] or 0) for r in pdp_rows)
    total_purch = sum(int(r["PURCH"] or 0) for r in pdp_rows)

    by_variant = []
    for r in variant_rows:
        sku = r["SKU"]
        meta = sku_meta.get(sku, {})
        units = int(r["UNITS"] or 0)
        plan = plan_rows.get(sku)
        by_variant.append({
            "sku": sku,
            "name": meta.get("name", sku),
            "shade": meta.get("shade", ""),
            "color": meta.get("color", "#3A9E98"),
            "netSales": f2(r["NET_SALES"]),
            "units": units,
            "orders": int(r["ORDERS"] or 0),
            "newCustomers": int(r["NEW_CUSTOMERS"] or 0),
            "retCustomers": int(r["RET_CUSTOMERS"] or 0),
            "planUnits": plan,
            "pctToPlanUnits": round(units / plan * 100, 1) if plan else None,
        })

    daily, cum_u, cum_s = [], 0, 0.0
    for r in daily_rows:
        u, s = int(r["UNITS"] or 0), f2(r["NET_SALES"])
        cum_u += u
        cum_s = round(cum_s + s, 2)
        daily.append({"date": str(r["D"]), "units": u, "netSales": s,
                      "cumUnits": cum_u, "cumSales": cum_s})

    pdp = []
    for r in pdp_rows:
        views = int(r["PDP_VIEWS"] or 0)
        pdp.append({
            "sku": r["SKU"],
            "name": sku_meta.get(r["SKU"], {}).get("name", r["SKU"]),
            "pdpViews": views,
            "atc": int(r["ATC"] or 0),
            "ckts": int(r["CKTS"] or 0),
            "purch": int(r["PURCH"] or 0),
            "rev": f2(r["REV"]),
            "atcRate": round(int(r["ATC"] or 0) / views * 100, 2) if views else 0,
            "purchRate": round(int(r["PURCH"] or 0) / views * 100, 2) if views else 0,
        })

    return {
        "launchId": lc["id"],
        "name": lc["name"],
        "launchDate": launch_date,
        "internalDate": lc.get("internal_date"),
        "status": "LIVE",
        "category": lc.get("category"),
        "subtitle": lc.get("subtitle", ""),
        "accent": lc.get("accent", "#3A9E98"),
        "summary": {
            "netSales": net_sales,
            "units": total_units,
            "orders": total_orders,
            "aov": round(net_sales / total_orders, 2) if total_orders else 0,
            "newCustomers": new_cust,
            "retCustomers": ret_cust,
            "totalCustomers": total_cust,
            "newPct": round(new_cust / total_cust * 100, 1) if total_cust else 0,
            "retPct": round(ret_cust / total_cust * 100, 1) if total_cust else 0,
            "planUnits": plan_total,
            "pctToPlanUnits": round(total_units / plan_total * 100, 1) if plan_total else None,
            "subscriptionOrders": int(subs_row["SUB_ORDERS"] or 0),
            "subscriptionUnits": int(subs_row["SUB_UNITS"] or 0),
            "pdpViews": total_pdp_views,
            "pdpAtcRate": round(total_atc / total_pdp_views * 100, 1) if total_pdp_views else 0,
            "pdpCvr": round(total_purch / total_pdp_views * 100, 1) if total_pdp_views else 0,
        },
        "byVariant": by_variant,
        "dailySales": daily,
        "pdp": pdp,
        "crossSell": [
            {"product": r["PRODUCT"], "sku": r["SKU"], "pairs": int(r["PAIRS"] or 0)}
            for r in cross_rows
        ],
        "categoryCustomers": cc,
    }


def pending_launch(lc):
    return {
        "launchId": lc["id"], "name": lc["name"], "launchDate": lc["launch_date"],
        "internalDate": lc.get("internal_date"), "status": "LIVE",
        "category": lc.get("category"), "subtitle": lc.get("subtitle", ""),
        "accent": lc.get("accent", "#8B5CF6"), "skusPending": not lc["skus"],
        "summary": None, "byVariant": [], "dailySales": [], "pdp": [],
        "crossSell": [], "categoryCustomers": None,
    }


def build_traffic(cur, params):
    ch_rows = rows(cur, q_traffic_by_channel(), params)
    by_channel = []
    for r in ch_rows:
        sessions = int(r["SESSIONS"] or 0)
        by_channel.append({
            "ch": r["CH"] or "Unassigned",
            "sessions": sessions,
            "txns": int(r["TXNS"] or 0),
            "rev": f2(r["REV"]),
            "cvr": round(int(r["TXNS"] or 0) / sessions * 100, 2) if sessions else 0,
            "eng": round(int(r["ENGAGED"] or 0) / sessions * 100, 1) if sessions else 0,
        })
    mo_rows = rows(cur, q_traffic_monthly(), params)
    monthly, order = {}, []
    for r in mo_rows:
        m = r["MONTH"]
        if m not in monthly:
            monthly[m] = {}
            order.append(m)
        monthly[m][r["CH"] or "Unassigned"] = int(r["SESSIONS"] or 0)
    return {"byChannel": by_channel,
            "monthly": [{"month": m, "chs": monthly[m]} for m in order]}


def build_landing(cur, cfg, params):
    slugs = set()
    for lc in cfg["launches"]:
        slugs.update(s["sku"] for s in lc["skus"])
        slugs.add(lc["name"].split(" ")[0].lower())
    lrows = rows(cur, q_landing(sorted(slugs)), params)
    out = []
    for r in lrows:
        sessions = int(r["SESSIONS"] or 0)
        page = r["PAGE"] or ""
        out.append({
            "label": page.split("/")[-1][:40] or page[:40],
            "page": page,
            "pageviews": int(r["PAGEVIEWS"] or 0),
            "sessions": sessions,
            "txns": int(r["TXNS"] or 0),
            "rev": f2(r["REV"]),
            "eng": round(float(r["ENG"] or 0), 1),
            "cvr": round(int(r["TXNS"] or 0) / sessions * 100, 2) if sessions else 0,
        })
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="print all queries without connecting to Snowflake")
    args = ap.parse_args()

    cfg = json.loads(CONFIG_PATH.read_text())
    retention = int(cfg.get("retention_days", 122))
    cat_col = cfg.get("category_type_column", "PRODUCT_TYPE")

    today = dt.date.today()
    cutoff = os.environ.get("DATA_CUTOFF") or str(today - dt.timedelta(days=1))
    if cutoff >= str(today):
        sys.exit(f"DATA_CUTOFF {cutoff} must be in the past (last full day = yesterday).")
    ga_cutoff = str(dt.date.fromisoformat(cutoff) - dt.timedelta(days=GA4_LAG_DAYS))

    launched = [lc for lc in cfg["launches"] if lc["launch_date"] <= cutoff]
    active = [lc for lc in launched
              if (dt.date.fromisoformat(cutoff) - dt.date.fromisoformat(lc["launch_date"])).days <= retention]
    with_skus = [lc for lc in active if lc["skus"]]
    no_skus = [lc for lc in active if not lc["skus"]]
    # launched too recently for the cutoff (e.g. launched today/yesterday)
    too_new = [lc for lc in cfg["launches"] if lc["launch_date"] > cutoff and lc["launch_date"] <= str(today)]
    traffic_start = min((lc["launch_date"] for lc in active), default=cutoff)
    params = {"traffic_start": traffic_start, "ga_cutoff": ga_cutoff}

    if args.dry_run:
        print(f"-- cutoff={cutoff} ga_cutoff={ga_cutoff} traffic_start={traffic_start}")
        print(f"-- active launches: {[lc['id'] for lc in active]}")
        print(f"-- skipped (no SKUs yet): {[lc['id'] for lc in no_skus]}")
        for lc in with_skus:
            skus = [s["sku"] for s in lc["skus"]]
            cat_types = lc.get("category_product_types") or []
            print(f"\n-- ========== {lc['id']} ==========")
            for name, sql in [
                ("summary", q_summary(skus)), ("by_variant", q_by_variant(skus)),
                ("daily", q_daily(skus)), ("pdp", q_pdp(skus)),
                ("cross_sell", q_cross_sell(skus)), ("subs", q_subs(skus)),
                ("plan", q_plan(skus)),
                ("category_customers", q_category_customers(skus, cat_types, cat_col) if cat_types else "-- no category types configured"),
            ]:
                print(f"\n-- {lc['id']}.{name}\n{sql}")
        print(f"\n-- traffic_by_channel\n{q_traffic_by_channel()}")
        print(f"\n-- traffic_monthly\n{q_traffic_monthly()}")
        print("\n-- landing page query built from configured SKUs/slugs at runtime")
        return

    for lc in no_skus:
        print(f"WARNING: {lc['id']} launched {lc['launch_date']} but has no SKUs "
              f"in config/launches.json — skipping (will show as 'data pending').")

    conn = connect()
    try:
        cur = conn.cursor()
        launches = [build_launch(cur, lc, cutoff, cat_col) for lc in with_skus]
        launches += [pending_launch(lc) for lc in no_skus + too_new]
        traffic = build_traffic(cur, params)
        landing = build_landing(cur, cfg, params)
    finally:
        conn.close()

    data = {
        "meta": {
            "dataCutoff": cutoff,
            "gaCutoff": ga_cutoff,
            "trafficStart": traffic_start,
            "generatedAt": str(today),
            "mode": "automated",
            "sourceDb": os.environ.get("SNOWFLAKE_DATABASE", "DAASITY_DB"),
            "retentionDays": retention,
        },
        "launches": launches,
        "traffic": traffic,
        "landing": landing,
        "upcoming": [
            {"name": u["name"], "launchDate": u["launch_date"],
             "internalDate": u.get("internal_date"), "trackedId": u.get("tracked_id")}
            for u in cfg.get("upcoming", [])
        ],
    }

    OUT_PATH.write_text(
        "/* GENERATED FILE — do not hand-edit.\n"
        f"   Built by scripts/refresh_data.py on {today} · cutoff {cutoff} · "
        f"source Snowflake {data['meta']['sourceDb']} */\n"
        "window.DASHBOARD_DATA = "
        + json.dumps(data, indent=2, default=str)
        + ";\n"
    )
    print(f"Wrote {OUT_PATH} · cutoff {cutoff} · {len(launches)} launches "
          f"({len(with_skus)} with data, {len(no_skus) + len(too_new)} pending)")


if __name__ == "__main__":
    main()
