#!/usr/bin/env python3
"""
Regenerate data.js for the Thrive Causemetics Launch Dashboard from Snowflake.

Reads config/launches.json, runs read-only SELECTs against DAASITY_DB, and
rewrites data.js in the repo root with the same structure the dashboard
already consumes (window.DASHBOARD_DATA).

REVIEW & DRY-RUN FIRST
----------------------
Every query is a read-only SELECT, but they run against production data
(Shopify + NetSuite via Daasity, GA4, Recharge). `--dry-run` prints every
query without connecting.

All column names in COLS were verified against DAASITY_DB INFORMATION_SCHEMA
on 2026-07-23. Methodology notes (documented deviations from the old manual
snapshot, which may differ by a low single-digit %):
  - Net sales = SUM((PRICE - DISCOUNT_AMOUNT/QTY) * QTY) on line items with
    REFUND_FLAG = FALSE, converted to USD via CURRENCY_CONVERSION_RATE.
  - New vs returning customers = first-ever UOS order date on/after launch
    date (the old snapshot estimated this from a historical ratio).
  - % to Plan = units vs SUM(PLAN_UNITS) between launch date and cutoff.
  - Cross-sell pairs = same-cart co-occurrence events from
    DRP.PRODUCT_AFFINITY_SETS (launch SKU as primary, ordered in window).
  - Category membership = UOS.PRODUCTS.PRODUCT_TYPE matched with the ILIKE
    patterns in config/launches.json (category_type_patterns).

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

# Tables are always resolved in this database, regardless of the connection's
# default database context (SNOWFLAKE_DATABASE).
SRC_DB = os.environ.get("SNOWFLAKE_SOURCE_DB", "DAASITY_DB")

# ---------------------------------------------------------------------------
# Column names, centralized so fixes happen in one place.
# Verified against DAASITY_DB INFORMATION_SCHEMA on 2026-07-23.
# ---------------------------------------------------------------------------
COLS = {
    "orders": {
        "table": "UOS.ORDERS",
        "order_id": "ORDER_ID",
        "customer_id": "CUSTOMER_ID",
        "order_date": "ORDER_DATE",      # TIMESTAMP_NTZ
    },
    "lines": {
        "table": "UOS.ORDER_LINE_ITEMS",
        "order_id": "ORDER_ID",
        "product_id": "PRODUCT_ID",
        "sku": "SKU",
        "qty": "QUANTITY",
        "price": "PRICE",                # unit price, original currency
        "discount": "DISCOUNT_AMOUNT",   # line-level discount
        "fx": "CURRENCY_CONVERSION_RATE",  # original currency -> USD
        "refund_flag": "REFUND_FLAG",
    },
    "products": {
        "table": "UOS.PRODUCTS",
        "product_id": "PRODUCT_ID",
        "product_type": "PRODUCT_TYPE",  # granular values, matched via ILIKE
    },
    "traffic": {
        "table": "GA4_API.BASE_TRAFFIC",
        "date": "CREATED_ON",            # YYYYMMDD text
        "channel": "SESSION_DEFAULT_CHANNEL_GROUPING",
        "sessions": "SESSIONS",
        "transactions": "TRANSACTIONS",
        "revenue": "PURCHASE_REVENUE",
        "engaged_sessions": "ENGAGED_SESSIONS",
    },
    "landing": {
        "table": "GA4_BQ_STG.BASE_LANDING_PAGE_STG2",
        "date": "CREATED_ON",            # TIMESTAMP_NTZ
        "page": "LANDING_PAGE_PATH",     # full URL incl. domain
        "pageviews": "PAGEVIEWS",
        "sessions": "SESSIONS",
        "transactions": "TRANSACTIONS",
        "revenue": "TRANSACTION_REVENUE",
        "engaged_sessions": "ENGAGED_SESSIONS",
    },
    "pdp": {
        "table": "UTS.PRODUCT_PAGE",
        "date": "TRAFFIC_DATE",          # TIMESTAMP_NTZ
        "sku": "PRODUCT_SKU",
        "views": "PRODUCT_DETAIL_VIEWS",
        "atc": "PRODUCT_ADDS_TO_CART",
        "checkouts": "PRODUCT_CHECKOUTS",
        "purchases": "PURCHASES",
        "revenue": "REVENUE",
    },
    "affinity": {
        # One row per same-cart basket; paired products live in the
        # secondary..quinary slot columns.
        "table": "DRP.PRODUCT_AFFINITY_SETS",
        "primary_sku": "PRIMARY_SKU",
        "order_date": "PRIMARY_ORDER_DATE",
        "slots": ["SECONDARY", "TERTIARY", "QUATERNARY", "QUINARY"],
    },
    "subs": {
        "table": "USS.ORDER_LINE_ITEMS",  # Recharge; no order dates — all-time
        "order_id": "SUBSCRIPTION_ORDER_ID",
        "sku": "SKU",
        "qty": "QUANTITY",
        "onetime_flag": "IS_ONETIME",    # FALSE = true subscription line
    },
    "plan": {
        "table": "GSHEETS.SKU_LAUNCH_DAY_FORECAST",
        "date": "DATE",                  # daily plan rows
        "sku": "SKU",
        "units": "PLAN_UNITS",
    },
}


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
         (li.{li['price']} - li.{li['discount']} / NULLIF(li.{li['qty']}, 0)) * li.{li['qty']}
           * COALESCE(li.{li['fx']}, 1) AS NET_LINE,
         o.{o['order_id']} AS ORDER_ID, o.{o['customer_id']} AS CUSTOMER_ID,
         CAST(o.{o['order_date']} AS DATE) AS ORDER_DAY
  FROM {SRC_DB}.{li['table']} li
  JOIN {SRC_DB}.{o['table']} o ON o.{o['order_id']} = li.{li['order_id']}
  WHERE li.{li['sku']} IN ({sku_list_sql(skus)})
    AND CAST(o.{o['order_date']} AS DATE) BETWEEN %(launch_date)s AND %(cutoff)s
    AND COALESCE(li.{li['refund_flag']}, FALSE) = FALSE
)"""


def q_summary(skus):
    o = COLS["orders"]
    sql = q_launch_lines_cte(skus) + f""",
first_orders AS (
  SELECT {o['customer_id']} AS CUSTOMER_ID, MIN(CAST({o['order_date']} AS DATE)) AS FIRST_ORDER_DAY
  FROM {SRC_DB}.{o['table']}
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
  FROM {SRC_DB}.{o['table']}
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


def _prior_category_cte(cat_patterns):
    """Distinct customers with a pre-launch non-refunded purchase in the
    category (matched on UOS.PRODUCTS.PRODUCT_TYPE via ILIKE patterns).

    Literal % is doubled: these queries run with bound params, so the
    connector treats single % as a pyformat placeholder."""
    o, li, pr = COLS["orders"], COLS["lines"], COLS["products"]
    pattern_match = " OR ".join(
        f"pr.{pr['product_type']} ILIKE {v}".replace("%", "%%")
        for v in str_list_sql(cat_patterns).split(", ")
    )
    return f"""
prior_category_buyers AS (
  SELECT DISTINCT o.{o['customer_id']} AS CUSTOMER_ID
  FROM {SRC_DB}.{li['table']} li
  JOIN {SRC_DB}.{o['table']} o ON o.{o['order_id']} = li.{li['order_id']}
  JOIN {SRC_DB}.{pr['table']} pr ON pr.{pr['product_id']} = li.{li['product_id']}
  WHERE ({pattern_match})
    AND CAST(o.{o['order_date']} AS DATE) < %(launch_date)s
    AND COALESCE(li.{li['refund_flag']}, FALSE) = FALSE
)"""


def q_category_customers(skus, cat_patterns):
    """New-to-category vs existing-category customers for a launch.

    Launch buyers = distinct customers with a non-refunded order containing a
    launch SKU between launch date and cutoff. 'Existing' = same customer had
    any earlier non-refunded order containing a product in the launch's
    category (full history lookback before launch date)."""
    return q_launch_lines_cte(skus) + f""",
launch_buyers AS (
  SELECT DISTINCT CUSTOMER_ID FROM launch_lines
),{_prior_category_cte(cat_patterns)}
SELECT COUNT(*)                            AS TOTAL,
       COUNT(p.CUSTOMER_ID)                AS EXISTING_CATEGORY,
       COUNT(*) - COUNT(p.CUSTOMER_ID)     AS NEW_TO_CATEGORY
FROM launch_buyers b
LEFT JOIN prior_category_buyers p ON p.CUSTOMER_ID = b.CUSTOMER_ID
"""


def q_category_by_variant(skus, cat_patterns):
    return q_launch_lines_cte(skus) + f""",
buyer_variants AS (
  SELECT DISTINCT SKU, CUSTOMER_ID FROM launch_lines
),{_prior_category_cte(cat_patterns)}
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
FROM {SRC_DB}.{c['table']}
WHERE {c['sku']} IN ({sku_list_sql(skus)})
  AND CAST({c['date']} AS DATE) BETWEEN %(launch_date)s AND %(cutoff)s
GROUP BY 1 ORDER BY PDP_VIEWS DESC
"""


def q_cross_sell(skus):
    c = COLS["affinity"]
    in_list = sku_list_sql(skus)
    slot_selects = "\n  UNION ALL\n".join(
        f"  SELECT {s}_PRODUCT_NAME AS PRODUCT, {s}_SKU AS SKU FROM baskets WHERE {s}_SKU IS NOT NULL"
        for s in c["slots"]
    )
    return f"""
WITH baskets AS (
  SELECT * FROM {SRC_DB}.{c['table']}
  WHERE {c['primary_sku']} IN ({in_list})
    AND CAST({c['order_date']} AS DATE) BETWEEN %(launch_date)s AND %(cutoff)s
),
paired AS (
{slot_selects}
)
SELECT PRODUCT, SKU, COUNT(*) AS PAIRS
FROM paired
WHERE SKU NOT IN ({in_list})
GROUP BY 1, 2 ORDER BY PAIRS DESC LIMIT 10
"""


def q_subs(skus):
    c = COLS["subs"]
    return f"""
SELECT COUNT(DISTINCT {c['order_id']}) AS SUB_ORDERS, SUM({c['qty']}) AS SUB_UNITS
FROM {SRC_DB}.{c['table']}
WHERE {c['sku']} IN ({sku_list_sql(skus)})
  AND COALESCE({c['onetime_flag']}, FALSE) = FALSE
"""


def q_plan(skus):
    c = COLS["plan"]
    return f"""
SELECT {c['sku']} AS SKU, SUM({c['units']}) AS PLAN_UNITS
FROM {SRC_DB}.{c['table']}
WHERE {c['sku']} IN ({sku_list_sql(skus)})
  AND CAST({c['date']} AS DATE) BETWEEN %(launch_date)s AND %(cutoff)s
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
FROM {SRC_DB}.{c['table']}
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
FROM {SRC_DB}.{c['table']}
WHERE TO_DATE({c['date']}, 'YYYYMMDD') BETWEEN %(traffic_start)s AND %(ga_cutoff)s
GROUP BY 1, 3 ORDER BY MONTH_START
"""


def q_landing(all_skus_and_slugs):
    c = COLS["landing"]
    # %% because this query runs with bound params (pyformat)
    like = " OR ".join(
        f"{c['page']} ILIKE '%%{s}%%'" for s in all_skus_and_slugs
    )
    return f"""
SELECT {c['page']} AS PAGE,
       SUM({c['pageviews']})    AS PAGEVIEWS,
       SUM({c['sessions']})     AS SESSIONS,
       SUM({c['transactions']}) AS TXNS,
       SUM({c['revenue']})      AS REV,
       SUM({c['engaged_sessions']}) AS ENGAGED
FROM {SRC_DB}.{c['table']}
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
    conn = snowflake.connector.connect(**{k: v for k, v in kwargs.items() if v})
    # Activate every role granted to the user, not just the primary one —
    # schema grants (e.g. DAASITY_DB.UTS) are often split across roles.
    conn.cursor().execute("USE SECONDARY ROLES ALL")
    return conn


def rows(cur, sql, params):
    # No-params queries skip pyformat entirely so literal % needs no escaping.
    cur.execute(sql, params or None)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def f2(v):
    return round(float(v or 0), 2)


_PLAN_FALLBACK = None


def plan_from_fallback(skus, launch_date, cutoff):
    """Per-SKU plan units summed from the committed GSHEETS snapshot
    (config/plan_fallback.json), for when the workflow's Snowflake user has
    no grant on the GSHEETS schema. The live query takes precedence."""
    global _PLAN_FALLBACK
    if _PLAN_FALLBACK is None:
        path = ROOT / "config" / "plan_fallback.json"
        _PLAN_FALLBACK = (json.loads(path.read_text()) if path.exists() else {})
    by_sku = _PLAN_FALLBACK.get("plan_units_by_sku_date", {})
    out = {}
    for sku in skus:
        total = sum(u for d, u in by_sku.get(sku, {}).items() if launch_date <= d <= cutoff)
        if total:
            out[sku] = total
    return out


def safe_fetch(label, fn, default):
    """Run one data-source fetch; on failure (e.g. schema not granted to the
    workflow's Snowflake user) log a warning and continue with a default so a
    single inaccessible source doesn't kill the whole refresh."""
    try:
        return fn()
    except Exception as e:
        first_line = str(e).replace("\n", " ")[:160]
        print(f"WARNING: {label} unavailable — {first_line} (continuing without it)")
        return default


def _build_category(cur, skus, cat_patterns, params, lc, sku_meta):
    cc_row = rows(cur, q_category_customers(skus, cat_patterns), params)[0]
    cc_var = rows(cur, q_category_by_variant(skus, cat_patterns), params)
    return {
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


def build_launch(cur, lc, cutoff):
    launch_date = lc["launch_date"]
    skus = [s["sku"] for s in lc["skus"]]
    sku_meta = {s["sku"]: s for s in lc["skus"]}
    params = {"launch_date": launch_date, "cutoff": cutoff}

    lid = lc["id"]
    # Core sales data (UOS) — a failure here is a real failure.
    summary_row = rows(cur, q_summary(skus), params)[0]
    variant_rows = rows(cur, q_by_variant(skus), params)
    daily_rows = rows(cur, q_daily(skus), params)
    # Optional sources degrade gracefully if the schema isn't granted.
    pdp_rows = safe_fetch(f"{lid}.pdp (UTS)", lambda: rows(cur, q_pdp(skus), params), [])
    cross_rows = safe_fetch(f"{lid}.cross_sell (DRP)", lambda: rows(cur, q_cross_sell(skus), params), [])
    subs_row = safe_fetch(f"{lid}.subscriptions (USS)", lambda: rows(cur, q_subs(skus), {})[0],
                          {"SUB_ORDERS": 0, "SUB_UNITS": 0})
    plan_rows = safe_fetch(f"{lid}.plan (GSHEETS)",
                           lambda: {r["SKU"]: int(r["PLAN_UNITS"] or 0) for r in rows(cur, q_plan(skus), params)},
                           {})
    if not plan_rows:
        plan_rows = plan_from_fallback(skus, launch_date, cutoff)
        if plan_rows:
            print(f"NOTE: {lid}.plan using committed config/plan_fallback.json "
                  f"(GSHEETS snapshot) instead of a live query.")

    cat_patterns = lc.get("category_type_patterns") or []
    cc = None
    if cat_patterns:
        cc = safe_fetch(f"{lid}.category_customers (UOS.PRODUCTS)",
                        lambda: _build_category(cur, skus, cat_patterns, params, lc, sku_meta), None)

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
        page = (r["PAGE"] or "").replace("https://", "").rstrip("/")
        slug = page.split("/")[-1].replace("-", " ").title()
        domain = page.split("/")[0].replace("thrivecausemetics", "").replace("www.", "")
        out.append({
            "label": (slug[:34] + (" (" + domain.lstrip(".") + ")" if domain else "")) or page[:40],
            "page": page,
            "pageviews": int(r["PAGEVIEWS"] or 0),
            "sessions": sessions,
            "txns": int(r["TXNS"] or 0),
            "rev": f2(r["REV"]),
            "eng": round(int(r["ENGAGED"] or 0) / sessions * 100, 1) if sessions else 0,
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
            cat_patterns = lc.get("category_type_patterns") or []
            print(f"\n-- ========== {lc['id']} ==========")
            for name, sql in [
                ("summary", q_summary(skus)), ("by_variant", q_by_variant(skus)),
                ("daily", q_daily(skus)), ("pdp", q_pdp(skus)),
                ("cross_sell", q_cross_sell(skus)), ("subs", q_subs(skus)),
                ("plan", q_plan(skus)),
                ("category_customers", q_category_customers(skus, cat_patterns) if cat_patterns else "-- no category patterns configured"),
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
        launches = [build_launch(cur, lc, cutoff) for lc in with_skus]
        launches += [pending_launch(lc) for lc in no_skus + too_new]
        traffic = safe_fetch("traffic (GA4_API)", lambda: build_traffic(cur, params),
                             {"byChannel": [], "monthly": []})
        landing = safe_fetch("landing (GA4_BQ_STG)", lambda: build_landing(cur, cfg, params), [])
    finally:
        conn.close()

    data = {
        "meta": {
            "dataCutoff": cutoff,
            "gaCutoff": ga_cutoff,
            "trafficStart": traffic_start,
            "generatedAt": str(today),
            "mode": "automated",
            "sourceDb": SRC_DB,
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
