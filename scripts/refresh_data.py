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
# Trailing window behind the out-of-stock projection. Seven days smooths the
# weekday/weekend swing without letting launch-week spikes dominate.
OOS_WINDOW_DAYS = 7

LEARNINGS = {}
ASANA_LINKS = {}
NPD_OOS = {}      # Demand Planning's own OOS date and on-hand units, by SKU

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
    "inventory": {
        # One row per SKU per fulfillment location. AVAILABLE_QUANTITY equals
        # INVENTORY_QUANTITY in practice; COMMITTED/IN_TRANSIT/ON_ORDER are
        # not populated by the sync, so they are deliberately not used.
        "table": "UOS.INVENTORY_LEVELS",
        "sku": "SKU",
        "location": "LOCATION_ID",
        "available": "AVAILABLE_QUANTITY",
        "date": "INVENTORY_DATE",
        # Which storefront the location belongs to. The Canadian warehouse
        # (Ontario) syncs under thrive-causemetics-canada; every US location
        # (Memphis, Las Vegas, HQ, the pop-up) under thrive-causemetics.
        "shop": "__SHOP_ID",
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
         CAST(o.{o['order_date']} AS DATE) AS ORDER_DAY,
         CASE WHEN o.ORIGINAL_CURRENCY = 'CAD' THEN 'CA' ELSE 'US' END AS REGION
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
  COUNT(DISTINCT CASE WHEN fo.FIRST_ORDER_DAY >= %(launch_date)s THEN ll.CUSTOMER_ID END) AS NEW_CUSTOMERS,
  SUM(CASE WHEN fo.FIRST_ORDER_DAY >= %(launch_date)s THEN ll.NET_LINE ELSE 0 END) AS NEW_NET_SALES,
  SUM(CASE WHEN ll.REGION = 'US' THEN ll.QTY ELSE 0 END)      AS US_UNITS,
  SUM(CASE WHEN ll.REGION = 'CA' THEN ll.QTY ELSE 0 END)      AS CA_UNITS,
  SUM(CASE WHEN ll.REGION = 'US' THEN ll.NET_LINE ELSE 0 END) AS US_NET_SALES,
  SUM(CASE WHEN ll.REGION = 'CA' THEN ll.NET_LINE ELSE 0 END) AS CA_NET_SALES,
  COUNT(DISTINCT CASE WHEN ll.REGION = 'US' THEN ll.ORDER_ID END) AS US_ORDERS,
  COUNT(DISTINCT CASE WHEN ll.REGION = 'CA' THEN ll.ORDER_ID END) AS CA_ORDERS
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
       COUNT(DISTINCT CASE WHEN fo.FIRST_ORDER_DAY <  %(launch_date)s THEN ll.CUSTOMER_ID END) AS RET_CUSTOMERS,
       SUM(CASE WHEN ll.REGION = 'US' THEN ll.QTY ELSE 0 END) AS US_UNITS,
       SUM(CASE WHEN ll.REGION = 'CA' THEN ll.QTY ELSE 0 END) AS CA_UNITS,
       SUM(CASE WHEN ll.REGION = 'US' THEN ll.NET_LINE ELSE 0 END) AS US_NET_SALES,
       SUM(CASE WHEN ll.REGION = 'CA' THEN ll.NET_LINE ELSE 0 END) AS CA_NET_SALES
FROM launch_lines ll
LEFT JOIN first_orders fo ON fo.CUSTOMER_ID = ll.CUSTOMER_ID
GROUP BY 1 ORDER BY NET_SALES DESC
"""


def q_daily(skus):
    """Day x SKU.

    Carries the SKU dimension because the dashboard filters the daily trend
    and the daily table by shade, and a launch-level-only series meant those
    kept showing the whole launch while everything above them was filtered.
    Launch-level totals are rolled up from these rows in Python — units and
    revenue are additive, so querying them a second time would be waste.

    New/returning counts here are per SKU per day: a customer buying two
    shades on one day is counted in each. That is the same treatment the
    per-variant totals already get, and the filter bar says so. The exact
    launch-level split comes from q_daily_customers() below.
    """
    o = COLS["orders"]
    return q_launch_lines_cte(skus) + f""",
first_orders AS (
  SELECT {o['customer_id']} AS CUSTOMER_ID, MIN(CAST({o['order_date']} AS DATE)) AS FIRST_ORDER_DAY
  FROM {SRC_DB}.{o['table']}
  GROUP BY 1
)
SELECT ll.ORDER_DAY AS D, ll.SKU AS SKU,
       SUM(ll.QTY) AS UNITS, SUM(ll.NET_LINE) AS NET_SALES,
       SUM(CASE WHEN ll.REGION = 'US' THEN ll.QTY ELSE 0 END)      AS US_UNITS,
       SUM(CASE WHEN ll.REGION = 'CA' THEN ll.QTY ELSE 0 END)      AS CA_UNITS,
       SUM(CASE WHEN ll.REGION = 'US' THEN ll.NET_LINE ELSE 0 END) AS US_NET_SALES,
       SUM(CASE WHEN ll.REGION = 'CA' THEN ll.NET_LINE ELSE 0 END) AS CA_NET_SALES,
       COUNT(DISTINCT CASE WHEN fo.FIRST_ORDER_DAY >= %(launch_date)s THEN ll.CUSTOMER_ID END) AS NEW_CUSTOMERS,
       COUNT(DISTINCT CASE WHEN fo.FIRST_ORDER_DAY <  %(launch_date)s THEN ll.CUSTOMER_ID END) AS RET_CUSTOMERS
FROM launch_lines ll
LEFT JOIN first_orders fo ON fo.CUSTOMER_ID = ll.CUSTOMER_ID
GROUP BY 1, 2 ORDER BY 1, 2
"""


def q_daily_customers(skus):
    """Launch-level new vs returning buyers per day.

    Distinct customer counts are not additive across SKUs, so this cannot be
    summed out of q_daily(). It answers "of the people who bought on day N,
    what share had never bought from us before" — the same new/returning
    definition as the donut, sliced by day instead of over the whole launch.
    """
    o = COLS["orders"]
    return q_launch_lines_cte(skus) + f""",
first_orders AS (
  SELECT {o['customer_id']} AS CUSTOMER_ID, MIN(CAST({o['order_date']} AS DATE)) AS FIRST_ORDER_DAY
  FROM {SRC_DB}.{o['table']}
  GROUP BY 1
)
SELECT ll.ORDER_DAY AS D,
       COUNT(DISTINCT ll.CUSTOMER_ID) AS TOTAL_CUSTOMERS,
       COUNT(DISTINCT CASE WHEN fo.FIRST_ORDER_DAY >= %(launch_date)s THEN ll.CUSTOMER_ID END) AS NEW_CUSTOMERS,
       COUNT(DISTINCT CASE WHEN fo.FIRST_ORDER_DAY <  %(launch_date)s THEN ll.CUSTOMER_ID END) AS RET_CUSTOMERS
FROM launch_lines ll
LEFT JOIN first_orders fo ON fo.CUSTOMER_ID = ll.CUSTOMER_ID
GROUP BY 1 ORDER BY 1
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


def q_category_daily(skus, cat_patterns):
    """New-to-category vs existing-category buyers per day.

    'Existing' is judged against the pre-launch history once, not per day, so
    a customer who is new to the category on day 3 stays counted as new on
    day 3 even if they bought the category again on day 40. The question is
    what each day's buyers looked like when they arrived.
    """
    return q_launch_lines_cte(skus) + f""",
buyer_days AS (
  SELECT DISTINCT ORDER_DAY, CUSTOMER_ID FROM launch_lines
),{_prior_category_cte(cat_patterns)}
SELECT bd.ORDER_DAY                        AS D,
       COUNT(*)                            AS TOTAL,
       COUNT(*) - COUNT(p.CUSTOMER_ID)     AS NEW_TO_CATEGORY,
       COUNT(p.CUSTOMER_ID)                AS EXISTING_CATEGORY
FROM buyer_days bd
LEFT JOIN prior_category_buyers p ON p.CUSTOMER_ID = bd.CUSTOMER_ID
GROUP BY 1 ORDER BY 1
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


def q_pdp_windows(skus):
    """Add-to-cart rate over the trailing week and the week before it.

    The launch-to-date PDP figures cannot answer "is this getting worse".
    A shade that has always converted at 4% is a merchandising fact; one that
    fell from 12% to 4% last week is something that happened, and only the
    second is worth waking anyone up for.
    """
    c = COLS["pdp"]
    return f"""
SELECT {c['sku']} AS SKU,
       SUM(CASE WHEN CAST({c['date']} AS DATE) > %(pdp_mid)s THEN {c['views']} ELSE 0 END) AS RECENT_VIEWS,
       SUM(CASE WHEN CAST({c['date']} AS DATE) > %(pdp_mid)s THEN {c['atc']}   ELSE 0 END) AS RECENT_ATC,
       SUM(CASE WHEN CAST({c['date']} AS DATE) <= %(pdp_mid)s THEN {c['views']} ELSE 0 END) AS PRIOR_VIEWS,
       SUM(CASE WHEN CAST({c['date']} AS DATE) <= %(pdp_mid)s THEN {c['atc']}   ELSE 0 END) AS PRIOR_ATC
FROM {SRC_DB}.{c['table']}
WHERE {c['sku']} IN ({sku_list_sql(skus)})
  AND CAST({c['date']} AS DATE) BETWEEN %(pdp_start)s AND %(pdp_end)s
GROUP BY 1
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
SELECT COUNT(DISTINCT {c['order_id']}) AS SUB_ORDERS, SUM({c['qty']}) AS SUB_UNITS,
       SUM(UNIT_PRICE * {c['qty']}) AS SUB_REVENUE
FROM {SRC_DB}.{c['table']}
WHERE {c['sku']} IN ({sku_list_sql(skus)})
  AND COALESCE({c['onetime_flag']}, FALSE) = FALSE
"""


def q_plan(skus):
    # Daily grain: per-SKU totals AND the cumulative plan curve both derive
    # from this one result.
    c = COLS["plan"]
    return f"""
SELECT {c['sku']} AS SKU, CAST({c['date']} AS DATE) AS D, SUM({c['units']}) AS PLAN_UNITS
FROM {SRC_DB}.{c['table']}
WHERE {c['sku']} IN ({sku_list_sql(skus)})
  AND CAST({c['date']} AS DATE) BETWEEN %(launch_date)s AND %(cutoff)s
GROUP BY 1, 2 ORDER BY 2
"""


def q_plan_full(skus):
    """Whole-curve plan per SKU — deliberately NOT bounded by the cutoff.

    q_plan() stops at the cutoff, which is right for "% to plan so far" but
    cannot answer "how many units are still owed against the goal". The
    forecast is a full 120-day curve per SKU, so summing it unbounded gives
    the launch goal, and its last date is the real end of the decay curve
    rather than a launch_date + N guess.
    """
    c = COLS["plan"]
    return f"""
SELECT {c['sku']} AS SKU, SUM({c['units']}) AS PLAN_TOTAL,
       MAX(CAST({c['date']} AS DATE)) AS PLAN_END
FROM {SRC_DB}.{c['table']}
WHERE {c['sku']} IN ({sku_list_sql(skus)})
GROUP BY 1
"""


def q_cross_sell_by_sku(skus):
    """Same pairings as q_cross_sell but kept split by the launch SKU that
    drove the basket, so the Cross-Sells tab can filter by shade. The
    launch-wide list stays a separate exact query: rolling these per-shade
    top tens back up would miss a product that ranks 11th for every shade.
    """
    c = COLS["affinity"]
    in_list = sku_list_sql(skus)
    slot_selects = "\n  UNION ALL\n".join(
        f"  SELECT {c['primary_sku']} AS PRIMARY_SKU, {s}_PRODUCT_NAME AS PRODUCT, "
        f"{s}_SKU AS SKU FROM baskets WHERE {s}_SKU IS NOT NULL"
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
SELECT PRIMARY_SKU, PRODUCT, SKU, COUNT(*) AS PAIRS
FROM paired
WHERE SKU NOT IN ({in_list})
GROUP BY 1, 2, 3
QUALIFY ROW_NUMBER() OVER (PARTITION BY PRIMARY_SKU ORDER BY COUNT(*) DESC) <= 10
ORDER BY PRIMARY_SKU, PAIRS DESC
"""


def q_inventory(skus):
    """Currently available units per SKU, split US vs Canada.

    The table holds a rolling snapshot, so keep only the newest row per
    (SKU, location) rather than summing every snapshot ever synced.

    Region comes from the storefront the location syncs under, not from the
    location name: names are free text and change, __SHOP_ID is what the sync
    keys on. Anything that is not the Canadian shop counts as US, so a new US
    warehouse is included the day it appears rather than silently dropped.
    """
    c = COLS["inventory"]
    return f"""
SELECT SKU,
       SUM(AVAILABLE)                                        AS AVAILABLE,
       SUM(CASE WHEN REGION = 'US' THEN AVAILABLE ELSE 0 END) AS US_AVAILABLE,
       SUM(CASE WHEN REGION = 'CA' THEN AVAILABLE ELSE 0 END) AS CA_AVAILABLE
FROM (
  SELECT {c['sku']} AS SKU, COALESCE({c['available']}, 0) AS AVAILABLE,
         CASE WHEN {c['shop']} ILIKE '%%canada%%' THEN 'CA' ELSE 'US' END AS REGION
  FROM {SRC_DB}.{c['table']}
  WHERE {c['sku']} IN ({sku_list_sql(skus)})
  QUALIFY ROW_NUMBER() OVER (
            PARTITION BY {c['sku']}, {c['location']}
            ORDER BY {c['date']} DESC) = 1
)
GROUP BY 1
"""


def q_trailing_units(skus):
    """Units sold per SKU over the trailing window ending at the cutoff.

    This is the sell-through rate behind the out-of-stock projection — recent
    demand, not launch-to-date average, which decays badly after launch week.
    """
    return q_launch_lines_cte(skus) + """
SELECT SKU, SUM(QTY) AS UNITS,
       SUM(CASE WHEN REGION = 'US' THEN QTY ELSE 0 END) AS US_UNITS,
       SUM(CASE WHEN REGION = 'CA' THEN QTY ELSE 0 END) AS CA_UNITS
FROM launch_lines
WHERE ORDER_DAY BETWEEN %(oos_start)s AND %(cutoff)s
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
  -- Content pages only. Substring matching otherwise picks up order URLs and
  -- web-pixel endpoints whose hex hashes happen to contain the match term
  -- ("becca" inside .../orders/71becca7fcc...), which are not landing pages.
  AND ({c['page']} ILIKE '%%/products/%%' OR {c['page']} ILIKE '%%/pages/%%')
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


class Compact:
    """Marks a value to be written on one line, ignoring the output indent."""

    __slots__ = ("value",)

    def __init__(self, value):
        self.value = value


def dump_js(data):
    """json.dumps(indent=2), except Compact() values stay on a single line.

    The per-SKU daily series is a couple of thousand rows of six numbers.
    Pretty-printed at indent=2 that is fifteen thousand lines and roughly
    triples the file every browser downloads on page load, to make a number
    grid nobody reads marginally easier to read. Everything a human might
    actually open this file to check stays indented.
    """
    blobs = []

    def default(o):
        if isinstance(o, Compact):
            blobs.append(json.dumps(o.value, default=str))
            return f"@@COMPACT{len(blobs) - 1}@@"
        return str(o)

    out = json.dumps(data, indent=2, default=default)
    for i, blob in enumerate(blobs):
        out = out.replace(f'"@@COMPACT{i}@@"', blob)
    return out


_PLAN_FALLBACK = None


def _plan_fallback_by_sku():
    """Committed GSHEETS snapshot (config/plan_fallback.json), loaded once."""
    global _PLAN_FALLBACK
    if _PLAN_FALLBACK is None:
        path = ROOT / "config" / "plan_fallback.json"
        _PLAN_FALLBACK = (json.loads(path.read_text()) if path.exists() else {})
    return _PLAN_FALLBACK.get("plan_units_by_sku_date", {})


def plan_detail_from_fallback(skus, launch_date, cutoff):
    """Per-SKU daily plan rows [(sku, date, units)] from the committed GSHEETS
    snapshot, for when the workflow's Snowflake user has no grant on the
    GSHEETS schema. The live query takes precedence."""
    by_sku = _plan_fallback_by_sku()
    out = []
    for sku in skus:
        for d, u in by_sku.get(sku, {}).items():
            if launch_date <= d <= cutoff:
                out.append((sku, d, u))
    return out


LEARNINGS_PATH = ROOT / "config" / "learnings.json"


def load_learnings():
    """Retro learnings keyed by launch id, or {}.

    Kept in the repo rather than a database because this dashboard's container
    has no write path — see the note in config/learnings.json.
    """
    if not LEARNINGS_PATH.exists():
        return {}
    try:
        return (json.loads(LEARNINGS_PATH.read_text()).get("byLaunch") or {})
    except (json.JSONDecodeError, OSError) as e:
        print(f"WARNING: {LEARNINGS_PATH.name} unreadable ({e}); continuing without learnings")
        return {}


NPD_OOS_PATH = ROOT / "config" / "npd_oos_tracking.json"


def load_npd_oos():
    """Demand Planning's decay-curve OOS and on-hand units, keyed by SKU.

    Written by scripts/sync_npd_oos.py from the NPD OOS Tracking sheet. Shown
    next to the Snowflake-derived figures rather than replacing them: the two
    disagree, and which one is right is Demand Planning's call.
    """
    if not NPD_OOS_PATH.exists():
        return {}, None
    try:
        d = json.loads(NPD_OOS_PATH.read_text())
        return (d.get("bySku") or {}), d.get("syncedAt")
    except (json.JSONDecodeError, OSError) as e:
        print(f"WARNING: {NPD_OOS_PATH.name} unreadable ({e}); "
              f"continuing without Demand Planning's figures")
        return {}, None


CALENDAR_PATH = ROOT / "config" / "launch_calendar.json"


def load_launch_calendar():
    """The Asana marketing calendar snapshot, or None.

    Written by scripts/sync_launch_calendar.py. Read here rather than calling
    Asana directly so the Snowflake refresh never depends on Asana being
    reachable, and so every date change is visible in git history.
    """
    if not CALENDAR_PATH.exists():
        return None
    try:
        cal = json.loads(CALENDAR_PATH.read_text())
        return cal if cal.get("entries") else None
    except (json.JSONDecodeError, OSError) as e:
        print(f"WARNING: {CALENDAR_PATH.name} unreadable ({e}); "
              f"falling back to the upcoming list in launches.json")
        return None


def plan_totals_from_fallback(skus):
    """Whole-curve plan totals and curve end date from the committed snapshot,
    for when the workflow's Snowflake user has no GSHEETS grant. Unbounded by
    date on purpose — this is the goal, not progress against it."""
    by_sku = _plan_fallback_by_sku()
    out = {}
    for sku in skus:
        days = by_sku.get(sku) or {}
        if days:
            out[sku] = (sum(int(v or 0) for v in days.values()), max(days))
    return out


# Per-source outcome across the whole run: 'live', 'fallback', or
# 'unavailable'. Written to meta.sourceStatus so the dashboard's Data Guide
# can show which sources the refresh could actually reach.
SOURCE_STATUS = {"uos": "live"}


def _mark_source(key, ok):
    if not key:
        return
    if not ok:
        SOURCE_STATUS[key] = "unavailable"
    elif SOURCE_STATUS.get(key) != "unavailable":
        SOURCE_STATUS[key] = "live"


def safe_fetch(label, fn, default, key=None):
    """Run one data-source fetch; on failure (e.g. schema not granted to the
    workflow's Snowflake user) log a warning and continue with a default so a
    single inaccessible source doesn't kill the whole refresh."""
    try:
        result = fn()
        _mark_source(key, True)
        return result
    except Exception as e:
        first_line = str(e).replace("\n", " ")[:160]
        print(f"WARNING: {label} unavailable — {first_line} (continuing without it)")
        _mark_source(key, False)
        return default


def _build_category(cur, skus, cat_patterns, params, lc, sku_meta):
    cc_row = rows(cur, q_category_customers(skus, cat_patterns), params)[0]
    cc_var = rows(cur, q_category_by_variant(skus, cat_patterns), params)
    cc_daily = rows(cur, q_category_daily(skus, cat_patterns), params)
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
        # [date, newToCategory, existingCategory] per day, for the trend view.
        "daily": Compact([[str(r["D"]), int(r["NEW_TO_CATEGORY"] or 0),
                           int(r["EXISTING_CATEGORY"] or 0)] for r in cc_daily]),
    }


# ── LAUNCH SIGNALS ──
# Rule-based, deliberately NOT a language model. These sit beside numbers merch
# and finance act on, so they must be deterministic, reproducible from the same
# data.js, and incapable of inventing a figure. Every signal quotes the values
# it fired on so a reader can check it against the tables.
#
# Computed here rather than in the browser so the dashboard panel and the Slack
# alert are the same code path. Two implementations of the same thresholds
# would drift, and an alert that disagrees with the page it links to is worse
# than no alert.
SIGNAL_RULES = {
    "oosSoonDays": 21,        # flag SKUs projected out of stock within this window
    "atcDropPts": 2.0,        # percentage-point fall in ATC rate, week over week
    "atcMinMissedAtc": 500,   # ...costing at least this many add-to-carts
    "atcMinViews": 1000,      # ...on at least this many views in each week
    "recentWindowDays": 7,    # trailing window for "how is it going lately"
    "recentBehindPct": 85,    # recent units below this share of recent plan
    "recentAheadPct": 115,
    "minRecentPlanUnits": 50,  # ignore attainment on a plan too small to mean anything
    # Per-rule cap. Without it one rule fires five near-identical lines and
    # crowds every other signal type out of the group.
    "maxPerRule": 2,
}


def _num(v):
    return f"{int(round(float(v or 0))):,}"


def _money(v):
    return f"${int(round(float(v or 0))):,}"


def _date(iso):
    if not iso:
        return ""
    return dt.date.fromisoformat(str(iso)[:10]).strftime("%b %-d, %Y")


REGION_LABEL = {"us": "US (.com)", "ca": "Canada (.ca)"}


def _stock_signals(by_variant, R):
    """Stock alerts, per storefront.

    Stock does not move between the two, so a combined alert can read
    comfortable while one side is empty — and, worse, sends someone to solve a
    demand problem when the real one is where the pallets are. Each region is
    judged on its own units and its own sell-through.
    """
    out = []
    for v in by_variant:
        for rg in ("us", "ca"):
            units = v.get(f"{rg}InventoryUnits")
            if units is None:
                continue
            days = v.get(f"{rg}DaysToOOS")
            label = REGION_LABEL[rg]
            other = "ca" if rg == "us" else "us"
            other_units = v.get(f"{other}InventoryUnits")
            # A SKU healthy on the other side is a distribution problem, not a
            # demand one, and the alert should say which.
            lopsided = (other_units or 0) > 0 and units == 0
            if units == 0:
                out.append({
                    "rank": 0, "key": f"oos:{rg}:{v['sku']}",
                    "title": f"{v['name']} is out of stock in {label}",
                    "detail": f"0 units in {label}."
                              + (f" {_num(other_units)} units still in {REGION_LABEL[other]}, so this is a "
                                 f"distribution problem, not a demand one." if lopsided else ""),
                    "action": (f"Route or transfer stock to {label} — the units exist, they are in the wrong place."
                               if lopsided else f"Reorder for {label}."),
                })
            elif days is not None and 0 < days <= R["oosSoonDays"]:
                rate = v.get(f"{rg}RunRateUnitsPerDay")
                weeks = round(units / rate / 7, 1) if rate else None
                out.append({
                    "rank": 1, "key": f"oos-soon:{rg}:{v['sku']}",
                    "title": f"{v['name']} runs out in {label} in ~{days} days",
                    "detail": f"{_num(units)} units left in {label} at {rate}/day"
                              + (f" ({weeks} weeks cover)." if weeks is not None else "."),
                    "action": f"Confirm a replenishment date for {label} with Demand Planning, "
                              f"or plan for the shade to go dark there.",
                })
    # Out-of-stock first, then soonest.
    out.sort(key=lambda a: (a["rank"], a["title"]))
    by_rank = {}
    for a in out:
        by_rank.setdefault(a["rank"], []).append(a)
    capped = []
    for rank in sorted(by_rank):
        capped.extend(by_rank[rank][: R["maxPerRule"] * 2])
    return capped


def compute_signals(by_variant, pdp, daily, plan_daily, cross_sell, cc,
                    daily_by_sku=None, plan_by_sku=None, pdp_windows=None):
    """Returns {"attention": [...], "working": [...]}, attention ranked.

    Every attention signal carries an `action` — the alert is only worth
    sending if somebody can do something about it, and writing the action down
    forces that test at the point the rule is written rather than leaving it to
    whoever reads the message.

    Ranks are the alert priority order: 0 out of stock, 1 running out soon,
    2 conversion falling, 3 recent attainment, 4 launch pacing.
    """
    R, attention, working = SIGNAL_RULES, [], []
    asp = {v["sku"]: (v["netSales"] / v["units"] if v.get("units") else 0) for v in by_variant}
    name = {v["sku"]: v["name"] for v in by_variant}

    attention.extend(_stock_signals(by_variant, R))

    # ── Conversion, week over week ──────────────────────────────────────────
    # A level ("below the launch average") is arithmetic: on a seven-shade
    # launch five shades are below the mean by definition, so it fires forever
    # and means nothing. A fall is an event.
    for w in (pdp_windows or []):
        rv, ra = w["recentViews"], w["recentAtc"]
        pv, pa = w["priorViews"], w["priorAtc"]
        if min(rv, pv) < R["atcMinViews"]:
            continue
        recent_rate, prior_rate = ra / rv * 100, pa / pv * 100
        drop = prior_rate - recent_rate
        missed = round(rv * drop / 100)
        # Two gates, and both are needed. The point drop says the rate really
        # moved rather than wobbled; the missed add-to-carts say it cost enough
        # to be worth someone's morning. A two-point fall on a thousand views
        # is twenty carts, which is noise wearing a percentage.
        if drop < R["atcDropPts"] or missed < R["atcMinMissedAtc"]:
            continue
        # Valued at this SKU's own ATC-to-purchase rate and average selling
        # price, so the figure is the SKU's own behaviour rather than a
        # launch-wide assumption applied to it.
        row = next((r for r in pdp if r["sku"] == w["sku"]), None)
        conv = (row["purch"] / row["atc"]) if row and row.get("atc") else 0
        value = missed * conv * asp.get(w["sku"], 0)
        attention.append({
            "rank": 2, "key": f"atc-drop:{w['sku']}", "missedAtc": missed,
            "title": f"{name.get(w['sku'], w['sku'])} add-to-cart rate fell to {recent_rate:.1f}%",
            "detail": f"Down {drop:.1f} points from {prior_rate:.1f}% the week before, on "
                      f"{_num(rv)} views — about {_num(missed)} fewer add-to-carts"
                      + (f", roughly {_money(value)} at this shade's own purchase rate and price." if value > 0 else "."),
            "action": "Check whether anything changed on the page or in the traffic mix — "
                      "creative, price, promo placement, or a paid source sending colder traffic.",
        })
    # Keep the costliest, not the first alphabetically.
    for extra in sorted([a for a in attention if a["rank"] == 2],
                        key=lambda a: -a["missedAtc"])[R["maxPerRule"]:]:
        attention.remove(extra)

    # ── Recent attainment, per SKU ──────────────────────────────────────────
    # Cumulative attainment keeps a SKU flagged for a stumble in week one long
    # after it recovered. What matters is the last seven days.
    recent_by_sku = _recent_attainment(daily_by_sku, plan_by_sku, R["recentWindowDays"])
    behind = sorted(
        [(sku, u, p) for sku, (u, p) in recent_by_sku.items()
         if p >= R["minRecentPlanUnits"] and u / p * 100 < R["recentBehindPct"]],
        key=lambda x: x[1] / x[2])
    for sku, u, p in behind[: R["maxPerRule"]]:
        short = p - u
        attention.append({
            "rank": 3, "key": f"recent-behind:{sku}",
            "title": f"{name.get(sku, sku)} is behind plan over the last {R['recentWindowDays']} days",
            "detail": f"{_num(u)} units against {_num(p)} planned ({u / p * 100:.0f}%), "
                      f"{_num(short)} short — about {_money(short * asp.get(sku, 0))} at its current price.",
            "action": "Recent, not cumulative — this is the last week only. Worth checking "
                      "against promo calendar and paid support before treating it as demand.",
        })
    for sku, (u, p) in recent_by_sku.items():
        if p >= R["minRecentPlanUnits"] and u / p * 100 >= R["recentAheadPct"]:
            working.append({
                "key": f"recent-ahead:{sku}",
                "title": f"{name.get(sku, sku)} is ahead of plan over the last {R['recentWindowDays']} days",
                "detail": f"{_num(u)} units against {_num(p)} planned ({u / p * 100:.0f}%).",
            })

    # ── Launch-level pacing ─────────────────────────────────────────────────
    recent = [(r["units"], plan_daily[r["date"]]) for r in daily[-R["recentWindowDays"]:]
              if r["date"] in plan_daily]
    if len(recent) == R["recentWindowDays"]:
        au, ap = sum(x[0] for x in recent), sum(x[1] for x in recent)
        pct = (au / ap * 100) if ap > 0 else None
        if pct is not None and pct < R["recentBehindPct"]:
            attention.append({
                "rank": 4, "key": "pacing",
                "title": f"The whole launch is pacing behind plan",
                "detail": f"{_num(au)} units vs {_num(ap)} planned over the last "
                          f"{R['recentWindowDays']} days ({pct:.0f}%). "
                          "Cumulative attainment can stay green while recent days slip.",
                "action": "Launch-wide rather than one shade — look at traffic and promo "
                          "support before shade-level merchandising.",
            })
        elif pct is not None:
            working.append({
                "key": "pacing",
                "title": f"Last {R['recentWindowDays']} days are on or above plan",
                "detail": f"{_num(au)} units vs {_num(ap)} planned ({pct:.0f}%).",
            })

    # ── Cross-sell and category acquisition ────────────────────────────────
    top = (cross_sell or [{}])[0]
    if top.get("pairs", 0) > 0:
        working.append({
            "key": f"pairing:{top.get('sku')}",
            "title": f"Most common basket pairing: {top['product']}",
            "detail": f"{_num(top['pairs'])} same-cart orders. A bundle or PDP cross-sell "
                      "placement is the obvious test.",
        })

    if cc and (cc.get("total") or 0) > 0:
        pct = cc["newToCategory"] / cc["total"] * 100
        working.append({
            "key": "new-to-category",
            "title": f"{pct:.0f}% of buyers are new to {cc['category']}",
            "detail": f"{_num(cc['newToCategory'])} of {_num(cc['total'])} buyers had not "
                      "purchased this category before.",
        })

    attention.sort(key=lambda s: s["rank"])
    return {"attention": attention, "working": working}


def _recent_attainment(daily_by_sku, plan_by_sku, window):
    """{sku: (units, plan_units)} over the last `window` days with plan data.

    Returns nothing when the per-SKU series is missing, which is honest: with
    no per-SKU plan there is no attainment to judge, and a launch with no
    forecast at all should raise no attainment alerts rather than look healthy.
    """
    if not daily_by_sku or not plan_by_sku:
        return {}
    out = {}
    for sku, rows in daily_by_sku.items():
        plan = dict(plan_by_sku.get(sku) or [])
        if not plan:
            continue
        recent = rows[-window:]
        if len(recent) < window:
            continue
        u = sum(r[1] for r in recent)
        p = sum(plan.get(r[0], 0) for r in recent)
        if p > 0:
            out[sku] = (u, p)
    return out


def project_oos(avail, run_rate, cutoff):
    """(days_to_oos, iso_date) for available units at a sell-through rate.

    An assumption, not a forecast: it holds demand steady and assumes no
    replenishment arrives. Returns (None, None) when there is nothing honest
    to project from — no inventory figure at all, or no recent sales.
    """
    if avail is None:
        return None, None                      # inventory source unavailable
    if avail <= 0:
        return 0, cutoff                       # already out of stock
    if run_rate > 0:
        days = int(avail // run_rate)
        return days, str(dt.date.fromisoformat(cutoff) + dt.timedelta(days=days))
    return None, None                          # no recent sales, no runway
def build_launch(cur, lc, cutoff, ga_cutoff, full=True):
    launch_date = lc["launch_date"]
    skus = [s["sku"] for s in lc["skus"]]
    sku_meta = {s["sku"]: s for s in lc["skus"]}
    params = {"launch_date": launch_date, "cutoff": cutoff}
    # Trailing window for the OOS projection, clamped to the launch date so a
    # launch younger than the window isn't credited with pre-launch days.
    oos_start = max(launch_date,
                    str(dt.date.fromisoformat(cutoff) - dt.timedelta(days=OOS_WINDOW_DAYS - 1)))
    oos_days = (dt.date.fromisoformat(cutoff) - dt.date.fromisoformat(oos_start)).days + 1
    params["oos_start"] = oos_start
    # Two adjacent seven-day windows ending at the GA cutoff, for the
    # week-over-week conversion comparison. Clamped to the launch date so a
    # launch younger than a fortnight compares only real days.
    pdp_end = min(ga_cutoff, cutoff)
    pdp_start = max(launch_date, str(dt.date.fromisoformat(pdp_end) - dt.timedelta(days=13)))
    params["pdp_end"] = pdp_end
    params["pdp_start"] = pdp_start
    params["pdp_mid"] = str(dt.date.fromisoformat(pdp_end) - dt.timedelta(days=6, seconds=0)
                            - dt.timedelta(days=1))

    lid = lc["id"]
    # Core sales data (UOS) — a failure here is a real failure.
    summary_row = rows(cur, q_summary(skus), params)[0]
    variant_rows = rows(cur, q_by_variant(skus), params)
    daily_rows = rows(cur, q_daily(skus), params)
    daily_customer_rows = rows(cur, q_daily_customers(skus), params)
    # Optional sources degrade gracefully if the schema isn't granted.
    # Archived launches (past the decay curve) keep sales, variants, daily and
    # plan so the Historical view can render, but skip the per-tab detail: those
    # figures cannot change any more and re-querying them daily is pure cost.
    if full:
        pdp_rows = safe_fetch(f"{lid}.pdp (UTS)", lambda: rows(cur, q_pdp(skus), params), [], key="uts")
        pdp_window_rows = safe_fetch(f"{lid}.pdp_windows (UTS)",
                                     lambda: rows(cur, q_pdp_windows(skus), params), [], key="uts")
        cross_rows = safe_fetch(f"{lid}.cross_sell (DRP)", lambda: rows(cur, q_cross_sell(skus), params), [], key="drp")
        cross_sku_rows = safe_fetch(f"{lid}.cross_sell_by_sku (DRP)",
                                    lambda: rows(cur, q_cross_sell_by_sku(skus), params), [], key="drp")
        subs_row = safe_fetch(f"{lid}.subscriptions (USS)", lambda: rows(cur, q_subs(skus), {})[0],
                              {"SUB_ORDERS": 0, "SUB_UNITS": 0, "SUB_REVENUE": None}, key="uss")
        inv_rows = safe_fetch(f"{lid}.inventory (UOS.INVENTORY_LEVELS)",
                              lambda: rows(cur, q_inventory(skus), {}), [], key="uos_inventory")
        trail_rows = safe_fetch(f"{lid}.trailing_units (UOS)",
                                lambda: rows(cur, q_trailing_units(skus), params), [], key="uos_inventory")
        # GA4 window for THIS launch. Previously both of these ran once
        # globally from the earliest active launch date, so every launch showed
        # the same April-onwards numbers and every launch's landing pages.
        ga_params = {"traffic_start": launch_date, "ga_cutoff": ga_cutoff}
        traffic = safe_fetch(f"{lid}.traffic (GA4_API)",
                             lambda: build_traffic(cur, ga_params),
                             {"byChannel": [], "monthly": []}, key="ga4_api")
        landing = safe_fetch(f"{lid}.landing (GA4_BQ_STG)",
                             lambda: build_landing(cur, lc, ga_params), [], key="ga4_bq")
    else:
        pdp_rows, cross_rows, cross_sku_rows, inv_rows, trail_rows = [], [], [], [], []
        pdp_window_rows = []
        subs_row = {"SUB_ORDERS": 0, "SUB_UNITS": 0, "SUB_REVENUE": None}
        traffic, landing = {"byChannel": [], "monthly": []}, []
    plan_detail = safe_fetch(f"{lid}.plan (GSHEETS)",
                             lambda: [(r["SKU"], str(r["D"]), int(r["PLAN_UNITS"] or 0))
                                      for r in rows(cur, q_plan(skus), params)],
                             [], key="gsheets")
    if not plan_detail:
        plan_detail = plan_detail_from_fallback(skus, launch_date, cutoff)
        if plan_detail:
            SOURCE_STATUS["gsheets"] = "fallback"
            print(f"NOTE: {lid}.plan using committed config/plan_fallback.json "
                  f"(GSHEETS snapshot) instead of a live query.")
    # Whole-curve goal, separate from the to-date plan above. Without this the
    # dashboard can say "116% to plan so far" but not "how many units are still
    # owed against the goal", and cannot know when the curve actually ends.
    plan_full = safe_fetch(f"{lid}.plan_full (GSHEETS)",
                           lambda: {r["SKU"]: (int(r["PLAN_TOTAL"] or 0), str(r["PLAN_END"]))
                                    for r in rows(cur, q_plan_full(skus), {})},
                           {}, key="gsheets")
    if not plan_full:
        plan_full = plan_totals_from_fallback(skus)

    plan_rows, plan_daily = {}, {}
    for sku, d, u in plan_detail:
        plan_rows[sku] = plan_rows.get(sku, 0) + u
        plan_daily[d] = plan_daily.get(d, 0) + u
    plan_curve, _cum = [], 0
    for d in sorted(plan_daily):
        _cum += plan_daily[d]
        plan_curve.append({"date": d, "cumPlanUnits": _cum})

    cat_patterns = lc.get("category_type_patterns") or []
    cc = None
    if cat_patterns:
        cc = safe_fetch(f"{lid}.category_customers (UOS.PRODUCTS)",
                        lambda: _build_category(cur, skus, cat_patterns, params, lc, sku_meta), None,
                        key="uos_products")

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

    inv_by_sku = {r["SKU"]: int(r["AVAILABLE"] or 0) for r in inv_rows}
    trail_by_sku = {r["SKU"]: int(r["UNITS"] or 0) for r in trail_rows}
    # Same figures split by storefront. Stock does not move between the two —
    # a Canadian order cannot be filled from Memphis — so a combined
    # projection can read comfortable while one side is already empty.
    inv_by_region = {r["SKU"]: {"us": int(r["US_AVAILABLE"] or 0),
                                "ca": int(r["CA_AVAILABLE"] or 0)} for r in inv_rows}
    trail_by_region = {r["SKU"]: {"us": int(r["US_UNITS"] or 0),
                                  "ca": int(r["CA_UNITS"] or 0)} for r in trail_rows}

    by_variant = []
    for r in variant_rows:
        sku = r["SKU"]
        meta = sku_meta.get(sku, {})
        units = int(r["UNITS"] or 0)
        plan = plan_rows.get(sku)
        # Out-of-stock projection: available units divided by the trailing
        # sell-through rate. A straight-line estimate that assumes demand
        # holds and ignores replenishment — the dashboard labels it as such.
        goal, curve_end = plan_full.get(sku, (None, None))
        avail = inv_by_sku.get(sku)
        run_rate = round(trail_by_sku.get(sku, 0) / oos_days, 2) if oos_days else 0
        days_to_oos, est_oos = project_oos(avail, run_rate, cutoff)
        # Per storefront. None when the inventory source is unavailable, so
        # the page can tell "no data" apart from "no stock".
        region_oos = {}
        for rg in ("us", "ca"):
            r_avail = (inv_by_region.get(sku) or {}).get(rg) if sku in inv_by_region else None
            r_rate = (round((trail_by_region.get(sku) or {}).get(rg, 0) / oos_days, 2)
                      if oos_days else 0)
            r_days, r_date = project_oos(r_avail, r_rate, cutoff)
            region_oos[rg] = {"units": r_avail, "runRate": r_rate,
                              "days": r_days, "date": r_date}
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
            "usUnits": int(r["US_UNITS"] or 0),
            "caUnits": int(r["CA_UNITS"] or 0),
            "usNetSales": f2(r["US_NET_SALES"]),
            "caNetSales": f2(r["CA_NET_SALES"]),
            "planUnits": plan,
            "pctToPlanUnits": round(units / plan * 100, 1) if plan else None,
            "inventoryUnits": avail,
            "runRateUnitsPerDay": run_rate,
            "daysToOOS": days_to_oos,
            "estOOSDate": est_oos,
            # Stock cannot move between storefronts, so each side is projected
            # against its own inventory and its own sell-through rate.
            "usInventoryUnits": region_oos["us"]["units"],
            "caInventoryUnits": region_oos["ca"]["units"],
            "usRunRateUnitsPerDay": region_oos["us"]["runRate"],
            "caRunRateUnitsPerDay": region_oos["ca"]["runRate"],
            "usDaysToOOS": region_oos["us"]["days"],
            "caDaysToOOS": region_oos["ca"]["days"],
            "usEstOOSDate": region_oos["us"]["date"],
            "caEstOOSDate": region_oos["ca"]["date"],
            # Whole-curve goal, distinct from planUnits above (plan to date).
            "planTotalUnits": goal,
            "pctToGoalUnits": round(units / goal * 100, 1) if goal else None,
            "unitsToGoal": max(0, goal - units) if goal else None,
            "planEndDate": curve_end,
            # Cover at the trailing sell-through rate.
            "weeksOfStock": round(avail / run_rate / 7, 1) if avail and run_rate > 0 else None,
            # Demand Planning's figures, carried through as-is. Deliberately
            # separate fields rather than overwriting inventoryUnits/estOOSDate:
            # the two sources disagree and the dashboard shows both.
            "decayCurveOOS": (NPD_OOS.get(sku) or {}).get("decayCurveOOS"),
            "realInventoryUnits": (NPD_OOS.get(sku) or {}).get("realInventoryUnits"),
        })

    # daily_rows is day x SKU. Roll it up to launch-level totals here — units
    # and revenue are additive — and keep the per-SKU rows so the page can
    # rebuild the same series for a shade subset.
    by_day = {}
    daily_by_sku = {}
    for r in daily_rows:
        day, sku = str(r["D"]), r["SKU"]
        t = by_day.setdefault(day, {"units": 0, "netSales": 0.0, "usUnits": 0, "caUnits": 0,
                                    "usNetSales": 0.0, "caNetSales": 0.0})
        t["units"] += int(r["UNITS"] or 0)
        t["netSales"] += float(r["NET_SALES"] or 0)
        t["usUnits"] += int(r["US_UNITS"] or 0)
        t["caUnits"] += int(r["CA_UNITS"] or 0)
        t["usNetSales"] += float(r["US_NET_SALES"] or 0)
        t["caNetSales"] += float(r["CA_NET_SALES"] or 0)
        # Column order matches DAILY_SKU_COLUMNS in launch.html. Positional
        # rather than keyed: this is the one structure in data.js big enough
        # for the key names to cost more than the values.
        daily_by_sku.setdefault(sku, []).append([
            day, int(r["UNITS"] or 0), f2(r["NET_SALES"]),
            int(r["US_UNITS"] or 0), int(r["CA_UNITS"] or 0),
            f2(r["US_NET_SALES"]), f2(r["CA_NET_SALES"]),
            int(r["NEW_CUSTOMERS"] or 0), int(r["RET_CUSTOMERS"] or 0),
        ])

    # Exact launch-level new/returning per day. Distinct customer counts do
    # not sum across SKUs, so this cannot come out of the rollup above.
    cust_by_day = {str(r["D"]): (int(r["NEW_CUSTOMERS"] or 0), int(r["RET_CUSTOMERS"] or 0))
                   for r in daily_customer_rows}

    # plan_daily holds the per-day plan the cumulative curve is built from;
    # carrying it onto each row lets the dashboard show daily actual vs plan
    # instead of only the cumulative comparison.
    daily, cum_u, cum_s, cum_p = [], 0, 0.0, 0
    for day in sorted(by_day):
        t = by_day[day]
        u, s = t["units"], round(t["netSales"], 2)
        plan_u = plan_daily.get(day)
        cum_u += u
        cum_s = round(cum_s + s, 2)
        if plan_u is not None:
            cum_p += plan_u
        new_c, ret_c = cust_by_day.get(day, (None, None))
        daily.append({"date": day, "units": u, "netSales": s,
                      "usUnits": t["usUnits"], "caUnits": t["caUnits"],
                      "usNetSales": round(t["usNetSales"], 2),
                      "caNetSales": round(t["caNetSales"], 2),
                      "cumUnits": cum_u, "cumSales": cum_s,
                      "planUnits": plan_u,
                      "cumPlanUnits": cum_p if plan_daily else None,
                      "newCustomers": new_c, "retCustomers": ret_c})

    # Per-SKU daily plan, so the plan line and "% to Plan" follow a shade
    # filter instead of comparing a subset's units to the whole launch's plan.
    plan_by_sku = {}
    for sku, d, u in plan_detail:
        plan_by_sku.setdefault(sku, []).append([d, u])
    for v in plan_by_sku.values():
        v.sort()

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
            "subscriptionRevenue": f2(subs_row["SUB_REVENUE"]) if subs_row.get("SUB_REVENUE") is not None else None,
            "newCustomerRevenue": f2(summary_row["NEW_NET_SALES"]),
            "retCustomerRevenue": round(net_sales - f2(summary_row["NEW_NET_SALES"]), 2),
            "pdpViews": total_pdp_views,
            "pdpAtcRate": round(total_atc / total_pdp_views * 100, 1) if total_pdp_views else 0,
            "pdpCvr": round(total_purch / total_pdp_views * 100, 1) if total_pdp_views else 0,
        },
        "regions": {
            "us": {"units": int(summary_row["US_UNITS"] or 0), "netSales": f2(summary_row["US_NET_SALES"]),
                   "orders": int(summary_row["US_ORDERS"] or 0)},
            "ca": {"units": int(summary_row["CA_UNITS"] or 0), "netSales": f2(summary_row["CA_NET_SALES"]),
                   "orders": int(summary_row["CA_ORDERS"] or 0)},
        },
        "trafficStart": launch_date,
        "trafficEnd": ga_cutoff,
        "traffic": traffic,
        "landing": landing,
        "learnings": LEARNINGS.get(lc["id"], []),
        "asanaLink": ASANA_LINKS.get(lc["id"]),
        "planCurve": plan_curve,
        # Launch-level goal and the real end of the decay curve, taken from the
        # forecast itself rather than launch_date + N.
        "planTotalUnits": sum(v[0] for v in plan_full.values()) or None,
        "planEndDate": max((v[1] for v in plan_full.values()), default=None),
        "archived": not full,
        "byVariant": by_variant,
        # Same rules the page renders, computed once here so the dashboard
        # panel and the Slack alert can never disagree.
        "signals": compute_signals(
            by_variant, pdp, daily, plan_daily,
            [{"product": r["PRODUCT"], "sku": r["SKU"], "pairs": int(r["PAIRS"] or 0)}
             for r in cross_rows],
            cc,
            daily_by_sku=daily_by_sku, plan_by_sku=plan_by_sku,
            pdp_windows=[{"sku": r["SKU"],
                          "recentViews": int(r["RECENT_VIEWS"] or 0),
                          "recentAtc": int(r["RECENT_ATC"] or 0),
                          "priorViews": int(r["PRIOR_VIEWS"] or 0),
                          "priorAtc": int(r["PRIOR_ATC"] or 0)} for r in pdp_window_rows]),
        "dailySales": daily,
        # Per-SKU daily series, so a shade filter can rebuild the daily trend,
        # the daily table and the new-vs-returning trend from the same numbers
        # instead of leaving them showing the whole launch.
        "dailySkuColumns": ["date", "units", "netSales", "usUnits", "caUnits",
                            "usNetSales", "caNetSales", "newCustomers", "retCustomers"],
        "dailyBySku": {k: Compact(v) for k, v in daily_by_sku.items()},
        "planBySku": {k: Compact(v) for k, v in plan_by_sku.items()},
        "pdp": pdp,
        "crossSell": [
            {"product": r["PRODUCT"], "sku": r["SKU"], "pairs": int(r["PAIRS"] or 0)}
            for r in cross_rows
        ],
        # Same pairings split by the launch shade that drove the basket, so the
        # Cross-Sells tab can filter by variant.
        "crossSellBySku": [
            {"primarySku": r["PRIMARY_SKU"], "product": r["PRODUCT"],
             "sku": r["SKU"], "pairs": int(r["PAIRS"] or 0)}
            for r in cross_sku_rows
        ],
        "categoryCustomers": cc,
    }


def pending_launch(lc):
    return {
        "launchId": lc["id"], "name": lc["name"], "launchDate": lc["launch_date"],
        "internalDate": lc.get("internal_date"), "status": "LIVE",
        "category": lc.get("category"), "subtitle": lc.get("subtitle", ""),
        "accent": lc.get("accent", "#8B5CF6"), "skusPending": not lc["skus"],
        "summary": None, "regions": None, "planCurve": [],
        "byVariant": [], "dailySales": [], "pdp": [],
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


def build_landing(cur, lc, params):
    """Landing pages for ONE launch.

    This used to union every launch's SKUs and product-name slugs into a
    single global query, so whichever launch you opened, the tab listed other
    launches' pages over a window starting at the earliest active launch.
    Scoped now to this launch's own SKUs and slug, over its own window.
    """
    # landing_patterns wins when set: the config name is an internal one and
    # frequently does not appear in the storefront URL ("Becca Brow" ships as
    # instant-brow-fix-* / infinity-waterproof-brow-liner), so deriving a slug
    # from it silently matches nothing or the wrong thing.
    explicit = lc.get("landing_patterns")
    if explicit:
        slugs = set(explicit)
    else:
        slugs = {s["sku"] for s in lc["skus"]}
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
    global LEARNINGS, NPD_OOS
    LEARNINGS = load_learnings()   # before build_launch(), which reads it
    NPD_OOS, npd_synced_at = load_npd_oos()   # likewise
    print(f"Demand Planning OOS snapshot: {len(NPD_OOS)} SKUs"
          + (f" (synced {npd_synced_at})" if npd_synced_at else " (never synced)"))

    cal = load_launch_calendar()
    tracked_by_gid = {l["asana_gid"]: l["id"] for l in cfg["launches"] if l.get("asana_gid")}

    # Resolved here, before build_launch() below reads ASANA_LINKS and
    # LEARNINGS. Doing this after the launches are built leaves every link
    # empty and silently drops every Asana learning.
    # Attach Asana learnings to launches. An explicit asana_gid in config wins.
    # Failing that, a launch is matched to a calendar entry sharing its exact
    # launch date, but only when that date maps to exactly one entry on each
    # side — the calendar and this repo name the same launch differently, and
    # an ambiguous date must not silently attach one launch's retro notes to
    # another. How each link was made is recorded so the page can show it.
    if cal:
        by_gid = {e["asana_gid"]: e for e in cal["entries"]}
        cal_dates = {}
        for e in cal["entries"]:
            cal_dates.setdefault(e["launch_date"], []).append(e)
        cfg_dates = {}
        for l in cfg["launches"]:
            cfg_dates.setdefault(l["launch_date"], []).append(l)

        for l in cfg["launches"]:
            gid, how = l.get("asana_gid"), "config"
            if not gid:
                same = cal_dates.get(l["launch_date"], [])
                if len(same) == 1 and len(cfg_dates.get(l["launch_date"], [])) == 1:
                    gid, how = same[0]["asana_gid"], "launch date"
            if not gid:
                continue
            entry = by_gid.get(gid)
            found = (cal.get("learnings_by_gid") or {}).get(gid) or []
            ASANA_LINKS[l["id"]] = {
                "asanaUrl": entry.get("asana_url") if entry else None,
                "asanaName": entry.get("name") if entry else None,
                "linkedBy": how,
            }
            if found:
                LEARNINGS.setdefault(l["id"], []).extend(found)
                print(f"  {l['id']}: {len(found)} Asana learning(s) via {how}")

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
    # Past the decay curve. These used to be dropped entirely, which silently
    # removed finished launches from the dashboard — LLEM Shade Extension
    # vanished on day 123 despite being a $1.6M launch. Still built, with the
    # per-tab detail skipped, so the Historical view can show them.
    archived = [lc for lc in launched if lc["skus"] and lc not in active]
    # launched too recently for the cutoff (e.g. launched today/yesterday)
    too_new = [lc for lc in cfg["launches"] if lc["launch_date"] > cutoff and lc["launch_date"] <= str(today)]
    if args.dry_run:
        print(f"-- cutoff={cutoff} ga_cutoff={ga_cutoff}")
        print("-- GA4 traffic/landing windows are per launch (launch_date -> ga_cutoff)")
        print(f"-- active launches: {[lc['id'] for lc in active]}")
        print(f"-- archived (past decay curve, reduced query set): {[lc['id'] for lc in archived]}")
        print(f"-- skipped (no SKUs yet): {[lc['id'] for lc in no_skus]}")
        for lc in with_skus + archived:
            skus = [s["sku"] for s in lc["skus"]]
            cat_patterns = lc.get("category_type_patterns") or []
            is_archived = lc in archived
            print(f"\n-- ========== {lc['id']}{' (archived)' if is_archived else ''} ==========")
            queries = [
                ("summary", q_summary(skus)), ("by_variant", q_by_variant(skus)),
                ("daily", q_daily(skus)), ("daily_customers", q_daily_customers(skus)),
                ("pdp_windows", q_pdp_windows(skus)),
                ("plan", q_plan(skus)),
                ("plan_full", q_plan_full(skus)),
            ]
            if not is_archived:
                queries += [
                    ("pdp", q_pdp(skus)), ("cross_sell", q_cross_sell(skus)),
                    ("cross_sell_by_sku", q_cross_sell_by_sku(skus)),
                    ("subs", q_subs(skus)), ("inventory", q_inventory(skus)),
                    ("trailing_units", q_trailing_units(skus)),
                    ("category_customers", q_category_customers(skus, cat_patterns) if cat_patterns else "-- no category patterns configured"),
                ]
            for name, sql in queries:
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
        launches = [build_launch(cur, lc, cutoff, ga_cutoff) for lc in with_skus]
        launches += [build_launch(cur, lc, cutoff, ga_cutoff, full=False) for lc in archived]
        launches += [pending_launch(lc) for lc in no_skus + too_new]

    finally:
        conn.close()

    # Upcoming comes from the Asana marketing calendar when a snapshot exists —
    # that is the team's source of truth. The hand-maintained list in
    # launches.json stays as the fallback so the dashboard still works if the
    # snapshot is missing. Tracked launches keep their config dates either way;
    # a live launch's date is never rewritten from Asana automatically.
    if cal:
        upcoming_out = [
            {"name": e["name"], "launchDate": e["launch_date"],
             "internalDate": e.get("internal_date"),
             "trackedId": tracked_by_gid.get(e["asana_gid"]),
             "section": e.get("section"), "asanaUrl": e.get("asana_url")}
            for e in cal["entries"]
        ]
        calendar_out = {
            "source": "asana",
            "projectUrl": cal.get("asana_project_url"),
            "mismatches": cal.get("mismatches") or [],
            "notLinked": cal.get("not_in_config") or [],
            "configWithoutLink": cal.get("config_without_asana_gid") or [],
        }
        print(f"Upcoming sourced from the Asana calendar snapshot "
              f"({len(upcoming_out)} entries, {len(calendar_out['mismatches'])} date mismatches)")
    else:
        upcoming_out = [
            {"name": u["name"], "launchDate": u["launch_date"],
             "internalDate": u.get("internal_date"), "trackedId": u.get("tracked_id")}
            for u in cfg.get("upcoming", [])
        ]
        calendar_out = {"source": "config", "projectUrl": None,
                        "mismatches": [], "notLinked": [], "configWithoutLink": []}

    data = {
        "meta": {
            "dataCutoff": cutoff,
            "gaCutoff": ga_cutoff,
            "generatedAt": str(today),
            "mode": "automated",
            "sourceDb": SRC_DB,
            "retentionDays": retention,
            "oosWindowDays": OOS_WINDOW_DAYS,
            "npdOosSyncedAt": npd_synced_at,
            "sourceStatus": SOURCE_STATUS,
        },
        "launches": launches,
        "upcoming": upcoming_out,
        "calendar": calendar_out,
    }

    OUT_PATH.write_text(
        "/* GENERATED FILE — do not hand-edit.\n"
        f"   Built by scripts/refresh_data.py on {today} · cutoff {cutoff} · "
        f"source Snowflake {data['meta']['sourceDb']} */\n"
        "window.DASHBOARD_DATA = "
        + dump_js(data)
        + ";\n"
    )
    print(f"Wrote {OUT_PATH} · cutoff {cutoff} · {len(launches)} launches "
          f"({len(with_skus)} with data, {len(archived)} archived, "
          f"{len(no_skus) + len(too_new)} pending)")


if __name__ == "__main__":
    main()
