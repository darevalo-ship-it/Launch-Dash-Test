'use strict';

const express   = require('express');
const path      = require('path');
const snowflake = require('snowflake-sdk');

const app  = express();
const PORT = process.env.PORT || 3000;

snowflake.configure({ logLevel: 'error' });

// ─── LAUNCH CONFIG ────────────────────────────────────────────────────────────

const LAUNCHES = {
  empowershine: {
    id:         'empowershine',
    name:       'EmpowerShine Satin Lip Cream',
    launchDate: '2026-04-07',
    status:     'LIVE',
    skus: {
      TVG5950: { name: 'Ragan',    shade: 'Warm Plum',     color: '#7B3FA0' },
      TVG5960: { name: 'Joan',     shade: 'Cool Rose',     color: '#E8748A' },
      TVG5920: { name: 'Kaisa',    shade: 'Dusty Rose',    color: '#D4879A' },
      TVG5880: { name: 'Ilene',    shade: 'Natural Rose',  color: '#C89EA0' },
      TVG5940: { name: 'Kathy',    shade: 'Soft Apricot',  color: '#F4A460' },
      TVG5890: { name: 'Michelle', shade: 'Neutral Mauve', color: '#A0888A' },
      TVG5910: { name: 'Linda',    shade: 'Cherry Red',    color: '#C01830' },
      TVG5930: { name: 'Chanice',  shade: 'Magenta Pink',  color: '#C930B8' },
      TVG5900: { name: 'Rosa',     shade: 'Deep Berry',    color: '#7A1840' },
      TVG5970: { name: 'Kackie',   shade: 'Mocha',         color: '#8B6355' },
    },
  },
  'becca-brow': {
    id:         'becca-brow',
    name:       'Becca Brow',
    launchDate: '2026-06-08',
    status:     'LIVE',
    skus: {
      TVG6680: { name: 'Brow Pencil', shade: 'Silver Grey', color: '#4B5563' },
      TVG6690: { name: 'Brow Gel',    shade: 'Silver Grey', color: '#6B7A8D' },
      TVG6710: { name: 'Brow Liner',  shade: 'Silver Grey', color: '#9CA3AF' },
    },
  },
};

// ─── SNOWFLAKE ────────────────────────────────────────────────────────────────
// Required env vars: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USERNAME, SNOWFLAKE_PASSWORD
// Optional:         SNOWFLAKE_WAREHOUSE (default: COMPUTE_WH), SNOWFLAKE_ROLE

let _conn = null;

async function getConn() {
  if (_conn) return _conn;
  _conn = snowflake.createConnection({
    account:   process.env.SNOWFLAKE_ACCOUNT,
    username:  process.env.SNOWFLAKE_USERNAME,
    password:  process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'COMPUTE_WH',
    database:  'DAASITY_DB',
    schema:    'UOS',
    role:      process.env.SNOWFLAKE_ROLE,
  });
  await new Promise((res, rej) => _conn.connect(err => err ? rej(err) : res()));
  return _conn;
}

async function runQuery(sql) {
  const conn = await getConn();
  return new Promise((res, rej) =>
    conn.execute({
      sqlText: sql,
      complete: (err, _stmt, rows) => {
        if (err) { _conn = null; rej(err); }
        else res(rows || []);
      },
    })
  );
}

// ─── SQL BUILDERS ─────────────────────────────────────────────────────────────

function inList(skus) {
  return Object.keys(skus).map(s => `'${s}'`).join(',');
}

function salesBySkuSQL(sl, ld) {
  return `
    SELECT
      oli.SKU,
      SUM(oli.PRICE * oli.QUANTITY - COALESCE(oli.DISCOUNT_AMOUNT, 0)) AS NET_SALES,
      SUM(oli.QUANTITY)                                                  AS UNITS,
      COUNT(DISTINCT o.ORDER_ID)                                         AS ORDERS
    FROM DAASITY_DB.UOS.ORDER_LINE_ITEMS oli
    JOIN DAASITY_DB.UOS.ORDERS o ON oli.ORDER_ID = o.ORDER_ID
    WHERE oli.SKU IN (${sl})
      AND o.ORDER_DATE::DATE >= '${ld}'
      AND (oli.REFUND_FLAG IS NULL OR oli.REFUND_FLAG = FALSE)
    GROUP BY oli.SKU`;
}

function dailySalesSQL(sl, ld) {
  return `
    SELECT
      o.ORDER_DATE::DATE                                                 AS SALE_DATE,
      SUM(oli.PRICE * oli.QUANTITY - COALESCE(oli.DISCOUNT_AMOUNT, 0)) AS NET_SALES,
      SUM(oli.QUANTITY)                                                  AS UNITS
    FROM DAASITY_DB.UOS.ORDER_LINE_ITEMS oli
    JOIN DAASITY_DB.UOS.ORDERS o ON oli.ORDER_ID = o.ORDER_ID
    WHERE oli.SKU IN (${sl})
      AND o.ORDER_DATE::DATE >= '${ld}'
      AND (oli.REFUND_FLAG IS NULL OR oli.REFUND_FLAG = FALSE)
    GROUP BY SALE_DATE
    ORDER BY SALE_DATE`;
}

function nvrSQL(sl, ld) {
  return `
    WITH cust_first AS (
      SELECT CUSTOMER_ID, MIN(ORDER_DATE::DATE) AS FIRST_ORDER_DATE
      FROM DAASITY_DB.UOS.ORDERS
      GROUP BY CUSTOMER_ID
    ),
    launch_ords AS (
      SELECT DISTINCT o.ORDER_ID, o.CUSTOMER_ID, o.ORDER_DATE::DATE AS OD
      FROM DAASITY_DB.UOS.ORDER_LINE_ITEMS oli
      JOIN DAASITY_DB.UOS.ORDERS o ON oli.ORDER_ID = o.ORDER_ID
      WHERE oli.SKU IN (${sl})
        AND o.ORDER_DATE::DATE >= '${ld}'
        AND (oli.REFUND_FLAG IS NULL OR oli.REFUND_FLAG = FALSE)
    )
    SELECT
      CASE WHEN lo.OD = cf.FIRST_ORDER_DATE THEN 'New' ELSE 'Returning' END AS CTYPE,
      COUNT(DISTINCT lo.CUSTOMER_ID)                                          AS CUSTOMERS,
      SUM(oli2.PRICE * oli2.QUANTITY - COALESCE(oli2.DISCOUNT_AMOUNT, 0))   AS NET_SALES,
      SUM(oli2.QUANTITY)                                                       AS UNITS
    FROM launch_ords lo
    JOIN DAASITY_DB.UOS.ORDER_LINE_ITEMS oli2
      ON lo.ORDER_ID = oli2.ORDER_ID
      AND oli2.SKU IN (${sl})
      AND (oli2.REFUND_FLAG IS NULL OR oli2.REFUND_FLAG = FALSE)
    JOIN cust_first cf ON lo.CUSTOMER_ID = cf.CUSTOMER_ID
    GROUP BY CTYPE`;
}

function nvrBySkuSQL(sl, ld) {
  return `
    WITH cust_first AS (
      SELECT CUSTOMER_ID, MIN(ORDER_DATE::DATE) AS FIRST_ORDER_DATE
      FROM DAASITY_DB.UOS.ORDERS
      GROUP BY CUSTOMER_ID
    )
    SELECT
      oli.SKU,
      CASE WHEN o.ORDER_DATE::DATE = cf.FIRST_ORDER_DATE THEN 'New' ELSE 'Returning' END AS CTYPE,
      COUNT(DISTINCT o.CUSTOMER_ID) AS CUSTOMERS
    FROM DAASITY_DB.UOS.ORDER_LINE_ITEMS oli
    JOIN DAASITY_DB.UOS.ORDERS o  ON oli.ORDER_ID = o.ORDER_ID
    JOIN cust_first cf ON o.CUSTOMER_ID = cf.CUSTOMER_ID
    WHERE oli.SKU IN (${sl})
      AND o.ORDER_DATE::DATE >= '${ld}'
      AND (oli.REFUND_FLAG IS NULL OR oli.REFUND_FLAG = FALSE)
    GROUP BY oli.SKU, CTYPE`;
}

function planSQL(sl, ld) {
  return `
    SELECT
      SKU,
      SUM(PLAN_UNITS)     AS PLAN_UNITS,
      SUM(PLAN_NET_SALES) AS PLAN_NET_SALES
    FROM DAASITY_DB.GSHEETS.SKU_LAUNCH_DAY_FORECAST
    WHERE SKU IN (${sl})
      AND DATE::DATE >= '${ld}'
      AND DATE::DATE <= CURRENT_DATE()
    GROUP BY SKU`;
}

// ─── ASSEMBLY ─────────────────────────────────────────────────────────────────

async function fetchLaunch(launchId) {
  const launch = LAUNCHES[launchId];
  if (!launch) throw new Error(`Unknown launch: ${launchId}`);

  const sl = inList(launch.skus);
  const ld = launch.launchDate;

  const [skuRows, dayRows, nvrRows, nvrSkuRows, planRows] = await Promise.all([
    runQuery(salesBySkuSQL(sl, ld)),
    runQuery(dailySalesSQL(sl, ld)),
    runQuery(nvrSQL(sl, ld)),
    runQuery(nvrBySkuSQL(sl, ld)),
    runQuery(planSQL(sl, ld)),
  ]);

  let totalSales = 0, totalUnits = 0, totalOrders = 0;
  const byVariant = [];

  for (const r of skuRows) {
    const meta = launch.skus[r.SKU] || { name: r.SKU, shade: '', color: '#3A9E98' };
    totalSales  += Number(r.NET_SALES) || 0;
    totalUnits  += Number(r.UNITS)     || 0;
    totalOrders += Number(r.ORDERS)    || 0;
    byVariant.push({
      sku:      r.SKU,
      name:     meta.name,
      shade:    meta.shade,
      color:    meta.color,
      netSales: Number(r.NET_SALES) || 0,
      units:    Number(r.UNITS)    || 0,
      orders:   Number(r.ORDERS)   || 0,
    });
  }
  byVariant.sort((a, b) => b.units - a.units);

  let newCust = 0, retCust = 0;
  for (const r of nvrRows) {
    if (r.CTYPE === 'New') newCust = Number(r.CUSTOMERS) || 0;
    else                    retCust = Number(r.CUSTOMERS) || 0;
  }
  const totalCust = newCust + retCust;

  const nvrBySku = {};
  for (const r of nvrSkuRows) {
    if (!nvrBySku[r.SKU]) nvrBySku[r.SKU] = { new: 0, ret: 0 };
    if (r.CTYPE === 'New') nvrBySku[r.SKU].new = Number(r.CUSTOMERS) || 0;
    else                    nvrBySku[r.SKU].ret = Number(r.CUSTOMERS) || 0;
  }
  for (const v of byVariant) {
    const m = nvrBySku[v.sku] || { new: 0, ret: 0 };
    v.newCustomers = m.new;
    v.retCustomers = m.ret;
  }

  let planUnits = 0, planSales = 0;
  const planBySku = {};
  for (const r of planRows) {
    planBySku[r.SKU] = {
      planUnits: Number(r.PLAN_UNITS)     || 0,
      planSales: Number(r.PLAN_NET_SALES) || 0,
    };
    planUnits += planBySku[r.SKU].planUnits;
    planSales += planBySku[r.SKU].planSales;
  }
  for (const v of byVariant) {
    const p = planBySku[v.sku] || { planUnits: 0, planSales: 0 };
    v.planUnits      = p.planUnits;
    v.planSales      = p.planSales;
    v.pctToPlanUnits = p.planUnits > 0 ? (v.units / p.planUnits * 100) : null;
  }

  let cumUnits = 0, cumSales = 0;
  const dailySales = dayRows.map(r => {
    cumUnits  += Number(r.UNITS)     || 0;
    cumSales  += Number(r.NET_SALES) || 0;
    return {
      date:     r.SALE_DATE,
      units:    Number(r.UNITS)     || 0,
      netSales: Number(r.NET_SALES) || 0,
      cumUnits,
      cumSales,
    };
  });

  const daysLive = Math.max(1, Math.floor((Date.now() - new Date(ld).getTime()) / 86400000) + 1);

  return {
    launchId:   launch.id,
    name:       launch.name,
    launchDate: launch.launchDate,
    status:     launch.status,
    daysLive,
    summary: {
      netSales:       totalSales,
      units:          totalUnits,
      orders:         totalOrders,
      aov:            totalOrders > 0 ? totalSales / totalOrders : 0,
      newCustomers:   newCust,
      retCustomers:   retCust,
      totalCustomers: totalCust,
      newPct:         totalCust > 0 ? newCust / totalCust * 100 : 0,
      retPct:         totalCust > 0 ? retCust / totalCust * 100 : 0,
      planUnits,
      planSales,
      pctToPlanUnits: planUnits > 0 ? totalUnits / planUnits * 100 : null,
      pctToPlanSales: planSales > 0 ? totalSales / planSales * 100 : null,
    },
    byVariant,
    dailySales,
  };
}

// ─── CACHE ────────────────────────────────────────────────────────────────────

const CACHE_TTL = 10 * 60 * 1000;
const _cache = {};

async function getLaunchData(id) {
  const now = Date.now();
  if (_cache[id] && (now - _cache[id].ts) < CACHE_TTL) return _cache[id].data;
  const data = await fetchLaunch(id);
  _cache[id] = { data, ts: now };
  return data;
}

function warmCache() {
  Promise.allSettled(
    Object.keys(LAUNCHES).map(id =>
      getLaunchData(id)
        .then(() => console.log(`[cache] warmed: ${id}`))
        .catch(e => console.error(`[cache] ${id} failed:`, e.message))
    )
  );
}

setTimeout(warmCache, 3000);

// ─── ROUTES ───────────────────────────────────────────────────────────────────

app.use('/launch-dashboards', express.static(path.join(__dirname, 'public')));
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/health', (_req, res) =>
  res.json({ ok: true, ts: new Date().toISOString(), launches: Object.keys(LAUNCHES) })
);

app.get('/api/launches', async (_req, res) => {
  try {
    const all = await Promise.all(Object.keys(LAUNCHES).map(getLaunchData));
    res.json(all.map(({ launchId, name, launchDate, status, daysLive, summary }) =>
      ({ launchId, name, launchDate, status, daysLive, summary })
    ));
  } catch (e) {
    console.error('[/api/launches]', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/launch-data', async (req, res) => {
  const id = req.query.launch;
  if (!id || !LAUNCHES[id]) return res.status(400).json({ error: 'Unknown launch id' });
  try {
    res.json(await getLaunchData(id));
  } catch (e) {
    console.error(`[/api/launch-data?launch=${id}]`, e.message);
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, () => console.log(`Launch Intelligence Hub on :${PORT}`));
