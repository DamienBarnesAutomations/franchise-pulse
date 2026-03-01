-- =============================================================
-- FranchisePulse — Reporting Views
-- File: sql/views/01_reporting_views.sql
--
-- These views sit on top of the star schema and represent
-- the output layer of the pipeline. A BI tool or stakeholder
-- query hits these — never the raw fact/dim tables directly.
-- =============================================================

USE FranchisePulse;
GO

-- =============================================================
-- vw_daily_sales_by_store
-- One row per store per day
-- Primary operational view — answers "how did each store do today?"
-- =============================================================
CREATE OR ALTER VIEW dbo.vw_daily_sales_by_store AS
SELECT
    dd.full_date                            AS sale_date,
    dd.day_name,
    dd.is_weekend,
    dd.week_of_year,
    dd.month_name,
    dd.quarter_number,
    dd.year_number,
    ds.store_id,
    ds.store_name,
    ds.city,
    ds.region,
    ds.franchise_owner,
    COUNT(*)                                AS total_transactions,
    SUM(fs.quantity)                        AS total_items_sold,
    SUM(fs.gross_revenue)                   AS gross_revenue,
    SUM(fs.discount_amount)                 AS total_discounts,
    SUM(fs.net_revenue)                     AS net_revenue,
    AVG(fs.net_revenue)                     AS avg_transaction_value,
    COUNT(DISTINCT fs.cashier_id)           AS active_cashiers,
    SUM(CASE WHEN fs.quantity < 0
             THEN 1 ELSE 0 END)             AS refund_count,
    SUM(CASE WHEN fs.quantity < 0
             THEN fs.net_revenue ELSE 0 END) AS refund_value
FROM dbo.fact_sales fs
JOIN dbo.dim_date    dd ON dd.date_key  = fs.date_key
JOIN dbo.dim_store   ds ON ds.store_key = fs.store_key
GROUP BY
    dd.full_date, dd.day_name, dd.is_weekend,
    dd.week_of_year, dd.month_name, dd.quarter_number, dd.year_number,
    ds.store_id, ds.store_name, ds.city, ds.region, ds.franchise_owner;
GO


-- =============================================================
-- vw_weekly_product_mix
-- One row per product per week
-- Answers "what's selling, and where?"
-- Finance uses this for margin analysis
-- =============================================================
CREATE OR ALTER VIEW dbo.vw_weekly_product_mix AS
SELECT
    dd.year_number,
    dd.week_of_year,
    -- Week start/end for readability
    MIN(dd.full_date)                       AS week_start,
    MAX(dd.full_date)                       AS week_end,
    ds.region,
    ds.store_id,
    ds.store_name,
    dp.product_sku,
    dp.product_name,
    dp.category,
    dp.standard_price,
    COUNT(*)                                AS times_sold,
    SUM(fs.quantity)                        AS total_quantity,
    SUM(fs.gross_revenue)                   AS gross_revenue,
    SUM(fs.discount_amount)                 AS total_discounts,
    SUM(fs.net_revenue)                     AS net_revenue,
    -- Discount rate as a percentage
    CASE
        WHEN SUM(fs.gross_revenue) = 0 THEN 0
        ELSE ROUND(
            SUM(fs.discount_amount) / SUM(fs.gross_revenue) * 100,
        2)
    END                                     AS discount_rate_pct,
    -- Revenue per unit (net)
    CASE
        WHEN SUM(fs.quantity) = 0 THEN 0
        ELSE ROUND(SUM(fs.net_revenue) / SUM(fs.quantity), 2)
    END                                     AS net_revenue_per_unit
FROM dbo.fact_sales fs
JOIN dbo.dim_date    dd ON dd.date_key    = fs.date_key
JOIN dbo.dim_store   ds ON ds.store_key   = fs.store_key
JOIN dbo.dim_product dp ON dp.product_key = fs.product_key
WHERE fs.quantity > 0   -- exclude refunds from product mix
GROUP BY
    dd.year_number, dd.week_of_year,
    ds.region, ds.store_id, ds.store_name,
    dp.product_sku, dp.product_name, dp.category, dp.standard_price;
GO


-- =============================================================
-- vw_regional_revenue_summary
-- One row per region per month
-- Executive summary view — answers "how is each region performing?"
-- This is the top-level aggregated reporting dataset
-- =============================================================
CREATE OR ALTER VIEW dbo.vw_regional_revenue_summary AS
SELECT
    dd.year_number,
    dd.quarter_number,
    dd.month_number,
    dd.month_name,
    ds.region,
    COUNT(DISTINCT ds.store_id)             AS store_count,
    COUNT(DISTINCT dd.full_date)            AS trading_days,
    COUNT(*)                                AS total_transactions,
    SUM(fs.quantity)                        AS total_items_sold,
    SUM(fs.gross_revenue)                   AS gross_revenue,
    SUM(fs.discount_amount)                 AS total_discounts,
    SUM(fs.net_revenue)                     AS net_revenue,
    -- Revenue per store (normalises for region size)
    ROUND(
        SUM(fs.net_revenue) / COUNT(DISTINCT ds.store_id),
    2)                                      AS net_revenue_per_store,
    -- Revenue per trading day
    ROUND(
        SUM(fs.net_revenue) / NULLIF(COUNT(DISTINCT dd.full_date), 0),
    2)                                      AS net_revenue_per_day,
    AVG(fs.net_revenue)                     AS avg_transaction_value,
    -- Top payment method by volume
    (
        SELECT TOP 1 dm2.payment_method
        FROM dbo.fact_sales fs2
        JOIN dbo.dim_store ds2          ON ds2.store_key   = fs2.store_key
        JOIN dbo.dim_date dd2           ON dd2.date_key    = fs2.date_key
        JOIN dbo.dim_payment_method dm2 ON dm2.payment_key = fs2.payment_key
        WHERE ds2.region       = ds.region
        AND   dd2.year_number  = dd.year_number
        AND   dd2.month_number = dd.month_number
        GROUP BY dm2.payment_method
        ORDER BY COUNT(*) DESC
    )                                       AS top_payment_method
FROM dbo.fact_sales fs
JOIN dbo.dim_date    dd ON dd.date_key  = fs.date_key
JOIN dbo.dim_store   ds ON ds.store_key = fs.store_key
GROUP BY
    dd.year_number, dd.quarter_number,
    dd.month_number, dd.month_name,
    ds.region;
GO


-- =============================================================
-- Verify — query all three views
-- =============================================================

-- Daily store performance
SELECT TOP 5
    sale_date, store_name, total_transactions,
    net_revenue, avg_transaction_value
FROM dbo.vw_daily_sales_by_store
ORDER BY sale_date DESC, net_revenue DESC;
GO

-- Weekly product mix
SELECT TOP 5
    week_start, store_name, product_name,
    total_quantity, net_revenue, discount_rate_pct
FROM dbo.vw_weekly_product_mix
ORDER BY week_start DESC, net_revenue DESC;
GO

-- Regional summary
SELECT
    month_name, region, store_count,
    total_transactions, net_revenue,
    net_revenue_per_store, top_payment_method
FROM dbo.vw_regional_revenue_summary
ORDER BY year_number, month_number, net_revenue DESC;
GO