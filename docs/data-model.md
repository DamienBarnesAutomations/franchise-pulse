# Data Model

## Overview

FranchisePulse uses a **star schema** — the standard pattern for analytical
workloads. One central fact table surrounded by dimension tables. Simple to
query, fast to aggregate, easy to explain.

```
                    ┌──────────────────┐
                    │   dim_date       │
                    │  PK: date_key    │
                    └────────┬─────────┘
                             │
┌──────────────────┐         │         ┌──────────────────┐
│   dim_store      │         │         │   dim_product    │
│  PK: store_key   ├─────────┼─────────┤  PK: product_key │
└──────────────────┘         │         └──────────────────┘
                             │
                    ┌────────▼─────────┐
                    │   fact_sales     │
                    │  PK: sales_key   │
                    │  FK: date_key    │
                    │  FK: store_key   │
                    │  FK: product_key │
                    │  FK: payment_key │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │ dim_payment_     │
                    │ method           │
                    │ PK: payment_key  │
                    └──────────────────┘
```

---

## Design Decisions

### Why star schema over a flat wide table?

A flat table would denormalise everything into one wide row — store name, product
name, category, region all repeated on every transaction. That's simpler to load
but creates real problems:

- **Update anomalies** — if a store changes ownership, you'd update millions of
  rows. In a star schema you update one row in `dim_store`.
- **Query performance** — aggregating by region or category is faster when those
  values are in small dimension tables rather than scanned across a 280,000 row
  fact table.
- **Flexibility** — adding a new attribute to a store (e.g. seating capacity)
  means adding one column to `dim_store`, not rebuilding the fact table.

### Why surrogate keys?

Every dimension table has a system-generated integer primary key (`store_key`,
`product_key` etc.) separate from the natural business key (`store_id`, `sku`).

Reasons:
- Natural keys can change. A store ID reassignment would break every fact row
  that references it. Surrogate keys are immutable.
- Integer joins are faster than VARCHAR joins at scale.
- Consistent pattern across all dimensions regardless of source system key format.

The natural keys (`store_id`, `product_sku`) are kept in the dimension tables
as unique constraints — they're still queryable, just not used as join keys.

### Why computed columns on fact_sales?

`gross_revenue`, `discount_amount`, and `net_revenue` are defined as computed
columns in SQL Server:

```sql
gross_revenue   AS (quantity * unit_price),
discount_amount AS (quantity * unit_price * discount_applied),
net_revenue     AS (quantity * unit_price * (1 - discount_applied))
```

They are never stored — SQL Server calculates them on read. This means:
- A value can never get out of sync with its inputs
- No ETL logic needed to calculate them
- `unit_price` and `discount_applied` are the single source of truth

The tradeoff is a small CPU cost on read. At this data volume that's irrelevant.
At Synapse scale you'd consider persisting them.

### Why keep staging as all VARCHAR?

Source data arrives dirty. S03 Cork City sends prices like `3.80X`. If the
staging table declared `unit_price` as `DECIMAL`, the bulk insert would fail
on the first bad row and reject the entire file.

By staging everything as VARCHAR first, we:
1. Get all rows into the database regardless of quality
2. Run explicit validation rules against them
3. Quarantine only the bad rows with a specific reason
4. Cast to proper types only on the way into `fact_sales`

This pattern — load first, validate second — is standard in production pipelines.

### Why a separate dim_date?

A calendar dimension table lets you query by business date attributes without
calculating them at query time:

```sql
-- Without dim_date
WHERE DATEPART(WEEKDAY, transaction_date) IN (1, 7)

-- With dim_date (faster, readable)
WHERE dd.is_weekend = 1
```

More importantly it lets you add business-specific attributes — Irish public
holidays, promotional periods, fiscal week definitions — without touching
fact data. The `is_public_holiday` column exists for exactly this.

The table covers 2024-01-01 to 2027-12-31 (1,461 rows). It's populated once
and never changes.

---

## Table Reference

### fact_sales

| Column | Type | Notes |
|---|---|---|
| sales_key | BIGINT IDENTITY | Surrogate PK |
| date_key | INT | FK → dim_date |
| store_key | INT | FK → dim_store |
| product_key | INT | FK → dim_product |
| payment_key | INT | FK → dim_payment_method |
| transaction_id | VARCHAR(50) | Degenerate dimension — natural key from source |
| cashier_id | VARCHAR(20) | Nullable — ~1% missing in source data |
| quantity | SMALLINT | Negative values = refunds |
| unit_price | DECIMAL(10,2) | Validated on ingest |
| discount_applied | DECIMAL(10,2) | 0.00–1.00 (0% to 100%) |
| gross_revenue | Computed | quantity × unit_price |
| discount_amount | Computed | quantity × unit_price × discount_applied |
| net_revenue | Computed | quantity × unit_price × (1 − discount_applied) |
| source_file | VARCHAR(255) | Audit — which file this row came from |
| loaded_at | DATETIME2 | Audit — when this row was loaded |

### stg_sales (staging)

All source columns are VARCHAR. Additional metadata columns:

| Column | Type | Notes |
|---|---|---|
| source_file | VARCHAR(255) | Filename including store_id and date |
| load_timestamp | DATETIME2 | When row was staged |
| row_hash | CHAR(64) | SHA2_256 of transaction_id + store_id + date |
| validation_status | VARCHAR(10) | PENDING → PASS or FAIL |
| rejection_reason | VARCHAR(500) | Rule code + description if FAIL |

### Validation Rules

| Code | Rule |
|---|---|
| V01 | transaction_id is not null or empty |
| V02 | store_id exists in dim_store |
| V03 | transaction_date is a valid datetime |
| V04 | product_sku exists in dim_product |
| V05 | unit_price is a valid positive decimal |
| V06 | quantity is a valid integer |
| V07 | payment_method exists in dim_payment_method |
| DUPLICATE | Row hash already exists in fact_sales or current batch |

---

## Reporting Layer

Three views provide the output dataset for consumers:

**vw_daily_sales_by_store** — operational view. One row per store per day.
Includes transaction count, revenue, refund count, active cashiers.
Used by store managers and operations teams.

**vw_weekly_product_mix** — finance view. One row per product per store per week.
Includes quantity sold, revenue, discount rate percentage, revenue per unit.
Used for margin analysis and promotional planning.

**vw_regional_revenue_summary** — executive view. One row per region per month.
Normalises revenue per store so regions of different sizes are comparable.
Includes top payment method by volume per region.

---

## Future State

**Slowly Changing Dimensions (SCD Type 2)** — currently dimensions are Type 1
(overwrite on change). If a store changes franchise owner, historical fact rows
would now show the new owner. For financial reporting accuracy, SCD Type 2 would
preserve the historical owner by adding effective date columns and a current flag.

**Data vault** — at enterprise scale (50+ sources, complex history requirements)
a data vault layer between staging and the star schema would provide better
auditability. Not warranted at this scope.
