# Scaling Considerations

## Current Scale

| Metric | Current |
|---|---|
| Stores | 12 |
| Rows per day | ~28,000 |
| Rows total (90 days) | ~278,000 |
| Pipeline duration | ~9 seconds |
| Storage | < 50MB |

This is a small-to-medium dataset. The current architecture handles it
comfortably with no tuning required. The scaling considerations below
describe what changes — and what stays the same — as the system grows.

---

## Scenario 1 — Volume Growth (10x–100x)

**Trigger:** Data volume grows from 28,000 rows/day to 280,000–2,800,000 rows/day.
This would happen if the franchise expanded to 120+ locations or if transaction
granularity increased (e.g. individual item scans instead of transaction summaries).

**What breaks first:** SQL Server on a Basic tier Azure SQL Database has limited
DTUs (Database Transaction Units). Heavy INSERT and MERGE operations at 10x volume
would saturate the compute tier and cause timeouts.

**The fix:** Move the transformation layer to **Azure Synapse Analytics**.

What changes:
- SQL Server → Synapse dedicated SQL pool
- Connection strings in the pipeline
- Potentially column store indexes instead of row store on fact_sales

What stays the same:
- Star schema design (identical in Synapse)
- All stored procedure logic (T-SQL compatible)
- Airflow DAG structure
- Staging and validation approach
- Reporting views

The pipeline is designed so the orchestration layer is independent of the
compute layer. Swapping SQL Server for Synapse is a configuration change,
not a redesign.

**Intermediate option:** Before Synapse, SQL Server performance can be extended by:
- Moving to a higher Azure SQL tier (Standard S3 → S6)
- Adding columnstore indexes to fact_sales for analytical queries
- Partitioning fact_sales by month

---

## Scenario 2 — New Locations

**Trigger:** The franchise opens new locations. Currently 12 stores, potentially
growing to 50, 100, or 200.

**What changes:** One INSERT into `dim_store`:

```sql
INSERT INTO dbo.dim_store (store_id, store_name, city, region, franchise_owner)
VALUES ('S13', 'FranchisePulse Limerick City', 'Limerick', 'Munster', 'Ryan Group');
```

**What stays the same:** Everything else. The `STORE_IDS` list in the Airflow
DAG would need updating to include the new store, but the pipeline logic,
validation rules, and reporting views are entirely unchanged. The ForEach loop
handles any number of stores.

The `files_expected` count in `pipeline_run_log` would increase automatically
to reflect the new total.

**At very large store counts (200+):** The `STORE_IDS` list in the DAG would
be moved from a hardcoded Python list to a database query:

```python
cursor.execute("SELECT store_id FROM dbo.dim_store WHERE is_active = 1")
STORE_IDS = [row[0] for row in cursor.fetchall()]
```

This means adding a store to `dim_store` automatically adds it to the pipeline
with no code changes at all.

---

## Scenario 3 — Near-Real-Time Latency

**Trigger:** Business requirement changes from daily reporting to hourly or
near-real-time (e.g. live dashboard in busy locations, fraud detection).

**What changes:** The ingestion trigger and source format.

Current: Nightly CSV drop → scheduled 06:00 trigger → previous day's data.

Near-real-time: POS system pushes events → event-driven trigger → minutes latency.

The path to near-real-time on Azure:

```
POS System
    │
    ▼
Azure Event Hub         ← replaces CSV file drop
    │
    ▼
ADF Event Trigger       ← replaces scheduled trigger
    │
    ▼
Staging → Validation → fact_sales   ← unchanged
```

Alternatively, **Azure Stream Analytics** could sit between Event Hub and
the database for real-time aggregation without touching the batch pipeline.

**What stays the same:** Validation rules, fact table schema, stored procedures,
reporting views. The transformation layer is decoupled from the ingestion method.

---

## Scenario 4 — Additional Data Sources

**Trigger:** The business wants to combine sales data with other sources —
loyalty programme data, weather data, marketing spend.

**Current state:** Single source (POS CSV files).

**The pattern:** Each new source gets its own staging table, its own validation
procedure, and its own dimension or fact table. They share the same `dim_date`
and can be joined through it.

Example — adding loyalty data:
```
stg_loyalty → validation → fact_loyalty
                               │
                        joins on date_key + store_key
                               │
                        combined reporting view
```

The pipeline gets a new DAG task or a new DAG entirely. The existing pipeline
is not modified.

---

## What Never Needs to Change

Regardless of scale scenario, these components are stable:

- **Star schema design** — works at 10K rows or 10 billion rows
- **Validation rule pattern** — add rules, don't replace the framework
- **`pipeline_run_log` structure** — monitoring approach scales with the pipeline
- **Stored procedure interfaces** — same EXEC calls regardless of what's behind them
- **Reporting view structure** — consumers never see schema changes

This stability is intentional. The pipeline is designed so that growth requires
adding components, not rebuilding existing ones.
