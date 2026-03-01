# Pipeline Design

## Overview

The pipeline is a **daily batch process** that runs at 06:00 UTC. It collects
the previous day's POS sales exports from 12 franchise locations, validates and
transforms them, and loads them into the reporting layer.

It is built with Apache Airflow locally and maps directly to an Azure Data Factory
pipeline in production.

---

## Pipeline Stages

```
[start_run]
     │
     ▼
[scan_files]          ← checks which stores delivered files
     │                   logs missing stores, does not fail
     ▼
[ingest_files]        ← reads CSVs into stg_sales
     │                   handles UTF-16 encoding (S09)
     ▼
[validate_rows]       ← runs 8 validation rules
     │                   quarantines bad rows with reason
     ▼
[load_fact]           ← MERGEs clean rows into fact_sales
     │                   resolves surrogate keys
     ▼
[archive_files]       ← moves raw files to /archive/
     │
     ▼
[end_run]             ← closes run log, logs summary
```

Every task must succeed before the next begins. A failure stops the pipeline
at that stage and triggers Airflow's retry logic (2 retries, 5 minute delay).

---

## Task Detail

### start_run
Opens a `pipeline_run_log` record with status `RUNNING`. Returns a `run_id`
that all subsequent tasks reference via Airflow XCom. Every database operation
in the pipeline is traceable back to this ID.

**ADF equivalent:** Pipeline run ID, written to a custom logging table via
a Stored Procedure activity at pipeline start.

### scan_files
Loops over all 12 expected store filenames for the run date. For each file:
- If found → added to the `found_files` XCom list for downstream tasks
- If missing → logged to `missing_file_log`, `files_missing` counter incremented

The pipeline continues regardless of missing files. Only a complete absence
of files raises a hard failure.

**ADF equivalent:** GetMetadata activity → ForEach with If Condition activity
checking file existence. Missing files written via Stored Procedure activity.

**Design decision:** Missing files are a business event, not a technical failure.
A store's POS system going offline shouldn't stop 11 other stores from being
processed. The missing file log gives operations visibility to follow up.

### ingest_files
Reads each CSV in Python using `csv.DictReader`. Inserts rows into `stg_sales`
in batches of 500 for performance.

All columns are inserted as strings — no type casting at ingest time. The staging
table is designed to accept dirty data and let the validation step decide what's
clean.

**Encoding detection:** S09 Kilkenny sends UTF-16 files. The store_id is parsed
from the filename and the correct encoding is selected before reading.

**ADF equivalent:** Copy activity with CSV source, staging table sink.
Fault tolerance set to skip incompatible rows.

### validate_rows
Calls `usp_validate_staged_rows`. Eight rules run in sequence:

1. Duplicate detection first (SHA2_256 hash comparison)
2. Then business rules (null checks, referential integrity, type checks)

Rows that fail get `validation_status = FAIL` and a `rejection_reason`.
Rows that pass all rules get `validation_status = PASS`.
Nothing is deleted from staging at this point.

**ADF equivalent:** Data Flow activity with conditional split transform.
Error rows routed to a sink in ADLS silver layer.

### load_fact
Calls `usp_load_fact_sales` which executes a MERGE statement. For each PASS row:
- Resolves `store_id` → `store_key`, `product_sku` → `product_key` etc.
- Casts VARCHAR columns to proper types
- Inserts into `fact_sales` if `transaction_id` not already present

The MERGE handles the edge case where a row somehow passes validation but
already exists in the fact table (belt and braces on top of hash deduplication).

**ADF equivalent:** Stored Procedure activity calling the same MERGE proc.

### archive_files
Moves processed CSV files from `/data/raw/YYYY-MM-DD/` to
`/data/archive/YYYY-MM-DD/`. This keeps the raw landing zone clean and
provides a recoverable copy of source files if reprocessing is ever needed.

**ADF equivalent:** Copy activity moving blobs between ADLS containers,
followed by Delete activity on the source.

### end_run
Updates `pipeline_run_log` with final counts and `status = SUCCESS`.
Queries and logs the run summary. Provides the data for operational monitoring.

---

## Scheduling

```
schedule_interval = "0 6 * * *"
```

Runs at 06:00 UTC daily. Processes the previous day's data (`execution_date - 1 day`).

The gap between data arrival and processing is intentional — stores close at 20:00
and files are exported overnight. By 06:00 all 12 stores have had 10 hours to
deliver their files.

`catchup = False` prevents Airflow from backfilling historical runs on first
deployment. For deliberate backfills, the DAG is triggered manually with a
specific execution date.

---

## XCom Data Flow

Airflow XCom (cross-communication) passes state between tasks:

| Key | Set by | Used by |
|---|---|---|
| `run_id` | start_run | scan_files, end_run |
| `run_date` | start_run | scan_files, archive_files |
| `found_files` | scan_files | ingest_files, archive_files |
| `missing` | scan_files | (logged, not consumed downstream) |
| `rows_staged` | ingest_files | (informational) |
| `rows_loaded` | load_fact | (informational) |

---

## ADF Mapping

This table shows how each local component maps to Azure Data Factory
for the production deployment:

| Local | ADF | Notes |
|---|---|---|
| DAG schedule trigger | Tumbling Window Trigger | Daily at 06:00 UTC |
| Python ForEach loop | ForEach activity | Iterates over store list |
| File existence check | GetMetadata + If Condition | Checks blob existence |
| Missing file log | Stored Procedure activity | Writes to missing_file_log |
| CSV read + insert | Copy activity | CSV → Azure SQL staging table |
| usp_validate_staged_rows | Stored Procedure activity | Same proc, no changes |
| usp_load_fact_sales | Stored Procedure activity | Same proc, no changes |
| File archive | Copy + Delete activity | Moves blobs between containers |
| usp_end_pipeline_run | Stored Procedure activity | Same proc, no changes |
| Airflow retry logic | Activity retry settings | 2 retries, 5 min delay |

The stored procedures are identical between local and Azure deployments.
Only the orchestration layer changes.

---

## Design Decisions

**Why PythonOperator throughout instead of MsSqlOperator?**

Airflow's `MsSqlOperator` executes SQL directly but gives limited control
over error handling and result processing. Using `PythonOperator` with
explicit `pyodbc` connections means:
- Full control over connection lifecycle
- Ability to read result sets and push to XCom
- Clear separation: orchestration logic in Python, business logic in SQL
- Easier to unit test individual functions

**Why linear dependencies instead of parallel tasks?**

Validation must complete before loading. Loading must complete before archiving.
There is no opportunity for meaningful parallelism in this pipeline without
introducing complexity that the data volume doesn't justify.

At scale — processing hundreds of stores — the ingest step could be parallelised
with Airflow's dynamic task mapping. The current design is deliberately simple
and easy to reason about.

**Why store business logic in stored procedures rather than Python?**

SQL Server admins and data analysts can read and modify stored procedures
without touching the pipeline code. The transformation logic is independently
testable — you can call `EXEC dbo.usp_validate_staged_rows` directly in Azure
Data Studio without running Airflow at all. This separation of concerns is
particularly valuable in team environments.
