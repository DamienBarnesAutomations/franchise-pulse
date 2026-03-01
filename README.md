# FranchisePulse — Azure Retail Data Pipeline

A production-grade data pipeline built to ingest, validate, transform and aggregate
daily POS sales data from 12 franchise coffee shop locations across Ireland.

Built locally using Docker to simulate an Azure cloud architecture:
**Azure Data Lake → Azure Data Factory → Azure SQL Database**.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│                                                                     │
│  S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12                  │
│  └─────────────────────────────────────────────┘                   │
│            Daily CSV exports (POS systems)                          │
│            ~250-380 rows per store per day                          │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   STORAGE LAYER                                     │
│              (Simulates Azure Data Lake Gen2)                       │
│                                                                     │
│   /data/raw/YYYY-MM-DD/       ← files land here                    │
│   /data/bronze/               ← validated copies                   │
│   /data/silver/               ← quarantined rejected rows          │
│   /data/archive/YYYY-MM-DD/   ← processed files moved here         │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  ORCHESTRATION LAYER                                │
│           Apache Airflow  (Simulates Azure Data Factory)            │
│                                                                     │
│   DAG: franchisepulse_daily_pipeline                                │
│   Schedule: 06:00 UTC daily                                         │
│                                                                     │
│   [start_run] → [scan_files] → [ingest_files] → [validate_rows]    │
│                                                                     │
│   [validate_rows] → [load_fact] → [archive_files] → [end_run]      │
│                                                                     │
│   • Missing files logged, pipeline continues                        │
│   • Bad rows quarantined with rejection reason                      │
│   • Full audit trail written to pipeline_run_log                    │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   DATABASE LAYER                                    │
│              SQL Server 2022  (Simulates Azure SQL)                 │
│                                                                     │
│   STAGING          stg_sales                                        │
│                    └─ raw load, all VARCHAR                         │
│                    └─ row hash for deduplication                    │
│                    └─ validation_status + rejection_reason          │
│                                                                     │
│   STAR SCHEMA      fact_sales                                       │
│                    ├─ dim_store                                     │
│                    ├─ dim_product                                   │
│                    ├─ dim_date                                       │
│                    └─ dim_payment_method                            │
│                                                                     │
│   MONITORING       pipeline_run_log                                 │
│                    missing_file_log                                 │
│                                                                     │
│   REPORTING        vw_daily_sales_by_store                          │
│   VIEWS            vw_weekly_product_mix                            │
│                    vw_regional_revenue_summary                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Azure Equivalent Architecture

| Local Component | Azure Equivalent |
|---|---|
| Local folders (`/data/*`) | Azure Data Lake Storage Gen2 |
| Apache Airflow (Docker) | Azure Data Factory |
| SQL Server 2022 (Docker) | Azure SQL Database |
| Airflow DAG JSON export | ADF Pipeline ARM template |
| Airflow monitoring UI | ADF Monitor |

The pipeline is designed to be Azure-deployable with minimal changes.
ADF pipeline JSON is included in `/adf/`.

---

## Project Structure

```
franchise-pulse/
├── dags/
│   └── franchisepulse_pipeline.py    # Airflow DAG — 7-task daily pipeline
├── sql/
│   ├── ddl/
│   │   ├── 01_dimensions.sql         # dim_store, dim_product, dim_date, dim_payment_method
│   │   └── 02_fact_and_staging.sql   # fact_sales, stg_sales, pipeline_run_log, missing_file_log
│   ├── transforms/
│   │   └── 01_stored_procedures.sql  # 6 stored procedures — validation, load, logging
│   └── views/
│       └── 01_reporting_views.sql    # 3 reporting views — operational, product, regional
├── scripts/
│   └── generate_data.py              # Generates 90 days of realistic messy sales data
├── data/
│   ├── raw/                          # Source CSVs land here (gitignored)
│   ├── bronze/                       # Validated copies
│   ├── silver/                       # Rejected rows
│   └── archive/                      # Processed files
├── docs/
│   ├── data-model.md                 # Star schema design decisions
│   ├── pipeline-design.md            # Orchestration architecture
│   ├── error-handling.md             # Three-layer error handling approach
│   ├── monitoring.md                 # Pipeline observability
│   └── scaling.md                   # How this scales to Azure production
├── docker-compose.yml                # Full local environment — SQL Server + Airflow
├── requirements.txt                  # Python dependencies
├── .gitignore
└── README.md
```

---

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.12+
- Azure Data Studio or SSMS (for SQL Server)

### 1. Clone and set up

```bash
git clone https://github.com/DamienBarnesAutomations/franchise-pulse.git
cd franchise-pulse

# Create data folder structure
New-Item -ItemType Directory -Path data/raw, data/bronze, data/silver, data/archive -Force
```

### 2. Start the Docker environment

```bash
docker compose up airflow-init
# Wait for "Airflow initialised."

docker compose up -d
```

Services:
- Airflow UI → http://localhost:8080 (admin / admin)
- SQL Server → localhost:1433 (sa / FranchisePulse2024!)

### 3. Set up the database

Connect to SQL Server and run in order:

```
sql/ddl/01_dimensions.sql
sql/ddl/02_fact_and_staging.sql
sql/transforms/01_stored_procedures.sql
sql/views/01_reporting_views.sql
```

### 4. Generate sample data

```bash
pip install faker
python scripts/generate_data.py
```

Generates 90 days of sales data across 12 stores (~280,000 rows) with intentional
data quality issues for pipeline testing.

### 5. Run the pipeline

Open http://localhost:8080, find `franchisepulse_daily_pipeline` and trigger it.

Or trigger from CLI:
```bash
docker exec -it franchisepulse-airflow-scheduler \
  airflow dags trigger franchisepulse_daily_pipeline
```

---

## Data Model

Star schema with one fact table and four dimension tables.

```
dim_date ──────────────────┐
dim_store ─────────────────┤
dim_product ───────────────┼──► fact_sales
dim_payment_method ────────┘
```

**fact_sales** — one row per transaction line item. Surrogate keys to all
dimensions. Computed columns for `gross_revenue`, `discount_amount`, and
`net_revenue` ensure calculated values never go out of sync with their inputs.

**Staging table** — all columns VARCHAR on ingest. Type casting happens after
validation, not before. This prevents bulk load failures on malformed data and
gives the pipeline a chance to quarantine bad rows with a specific rejection reason
rather than failing the entire file.

**Row hashing** — SHA2_256 hash of `transaction_id + store_id + transaction_date`
detects duplicates both within a batch and against previously loaded data. Store S11
resends ~2% of rows as duplicates — these are caught and logged without failing
the pipeline or corrupting the fact table.

---

## Orchestration Design

The Airflow DAG mirrors an Azure Data Factory pipeline structure:

| ADF Concept | Local Implementation |
|---|---|
| Pipeline trigger | Airflow schedule `0 6 * * *` |
| ForEach activity | Python loop over store file list |
| Copy activity | Python CSV reader → SQL insert |
| Stored procedure activity | `cursor.execute("EXEC dbo.usp_...")` |
| Fault tolerance | Missing files logged, not raised |
| Pipeline run history | `pipeline_run_log` table |

**Design decision:** Child tasks are kept as simple Python functions rather than
using Airflow's `MsSqlOperator`. This keeps the SQL logic in stored procedures
(version controlled, testable independently) and the orchestration logic in Python
(readable, debuggable). The DAG is the coordinator — not the place for business logic.

---

## Error Handling

Three layers, three different failure modes:

**File level** — if a store doesn't deliver a file, it's logged to `missing_file_log`
with the run_id, date, store_id and expected filename. The pipeline continues
processing all other stores. S07 Limerick is configured to go silent for 3 days
in the sample data to test this path.

**Row level** — every row is staged as VARCHAR first, then run through 8 validation
rules. Failures are marked with `validation_status = 'FAIL'` and a specific
`rejection_reason` (e.g. `V05: unit_price is not a valid positive decimal - 3.80X`).
Rejected rows stay in staging for 7 days for investigation. S03 Cork City generates
~2% malformed price values to exercise this path.

**Pipeline level** — Airflow retries failed tasks twice with a 5-minute delay.
`pipeline_run_log` records start time, end time, status, and row counts for every
run. A failed run leaves a `FAILED` status record so nothing goes silently missing.

---

## Monitoring

Every pipeline run writes a record to `pipeline_run_log`:

```sql
SELECT * FROM dbo.pipeline_run_log ORDER BY run_id DESC;
```

Missing file alerts:
```sql
SELECT * FROM dbo.missing_file_log ORDER BY logged_at DESC;
```

Data quality check — rejection rate by store:
```sql
SELECT
    store_id,
    COUNT(*)                                            AS total_rows,
    SUM(CASE WHEN validation_status = 'FAIL' THEN 1 ELSE 0 END) AS rejected,
    ROUND(
        SUM(CASE WHEN validation_status = 'FAIL' THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100, 2
    )                                                   AS rejection_rate_pct
FROM dbo.stg_sales
GROUP BY store_id
ORDER BY rejection_rate_pct DESC;
```

In a production Azure deployment, Azure Monitor alerts would be configured to
email on pipeline failure or if duration exceeds an SLA threshold. The
`pipeline_run_log` table provides the same data locally.

---

## Scaling Considerations

**Volume** — current design handles ~50,000 rows/day comfortably in SQL Server.
At 10x volume, the transformation layer moves to Azure Synapse Analytics.
The pipeline structure, stored procedures, and star schema are identical —
only the compute layer changes.

**New locations** — adding a new store requires one INSERT into `dim_store`.
The ForEach loop in the DAG picks up the new store's files automatically on the
next run. No pipeline changes needed.

**Latency** — currently daily batch at 06:00 UTC. Moving to near-real-time
requires replacing CSV file drops with Event Hub ingestion and switching the
ADF/Airflow trigger from schedule-based to event-based (file arrival trigger).
The transformation and loading logic is unchanged.

**Schema changes** — new columns are added to staging first (always VARCHAR),
validated, then promoted to the fact table. This prevents source schema changes
from breaking the pipeline mid-run.

---

## Tech Stack

| Layer | Local | Azure Equivalent |
|---|---|---|
| Orchestration | Apache Airflow 2.8 | Azure Data Factory |
| Database | SQL Server 2022 Developer | Azure SQL Database |
| Storage | Local filesystem | Azure Data Lake Gen2 |
| Containerisation | Docker Compose | — |
| Language | Python 3.12, T-SQL | Python, T-SQL |

---

## Interview Notes

This project demonstrates:

- **Pipeline engineering** — end to end data flow from raw files to reporting layer
- **Data modeling** — star schema design, surrogate keys, computed columns
- **Error handling** — three-layer approach, no silent failures
- **Orchestration** — DAG design, task dependencies, retry logic
- **SQL depth** — MERGE statements, window functions, stored procedures, views
- **Monitoring** — audit trail, data quality metrics, operational observability
- **Azure literacy** — architecture maps directly to ADF + ADLS + Azure SQL

---

*Built as Portfolio Project 3 — Data & Systems Engineer with Azure pipeline experience.*
