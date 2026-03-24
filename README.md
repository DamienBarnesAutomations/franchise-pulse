# FranchisePulse

FranchisePulse is a daily batch sales data pipeline for a coffee franchise, demonstrating a robust ELT (Extract, Load, Transform) architecture. It automates the collection of POS sales data from 12 franchise locations, performs thorough validation and error handling, and loads clean data into a dimensional data model for reporting and analysis. This project is a technical portfolio piece designed to showcase engineering expertise in orchestration (Airflow), data warehousing (SQL Server), and data modeling (dbt), reflecting real-world complexities such as varying file encodings, missing data events, and duplicate handling.

## Architecture

```text
                                 [ Orchestration: Apache Airflow ]
                                                │
       [ Source: POS CSVs ]                     │
       (Local File System)                      ▼
               │                [ Stage 1: Ingest (Python/pyodbc) ]
               │                                │
               └───────────────────► [ SQL Server: stg_sales ]
                                                │
                                                ▼
                                [ Stage 2: Validate (SQL Proc) ]
                                                │
                                                ▼
                                [ Stage 3: Load (SQL MERGE Proc) ]
                                                │
                                                ▼
                                [ Destination: fact_sales (Star Schema) ]
                                                │
                                                ▼
                                [ Final Stage: Transform (dbt) ]
                                                │
                                                ▼
                                [ Reporting Layer (SQL Views) ]
```

## Tech Stack

| Component | Technology | Purpose |
|---|---|---|
| Orchestration | Apache Airflow 2.8.1 | Manages task scheduling, dependencies, and retries. |
| Database | SQL Server 2022 | Primary data warehouse for staging and dimensional modeling. |
| Transformation | dbt | Semantic modeling and creation of reporting-ready views. |
| Ingestion | Python (pyodbc) | Handles bulk loading, CSV parsing, and encoding detection. |
| Environment | Docker Compose | Orchestrates the full stack (SQL Server, Airflow, dbt). |

## Project Structure

- `dags/franchisepulse_pipeline.py` — The core Airflow DAG orchestrating the 8-stage pipeline.
- `data/` — Local landing zone for raw, bronze, silver, and archived sales data.
- `dbt/` — dbt project files for modeling the final reporting layer.
- `docs/` — Detailed technical documentation for design, monitoring, and scaling.
- `sql/ddl/` — SQL scripts defining the dimensional model (Fact/Dimensions) and staging tables.
- `sql/transforms/` — Stored procedures for data validation and fact loading logic.
- `scripts/generate_data.py` — Utility to generate 90 days of realistic, messy sales data for testing.
- `docker-compose.yml` — Multi-container configuration for the entire pipeline stack.

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/[user]/franchise-pulse.git
cd franchise-pulse

# 2. Generate test data (simulates 90 days of sales)
python scripts/generate_data.py

# 3. Spin up the infrastructure
docker compose up -d

# 4. Monitor the pipeline
# Access Airflow UI at http://localhost:8080 (Login: admin / admin)
```

## How It Works

1. **start_run**: Opens a `pipeline_run_log` entry to track the audit trail for the batch run.
2. **scan_files**: Checks `data/raw/` for the current date's 12 store files; logs missing files as business events in `missing_file_log`.
3. **ingest_files**: Reads CSVs using `csv.DictReader` and bulk-inserts into `stg_sales`. Specifically handles UTF-16 encoding for store S09 Kilkenny.
4. **validate_rows**: Executes `usp_validate_staged_rows`, running 8 validation rules (e.g., duplicates, null checks, numeric formatting) and quarantines failures with rejection reasons.
5. **load_fact**: Runs `usp_load_fact_sales` to MERGE clean staging data into the `fact_sales` table while resolving surrogate keys from dimension tables.
6. **archive_files**: Moves processed files to `data/archive/` to keep the landing zone clean and ensure source file recoverability.
7. **end_run**: Finalizes the run log with success metrics, including row counts and total duration.
8. **dbt_run**: Triggers a dbt container to rebuild downstream semantic views for the final reporting layer.

## Design Decisions

- **Stored Procedures for Business Logic**: Keeping validation and transformation in SQL ensures the logic is independently testable, portable across different orchestrators (like Azure Data Factory), and accessible to SQL-proficient analysts.
- **Python-Based Ingestion (pyodbc)**: Using `PythonOperator` with `pyodbc` instead of a generic SQL operator provides granular control over file-specific handling (like UTF-16 detection) and enhanced XCom-driven traceability.
- **Missing Files as Business Events**: The pipeline is designed to continue if a store file is missing, treating it as a store-level operational issue rather than a technical failure that blocks the entire company's data delivery.
- **Medallion Architecture (Staging to Fact)**: A clear separation between "dirty" staging and "clean" fact tables allows for exhaustive validation and quarantine without losing source data visibility.
- **XCom for Pipeline State**: Leveraging Airflow XComs to pass metadata like `run_id` and `found_files` between tasks ensures a cohesive audit trail and cross-task state management.

## What This Demonstrates

- **Production-Grade ETL/ELT**: Implementation of a resilient, idempotent pipeline handling common real-world edge cases like file encoding and missing data.
- **Data Modeling (Star Schema)**: Designing and populating a classic dimensional model with surrogate keys for optimized analytical reporting.
- **Orchestration & DevOps**: Managing complex dependencies and multi-service environment configuration via Airflow and Docker Compose.
- **Advanced SQL Engineering**: Using MERGE statements, window functions for deduplication, and modular stored procedures.
- **Defensive Engineering**: Built-in validation rules, encoding detection, and exhaustive audit logging for full operational transparency.
