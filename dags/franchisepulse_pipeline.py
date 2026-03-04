"""
FranchisePulse — Main Pipeline DAG
=====================================
Orchestrates the daily sales data pipeline for 12 franchise locations.

Schedule: Daily at 06:00 UTC
Processes: Previous day's sales CSVs from data/raw/<YYYY-MM-DD>/

Pipeline stages:
    1. start_run          — Open pipeline_run_log record
    2. scan_files         — Check which store files exist, log missing
    3. ingest_files       — Bulk load CSVs into stg_sales
    4. validate_rows      — Run usp_validate_staged_rows
    5. load_fact          — Run usp_load_fact_sales
    6. archive_files      — Move processed files raw/ → archive/
    7. end_run            — Close run log, print summary

Simulates Azure Data Factory pipeline behaviour:
    - Missing files logged but do not fail pipeline (like ADF fault tolerance)
    - Bad rows quarantined with rejection reason (like ADF data flow error rows)
    - Full audit trail in pipeline_run_log (like ADF monitor runs)
"""

import os
import csv
import shutil
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pyodbc
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# -----------------------------------------------------------
# Logger
# -----------------------------------------------------------
log = logging.getLogger(__name__)

# -----------------------------------------------------------
# Constants
# -----------------------------------------------------------
DATA_ROOT   = Path(os.environ.get("PIPELINE_DATA_ROOT", "/opt/airflow/pipeline_data"))
RAW_DIR     = DATA_ROOT / "raw"
BRONZE_DIR  = DATA_ROOT / "bronze"
SILVER_DIR  = DATA_ROOT / "silver"
ARCHIVE_DIR = DATA_ROOT / "archive"

STORE_IDS = [
    "S01", "S02", "S03", "S04", "S05", "S06",
    "S07", "S08", "S09", "S10", "S11", "S12"
]

DB_CONFIG = {
    "server":   "sqlserver",
    "port":     1433,
    "user":     "sa",
    "password": "FranchisePulse2024!",
    "database": "FranchisePulse",
}

# -----------------------------------------------------------
# Default DAG args
# -----------------------------------------------------------
default_args = {
    "owner":            "franchisepulse",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}


# -----------------------------------------------------------
# Helper — get DB connection
# NOTE: pyodbc uses ? as parameter marker, not %s
# -----------------------------------------------------------
def get_connection():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={DB_CONFIG['server']},{DB_CONFIG['port']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['user']};"
        f"PWD={DB_CONFIG['password']};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


# -----------------------------------------------------------
# Helper — get business date from execution context
# -----------------------------------------------------------
def get_run_date(context):
    execution_date = context["execution_date"]
    return (execution_date - timedelta(days=1)).date()


# -----------------------------------------------------------
# Task 1 — start_run
# -----------------------------------------------------------
def start_run(**context):
    run_date = get_run_date(context)
    log.info(f"Starting pipeline run for business date: {run_date}")

    conn   = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO dbo.pipeline_run_log (run_date, files_expected, status)
        OUTPUT INSERTED.run_id
        VALUES (?, ?, 'RUNNING')
    """, (str(run_date), len(STORE_IDS)))

    row    = cursor.fetchone()
    run_id = int(row[0])

    conn.commit()
    conn.close()

    log.info(f"Pipeline run started. run_id={run_id}, run_date={run_date}")
    context["ti"].xcom_push(key="run_id",   value=run_id)
    context["ti"].xcom_push(key="run_date", value=str(run_date))


# -----------------------------------------------------------
# Task 2 — scan_files
# -----------------------------------------------------------
def scan_files(**context):
    ti       = context["ti"]
    run_id   = ti.xcom_pull(key="run_id",   task_ids="start_run")
    run_date = ti.xcom_pull(key="run_date", task_ids="start_run")

    date_folder = RAW_DIR / run_date
    found_files = []
    missing     = []

    log.info(f"Scanning {date_folder} for store files...")

    conn   = get_connection()
    cursor = conn.cursor()

    for store_id in STORE_IDS:
        filename = f"{store_id}_sales_{run_date}.csv"
        filepath = date_folder / filename

        if filepath.exists():
            found_files.append(str(filepath))
            log.info(f"  [FOUND]   {filename}")
        else:
            missing.append(store_id)
            log.warning(f"  [MISSING] {filename}")
            cursor.execute("""
                INSERT INTO dbo.missing_file_log (run_id, run_date, store_id, expected_file)
                VALUES (?, ?, ?, ?)
            """, (run_id, run_date, store_id, filename))
            cursor.execute("""
                UPDATE dbo.pipeline_run_log
                SET files_missing = ISNULL(files_missing, 0) + 1
                WHERE run_id = ?
            """, (run_id,))

    conn.commit()
    conn.close()

    log.info(f"Scan complete. Found: {len(found_files)}, Missing: {len(missing)}")

    if not found_files:
        log.warning(f"No files found for {run_date}. Skipping.")
        ti.xcom_push(key="found_files", value=[])
        ti.xcom_push(key="missing", value=STORE_IDS)
        return

    ti.xcom_push(key="found_files", value=found_files)
    ti.xcom_push(key="missing",     value=missing)


# -----------------------------------------------------------
# Task 3 — ingest_files
# -----------------------------------------------------------
def ingest_files(**context):
    ti          = context["ti"]
    found_files = ti.xcom_pull(key="found_files", task_ids="scan_files")
    if not found_files:
        log.warning("No files to ingest. Skipping.")
        return

    conn   = get_connection()
    cursor = conn.cursor()

    cursor.execute("EXEC dbo.usp_clear_staging")
    conn.commit()

    total_rows = 0

    for filepath in found_files:
        path     = Path(filepath)
        filename = path.name
        store_id = filename.split("_")[0]

        log.info(f"Ingesting {filename}...")

        encoding = "utf-16" if store_id == "S09" else "utf-8"

        try:
            rows_loaded = 0
            batch       = []

            with open(filepath, "r", encoding=encoding) as f:
                reader = csv.DictReader(f)

                for row in reader:
                    batch.append((
                        row.get("transaction_id",  "") or None,
                        row.get("store_id",         "") or None,
                        row.get("transaction_date", "") or None,
                        row.get("product_sku",      "") or None,
                        row.get("product_name",     "") or None,
                        row.get("category",         "") or None,
                        row.get("quantity",         "") or None,
                        row.get("unit_price",       "") or None,
                        row.get("discount_applied", "") or None,
                        row.get("payment_method",   "") or None,
                        row.get("cashier_id",       "") or None,
                        filename,
                    ))

                    if len(batch) >= 500:
                        cursor.executemany("""
                            INSERT INTO dbo.stg_sales (
                                transaction_id, store_id, transaction_date,
                                product_sku, product_name, category,
                                quantity, unit_price, discount_applied,
                                payment_method, cashier_id, source_file
                            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                        """, batch)
                        rows_loaded += len(batch)
                        batch = []

                if batch:
                    cursor.executemany("""
                        INSERT INTO dbo.stg_sales (
                            transaction_id, store_id, transaction_date,
                            product_sku, product_name, category,
                            quantity, unit_price, discount_applied,
                            payment_method, cashier_id, source_file
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """, batch)
                    rows_loaded += len(batch)

            conn.commit()
            total_rows += rows_loaded
            log.info(f"  {filename} — {rows_loaded} rows staged")

        except Exception as e:
            log.error(f"  ERROR ingesting {filename}: {e}")
            conn.rollback()
            raise

    conn.close()
    log.info(f"Ingestion complete. Total rows staged: {total_rows}")
    context["ti"].xcom_push(key="rows_staged", value=total_rows)


# -----------------------------------------------------------
# Task 4 — validate_rows
# -----------------------------------------------------------
def validate_rows(**context):
    conn   = get_connection()
    cursor = conn.cursor()

    log.info("Running validation rules against staged rows...")
    cursor.execute("EXEC dbo.usp_validate_staged_rows")

    rows = cursor.fetchall()
    for row in rows:
        status, count = row
        log.info(f"  {status}: {count} rows")

    conn.commit()
    conn.close()


# -----------------------------------------------------------
# Task 5 — load_fact
# -----------------------------------------------------------
def load_fact(**context):
    conn   = get_connection()
    cursor = conn.cursor()

    log.info("Loading validated rows into fact_sales...")

    cursor.execute("""
        DECLARE @rows_loaded INT;
        EXEC dbo.usp_load_fact_sales @rows_loaded OUTPUT;
        SELECT @rows_loaded AS rows_loaded;
    """)

    row         = cursor.fetchone()
    rows_loaded = row[0] if row else 0

    conn.commit()
    conn.close()

    log.info(f"Fact load complete. Rows loaded: {rows_loaded}")
    context["ti"].xcom_push(key="rows_loaded", value=rows_loaded)


# -----------------------------------------------------------
# Task 6 — archive_files
# -----------------------------------------------------------
def archive_files(**context):
    ti          = context["ti"]
    run_date    = ti.xcom_pull(key="run_date",    task_ids="start_run")
    found_files = ti.xcom_pull(key="found_files", task_ids="scan_files")

    archive_date_folder = ARCHIVE_DIR / run_date
    archive_date_folder.mkdir(parents=True, exist_ok=True)

    for filepath in found_files:
        src  = Path(filepath)
        dest = archive_date_folder / src.name
        shutil.move(str(src), str(dest))
        log.info(f"  Archived {src.name} → archive/{run_date}/")

    log.info(f"Archiving complete. {len(found_files)} files moved.")

def run_dbt(**context):
    import subprocess
    result = subprocess.run(
        ["docker", "exec", "franchise-pulse-dbt", "dbt", "run"],
        capture_output=True,
        text=True
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise Exception(f"dbt run failed:\n{result.stderr}")

# -----------------------------------------------------------
# Task 7 — end_run
# -----------------------------------------------------------
def end_run(**context):
    ti     = context["ti"]
    run_id = ti.xcom_pull(key="run_id", task_ids="start_run")

    conn   = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE dbo.pipeline_run_log
        SET
            pipeline_end    = SYSUTCDATETIME(),
            status          = 'SUCCESS',
            files_processed = (SELECT COUNT(DISTINCT source_file) FROM dbo.stg_sales),
            rows_staged     = (SELECT COUNT(*) FROM dbo.stg_sales),
            rows_passed     = (SELECT COUNT(*) FROM dbo.stg_sales WHERE validation_status = 'PASS'),
            rows_rejected   = (SELECT COUNT(*) FROM dbo.stg_sales WHERE validation_status = 'FAIL'),
            rows_duplicates = (SELECT COUNT(*) FROM dbo.stg_sales WHERE rejection_reason = 'DUPLICATE'),
            rows_loaded     = (SELECT COUNT(*) FROM dbo.fact_sales WHERE source_file IN
                                (SELECT DISTINCT source_file FROM dbo.stg_sales))
        WHERE run_id = ?
    """, (run_id,))

    conn.commit()

    cursor.execute("""
        SELECT TOP 1
            run_id, run_date, status,
            files_expected, files_processed, files_missing,
            rows_staged, rows_passed, rows_rejected,
            rows_duplicates, rows_loaded,
            DATEDIFF(SECOND, pipeline_start, pipeline_end) AS duration_seconds
        FROM dbo.pipeline_run_log
        WHERE run_id = ?
    """, (run_id,))

    row = cursor.fetchone()
    if row:
        log.info("=" * 50)
        log.info("PIPELINE RUN SUMMARY")
        log.info("=" * 50)
        log.info(f"  Run ID          : {row[0]}")
        log.info(f"  Run Date        : {row[1]}")
        log.info(f"  Status          : {row[2]}")
        log.info(f"  Files Expected  : {row[3]}")
        log.info(f"  Files Processed : {row[4]}")
        log.info(f"  Files Missing   : {row[5]}")
        log.info(f"  Rows Staged     : {row[6]}")
        log.info(f"  Rows Passed     : {row[7]}")
        log.info(f"  Rows Rejected   : {row[8]}")
        log.info(f"  Rows Duplicates : {row[9]}")
        log.info(f"  Rows Loaded     : {row[10]}")
        log.info(f"  Duration (sec)  : {row[11]}")
        log.info("=" * 50)

    conn.close()


# -----------------------------------------------------------
# DAG definition
# -----------------------------------------------------------
with DAG(
    dag_id="franchisepulse_daily_pipeline",
    description="Daily sales pipeline for 12 FranchisePulse locations",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 12, 2),
    catchup=True,
    max_active_runs=1,
    tags=["franchisepulse", "sales", "daily"],
) as dag:

    t1_start_run = PythonOperator(
        task_id="start_run",
        python_callable=start_run,
    )

    t2_scan_files = PythonOperator(
        task_id="scan_files",
        python_callable=scan_files,
    )

    t3_ingest_files = PythonOperator(
        task_id="ingest_files",
        python_callable=ingest_files,
    )

    t4_validate_rows = PythonOperator(
        task_id="validate_rows",
        python_callable=validate_rows,
    )

    t5_load_fact = PythonOperator(
        task_id="load_fact",
        python_callable=load_fact,
    )

    t6_archive_files = PythonOperator(
        task_id="archive_files",
        python_callable=archive_files,
    )

    t7_end_run = PythonOperator(
        task_id="end_run",
        python_callable=end_run,
    )
    t8_dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt,
    )

    # Linear pipeline — each stage depends on previous success
    t1_start_run >> t2_scan_files >> t3_ingest_files >> t4_validate_rows >> t5_load_fact >> t6_archive_files >> t7_end_run >> t8_dbt_run