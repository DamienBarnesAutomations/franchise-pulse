# Error Handling

## Philosophy

Bad data and missing files are expected, not exceptional. The pipeline is
designed so that **one store's problems never affect another store's data**,
and **bad rows never silently corrupt the fact table**.

Three distinct failure modes require three distinct responses:

---

## Layer 1 — File Level

**Failure mode:** A store doesn't deliver its CSV by pipeline run time.

**Response:** Log it, continue processing all other stores.

**Implementation:**
```sql
-- missing_file_log records every absent file
INSERT INTO dbo.missing_file_log (run_id, run_date, store_id, expected_file)
VALUES (?, ?, ?, ?)
```

The `scan_files` task loops over all 12 expected filenames. Missing files are
written to `missing_file_log` and the `files_missing` counter on `pipeline_run_log`
is incremented. The file is simply skipped — no exception is raised.

**The only hard failure at file level:** If zero files are found for a date,
the pipeline raises a `ValueError` and stops. Zero files means something is
systemically wrong — the pipeline shouldn't silently produce an empty load.

**Test case:** S07 Limerick is configured to go silent for 3 consecutive days
in the sample data. The pipeline processes all 11 other stores normally on
those days and logs 3 missing file records per run.

**ADF equivalent:** If Condition activity checking blob existence. Missing files
written via Stored Procedure activity. Pipeline continues via the True/False
branch of the condition.

---

## Layer 2 — Row Level

**Failure mode:** A file loads successfully but contains invalid data.

**Response:** Quarantine the bad rows with a specific rejection reason.
Load all clean rows from the same file normally.

**Implementation:**

Staging table accepts all data as VARCHAR regardless of content. Validation
runs as a separate step after ingest:

```
V01  transaction_id is not null or empty
V02  store_id exists in dim_store
V03  transaction_date is a valid datetime
V04  product_sku exists in dim_product
V05  unit_price is a valid positive decimal
V06  quantity is a valid integer
V07  payment_method exists in dim_payment_method
DUP  row hash already exists (duplicate detection)
```

Each rule adds a specific rejection reason:
```
V05: unit_price is not a valid positive decimal - 3.80X
```

Failed rows get `validation_status = FAIL`. They stay in staging for 7 days
for investigation before being purged. Clean rows get `validation_status = PASS`
and proceed to the fact load.

**Deduplication:** A SHA2_256 hash of `transaction_id + store_id + transaction_date`
is generated for every staged row. A row is a duplicate if:
- Its hash matches an existing `transaction_id` in `fact_sales` (previously loaded)
- Its hash matches another row in the current staging batch (resent file)

S11 Sligo resends ~2% of rows as duplicates in the sample data. These are caught
and logged without any fact table impact.

**Test cases:**
- S03 Cork City: ~2% of rows have malformed prices (`3.80X`) → caught by V05
- S11 Sligo: ~2% of rows are duplicates → caught by hash deduplication
- S09 Kilkenny: UTF-16 encoding → handled at ingest, not a validation failure

**ADF equivalent:** Data Flow activity with conditional split. Error rows routed
to a rejection sink in the ADLS silver layer with error reason column appended.

---

## Layer 3 — Pipeline Level

**Failure mode:** A task throws an unhandled exception.

**Response:** Retry twice with a 5-minute delay. If all retries fail, mark
the run as failed and alert.

**Implementation:**

Airflow default args:
```python
"retries":      2,
"retry_delay":  timedelta(minutes=5),
```

If all retries are exhausted, the task and DAG run are marked `failed` in
Airflow's metadata database. The `pipeline_run_log` record retains `RUNNING`
status — a `RUNNING` record with no corresponding `SUCCESS` is itself an
indicator of failure when querying the log.

For a production deployment, Airflow's email alerting would be configured
to notify on DAG failure. The `pipeline_run_log` table provides the same
visibility locally.

**ADF equivalent:** Activity retry settings (2 retries, 5 minute interval).
Pipeline failure triggers an Azure Monitor alert which sends email via
Logic App or Action Group.

---

## Intentional Data Quality Issues in Sample Data

The data generator (`scripts/generate_data.py`) builds in specific problems
to test each error handling path:

| Store | Issue | Layer | Rule |
|---|---|---|---|
| S03 Cork City | ~2% of rows have malformed unit prices (`3.80X`) | Row | V05 |
| S07 Limerick | Silent for 3 consecutive days | File | Missing file log |
| S09 Kilkenny | Files encoded as UTF-16 instead of UTF-8 | Ingest | Encoding detection |
| S11 Sligo | ~2% of rows are duplicates (resent file simulation) | Row | Hash dedup |
| All stores | ~1% of rows have null cashier_id | Row | Allowed (nullable) |
| All stores | ~0.5% of rows have negative quantity (refunds) | Row | Allowed (valid) |

Null cashier_id and negative quantities are not validation failures — they are
valid business scenarios (cashier not logged in, refund transaction). The pipeline
handles them correctly rather than rejecting them.

---

## Querying Error State

**View rejection summary for a specific run:**
```sql
SELECT
    source_file,
    rejection_reason,
    COUNT(*) AS rejected_rows
FROM dbo.stg_sales
WHERE validation_status = 'FAIL'
GROUP BY source_file, rejection_reason
ORDER BY rejected_rows DESC;
```

**View missing files:**
```sql
SELECT * FROM dbo.missing_file_log ORDER BY logged_at DESC;
```

**View pipeline run outcomes:**
```sql
SELECT
    run_date, status,
    files_expected, files_processed, files_missing,
    rows_staged, rows_passed, rows_rejected, rows_duplicates
FROM dbo.pipeline_run_log
ORDER BY run_id DESC;
```
