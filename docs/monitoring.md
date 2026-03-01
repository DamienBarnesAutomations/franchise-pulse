# Monitoring

## Two Audiences, Two Questions

Operational monitoring and data quality monitoring serve different people
and answer different questions. They are kept separate deliberately.

**Operations** asks: *Did the pipeline run? Did it finish? How long did it take?*

**Data / Finance** asks: *Can I trust the numbers? Are any stores missing?
What percentage of rows were rejected?*

Mixing these into a single view produces something that's not quite useful
for either audience.

---

## Operational Monitoring

### pipeline_run_log

Every pipeline execution writes one record:

```sql
SELECT
    run_id,
    run_date,
    status,                 -- RUNNING / SUCCESS / FAILED
    pipeline_start,
    pipeline_end,
    DATEDIFF(SECOND, pipeline_start, pipeline_end)  AS duration_seconds,
    files_expected,
    files_processed,
    files_missing,
    error_message
FROM dbo.pipeline_run_log
ORDER BY run_id DESC;
```

A healthy run looks like:
```
run_id  run_date    status   duration  expected  processed  missing
------  ----------  -------  --------  --------  ---------  -------
12      2026-02-28  SUCCESS  9         12        12         0
11      2026-02-27  SUCCESS  8         12        12         0
10      2026-02-26  SUCCESS  11        12        11         1    ← S07 silent
```

**What to watch:**
- Any `status != SUCCESS` — investigate immediately
- `duration_seconds` trending upward over time — data volume growing, may need tuning
- `files_missing > 0` — check `missing_file_log` for which store

### Detecting a Failed Run

A `RUNNING` record with no corresponding `SUCCESS` indicates the pipeline
died without completing `end_run`:

```sql
SELECT *
FROM dbo.pipeline_run_log
WHERE status = 'RUNNING'
AND pipeline_start < DATEADD(HOUR, -1, SYSUTCDATETIME());
```

Any result here means a run started over an hour ago and never finished.

---

## Data Quality Monitoring

### Rejection Rate by Store

```sql
SELECT
    source_file,
    COUNT(*)                                                AS total_rows,
    SUM(CASE WHEN validation_status = 'PASS' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN validation_status = 'FAIL' THEN 1 ELSE 0 END) AS failed,
    ROUND(
        SUM(CASE WHEN validation_status = 'FAIL' THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100, 2
    )                                                       AS rejection_rate_pct
FROM dbo.stg_sales
GROUP BY source_file
ORDER BY rejection_rate_pct DESC;
```

A normal rejection rate is 1-3% (S03's malformed prices, occasional bad rows).
A store suddenly showing 20%+ rejection rate signals a POS system issue.

### Rejection Reasons Breakdown

```sql
SELECT
    rejection_reason,
    COUNT(*)    AS occurrences
FROM dbo.stg_sales
WHERE validation_status = 'FAIL'
GROUP BY rejection_reason
ORDER BY occurrences DESC;
```

This tells you *why* rows are being rejected, not just that they are.
Useful for identifying systematic data quality problems at source.

### Missing File History

```sql
SELECT
    run_date,
    store_id,
    expected_file,
    logged_at
FROM dbo.missing_file_log
ORDER BY run_date DESC, store_id;
```

If S07 appears missing for 3+ consecutive days, that's a known test scenario.
Any other store appearing repeatedly warrants investigation.

### Duplicate Rate

```sql
SELECT
    source_file,
    COUNT(*) AS duplicate_rows
FROM dbo.stg_sales
WHERE rejection_reason = 'DUPLICATE'
GROUP BY source_file
ORDER BY duplicate_rows DESC;
```

S11 Sligo will show duplicates regularly — this is expected. Any other store
showing duplicates may be resending files or have a POS configuration issue.

---

## Pipeline Summary — Last 7 Runs

```sql
EXEC dbo.usp_get_pipeline_summary @last_n_runs = 7;
```

Returns a quick overview suitable for a daily ops check.

---

## Production Azure Monitoring

In a production Azure deployment this local monitoring would be augmented with:

**Azure Monitor Alerts**
- Alert if ADF pipeline fails → email via Action Group
- Alert if pipeline duration exceeds 30 minutes → possible performance issue
- Alert if `files_missing > 2` → multiple stores failing to deliver

**ADF Monitor Dashboard**
- Built-in pipeline run history with task-level drill down
- Gantt chart of task durations per run
- Trigger history and upcoming scheduled runs

**Separation remains the same** — ADF Monitor covers operational health,
`pipeline_run_log` and `stg_sales` queries cover data quality. Two audiences,
two tools.

---

## SLA Definition

| Metric | Target |
|---|---|
| Pipeline completion | By 07:00 UTC daily (1 hour from trigger) |
| Rejection rate | < 5% per store per day |
| Missing files | 0 per day (alert on any missing) |
| Duplicate rate | < 3% (S11 expected, others alert) |
| fact_sales freshness | Previous day's data available by 07:00 UTC |
