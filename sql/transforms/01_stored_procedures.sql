-- =============================================================
-- FranchisePulse — Transformation Stored Procedures
-- File: sql/transforms/01_stored_procedures.sql
-- =============================================================

USE FranchisePulse;
GO

-- =============================================================
-- usp_start_pipeline_run
-- Call at the beginning of each pipeline execution
-- Creates a run log record and returns the run_id
-- =============================================================
CREATE OR ALTER PROCEDURE dbo.usp_start_pipeline_run
    @run_date           DATE,
    @files_expected     SMALLINT,
    @run_id             BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.pipeline_run_log (run_date, files_expected, status)
    VALUES (@run_date, @files_expected, 'RUNNING');

    SET @run_id = SCOPE_IDENTITY();
END;
GO


-- =============================================================
-- usp_end_pipeline_run
-- Call at the end of each pipeline execution
-- Updates the run log with final counts and status
-- =============================================================
CREATE OR ALTER PROCEDURE dbo.usp_end_pipeline_run
    @run_id         BIGINT,
    @status         VARCHAR(20),    -- SUCCESS or FAILED
    @error_message  VARCHAR(1000)   = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.pipeline_run_log
    SET
        pipeline_end    = SYSUTCDATETIME(),
        status          = @status,
        files_processed = (SELECT COUNT(DISTINCT source_file) FROM dbo.stg_sales WHERE validation_status != 'PENDING'),
        rows_staged     = (SELECT COUNT(*) FROM dbo.stg_sales),
        rows_passed     = (SELECT COUNT(*) FROM dbo.stg_sales WHERE validation_status = 'PASS'),
        rows_rejected   = (SELECT COUNT(*) FROM dbo.stg_sales WHERE validation_status = 'FAIL'),
        rows_duplicates = (SELECT COUNT(*) FROM dbo.stg_sales WHERE rejection_reason = 'DUPLICATE'),
        rows_loaded     = (SELECT COUNT(*) FROM dbo.stg_sales WHERE validation_status = 'PASS'),
        error_message   = @error_message
    WHERE run_id = @run_id;
END;
GO


-- =============================================================
-- usp_log_missing_file
-- Call for each expected file that was not found
-- =============================================================
CREATE OR ALTER PROCEDURE dbo.usp_log_missing_file
    @run_id         BIGINT,
    @run_date       DATE,
    @store_id       VARCHAR(10),
    @expected_file  VARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.missing_file_log (run_id, run_date, store_id, expected_file)
    VALUES (@run_id, @run_date, @store_id, @expected_file);

    -- Increment missing file count on the run log
    UPDATE dbo.pipeline_run_log
    SET files_missing = ISNULL(files_missing, 0) + 1
    WHERE run_id = @run_id;
END;
GO


-- =============================================================
-- usp_validate_staged_rows
-- Runs validation rules against stg_sales
-- Marks each row PASS or FAIL with a rejection reason
-- Must be called after rows are loaded into stg_sales
--
-- Validation rules:
--   V01  transaction_id is not null or empty
--   V02  store_id exists in dim_store
--   V03  transaction_date is a valid datetime
--   V04  product_sku exists in dim_product
--   V05  unit_price is a valid positive decimal
--   V06  quantity is a valid integer
--   V07  payment_method exists in dim_payment_method
--   V08  duplicate row_hash (duplicate transaction)
-- =============================================================
CREATE OR ALTER PROCEDURE dbo.usp_validate_staged_rows
AS
BEGIN
    SET NOCOUNT ON;

    -- Step 1 — Generate row hash for deduplication
    -- Hash is based on natural key columns only
    UPDATE dbo.stg_sales
    SET row_hash = CONVERT(
        CHAR(64),
        HASHBYTES(
            'SHA2_256',
            ISNULL(transaction_id, '') + '|' +
            ISNULL(store_id, '')       + '|' +
            ISNULL(transaction_date, '')
        ),
        2   -- binary to hex string
    )
    WHERE validation_status = 'PENDING';

    -- Step 2 — Mark duplicates first
    -- A row is a duplicate if its hash already exists in fact_sales
    -- OR appears more than once in the current staging batch
    UPDATE s
    SET
        validation_status = 'FAIL',
        rejection_reason  = 'DUPLICATE'
    FROM dbo.stg_sales s
    WHERE s.validation_status = 'PENDING'
    AND (
        -- Already loaded in a previous run
        EXISTS (
            SELECT 1 FROM dbo.fact_sales f
            WHERE f.transaction_id = s.transaction_id
        )
        OR
        -- Duplicate within current batch (keep lowest stg_id)
        s.stg_id > (
            SELECT MIN(s2.stg_id)
            FROM dbo.stg_sales s2
            WHERE s2.row_hash = s.row_hash
            AND s2.validation_status = 'PENDING'
        )
    );

    -- Step 3 — Run validation rules against remaining PENDING rows
    -- V01: transaction_id missing
    UPDATE dbo.stg_sales
    SET validation_status = 'FAIL',
        rejection_reason  = 'V01: transaction_id is null or empty'
    WHERE validation_status = 'PENDING'
    AND (transaction_id IS NULL OR LTRIM(RTRIM(transaction_id)) = '');

    -- V02: store_id not in dim_store
    UPDATE dbo.stg_sales
    SET validation_status = 'FAIL',
        rejection_reason  = 'V02: store_id not found in dim_store - ' + ISNULL(store_id, 'NULL')
    WHERE validation_status = 'PENDING'
    AND store_id NOT IN (SELECT store_id FROM dbo.dim_store);

    -- V03: transaction_date not a valid datetime
    UPDATE dbo.stg_sales
    SET validation_status = 'FAIL',
        rejection_reason  = 'V03: transaction_date is not a valid datetime - ' + ISNULL(transaction_date, 'NULL')
    WHERE validation_status = 'PENDING'
    AND TRY_CONVERT(DATETIME2, transaction_date) IS NULL;

    -- V04: product_sku not in dim_product
    UPDATE dbo.stg_sales
    SET validation_status = 'FAIL',
        rejection_reason  = 'V04: product_sku not found in dim_product - ' + ISNULL(product_sku, 'NULL')
    WHERE validation_status = 'PENDING'
    AND product_sku NOT IN (SELECT product_sku FROM dbo.dim_product);

    -- V05: unit_price not a valid positive decimal (catches S03 malformed prices)
    UPDATE dbo.stg_sales
    SET validation_status = 'FAIL',
        rejection_reason  = 'V05: unit_price is not a valid positive decimal - ' + ISNULL(unit_price, 'NULL')
    WHERE validation_status = 'PENDING'
    AND (
        TRY_CONVERT(DECIMAL(10,2), unit_price) IS NULL
        OR TRY_CONVERT(DECIMAL(10,2), unit_price) < 0
    );

    -- V06: quantity not a valid integer
    UPDATE dbo.stg_sales
    SET validation_status = 'FAIL',
        rejection_reason  = 'V06: quantity is not a valid integer - ' + ISNULL(quantity, 'NULL')
    WHERE validation_status = 'PENDING'
    AND TRY_CONVERT(SMALLINT, quantity) IS NULL;

    -- V07: payment_method not in dim_payment_method
    UPDATE dbo.stg_sales
    SET validation_status = 'FAIL',
        rejection_reason  = 'V07: payment_method not found in dim_payment_method - ' + ISNULL(payment_method, 'NULL')
    WHERE validation_status = 'PENDING'
    AND payment_method NOT IN (SELECT payment_method FROM dbo.dim_payment_method);

    -- Step 4 — Everything still PENDING passed all rules
    UPDATE dbo.stg_sales
    SET validation_status = 'PASS'
    WHERE validation_status = 'PENDING';

    -- Step 5 — Return validation summary
    SELECT
        validation_status,
        COUNT(*)        AS row_count
    FROM dbo.stg_sales
    GROUP BY validation_status
    ORDER BY validation_status;
END;
GO


-- =============================================================
-- usp_load_fact_sales
-- Loads validated rows from stg_sales into fact_sales
-- Uses MERGE to handle any late-arriving duplicates
-- Only processes rows with validation_status = 'PASS'
-- =============================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_fact_sales
    @rows_loaded    INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.fact_sales AS target
    USING (
        SELECT
            -- Resolve dimension keys
            CONVERT(INT, FORMAT(TRY_CONVERT(DATETIME2, s.transaction_date), 'yyyyMMdd'))
                                                AS date_key,
            ds.store_key,
            dp.product_key,
            dm.payment_key,
            s.transaction_id,
            NULLIF(LTRIM(RTRIM(s.cashier_id)), '')
                                                AS cashier_id,
            TRY_CONVERT(SMALLINT,   s.quantity)         AS quantity,
            TRY_CONVERT(DECIMAL(10,2), s.unit_price)    AS unit_price,
            TRY_CONVERT(DECIMAL(10,2), s.discount_applied)
                                                AS discount_applied,
            s.source_file
        FROM dbo.stg_sales s
        JOIN dbo.dim_store ds
            ON ds.store_id      = s.store_id
        JOIN dbo.dim_product dp
            ON dp.product_sku   = s.product_sku
        JOIN dbo.dim_payment_method dm
            ON dm.payment_method = s.payment_method
        WHERE s.validation_status = 'PASS'
    ) AS source
    ON target.transaction_id = source.transaction_id

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            date_key, store_key, product_key, payment_key,
            transaction_id, cashier_id,
            quantity, unit_price, discount_applied,
            source_file
        )
        VALUES (
            source.date_key, source.store_key, source.product_key, source.payment_key,
            source.transaction_id, source.cashier_id,
            source.quantity, source.unit_price, source.discount_applied,
            source.source_file
        );

    SET @rows_loaded = @@ROWCOUNT;
END;
GO


-- =============================================================
-- usp_clear_staging
-- Clears the staging table before each pipeline run
-- Keeps the last 7 days of rejected rows for investigation
-- =============================================================
CREATE OR ALTER PROCEDURE dbo.usp_clear_staging
AS
BEGIN
    SET NOCOUNT ON;

    -- Archive rejected rows older than 7 days then clear all
    DELETE FROM dbo.stg_sales
    WHERE load_timestamp < DATEADD(DAY, -7, SYSUTCDATETIME())
    AND validation_status = 'FAIL';

    -- Clear all PASS and PENDING rows regardless of age
    DELETE FROM dbo.stg_sales
    WHERE validation_status IN ('PASS', 'PENDING');

    SELECT COUNT(*) AS remaining_rejected_rows FROM dbo.stg_sales;
END;
GO


-- =============================================================
-- usp_get_pipeline_summary
-- Returns a summary of the last N pipeline runs
-- Used for monitoring and the Airflow callback
-- =============================================================
CREATE OR ALTER PROCEDURE dbo.usp_get_pipeline_summary
    @last_n_runs    INT = 7
AS
BEGIN
    SET NOCOUNT ON;

    SELECT TOP (@last_n_runs)
        run_id,
        run_date,
        status,
        files_expected,
        files_processed,
        files_missing,
        rows_staged,
        rows_passed,
        rows_rejected,
        rows_duplicates,
        rows_loaded,
        DATEDIFF(SECOND, pipeline_start, pipeline_end)  AS duration_seconds,
        error_message
    FROM dbo.pipeline_run_log
    ORDER BY run_id DESC;
END;
GO


-- =============================================================
-- Verify — list all stored procedures
-- =============================================================
SELECT
    name            AS procedure_name,
    create_date,
    modify_date
FROM sys.procedures
WHERE schema_id = SCHEMA_ID('dbo')
ORDER BY name;
GO