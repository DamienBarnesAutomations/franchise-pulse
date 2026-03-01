-- =============================================================
-- FranchisePulse — Fact Table & Staging Table
-- File: sql/ddl/02_fact_and_staging.sql
-- =============================================================

USE FranchisePulse;
GO

-- -----------------------------------------------------------
-- fact_sales
-- One row per transaction line item
-- Foreign keys to all four dimension tables
-- Calculated revenue columns stored for query performance
-- -----------------------------------------------------------
CREATE TABLE dbo.fact_sales (
    sales_key           BIGINT IDENTITY(1,1)    NOT NULL,
    -- Dimension foreign keys
    date_key            INT                     NOT NULL,
    store_key           INT                     NOT NULL,
    product_key         INT                     NOT NULL,
    payment_key         INT                     NOT NULL,
    -- Degenerate dimensions (useful for lookups, not worth a dim table)
    transaction_id      VARCHAR(50)             NOT NULL,
    cashier_id          VARCHAR(20)             NULL,       -- nullable, ~1% missing
    -- Measures
    quantity            SMALLINT                NOT NULL,
    unit_price          DECIMAL(10,2)           NOT NULL,
    discount_applied    DECIMAL(10,2)           NOT NULL    DEFAULT 0.00,
    gross_revenue       AS (quantity * unit_price),                          -- computed
    discount_amount     AS (quantity * unit_price * discount_applied),       -- computed
    net_revenue         AS (quantity * unit_price * (1 - discount_applied)), -- computed
    -- Audit
    source_file         VARCHAR(255)            NOT NULL,
    loaded_at           DATETIME2               NOT NULL    DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_fact_sales PRIMARY KEY (sales_key),
    CONSTRAINT UQ_fact_sales_transaction UNIQUE (transaction_id),
    CONSTRAINT FK_fact_sales_date    FOREIGN KEY (date_key)    REFERENCES dbo.dim_date(date_key),
    CONSTRAINT FK_fact_sales_store   FOREIGN KEY (store_key)   REFERENCES dbo.dim_store(store_key),
    CONSTRAINT FK_fact_sales_product FOREIGN KEY (product_key) REFERENCES dbo.dim_product(product_key),
    CONSTRAINT FK_fact_sales_payment FOREIGN KEY (payment_key) REFERENCES dbo.dim_payment_method(payment_key)
);
GO

-- Indexes to support common reporting query patterns
CREATE NONCLUSTERED INDEX IX_fact_sales_date_store
    ON dbo.fact_sales (date_key, store_key)
    INCLUDE (net_revenue, quantity);
GO

CREATE NONCLUSTERED INDEX IX_fact_sales_product
    ON dbo.fact_sales (product_key)
    INCLUDE (net_revenue, quantity);
GO

-- -----------------------------------------------------------
-- stg_sales
-- Staging table — raw ingested rows land here first
-- Validated and transformed before loading into fact_sales
-- Bad rows stay here with rejection_reason populated
-- Cleared at the start of each pipeline run for that batch
-- -----------------------------------------------------------
CREATE TABLE dbo.stg_sales (
    stg_id              BIGINT IDENTITY(1,1)    NOT NULL,
    -- Raw source columns (all VARCHAR — we validate types after load)
    transaction_id      VARCHAR(50)             NULL,
    store_id            VARCHAR(10)             NULL,
    transaction_date    VARCHAR(30)             NULL,
    product_sku         VARCHAR(20)             NULL,
    product_name        VARCHAR(100)            NULL,
    category            VARCHAR(50)             NULL,
    quantity            VARCHAR(10)             NULL,
    unit_price          VARCHAR(20)             NULL,   -- VARCHAR catches S03 malformed prices
    discount_applied    VARCHAR(10)             NULL,
    payment_method      VARCHAR(50)             NULL,
    cashier_id          VARCHAR(20)             NULL,
    -- Pipeline metadata
    source_file         VARCHAR(255)            NOT NULL,
    load_timestamp      DATETIME2               NOT NULL    DEFAULT SYSUTCDATETIME(),
    row_hash            CHAR(64)                NULL,       -- SHA2_256 for deduplication
    -- Validation outcome
    validation_status   VARCHAR(10)             NOT NULL    DEFAULT 'PENDING',  -- PENDING / PASS / FAIL
    rejection_reason    VARCHAR(500)            NULL,

    CONSTRAINT PK_stg_sales PRIMARY KEY (stg_id)
);
GO

CREATE NONCLUSTERED INDEX IX_stg_sales_hash
    ON dbo.stg_sales (row_hash)
    WHERE row_hash IS NOT NULL;
GO

CREATE NONCLUSTERED INDEX IX_stg_sales_status
    ON dbo.stg_sales (validation_status);
GO

-- -----------------------------------------------------------
-- pipeline_run_log
-- One row per pipeline execution
-- Tracks file-level outcomes for monitoring and alerting
-- -----------------------------------------------------------
CREATE TABLE dbo.pipeline_run_log (
    run_id              BIGINT IDENTITY(1,1)    NOT NULL,
    run_date            DATE                    NOT NULL,   -- the business date being processed
    pipeline_start      DATETIME2               NOT NULL    DEFAULT SYSUTCDATETIME(),
    pipeline_end        DATETIME2               NULL,
    status              VARCHAR(20)             NOT NULL    DEFAULT 'RUNNING', -- RUNNING / SUCCESS / FAILED
    files_expected      SMALLINT                NULL,
    files_processed     SMALLINT                NULL,
    files_missing       SMALLINT                NULL,
    rows_staged         INT                     NULL,
    rows_passed         INT                     NULL,
    rows_rejected       INT                     NULL,
    rows_loaded         INT                     NULL,
    rows_duplicates     INT                     NULL,
    error_message       VARCHAR(1000)           NULL,
    notes               VARCHAR(500)            NULL,

    CONSTRAINT PK_pipeline_run_log PRIMARY KEY (run_id)
);
GO

-- -----------------------------------------------------------
-- missing_file_log
-- Tracks which stores failed to deliver files on which dates
-- Used for alerting and operational reporting
-- -----------------------------------------------------------
CREATE TABLE dbo.missing_file_log (
    log_id              BIGINT IDENTITY(1,1)    NOT NULL,
    run_id              BIGINT                  NOT NULL,
    run_date            DATE                    NOT NULL,
    store_id            VARCHAR(10)             NOT NULL,
    expected_file       VARCHAR(255)            NOT NULL,
    logged_at           DATETIME2               NOT NULL    DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_missing_file_log PRIMARY KEY (log_id),
    CONSTRAINT FK_missing_file_run FOREIGN KEY (run_id) REFERENCES dbo.pipeline_run_log(run_id)
);
GO

-- -----------------------------------------------------------
-- Verify
-- -----------------------------------------------------------
SELECT
    t.name          AS table_name,
    p.rows          AS row_count
FROM sys.tables t
JOIN sys.partitions p
    ON t.object_id = p.object_id
    AND p.index_id IN (0, 1)
WHERE t.schema_id = SCHEMA_ID('dbo')
ORDER BY t.name;
GO