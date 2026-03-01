-- =============================================================
-- FranchisePulse — Dimension Tables
-- File: sql/ddl/01_dimensions.sql
-- =============================================================

USE FranchisePulse;
GO

-- -----------------------------------------------------------
-- dim_store
-- One row per franchise location
-- Slowly changing — owner or region could change over time
-- For this project we treat as static (Type 1)
-- -----------------------------------------------------------
CREATE TABLE dbo.dim_store (
    store_key       INT IDENTITY(1,1)   NOT NULL,
    store_id        VARCHAR(10)         NOT NULL,   -- natural key e.g. S01
    store_name      VARCHAR(100)        NOT NULL,
    city            VARCHAR(50)         NOT NULL,
    region          VARCHAR(50)         NOT NULL,
    franchise_owner VARCHAR(100)        NOT NULL,
    is_active       BIT                 NOT NULL    DEFAULT 1,
    created_at      DATETIME2           NOT NULL    DEFAULT SYSUTCDATETIME(),
    updated_at      DATETIME2           NOT NULL    DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_dim_store PRIMARY KEY (store_key),
    CONSTRAINT UQ_dim_store_id UNIQUE (store_id)
);
GO

-- -----------------------------------------------------------
-- dim_product
-- One row per product SKU
-- standard_price is the catalogue price — actual sale price
-- lives in fact_sales to capture promotions
-- -----------------------------------------------------------
CREATE TABLE dbo.dim_product (
    product_key     INT IDENTITY(1,1)   NOT NULL,
    product_sku     VARCHAR(20)         NOT NULL,   -- natural key e.g. HOT001
    product_name    VARCHAR(100)        NOT NULL,
    category        VARCHAR(50)         NOT NULL,
    standard_price  DECIMAL(10,2)       NOT NULL,
    is_active       BIT                 NOT NULL    DEFAULT 1,
    created_at      DATETIME2           NOT NULL    DEFAULT SYSUTCDATETIME(),
    updated_at      DATETIME2           NOT NULL    DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_dim_product PRIMARY KEY (product_key),
    CONSTRAINT UQ_dim_product_sku UNIQUE (product_sku)
);
GO

-- -----------------------------------------------------------
-- dim_date
-- Pre-populated calendar table
-- Covers 2024-01-01 to 2027-12-31
-- Generated once, never changes
-- -----------------------------------------------------------
CREATE TABLE dbo.dim_date (
    date_key        INT                 NOT NULL,   -- YYYYMMDD e.g. 20250101
    full_date       DATE                NOT NULL,
    day_of_week     TINYINT             NOT NULL,   -- 1=Monday, 7=Sunday
    day_name        VARCHAR(10)         NOT NULL,
    day_of_month    TINYINT             NOT NULL,
    day_of_year     SMALLINT            NOT NULL,
    week_of_year    TINYINT             NOT NULL,
    month_number    TINYINT             NOT NULL,
    month_name      VARCHAR(10)         NOT NULL,
    quarter_number  TINYINT             NOT NULL,
    year_number     SMALLINT            NOT NULL,
    is_weekend      BIT                 NOT NULL,
    is_public_holiday BIT               NOT NULL    DEFAULT 0,

    CONSTRAINT PK_dim_date PRIMARY KEY (date_key)
);
GO

-- -----------------------------------------------------------
-- dim_payment_method
-- Small static lookup table
-- -----------------------------------------------------------
CREATE TABLE dbo.dim_payment_method (
    payment_key     INT IDENTITY(1,1)   NOT NULL,
    payment_method  VARCHAR(50)         NOT NULL,

    CONSTRAINT PK_dim_payment_method PRIMARY KEY (payment_key),
    CONSTRAINT UQ_dim_payment_method UNIQUE (payment_method)
);
GO

-- -----------------------------------------------------------
-- Populate dim_date (2024-01-01 to 2027-12-31)
-- -----------------------------------------------------------
WITH date_sequence AS (
    SELECT CAST('2024-01-01' AS DATE) AS d
    UNION ALL
    SELECT DATEADD(DAY, 1, d)
    FROM date_sequence
    WHERE d < '2027-12-31'
)
INSERT INTO dbo.dim_date (
    date_key, full_date, day_of_week, day_name,
    day_of_month, day_of_year, week_of_year,
    month_number, month_name, quarter_number,
    year_number, is_weekend
)
SELECT
    CONVERT(INT, FORMAT(d, 'yyyyMMdd'))             AS date_key,
    d                                               AS full_date,
    -- ISO week: Monday=1, Sunday=7
    CASE DATEPART(WEEKDAY, d)
        WHEN 1 THEN 7  -- Sunday → 7
        ELSE DATEPART(WEEKDAY, d) - 1
    END                                             AS day_of_week,
    DATENAME(WEEKDAY, d)                            AS day_name,
    DAY(d)                                          AS day_of_month,
    DATEPART(DAYOFYEAR, d)                          AS day_of_year,
    DATEPART(ISO_WEEK, d)                           AS week_of_year,
    MONTH(d)                                        AS month_number,
    DATENAME(MONTH, d)                              AS month_name,
    DATEPART(QUARTER, d)                            AS quarter_number,
    YEAR(d)                                         AS year_number,
    CASE WHEN DATEPART(WEEKDAY, d) IN (1, 7)
         THEN 1 ELSE 0
    END                                             AS is_weekend
FROM date_sequence
OPTION (MAXRECURSION 2000);
GO

-- -----------------------------------------------------------
-- Populate dim_payment_method
-- -----------------------------------------------------------
INSERT INTO dbo.dim_payment_method (payment_method)
VALUES ('Card'), ('Cash'), ('Contactless'), ('Apple Pay'), ('Google Pay');
GO

-- -----------------------------------------------------------
-- Populate dim_store
-- -----------------------------------------------------------
INSERT INTO dbo.dim_store (store_id, store_name, city, region, franchise_owner)
VALUES
    ('S01', 'FranchisePulse Grafton St',      'Dublin',    'Leinster', 'Murphy Catering Ltd'),
    ('S02', 'FranchisePulse Dundrum',         'Dublin',    'Leinster', 'Murphy Catering Ltd'),
    ('S03', 'FranchisePulse Cork City',       'Cork',      'Munster',  'O''Brien Foods Ltd'),
    ('S04', 'FranchisePulse Mahon Point',     'Cork',      'Munster',  'O''Brien Foods Ltd'),
    ('S05', 'FranchisePulse Galway',          'Galway',    'Connacht', 'Walsh Hospitality'),
    ('S06', 'FranchisePulse Eyre Square',     'Galway',    'Connacht', 'Walsh Hospitality'),
    ('S07', 'FranchisePulse Limerick',        'Limerick',  'Munster',  'Ryan Group'),
    ('S08', 'FranchisePulse Waterford',       'Waterford', 'Munster',  'Ryan Group'),
    ('S09', 'FranchisePulse Kilkenny',        'Kilkenny',  'Leinster', 'Brennan Retail'),
    ('S10', 'FranchisePulse Drogheda',        'Drogheda',  'Leinster', 'Brennan Retail'),
    ('S11', 'FranchisePulse Sligo',           'Sligo',     'Connacht', 'Kelly Ventures'),
    ('S12', 'FranchisePulse Athlone',         'Athlone',   'Leinster', 'Kelly Ventures');
GO

-- -----------------------------------------------------------
-- Populate dim_product
-- -----------------------------------------------------------
INSERT INTO dbo.dim_product (product_sku, product_name, category, standard_price)
VALUES
    ('HOT001', 'Espresso',            'Hot Drinks',  2.50),
    ('HOT002', 'Americano',           'Hot Drinks',  3.00),
    ('HOT003', 'Flat White',          'Hot Drinks',  3.50),
    ('HOT004', 'Cappuccino',          'Hot Drinks',  3.50),
    ('HOT005', 'Latte',               'Hot Drinks',  3.80),
    ('HOT006', 'Hot Chocolate',       'Hot Drinks',  3.80),
    ('COL001', 'Iced Latte',          'Cold Drinks', 4.20),
    ('COL002', 'Iced Americano',      'Cold Drinks', 3.80),
    ('COL003', 'Cold Brew',           'Cold Drinks', 4.50),
    ('COL004', 'Sparkling Water',     'Cold Drinks', 1.80),
    ('FOD001', 'Butter Croissant',    'Food',        2.80),
    ('FOD002', 'Blueberry Muffin',    'Food',        3.20),
    ('FOD003', 'Chicken Wrap',        'Food',        6.50),
    ('FOD004', 'Ham & Cheese Toastie','Food',        5.80),
    ('FOD005', 'Banana Bread',        'Food',        3.00),
    ('MER001', 'Branded Mug',         'Merch',       12.00),
    ('MER002', 'Reusable Cup',        'Merch',       8.00);
GO

-- -----------------------------------------------------------
-- Verify
-- -----------------------------------------------------------
SELECT 'dim_store'          AS tbl, COUNT(*) AS rows FROM dbo.dim_store
UNION ALL
SELECT 'dim_product',               COUNT(*)         FROM dbo.dim_product
UNION ALL
SELECT 'dim_date',                  COUNT(*)         FROM dbo.dim_date
UNION ALL
SELECT 'dim_payment_method',        COUNT(*)         FROM dbo.dim_payment_method;
GO