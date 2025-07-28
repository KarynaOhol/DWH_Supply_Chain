-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - DM LAYER
-- Script 1: DDL + Default Rows (Run ONCE)
-- Purpose: Create BL_DM schema with star schema design
-- =====================================================

-- Connect to dwh_dev database
\c dwh_dev_pgsql;

-- =====================================================
-- SECTION 1: SCHEMA CREATION
-- =====================================================

CREATE SCHEMA IF NOT EXISTS BL_DM;
SET search_path = BL_DM, public,BL_CL;

-- =====================================================
-- SECTION 2: SEQUENCES CREATION
-- =====================================================

CREATE SEQUENCE IF NOT EXISTS SEQ_DIM_CUSTOMERS START 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_DIM_PRODUCTS_SCD START 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_DIM_GEOGRAPHIES START 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_DIM_SALES_REPRESENTATIVES START 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_DIM_WAREHOUSES START 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_DIM_CARRIERS START 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_DIM_ORDER_STATUSES START 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_DIM_PAYMENT_METHODS START 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_DIM_SHIPPING_MODES START 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_DIM_DELIVERY_STATUSES START 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_FCT_ORDER_LINE_SHIPMENTS START 1;

-- =====================================================
-- SECTION 3: DIMENSION TABLES (SCD TYPE 1)
-- =====================================================

-- Customers Dimension
CREATE TABLE IF NOT EXISTS BL_DM.DIM_CUSTOMERS
(
    CUSTOMER_SURR_ID       BIGINT       NOT NULL DEFAULT NEXTVAL('SEQ_DIM_CUSTOMERS'),
    CUSTOMER_SRC_ID        BIGINT       NOT NULL,
    CUSTOMER_FIRST_NAME    VARCHAR(100) NOT NULL,
    CUSTOMER_LAST_NAME     VARCHAR(100) NOT NULL,
    CUSTOMER_FULL_NAME     VARCHAR(200) NOT NULL,
    CUSTOMER_GENDER        VARCHAR(10)  NOT NULL,
    CUSTOMER_YEAR_OF_BIRTH INTEGER      NOT NULL,
    CUSTOMER_EMAIL         VARCHAR(255) NOT NULL,
    CUSTOMER_SEGMENT       VARCHAR(50)  NOT NULL,
    SOURCE_SYSTEM          VARCHAR(50)  NOT NULL,
    SOURCE_ENTITY          VARCHAR(100) NOT NULL,
    TA_INSERT_DT           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    TA_UPDATE_DT           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_DIM_CUSTOMERS PRIMARY KEY (CUSTOMER_SURR_ID)
);

-- Geographies Dimension (Flattened Hierarchy)
CREATE TABLE IF NOT EXISTS BL_DM.DIM_GEOGRAPHIES
(
    GEOGRAPHY_SURR_ID BIGINT       NOT NULL DEFAULT NEXTVAL('SEQ_DIM_GEOGRAPHIES'),
    GEOGRAPHY_SRC_ID  BIGINT       NOT NULL,
    CITY_NAME         VARCHAR(100) NOT NULL,
    CITY_SRC_ID       BIGINT       NOT NULL,
    STATE_NAME        VARCHAR(100) NOT NULL,
    STATE_SRC_ID      BIGINT       NOT NULL,
    STATE_CODE        VARCHAR(10)  NULL,
    COUNTRY_NAME      VARCHAR(100) NOT NULL,
    COUNTRY_SRC_ID    BIGINT       NOT NULL,
    COUNTRY_CODE      VARCHAR(10)  NULL,
    REGION_NAME       VARCHAR(100) NOT NULL,
    REGION_SRC_ID     BIGINT       NOT NULL,
    SOURCE_SYSTEM     VARCHAR(50)  NOT NULL,
    SOURCE_ENTITY     VARCHAR(100) NOT NULL,
    TA_INSERT_DT      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    TA_UPDATE_DT      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_DIM_GEOGRAPHIES PRIMARY KEY (GEOGRAPHY_SURR_ID)
);

-- Sales Representatives Dimension
CREATE TABLE IF NOT EXISTS BL_DM.DIM_SALES_REPRESENTATIVES
(
    SALES_REP_SURR_ID BIGINT       NOT NULL DEFAULT NEXTVAL('SEQ_DIM_SALES_REPRESENTATIVES'),
    SALES_REP_SRC_ID  VARCHAR(50)  NOT NULL,
    SALES_REP_NAME    VARCHAR(100) NOT NULL,
    SOURCE_SYSTEM     VARCHAR(50)  NOT NULL,
    SOURCE_ENTITY     VARCHAR(100) NOT NULL,
    TA_INSERT_DT      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    TA_UPDATE_DT      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_DIM_SALES_REPRESENTATIVES PRIMARY KEY (SALES_REP_SURR_ID)
);

-- Warehouses Dimension
CREATE TABLE IF NOT EXISTS BL_DM.DIM_WAREHOUSES
(
    WAREHOUSE_SURR_ID BIGINT       NOT NULL DEFAULT NEXTVAL('SEQ_DIM_WAREHOUSES'),
    WAREHOUSE_SRC_ID  VARCHAR(50)  NOT NULL,
    WAREHOUSE_NAME    VARCHAR(100) NOT NULL,
    SOURCE_SYSTEM     VARCHAR(50)  NOT NULL,
    SOURCE_ENTITY     VARCHAR(100) NOT NULL,
    TA_INSERT_DT      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    TA_UPDATE_DT      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_DIM_WAREHOUSES PRIMARY KEY (WAREHOUSE_SURR_ID)
);

-- Carriers Dimension
CREATE TABLE IF NOT EXISTS BL_DM.DIM_CARRIERS
(
    CARRIER_SURR_ID BIGINT       NOT NULL DEFAULT NEXTVAL('SEQ_DIM_CARRIERS'),
    CARRIER_SRC_ID  BIGINT       NOT NULL,
    CARRIER_NAME    VARCHAR(100) NOT NULL,
    CARRIER_TYPE    VARCHAR(50)  NULL,
    SOURCE_SYSTEM   VARCHAR(50)  NOT NULL,
    SOURCE_ENTITY   VARCHAR(100) NOT NULL,
    TA_INSERT_DT    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    TA_UPDATE_DT    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_DIM_CARRIERS PRIMARY KEY (CARRIER_SURR_ID)
);

-- Order Statuses Dimension
CREATE TABLE IF NOT EXISTS BL_DM.DIM_ORDER_STATUSES
(
    ORDER_STATUS_SURR_ID BIGINT       NOT NULL DEFAULT NEXTVAL('SEQ_DIM_ORDER_STATUSES'),
    ORDER_STATUS_SRC_ID  BIGINT       NOT NULL,
    ORDER_STATUS         VARCHAR(50)  NOT NULL,
    SOURCE_SYSTEM        VARCHAR(50)  NOT NULL,
    SOURCE_ENTITY        VARCHAR(100) NOT NULL,
    TA_INSERT_DT         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    TA_UPDATE_DT         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_DIM_ORDER_STATUSES PRIMARY KEY (ORDER_STATUS_SURR_ID)
);

-- Payment Methods Dimension
CREATE TABLE IF NOT EXISTS BL_DM.DIM_PAYMENT_METHODS
(
    PAYMENT_METHOD_SURR_ID BIGINT       NOT NULL DEFAULT NEXTVAL('SEQ_DIM_PAYMENT_METHODS'),
    PAYMENT_METHOD_SRC_ID  BIGINT       NOT NULL,
    PAYMENT_METHOD         VARCHAR(100) NOT NULL,
    SOURCE_SYSTEM          VARCHAR(50)  NOT NULL,
    SOURCE_ENTITY          VARCHAR(100) NOT NULL,
    TA_INSERT_DT           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    TA_UPDATE_DT           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_DIM_PAYMENT_METHODS PRIMARY KEY (PAYMENT_METHOD_SURR_ID)
);

-- Shipping Modes Dimension
CREATE TABLE IF NOT EXISTS BL_DM.DIM_SHIPPING_MODES
(
    SHIPPING_MODE_SURR_ID BIGINT       NOT NULL DEFAULT NEXTVAL('SEQ_DIM_SHIPPING_MODES'),
    SHIPPING_MODE_SRC_ID  BIGINT       NOT NULL,
    SHIPPING_MODE         VARCHAR(50)  NOT NULL,
    SOURCE_SYSTEM         VARCHAR(50)  NOT NULL,
    SOURCE_ENTITY         VARCHAR(100) NOT NULL,
    TA_INSERT_DT          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    TA_UPDATE_DT          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_DIM_SHIPPING_MODES PRIMARY KEY (SHIPPING_MODE_SURR_ID)
);

-- Delivery Statuses Dimension
CREATE TABLE IF NOT EXISTS BL_DM.DIM_DELIVERY_STATUSES
(
    DELIVERY_STATUS_SURR_ID BIGINT       NOT NULL DEFAULT NEXTVAL('SEQ_DIM_DELIVERY_STATUSES'),
    DELIVERY_STATUS_SRC_ID  BIGINT       NOT NULL,
    DELIVERY_STATUS         VARCHAR(50)  NOT NULL,
    SOURCE_SYSTEM           VARCHAR(50)  NOT NULL,
    SOURCE_ENTITY           VARCHAR(100) NOT NULL,
    TA_INSERT_DT            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    TA_UPDATE_DT            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_DIM_DELIVERY_STATUSES PRIMARY KEY (DELIVERY_STATUS_SURR_ID)
);

-- =====================================================
-- SECTION 4: SCD TYPE 2 DIMENSION
-- =====================================================

-- Products Dimension (SCD Type 2) - Flattened with Product Hierarchy
CREATE TABLE IF NOT EXISTS BL_DM.DIM_PRODUCTS_SCD
(
    PRODUCT_SURR_ID         BIGINT       NOT NULL DEFAULT NEXTVAL('SEQ_DIM_PRODUCTS_SCD'),
    PRODUCT_SRC_ID          VARCHAR(50)  NOT NULL,
    PRODUCT_NAME            VARCHAR(255) NOT NULL,
    BRAND_NAME              VARCHAR(100) NOT NULL,
    BRAND_SRC_ID            BIGINT       NOT NULL,
    PRIMARY_CATEGORY_SRC_ID BIGINT       NOT NULL,
    PRIMARY_CATEGORY_NAME   VARCHAR(100) NOT NULL,
    DEPARTMENT_SRC_ID       BIGINT       NOT NULL,
    DEPARTMENT_NAME         VARCHAR(100) NOT NULL,
    ALL_CATEGORY_SRC_IDS    TEXT         NULL, -- Pipe delimited
    ALL_CATEGORY_NAMES      TEXT         NULL, -- Pipe delimited
    PRODUCT_STATUS_NAME     VARCHAR(50)  NOT NULL,
    PRODUCT_STATUS_SRC_ID   BIGINT       NOT NULL,
    START_DT                DATE         NOT NULL DEFAULT '1990-01-01',
    END_DT                  DATE         NOT NULL DEFAULT '9999-12-31',
    IS_ACTIVE               VARCHAR(1)   NOT NULL DEFAULT 'Y',
    SOURCE_SYSTEM           VARCHAR(50)  NOT NULL,
    SOURCE_ENTITY           VARCHAR(100) NOT NULL,
    TA_INSERT_DT            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    TA_UPDATE_DT            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_DIM_PRODUCTS_SCD PRIMARY KEY (PRODUCT_SURR_ID, START_DT)
);

-- =====================================================
-- SECTION 5: TIME DIMENSION (Using existing structure)
-- =====================================================

-- Time Dimension
CREATE TABLE IF NOT EXISTS BL_DM.DIM_TIME_DAY
(
    DT_SURR_ID       BIGINT       NOT NULL,
    CALENDAR_DT      DATE         NOT NULL UNIQUE,
    YEAR_NUM         INTEGER      NOT NULL,
    QUARTER_NUM      INTEGER      NOT NULL,
    MONTH_NUM        INTEGER      NOT NULL,
    WEEK_NUM         INTEGER      NOT NULL,
    DAY_NUM          INTEGER      NOT NULL,
    DAY_OF_YEAR_NUM  INTEGER      NOT NULL,
    DAY_OF_WEEK_NUM  INTEGER      NOT NULL,
    MONTH_NAME       VARCHAR(20)  NOT NULL,
    MONTH_NAME_SHORT VARCHAR(3)   NOT NULL,
    DAY_NAME         VARCHAR(20)  NOT NULL,
    DAY_NAME_SHORT   VARCHAR(3)   NOT NULL,
    QUARTER_NAME     VARCHAR(10)  NOT NULL,
    YEAR_QUARTER     VARCHAR(10)  NOT NULL,
    YEAR_MONTH       VARCHAR(10)  NOT NULL,
    IS_WEEKEND       BOOLEAN      NOT NULL,
    IS_HOLIDAY       BOOLEAN      NOT NULL,
    HOLIDAY_NAME     VARCHAR(100) NULL,
    SOURCE_SYSTEM    VARCHAR(50)  NOT NULL DEFAULT 'SYSTEM',
    SOURCE_ENTITY    VARCHAR(50)  NOT NULL DEFAULT 'DATE_GENERATOR',
    TA_INSERT_DT     DATE         NOT NULL DEFAULT CURRENT_DATE,
    TA_UPDATE_DT     DATE         NOT NULL DEFAULT CURRENT_DATE,
    CONSTRAINT PK_DIM_TIME_DAY PRIMARY KEY (DT_SURR_ID)
);

-- =====================================================
-- SECTION 6: FACT TABLE
-- =====================================================

-- Single Fact Table: Order Line Shipments (Grain: Order Line per Shipment)
CREATE TABLE IF NOT EXISTS BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
(
    ORDER_LINE_SHIPMENT_SURR_ID BIGINT         NOT NULL DEFAULT NEXTVAL('SEQ_FCT_ORDER_LINE_SHIPMENTS'),

    -- Main Time Key
    EVENT_DT                    DATE           NOT NULL,

    -- Dimension Foreign Keys
    CUSTOMER_SURR_ID            BIGINT         NOT NULL,
    PRODUCT_SURR_ID             BIGINT         NOT NULL,
    SALES_REP_SURR_ID           BIGINT         NOT NULL,
    ORDER_DT_SURR_ID            BIGINT         NOT NULL,
    SHIP_DT_SURR_ID             BIGINT         NOT NULL,
    DELIVERY_DT_SURR_ID         BIGINT         NULL,
    CUSTOMER_GEOGRAPHY_SURR_ID  BIGINT         NOT NULL,
    WAREHOUSE_SURR_ID           BIGINT         NOT NULL,
    CARRIER_SURR_ID             BIGINT         NOT NULL,
    PAYMENT_METHOD_SURR_ID      BIGINT         NOT NULL,
    ORDER_STATUS_SURR_ID        BIGINT         NOT NULL,
    SHIPPING_MODE_SURR_ID       BIGINT         NOT NULL,
    DELIVERY_STATUS_SURR_ID     BIGINT         NOT NULL,

    -- Degenerate Dimensions (Natural Keys)
    ORDER_SRC_ID                BIGINT         NOT NULL,
    ORDER_LINE_SRC_ID           BIGINT         NOT NULL,
    SHIPMENT_SRC_ID             BIGINT         NOT NULL,
    SHIPMENT_LINE_SRC_ID        BIGINT         NOT NULL,
    DELIVERY_SRC_ID             BIGINT         NULL,

    -- Sales Measures
    UNIT_PRICE_ACT              DECIMAL(15, 2) NOT NULL,
    UNIT_COST_ACT               DECIMAL(15, 2) NULL,
    LINE_TOTAL_ACT              DECIMAL(15, 2) NOT NULL,
    ORDERED_QUANTITY_CNT        INTEGER        NOT NULL,
    ORDER_PROFIT_ACT            DECIMAL(15, 2) NULL,

    -- Shipment Measures
    SHIPPED_QUANTITY_CNT        INTEGER        NOT NULL,
    SHIPPING_COST_ACT           DECIMAL(15, 2) NULL,
    ALLOCATED_SHIPPING_COST_ACT DECIMAL(15, 2) NULL,

    -- Delivery Measures
    DELIVERY_DAYS_CNT           INTEGER        NULL,
    PLANNED_DELIVERY_DAYS_CNT   INTEGER        NULL,
    ORDER_TO_SHIP_DAYS_CNT      INTEGER        NULL,
    ON_TIME_DELIVERY_FLAG       DECIMAL(1, 0)  NULL,
    LATE_DELIVERY_FLAG          DECIMAL(1, 0)  NULL,

    -- Calculated Measures
    SHIPPED_SALES_AMOUNT_ACT    DECIMAL(15, 2) NOT NULL,
    UNSHIPPED_QUANTITY_CNT      DECIMAL(10, 2) NOT NULL,
    FILL_RATE_PCT               DECIMAL(5, 2)  NULL,

    -- Technical Attributes
    TA_INSERT_DT                TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    TA_UPDATE_DT                TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT PK_FCT_ORDER_LINE_SHIPMENTS_DD PRIMARY KEY (ORDER_LINE_SHIPMENT_SURR_ID, EVENT_DT)
) PARTITION BY RANGE (EVENT_DT);

-- =====================================================
-- SECTION 7: FOREIGN KEY CONSTRAINTS
-- =====================================================

-- Standard SCD1 Foreign Keys
ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    ADD CONSTRAINT FK_FCT_CUSTOMER
        FOREIGN KEY (CUSTOMER_SURR_ID) REFERENCES DIM_CUSTOMERS (CUSTOMER_SURR_ID);

ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    ADD CONSTRAINT FK_FCT_SALES_REP
        FOREIGN KEY (SALES_REP_SURR_ID) REFERENCES DIM_SALES_REPRESENTATIVES (SALES_REP_SURR_ID);

ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    ADD CONSTRAINT FK_FCT_CUSTOMER_GEOGRAPHY
        FOREIGN KEY (CUSTOMER_GEOGRAPHY_SURR_ID) REFERENCES DIM_GEOGRAPHIES (GEOGRAPHY_SURR_ID);

ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    ADD CONSTRAINT FK_FCT_WAREHOUSE
        FOREIGN KEY (WAREHOUSE_SURR_ID) REFERENCES DIM_WAREHOUSES (WAREHOUSE_SURR_ID);

ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    ADD CONSTRAINT FK_FCT_CARRIER
        FOREIGN KEY (CARRIER_SURR_ID) REFERENCES DIM_CARRIERS (CARRIER_SURR_ID);

ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    ADD CONSTRAINT FK_FCT_PAYMENT_METHOD
        FOREIGN KEY (PAYMENT_METHOD_SURR_ID) REFERENCES DIM_PAYMENT_METHODS (PAYMENT_METHOD_SURR_ID);

ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    ADD CONSTRAINT FK_FCT_ORDER_STATUS
        FOREIGN KEY (ORDER_STATUS_SURR_ID) REFERENCES DIM_ORDER_STATUSES (ORDER_STATUS_SURR_ID);

ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    ADD CONSTRAINT FK_FCT_SHIPPING_MODE
        FOREIGN KEY (SHIPPING_MODE_SURR_ID) REFERENCES DIM_SHIPPING_MODES (SHIPPING_MODE_SURR_ID);

ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    ADD CONSTRAINT FK_FCT_DELIVERY_STATUS
        FOREIGN KEY (DELIVERY_STATUS_SURR_ID) REFERENCES DIM_DELIVERY_STATUSES (DELIVERY_STATUS_SURR_ID);

-- Time Dimension Foreign Keys (Role-playing)
ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    ADD CONSTRAINT FK_FCT_ORDER_DATE
        FOREIGN KEY (ORDER_DT_SURR_ID) REFERENCES DIM_TIME_DAY (DT_SURR_ID);

ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    ADD CONSTRAINT FK_FCT_SHIP_DATE
        FOREIGN KEY (SHIP_DT_SURR_ID) REFERENCES DIM_TIME_DAY (DT_SURR_ID);

ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    ADD CONSTRAINT FK_FCT_DELIVERY_DATE
        FOREIGN KEY (DELIVERY_DT_SURR_ID) REFERENCES DIM_TIME_DAY (DT_SURR_ID);

-- Note: SCD2 Product constraint is logical only and will be enforced in ETL
-- ALTER TABLE FCT_ORDER_LINE_SHIPMENTS_DD
--     ADD CONSTRAINT FK_FCT_PRODUCT_SCD
--         FOREIGN KEY (PRODUCT_SURR_ID) REFERENCES DIM_PRODUCTS_SCD (PRODUCT_SURR_ID);

-- =====================================================
-- SECTION 8: INDEXES FOR PERFORMANCE
-- =====================================================

-- Dimension Source ID indexes for ETL lookups
CREATE INDEX IF NOT EXISTS IDX_DIM_CUSTOMERS_SRC_ID ON BL_DM.DIM_CUSTOMERS (CUSTOMER_SRC_ID);
CREATE INDEX IF NOT EXISTS IDX_DIM_PRODUCTS_SRC_ID ON BL_DM.DIM_PRODUCTS_SCD (PRODUCT_SRC_ID, IS_ACTIVE);
CREATE INDEX IF NOT EXISTS IDX_DIM_GEOGRAPHIES_SRC_ID ON BL_DM.DIM_GEOGRAPHIES (GEOGRAPHY_SRC_ID);
CREATE INDEX IF NOT EXISTS IDX_DIM_SALES_REPS_SRC_ID ON BL_DM.DIM_SALES_REPRESENTATIVES (SALES_REP_SRC_ID);
CREATE INDEX IF NOT EXISTS IDX_DIM_WAREHOUSES_SRC_ID ON BL_DM.DIM_WAREHOUSES (WAREHOUSE_SRC_ID);
CREATE INDEX IF NOT EXISTS IDX_DIM_CARRIERS_SRC_ID ON BL_DM.DIM_CARRIERS (CARRIER_SRC_ID);
CREATE INDEX IF NOT EXISTS IDX_DIM_ORDER_STATUSES_SRC_ID ON BL_DM.DIM_ORDER_STATUSES (ORDER_STATUS_SRC_ID);
CREATE INDEX IF NOT EXISTS IDX_DIM_PAYMENT_METHODS_SRC_ID ON BL_DM.DIM_PAYMENT_METHODS (PAYMENT_METHOD_SRC_ID);
CREATE INDEX IF NOT EXISTS IDX_DIM_SHIPPING_MODES_SRC_ID ON BL_DM.DIM_SHIPPING_MODES (SHIPPING_MODE_SRC_ID);
CREATE INDEX IF NOT EXISTS IDX_DIM_DELIVERY_STATUSES_SRC_ID ON BL_DM.DIM_DELIVERY_STATUSES (DELIVERY_STATUS_SRC_ID);

-- Time dimension indexes
CREATE INDEX IF NOT EXISTS IDX_DIM_TIME_DAY_CALENDAR_DT ON BL_DM.DIM_TIME_DAY (CALENDAR_DT);
CREATE INDEX IF NOT EXISTS IDX_DIM_TIME_DAY_YEAR_MONTH ON BL_DM.DIM_TIME_DAY (YEAR_NUM, MONTH_NUM);

-- SCD2 specific indexes
CREATE INDEX IF NOT EXISTS IDX_DIM_PRODUCTS_EFFECTIVE_DATE ON BL_DM.DIM_PRODUCTS_SCD (START_DT, END_DT);
CREATE INDEX IF NOT EXISTS IDX_DIM_PRODUCTS_SRC_ACTIVE ON BL_DM.DIM_PRODUCTS_SCD (PRODUCT_SRC_ID, START_DT, END_DT) WHERE IS_ACTIVE = 'Y';

-- Fact table indexes
CREATE INDEX IF NOT EXISTS IDX_FCT_EVENT_DT ON BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD (EVENT_DT);
CREATE INDEX IF NOT EXISTS IDX_FCT_ORDER_DT_SURR_ID ON BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD (ORDER_DT_SURR_ID);
CREATE INDEX IF NOT EXISTS IDX_FCT_SHIP_DT_SURR_ID ON BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD (SHIP_DT_SURR_ID);
CREATE INDEX IF NOT EXISTS IDX_FCT_CUSTOMER_SURR_ID ON BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD (CUSTOMER_SURR_ID);
CREATE INDEX IF NOT EXISTS IDX_FCT_PRODUCT_SURR_ID ON BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD (PRODUCT_SURR_ID);

-- Degenerate dimension indexes for drill-down
CREATE INDEX IF NOT EXISTS IDX_FCT_ORDER_SRC_ID ON BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD (ORDER_SRC_ID);
CREATE INDEX IF NOT EXISTS IDX_FCT_ORDER_LINE_SRC_ID ON BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD (ORDER_LINE_SRC_ID);

-- =====================================================
-- SECTION 10: INSERT TIME DIMENSION DATA
-- =====================================================

INSERT INTO BL_DM.DIM_TIME_DAY (DT_SURR_ID,
                                CALENDAR_DT,
                                YEAR_NUM,
                                QUARTER_NUM,
                                MONTH_NUM,
                                WEEK_NUM,
                                DAY_NUM,
                                DAY_OF_YEAR_NUM,
                                DAY_OF_WEEK_NUM,
                                MONTH_NAME,
                                MONTH_NAME_SHORT,
                                DAY_NAME,
                                DAY_NAME_SHORT,
                                QUARTER_NAME,
                                YEAR_QUARTER,
                                YEAR_MONTH,
                                IS_WEEKEND,
                                IS_HOLIDAY,
                                HOLIDAY_NAME,
                                SOURCE_SYSTEM,
                                SOURCE_ENTITY,
                                TA_INSERT_DT,
                                TA_UPDATE_DT)
SELECT
    -- DT_SURR_ID as YYYYMMDD format
    CAST(TO_CHAR(date_series, 'YYYYMMDD') AS BIGINT)                            AS DT_SURR_ID,

    -- Calendar Date
    date_series                                                                 AS CALENDAR_DT,

    -- Year
    EXTRACT(YEAR FROM date_series)                                              AS YEAR_NUM,

    -- Quarter (1-4)
    EXTRACT(QUARTER FROM date_series)                                           AS QUARTER_NUM,

    -- Month (1-12)
    EXTRACT(MONTH FROM date_series)                                             AS MONTH_NUM,

    -- Week of Year (1-53)
    EXTRACT(WEEK FROM date_series)                                              AS WEEK_NUM,

    -- Day of Month (1-31)
    EXTRACT(DAY FROM date_series)                                               AS DAY_NUM,

    -- Day of Year (1-366)
    EXTRACT(DOY FROM date_series)                                               AS DAY_OF_YEAR_NUM,

    -- Day of Week (1=Monday, 7=Sunday)
    CASE EXTRACT(DOW FROM date_series)
        WHEN 0 THEN 7 -- Sunday
        ELSE EXTRACT(DOW FROM date_series)
        END                                                                     AS DAY_OF_WEEK_NUM,

    -- Month Name
    TO_CHAR(date_series, 'Month')                                               AS MONTH_NAME,

    -- Month Name Short
    TO_CHAR(date_series, 'Mon')                                                 AS MONTH_NAME_SHORT,

    -- Day Name
    TO_CHAR(date_series, 'Day')                                                 AS DAY_NAME,

    -- Day Name Short
    TO_CHAR(date_series, 'Dy')                                                  AS DAY_NAME_SHORT,

    -- Quarter Name
    'Q' || EXTRACT(QUARTER FROM date_series)                                    AS QUARTER_NAME,

    -- Year Quarter
    EXTRACT(YEAR FROM date_series) || '-Q' || EXTRACT(QUARTER FROM date_series) AS YEAR_QUARTER,

    -- Year Month
    TO_CHAR(date_series, 'YYYY-MM')                                             AS YEAR_MONTH,

    -- Is Weekend (Saturday=6, Sunday=0)
    CASE
        WHEN EXTRACT(DOW FROM date_series) IN (0, 6) THEN TRUE
        ELSE FALSE
        END                                                                     AS IS_WEEKEND,

    -- Holiday information using our function
    (BL_CL.is_holiday(date_series::DATE)).is_holiday                            AS IS_HOLIDAY,
    (BL_CL.is_holiday(date_series::DATE)).holiday_name                          AS HOLIDAY_NAME,

    -- Source Information
    'SYSTEM'                                                                    AS SOURCE_SYSTEM,
    'DATE_GENERATOR'                                                            AS SOURCE_ENTITY,

    -- Audit Dates
    CURRENT_DATE                                                                AS TA_INSERT_DT,
    CURRENT_DATE                                                                AS TA_UPDATE_DT

FROM (SELECT DATE '2023-01-01' + (n || ' days')::INTERVAL AS date_series
      FROM generate_series(0, (DATE '2025-12-31' - DATE '2023-01-01')) AS n) AS date_range;

-- =====================================================
-- SECTION : Unique constraints for merge
-- =====================================================

-- 1. BL_DM.DIM_GEOGRAPHIES
ALTER TABLE BL_DM.DIM_GEOGRAPHIES
    ADD CONSTRAINT uk_dim_geographies_src_system
        UNIQUE (geography_src_id, source_system);

-- 2.BL_DM.DIM_CUSTOMERS
ALTER TABLE BL_DM.DIM_CUSTOMERS
    ADD CONSTRAINT uk_dim_customers_src_system
        UNIQUE (CUSTOMER_SRC_ID, SOURCE_SYSTEM);

-- 3. BL_DM.DIM_SALES_REPRESENTATIVES
ALTER TABLE BL_DM.DIM_SALES_REPRESENTATIVES
    ADD CONSTRAINT uk_dim_sales_reps_src_system
        UNIQUE (sales_rep_src_id, source_system);

-- 4. BL_DM.DIM_WAREHOUSES
ALTER TABLE BL_DM.DIM_WAREHOUSES
    ADD CONSTRAINT uk_dim_warehouses_src_system
        UNIQUE (warehouse_src_id, source_system);

-- 5. BL_DM.DIM_CARRIERS
ALTER TABLE BL_DM.DIM_CARRIERS
    ADD CONSTRAINT uk_dim_carriers_src_system
        UNIQUE (carrier_src_id, source_system);

-- 6. BL_DM.DIM_ORDER_STATUSES
ALTER TABLE BL_DM.DIM_ORDER_STATUSES
    ADD CONSTRAINT uk_dim_order_statuses_src_system
        UNIQUE (order_status_src_id, source_system);

-- 7. BL_DM.DIM_PAYMENT_METHODS
ALTER TABLE BL_DM.DIM_PAYMENT_METHODS
    ADD CONSTRAINT uk_dim_payment_methods_src_system
        UNIQUE (payment_method_src_id, source_system);

-- 8. BL_DM.DIM_SHIPPING_MODES
ALTER TABLE BL_DM.DIM_SHIPPING_MODES
    ADD CONSTRAINT uk_dim_shipping_modes_src_system
        UNIQUE (shipping_mode_src_id, source_system);

-- 9. BL_DM.DIM_DELIVERY_STATUSES
ALTER TABLE BL_DM.DIM_DELIVERY_STATUSES
    ADD CONSTRAINT uk_dim_delivery_statuses_src_system
        UNIQUE (delivery_status_src_id, source_system);

-- Add unique constraint for SCD2 UPSERT operations
ALTER TABLE BL_DM.DIM_PRODUCTS_SCD
    ADD CONSTRAINT uk_dim_products_scd_src_system
        UNIQUE (PRODUCT_SRC_ID, START_DT, SOURCE_SYSTEM);

-- =====================================================
-- SECTION 11: VERIFICATION QUERIES
-- =====================================================

-- Verify table creation
SELECT schemaname,
       tablename,
       tableowner
FROM pg_tables
WHERE schemaname = 'bl_dm'
ORDER BY tablename;


-- Verify foreign key constraints
SELECT tc.table_name,
       ccu.table_name  AS foreign_table_name,
       tc.constraint_name,
       tc.constraint_type,
       kcu.column_name,
       ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
         JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
         JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = 'bl_dm'
ORDER BY tc.table_name, tc.constraint_name;

-- Verify time dimension
SELECT 'Time Dimension Records' as metric, COUNT(*) as count
FROM BL_DM.DIM_TIME_DAY
WHERE DT_SURR_ID != 19000101;

SELECT 'Date Range'                                               as metric,
       MIN(CALENDAR_DT)::TEXT || ' to ' || MAX(CALENDAR_DT)::TEXT as range
FROM BL_DM.DIM_TIME_DAY
WHERE DT_SURR_ID != 19000101;

COMMIT;