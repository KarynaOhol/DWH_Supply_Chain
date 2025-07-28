-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - DM LAYER FOUNDATION
-- Purpose: Create composite types for DM layer dimension loading procedures
-- Requirements: Define all composite types used across dimension procedures
-- Run as: dwh_cleansing_user
-- =====================================================

SELECT CURRENT_USER, SESSION_USER;

SET ROLE dwh_cleansing_user;
SET search_path = BL_CL, BL_3NF, BL_DM, public;

-- =====================================================
-- SECTION 1: DROP EXISTING COMPOSITE TYPES (For Rerun Capability)
-- =====================================================

-- Drop composite types if they exist (in reverse dependency order)
DROP TYPE IF EXISTS BL_CL.t_dim_scd2_change_record CASCADE;
DROP TYPE IF EXISTS BL_CL.t_dim_validation_result CASCADE;
DROP TYPE IF EXISTS BL_CL.t_dim_load_result CASCADE;
DROP TYPE IF EXISTS BL_CL.t_dim_load_config CASCADE;
DROP TYPE IF EXISTS BL_CL.t_geography_hierarchy CASCADE;
DROP TYPE IF EXISTS BL_CL.t_product_hierarchy CASCADE;

DROP TYPE IF EXISTS BL_CL.t_source_mapping CASCADE;
DROP TYPE IF EXISTS BL_CL.t_dim_parameter CASCADE;
-- =====================================================
-- SECTION 2: PARAMETER/CONFIG COMPOSITE TYPES
-- =====================================================

-- Core dimension loading configuration

CREATE TYPE BL_CL.t_dim_load_config AS
(
    source_table          VARCHAR(100), -- e.g., 'BL_3NF.CE_CUSTOMERS'
    target_table          VARCHAR(100), -- e.g., 'BL_DM.DIM_CUSTOMERS'
    business_key_column   VARCHAR(100), -- e.g., 'CUSTOMER_SRC_ID'
    surrogate_key_column  VARCHAR(100), -- e.g., 'CUSTOMER_SURR_ID'
    load_mode             VARCHAR(20),  -- 'DELTA', 'FULL', 'SCD2'
    include_source_system VARCHAR(50),  -- Filter: '3NF_LAYER', 'OMS', 'LMS', 'ALL'
    validation_level      VARCHAR(10),  -- 'STRICT', 'RELAXED', 'NONE'
    batch_size            INTEGER,      -- For cursor processing (0 = unlimited)
    enable_logging        BOOLEAN       -- Enable detailed logging
);



-- Geography hierarchy structure for complex flattening
CREATE TYPE BL_CL.t_geography_hierarchy AS
(
    geography_src_id VARCHAR(50),
    city_name        VARCHAR(100),
    city_src_id      VARCHAR(50),
    state_name       VARCHAR(100),
    state_src_id     VARCHAR(50),
    state_code       VARCHAR(10),
    country_name     VARCHAR(100),
    country_src_id   VARCHAR(50),
    country_code     VARCHAR(10),
    region_name      VARCHAR(100),
    region_src_id    VARCHAR(50),
    source_system    VARCHAR(50),
    source_entity    VARCHAR(100)
);

-- Product hierarchy structure for SCD2 and flattening
CREATE TYPE BL_CL.t_product_hierarchy AS
(
    product_src_id          VARCHAR(50),
    product_name            VARCHAR(255),
    brand_name              VARCHAR(100),
    brand_src_id             VARCHAR(50),
    primary_category_name   VARCHAR(100),
    primary_category_src_id VARCHAR(100),
    department_name         VARCHAR(100),
    department_src_id        VARCHAR(50),
    all_category_src_ids    TEXT, -- Pipe delimited
    all_category_names      TEXT, -- Pipe delimited
    status_name             VARCHAR(50),
    status_src_id            VARCHAR(50),
    source_system           VARCHAR(50),
    source_entity           VARCHAR(100),
    effective_date          DATE  -- For SCD2 processing
);

-- =====================================================
-- SECTION 3: RESULT/OUTPUT COMPOSITE TYPES
-- =====================================================

-- Standard dimension loading result statistics
CREATE TYPE BL_CL.t_dim_load_result AS
(
    rows_inserted     INTEGER,
    rows_updated      INTEGER,
    rows_deleted      INTEGER,
    rows_unchanged    INTEGER,
    validation_errors INTEGER,
    business_errors   INTEGER,
    execution_time_ms INTEGER,
    records_processed INTEGER,
    start_time        TIMESTAMP,
    end_time          TIMESTAMP,
    status            VARCHAR(20), -- 'SUCCESS', 'WARNING', 'ERROR'
    message           TEXT,
    error_details     TEXT
);

-- Data validation result for quality checks
CREATE TYPE BL_CL.t_dim_validation_result AS
(
    validation_rule       VARCHAR(100),
    failed_count          INTEGER,
    total_count           INTEGER,
    failure_rate          DECIMAL(5, 2),
    severity              VARCHAR(10), -- 'ERROR', 'WARNING', 'INFO'
    sample_failed_records TEXT,        -- Comma delimited sample IDs
    recommendation        TEXT
);

-- SCD2 change detection and tracking
CREATE TYPE BL_CL.t_dim_scd2_change_record AS
(
    source_key        VARCHAR(50),
    change_type       VARCHAR(20),  -- 'NEW', 'CHANGED', 'DELETED', 'UNCHANGED'
    change_reason     VARCHAR(100), -- Which attributes changed
    effective_date    DATE,
    expiration_date   DATE,
    old_surrogate_key BIGINT,       -- For updates
    new_surrogate_key BIGINT,       -- For inserts
    attribute_changes TEXT,         -- JSON or delimited list of changes
    confidence_level  VARCHAR(10)   -- 'HIGH', 'MEDIUM', 'LOW'
);

-- =====================================================
-- SECTION 4: UTILITY COMPOSITE TYPES
-- =====================================================

-- Generic key-value pair for flexible parameter passing
CREATE TYPE BL_CL.t_dim_parameter AS
(
    param_name        VARCHAR(100),
    param_value       TEXT,
    param_type        VARCHAR(20), -- 'STRING', 'INTEGER', 'DECIMAL', 'DATE', 'BOOLEAN'
    param_description VARCHAR(255)
);

-- Source system mapping for multi-source dimensions
CREATE TYPE BL_CL.t_source_mapping AS
(
    source_system       VARCHAR(50),
    source_entity       VARCHAR(100),
    source_key_column   VARCHAR(100),
    target_key_column   VARCHAR(100),
    transformation_rule VARCHAR(255),
    is_active           BOOLEAN,
    priority            INTEGER -- For conflict resolution
);

-- =====================================================
-- SECTION 5: VERIFICATION AND TESTING
-- =====================================================

-- Test composite type creation and basic functionality
DO
$$
    DECLARE
        v_config      BL_CL.t_dim_load_config;
        v_result      BL_CL.t_dim_load_result;
        v_geography   BL_CL.t_geography_hierarchy;
        v_product     BL_CL.t_product_hierarchy;
        v_validation  BL_CL.t_dim_validation_result;
        v_scd2_change BL_CL.t_dim_scd2_change_record;
        v_parameter   BL_CL.t_dim_parameter;
        v_mapping     BL_CL.t_source_mapping;
    BEGIN
        -- Test t_dim_load_config
        v_config := ROW (
            'BL_3NF.CE_CUSTOMERS', -- source_table
            'BL_DM.DIM_CUSTOMERS', -- target_table
            'CUSTOMER_SRC_ID', -- business_key_column
            'CUSTOMER_SURR_ID', -- surrogate_key_column
            'DELTA', -- load_mode
            '3NF_LAYER', -- include_source_system
            'STRICT', -- validation_level
            1000, -- batch_size
            TRUE -- enable_logging
            )::BL_CL.t_dim_load_config;

        -- Test t_dim_load_result
        v_result := ROW (
            100, 50, 5, 200, 0, 0, 5000, 355,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
            'SUCCESS', 'Test completed', NULL
            )::BL_CL.t_dim_load_result;

    -- Test t_geography_hierarchy
    v_geography := ROW(
        'New York|New York|USA',         -- geography_src_id
         'New York', 'NYC',  'New York', 'NY', 'NY',
         'USA', 'US', 'US',  'North America', 'NAM',
        '3NF_LAYER', -- source_system
            'CE_GEOGRAPHIES'
    )::BL_CL.t_geography_hierarchy;

    -- Test t_product_hierarchy
    v_product := ROW(
        'PROD001', 'Test Product',  'Test Brand', 'BRAND001',
          'CAT001', 'Electronics', 'DEPT001','Technology',
        'CAT001|CAT002', 'Electronics|Gadgets',
         'Active', 'ACTIVE', '3NF_LAYER', 'CE_PRODUCTS_SCD', CURRENT_DATE
    )::BL_CL.t_product_hierarchy;

        -- Test t_dim_validation_result
        v_validation := ROW (
            'Customer Email Format', 5, 1000, 0.50,
            'WARNING', 'CUST001,CUST002', 'Review email validation rules'
            )::BL_CL.t_dim_validation_result;

        -- Test t_dim_scd2_change_record
        v_scd2_change := ROW (
            'PROD001', 'CHANGED', 'Price updated', CURRENT_DATE, '9999-12-31'::DATE,
            100, 101, '{"price": {"old": 10.99, "new": 12.99}}', 'HIGH'
            )::BL_CL.t_dim_scd2_change_record;

        -- Test t_dim_parameter
        v_parameter := ROW (
            'max_batch_size', '1000', 'INTEGER', 'Maximum records to process per batch'
            )::BL_CL.t_dim_parameter;

        -- Test t_source_mapping
        v_mapping := ROW (
            'OMS', 'SRC_OMS', 'customer_src_id', 'CUSTOMER_SRC_ID',
            'UPPER(TRIM(source_value))', TRUE, 1
            )::BL_CL.t_source_mapping;

        -- Log successful creation
        RAISE NOTICE 'All composite types created and tested successfully!';
        RAISE NOTICE 'Sample config - Source: %, Target: %, Mode: %',
            v_config.source_table, v_config.target_table, v_config.load_mode;
        RAISE NOTICE 'Sample result - Inserted: %, Updated: %, Status: %',
            v_result.rows_inserted, v_result.rows_updated, v_result.status;
        RAISE NOTICE 'Sample geography - City: %, State: %, Country: %',
            v_geography.city_name, v_geography.state_name, v_geography.country_name;
        RAISE NOTICE 'Sample product - Name: %, Brand: %, Category: %',
            v_product.product_name, v_product.brand_name, v_product.primary_category_name;

    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Error testing composite types: %', SQLERRM;
    END
$$;

-- =====================================================
-- SECTION 6: VERIFICATION QUERIES
-- =====================================================

-- Show all created composite types in BL_CL schema
SELECT n.nspname                         AS schema_name,
       t.typname                         AS type_name,
       CASE
           WHEN t.typtype = 'c' THEN 'Composite Type'
           WHEN t.typtype = 'e' THEN 'Enum Type'
           ELSE 'Other Type'
           END                           AS type_category,
       obj_description(t.oid, 'pg_type') AS description
FROM pg_type t
         JOIN pg_namespace n ON t.typnamespace = n.oid
WHERE n.nspname = 'bl_cl'
  AND t.typname LIKE 't_dim_%'
ORDER BY t.typname;

-- Show composite type attributes for documentation
SELECT t.typname                                              AS composite_type,
       a.attname                                              AS attribute_name,
       pg_catalog.format_type(a.atttypid, a.atttypmod)        AS data_type,
       a.attnum                                               AS attribute_order,
       CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE 'NULL' END AS null_constraint
FROM pg_type t
         JOIN pg_namespace n ON t.typnamespace = n.oid
         JOIN pg_attribute a ON a.attrelid = t.typrelid
WHERE n.nspname = 'bl_cl'
  AND t.typname LIKE 't_dim_%'
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY t.typname, a.attnum;

-- Verify composite types can be used in function signatures (syntax check)
SELECT 'Composite types ready for use in procedure parameters and return values' AS status,
       COUNT(*)                                                                  AS total_types_created
FROM pg_type t
         JOIN pg_namespace n ON t.typnamespace = n.oid
WHERE n.nspname = 'bl_cl'
  AND t.typname LIKE 't_dim_%';

COMMIT;

-- =====================================================
-- USAGE EXAMPLES FOR DEVELOPERS
-- =====================================================

/*
USAGE EXAMPLES:

1. Function Parameter Example:
CREATE OR REPLACE FUNCTION BL_CL.load_dimension_generic(
    p_config BL_CL.t_dim_load_config
) RETURNS BL_CL.t_dim_load_result

2. Variable Declaration Example:
DECLARE
    v_config BL_CL.t_dim_load_config;
    v_result BL_CL.t_dim_load_result;
BEGIN
    v_config.source_table := 'BL_3NF.CE_CUSTOMERS';
    v_config.target_table := 'BL_DM.DIM_CUSTOMERS';
    v_config.load_mode := 'DELTA';
END;

3. Function Return Example:
RETURN ROW(
    inserted_count, updated_count, deleted_count, unchanged_count,
    validation_errors, business_errors, execution_time,
    total_processed, start_time, end_time,
    'SUCCESS', 'Load completed', NULL
)::BL_CL.t_dim_load_result;

4. Array of Composite Types:
DECLARE
    v_validations BL_CL.t_dim_validation_result[];
    v_changes BL_CL.t_dim_scd2_change_record[];
*/