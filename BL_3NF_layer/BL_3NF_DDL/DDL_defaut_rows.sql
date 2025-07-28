SET search_path = BL_3NF, public;

-- =====================================================
-- SECTION 8: DEFAULT ROWS INSERTION
-- =====================================================

-- PROCEDURE: Load Default Rows for All Dimension Tables
CREATE OR REPLACE PROCEDURE BL_CL.load_default_rows()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_total_rows     INTEGER   := 0;
    v_execution_time INTEGER;
    v_table_name     TEXT;
    v_temp_rows      INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_default_rows') THEN
        RAISE EXCEPTION 'Procedure load_default_rows is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.ALL_DIMENSION_TABLES', 'START', 0, 'Starting default rows load'
         );

    -- Set search path
    SET search_path = BL_3NF, public;

    -- Default row for Customers
    v_table_name := 'CE_CUSTOMERS';
    INSERT INTO CE_CUSTOMERS (CUSTOMER_ID, CUSTOMER_SRC_ID, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME, CUSTOMER_GENDER,
                              CUSTOMER_YEAR_OF_BIRTH, CUSTOMER_EMAIL, CUSTOMER_SEGMENT, SOURCE_SYSTEM, SOURCE_ENTITY,
                              TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1,
           'n.a.',
           'n.a.',
           'n.a.',
           'n.a.',
           1900,
           'n.a.',
           'n.a.',
           'MANUAL',
           'DEFAULT',
           CURRENT_TIMESTAMP,
           CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_CUSTOMERS WHERE CUSTOMER_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Departments
    v_table_name := 'CE_DEPARTMENTS';
    INSERT INTO CE_DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_SRC_ID, DEPARTMENT_NAME, SOURCE_SYSTEM, SOURCE_ENTITY,
                                TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', 'n.a.', 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_DEPARTMENTS WHERE DEPARTMENT_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Categories
    v_table_name := 'CE_CATEGORIES';
    INSERT INTO CE_CATEGORIES (CATEGORY_ID, CATEGORY_SRC_ID, CATEGORY_NAME, CATEGORY_CODE, DEPARTMENT_ID, SOURCE_SYSTEM,
                               SOURCE_ENTITY, TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', 'n.a.', 'n.a.', -1, 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_CATEGORIES WHERE CATEGORY_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Brands
    v_table_name := 'CE_BRANDS';
    INSERT INTO CE_BRANDS (BRAND_ID, BRAND_SRC_ID, BRAND_NAME, SOURCE_SYSTEM, SOURCE_ENTITY,
                           TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', 'n.a.', 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_BRANDS WHERE BRAND_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Brand Categories
    v_table_name := 'CE_BRAND_CATEGORIES';
    INSERT INTO CE_BRAND_CATEGORIES(BRAND_CATEGORY_ID, BRAND_ID, CATEGORY_ID, RELATIONSHIP_STRENGTH, PRODUCT_COUNT,
                                    SOURCE_SYSTEM, SOURCE_ENTITY, TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, -1, -1, -1, -1, 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_BRAND_CATEGORIES WHERE BRAND_CATEGORY_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Product Categories
    v_table_name := 'CE_PRODUCT_CATEGORIES';
    INSERT INTO CE_PRODUCT_CATEGORIES(PRODUCT_CATEGORY_ID, PRODUCT_ID, CATEGORY_ID, IS_PRIMARY, SOURCE_SYSTEM,
                                      SOURCE_ENTITY, TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, -1, -1, 'n.a', 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_PRODUCT_CATEGORIES WHERE PRODUCT_CATEGORY_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Product Statuses
    v_table_name := 'CE_PRODUCT_STATUSES';
    INSERT INTO CE_PRODUCT_STATUSES (STATUS_ID, STATUS_SRC_ID, STATUS_NAME, SOURCE_SYSTEM, SOURCE_ENTITY,
                                     TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', 'n.a.', 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_PRODUCT_STATUSES WHERE STATUS_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Regions
    v_table_name := 'CE_REGIONS';
    INSERT INTO CE_REGIONS (REGION_ID, REGION_SRC_ID, REGION_NAME, REGION_CODE, SOURCE_SYSTEM, SOURCE_ENTITY,
                            TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', 'n.a.', 'n.a.', 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_REGIONS WHERE REGION_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Countries
    v_table_name := 'CE_COUNTRIES';
    INSERT INTO CE_COUNTRIES (COUNTRY_ID, COUNTRY_SRC_ID, COUNTRY_NAME, COUNTRY_CODE, REGION_ID, SOURCE_SYSTEM,
                              SOURCE_ENTITY, TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', 'n.a.', 'n.a.', -1, 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_COUNTRIES WHERE COUNTRY_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for States
    v_table_name := 'CE_STATES';
    INSERT INTO CE_STATES (STATE_ID, STATE_SRC_ID, STATE_NAME, STATE_CODE, COUNTRY_ID, SOURCE_SYSTEM, SOURCE_ENTITY,
                           TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', 'n.a.', 'n.a.', -1, 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_STATES WHERE STATE_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Cities
    v_table_name := 'CE_CITIES';
    INSERT INTO CE_CITIES (CITY_ID, CITY_SRC_ID, CITY_NAME, CITY_CODE, STATE_ID, SOURCE_SYSTEM, SOURCE_ENTITY,
                           TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', 'n.a.', 'n.a.', -1, 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_CITIES WHERE CITY_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Geographies
    v_table_name := 'CE_GEOGRAPHIES';
    INSERT INTO CE_GEOGRAPHIES (GEOGRAPHY_ID, GEOGRAPHY_SRC_ID, CITY_ID, SOURCE_SYSTEM, SOURCE_ENTITY,
                                TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', -1, 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_GEOGRAPHIES WHERE GEOGRAPHY_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Sales Representatives
    v_table_name := 'CE_SALES_REPRESENTATIVES';
    INSERT INTO CE_SALES_REPRESENTATIVES (SALES_REP_ID, SALES_REP_SRC_ID, SALES_REP_NAME, SOURCE_SYSTEM,
                                          SOURCE_ENTITY, TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', 'n.a.', 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_SALES_REPRESENTATIVES WHERE SALES_REP_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Warehouses
    v_table_name := 'CE_WAREHOUSES';
    INSERT INTO CE_WAREHOUSES (WAREHOUSE_ID, WAREHOUSE_SRC_ID, WAREHOUSE_NAME, SOURCE_SYSTEM, SOURCE_ENTITY,
                               TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', 'n.a.', 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_WAREHOUSES WHERE WAREHOUSE_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Carriers
    v_table_name := 'CE_CARRIERS';
    INSERT INTO CE_CARRIERS (CARRIER_ID, CARRIER_SRC_ID, CARRIER_NAME, CARRIER_TYPE, SOURCE_SYSTEM, SOURCE_ENTITY,
                             TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', 'n.a.', 'n.a.', 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_CARRIERS WHERE CARRIER_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Order Statuses
    v_table_name := 'CE_ORDER_STATUSES';
    INSERT INTO CE_ORDER_STATUSES (ORDER_STATUS_ID, ORDER_STATUS_SRC_ID, ORDER_STATUS, SOURCE_SYSTEM, SOURCE_ENTITY,
                                   TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', 'n.a.', 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_ORDER_STATUSES WHERE ORDER_STATUS_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Payment Methods
    v_table_name := 'CE_PAYMENT_METHODS';
    INSERT INTO CE_PAYMENT_METHODS (PAYMENT_METHOD_ID, PAYMENT_METHOD_SRC_ID, PAYMENT_METHOD, SOURCE_SYSTEM, SOURCE_ENTITY,
                                    TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', 'n.a.', 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_PAYMENT_METHODS WHERE PAYMENT_METHOD_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Shipping Modes
    v_table_name := 'CE_SHIPPING_MODES';
    INSERT INTO CE_SHIPPING_MODES (SHIPPING_MODE_ID, SHIPPING_MODE_SRC_ID, SHIPPING_MODE, SOURCE_SYSTEM, SOURCE_ENTITY,
                                   TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', 'n.a.', 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_SHIPPING_MODES WHERE SHIPPING_MODE_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Delivery Statuses
    v_table_name := 'CE_DELIVERY_STATUSES';
    INSERT INTO CE_DELIVERY_STATUSES (DELIVERY_STATUS_ID, DELIVERY_STATUS_SRC_ID, DELIVERY_STATUS, SOURCE_SYSTEM,
                                      SOURCE_ENTITY, TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1, 'n.a.', 'n.a.', 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_DELIVERY_STATUSES WHERE DELIVERY_STATUS_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    -- Default row for Products (SCD Type 2)
    v_table_name := 'CE_PRODUCTS_SCD';
    INSERT INTO CE_PRODUCTS_SCD (PRODUCT_ID, START_DT, PRODUCT_SRC_ID, PRODUCT_NAME, BRAND_ID, PRIMARY_CATEGORY_ID,
                                 STATUS_ID, END_DT, IS_ACTIVE, SOURCE_SYSTEM, SOURCE_ENTITY, TA_INSERT_DT, TA_UPDATE_DT)
    SELECT -1,
           '1990-01-01',
           'n.a.',
           'n.a.',
           -1,
           -1,
           -1,
           '9999-12-31',
           'Y',
           'MANUAL',
           'DEFAULT',
           CURRENT_TIMESTAMP,
           CURRENT_TIMESTAMP
    WHERE NOT EXISTS (SELECT 1 FROM CE_PRODUCTS_SCD WHERE PRODUCT_ID = -1);

    GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_temp_rows;

    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.' || v_table_name, 'INFO',
            v_temp_rows, 'Default row inserted for ' || v_table_name
         );

    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_default_rows', 'MANUAL', 'BL_3NF.ALL_DIMENSION_TABLES', 'SUCCESS',
            v_total_rows, 'Default rows load completed successfully', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_default_rows');
EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_default_rows');
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_default_rows', 'MANUAL', 'BL_3NF.ALL_DIMENSION_TABLES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;


-- =====================================================
-- SECTION 9: VERIFICATION QUERIES
-- =====================================================

-- Verify default rows
SELECT 'CE_REGIONS' as table_name, COUNT(*) as default_rows
FROM CE_REGIONS
WHERE REGION_ID = -1
UNION ALL
SELECT 'CE_COUNTRIES', COUNT(*)
FROM CE_COUNTRIES
WHERE COUNTRY_ID = -1
UNION ALL
SELECT 'CE_STATES', COUNT(*)
FROM CE_STATES
WHERE STATE_ID = -1
UNION ALL
SELECT 'CE_CITIES', COUNT(*)
FROM CE_CITIES
WHERE CITY_ID = -1
UNION ALL
SELECT 'CE_GEOGRAPHIES', COUNT(*)
FROM CE_GEOGRAPHIES
WHERE GEOGRAPHY_ID = -1
UNION ALL
SELECT 'CE_DEPARTMENTS', COUNT(*)
FROM CE_DEPARTMENTS
WHERE DEPARTMENT_ID = -1
UNION ALL
SELECT 'CE_CATEGORIES', COUNT(*)
FROM CE_CATEGORIES
WHERE CATEGORY_ID = -1
UNION ALL
SELECT 'CE_BRANDS', COUNT(*)
FROM CE_BRANDS
WHERE BRAND_ID = -1
UNION ALL
SELECT 'CE_BRAND_CATEGORIES', COUNT(*)
FROM CE_BRAND_CATEGORIES
WHERE BRAND_CATEGORY_ID = -1
UNION ALL
SELECT 'CE_PRODUCT_CATEGORIES', COUNT(*)
FROM CE_PRODUCT_CATEGORIES
WHERE PRODUCT_CATEGORY_ID = -1
UNION ALL
SELECT 'CE_PRODUCT_STATUSES', COUNT(*)
FROM CE_PRODUCT_STATUSES
WHERE STATUS_ID = -1
UNION ALL
SELECT 'CE_PRODUCTS_SCD', COUNT(*)
FROM CE_PRODUCTS_SCD
WHERE PRODUCT_ID = -1
UNION ALL
SELECT 'CE_CUSTOMERS', COUNT(*)
FROM CE_CUSTOMERS
WHERE CUSTOMER_ID = -1
UNION ALL
SELECT 'CE_SALES_REPRESENTATIVES', COUNT(*)
FROM CE_SALES_REPRESENTATIVES
WHERE SALES_REP_ID = -1
UNION ALL
SELECT 'CE_WAREHOUSES', COUNT(*)
FROM CE_WAREHOUSES
WHERE WAREHOUSE_ID = -1
UNION ALL
SELECT 'CE_CARRIERS', COUNT(*)
FROM CE_CARRIERS
WHERE CARRIER_ID = -1
UNION ALL
SELECT 'CE_ORDER_STATUSES', COUNT(*)
FROM CE_ORDER_STATUSES
WHERE ORDER_STATUS_ID = -1
UNION ALL
SELECT 'CE_PAYMENT_METHODS', COUNT(*)
FROM CE_PAYMENT_METHODS
WHERE PAYMENT_METHOD_ID = -1
UNION ALL
SELECT 'CE_SHIPPING_MODES', COUNT(*)
FROM CE_SHIPPING_MODES
WHERE SHIPPING_MODE_ID = -1
UNION ALL
SELECT 'CE_DELIVERY_STATUSES', COUNT(*)
FROM CE_DELIVERY_STATUSES
WHERE DELIVERY_STATUS_ID = -1;

