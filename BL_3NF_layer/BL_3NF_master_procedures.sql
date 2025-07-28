-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - MASTER ETL ORCHESTRATION
-- Purpose: Create master procedures for complete ETL orchestration
-- Run as: dwh_cleansing_user
-- Dependencies: All dimension and fact procedures completed
-- =====================================================

SET ROLE dwh_cleansing_user;
-- Set search path to work in BL_CL schema
SET search_path = BL_CL, BL_3NF, SA_OMS, SA_LMS, public;

-- =====================================================
-- SECTION 1: DIMENSION ORCHESTRATION PROCEDURE
-- =====================================================

-- MASTER PROCEDURE: Load all dimension tables in dependency order
CREATE OR REPLACE PROCEDURE BL_CL.load_all_dimensions()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time        TIMESTAMP := CURRENT_TIMESTAMP;
    v_total_rows        INTEGER   := 0;
    v_execution_time    INTEGER;
    v_procedure_count   INTEGER   := 0;
    v_failed_procedures TEXT      := '';
BEGIN
    -- Check if procedure is already running
    IF BL_CL.is_procedure_running('load_all_dimensions') THEN
        RAISE EXCEPTION 'Procedure load_all_dimensions is already running';
    END IF;

    -- Log master procedure start
    CALL BL_CL.log_procedure_event(
            'load_all_dimensions', 'SA_OMS,SA_LMS', 'ALL_DIMENSIONS', 'START', 0,
            'Starting complete dimension loading orchestration'
         );

    -- PHASE 1: GEOGRAPHIC HIERARCHY (Load in dependency order)
    BEGIN
        CALL BL_CL.load_ce_regions();
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_regions, ';
            RAISE NOTICE 'Failed to load regions: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_countries();
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_countries, ';
            RAISE NOTICE 'Failed to load countries: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_states();
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_states, ';
            RAISE NOTICE 'Failed to load states: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_cities();
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_cities, ';
            RAISE NOTICE 'Failed to load cities: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_geographies(); -- Uses FOR LOOP function
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_geographies, ';
            RAISE NOTICE 'Failed to load geographies: %', SQLERRM;
    END;

    -- PHASE 2: PRODUCT HIERARCHY (Load in dependency order)
    BEGIN
        CALL BL_CL.load_ce_departments();
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_departments, ';
            RAISE NOTICE 'Failed to load departments: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_categories();
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_categories, ';
            RAISE NOTICE 'Failed to load categories: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_brands();
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_brands, ';
            RAISE NOTICE 'Failed to load brands: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_product_statuses(); -- Uses MERGE
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_product_statuses, ';
            RAISE NOTICE 'Failed to load product statuses: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_products_scd(); -- SCD Type 2 with staging function
        v_procedure_count := v_procedure_count + 1;

    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_products_scd, ';
            RAISE NOTICE 'Failed to load products SCD: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_brand_categories(); -- Bridge table
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_brand_categories, ';
            RAISE NOTICE 'Failed to load brand categories: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_product_categories(); -- Bridge table
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_product_categories, ';
            RAISE NOTICE 'Failed to load product categories: %', SQLERRM;
    END;

    -- PHASE 3: BUSINESS ENTITIES
    BEGIN
        CALL BL_CL.load_ce_customers(); -- Uses proven deduplication logic
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_customers, ';
            RAISE NOTICE 'Failed to load customers: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_sales_representatives();
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_sales_representatives, ';
            RAISE NOTICE 'Failed to load sales representatives: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_warehouses();
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_warehouses, ';
            RAISE NOTICE 'Failed to load warehouses: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_carriers();
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_carriers, ';
            RAISE NOTICE 'Failed to load carriers: %', SQLERRM;
    END;

    -- PHASE 4: OPERATIONAL DIMENSIONS
    BEGIN
        CALL BL_CL.load_ce_order_statuses(); -- Uses MERGE
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_order_statuses, ';
            RAISE NOTICE 'Failed to load order statuses: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_payment_methods();
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_payment_methods, ';
            RAISE NOTICE 'Failed to load payment methods: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_shipping_modes();
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_shipping_modes, ';
            RAISE NOTICE 'Failed to load shipping modes: %', SQLERRM;
    END;

    BEGIN
        CALL BL_CL.load_ce_delivery_statuses();
        v_procedure_count := v_procedure_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_delivery_statuses, ';
            RAISE NOTICE 'Failed to load delivery statuses: %', SQLERRM;
    END;

    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Determine overall status
    IF v_failed_procedures = '' THEN
        -- All procedures succeeded
        CALL BL_CL.log_procedure_event(
                'load_all_dimensions', 'SA_OMS,SA_LMS', 'ALL_DIMENSIONS', 'SUCCESS',
                v_procedure_count, FORMAT('All %s dimension procedures completed successfully', v_procedure_count),
                v_execution_time
             );
    ELSE
        -- Some procedures failed
        CALL BL_CL.log_procedure_event(
                'load_all_dimensions', 'SA_OMS,SA_LMS', 'ALL_DIMENSIONS', 'WARNING',
                v_procedure_count, FORMAT('Completed %s procedures. Failed: %s', v_procedure_count,
                                          TRIM(TRAILING ', ' FROM v_failed_procedures)), v_execution_time
             );

        RAISE NOTICE 'Dimension loading completed with some failures. Check logs for details.';
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_all_dimensions', 'SA_OMS,SA_LMS', 'ALL_DIMENSIONS', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        RAISE;
END
$$;

-- =====================================================
-- SECTION 2: FACT ORCHESTRATION PROCEDURE - FIXED
-- =====================================================

-- MASTER PROCEDURE: Load all fact tables using shared temp tables
CREATE OR REPLACE PROCEDURE BL_CL.load_all_facts()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time          TIMESTAMP := CURRENT_TIMESTAMP;
    v_execution_time      INTEGER;
    v_procedure_count     INTEGER   := 0;
    v_failed_procedures   TEXT      := '';
    v_temp_table_created  BOOLEAN   := FALSE;
    v_lock_acquired       BOOLEAN   := FALSE;
BEGIN
    -- Check if procedure is already running
    IF BL_CL.is_procedure_running('load_all_facts') THEN
        RAISE EXCEPTION 'Procedure load_all_facts is already running';
    END IF;

    -- Log master procedure start
    CALL BL_CL.log_procedure_event(
        'load_all_facts', 'SA_OMS,SA_LMS', 'ALL_FACTS', 'START', 0,
        'Starting fact loading orchestration'
    );

--     -- PHASE 1: Create clean LMS temp table (only one needed!)
--     BEGIN
--         CALL BL_CL._create_clean_lms_data();
--         v_temp_table_created := TRUE;
--         v_procedure_count := v_procedure_count + 1;
--         RAISE NOTICE 'PHASE 1: Clean LMS temp table created successfully';
--     EXCEPTION
--         WHEN OTHERS THEN
--             v_failed_procedures := v_failed_procedures || '_create_clean_lms_data, ';
--             RAISE NOTICE 'PHASE 1: Clean LMS temp table creation failed: %', SQLERRM;
--     END;

    -- PHASE 2: Load all CE (fact) tables using idempotent procedures
    BEGIN
        RAISE NOTICE 'start load CE_ORDERS';
        CALL BL_CL.load_ce_orders_idempotent();
        v_procedure_count := v_procedure_count + 1;
        RAISE NOTICE 'CE_ORDERS loaded';
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_orders_idempotent, ';
            RAISE NOTICE 'Failed to load CE orders: %', SQLERRM;
    END;

    BEGIN
         RAISE NOTICE 'start load CE_ORDER_LINES';
        CALL BL_CL.load_ce_order_lines_idempotent();
        v_procedure_count := v_procedure_count + 1;
          RAISE NOTICE 'CE_ORDER_LINES loaded';
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_order_lines_idempotent, ';
            RAISE NOTICE 'Failed to load CE order lines: %', SQLERRM;
    END;

    BEGIN
         RAISE NOTICE 'start load CE_TRANSACTIONS';
        CALL BL_CL.load_ce_transactions_idempotent();
        v_procedure_count := v_procedure_count + 1;
         RAISE NOTICE 'CE_TRANSACTIONS loaded';
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_transactions_idempotent, ';
            RAISE NOTICE 'Failed to load CE transactions: %', SQLERRM;
    END;

    BEGIN
        RAISE NOTICE 'start load CE_SHIPMENTS';
        CALL BL_CL.load_ce_shipments_idempotent();
        v_procedure_count := v_procedure_count + 1;
        RAISE NOTICE 'CE_SHIPMENTS loaded';
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_shipments_idempotent, ';
            RAISE NOTICE 'Failed to load CE shipments: %', SQLERRM;
    END;

    BEGIN
        RAISE NOTICE 'start load CE_SHIPMENT_LINES';
        CALL BL_CL.load_ce_shipment_lines_idempotent();
        v_procedure_count := v_procedure_count + 1;
         RAISE NOTICE 'E_SHIPMENT_LINES loaded';
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_shipment_lines_idempotent, ';
            RAISE NOTICE 'Failed to load CE shipment lines: %', SQLERRM;
    END;

    BEGIN
         RAISE NOTICE 'start load CE_DELIVERIES';
        CALL BL_CL.load_ce_deliveries_idempotent();
        v_procedure_count := v_procedure_count + 1;
         RAISE NOTICE 'CE_DELIVERIES loaded';
    EXCEPTION
        WHEN OTHERS THEN
            v_failed_procedures := v_failed_procedures || 'load_ce_deliveries_idempotent, ';
            RAISE NOTICE 'Failed to load CE deliveries: %', SQLERRM;
    END;

--     -- PHASE 3: Cleanup temp tables
--     BEGIN
--         DROP TABLE IF EXISTS clean_lms_data;
--         RAISE NOTICE 'PHASE 3: Temporary tables cleaned up successfully';
--     EXCEPTION
--         WHEN OTHERS THEN
--             RAISE NOTICE 'PHASE 3: Cleanup warning (non-critical): %', SQLERRM;
--     END;

    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Determine overall status
    IF v_failed_procedures = '' THEN
        -- All procedures succeeded
        CALL BL_CL.log_procedure_event(
                'load_all_facts', 'SA_OMS,SA_LMS', 'ALL_FACTS', 'SUCCESS',
                v_procedure_count, FORMAT('All %s fact procedures completed successfully', v_procedure_count),
                v_execution_time
             );
        RAISE NOTICE 'CE (FACT) LOADING SUCCESS: All %s CE procedures completed in % ms', v_procedure_count, v_execution_time;
    ELSE
        -- Some procedures failed
        CALL BL_CL.log_procedure_event(
                'load_all_facts', 'SA_OMS,SA_LMS', 'ALL_FACTS', 'WARNING',
                v_procedure_count, FORMAT('Completed %s procedures. Failed: %s', v_procedure_count,
                                          TRIM(TRAILING ', ' FROM v_failed_procedures)), v_execution_time
             );
        RAISE NOTICE 'CE (FACT) LOADING PARTIAL: Completed with some failures: %', TRIM(TRAILING ', ' FROM v_failed_procedures);
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        -- Cleanup temp tables on error
        BEGIN
            DROP TABLE IF EXISTS clean_lms_data;
        EXCEPTION
            WHEN OTHERS THEN
                NULL; -- Ignore cleanup errors
        END;

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_all_facts', 'SA_OMS,SA_LMS', 'ALL_FACTS', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        RAISE;
END
$$;

-- =====================================================
-- SECTION 3: COMPLETE ETL ORCHESTRATION PROCEDURE
-- =====================================================

-- MASTER PROCEDURE: Complete 3NF layer loading (dimensions + facts)
CREATE OR REPLACE PROCEDURE BL_CL.load_bl_3nf_full()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time        TIMESTAMP := CURRENT_TIMESTAMP;
    v_execution_time    INTEGER;
    v_dimensions_status TEXT      := 'NOT_STARTED';
    v_facts_status      TEXT      := 'NOT_STARTED';
BEGIN
    -- Check if procedure is already running
    IF BL_CL.is_procedure_running('load_bl_3nf_full') THEN
        RAISE EXCEPTION 'Procedure load_bl_3nf_full is already running';
    END IF;

    -- Log complete ETL start
    CALL BL_CL.log_procedure_event(
            'load_bl_3nf_full', 'SA_OMS,SA_LMS', 'COMPLETE_3NF', 'START', 0,
            'Starting complete 3NF ETL: dimensions + facts'
         );

    -- PHASE 1: Load all dimensions first (dependencies for facts)
    BEGIN
        RAISE NOTICE 'PHASE 1: Starting dimension loading...';
        CALL BL_CL.load_all_dimensions();
        v_dimensions_status := 'SUCCESS';
        RAISE NOTICE 'PHASE 1: Dimension loading completed successfully';
--         -- update start_date of products
--         UPDATE BL_3NF.CE_PRODUCTS_SCD
--         SET start_dt = '2020-01-01'::DATE
--         WHERE source_system IN ('OMS', 'LMS');
--         RAISE NOTICE 'PHASE 1: Dimension loading: completed successfully + start_date products UPDATED ';
    EXCEPTION
        WHEN OTHERS THEN
            v_dimensions_status := 'FAILED';
            RAISE NOTICE 'PHASE 1: Dimension loading failed: %', SQLERRM;
        -- Continue to facts anyway (partial load scenario)
    END;

    -- PHASE 2: Load all facts (depends on dimensions being loaded)
    BEGIN
        RAISE NOTICE 'PHASE 2: Starting fact loading...';
        CALL BL_CL.load_all_facts();
        v_facts_status := 'SUCCESS';
        RAISE NOTICE 'PHASE 2: Fact loading completed successfully';
    EXCEPTION
        WHEN OTHERS THEN
            v_facts_status := 'FAILED';
            RAISE NOTICE 'PHASE 2: Fact loading failed: %', SQLERRM;
    END;

    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Determine overall ETL status
    IF v_dimensions_status = 'SUCCESS' AND v_facts_status = 'SUCCESS' THEN
        -- Complete success
        CALL BL_CL.log_procedure_event(
                'load_bl_3nf_full', 'SA_OMS,SA_LMS', 'COMPLETE_3NF', 'SUCCESS',
                26, 'Complete 3NF ETL completed successfully: all dimensions and facts loaded', v_execution_time
             );
        RAISE NOTICE 'COMPLETE ETL SUCCESS: All 20 dimensions + 6 facts loaded successfully in % ms', v_execution_time;

    ELSIF v_dimensions_status = 'SUCCESS' AND v_facts_status = 'FAILED' THEN
        -- Partial success
        CALL BL_CL.log_procedure_event(
                'load_bl_3nf_full', 'SA_OMS,SA_LMS', 'COMPLETE_3NF', 'WARNING',
                20, 'Partial ETL success: dimensions loaded, facts failed', v_execution_time
             );
        RAISE NOTICE 'PARTIAL ETL SUCCESS: Dimensions loaded successfully, but facts failed. Check fact loading logs.';

    ELSIF v_dimensions_status = 'FAILED' AND v_facts_status = 'SUCCESS' THEN
        -- Unusual case
        CALL BL_CL.log_procedure_event(
                'load_bl_3nf_full', 'SA_OMS,SA_LMS', 'COMPLETE_3NF', 'WARNING',
                6, 'Partial ETL success: facts loaded, dimensions had issues', v_execution_time
             );
        RAISE NOTICE 'PARTIAL ETL SUCCESS: Facts loaded successfully, but dimensions had issues. Check dimension loading logs.';

    ELSE
        -- Both failed
        CALL BL_CL.log_procedure_event(
                'load_bl_3nf_full', 'SA_OMS,SA_LMS', 'COMPLETE_3NF', 'ERROR',
                0, 'Complete ETL failed: both dimensions and facts failed', v_execution_time
             );
        RAISE EXCEPTION 'COMPLETE ETL FAILURE: Both dimension and fact loading failed. Check individual procedure logs for details.';
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_bl_3nf_full', 'SA_OMS,SA_LMS', 'COMPLETE_3NF', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        RAISE;
END
$$;

-- =====================================================
-- SECTION 4: INCREMENTAL LOADING PROCEDURE -- NOT IMPLEMENTED
-- =====================================================

-- PROCEDURE: Incremental loading for delta updates
CREATE OR REPLACE PROCEDURE BL_CL.load_bl_3nf_incremental()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_execution_time INTEGER;
    v_last_full_load TIMESTAMP;
BEGIN
    -- Check if procedure is already running
    IF BL_CL.is_procedure_running('load_bl_3nf_incremental') THEN
        RAISE EXCEPTION 'Procedure load_bl_3nf_incremental is already running';
    END IF;

    -- Get last successful full load time
    SELECT MAX(log_datetime)
    INTO v_last_full_load
    FROM BL_CL.MTA_PROCESS_LOG
    WHERE procedure_name = 'load_bl_3nf_full'
      AND status = 'SUCCESS';

    -- Log incremental ETL start
    CALL BL_CL.log_procedure_event(
            'load_bl_3nf_incremental', 'SA_OMS,SA_LMS', 'INCREMENTAL_3NF', 'START', 0,
            FORMAT('Starting incremental 3NF ETL since last full load: %s', COALESCE(v_last_full_load::TEXT, 'NEVER'))
         );

    -- Load dimensions (most will skip if no new data)
    RAISE NOTICE 'Loading dimensions incrementally...';
    CALL BL_CL.load_all_dimensions();

    -- Load facts (will only load new transactions/shipments)
    RAISE NOTICE 'Loading facts incrementally...';
    CALL BL_CL.load_all_facts();

    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_bl_3nf_incremental', 'SA_OMS,SA_LMS', 'INCREMENTAL_3NF', 'SUCCESS',
            0, 'Incremental 3NF ETL completed successfully', v_execution_time
         );

    RAISE NOTICE 'INCREMENTAL ETL SUCCESS: Completed in % ms', v_execution_time;

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_bl_3nf_incremental', 'SA_OMS,SA_LMS', 'INCREMENTAL_3NF', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        RAISE;
END
$$;

-- =====================================================
-- SECTION 5: VERIFICATION AND MONITORING QUERIES
-- =====================================================

-- Verify all master procedures were created
SELECT routine_name,
       routine_type,
       routine_definition IS NOT NULL as has_definition
FROM information_schema.routines
WHERE routine_schema = 'bl_cl'
  AND routine_type = 'PROCEDURE'
  AND routine_name IN (
                       'load_all_dimensions',
                       'load_all_facts',
                       'load_bl_3nf_full',
                       'load_bl_3nf_incremental'
    )
ORDER BY routine_name;

-- Master procedure execution summary
SELECT procedure_name,
       COUNT(*)                                                     as total_executions,
       COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END)               as successful_runs,
       COUNT(CASE WHEN status = 'ERROR' THEN 1 END)                 as failed_runs,
       COUNT(CASE WHEN status = 'WARNING' THEN 1 END)               as partial_runs,
       MAX(CASE WHEN status = 'SUCCESS' THEN log_datetime END)      as last_successful_run,
       AVG(CASE WHEN status = 'SUCCESS' THEN execution_time_ms END) as avg_execution_time_ms
FROM BL_CL.MTA_PROCESS_LOG
WHERE procedure_name IN ('load_all_dimensions', 'load_all_facts', 'load_bl_3nf_full', 'load_bl_3nf_incremental')
GROUP BY procedure_name
ORDER BY procedure_name;

COMMIT;