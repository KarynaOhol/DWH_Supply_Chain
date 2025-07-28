-- =====================================================
-- DIMENSION TESTING SUITE
-- Cleanup Script + Idempotency Testing
-- =====================================================

SELECT CURRENT_USER, SESSION_USER;
SET ROLE dwh_cleansing_user;
SET search_path = BL_CL, BL_3NF, BL_DM, public;

-- =====================================================
-- SECTION 1: DIMENSION TABLE CLEANUP SCRIPT
-- =====================================================

-- Function to clean all dimension tables (preserve default records with _id = -1)
CREATE OR REPLACE PROCEDURE BL_CL.cleanup_all_dimension_tables()
LANGUAGE plpgsql
AS $$
DECLARE
    v_sql TEXT;
    v_table_name TEXT;
    v_pk_column TEXT;
    v_deleted_count INTEGER;
    v_total_deleted INTEGER := 0;
BEGIN
    RAISE NOTICE 'Starting dimension table cleanup - preserving default records (_id = -1)...';

    -- Clean DIM_GEOGRAPHIES
    DELETE FROM BL_DM.DIM_GEOGRAPHIES WHERE GEOGRAPHY_SURR_ID != -1;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    v_total_deleted := v_total_deleted + v_deleted_count;
    RAISE NOTICE 'DIM_GEOGRAPHIES: % records deleted', v_deleted_count;

    -- Clean DIM_PRODUCTS (fresh start for full load testing)
    DELETE FROM BL_DM.DIM_PRODUCTS_SCD WHERE PRODUCT_SURR_ID != -1;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    v_total_deleted := v_total_deleted + v_deleted_count;
    RAISE NOTICE 'DIM_PRODUCTS: % records deleted', v_deleted_count;

    -- Clean DIM_CUSTOMERS
    DELETE FROM BL_DM.DIM_CUSTOMERS WHERE CUSTOMER_SURR_ID != -1;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    v_total_deleted := v_total_deleted + v_deleted_count;
    RAISE NOTICE 'DIM_CUSTOMERS: % records deleted', v_deleted_count;

    -- Clean DIM_SALES_REPRESENTATIVES
    DELETE FROM BL_DM.DIM_SALES_REPRESENTATIVES WHERE SALES_REP_SURR_ID != -1;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    v_total_deleted := v_total_deleted + v_deleted_count;
    RAISE NOTICE 'DIM_SALES_REPRESENTATIVES: % records deleted', v_deleted_count;

    -- Clean DIM_WAREHOUSES
    DELETE FROM BL_DM.DIM_WAREHOUSES WHERE WAREHOUSE_SURR_ID != -1;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    v_total_deleted := v_total_deleted + v_deleted_count;
    RAISE NOTICE 'DIM_WAREHOUSES: % records deleted', v_deleted_count;

    -- Clean DIM_CARRIERS
    DELETE FROM BL_DM.DIM_CARRIERS WHERE CARRIER_SURR_ID != -1;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    v_total_deleted := v_total_deleted + v_deleted_count;
    RAISE NOTICE 'DIM_CARRIERS: % records deleted', v_deleted_count;

    -- Clean DIM_ORDER_STATUSES
    DELETE FROM BL_DM.DIM_ORDER_STATUSES WHERE ORDER_STATUS_SURR_ID != -1;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    v_total_deleted := v_total_deleted + v_deleted_count;
    RAISE NOTICE 'DIM_ORDER_STATUSES: % records deleted', v_deleted_count;

    -- Clean DIM_PAYMENT_METHODS
    DELETE FROM BL_DM.DIM_PAYMENT_METHODS WHERE PAYMENT_METHOD_SURR_ID != -1;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    v_total_deleted := v_total_deleted + v_deleted_count;
    RAISE NOTICE 'DIM_PAYMENT_METHODS: % records deleted', v_deleted_count;

    -- Clean DIM_SHIPPING_MODES
    DELETE FROM BL_DM.DIM_SHIPPING_MODES WHERE SHIPPING_MODE_SURR_ID != -1;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    v_total_deleted := v_total_deleted + v_deleted_count;
    RAISE NOTICE 'DIM_SHIPPING_MODES: % records deleted', v_deleted_count;

    -- Clean DIM_DELIVERY_STATUSES
    DELETE FROM BL_DM.DIM_DELIVERY_STATUSES WHERE DELIVERY_STATUS_SURR_ID != -1;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    v_total_deleted := v_total_deleted + v_deleted_count;
    RAISE NOTICE 'DIM_DELIVERY_STATUSES: % records deleted', v_deleted_count;

    RAISE NOTICE 'CLEANUP COMPLETE: % total records deleted from all dimension tables', v_total_deleted;
    RAISE NOTICE 'All default records (_id = -1) have been preserved.';

    -- Clear procedure logs for clean testing
    DELETE FROM BL_CL.mta_process_log WHERE ta_insert_dt < CURRENT_TIMESTAMP;
    RAISE NOTICE 'Procedure logs cleared for clean testing.';
END $$;

-- =====================================================
-- SECTION 2: VERIFICATION QUERIES
-- =====================================================

-- Function to verify clean state of all dimension tables

CREATE OR REPLACE FUNCTION BL_CL.verify_dimension_clean_state()
RETURNS TABLE (
    table_name TEXT,
    total_records INTEGER,
    default_records INTEGER,
    data_records INTEGER,
    is_clean BOOLEAN
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH dimension_counts AS (
        SELECT 'DIM_GEOGRAPHIES'::TEXT as table_name,
               COUNT(*)::INTEGER as total_records,
               COUNT(*) FILTER (WHERE GEOGRAPHY_SURR_ID = -1)::INTEGER as default_records,
               COUNT(*) FILTER (WHERE GEOGRAPHY_SURR_ID != -1)::INTEGER as data_records
        FROM BL_DM.DIM_GEOGRAPHIES

        UNION ALL

        SELECT 'DIM_PRODUCTS'::TEXT,
               COUNT(*)::INTEGER,
               COUNT(*) FILTER (WHERE PRODUCT_SURR_ID = -1)::INTEGER,
               COUNT(*) FILTER (WHERE PRODUCT_SURR_ID != -1)::INTEGER
        FROM BL_DM.DIM_PRODUCTS_SCD

        UNION ALL

        SELECT 'DIM_CUSTOMERS'::TEXT,
               COUNT(*)::INTEGER,
               COUNT(*) FILTER (WHERE CUSTOMER_SURR_ID = -1)::INTEGER,
               COUNT(*) FILTER (WHERE CUSTOMER_SURR_ID != -1)::INTEGER
        FROM BL_DM.DIM_CUSTOMERS

        UNION ALL

        SELECT 'DIM_SALES_REPRESENTATIVES'::TEXT,
               COUNT(*)::INTEGER,
               COUNT(*) FILTER (WHERE SALES_REP_SURR_ID = -1)::INTEGER,
               COUNT(*) FILTER (WHERE SALES_REP_SURR_ID != -1)::INTEGER
        FROM BL_DM.DIM_SALES_REPRESENTATIVES

        UNION ALL

        SELECT 'DIM_WAREHOUSES'::TEXT,
               COUNT(*)::INTEGER,
               COUNT(*) FILTER (WHERE WAREHOUSE_SURR_ID = -1)::INTEGER,
               COUNT(*) FILTER (WHERE WAREHOUSE_SURR_ID != -1)::INTEGER
        FROM BL_DM.DIM_WAREHOUSES

        UNION ALL

        SELECT 'DIM_CARRIERS'::TEXT,
               COUNT(*)::INTEGER,
               COUNT(*) FILTER (WHERE CARRIER_SURR_ID = -1)::INTEGER,
               COUNT(*) FILTER (WHERE CARRIER_SURR_ID != -1)::INTEGER
        FROM BL_DM.DIM_CARRIERS

        UNION ALL

        SELECT 'DIM_ORDER_STATUSES'::TEXT,
               COUNT(*)::INTEGER,
               COUNT(*) FILTER (WHERE ORDER_STATUS_SURR_ID = -1)::INTEGER,
               COUNT(*) FILTER (WHERE ORDER_STATUS_SURR_ID != -1)::INTEGER
        FROM BL_DM.DIM_ORDER_STATUSES

        UNION ALL

        SELECT 'DIM_PAYMENT_METHODS'::TEXT,
               COUNT(*)::INTEGER,
               COUNT(*) FILTER (WHERE PAYMENT_METHOD_SURR_ID = -1)::INTEGER,
               COUNT(*) FILTER (WHERE PAYMENT_METHOD_SURR_ID != -1)::INTEGER
        FROM BL_DM.DIM_PAYMENT_METHODS

        UNION ALL

        SELECT 'DIM_SHIPPING_MODES'::TEXT,
               COUNT(*)::INTEGER,
               COUNT(*) FILTER (WHERE SHIPPING_MODE_SURR_ID = -1)::INTEGER,
               COUNT(*) FILTER (WHERE SHIPPING_MODE_SURR_ID != -1)::INTEGER
        FROM BL_DM.DIM_SHIPPING_MODES

        UNION ALL

        SELECT 'DIM_DELIVERY_STATUSES'::TEXT,
               COUNT(*)::INTEGER,
               COUNT(*) FILTER (WHERE DELIVERY_STATUS_SURR_ID = -1)::INTEGER,
               COUNT(*) FILTER (WHERE DELIVERY_STATUS_SURR_ID != -1)::INTEGER
        FROM BL_DM.DIM_DELIVERY_STATUSES
    )
    SELECT
        dc.table_name,
        dc.total_records,
        dc.default_records,
        dc.data_records,
        (dc.data_records = 0 AND dc.default_records >= 1) as is_clean
    FROM dimension_counts dc
    ORDER BY dc.table_name;
END $$;

-- =====================================================
-- SECTION 3: IDEMPOTENCY TESTING FRAMEWORK
-- =====================================================

-- Function to test idempotency of a specific procedure
CREATE OR REPLACE PROCEDURE BL_CL.test_procedure_idempotency(
    p_procedure_name VARCHAR(100),
    p_procedure_call TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_first_run_data_count INTEGER;
    v_second_run_data_count INTEGER;
    v_first_run_time TIMESTAMP;
    v_second_run_time TIMESTAMP;
    v_test_passed BOOLEAN;
    v_is_master_procedure BOOLEAN;
    rec RECORD;
BEGIN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'IDEMPOTENCY TEST: %', p_procedure_name;
    RAISE NOTICE '==========================================';

    -- Determine if this is a master procedure (calls other procedures)
    v_is_master_procedure := (p_procedure_name = 'load_all_dm_dimensions');

    -- Clear procedure logs for this test
    DELETE FROM BL_CL.mta_process_log
    WHERE procedure_name = p_procedure_name
    AND ta_insert_dt >= CURRENT_TIMESTAMP - INTERVAL '1 hour';

    -- Get baseline data count before any operations
    SELECT COUNT(*) INTO v_first_run_data_count
    FROM (
        SELECT 1 FROM BL_DM.DIM_SHIPPING_MODES WHERE SHIPPING_MODE_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_DELIVERY_STATUSES WHERE DELIVERY_STATUS_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_GEOGRAPHIES WHERE GEOGRAPHY_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_PRODUCTS_SCD WHERE PRODUCT_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_CUSTOMERS WHERE CUSTOMER_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_SALES_REPRESENTATIVES WHERE SALES_REP_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_WAREHOUSES WHERE WAREHOUSE_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_CARRIERS WHERE CARRIER_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_ORDER_STATUSES WHERE ORDER_STATUS_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_PAYMENT_METHODS WHERE PAYMENT_METHOD_SURR_ID != -1
    ) all_records;

    RAISE NOTICE 'Step 1: Executing first run of procedure...';
    RAISE NOTICE 'Initial data count: % records', v_first_run_data_count;
    v_first_run_time := CURRENT_TIMESTAMP;

    -- Execute the procedure (first time)
    EXECUTE p_procedure_call;

    RAISE NOTICE 'Step 2: Checking first run results...';

    -- Display first run log results
    RAISE NOTICE 'FIRST RUN LOG RESULTS:';
    FOR rec IN
        SELECT status, rows_affected, message, ta_insert_dt
        FROM BL_CL.mta_process_log
        WHERE (
            (v_is_master_procedure AND procedure_name = p_procedure_name) OR
            (NOT v_is_master_procedure AND procedure_name = p_procedure_name)
        )
        AND ta_insert_dt >= v_first_run_time
        ORDER BY ta_insert_dt
    LOOP
        RAISE NOTICE '  %: % records - % (at %)', rec.status, rec.rows_affected, rec.message, rec.ta_insert_dt;
    END LOOP;

    -- Get data count after first run
    SELECT COUNT(*) INTO v_second_run_data_count
    FROM (
        SELECT 1 FROM BL_DM.DIM_SHIPPING_MODES WHERE SHIPPING_MODE_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_DELIVERY_STATUSES WHERE DELIVERY_STATUS_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_GEOGRAPHIES WHERE GEOGRAPHY_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_PRODUCTS_SCD WHERE PRODUCT_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_CUSTOMERS WHERE CUSTOMER_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_SALES_REPRESENTATIVES WHERE SALES_REP_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_WAREHOUSES WHERE WAREHOUSE_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_CARRIERS WHERE CARRIER_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_ORDER_STATUSES WHERE ORDER_STATUS_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_PAYMENT_METHODS WHERE PAYMENT_METHOD_SURR_ID != -1
    ) all_records;

    RAISE NOTICE 'Data count after first run: % records', v_second_run_data_count;
    RAISE NOTICE 'Records changed by first run: %', (v_second_run_data_count - v_first_run_data_count);

    -- Store the count after first run for comparison
    v_first_run_data_count := v_second_run_data_count;

    -- Wait a moment to ensure timestamp differences
    PERFORM pg_sleep(1);

    RAISE NOTICE 'Step 3: Executing second run of procedure (should be idempotent)...';
    v_second_run_time := CURRENT_TIMESTAMP;

    -- Execute the procedure (second time)
    EXECUTE p_procedure_call;

    RAISE NOTICE 'Step 4: Checking second run results...';

    -- Get data count after second run
    SELECT COUNT(*) INTO v_second_run_data_count
    FROM (
        SELECT 1 FROM BL_DM.DIM_SHIPPING_MODES WHERE SHIPPING_MODE_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_DELIVERY_STATUSES WHERE DELIVERY_STATUS_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_GEOGRAPHIES WHERE GEOGRAPHY_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_PRODUCTS_SCD WHERE PRODUCT_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_CUSTOMERS WHERE CUSTOMER_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_SALES_REPRESENTATIVES WHERE SALES_REP_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_WAREHOUSES WHERE WAREHOUSE_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_CARRIERS WHERE CARRIER_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_ORDER_STATUSES WHERE ORDER_STATUS_SURR_ID != -1
        UNION ALL
        SELECT 1 FROM BL_DM.DIM_PAYMENT_METHODS WHERE PAYMENT_METHOD_SURR_ID != -1
    ) all_records;

    -- Display second run log results
    RAISE NOTICE 'SECOND RUN LOG RESULTS:';
    FOR rec IN
        SELECT status, rows_affected, message, ta_insert_dt
        FROM BL_CL.mta_process_log
        WHERE (
            (v_is_master_procedure AND procedure_name = p_procedure_name) OR
            (NOT v_is_master_procedure AND procedure_name = p_procedure_name)
        )
        AND ta_insert_dt >= v_second_run_time
        ORDER BY ta_insert_dt
    LOOP
        RAISE NOTICE '  %: % records - % (at %)', rec.status, rec.rows_affected, rec.message, rec.ta_insert_dt;
    END LOOP;

    -- Evaluate idempotency based on ACTUAL DATA CHANGES, not log entries
    v_test_passed := (v_second_run_data_count = v_first_run_data_count);

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'IDEMPOTENCY TEST RESULTS:';
    RAISE NOTICE 'Data count before second run: % records', v_first_run_data_count;
    RAISE NOTICE 'Data count after second run:  % records', v_second_run_data_count;
    RAISE NOTICE 'Actual data changes on second run: %', (v_second_run_data_count - v_first_run_data_count);
    RAISE NOTICE 'Test Status: %', CASE WHEN v_test_passed THEN 'PASSED ✓' ELSE 'FAILED ✗' END;

    IF NOT v_test_passed THEN
        RAISE NOTICE 'EXPLANATION: Procedure is NOT idempotent!';
        RAISE NOTICE 'Expected: 0 data records changed on second run';
        RAISE NOTICE 'Actual: % data records changed on second run', (v_second_run_data_count - v_first_run_data_count);
        RAISE NOTICE 'This indicates the procedure is making data changes when it should not.';

        IF v_is_master_procedure THEN
            RAISE NOTICE 'NOTE: For master procedures, we test actual data changes, not procedure call counts.';
        END IF;
    ELSE
        RAISE NOTICE 'EXPLANATION: Procedure IS idempotent ✓';
        RAISE NOTICE 'No data records were changed on the second run, which is expected behavior.';
        RAISE NOTICE 'This means the procedure correctly identifies that no changes are needed.';

        IF v_is_master_procedure THEN
            RAISE NOTICE 'NOTE: Master procedure may log sub-procedure calls, but no actual data was changed.';
        END IF;
    END IF;

    RAISE NOTICE '==========================================';
END $$;

