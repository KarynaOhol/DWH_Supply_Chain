-- =====================================================
-- SEPARATED TESTING PROCEDURES
-- Individual test procedures for each load mode
-- =====================================================

SET ROLE dwh_cleansing_user;
SET search_path = BL_CL, BL_3NF, BL_DM, public;

-- =====================================================
-- SECTION 1: SETUP AND CLEANUP PROCEDURES
-- =====================================================

-- Setup clean environment for testing
CREATE OR REPLACE PROCEDURE BL_CL.test_setup_clean_environment()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;  -- For FOR loop iterations
BEGIN
    RAISE NOTICE '===============================================================';
    RAISE NOTICE 'SETTING UP CLEAN TEST ENVIRONMENT';
    RAISE NOTICE '===============================================================';

    -- Clean all dimension tables
    CALL BL_CL.cleanup_all_dimension_tables();

    -- Verify clean state
    RAISE NOTICE 'VERIFICATION: Clean State Check';
    FOR rec IN SELECT * FROM BL_CL.verify_dimension_clean_state() LOOP
        RAISE NOTICE '%: Total=%, Default=%, Data=%, Clean=%',
                     rec.table_name, rec.total_records, rec.default_records,
                     rec.data_records, rec.is_clean;
    END LOOP;

    RAISE NOTICE 'Clean environment setup complete ✓';
END $$;

-- =====================================================
-- SECTION 2: FULL LOAD TESTING
-- =====================================================

-- Test Full Load Mode + Idempotency
CREATE OR REPLACE PROCEDURE BL_CL.test_full_load_mode()
LANGUAGE plpgsql
AS $$
DECLARE
    v_test_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_before_counts RECORD;
    v_after_counts RECORD;
    rec RECORD;  -- For FOR loop iterations
BEGIN
    RAISE NOTICE '===============================================================';
    RAISE NOTICE 'TESTING FULL LOAD MODE';
    RAISE NOTICE 'Test Start Time: %', v_test_start_time;
    RAISE NOTICE '===============================================================';

    -- Step 1: Setup clean environment
    CALL BL_CL.test_setup_clean_environment();

    -- Step 2: Get baseline counts
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 1: Recording baseline counts...';
    SELECT COUNT(*) as total_records INTO v_before_counts
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

    RAISE NOTICE 'Baseline: % total data records across all dimensions', v_before_counts.total_records;

    -- Step 3: Execute Full Load
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 2: Executing FULL LOAD...';
    CALL BL_CL.load_all_dimensions_full();

    -- Step 4: Check results
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 3: Checking full load results...';

    -- Get after counts
    SELECT COUNT(*) as total_records INTO v_after_counts
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

    RAISE NOTICE 'After Full Load: % total data records', v_after_counts.total_records;
    RAISE NOTICE 'Records Added: %', (v_after_counts.total_records - v_before_counts.total_records);

    -- Step 5: Test Idempotency
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 4: Testing FULL LOAD IDEMPOTENCY (using corrected logic)...';

    -- Use the detailed test for master procedures
    CALL BL_CL.test_master_procedure_idempotency_detailed(
        'load_all_dm_dimensions',
        'CALL BL_CL.load_all_dimensions_full()'
    );

    RAISE NOTICE '';
    RAISE NOTICE '===============================================================';
    RAISE NOTICE 'FULL LOAD MODE TEST COMPLETED';
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_test_start_time));
    RAISE NOTICE '===============================================================';
END $$;

-- Alternative version specifically for master procedures that checks individual sub-procedure results
CREATE OR REPLACE PROCEDURE BL_CL.test_master_procedure_idempotency_detailed(
    p_procedure_name VARCHAR(100),
    p_procedure_call TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_first_run_time TIMESTAMP;
    v_second_run_time TIMESTAMP;
    v_test_passed BOOLEAN := TRUE;
    v_first_run_data_count INTEGER;
    v_second_run_data_count INTEGER;
    v_individual_changes INTEGER;
    rec RECORD;
BEGIN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'DETAILED MASTER PROCEDURE IDEMPOTENCY TEST: %', p_procedure_name;
    RAISE NOTICE '==========================================';

    -- Clear procedure logs for this test
    DELETE FROM BL_CL.mta_process_log
    WHERE ta_insert_dt >= CURRENT_TIMESTAMP - INTERVAL '1 hour';

    -- Get baseline data count
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

    RAISE NOTICE 'Step 1: Executing first run of master procedure...';
    v_first_run_time := CURRENT_TIMESTAMP;
    EXECUTE p_procedure_call;

    RAISE NOTICE 'Step 2: Executing second run (should be idempotent)...';
    v_second_run_time := CURRENT_TIMESTAMP;
    EXECUTE p_procedure_call;

    -- Get final data count
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

    RAISE NOTICE 'Step 3: Analyzing individual sub-procedure results from second run...';

    -- Check individual procedure results from the second run only
    SELECT COALESCE(SUM(rows_affected), 0)
    INTO v_individual_changes
    FROM BL_CL.mta_process_log
    WHERE procedure_name != p_procedure_name  -- Individual procedures, not the master
    AND ta_insert_dt >= v_second_run_time
    AND status = 'SUCCESS'
    AND message NOT LIKE '%Completed %';  -- Exclude master procedure summary messages

    RAISE NOTICE 'INDIVIDUAL SUB-PROCEDURE RESULTS (Second Run Only):';
    FOR rec IN
        SELECT procedure_name, rows_affected, message, ta_insert_dt
        FROM BL_CL.mta_process_log
        WHERE procedure_name != p_procedure_name
        AND ta_insert_dt >= v_second_run_time
        AND status = 'SUCCESS'
        AND message NOT LIKE '%Completed %'
        ORDER BY ta_insert_dt
    LOOP
        RAISE NOTICE '  %: % records - %', rec.procedure_name, rec.rows_affected, rec.message;
    END LOOP;

    -- Test passes if no individual changes occurred AND total data count unchanged
    v_test_passed := (v_individual_changes = 0) AND (v_second_run_data_count = v_first_run_data_count);

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'DETAILED IDEMPOTENCY TEST RESULTS:';
    RAISE NOTICE 'Total data changes: %', (v_second_run_data_count - v_first_run_data_count);
    RAISE NOTICE 'Individual procedure changes (second run): %', v_individual_changes;
    RAISE NOTICE 'Test Status: %', CASE WHEN v_test_passed THEN 'PASSED ✓' ELSE 'FAILED ✗' END;

    IF NOT v_test_passed THEN
        RAISE NOTICE 'EXPLANATION: Master procedure is NOT idempotent!';
        RAISE NOTICE 'Individual sub-procedures made % changes on second run', v_individual_changes;
    ELSE
        RAISE NOTICE 'EXPLANATION: Master procedure IS idempotent ✓';
        RAISE NOTICE 'No individual sub-procedures made changes on second run.';
    END IF;

    RAISE NOTICE '==========================================';
END $$;

-- =====================================================
-- SECTION 3: DELTA LOAD TESTING
-- =====================================================

-- Test Delta Load Mode + Idempotency
CREATE OR REPLACE PROCEDURE BL_CL.test_delta_load_mode()
LANGUAGE plpgsql
AS $$
DECLARE
    v_test_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_baseline_time TIMESTAMP;
    rec RECORD;  -- For FOR loop iterations
BEGIN
    RAISE NOTICE '===============================================================';
    RAISE NOTICE 'TESTING DELTA LOAD MODE';
    RAISE NOTICE 'Test Start Time: %', v_test_start_time;
    RAISE NOTICE '===============================================================';

--     -- Step 1: Setup with full load first (delta needs baseline)
--     RAISE NOTICE '';
--     RAISE NOTICE 'STEP 1: Setting up baseline with full load...';
--     CALL BL_CL.test_setup_clean_environment();
--     CALL BL_CL.load_all_dimensions_full();
--     v_baseline_time := CURRENT_TIMESTAMP;
--
--     -- Wait a moment to ensure timestamp differences
--     PERFORM pg_sleep(1);

    -- Step 2: Create delta changes in 3NF
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 2: Creating delta test data in 3NF layer...';

    -- Clean up previous test data
DELETE FROM BL_3NF.CE_SHIPPING_MODES
WHERE shipping_mode_src_id = 'DRONE_EXPRESS' AND source_system = 'LMS';

DELETE FROM BL_3NF.CE_DELIVERY_STATUSES
WHERE delivery_status_src_id = 'WEATHER_HOLD' AND source_system = 'LMS';

    -- Add new shipping mode
    INSERT INTO BL_3NF.CE_SHIPPING_MODES (shipping_mode_src_id, shipping_mode, source_system, source_entity, ta_update_dt)
    VALUES ('DRONE_EXPRESS', 'Drone Express Delivery', 'LMS', 'SRC_LMS', CURRENT_TIMESTAMP);

    -- Add new delivery status
    INSERT INTO BL_3NF.CE_DELIVERY_STATUSES (delivery_status_src_id, delivery_status, source_system, source_entity, ta_update_dt)
    VALUES ('WEATHER_HOLD', 'Weather Hold', 'LMS', 'SRC_LMS', CURRENT_TIMESTAMP);

    -- Update existing delivery status
    UPDATE BL_3NF.CE_DELIVERY_STATUSES
    SET delivery_status = 'Delayed - Traffic Conditions',
        ta_update_dt = CURRENT_TIMESTAMP
    WHERE delivery_status_src_id = 'Delayed'
    AND delivery_status_id != -1;

    RAISE NOTICE 'Delta test data created: 1 new shipping mode, 1 new delivery status, 1 updated delivery status';

    -- Step 3: Execute Delta Load
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 3: Executing DELTA LOAD...';
    CALL BL_CL.load_all_dimensions_delta();

    -- Step 4: Verify delta results
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 4: Verifying delta load results...';

    -- Show recent logs
    FOR rec IN
        SELECT status, rows_affected, message, ta_insert_dt
        FROM BL_CL.mta_process_log
        WHERE procedure_name = 'load_all_dm_dimensions'
        AND ta_insert_dt >= v_baseline_time
        ORDER BY ta_insert_dt DESC
        LIMIT 5
    LOOP
        RAISE NOTICE 'LOG: % - % records - %', rec.status, rec.rows_affected, rec.message;
    END LOOP;

    -- Check if new records were loaded
    IF EXISTS (SELECT 1 FROM BL_DM.DIM_SHIPPING_MODES WHERE SHIPPING_MODE = 'Drone Express Delivery') THEN
        RAISE NOTICE '✓ New shipping mode successfully loaded';
    ELSE
        RAISE NOTICE '✗ New shipping mode NOT found in DM layer';
    END IF;

    IF EXISTS (SELECT 1 FROM BL_DM.DIM_DELIVERY_STATUSES WHERE DELIVERY_STATUS = 'Weather Hold') THEN
        RAISE NOTICE '✓ New delivery status successfully loaded';
    ELSE
        RAISE NOTICE '✗ New delivery status NOT found in DM layer';
    END IF;

    IF EXISTS (SELECT 1 FROM BL_DM.DIM_DELIVERY_STATUSES WHERE DELIVERY_STATUS = 'Delayed - Traffic Conditions') THEN
        RAISE NOTICE '✓ Updated delivery status successfully loaded';
    ELSE
        RAISE NOTICE '✗ Updated delivery status NOT found in DM layer';
    END IF;

    -- Step 5: Test Delta Load Idempotency
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 5: Testing DELTA LOAD IDEMPOTENCY...';
    CALL BL_CL.test_procedure_idempotency(
        'load_all_dm_dimensions',
        'CALL BL_CL.load_all_dimensions_delta()'
    );

    RAISE NOTICE '';
    RAISE NOTICE '===============================================================';
    RAISE NOTICE 'DELTA LOAD MODE TEST COMPLETED';
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_test_start_time));
    RAISE NOTICE '===============================================================';
END $$;

-- =====================================================
-- SECTION 4: SAFE FULL LOAD TESTING
-- =====================================================

-- Test Safe Full Load Mode (with rollback capability)
CREATE OR REPLACE PROCEDURE BL_CL.test_safe_full_load_mode()
LANGUAGE plpgsql
AS $$
DECLARE
    v_test_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    rec RECORD;  -- For FOR loop iterations
BEGIN
    RAISE NOTICE '===============================================================';
    RAISE NOTICE 'TESTING SAFE FULL LOAD MODE (with rollback protection)';
    RAISE NOTICE 'Test Start Time: %', v_test_start_time;
    RAISE NOTICE '===============================================================';

    -- Step 1: Setup clean environment
    CALL BL_CL.test_setup_clean_environment();

    -- Step 2: Execute Safe Full Load
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 1: Executing SAFE FULL LOAD...';
    RAISE NOTICE 'This mode will rollback ALL changes if ANY dimension fails';

    CALL BL_CL.load_all_dimensions_full_safe();

    -- Step 3: Check results
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 2: Checking safe full load results...';

    FOR rec IN
        SELECT status, rows_affected, message, ta_insert_dt
        FROM BL_CL.mta_process_log
        WHERE procedure_name = 'load_all_dm_dimensions'
        AND ta_insert_dt >= v_test_start_time
        ORDER BY ta_insert_dt DESC
        LIMIT 10
    LOOP
        RAISE NOTICE 'LOG: % - % records - %', rec.status, rec.rows_affected, rec.message;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '===============================================================';
    RAISE NOTICE 'SAFE FULL LOAD MODE TEST COMPLETED';
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_test_start_time));
    RAISE NOTICE '===============================================================';
END $$;

-- =====================================================
-- SECTION 5: SELECTIVE LOAD TESTING
-- =====================================================

-- Test Selective Load Mode
CREATE OR REPLACE PROCEDURE BL_CL.test_selective_load_mode()
LANGUAGE plpgsql
AS $$
DECLARE
    v_test_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    rec RECORD;  -- For FOR loop iterations
BEGIN
    RAISE NOTICE '===============================================================';
    RAISE NOTICE 'TESTING SELECTIVE LOAD MODE';
    RAISE NOTICE 'Test Start Time: %', v_test_start_time;
    RAISE NOTICE '===============================================================';

    -- Step 1: Setup clean environment
    CALL BL_CL.test_setup_clean_environment();

    -- Step 2: Load only specific dimensions
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 1: Loading only Shipping Modes and Delivery Statuses...';
    CALL BL_CL.load_dimensions_selective('Shipping Modes,Delivery Statuses');

    -- Step 3: Verify selective results
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 2: Verifying selective load results...';

    -- Check which dimensions have data
    FOR rec IN SELECT * FROM BL_CL.verify_dimension_clean_state() LOOP
        IF rec.data_records > 0 THEN
            RAISE NOTICE '✓ %: % data records loaded', rec.table_name, rec.data_records;
        ELSE
            RAISE NOTICE '○ %: No data records (expected for non-selected dimensions)', rec.table_name;
        END IF;
    END LOOP;

    -- Step 4: Test selective load idempotency
    RAISE NOTICE '';
    RAISE NOTICE 'STEP 3: Testing SELECTIVE LOAD IDEMPOTENCY...';
    CALL BL_CL.test_procedure_idempotency(
        'load_all_dm_dimensions',
        'CALL BL_CL.load_dimensions_selective(''Shipping Modes,Delivery Statuses'')'
    );

    RAISE NOTICE '';
    RAISE NOTICE '===============================================================';
    RAISE NOTICE 'SELECTIVE LOAD MODE TEST COMPLETED';
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_test_start_time));
    RAISE NOTICE '===============================================================';
END $$;

-- =====================================================
-- SECTION 6: INDIVIDUAL DIMENSION TESTING
-- =====================================================

-- Test individual dimension procedures
CREATE OR REPLACE PROCEDURE BL_CL.test_individual_dimensions()
LANGUAGE plpgsql
AS $$
DECLARE
    v_test_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    rec RECORD;  -- For FOR loop iterations (not used in this procedure, but for consistency)
BEGIN
    RAISE NOTICE '===============================================================';
    RAISE NOTICE 'TESTING INDIVIDUAL DIMENSION PROCEDURES';
    RAISE NOTICE 'Test Start Time: %', v_test_start_time;
    RAISE NOTICE '===============================================================';

    -- Test Shipping Modes individually
    RAISE NOTICE '';
    RAISE NOTICE 'Testing DIM_SHIPPING_MODES individually...';
    CALL BL_CL.test_procedure_idempotency(
        'load_dim_shipping_modes',
        'CALL BL_CL.load_dim_shipping_modes_full()'
    );

    -- Test Delivery Statuses individually
    RAISE NOTICE '';
    RAISE NOTICE 'Testing DIM_DELIVERY_STATUSES individually...';
    CALL BL_CL.test_procedure_idempotency(
        'load_dim_delivery_statuses',
        'CALL BL_CL.load_dim_delivery_statuses_full()'
    );

    RAISE NOTICE '';
    RAISE NOTICE '===============================================================';
    RAISE NOTICE 'INDIVIDUAL DIMENSION TESTING COMPLETED';
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_test_start_time));
    RAISE NOTICE '===============================================================';
END $$;

-- =====================================================
--  Run Individual Tests
-- =====================================================

-- 1. Test Full Load Mode:
CALL BL_CL.test_full_load_mode();

-- 2. Test Delta Load Mode:
CALL BL_CL.test_delta_load_mode();

-- 3. Test Safe Full Load Mode:
-- CALL BL_CL.test_safe_full_load_mode();

-- 4. Test Selective Load Mode:
-- CALL BL_CL.test_selective_load_mode();

-- 5. Test Individual Dimensions:
-- CALL BL_CL.test_individual_dimensions();

-- 6. Run All Tests (comprehensive):
-- CALL BL_CL.test_all_load_modes_comprehensive();

-- 7. Setup clean environment only:
-- CALL BL_CL.test_setup_clean_environment();

-- =====================================================