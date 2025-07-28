-- Initial setup (run once)
CALL BL_CL.setup_historical_partitions();

-- 3. Test the partition functionality
CALL BL_CL.test_partition_functionality();
-- =====================================================
-- FULL LOAD Testing
-- =====================================================
-- 4. Load your fact data
CALL BL_CL.load_fct_order_line_shipments_dd(FALSE);
-- Full load

-- 5. Check partition status
CALL BL_CL.show_partition_info();
SELECT *
FROM BL_CL.get_partition_counts();

-- =====================================================
-- INCREMENTAL LOAD Testing
-- =====================================================

-- STEP 1: Check current state before incremental load
SELECT COUNT(*)          as current_fact_records,
       MIN(ta_insert_dt) as oldest_record,
       MAX(ta_insert_dt) as newest_record
FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD;

-- Check last successful load timestamp
SELECT BL_CL.get_last_successful_load('load_fct_order_line_shipments_dd') as last_successful_load;

-- STEP 2: Test incremental load with no changes
-- This should process 0 records if nothing changed in source
CALL BL_CL.load_fct_order_line_shipments_dd(TRUE);

-- STEP 3: Verify what happened
-- Check if any new records were added
SELECT COUNT(*)                                                                           as total_records_after,
       COUNT(CASE
                 WHEN ta_insert_dt > (SELECT MAX(ta_insert_dt) - INTERVAL '5 minutes'
                                      FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD) THEN 1 END) as records_added_in_last_5_min
FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD;

-- STEP 4: Check what data the incremental logic would find
WITH incremental_scope AS (SELECT BL_CL.get_last_successful_load('load_fct_order_line_shipments_dd') as last_update_dt),
     changed_shipments AS (SELECT COUNT(*)            as shipments_changed,
                                  MIN(s.ta_update_dt) as earliest_change,
                                  MAX(s.ta_update_dt) as latest_change
                           FROM BL_3NF.CE_SHIPMENTS s
                                    CROSS JOIN incremental_scope i
                           WHERE s.ta_update_dt > i.last_update_dt),
     changed_order_lines AS (SELECT COUNT(*)             as order_lines_changed,
                                    MIN(ol.ta_update_dt) as earliest_change,
                                    MAX(ol.ta_update_dt) as latest_change
                             FROM BL_3NF.CE_ORDER_LINES ol
                                      CROSS JOIN incremental_scope i
                             WHERE ol.ta_update_dt > i.last_update_dt),
     changed_shipment_lines AS (SELECT COUNT(*)             as shipment_lines_changed,
                                       MIN(sl.ta_update_dt) as earliest_change,
                                       MAX(sl.ta_update_dt) as latest_change
                                FROM BL_3NF.CE_SHIPMENT_LINES sl
                                         CROSS JOIN incremental_scope i
                                WHERE sl.ta_update_dt > i.last_update_dt)
SELECT i.last_update_dt as incremental_since,
       cs.shipments_changed,
       col.order_lines_changed,
       csl.shipment_lines_changed,
       CASE
           WHEN cs.shipments_changed = 0 AND col.order_lines_changed = 0 AND csl.shipment_lines_changed = 0
               THEN 'NO CHANGES DETECTED - Should process 0 records'
           ELSE 'CHANGES DETECTED - Will process records'
           END          as expected_behavior
FROM incremental_scope i
         CROSS JOIN changed_shipments cs
         CROSS JOIN changed_order_lines col
         CROSS JOIN changed_shipment_lines csl;

-- STEP 5: SIMULATE CHANGES TO TEST INCREMENTAL BEHAVIOR
-- =====================================================

-- Test Scenario 1: Update a shipment to trigger incremental load

-- Update a single shipment
UPDATE BL_3NF.CE_SHIPMENTS
SET shipping_cost = shipping_cost + 1.00,
    ta_update_dt = CURRENT_TIMESTAMP
WHERE shipment_id = (
    SELECT shipment_id
    FROM BL_3NF.CE_SHIPMENTS
    LIMIT 1
);

-- Now run incremental load - should process records related to this shipment
CALL BL_CL.load_fct_order_line_shipments_dd(TRUE);

-- Check what happened
SELECT
    COUNT(*) as records_with_recent_updates,
    MAX(ta_update_dt) as most_recent_update
FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
WHERE ta_update_dt > CURRENT_TIMESTAMP - INTERVAL '5 minutes';


-- STEP 6: MONITOR INCREMENTAL LOAD PERFORMANCE
-- =====================================================

-- Check procedure execution log
SELECT ta_insert_dt,
       procedure_name,
       status,
       rows_affected,
       message,
       execution_time_ms
FROM BL_CL.mta_process_log
WHERE procedure_name = 'load_fct_order_line_shipments_dd'
ORDER BY ta_insert_dt DESC
LIMIT 10;

-- Check what gets deleted in incremental load
-- This shows the delta deletion logic
WITH last_load AS (SELECT BL_CL.get_last_successful_load('load_fct_order_line_shipments_dd') as last_dt),
     shipments_to_reload AS (SELECT DISTINCT s.SHIPMENT_ID, ol.ORDER_LINE_ID
                             FROM BL_3NF.CE_SHIPMENTS s
                                      JOIN BL_3NF.CE_SHIPMENT_LINES sl ON s.SHIPMENT_ID = sl.SHIPMENT_ID
                                      JOIN BL_3NF.CE_ORDER_LINES ol ON sl.ORDER_LINE_ID = ol.ORDER_LINE_ID
                                      CROSS JOIN last_load l
                             WHERE s.TA_UPDATE_DT > l.last_dt
                                OR ol.TA_UPDATE_DT > l.last_dt
                                OR sl.TA_UPDATE_DT > l.last_dt)
SELECT COUNT(*)      as fact_records_that_would_be_deleted,
       MIN(event_dt) as earliest_affected_date,
       MAX(event_dt) as latest_affected_date
FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD f
WHERE (f.SHIPMENT_SRC_ID, f.ORDER_LINE_SRC_ID) IN (SELECT SHIPMENT_ID, ORDER_LINE_ID
                                                   FROM shipments_to_reload);


-- =====================================================
-- ROLLING WINDOW MANAGEMENT DEMONSTRATION
-- =====================================================

-- =====================================================
-- STEP 1: UNDERSTAND CURRENT PARTITION STATE
-- =====================================================

-- Check all current partitions and their attachment status
SELECT
    'CURRENT PARTITIONS' as demo_step,
    child.relname as partition_name,
    CASE
        WHEN parent.relname IS NOT NULL THEN 'ATTACHED'
        ELSE 'DETACHED/STANDALONE'
    END as status,
    TO_DATE(RIGHT(child.relname, 6), 'YYYYMM') as partition_month,
    pg_size_pretty(pg_total_relation_size('bl_dm.' || child.relname)) as size
FROM pg_class child
LEFT JOIN pg_inherits i ON child.oid = i.inhrelid
LEFT JOIN pg_class parent ON i.inhparent = parent.oid AND parent.relname = 'fct_order_line_shipments_dd'
JOIN pg_namespace n ON child.relnamespace = n.oid
WHERE n.nspname = 'bl_dm'
AND child.relname LIKE 'fct_order_line_shipments_dd_%'
AND child.relname ~ 'fct_order_line_shipments_dd_[0-9]{6}$'
ORDER BY partition_month;

-- =====================================================
-- STEP 2: ANALYZE ROLLING WINDOW LOGIC
-- =====================================================

-- Show what the rolling window logic considers
WITH rolling_window_analysis AS (
    SELECT
        DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months') as cutoff_date_3_months,
        DATE_TRUNC('month', CURRENT_DATE - INTERVAL '6 months') as cutoff_date_6_months,
        DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months') as cutoff_date_12_months
),
partition_analysis AS (
    SELECT
        tablename,
        TO_DATE(RIGHT(tablename, 6), 'YYYYMM') as partition_date,
        CASE
            WHEN TO_DATE(RIGHT(tablename, 6), 'YYYYMM') >= '2025-01-01' THEN 'MANAGED_BY_ROLLING_WINDOW'
            ELSE 'HISTORICAL_PRESERVED'
        END as window_policy,
        CASE
            WHEN TO_DATE(RIGHT(tablename, 6), 'YYYYMM') < (SELECT cutoff_date_3_months FROM rolling_window_analysis)
                 AND TO_DATE(RIGHT(tablename, 6), 'YYYYMM') >= '2025-01-01'
            THEN 'CANDIDATE_FOR_DETACHMENT'
            ELSE 'KEEP_ATTACHED'
        END as rolling_window_action
    FROM pg_tables
    WHERE schemaname = 'bl_dm'
    AND tablename LIKE 'fct_order_line_shipments_dd_%'
    AND tablename ~ 'fct_order_line_shipments_dd_[0-9]{6}$'
)
SELECT
    'ROLLING WINDOW ANALYSIS' as demo_step,
    rwa.*,
    pa.tablename,
    pa.partition_date,
    pa.window_policy,
    pa.rolling_window_action
FROM rolling_window_analysis rwa
CROSS JOIN partition_analysis pa
ORDER BY pa.partition_date;

-- =====================================================
-- STEP 3: TEST ROLLING WINDOW MANUALLY
-- =====================================================

-- Call the rolling window procedure directly to see what it does
-- CALL BL_CL.manage_rolling_window(3);

-- Check the procedure log for rolling window activity
SELECT
    'ROLLING WINDOW LOG' as demo_step,
    event_timestamp,
    procedure_name,
    event_type,
    affected_rows,
    message,
    execution_time_ms
FROM BL_CL.procedure_log
WHERE procedure_name = 'manage_rolling_window'
ORDER BY event_timestamp DESC
LIMIT 5;

-- =====================================================
-- STEP 4: DEMONSTRATE DIFFERENT ROLLING WINDOW SETTINGS
-- =====================================================

-- Test with different month retention settings
-- WARNING: These will actually detach partitions!

-- Option A: Very aggressive (1 month retention) - FOR DEMO ONLY
-- CALL BL_CL.manage_rolling_window(1);

-- Option B: Conservative (6 months retention)
-- CALL BL_CL.manage_rolling_window(6);

-- Option C: Default (3 months retention)
-- CALL BL_CL.manage_rolling_window(3);

-- =====================================================
-- STEP 5: CREATE FUTURE PARTITIONS TO DEMONSTRATE
-- =====================================================

-- Create some future partitions to make rolling window more visible
-- Create partition for next month
DO $$
DECLARE
    v_next_month DATE := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month');
    v_partition_name TEXT;
BEGIN
    v_partition_name := BL_CL.get_partition_name('FCT_ORDER_LINE_SHIPMENTS_DD', v_next_month);

    IF NOT BL_CL.partition_exists(v_partition_name) THEN
        CALL BL_CL.create_fact_partition(v_next_month, TRUE);
        RAISE NOTICE 'Created future partition: %', v_partition_name;
    ELSE
        RAISE NOTICE 'Future partition already exists: %', v_partition_name;
    END IF;
END $$;

-- Create partition for 2 months ahead
DO $$
DECLARE
    v_future_month DATE := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '2 months');
    v_partition_name TEXT;
BEGIN
    v_partition_name := BL_CL.get_partition_name('FCT_ORDER_LINE_SHIPMENTS_DD', v_future_month);

    IF NOT BL_CL.partition_exists(v_partition_name) THEN
        CALL BL_CL.create_fact_partition(v_future_month, TRUE);
        RAISE NOTICE 'Created future partition: %', v_partition_name;
    ELSE
        RAISE NOTICE 'Future partition already exists: %', v_partition_name;
    END IF;
END $$;

-- =====================================================
-- STEP 6: COMPREHENSIVE ROLLING WINDOW TEST
-- =====================================================

-- Now test rolling window with more partitions present
SELECT
    'PRE-ROLLING-WINDOW STATE' as demo_step,
    COUNT(*) as total_partitions,
    COUNT(CASE WHEN TO_DATE(RIGHT(tablename, 6), 'YYYYMM') >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months') THEN 1 END) as within_3_months,
    COUNT(CASE WHEN TO_DATE(RIGHT(tablename, 6), 'YYYYMM') < DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months')
                AND TO_DATE(RIGHT(tablename, 6), 'YYYYMM') >= '2025-01-01' THEN 1 END) as candidates_for_detachment,
    COUNT(CASE WHEN TO_DATE(RIGHT(tablename, 6), 'YYYYMM') < '2025-01-01' THEN 1 END) as historical_protected
FROM pg_tables
WHERE schemaname = 'bl_dm'
AND tablename LIKE 'fct_order_line_shipments_dd_%'
AND tablename ~ 'fct_order_line_shipments_dd_[0-9]{6}$';

-- Run incremental load to trigger rolling window
CALL BL_CL.load_fct_order_line_shipments_dd(TRUE);


-- Test with aggressive 1-month retention to see immediate effect
CALL BL_CL.manage_rolling_window(1);

-- Check what happened
SELECT
    ta_insert_dt,
    rows_affected as partitions_detached,
    message
FROM BL_CL.mta_process_log
WHERE procedure_name = 'manage_rolling_window'
ORDER BY ta_insert_dt DESC
LIMIT 3;

-- Check what changed
SELECT
    'POST-ROLLING-WINDOW STATE' as demo_step,
    COUNT(*) as total_partitions,
    COUNT(CASE WHEN TO_DATE(RIGHT(tablename, 6), 'YYYYMM') >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months') THEN 1 END) as within_3_months,
    COUNT(CASE WHEN TO_DATE(RIGHT(tablename, 6), 'YYYYMM') < DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months')
                AND TO_DATE(RIGHT(tablename, 6), 'YYYYMM') >= '2025-01-01' THEN 1 END) as candidates_for_detachment,
    COUNT(CASE WHEN TO_DATE(RIGHT(tablename, 6), 'YYYYMM') < '2025-01-01' THEN 1 END) as historical_protected
FROM pg_tables
WHERE schemaname = 'bl_dm'
AND tablename LIKE 'fct_order_line_shipments_dd_%'
AND tablename ~ 'fct_order_line_shipments_dd_[0-9]{6}$';

-- =====================================================
-- STEP 7: VERIFY ROLLING WINDOW BEHAVIOR
-- =====================================================

-- Step 1: Simple partition list
SELECT
    tablename,
    RIGHT(tablename, 6) as year_month,
    CASE
        WHEN RIGHT(tablename, 6) LIKE '2023%' THEN 'HISTORICAL_2023'
        WHEN RIGHT(tablename, 6) LIKE '2024%' THEN 'HISTORICAL_2024'
        WHEN RIGHT(tablename, 6) LIKE '2025%' THEN 'CURRENT_2025'
        ELSE 'OTHER'
    END as category
FROM pg_tables
WHERE schemaname = 'bl_dm'
AND tablename LIKE 'FCT_ORDER_LINE_SHIPMENTS_DD_%'
ORDER BY tablename;

-- Step 2: Count by category
SELECT
    CASE
        WHEN RIGHT(tablename, 6) LIKE '2023%' THEN 'HISTORICAL_2023'
        WHEN RIGHT(tablename, 6) LIKE '2024%' THEN 'HISTORICAL_2024'
        WHEN RIGHT(tablename, 6) LIKE '2025%' THEN 'CURRENT_2025'
        ELSE 'OTHER'
    END as category,
    COUNT(*) as partition_count
FROM pg_tables
WHERE schemaname = 'bl_dm'
AND tablename LIKE 'FCT_ORDER_LINE_SHIPMENTS_DD_%'
GROUP BY 1
ORDER BY 1;

-- Step 3: Check which partitions are currently attached to the main table
SELECT
    child.relname as partition_name,
    'ATTACHED' as status
FROM pg_inherits i
JOIN pg_class parent ON i.inhparent = parent.oid
JOIN pg_class child ON i.inhrelid = child.oid
JOIN pg_namespace pn ON parent.relnamespace = pn.oid
JOIN pg_namespace cn ON child.relnamespace = cn.oid
WHERE pn.nspname = 'bl_dm'
AND parent.relname = 'fct_order_line_shipments_dd'
AND cn.nspname = 'bl_dm'
ORDER BY child.relname;

-- Step 4: Show 2025 partitions that could be affected by rolling window
SELECT
    tablename,
    RIGHT(tablename, 6) as year_month,
    CASE
        WHEN RIGHT(tablename, 6) = '202507' THEN 'CURRENT_MONTH (Keep)'
        WHEN RIGHT(tablename, 6) > '202504' THEN 'RECENT (Keep with 3-month window)'
        WHEN RIGHT(tablename, 6) <= '202504' THEN 'OLD_2025 (Candidate for detachment)'
        ELSE 'OTHER'
    END as rolling_window_status
FROM pg_tables
WHERE schemaname = 'bl_dm'
AND tablename LIKE 'FCT_ORDER_LINE_SHIPMENTS_DD_2025%'
ORDER BY tablename;

-- Step 5: Test rolling window with 1-month retention
-- This should detach 202501-202506 (Jan-June 2025) but keep 202507 (July)
CALL BL_CL.manage_rolling_window(1);

-- Step 6: After calling manage_rolling_window(1), check procedure log
SELECT
    ta_insert_dt,
    rows_affected,
    message,
    execution_time_ms
FROM BL_CL.mta_process_log
WHERE procedure_name = 'manage_rolling_window'
AND ta_insert_dt > CURRENT_TIMESTAMP - INTERVAL '5 minutes'
ORDER BY ta_insert_dt DESC;

-- Step 7: After rolling window, check which partitions are still attached
-- (This will show the difference if any partitions were detached)
SELECT
    'AFTER_ROLLING_WINDOW' as check_point,
    child.relname as partition_name,
    RIGHT(child.relname, 6) as year_month
FROM pg_inherits i
JOIN pg_class parent ON i.inhparent = parent.oid
JOIN pg_class child ON i.inhrelid = child.oid
JOIN pg_namespace pn ON parent.relnamespace = pn.oid
JOIN pg_namespace cn ON child.relnamespace = cn.oid
WHERE pn.nspname = 'bl_dm'
AND parent.relname = 'fct_order_line_shipments_dd'
AND cn.nspname = 'bl_dm'
AND child.relname LIKE '%2025%'  -- Focus on 2025 partitions
ORDER BY child.relname;

-- Step 8: Check if any partitions exist as standalone tables (detached)
SELECT
    'STANDALONE_PARTITIONS' as status,
    tablename,
    'Exists as table but not attached to parent' as note
FROM pg_tables t
WHERE schemaname = 'bl_dm'
AND tablename LIKE 'FCT_ORDER_LINE_SHIPMENTS_DD_2025%'
AND NOT EXISTS (
    SELECT 1
    FROM pg_inherits i
    JOIN pg_class parent ON i.inhparent = parent.oid
    JOIN pg_class child ON i.inhrelid = child.oid
    WHERE parent.relname = 'fct_order_line_shipments_dd'
    AND child.relname = t.tablename
);

-- =====================================================
-- REATTACH DETACHED PARTITIONS
-- =====================================================

-- Reattach the partitions (use correct case and full names)
ALTER TABLE bl_dm.fct_order_line_shipments_dd
ATTACH PARTITION bl_dm."FCT_ORDER_LINE_SHIPMENTS_DD_202501"
FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

ALTER TABLE bl_dm.fct_order_line_shipments_dd
ATTACH PARTITION bl_dm."FCT_ORDER_LINE_SHIPMENTS_DD_202502"
FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

ALTER TABLE bl_dm.fct_order_line_shipments_dd
ATTACH PARTITION bl_dm."FCT_ORDER_LINE_SHIPMENTS_DD_202503"
FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');