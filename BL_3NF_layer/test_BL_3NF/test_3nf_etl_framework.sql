-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - ETL TESTING & MONITORING
-- Purpose: Test and monitor complete ETL framework
-- Run as: dwh_cleansing_user
-- =====================================================

SET ROLE dwh_cleansing_user;
-- Set search path
SET search_path = BL_CL, BL_3NF, SA_OMS, SA_LMS, public;

-- =====================================================
-- SECTION 1: PRE-LOAD VALIDATION
-- =====================================================

-- 1.1 Check staging data availability
\echo '=== STAGING DATA VALIDATION ==='
SELECT 'SA_OMS.SRC_OMS'                as source_table,
       COUNT(*)                        as total_records,
       COUNT(DISTINCT order_src_id)    as unique_orders,
       COUNT(DISTINCT customer_src_id) as unique_customers,
       COUNT(DISTINCT product_src_id)  as unique_products,
       MIN(order_dt)                   as earliest_date,
       MAX(order_dt)                   as latest_date
FROM SA_OMS.SRC_OMS
WHERE order_src_id IS NOT NULL

UNION ALL

SELECT 'SA_LMS.SRC_LMS',
       COUNT(*),
       COUNT(DISTINCT shipment_src_id),
       COUNT(DISTINCT customer_src_id),
       COUNT(DISTINCT product_src_id),
       MIN(order_dt),
       MAX(ship_dt)
FROM SA_LMS.SRC_LMS
WHERE shipment_src_id IS NOT NULL;

-- 1.2 Check data quality issues (your known problems)
\echo '=== DATA QUALITY ISSUES IDENTIFIED ==='
SELECT 'Customers with multiple genders' as issue_type,
       COUNT(DISTINCT customer_src_id)   as affected_records
FROM (SELECT customer_src_id, COUNT(DISTINCT customer_gender) as gender_count
      FROM SA_OMS.SRC_OMS
      WHERE customer_src_id IS NOT NULL
      GROUP BY customer_src_id
      HAVING COUNT(DISTINCT customer_gender) > 1) multi_gender

UNION ALL

SELECT 'Orders with missing customer data',
       COUNT(*)
FROM SA_OMS.SRC_OMS
WHERE customer_src_id IS NULL
   OR customer_src_id = ''

UNION ALL

SELECT 'Orders with missing product data',
       COUNT(*)
FROM SA_OMS.SRC_OMS
WHERE product_src_id IS NULL
   OR product_src_id = ''

UNION ALL

SELECT 'Shipments with missing geography',
       COUNT(*)
FROM SA_LMS.SRC_LMS
WHERE destination_city IS NULL
   OR destination_state IS NULL
   OR destination_country IS NULL;

-- 1.3 Verify 3NF tables are empty (for fresh load)
\echo '=== 3NF TABLE STATUS (Should be mostly empty for fresh load) ==='
SELECT 'CE_CUSTOMERS' as table_name,
       COUNT(*)       as total_records,
       COUNT(*) - 1   as business_records
FROM BL_3NF.CE_CUSTOMERS

UNION ALL
SELECT 'CE_PRODUCTS_SCD', COUNT(*), COUNT(*) - 1
FROM BL_3NF.CE_PRODUCTS_SCD
UNION ALL
SELECT 'CE_ORDERS', COUNT(*), COUNT(*)
FROM BL_3NF.CE_ORDERS
UNION ALL
SELECT 'CE_ORDER_LINES', COUNT(*), COUNT(*)
FROM BL_3NF.CE_ORDER_LINES
UNION ALL
SELECT 'CE_SHIPMENTS', COUNT(*), COUNT(*)
FROM BL_3NF.CE_SHIPMENTS
UNION ALL
SELECT 'CE_DELIVERIES', COUNT(*), COUNT(*)
FROM BL_3NF.CE_DELIVERIES

ORDER BY table_name;

-- -- =====================================================
-- -- SECTION 2: ETL EXECUTION TESTS
-- -- =====================================================
--
-- \echo '=== STARTING ETL TESTING SEQUENCE ==='
--
-- -- 2.1 Test logging framework first
-- \echo '--- Testing Logging Framework ---'
-- DO
-- $$
--     DECLARE
--         v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
--         v_execution_time INTEGER;
--     BEGIN
--         -- Fixed parameter types with explicit casts
--         CALL BL_CL.log_procedure_event(
--                 'test_framework'::VARCHAR(100), -- procedure_name
--                 'TEST'::VARCHAR(100), -- source_table
--                 'TEST'::VARCHAR(100), -- target_table
--                 'START'::VARCHAR(20), -- status
--                 0::INTEGER, -- rows_affected
--                 'Testing ETL framework'::TEXT, -- message
--                 0::INTEGER, -- execution_time_ms
--                 NULL::VARCHAR(50) -- error_code
--              );
--
--         PERFORM pg_sleep(1);
--
--         v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
--
--         CALL BL_CL.log_procedure_event(
--                 'test_framework'::VARCHAR(100),
--                 'TEST'::VARCHAR(100),
--                 'TEST'::VARCHAR(100),
--                 'SUCCESS'::VARCHAR(20),
--                 1::INTEGER,
--                 'Framework test completed'::TEXT,
--                 v_execution_time::INTEGER,
--                 NULL::VARCHAR(50)
--              );
--
--         RAISE NOTICE 'Logging framework test completed successfully';
--     END
-- $$;
--
-- -- 2.2 Test individual dimension procedure
-- \echo '--- Testing Individual Dimension Procedure ---'
-- -- Record start time for performance tracking
-- \set start_time `date +%s`
--
-- -- Test regions first (simplest)
-- CALL BL_CL.load_ce_regions();
--
-- -- Check results
-- SELECT 'load_ce_regions' as test_procedure,
--        status,
--        rows_affected,
--        execution_time_ms,
--        message
-- FROM BL_CL.MTA_PROCESS_LOG
-- WHERE procedure_name = 'load_ce_regions'
-- ORDER BY log_datetime DESC
-- LIMIT 1;
--
-- -- Verify data was loaded
-- SELECT 'CE_REGIONS' as table_name, COUNT(*) as loaded_records
-- FROM BL_3NF.CE_REGIONS
-- WHERE region_id != -1;

-- =====================================================
-- SECTION 3: FULL ETL EXECUTION
-- =====================================================

\echo '=== EXECUTING FULL ETL LOAD ==='
-- \echo 'This may take 20-35 minutes for 500K records...'

-- Record overall start time
SELECT CURRENT_TIMESTAMP as etl_start_time;

-- Execute complete ETL
CALL BL_CL.load_bl_3nf_full();


-- Record completion time
SELECT CURRENT_TIMESTAMP as etl_end_time;

-- =====================================================
-- SECTION 4: POST-LOAD VALIDATION
-- =====================================================

\echo '=== POST-LOAD VALIDATION AND RESULTS ==='

-- 4.1 Overall ETL execution status
\echo '--- ETL Execution Summary ---'
SELECT procedure_name,
       log_datetime,
       status,
       rows_affected,
       ROUND(execution_time_ms / 1000.0, 2) as execution_time_seconds,
       message
FROM BL_CL.MTA_PROCESS_LOG
WHERE procedure_name IN ('load_bl_3nf_full', 'load_all_dimensions', 'load_all_facts')
  AND log_datetime >= CURRENT_DATE
ORDER BY log_datetime DESC;

-- 4.2 Individual procedure success/failure summary
\echo '--- Individual Procedure Results ---'
SELECT procedure_name,
       status,
       rows_affected,
       ROUND(execution_time_ms / 1000.0, 2) as execution_time_seconds,
       message
FROM BL_CL.MTA_PROCESS_LOG
WHERE log_datetime >= CURRENT_DATE
  AND procedure_name LIKE 'load_ce_%'
  AND status != 'START'
ORDER BY CASE status
             WHEN 'ERROR' THEN 1
             WHEN 'WARNING' THEN 2
             WHEN 'SUCCESS' THEN 3
             END,
         procedure_name;

-- 4.3 Dimension loading results
\echo '--- Dimension Loading Results ---'
SELECT 'CE_REGIONS'                                          as dimension_table,
       COUNT(*)                                              as total_records,
       COUNT(*) - 1                                          as business_records,
       CASE WHEN COUNT(*) > 1 THEN 'LOADED' ELSE 'EMPTY' END as status
FROM BL_3NF.CE_REGIONS

UNION ALL
SELECT 'CE_COUNTRIES', COUNT(*), COUNT(*) - 1, CASE WHEN COUNT(*) > 1 THEN 'LOADED' ELSE 'EMPTY' END
FROM BL_3NF.CE_COUNTRIES
UNION ALL
SELECT 'CE_STATES', COUNT(*), COUNT(*) - 1, CASE WHEN COUNT(*) > 1 THEN 'LOADED' ELSE 'EMPTY' END
FROM BL_3NF.CE_STATES
UNION ALL
SELECT 'CE_CITIES', COUNT(*), COUNT(*) - 1, CASE WHEN COUNT(*) > 1 THEN 'LOADED' ELSE 'EMPTY' END
FROM BL_3NF.CE_CITIES
UNION ALL
SELECT 'CE_GEOGRAPHIES', COUNT(*), COUNT(*) - 1, CASE WHEN COUNT(*) > 1 THEN 'LOADED' ELSE 'EMPTY' END
FROM BL_3NF.CE_GEOGRAPHIES
UNION ALL
SELECT 'CE_DEPARTMENTS', COUNT(*), COUNT(*) - 1, CASE WHEN COUNT(*) > 1 THEN 'LOADED' ELSE 'EMPTY' END
FROM BL_3NF.CE_DEPARTMENTS
UNION ALL
SELECT 'CE_CATEGORIES', COUNT(*), COUNT(*) - 1, CASE WHEN COUNT(*) > 1 THEN 'LOADED' ELSE 'EMPTY' END
FROM BL_3NF.CE_CATEGORIES
UNION ALL
SELECT 'CE_BRANDS', COUNT(*), COUNT(*) - 1, CASE WHEN COUNT(*) > 1 THEN 'LOADED' ELSE 'EMPTY' END
FROM BL_3NF.CE_BRANDS
UNION ALL
SELECT 'CE_CUSTOMERS', COUNT(*), COUNT(*) - 1, CASE WHEN COUNT(*) > 1 THEN 'LOADED' ELSE 'EMPTY' END
FROM BL_3NF.CE_CUSTOMERS
UNION ALL
SELECT 'CE_PRODUCTS_SCD', COUNT(*), COUNT(*) - 1, CASE WHEN COUNT(*) > 1 THEN 'LOADED' ELSE 'EMPTY' END
FROM BL_3NF.CE_PRODUCTS_SCD

ORDER BY dimension_table;

-- 4.4 Fact loading results
\echo '--- Fact Loading Results ---'
SELECT 'CE_ORDERS'                                           as fact_table,
       COUNT(*)                                              as total_records,
       CASE WHEN COUNT(*) > 0 THEN 'LOADED' ELSE 'EMPTY' END as status
FROM BL_3NF.CE_ORDERS

UNION ALL
SELECT 'CE_ORDER_LINES', COUNT(*), CASE WHEN COUNT(*) > 0 THEN 'LOADED' ELSE 'EMPTY' END
FROM BL_3NF.CE_ORDER_LINES
UNION ALL
SELECT 'CE_TRANSACTIONS', COUNT(*), CASE WHEN COUNT(*) > 0 THEN 'LOADED' ELSE 'EMPTY' END
FROM BL_3NF.CE_TRANSACTIONS
UNION ALL
SELECT 'CE_SHIPMENTS', COUNT(*), CASE WHEN COUNT(*) > 0 THEN 'LOADED' ELSE 'EMPTY' END
FROM BL_3NF.CE_SHIPMENTS
UNION ALL
SELECT 'CE_SHIPMENT_LINES', COUNT(*), CASE WHEN COUNT(*) > 0 THEN 'LOADED' ELSE 'EMPTY' END
FROM BL_3NF.CE_SHIPMENT_LINES
UNION ALL
SELECT 'CE_DELIVERIES', COUNT(*), CASE WHEN COUNT(*) > 0 THEN 'LOADED' ELSE 'EMPTY' END
FROM BL_3NF.CE_DELIVERIES

ORDER BY fact_table;

-- 4.5 Data quality validation
\echo '--- Data Quality Validation ---'

-- Check for orphaned records
SELECT 'Orphaned Order Lines'                             as quality_check,
       COUNT(*)                                           as failed_records,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM BL_3NF.CE_ORDER_LINES ol
         LEFT JOIN BL_3NF.CE_ORDERS o ON o.order_id = ol.order_id
WHERE o.order_id IS NULL

UNION ALL

SELECT 'Orphaned Shipment Lines',
       COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM BL_3NF.CE_SHIPMENT_LINES sl
         LEFT JOIN BL_3NF.CE_SHIPMENTS s ON s.shipment_id = sl.shipment_id
WHERE s.shipment_id IS NULL

UNION ALL

SELECT 'Orders with default customer (-1)',
       COUNT(*),
       CASE WHEN COUNT(*) < (SELECT COUNT(*) * 0.05 FROM BL_3NF.CE_ORDERS) THEN 'PASS' ELSE 'WARN' END
FROM BL_3NF.CE_ORDERS
WHERE customer_id = -1

UNION ALL

SELECT 'Products with default category (-1)',
       COUNT(*),
       CASE WHEN COUNT(*) < (SELECT COUNT(*) * 0.05 FROM BL_3NF.CE_PRODUCTS_SCD) THEN 'PASS' ELSE 'WARN' END
FROM BL_3NF.CE_PRODUCTS_SCD
WHERE primary_category_id = -1
  AND product_id != -1;

-- 4.6 Business validation - key metrics
\echo '--- Business Metrics Validation ---'
SELECT 'Expected vs Actual Customer Count'                                      as metric,
       20469                                                                    as expected_value,
       COUNT(*) - 1                                                             as actual_value,
       CASE WHEN ABS((COUNT(*) - 1) - 20469) <= 100 THEN 'PASS' ELSE 'FAIL' END as status
FROM BL_3NF.CE_CUSTOMERS

UNION ALL

SELECT 'Expected vs Actual Product Count',
       118,
       COUNT(*) - 1,
       CASE WHEN ABS((COUNT(*) - 1) - 118) <= 10 THEN 'PASS' ELSE 'FAIL' END
FROM BL_3NF.CE_PRODUCTS_SCD

UNION ALL

SELECT 'Expected vs Actual Geography Count',
       593,
       COUNT(*) - 1,
       CASE WHEN ABS((COUNT(*) - 1) - 593) <= 50 THEN 'PASS' ELSE 'FAIL' END
FROM BL_3NF.CE_GEOGRAPHIES;

-- 4.7 Sample data verification
\echo '--- Sample Data Verification ---'
SELECT 'Sample Order with Details' as data_type,
       o.order_id,
       c.customer_first_name,
       c.customer_last_name,
       COUNT(ol.order_line_id)     as order_lines,
       o.order_total,
       o.order_date
FROM BL_3NF.CE_ORDERS o
         JOIN BL_3NF.CE_CUSTOMERS c ON c.customer_id = o.customer_id
         LEFT JOIN BL_3NF.CE_ORDER_LINES ol ON ol.order_id = o.order_id
WHERE c.customer_id != -1
GROUP BY o.order_id, c.customer_first_name, c.customer_last_name, o.order_total, o.order_date
ORDER BY o.order_id
LIMIT 3;

-- 4.8 Cross-system data relationships
\echo '--- Cross-System Relationship Validation ---'
SELECT 'Orders with Shipments'                                                                 as relationship_type,
       COUNT(DISTINCT o.order_id)                                                              as orders_count,
       COUNT(DISTINCT s.shipment_id)                                                           as shipments_count,
       ROUND(COUNT(DISTINCT s.shipment_id) * 100.0 / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) as fulfillment_rate_pct
FROM BL_3NF.CE_ORDERS o
         LEFT JOIN BL_3NF.CE_SHIPMENTS s ON s.order_id = o.order_id

UNION ALL

SELECT 'Shipments with Deliveries',
       COUNT(DISTINCT s.shipment_id),
       COUNT(DISTINCT d.delivery_id),
       ROUND(COUNT(DISTINCT d.delivery_id) * 100.0 / COUNT(DISTINCT s.shipment_id), 2)
FROM BL_3NF.CE_SHIPMENTS s
         LEFT JOIN BL_3NF.CE_DELIVERIES d ON d.shipment_id = s.shipment_id;

-- =====================================================
-- SECTION 5: PERFORMANCE ANALYSIS
-- =====================================================

\echo '=== PERFORMANCE ANALYSIS ==='

-- 5.1 Procedure execution performance
\echo '--- Procedure Performance Analysis ---'
SELECT CASE
           WHEN procedure_name LIKE 'load_ce_%' THEN 'Individual Procedures'
           WHEN procedure_name IN ('load_all_dimensions', 'load_all_facts') THEN 'Orchestration Procedures'
           WHEN procedure_name = 'load_bl_3nf_full' THEN 'Master ETL'
           ELSE 'Other'
           END                              as procedure_type,
       procedure_name,
       status,
       rows_affected,
       ROUND(execution_time_ms / 1000.0, 2) as execution_time_seconds,
       ROUND(CASE WHEN rows_affected > 0 THEN rows_affected / (execution_time_ms / 1000.0) ELSE 0 END,
             0)                             as rows_per_second
FROM BL_CL.MTA_PROCESS_LOG
WHERE log_datetime >= CURRENT_DATE
  AND status IN ('SUCCESS', 'WARNING')
  AND execution_time_ms > 0
ORDER BY execution_time_ms DESC;

-- 5.2 Overall ETL performance summary
\echo '--- Overall ETL Performance Summary ---'
WITH etl_summary AS (SELECT MIN(CASE WHEN status = 'START' THEN log_datetime END)                          as start_time,
                            MAX(CASE WHEN status IN ('SUCCESS', 'ERROR', 'WARNING') THEN log_datetime END) as end_time,
                            SUM(CASE WHEN status IN ('SUCCESS', 'WARNING') THEN rows_affected ELSE 0 END)  as total_rows_processed
                     FROM BL_CL.MTA_PROCESS_LOG
                     WHERE procedure_name = 'load_bl_3nf_full'
                       AND log_datetime >= CURRENT_DATE)
SELECT start_time,
       end_time,
       end_time - start_time                                                                   as total_duration,
       total_rows_processed,
       ROUND(total_rows_processed / NULLIF(EXTRACT(EPOCH FROM (end_time - start_time)), 0), 0) as avg_rows_per_second
FROM etl_summary;

-- =====================================================
-- SECTION 6: TEST REPEATABILITY (IDEMPOTENT CHECK)
-- =====================================================

\echo '=== TESTING REPEATABILITY (IDEMPOTENT BEHAVIOR) ==='
\echo 'Running load_bl_3nf_full() again - should process 0 new rows...'

-- Record counts before second run
CREATE TEMP TABLE pre_rerun_counts AS
SELECT 'CE_CUSTOMERS' as table_name,
       COUNT(*)       as record_count
FROM BL_3NF.CE_CUSTOMERS
UNION ALL
SELECT 'CE_ORDERS', COUNT(*)
FROM BL_3NF.CE_ORDERS
UNION ALL
SELECT 'CE_ORDER_LINES', COUNT(*)
FROM BL_3NF.CE_ORDER_LINES
UNION ALL
SELECT 'CE_SHIPMENTS', COUNT(*)
FROM BL_3NF.CE_SHIPMENTS;

-- Run ETL again
CALL BL_CL.load_bl_3nf_full();

-- Compare counts after second run
\echo '--- Repeatability Test Results ---'
SELECT pre.table_name,
       pre.record_count                     as before_rerun,
       post.record_count                    as after_rerun,
       post.record_count - pre.record_count as difference,
       CASE
           WHEN post.record_count = pre.record_count THEN 'PASS - Idempotent'
           ELSE 'FAIL - Not Idempotent'
           END                              as test_result
FROM pre_rerun_counts pre
         JOIN (SELECT 'CE_CUSTOMERS' as table_name, COUNT(*) as record_count
               FROM BL_3NF.CE_CUSTOMERS
               UNION ALL
               SELECT 'CE_ORDERS', COUNT(*)
               FROM BL_3NF.CE_ORDERS
               UNION ALL
               SELECT 'CE_ORDER_LINES', COUNT(*)
               FROM BL_3NF.CE_ORDER_LINES
               UNION ALL
               SELECT 'CE_SHIPMENTS', COUNT(*)
               FROM BL_3NF.CE_SHIPMENTS) post ON pre.table_name = post.table_name
ORDER BY pre.table_name;

-- Check second run processing results
SELECT 'Second ETL Run' as test_type,
       status,
       rows_affected,
       message
FROM BL_CL.MTA_PROCESS_LOG
WHERE procedure_name = 'load_bl_3nf_full'
ORDER BY log_datetime DESC
LIMIT 1;

-- =====================================================
-- SECTION 7: FINAL REPORT SUMMARY
-- =====================================================

\echo '=== FINAL ETL TEST REPORT ==='

-- 7.1 Overall test status
SELECT 'ETL Framework Test Results' as report_section,
       CASE
           WHEN EXISTS (SELECT 1 FROM BL_3NF.CE_CUSTOMERS WHERE customer_id != -1)
               AND EXISTS (SELECT 1 FROM BL_3NF.CE_ORDERS)
               AND EXISTS (SELECT 1 FROM BL_3NF.CE_ORDER_LINES)
               AND EXISTS (SELECT 1 FROM BL_3NF.CE_SHIPMENTS)
               THEN 'SUCCESS - ETL Framework Working'
           ELSE 'FAILURE - ETL Framework Issues'
           END                      as overall_status;

-- 7.2 Task requirements compliance
\echo '--- Task Requirements Compliance ---'
SELECT 'FOR LOOP Function Usage'   as requirement,
       CASE
           WHEN EXISTS (SELECT 1
                        FROM BL_CL.MTA_PROCESS_LOG
                        WHERE procedure_name = 'load_ce_geographies'
                          AND message LIKE '%FOR LOOP%'
                          AND status = 'SUCCESS') THEN 'COMPLIANT'
           ELSE 'NOT VERIFIED' END as compliance_status

UNION ALL

SELECT 'MERGE Approach Usage',
       CASE
           WHEN EXISTS (SELECT 1
                        FROM BL_CL.MTA_PROCESS_LOG
                        WHERE procedure_name IN ('load_ce_order_statuses', 'load_ce_product_statuses')
                          AND message LIKE '%MERGE%'
                          AND status = 'SUCCESS') THEN 'COMPLIANT'
           ELSE 'NOT VERIFIED' END

UNION ALL

SELECT 'Table-Returning Functions',
       CASE
           WHEN EXISTS (SELECT 1
                        FROM information_schema.routines
                        WHERE routine_name IN ('get_staging_geographies', 'get_staging_products')
                          AND routine_schema = 'bl_cl') THEN 'COMPLIANT'
           ELSE 'NOT VERIFIED' END

UNION ALL

SELECT 'Exception Blocks in Procedures',
       CASE
           WHEN (SELECT COUNT(*)
                 FROM BL_CL.MTA_PROCESS_LOG
                 WHERE log_datetime >= CURRENT_DATE
                   AND procedure_name LIKE 'load_ce_%'
                   AND status IN ('SUCCESS', 'WARNING', 'ERROR')) >= 20 THEN 'COMPLIANT'
           ELSE 'NOT VERIFIED' END

UNION ALL

SELECT 'Logging Functionality',
       CASE
           WHEN EXISTS (SELECT 1
                        FROM BL_CL.MTA_PROCESS_LOG
                        WHERE log_datetime >= CURRENT_DATE) THEN 'COMPLIANT'
           ELSE 'NOT VERIFIED' END;

-- -- 7.3 Ready for demo checklist
-- \echo '--- Demo Readiness Checklist ---'
-- SELECT 'Data Loaded (500K+ staging records processed)' as demo_requirement,
--        CASE
--            WHEN (SELECT COUNT(*) FROM BL_3NF.CE_ORDER_LINES) > 100000
--                THEN 'READY'
--            ELSE 'NEEDS ATTENTION' END                  as status
--
-- UNION ALL
--
-- SELECT 'Procedures Can Be Executed Repeatedly',
--        CASE
--            WHEN (SELECT COUNT(*)
--                  FROM BL_CL.MTA_PROCESS_LOG
--                  WHERE procedure_name = 'load_bl_3nf_full'
--                    AND log_datetime >= CURRENT_DATE) >= 2 THEN 'READY'
--            ELSE 'NEEDS TESTING' END
--
-- UNION ALL
--
-- SELECT 'Logging Shows Row Counts',
--        CASE
--            WHEN EXISTS (SELECT 1
--                         FROM BL_CL.MTA_PROCESS_LOG
--                         WHERE rows_affected > 0
--                           AND log_datetime >= CURRENT_DATE) THEN 'READY'
--            ELSE 'NEEDS ATTENTION' END
--
-- UNION ALL
--
-- SELECT 'No Critical Errors in ETL',
--        CASE
--            WHEN NOT EXISTS (SELECT 1
--                             FROM BL_CL.MTA_PROCESS_LOG
--                             WHERE status = 'ERROR'
--                               AND procedure_name = 'load_bl_3nf_full'
--                               AND log_datetime >= CURRENT_DATE) THEN 'READY'
--            ELSE 'NEEDS FIXING' END;

\echo '=== ETL FRAMEWORK TESTING COMPLETED ==='
\echo 'Review the results above to ensure everything is working correctly.'
\echo 'If any issues are found, check the MTA_PROCESS_LOG for detailed error messages.'

-- =====================================================
-- SECTION 8: TROUBLESHOOTING QUERIES
-- =====================================================

\echo '=== TROUBLESHOOTING REFERENCE QUERIES ==='
\echo 'Use these queries if you encounter issues:'
\echo ''
\echo '-- Check recent errors:'
\echo 'SELECT * FROM BL_CL.MTA_PROCESS_LOG WHERE status = ''ERROR'' ORDER BY log_datetime DESC LIMIT 10;'
\echo ''
\echo '-- Check procedure execution history:'
\echo 'SELECT * FROM BL_CL.VW_RECENT_EXECUTIONS WHERE log_datetime >= CURRENT_DATE;'
\echo ''
\echo '-- Check for long-running procedures:'
\echo 'SELECT procedure_name, execution_time_ms/1000 as seconds FROM BL_CL.MTA_PROCESS_LOG WHERE execution_time_ms > 60000 ORDER BY execution_time_ms DESC;'
\echo ''
\echo '-- Manual cleanup if needed:'
\echo 'CALL BL_CL._cleanup_fact_temp_tables();'