-- =====================================================
-- COMPREHENSIVE SCD2 TESTING AND VERIFICATION FULL LOAD
-- =====================================================

-- STEP 1: call update on existing product
UPDATE SA_OMS.SRC_OMS
SET product_name = product_name || ' - UPDATED',
    ta_update_dt = CURRENT_TIMESTAMP
WHERE product_src_id = '1014'
  AND ta_insert_dt = (SELECT MAX(ta_insert_dt) FROM SA_OMS.SRC_OMS WHERE product_src_id = '1014')
  AND product_src_id = '1014';

-- STEP 2: Process through 3NF layer
CALL BL_CL.load_bl_3nf_full();

-- STEP 3: Verify 3NF shows the change
SELECT product_src_id, product_name, start_dt, end_dt, is_active, ta_update_dt
FROM BL_3NF.CE_PRODUCTS_SCD
WHERE product_src_id = '1014'
ORDER BY start_dt, ta_insert_dt;

-- STEP 4: Test Full load
CALL BL_CL.load_dim_products_scd_full();

-- STEP 5: Verify SCD2 versioning in DM layer
SELECT product_surr_id,
       product_src_id,
       product_name,
       start_dt,
       end_dt,
       is_active,
       ta_insert_dt,
       ta_update_dt,
       ROW_NUMBER() OVER (PARTITION BY product_src_id ORDER BY start_dt) as version_number
FROM BL_DM.DIM_PRODUCTS_SCD
WHERE product_src_id = '1014'
ORDER BY start_dt;


-- =====================================================
-- COMPREHENSIVE SCD2 TESTING AND VERIFICATION DELTA LOAD
-- =====================================================

-- STEP 1: Test another change to verify delta load works
UPDATE SA_OMS.SRC_OMS
SET product_name = 'O''Brien Men''s Neoprene Life Vest - UPDATED AGAIN',
    ta_update_dt = CURRENT_TIMESTAMP
WHERE product_src_id = '1014'
  AND ta_insert_dt = (SELECT MAX(ta_insert_dt) FROM SA_OMS.SRC_OMS WHERE product_src_id = '1014');

-- STEP 2: Process through 3NF layer
CALL BL_CL.load_bl_3nf_full();

-- STEP 3: Verify 3NF shows the change
SELECT product_src_id, product_name, start_dt, end_dt, is_active, ta_update_dt
FROM BL_3NF.CE_PRODUCTS_SCD
WHERE product_src_id = '1014'
ORDER BY start_dt, ta_insert_dt;

-- STEP 4: Test DELTA load
CALL BL_CL.load_dim_products_scd_delta();

-- STEP 5: Verify SCD2 versioning in DM layer
SELECT product_surr_id,
       product_name,
       start_dt,
       end_dt,
       is_active,
       ta_insert_dt,
       ta_update_dt,
       ROW_NUMBER() OVER (PARTITION BY product_src_id ORDER BY start_dt) as version_number
FROM BL_DM.DIM_PRODUCTS_SCD
WHERE product_src_id = '1014'
ORDER BY start_dt;

-- STEP 6: Test with a different product to ensure it works broadly
-- Let's test with product 44 adidas Men's F10 Messi TRX FG Soccer Cleat
UPDATE SA_OMS.SRC_OMS
SET product_name = product_name || ' - DELTA TEST',
    ta_update_dt = CURRENT_TIMESTAMP
WHERE product_src_id = '44'
  AND ta_insert_dt = (SELECT MAX(ta_insert_dt) FROM SA_OMS.SRC_OMS WHERE product_src_id = '44');

-- Process through layers
CALL BL_CL.load_bl_3nf_full();

-- Verify 3NF shows the change
SELECT product_src_id, product_name, start_dt, end_dt, is_active, ta_update_dt
FROM BL_3NF.CE_PRODUCTS_SCD
WHERE product_src_id IN ('1014', '44')
ORDER BY start_dt, ta_insert_dt;

CALL BL_CL.load_dim_products_scd_delta();

-- Verify both products have proper SCD2 versioning
SELECT product_src_id,
       product_name,
       start_dt,
       end_dt,
       is_active,
       'Version ' || ROW_NUMBER() OVER (PARTITION BY product_src_id ORDER BY start_dt) as version
FROM BL_DM.DIM_PRODUCTS_SCD
WHERE product_src_id IN ('1014', '44')
ORDER BY product_src_id, start_dt;

-- STEP 7: Performance and logging verification
SELECT procedure_name,
       log_datetime,
       status,
       rows_affected,
       execution_time_ms,
       message
FROM BL_CL.mta_process_log
WHERE procedure_name = 'load_dim_products_scd'
  AND ta_insert_dt >= CURRENT_DATE
ORDER BY ta_insert_dt DESC
LIMIT 10;

-- STEP 8: Verify cursor variable functionality worked correctly
-- Check that we have proper change detection in the logs
SELECT message
FROM BL_CL.mta_process_log
WHERE procedure_name = 'load_dim_products_scd'
  AND message LIKE '%SCD2 change detected%'
  AND ta_insert_dt >= CURRENT_DATE;

-- STEP 9: Summary statistics for SCD2 implementation
SELECT 'Total Products'                     as metric,
       COUNT(DISTINCT product_src_id)::TEXT as value
FROM BL_DM.DIM_PRODUCTS_SCD
WHERE source_system = '3NF_LAYER'
  AND product_surr_id != -1

UNION ALL

SELECT 'Products with Multiple Versions' as metric,
       COUNT(*)::TEXT                    as value
FROM (SELECT product_src_id
      FROM BL_DM.DIM_PRODUCTS_SCD
      WHERE source_system = '3NF_LAYER'
        AND product_surr_id != -1
      GROUP BY product_src_id
      HAVING COUNT(*) > 1) multi_version

UNION ALL

SELECT 'Active Products' as metric,
       COUNT(*)::TEXT    as value
FROM BL_DM.DIM_PRODUCTS_SCD
WHERE source_system = '3NF_LAYER'
  AND product_surr_id != -1
  AND is_active = 'Y'

UNION ALL

SELECT 'Historical Products' as metric,
       COUNT(*)::TEXT        as value
FROM BL_DM.DIM_PRODUCTS_SCD
WHERE source_system = '3NF_LAYER'
  AND product_surr_id != -1
  AND is_active = 'N';

-- STEP 10: Test edge cases
-- Test what happens when we try to load the same data again (should be no changes)
CALL BL_CL.load_dim_products_scd_delta();

-- Verify no new records were created
SELECT COUNT(*)                                    as total_records,
       COUNT(CASE WHEN is_active = 'Y' THEN 1 END) as active_records,
       COUNT(CASE WHEN is_active = 'N' THEN 1 END) as historical_records
FROM BL_DM.DIM_PRODUCTS_SCD
WHERE product_src_id = '1014';

