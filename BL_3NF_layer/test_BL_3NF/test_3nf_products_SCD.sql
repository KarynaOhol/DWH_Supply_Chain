--==================================================
    -- TEST SCD type 2 logic
--===================================================

   -- UPDATE PRODUCT NAME
--===================================================


-- See current active record for product 1014
SELECT 'BEFORE TEST - Current active record' as step,
       product_id, product_src_id, product_name, status_id, start_dt, end_dt, is_active
FROM BL_3NF.CE_PRODUCTS_SCD
WHERE product_src_id = '1014' AND is_active = 'Y';

-- Backup original value
SELECT 'Original data for product 1014:' as info,
       product_name, product_status
FROM SA_OMS.SRC_OMS
WHERE product_src_id = '1014'
  AND ta_insert_dt = (SELECT MAX(ta_insert_dt) FROM SA_OMS.SRC_OMS WHERE product_src_id = '1014');

-- Make a simple change to trigger SCD2
UPDATE SA_OMS.SRC_OMS
SET product_name = product_name || ' - UPDATED',
    ta_update_dt = CURRENT_TIMESTAMP
WHERE product_src_id = '1014'
  AND ta_insert_dt = (SELECT MAX(ta_insert_dt) FROM SA_OMS.SRC_OMS WHERE product_src_id = '1014')
  AND product_src_id = '1014';  -- Extra safety check

CALL BL_CL.load_ce_products_scd();

-- Check all records for product 1014
SELECT 'AFTER SCD LOAD - All records for 1014' as step,
       product_id, product_name, start_dt, end_dt, is_active,
       ta_insert_dt, ta_update_dt
FROM BL_3NF.CE_PRODUCTS_SCD
WHERE product_src_id = '1014'
ORDER BY start_dt, ta_insert_dt;

-- Verify total counts haven't changed unexpectedly
SELECT 'Count verification' as step,
       COUNT(*) as total_records,
       COUNT(CASE WHEN is_active = 'Y' THEN 1 END) as active_records
FROM BL_3NF.CE_PRODUCTS_SCD;

-- Remove the "- UPDATED" from all affected records
UPDATE SA_OMS.SRC_OMS
SET product_name = REPLACE(product_name, ' - UPDATED', ''),
    ta_update_dt = ta_insert_dt  -- Reset ta_update_dt to original value
WHERE product_name LIKE '% - UPDATED%';

-- Verify cleanup
SELECT COUNT(*) as remaining_updated_records
FROM SA_OMS.SRC_OMS
WHERE product_name LIKE '% - UPDATED%';


   -- INSERT TEST PRODUCT
--===================================================

-- Step 1: Insert a proper test product using all required columns
INSERT INTO SA_OMS.SRC_OMS (
    transaction_src_id, order_src_id, order_item_src_id, customer_src_id,
    customer_first_name, customer_last_name, customer_gender, customer_year_of_birth,
    customer_email, customer_segment, product_src_id, product_name, product_brand,
    product_status, product_category_src_id, product_category, department_src_id,
    department_name, sales_rep_src_id, sales_amount, quantity, order_total,
    unit_price, order_status, payment_method, source_system, order_dt,
    unit_cost, total_cost, order_year, order_month, order_quarter,
    order_day_of_week, order_week_of_year, ta_insert_dt, ta_update_dt
)
SELECT
    'TEST_' || transaction_src_id,
    'TEST_' || order_src_id,
    'TEST_' || order_item_src_id,
    customer_src_id, customer_first_name, customer_last_name, customer_gender, customer_year_of_birth,
    customer_email, customer_segment,
    '9999' as product_src_id,  -- Test product ID
    'Test SCD2 Product' as product_name,
    'Nike' as product_brand,  -- Use existing brand
    'Active' as product_status,  -- Use existing status
    product_category_src_id, product_category, department_src_id,
    department_name, sales_rep_src_id, sales_amount, quantity, order_total,
    unit_price, order_status, payment_method, source_system, order_dt,
    unit_cost, total_cost, order_year, order_month, order_quarter,
    order_day_of_week, order_week_of_year,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM SA_OMS.SRC_OMS
WHERE product_src_id = '1014'  -- Use existing product as template
LIMIT 1;

-- Step 2: Run complete dimension load
CALL BL_CL.load_all_dimensions();


-- Step 3: Check results - should be 120 products now
SELECT COUNT(*) as total_products FROM BL_3NF.CE_PRODUCTS_SCD;


-- Remove the test product from source data
DELETE FROM SA_OMS.SRC_OMS
WHERE product_src_id = '9999'
  AND transaction_src_id LIKE 'TEST_%';

-- Remove the test product from SCD table
DELETE FROM BL_3NF.CE_PRODUCTS_SCD
WHERE product_src_id = '9999';

-- Verify cleanup
SELECT 'Cleanup Verification' as step,
       COUNT(*) as total_scd_records
FROM BL_3NF.CE_PRODUCTS_SCD;


    --Verify id mapping is Idempotent
--==============================
CALL BL_CL.load_all_dimensions();

-- 2. Record current IDs
CREATE TEMP TABLE test_ids_before AS
SELECT 'brands' as table_name, brand_src_id as src_id, brand_id as surrogate_id
FROM BL_3NF.CE_BRANDS WHERE source_system = 'OMS'
UNION ALL
SELECT 'categories', category_src_id, category_id
FROM BL_3NF.CE_CATEGORIES WHERE source_system = 'OMS'
UNION ALL
SELECT 'statuses', status_src_id, status_id
FROM BL_3NF.CE_PRODUCT_STATUSES WHERE source_system = 'OMS';

select* from test_ids_before;

-- 3. Load dimensions second time
CALL BL_CL.load_all_dimensions();

-- 4. Check if IDs changed
CREATE TEMP TABLE test_ids_after AS
SELECT 'brands' as table_name, brand_src_id as src_id, brand_id as surrogate_id
FROM BL_3NF.CE_BRANDS WHERE source_system = 'OMS'
UNION ALL
SELECT 'categories', category_src_id, category_id
FROM BL_3NF.CE_CATEGORIES WHERE source_system = 'OMS'
UNION ALL
SELECT 'statuses', status_src_id, status_id
FROM BL_3NF.CE_PRODUCT_STATUSES WHERE source_system = 'OMS';
-- Compare with test_ids_before - should be identical!

SELECT
    before.table_name,
    before.src_id,
    before.surrogate_id as before_id,
    after.surrogate_id as after_id,
    (after.surrogate_id - before.surrogate_id) as id_difference
FROM test_ids_before before
JOIN test_ids_after after
    ON before.table_name = after.table_name
    AND before.src_id = after.src_id
WHERE before.surrogate_id != after.surrogate_id
ORDER BY before.table_name, before.src_id;


SELECT
    CASE
        WHEN EXISTS (
            SELECT table_name, src_id, surrogate_id FROM test_ids_before
            EXCEPT
            SELECT table_name, src_id, surrogate_id FROM test_ids_after
        ) OR EXISTS (
            SELECT table_name, src_id, surrogate_id FROM test_ids_after
            EXCEPT
            SELECT table_name, src_id, surrogate_id FROM test_ids_before
        ) THEN '❌ TABLES ARE DIFFERENT - IDs CHANGED!'
        ELSE '✅ TABLES ARE IDENTICAL - IDs STAYED THE SAME!'
    END as comparison_result;

-- 5. Clean up
DROP TABLE test_ids_before;
DROP TABLE test_ids_after;


