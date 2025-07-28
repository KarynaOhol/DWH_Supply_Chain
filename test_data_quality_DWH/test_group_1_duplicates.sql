-- Test Group 1: Duplicate Detection Tests
-- Based on your existing table structure

-- 1. Dimension Table Duplicate Tests (3NF Layer)
-- Test ce_customers for duplicates
SELECT 'ce_customers' as table_name,
       'DUPLICATES' as test_type,
       COUNT(*) as duplicate_count,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result
FROM (
    SELECT customer_src_id, COUNT(*) as cnt
    FROM BL_3NF.ce_customers
    WHERE customer_id != '-1'  -- Exclude default records
    GROUP BY customer_src_id
    HAVING COUNT(*) > 1
) duplicates

UNION ALL

-- Test ce_products_scd for duplicates (considering SCD nature)
SELECT 'ce_products_scd' as table_name,
       'DUPLICATES' as test_type,
       COUNT(*) as duplicate_count,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result
FROM (
    SELECT product_src_id, is_active, COUNT(*) as cnt
    FROM BL_3NF.ce_products_scd
    WHERE product_id != '-1'
    GROUP BY product_src_id, is_active
    HAVING COUNT(*) > 1
) duplicates

UNION ALL

-- Test ce_brands for duplicates
SELECT 'ce_brands' as table_name,
       'DUPLICATES' as test_type,
       COUNT(*) as duplicate_count,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result
FROM (
    SELECT brand_src_id, COUNT(*) as cnt
    FROM BL_3NF.ce_brands
    WHERE brand_id != '-1'
    GROUP BY brand_src_id
    HAVING COUNT(*) > 1
) duplicates

UNION ALL

-- Test ce_categories for duplicates
SELECT 'ce_categories' as table_name,
       'DUPLICATES' as test_type,
       COUNT(*) as duplicate_count,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result
FROM (
    SELECT category_src_id, COUNT(*) as cnt
    FROM BL_3NF.ce_categories
    WHERE category_id != '-1'
    GROUP BY category_src_id
    HAVING COUNT(*) > 1
) duplicates

UNION ALL

-- 2. Dimension Table Duplicate Tests (DM Layer)
-- Test DIM_CUSTOMERS for duplicates
SELECT 'DIM_CUSTOMERS' as table_name,
       'DUPLICATES' as test_type,
       COUNT(*) as duplicate_count,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result
FROM (
    SELECT customer_src_id, COUNT(*) as cnt
    FROM BL_DM.DIM_CUSTOMERS
    WHERE customer_surr_id != '-1'
    GROUP BY customer_src_id
    HAVING COUNT(*) > 1
) duplicates

UNION ALL

-- Test DIM_PRODUCTS_SCD for duplicates (active records only)
SELECT 'DIM_PRODUCTS_SCD' as table_name,
       'DUPLICATES' as test_type,
       COUNT(*) as duplicate_count,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result
FROM (
    SELECT product_src_id, COUNT(*) as cnt
    FROM BL_DM.DIM_PRODUCTS_SCD
    WHERE product_src_id != '-1' AND is_active = 'Y'
    GROUP BY product_src_id
    HAVING COUNT(*) > 1
) duplicates

UNION ALL

-- 3. Fact Table Duplicate Tests
-- Test ce_order_lines for duplicates (3NF)
SELECT 'ce_order_lines' as table_name,
       'DUPLICATES' as test_type,
       COUNT(*) as duplicate_count,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result
FROM (
    SELECT order_line_src_id, COUNT(*) as cnt
    FROM BL_3NF.ce_order_lines
    GROUP BY order_line_src_id
    HAVING COUNT(*) > 1
) duplicates

UNION ALL

-- Test fct_order_line_shipments_dd for duplicates (DM)
SELECT 'fct_order_line_shipments_dd' as table_name,
       'DUPLICATES' as test_type,
       COUNT(*) as duplicate_count,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result
FROM (
    SELECT order_line_src_id, COUNT(*) as cnt
    FROM BL_DM.fct_order_line_shipments_dd
    GROUP BY order_line_src_id
    HAVING COUNT(*) > 1
) duplicates;