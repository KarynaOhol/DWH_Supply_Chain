-- Test Group 2: Completeness Tests (SA → 3NF → DM)
-- Based on your existing layer comparison logic

WITH completeness_tests AS (
    -- Test 1: Customers completeness SA → 3NF
    SELECT 'customers_sa_to_3nf' as test_name,
           'COMPLETENESS' as test_type,
           (SELECT COUNT(DISTINCT customer_src_id)
            FROM (SELECT customer_src_id FROM sa_lms.src_lms WHERE customer_src_id IS NOT NULL
                  UNION
                  SELECT customer_src_id FROM sa_oms.src_oms WHERE customer_src_id IS NOT NULL) combined) as sa_count,
           (SELECT COUNT(*) FROM BL_3NF.ce_customers WHERE customer_id != '-1') as target_count,
           'SA_to_3NF' as test_layer

    UNION ALL

    -- Test 2: Products completeness SA → 3NF
    SELECT 'products_sa_to_3nf' as test_name,
           'COMPLETENESS' as test_type,
           (SELECT COUNT(DISTINCT product_src_id)
            FROM (SELECT product_src_id FROM sa_lms.src_lms WHERE product_src_id IS NOT NULL
                  UNION
                  SELECT product_src_id FROM sa_oms.src_oms WHERE product_src_id IS NOT NULL) combined) as sa_count,
           (SELECT COUNT(*) FROM BL_3NF.ce_products_scd WHERE product_id != '-1') as target_count,
           'SA_to_3NF' as test_layer

    UNION ALL

    -- Test 3: Brands completeness SA → 3NF
    SELECT 'brands_sa_to_3nf' as test_name,
           'COMPLETENESS' as test_type,
           (SELECT COUNT(DISTINCT product_brand) FROM sa_oms.src_oms WHERE product_brand IS NOT NULL) as sa_count,
           (SELECT COUNT(*) FROM BL_3NF.ce_brands WHERE brand_id != '-1') as target_count,
           'SA_to_3NF' as test_layer

    UNION ALL

    -- Test 4: Order lines completeness SA → 3NF
    SELECT 'order_lines_sa_to_3nf' as test_name,
           'COMPLETENESS' as test_type,
           (SELECT COUNT(DISTINCT CONCAT(order_src_id, '|', product_src_id, '|', customer_src_id))
            FROM sa_oms.src_oms WHERE order_src_id IS NOT NULL) as sa_count,
           (SELECT COUNT(*) FROM BL_3NF.ce_order_lines) as target_count,
           'SA_to_3NF' as test_layer

    UNION ALL

    -- Test 5: Customers completeness 3NF → DM
    SELECT 'customers_3nf_to_dm' as test_name,
           'COMPLETENESS' as test_type,
           (SELECT COUNT(*) FROM BL_3NF.ce_customers WHERE customer_id != '-1') as sa_count,
           (SELECT COUNT(*) FROM BL_DM.DIM_CUSTOMERS WHERE customer_surr_id != '-1') as target_count,
           '3NF_to_DM' as test_layer

    UNION ALL

    -- Test 6: Products completeness 3NF → DM
    SELECT 'products_3nf_to_dm' as test_name,
           'COMPLETENESS' as test_type,
           (SELECT COUNT(*) FROM BL_3NF.ce_products_scd WHERE product_id != '-1') as sa_count,
           (SELECT COUNT(*) FROM BL_DM.DIM_PRODUCTS_SCD WHERE product_src_id != '-1') as target_count,
           '3NF_to_DM' as test_layer

    UNION ALL

    -- Test 7: Order lines completeness 3NF → DM
    SELECT 'order_lines_3nf_to_dm' as test_name,
           'COMPLETENESS' as test_type,
           (SELECT COUNT(*) FROM BL_3NF.ce_order_lines) as sa_count,
           (SELECT COUNT(*) FROM BL_DM.fct_order_line_shipments_dd) as target_count,
           '3NF_to_DM' as test_layer
)

SELECT test_name,
       test_type,
       sa_count as source_count,
       target_count,
       (target_count - sa_count) as count_difference,
       CASE
           WHEN target_count = sa_count THEN 'PASS'
           WHEN target_count < sa_count THEN 'FAIL - DATA LOSS'
           ELSE 'WARNING - DATA INCREASE'
       END as test_result,
       test_layer
FROM completeness_tests
ORDER BY test_layer, test_name;