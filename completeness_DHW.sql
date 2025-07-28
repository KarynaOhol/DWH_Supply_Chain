-- Complete Data Warehouse Layer Row Count Comparison
-- Compares row counts across SA → 3NF → DM layers with business logic mapping

WITH
-- SA Layer Counts (Source tables)
sa_counts AS (
    -- Source tables
    SELECT 'SA'                                      as layer,
           'sa_lms.src_lms'                          as table_name,
           'Source'                                  as table_type,
           (SELECT COUNT(*) FROM sa_lms.src_lms)     as business_row_count,
           CAST(NULL AS INTEGER)                     as default_row_count,
           CAST(NULL AS BIGINT)                      as previous_layer_business_row_count,
           CAST(NULL AS BIGINT)                      as count_difference,
           'Logistics Management System source data' as note

    UNION ALL
    SELECT 'SA',
           'sa_oms.src_oms',
           'Source',
           (SELECT COUNT(*) FROM sa_oms.src_oms),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Order Management System source data'

    -- Virtual dimension extractions for mapping reference
    UNION ALL
    SELECT 'SA',
           'distinct_customers_combined',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT customer_src_id)
            FROM (SELECT customer_src_id
                  FROM sa_lms.src_lms
                  WHERE customer_src_id IS NOT NULL
                  UNION
                  SELECT customer_src_id
                  FROM sa_oms.src_oms
                  WHERE customer_src_id IS NOT NULL) combined),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct customers from both SA tables'

    UNION ALL
    SELECT 'SA',
           'distinct_products_combined',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT product_src_id)
            FROM (SELECT product_src_id
                  FROM sa_lms.src_lms
                  WHERE product_src_id IS NOT NULL
                  UNION
                  SELECT product_src_id
                  FROM sa_oms.src_oms
                  WHERE product_src_id IS NOT NULL) combined),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct products from both SA tables'

    -- LMS-specific dimension extractions
    UNION ALL
    SELECT 'SA',
           'distinct_warehouses_lms',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT warehouse_src_id) FROM sa_lms.src_lms WHERE warehouse_src_id IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct warehouses from LMS'

    UNION ALL
    SELECT 'SA',
           'distinct_carriers_lms',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT carrier_src_id) FROM sa_lms.src_lms WHERE carrier_src_id IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct carriers from LMS'

    UNION ALL
    SELECT 'SA',
           'distinct_shipping_modes_lms',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT shipping_mode) FROM sa_lms.src_lms WHERE shipping_mode IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct shipping modes from LMS'

    UNION ALL
    SELECT 'SA',
           'distinct_delivery_statuses_lms',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT delivery_status) FROM sa_lms.src_lms WHERE delivery_status IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct delivery statuses from LMS'

    -- Geographic extractions from LMS
    UNION ALL
    SELECT 'SA',
           'distinct_cities_lms',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT CONCAT(destination_city, '|', destination_state))
            FROM sa_lms.src_lms
            WHERE destination_city IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct cities from LMS destination data'

    UNION ALL
    SELECT 'SA',
           'distinct_states_lms',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT destination_state) FROM sa_lms.src_lms WHERE destination_state IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct states from LMS destination data'

    UNION ALL
    SELECT 'SA',
           'distinct_countries_lms',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT destination_country) FROM sa_lms.src_lms WHERE destination_country IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct countries from LMS destination data'
    UNION ALL
    SELECT 'SA',
           'distinct_geographies_lms',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT CONCAT(destination_city, '|', destination_state, '|', destination_country))
            FROM sa_lms.src_lms
            WHERE destination_country IS NOT NULL
              AND destination_city IS NOT NULL
              AND destination_state IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct countries from LMS destination data'

    -- OMS-specific dimension extractions
    UNION ALL
    SELECT 'SA',
           'distinct_sales_reps_oms',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT sales_rep_src_id) FROM sa_oms.src_oms WHERE sales_rep_src_id IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct sales representatives from OMS'

    UNION ALL
    SELECT 'SA',
           'distinct_order_statuses_oms',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT order_status) FROM sa_oms.src_oms WHERE order_status IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct order statuses from OMS'

    UNION ALL
    SELECT 'SA',
           'distinct_payment_methods_oms',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT payment_method) FROM sa_oms.src_oms WHERE payment_method IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct payment methods from OMS'

-- Product hierarchy extractions from OMS
    UNION ALL
    SELECT 'SA',
           'distinct_brands_oms',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT product_brand) FROM sa_oms.src_oms WHERE product_brand IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct brands from OMS'

    UNION ALL
    SELECT 'SA',
           'distinct_categories_oms',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT product_category_src_id) FROM sa_oms.src_oms WHERE product_category IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct categories from OMS'

    UNION ALL
    SELECT 'SA',
           'distinct_departments_oms',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT department_name) FROM sa_oms.src_oms WHERE department_name IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct departments from OMS'

    UNION ALL
    SELECT 'SA',
           'distinct_product_statuses_oms',
           'Virtual_Dim',
           (SELECT COUNT(DISTINCT product_status) FROM sa_oms.src_oms WHERE product_status IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct product statuses from OMS'

-- Fact-level aggregations for reference
    UNION ALL
    SELECT 'SA',
           'distinct_orders_oms',
           'Virtual_Fact',
           (SELECT COUNT(DISTINCT order_src_id) FROM sa_oms.src_oms WHERE order_src_id IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct orders from OMS'
    UNION ALL
    SELECT 'SA',
           'distinct_order_lines_oms',
           'Virtual_Fact',
           (SELECT count(distinct concat(order_src_id, '|', product_src_id, '|', customer_src_id))
            FROM sa_oms.src_oms
            WHERE order_src_id IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct order lines from OMS order_src_id|product_src_id|customer_src_id)'

    UNION ALL
    SELECT 'SA',
           'distinct_shipments_lms',
           'Virtual_Fact',
           (SELECT COUNT(DISTINCT shipment_src_id) FROM sa_lms.src_lms WHERE shipment_src_id IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct shipments from LMS'
    UNION ALL
    SELECT 'SA',
           'distinct_shipment_lines_lms',
           'Virtual_Fact',
           (SELECT count(distinct concat(shipment_src_id, '|', product_src_id, '|', customer_src_id))
            FROM sa_lms.src_lms
            WHERE shipment_src_id IS NOT NULL),
           CAST(NULL AS INTEGER),
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Distinct shipment lines from LMS shipment_src_id|product_src_id|customer_src_id)'),

-- 3NF Layer Counts
nf3_counts AS (
    -- Direct mapping dimension tables
    SELECT '3NF'                                                              as layer,
           'ce_carriers'                                                      as table_name,
           'Dimension'                                                        as table_type,
           (SELECT COUNT(*) FROM BL_3NF.ce_carriers WHERE carrier_id != '-1') as business_row_count,
           1                                                                  as default_row_count,
           (SELECT business_row_count
            FROM sa_counts
            WHERE table_name = 'distinct_carriers_lms')                       as previous_layer_business_row_count,
           CAST(NULL AS BIGINT)                                               as count_difference,
           'Extracted from distinct carrier_src_id in sa_lms.src_lms'         as note

    UNION ALL
    SELECT '3NF',
           'ce_customers',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_customers WHERE customer_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_customers_combined'),
           CAST(NULL AS BIGINT),
           'Extracted from distinct customer_src_id in both SA tables'

    UNION ALL
    SELECT '3NF',
           'ce_delivery_statuses',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_delivery_statuses WHERE delivery_status_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_delivery_statuses_lms'),
           CAST(NULL AS BIGINT),
           'Extracted from distinct delivery_status in sa_lms.src_lms'

    UNION ALL
    SELECT '3NF',
           'ce_order_statuses',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_order_statuses WHERE order_status_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_order_statuses_oms'),
           CAST(NULL AS BIGINT),
           'Extracted from distinct order_status in sa_oms.src_oms'

    UNION ALL
    SELECT '3NF',
           'ce_payment_methods',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_payment_methods WHERE payment_method_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_payment_methods_oms'),
           CAST(NULL AS BIGINT),
           'Extracted from distinct payment_method in sa_oms.src_oms'

    UNION ALL
    SELECT '3NF',
           'ce_sales_representatives',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_sales_representatives WHERE sales_rep_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_sales_reps_oms'),
           CAST(NULL AS BIGINT),
           'Extracted from distinct sales_rep_src_id in sa_oms.src_oms'

    UNION ALL
    SELECT '3NF',
           'ce_shipping_modes',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_shipping_modes WHERE shipping_mode_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_shipping_modes_lms'),
           CAST(NULL AS BIGINT),
           'Extracted from distinct shipping_mode in sa_lms.src_lms'

    UNION ALL
    SELECT '3NF',
           'ce_warehouses',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_warehouses WHERE warehouse_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_warehouses_lms'),
           CAST(NULL AS BIGINT),
           'Extracted from distinct warehouse_src_id in sa_lms.src_lms'

    -- Geography hierarchy tables
    UNION ALL
    SELECT '3NF',
           'ce_cities',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_cities WHERE city_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_cities_lms'),
           CAST(NULL AS BIGINT),
           'Extracted from distinct destination_city in sa_lms.src_lms'

    UNION ALL
    SELECT '3NF',
           'ce_states',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_states WHERE state_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_states_lms'),
           CAST(NULL AS BIGINT),
           'Extracted from distinct destination_state in sa_lms.src_lms'

    UNION ALL
    SELECT '3NF',
           'ce_countries',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_countries WHERE country_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_countries_lms'),
           CAST(NULL AS BIGINT),
           'Extracted from distinct destination_country in sa_lms.src_lms'

    UNION ALL
    SELECT '3NF',
           'ce_regions',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_regions WHERE region_id != '-1'),
           1,
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Manual reference data - no SA source (North America, South America, etc.)'

    UNION ALL
    SELECT '3NF',
           'ce_geographies',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_geographies WHERE geography_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_geographies_lms'),
           CAST(NULL AS BIGINT),
           'Geography hierarchy built from sa_lms.src_lms destination fields'

    -- Product hierarchy tables
    UNION ALL
    SELECT '3NF',
           'ce_products_scd',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_products_scd WHERE product_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_products_combined'),
           CAST(NULL AS BIGINT),
           'Extracted from distinct product_src_id in both SA tables'

    UNION ALL
    SELECT '3NF',
           'ce_brands',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_brands WHERE brand_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_brands_oms'),
           CAST(NULL AS BIGINT),
           'Extracted from distinct product_brand in sa_oms.src_oms'

    UNION ALL
    SELECT '3NF',
           'ce_categories',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_categories WHERE category_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_categories_oms'),
           CAST(NULL AS BIGINT),
           'Extracted from distinct product_category in sa_oms.src_oms'

    UNION ALL
    SELECT '3NF',
           'ce_departments',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_departments WHERE department_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_departments_oms'),
           CAST(NULL AS BIGINT),
           'Extracted from distinct department_name in sa_oms.src_oms'

    UNION ALL
    SELECT '3NF',
           'ce_product_statuses',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_product_statuses WHERE status_id != '-1'),
           1,
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_product_statuses_oms'),
           CAST(NULL AS BIGINT),
           'Extracted from distinct product_status in sa_oms.src_oms'

    UNION ALL
    SELECT '3NF',
           'ce_brand_categories',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_brand_categories WHERE brand_category_id != '-1'),
           1,
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Brand-category relationships derived from sa_oms.src_oms'

    UNION ALL
    SELECT '3NF',
           'ce_product_categories',
           'Dimension',
           (SELECT COUNT(*) FROM BL_3NF.ce_product_categories WHERE product_category_id != '-1'),
           1,
           CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT),
           'Product-category relationships derived from sa_oms.src_oms'

    -- Fact tables
    UNION ALL
    SELECT '3NF',
           'ce_orders',
           'Fact',
           (SELECT COUNT(*) FROM BL_3NF.ce_orders),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_orders_oms'),
           CAST(NULL AS BIGINT),
           'Aggregated from sa_oms.src_oms grouped by order_src_id'

    UNION ALL
    SELECT '3NF',
           'ce_order_lines',
           'Fact',
           (SELECT COUNT(*) FROM BL_3NF.ce_order_lines),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_order_lines_oms'),
           CAST(NULL AS BIGINT),
           'Direct 1:1 mapping from sa_oms.src_oms (grain: order_item_src_id)'

    UNION ALL
    SELECT '3NF',
           'ce_shipments',
           'Fact',
           (SELECT COUNT(*) FROM BL_3NF.ce_shipments),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_shipments_lms'),
           CAST(NULL AS BIGINT),
           'Aggregated from sa_lms.src_lms grouped by shipment_src_id'

    UNION ALL
    SELECT '3NF',
           'ce_shipment_lines',
           'Fact',
           (SELECT COUNT(*) FROM BL_3NF.ce_shipment_lines),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_shipment_lines_lms'),
           CAST(NULL AS BIGINT),
           'Direct 1:1 mapping from sa_lms.src_lms'

    UNION ALL
    SELECT '3NF',
           'ce_deliveries',
           'Fact',
           (SELECT COUNT(*) FROM BL_3NF.ce_deliveries),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'distinct_shipments_lms'),
           CAST(NULL AS BIGINT),
           'Delivery events aggregated from shipments in sa_lms.src_lms'

    UNION ALL
    SELECT '3NF',
           'ce_transactions',
           'Fact',
           (SELECT COUNT(*) FROM BL_3NF.ce_transactions),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM sa_counts WHERE table_name = 'sa_oms.src_oms'),
           CAST(NULL AS BIGINT),
           'Same as order lines - financial view from sa_oms.src_oms'),

-- DM Layer Counts (existing logic, now with 3NF as previous layer)
dm_counts AS (
    -- Direct mapping dimensions
    SELECT 'DM'                                                                    as layer,
           'DIM_CARRIERS'                                                          as table_name,
           'Dimension'                                                             as table_type,
           (SELECT COUNT(*) FROM BL_DM.DIM_CARRIERS WHERE carrier_surr_id != '-1') as business_row_count,
           1                                                                       as default_row_count,
           (SELECT business_row_count
            FROM nf3_counts
            WHERE table_name = 'ce_carriers')                                      as previous_layer_business_row_count,
           CAST(NULL AS BIGINT)                                                    as count_difference,
           'Direct 1:1 mapping from ce_carriers'                                   as note

    UNION ALL
    SELECT 'DM',
           'DIM_CUSTOMERS',
           'Dimension',
           (SELECT COUNT(*) FROM BL_DM.DIM_CUSTOMERS WHERE customer_surr_id != '-1'),
           1,
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_customers'),
           CAST(NULL AS BIGINT),
           'Direct 1:1 mapping from ce_customers'

    UNION ALL
    SELECT 'DM',
           'DIM_DELIVERY_STATUSES',
           'Dimension',
           (SELECT COUNT(*) FROM BL_DM.DIM_DELIVERY_STATUSES WHERE delivery_status_surr_id != '-1'),
           1,
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_delivery_statuses'),
           CAST(NULL AS BIGINT),
           'Direct 1:1 mapping from ce_delivery_statuses'

    UNION ALL
    SELECT 'DM',
           'DIM_ORDER_STATUSES',
           'Dimension',
           (SELECT COUNT(*) FROM BL_DM.DIM_ORDER_STATUSES WHERE order_status_surr_id != '-1'),
           1,
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_order_statuses'),
           CAST(NULL AS BIGINT),
           'Direct 1:1 mapping from ce_order_statuses'

    UNION ALL
    SELECT 'DM',
           'DIM_PAYMENT_METHODS',
           'Dimension',
           (SELECT COUNT(*) FROM BL_DM.DIM_PAYMENT_METHODS WHERE payment_method_surr_id != '-1'),
           1,
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_payment_methods'),
           CAST(NULL AS BIGINT),
           'Direct 1:1 mapping from ce_payment_methods'

    UNION ALL
    SELECT 'DM',
           'DIM_SALES_REPRESENTATIVES',
           'Dimension',
           (SELECT COUNT(*) FROM BL_DM.DIM_SALES_REPRESENTATIVES WHERE sales_rep_surr_id != '-1'),
           1,
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_sales_representatives'),
           CAST(NULL AS BIGINT),
           'Direct 1:1 mapping from ce_sales_representatives'

    UNION ALL
    SELECT 'DM',
           'DIM_SHIPPING_MODES',
           'Dimension',
           (SELECT COUNT(*) FROM BL_DM.DIM_SHIPPING_MODES WHERE shipping_mode_surr_id != '-1'),
           1,
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_shipping_modes'),
           CAST(NULL AS BIGINT),
           'Direct 1:1 mapping from ce_shipping_modes'

    UNION ALL
    SELECT 'DM',
           'DIM_WAREHOUSES',
           'Dimension',
           (SELECT COUNT(*) FROM BL_DM.DIM_WAREHOUSES WHERE warehouse_surr_id != '-1'),
           1,
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_warehouses'),
           CAST(NULL AS BIGINT),
           'Direct 1:1 mapping from ce_warehouses'

    -- Geography dimension (flattened hierarchy)
    UNION ALL
    SELECT 'DM',
           'DIM_GEOGRAPHIES',
           'Dimension',
           (SELECT COUNT(*) FROM BL_DM.DIM_GEOGRAPHIES WHERE geography_surr_id != '-1'),
           1,
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_geographies'),
           CAST(NULL AS BIGINT),
           'Flattened from ce_cities, ce_states, ce_countries, ce_regions'

    -- Embedded geography counts in DM_GEOGRAPHIES
    UNION ALL
    SELECT 'DM',
           'cities_in_DIM_GEOGRAPHIES',
           'Embedded',
           (SELECT COUNT(CONCAT(city_src_id, '|', state_src_id)) FROM BL_DM.DIM_GEOGRAPHIES WHERE city_src_id != '-1'),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_cities'),
           CAST(NULL AS BIGINT),
           'Distinct cities embedded in DIM_GEOGRAPHIES'

    UNION ALL
    SELECT 'DM',
           'states_in_DIM_GEOGRAPHIES',
           'Embedded',
           (SELECT COUNT(DISTINCT state_src_id) FROM BL_DM.DIM_GEOGRAPHIES),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_states'),
           CAST(NULL AS BIGINT),
           'Distinct states embedded in DIM_GEOGRAPHIES'

    UNION ALL
    SELECT 'DM',
           'countries_in_DIM_GEOGRAPHIES',
           'Embedded',
           (SELECT COUNT(DISTINCT country_src_id) FROM BL_DM.DIM_GEOGRAPHIES WHERE country_src_id != '-1'),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_countries'),
           CAST(NULL AS BIGINT),
           'Distinct countries embedded in DIM_GEOGRAPHIES'

    UNION ALL
    SELECT 'DM',
           'regions_in_DIM_GEOGRAPHIES',
           'Embedded',
           (SELECT COUNT(DISTINCT region_src_id) FROM BL_DM.DIM_GEOGRAPHIES WHERE region_src_id != '-1'),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_regions'),
           CAST(NULL AS BIGINT),
           'Distinct regions embedded in DIM_GEOGRAPHIES'

    -- Product dimension (flattened hierarchy)
    UNION ALL
    SELECT 'DM',
           'DIM_PRODUCTS',
           'Dimension',
           (SELECT COUNT(*) FROM BL_DM.DIM_PRODUCTS_SCD WHERE product_src_id != '-1'),
           1,
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_products_scd'),
           CAST(NULL AS BIGINT),
           'SCD Type 2 from ce_products_scd with embedded hierarchies'

    -- Embedded product hierarchy counts in DIM_PRODUCTS
    UNION ALL
    SELECT 'DM',
           'brands_in_DIM_PRODUCTS',
           'Embedded',
           (SELECT COUNT(DISTINCT brand_src_id)
            FROM BL_DM.DIM_PRODUCTS_SCD
            WHERE brand_src_id != '-1'
              AND is_active = 'Y'),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_brands'),
           CAST(NULL AS BIGINT),
           'Distinct brands embedded in DIM_PRODUCTS'

    UNION ALL
    SELECT 'DM',
           'categories_in_DIM_PRODUCTS',
           'Embedded',
           (SELECT COUNT(DISTINCT primary_category_src_id)
            FROM BL_DM.DIM_PRODUCTS_SCD
            WHERE primary_category_src_id != '-1'
              AND is_active = 'Y'),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_categories'),
           CAST(NULL AS BIGINT),
           'Distinct categories embedded in DIM_PRODUCTS'

    UNION ALL
    SELECT 'DM',
           'departments_in_DIM_PRODUCTS',
           'Embedded',
           (SELECT COUNT(DISTINCT department_src_id)
            FROM BL_DM.DIM_PRODUCTS_SCD
            WHERE department_src_id != '-1'
              AND is_active = 'Y'),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_departments'),
           CAST(NULL AS BIGINT),
           'Distinct departments embedded in DIM_PRODUCTS'

    UNION ALL
    SELECT 'DM',
           'product_statuses_in_DIM_PRODUCTS',
           'Embedded',
           (SELECT COUNT(DISTINCT product_status_src_id)
            FROM BL_DM.DIM_PRODUCTS_SCD
            WHERE product_status_src_id != '-1'
               --AND is_active = 'Y'
           ),


           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_product_statuses'),
           CAST(NULL AS BIGINT),
           'Distinct product statuses embedded in DIM_PRODUCTS !note we take fist ocurence of a product in dataset'


    -- Fact table
    UNION ALL
    SELECT 'DM',
           'fct_order_line_shipments_dd',
           'Fact',
           (SELECT COUNT(*) FROM BL_DM.fct_order_line_shipments_dd),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_order_lines'),
           CAST(NULL AS BIGINT),
           'Grain: one row per order line per shipment line'

    -- Fact table aggregations to show relationship
    UNION ALL
    SELECT 'DM',
           'distinct_orders_in_fact',
           'Fact_Agg',
           (SELECT COUNT(DISTINCT order_src_id) FROM BL_DM.fct_order_line_shipments_dd),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_orders'),
           CAST(NULL AS BIGINT),
           'Distinct orders in fact table'

    UNION ALL
    SELECT 'DM',
           'distinct_shipments_in_fact',
           'Fact_Agg',
           (SELECT COUNT(DISTINCT shipment_src_id) FROM BL_DM.fct_order_line_shipments_dd),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_shipments'),
           CAST(NULL AS BIGINT),
           'Distinct shipments in fact table'

    UNION ALL
    SELECT 'DM',
           'distinct_deliveries_in_fact',
           'Fact_Agg',
           (SELECT COUNT(DISTINCT delivery_src_id) FROM BL_DM.fct_order_line_shipments_dd),
           CAST(NULL AS INTEGER),
           (SELECT business_row_count FROM nf3_counts WHERE table_name = 'ce_deliveries'),
           CAST(NULL AS BIGINT),
           'Distinct deliveries in fact table'),

-- Combine all results
all_counts AS (SELECT *
               FROM sa_counts
               UNION ALL
               SELECT *
               FROM nf3_counts
               UNION ALL
               SELECT *
               FROM dm_counts)

-- Final result with calculated differences
SELECT layer,
       table_name,
       table_type,
       business_row_count,
       default_row_count,
       previous_layer_business_row_count,
       CASE
           WHEN previous_layer_business_row_count IS NULL THEN NULL
           ELSE business_row_count - previous_layer_business_row_count
           END as count_difference,
       note
FROM all_counts
ORDER BY CASE layer
             WHEN 'SA' THEN 1
             WHEN '3NF' THEN 2
             WHEN 'DM' THEN 3
             END,
         CASE table_type
             WHEN 'Source' THEN 1
             WHEN 'Virtual_Dim' THEN 2
             WHEN 'Virtual_Fact' THEN 3
             WHEN 'Dimension' THEN 4
             WHEN 'Embedded' THEN 5
             WHEN 'Fact' THEN 6
             WHEN 'Fact_Agg' THEN 7
             END,
         table_name;
----==================================================================

-- Query 1: Sales Performance Analysis by Geography and Time
-- Tests joins with dim_geographies, dim_time_day, dim_customers, and aggregation functionality
SELECT
    g.country_name,
    g.state_name,
    g.city_name,
    t.year_num,
    t.quarter_num,
    t.month_name,
    COUNT(DISTINCT f.order_dt_surr_id) as total_orders,
    COUNT(f.order_line_src_id) as total_order_lines,
    SUM(f.ordered_quantity_cnt) as total_quantity_ordered,
    SUM(f.shipped_quantity_cnt) as total_quantity_shipped,
    ROUND(AVG(f.unit_price_act), 2) as avg_unit_price,
    SUM(f.unit_price_act * f.shipped_quantity_cnt) as total_revenue,
    ROUND(
        CASE
            WHEN SUM(f.ordered_quantity_cnt) > 0
            THEN (SUM(f.shipped_quantity_cnt) * 100.0 / SUM(f.ordered_quantity_cnt))
            ELSE 0
        END, 2
    ) as fulfillment_rate_pct
FROM bl_dm.fct_order_line_shipments_dd f
INNER JOIN bl_dm.dim_geographies g ON f.customer_geography_surr_id = g.geography_surr_id
INNER JOIN bl_dm.dim_time_day t ON f.order_dt_surr_id = t.dt_surr_id
INNER JOIN bl_dm.dim_customers c ON f.customer_surr_id = c.customer_surr_id
WHERE t.year_num >= 2023  -- Focus on recent data
    AND f.shipped_quantity_cnt > 0  -- Only shipped orders
GROUP BY g.country_name, g.state_name, g.city_name,
         t.year_num, t.quarter_num, t.month_name
HAVING SUM(f.unit_price_act * f.shipped_quantity_cnt) > 1000  -- Minimum revenue threshold
ORDER BY total_revenue DESC, g.country_name, g.state_name, t.year_num, t.quarter_num;


-- Query 2: Delivery Performance and Logistics Analysis
-- Tests joins with multiple dimension tables and delivery/shipping metrics
SELECT
    p.product_name,
    p.brand_name,
    p.primary_category_name,
    car.carrier_name,
    car.carrier_type,
    sm.shipping_mode,
    ds.delivery_status,
    COUNT(*) as shipment_count,
    AVG(f.delivery_days_cnt) as avg_delivery_days,
    AVG(f.planned_delivery_days_cnt) as avg_planned_delivery_days,
    AVG(f.order_to_ship_days_cnt) as avg_processing_days,
    SUM(f.shipping_cost_act) as total_shipping_cost,
    SUM(f.allocated_shipping_cost_act) as total_allocated_shipping_cost,
    ROUND(AVG(f.fill_rate_pct), 2) as avg_fill_rate,
    -- Calculate on-time delivery rate
    ROUND(
        SUM(CASE WHEN f.delivery_days_cnt <= f.planned_delivery_days_cnt THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) as on_time_delivery_rate_pct,
    -- Calculate late delivery average
    ROUND(
        AVG(CASE WHEN f.delivery_days_cnt > f.planned_delivery_days_cnt
                 THEN f.delivery_days_cnt - f.planned_delivery_days_cnt
                 ELSE 0 END),
        2
    ) as avg_days_late
FROM bl_dm.fct_order_line_shipments_dd f
INNER JOIN bl_dm.dim_products_scd p ON f.product_surr_id = p.product_surr_id
    AND p.is_active = 'Y'  -- Get current active record for SCD
INNER JOIN bl_dm.dim_carriers car ON f.carrier_surr_id = car.carrier_surr_id
INNER JOIN bl_dm.dim_shipping_modes sm ON f.shipping_mode_surr_id = sm.shipping_mode_surr_id
INNER JOIN bl_dm.dim_delivery_statuses ds ON f.delivery_status_surr_id = ds.delivery_status_surr_id
INNER JOIN bl_dm.dim_time_day td ON f.delivery_dt_surr_id = td.dt_surr_id
WHERE f.shipped_quantity_cnt > 0  -- Only actual shipments
    AND td.year_num >= 2024  -- Recent data
    AND f.delivery_days_cnt IS NOT NULL  -- Only completed deliveries
GROUP BY p.product_name, p.brand_name, p.primary_category_name,
         car.carrier_name, car.carrier_type, sm.shipping_mode, ds.delivery_status
HAVING COUNT(*) >= 5  -- Minimum sample size for reliable metrics
ORDER BY shipment_count DESC, on_time_delivery_rate_pct DESC;
