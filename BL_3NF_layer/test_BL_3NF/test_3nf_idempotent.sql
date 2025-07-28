--=======================================================================================
-- BL_CL.load_all_dimensions() idempotent test
--=======================================================================================

TRUNCATE BL_3NF.CE_CUSTOMERS, BL_3NF.CE_SALES_REPRESENTATIVES, BL_3NF.CE_ORDER_STATUSES,
    BL_3NF.CE_PAYMENT_METHODS, BL_3NF.CE_PRODUCTS_SCD, BL_3NF.CE_BRANDS,
    BL_3NF.CE_CATEGORIES, BL_3NF.CE_PRODUCT_CATEGORIES, BL_3NF.CE_BRAND_CATEGORIES,
    BL_3NF.CE_PRODUCT_STATUSES, BL_3NF.CE_DEPARTMENTS, BL_3NF.CE_GEOGRAPHIES,
    BL_3NF.CE_CITIES, BL_3NF.CE_STATES, BL_3NF.CE_COUNTRIES, BL_3NF.CE_REGIONS,
    BL_3NF.CE_SHIPPING_MODES, BL_3NF.CE_WAREHOUSES, BL_3NF.CE_CARRIERS,
    BL_3NF.CE_DELIVERY_STATUSES RESTART IDENTITY CASCADE;

---!!! load Def rows file `DDL_def_rows.sql`
 CALL BL_CL.load_default_rows();

call bl_cl.load_all_dimensions();


--- DIM tables
SELECT 'ce_brand_categories' as table_name, COUNT(*) as count
FROM BL_3NF.ce_brand_categories
UNION ALL
SELECT 'ce_brands', COUNT(*)
FROM BL_3NF.ce_brands
UNION ALL
SELECT 'ce_carriers', COUNT(*)
FROM BL_3NF.ce_carriers
UNION ALL
SELECT 'ce_categories', COUNT(*)
FROM BL_3NF.ce_categories
UNION ALL
SELECT 'ce_cities', COUNT(*)
FROM BL_3NF.ce_cities
UNION ALL
SELECT 'ce_countries', COUNT(*)
FROM BL_3NF.ce_countries
UNION ALL
SELECT 'ce_customers', COUNT(*)
FROM BL_3NF.ce_customers
UNION ALL
SELECT 'ce_delivery_statuses', COUNT(*)
FROM BL_3NF.ce_delivery_statuses
UNION ALL
SELECT 'ce_departments', COUNT(*)
FROM BL_3NF.ce_departments
UNION ALL
SELECT 'ce_geographies', COUNT(*)
FROM BL_3NF.ce_geographies
UNION ALL
SELECT 'ce_order_statuses', COUNT(*)
FROM BL_3NF.ce_order_statuses
UNION ALL
SELECT 'ce_payment_methods', COUNT(*)
FROM BL_3NF.ce_payment_methods
UNION ALL
SELECT 'ce_product_categories', COUNT(*)
FROM BL_3NF.ce_product_categories
UNION ALL
SELECT 'ce_product_statuses', COUNT(*)
FROM BL_3NF.ce_product_statuses
UNION ALL
SELECT 'ce_products_scd', COUNT(*)
FROM BL_3NF.ce_products_scd
UNION ALL
SELECT 'ce_regions', COUNT(*)
FROM BL_3NF.ce_regions
UNION ALL
SELECT 'ce_sales_representatives', COUNT(*)
FROM BL_3NF.ce_sales_representatives
UNION ALL
SELECT 'ce_shipping_modes', COUNT(*)
FROM BL_3NF.ce_shipping_modes
UNION ALL
SELECT 'ce_states', COUNT(*)
FROM BL_3NF.ce_states
UNION ALL
SELECT 'ce_warehouses', COUNT(*)
FROM BL_3NF.ce_warehouses
ORDER BY table_name;


--=======================================================================================
-- BL_CL.load_all_facts() idempotent test
--=======================================================================================
-- 1. Clear fact tables to start fresh
TRUNCATE BL_3NF.CE_ORDER_LINES, BL_3NF.CE_SHIPMENTS, BL_3NF.CE_SHIPMENT_LINES,
    BL_3NF.CE_DELIVERIES, BL_3NF.CE_TRANSACTIONS, BL_3NF.CE_ORDERS
    RESTART IDENTITY CASCADE;

--call bl_cl._cleanup_fact_temp_tables();
-- 2. Run the procedure (first time)
CALL BL_CL.load_all_facts();

-- 3. Check the logging table for first execution
SELECT procedure_name,
       target_table,
       source_table,
       status,
       message,
       rows_affected,
       ta_insert_dt
FROM BL_CL.mta_process_log
WHERE procedure_name = 'load_all_facts'
ORDER BY ta_insert_dt DESC;

-- 4. Run the same procedure again (second time)
CALL BL_CL.load_all_facts();

-- 5. Check the logging table for second execution
SELECT procedure_name,
       target_table,
       source_table,
       status,
       message,
       rows_affected,
       ta_insert_dt
FROM BL_CL.mta_process_log
WHERE procedure_name = 'load_all_facts'
ORDER BY ta_insert_dt DESC
LIMIT 5;

-- 6. Check all individual procedure executions from both runs
SELECT procedure_name,
       source_table,
       target_table,
       status,
       rows_affected,
       ta_insert_dt,
       ROW_NUMBER() OVER (PARTITION BY procedure_name ORDER BY ta_insert_dt DESC) as execution_number
FROM BL_CL.mta_process_log
WHERE procedure_name LIKE '%_internal'
   OR procedure_name LIKE '%_mapping%'
   OR procedure_name LIKE '%temp_tables%'
ORDER BY ta_insert_dt DESC
limit 21;


-- 7. Verify that table counts remain the same after second execution
SELECT 'ce_orders' as table_name, COUNT(*) as count
FROM BL_3NF.ce_orders
UNION ALL
SELECT 'ce_order_lines', COUNT(*)
FROM BL_3NF.ce_order_lines
UNION ALL
SELECT 'ce_shipments', COUNT(*)
FROM BL_3NF.ce_shipments
UNION ALL
SELECT 'ce_shipment_lines', COUNT(*)
FROM BL_3NF.ce_shipment_lines
UNION ALL
SELECT 'ce_transactions', COUNT(*)
FROM BL_3NF.ce_transactions
UNION ALL
SELECT 'ce_deliveries', COUNT(*)
FROM BL_3NF.ce_deliveries
ORDER BY table_name;

--=======================================================================================
-- BL_CL.load_bl_3nf_full() idempotent test
--=======================================================================================

TRUNCATE BL_3NF.CE_ORDER_LINES, BL_3NF.CE_SHIPMENTS, BL_3NF.CE_SHIPMENT_LINES,
    BL_3NF.CE_DELIVERIES, BL_3NF.CE_TRANSACTIONS, Bl_3nf.ce_orders RESTART IDENTITY CASCADE;

TRUNCATE BL_3NF.CE_CUSTOMERS, BL_3NF.CE_SALES_REPRESENTATIVES, BL_3NF.CE_ORDER_STATUSES,
    BL_3NF.CE_PAYMENT_METHODS, BL_3NF.CE_PRODUCTS_SCD, BL_3NF.CE_BRANDS,
    BL_3NF.CE_CATEGORIES, BL_3NF.CE_PRODUCT_CATEGORIES, BL_3NF.CE_BRAND_CATEGORIES,
    BL_3NF.CE_PRODUCT_STATUSES, BL_3NF.CE_DEPARTMENTS, BL_3NF.CE_GEOGRAPHIES,
    BL_3NF.CE_CITIES, BL_3NF.CE_STATES, BL_3NF.CE_COUNTRIES, BL_3NF.CE_REGIONS,
    BL_3NF.CE_SHIPPING_MODES, BL_3NF.CE_WAREHOUSES, BL_3NF.CE_CARRIERS,
    BL_3NF.CE_DELIVERY_STATUSES RESTART IDENTITY CASCADE;

---!!! load Def rows file `DDL_def_rows.sql`
 CALL BL_CL.load_default_rows();

-- FULL load
CALL BL_CL.load_bl_3nf_full();
--FACT tables
SELECT 'ce_orders' as table_name, COUNT(*) as count
FROM BL_3NF.ce_orders
UNION ALL
SELECT 'ce_order_lines', COUNT(*)
FROM BL_3NF.ce_order_lines
UNION ALL
SELECT 'ce_shipments', COUNT(*)
FROM BL_3NF.ce_shipments
UNION ALL
SELECT 'ce_shipment_lines', COUNT(*)
FROM BL_3NF.ce_shipment_lines
UNION ALL
SELECT 'ce_transactions', COUNT(*)
FROM BL_3NF.ce_transactions
UNION ALL
SELECT 'ce_deliveries', COUNT(*)
FROM BL_3NF.ce_deliveries
ORDER BY table_name;
--- DIM tables
SELECT 'ce_brand_categories' as table_name, COUNT(*) as count
FROM BL_3NF.ce_brand_categories
UNION ALL
SELECT 'ce_brands', COUNT(*)
FROM BL_3NF.ce_brands
UNION ALL
SELECT 'ce_carriers', COUNT(*)
FROM BL_3NF.ce_carriers
UNION ALL
SELECT 'ce_categories', COUNT(*)
FROM BL_3NF.ce_categories
UNION ALL
SELECT 'ce_cities', COUNT(*)
FROM BL_3NF.ce_cities
UNION ALL
SELECT 'ce_countries', COUNT(*)
FROM BL_3NF.ce_countries
UNION ALL
SELECT 'ce_customers', COUNT(*)
FROM BL_3NF.ce_customers
UNION ALL
SELECT 'ce_delivery_statuses', COUNT(*)
FROM BL_3NF.ce_delivery_statuses
UNION ALL
SELECT 'ce_departments', COUNT(*)
FROM BL_3NF.ce_departments
UNION ALL
SELECT 'ce_geographies', COUNT(*)
FROM BL_3NF.ce_geographies
UNION ALL
SELECT 'ce_order_statuses', COUNT(*)
FROM BL_3NF.ce_order_statuses
UNION ALL
SELECT 'ce_payment_methods', COUNT(*)
FROM BL_3NF.ce_payment_methods
UNION ALL
SELECT 'ce_product_categories', COUNT(*)
FROM BL_3NF.ce_product_categories
UNION ALL
SELECT 'ce_product_statuses', COUNT(*)
FROM BL_3NF.ce_product_statuses
UNION ALL
SELECT 'ce_products_scd', COUNT(*)
FROM BL_3NF.ce_products_scd
UNION ALL
SELECT 'ce_regions', COUNT(*)
FROM BL_3NF.ce_regions
UNION ALL
SELECT 'ce_sales_representatives', COUNT(*)
FROM BL_3NF.ce_sales_representatives
UNION ALL
SELECT 'ce_shipping_modes', COUNT(*)
FROM BL_3NF.ce_shipping_modes
UNION ALL
SELECT 'ce_states', COUNT(*)
FROM BL_3NF.ce_states
UNION ALL
SELECT 'ce_warehouses', COUNT(*)
FROM BL_3NF.ce_warehouses
ORDER BY table_name;