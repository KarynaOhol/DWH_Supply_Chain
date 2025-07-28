-- Data Quality Monitoring Framework Implementation
DROP table data_quality_tests,data_quality_test_results CASCADE ;
-- 1. Create Test Registry Table
CREATE TABLE data_quality_tests (
    test_id SERIAL PRIMARY KEY,
    test_name VARCHAR(100) NOT NULL,
    test_category VARCHAR(50) NOT NULL, -- 'DUPLICATES', 'COMPLETENESS'
    test_sql TEXT NOT NULL,
    target_table VARCHAR(100),
    source_table VARCHAR(100),
    expected_result VARCHAR(50), -- '0' for duplicates, 'EQUAL' for completeness
    is_active BOOLEAN DEFAULT TRUE,
    test_layer VARCHAR(20), -- 'SA', '3NF', 'DM'
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description VARCHAR(500)
);

-- 2. Create Test Results Log Table
CREATE TABLE data_quality_test_results (
    result_id SERIAL PRIMARY KEY,
    test_id INT REFERENCES data_quality_tests(test_id),
    execution_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    test_result VARCHAR(20), -- 'PASS', 'FAIL', 'WARNING'
    actual_value VARCHAR(100),
    expected_value VARCHAR(100),
    execution_time_ms INT,
    error_message TEXT
);

-- 3. Insert Duplicate Tests
INSERT INTO data_quality_tests (test_name, test_category, test_sql, target_table, expected_result, test_layer, description) VALUES
('ce_customers_duplicates', 'DUPLICATES',
 'SELECT COUNT(*) FROM (SELECT customer_src_id, COUNT(*) FROM BL_3NF.ce_customers WHERE customer_id != ''-1'' GROUP BY customer_src_id HAVING COUNT(*) > 1) x',
 'ce_customers', '0', '3NF', 'Check for duplicate customers in 3NF layer'),

('ce_products_scd_duplicates', 'DUPLICATES',
 'SELECT COUNT(*) FROM (SELECT product_src_id, is_active, COUNT(*) FROM BL_3NF.ce_products_scd WHERE product_id != ''-1'' GROUP BY product_src_id, is_active HAVING COUNT(*) > 1) x',
 'ce_products_scd', '0', '3NF', 'Check for duplicate active products in 3NF SCD'),

('DIM_CUSTOMERS_duplicates', 'DUPLICATES',
 'SELECT COUNT(*) FROM (SELECT customer_src_id, COUNT(*) FROM BL_DM.DIM_CUSTOMERS WHERE customer_surr_id != ''-1'' GROUP BY customer_src_id HAVING COUNT(*) > 1) x',
 'DIM_CUSTOMERS', '0', 'DM', 'Check for duplicate customers in DM layer'),

('DIM_PRODUCTS_SCD_duplicates', 'DUPLICATES',
 'SELECT COUNT(*) FROM (SELECT product_src_id, COUNT(*) FROM BL_DM.DIM_PRODUCTS_SCD WHERE product_src_id != ''-1'' AND is_active = ''Y'' GROUP BY product_src_id HAVING COUNT(*) > 1) x',
 'DIM_PRODUCTS_SCD', '0', 'DM', 'Check for duplicate active products in DM layer'),

('fct_order_line_shipments_dd_duplicates', 'DUPLICATES',
 'SELECT COUNT(*) FROM (SELECT order_line_src_id, COUNT(*) FROM BL_DM.fct_order_line_shipments_dd GROUP BY order_line_src_id HAVING COUNT(*) > 1) x',
 'fct_order_line_shipments_dd', '0', 'DM', 'Check for duplicate fact records');

-- 4. Insert Completeness Tests
INSERT INTO data_quality_tests (test_name, test_category, test_sql, target_table, source_table, expected_result, test_layer, description) VALUES
('customers_sa_to_3nf_completeness', 'COMPLETENESS',
 'SELECT CASE WHEN sa_count = nf3_count THEN ''PASS'' ELSE ''FAIL'' END FROM (SELECT (SELECT COUNT(DISTINCT customer_src_id) FROM (SELECT customer_src_id FROM sa_lms.src_lms WHERE customer_src_id IS NOT NULL UNION SELECT customer_src_id FROM sa_oms.src_oms WHERE customer_src_id IS NOT NULL) c) as sa_count, (SELECT COUNT(*) FROM BL_3NF.ce_customers WHERE customer_id != ''-1'') as nf3_count) x',
 'ce_customers', 'sa_lms.src_lms,sa_oms.src_oms', 'PASS', '3NF', 'Verify all SA customers are loaded to 3NF'),

('products_sa_to_3nf_completeness', 'COMPLETENESS',
 'SELECT CASE WHEN sa_count = nf3_count THEN ''PASS'' ELSE ''FAIL'' END FROM (SELECT (SELECT COUNT(DISTINCT product_src_id) FROM (SELECT product_src_id FROM sa_lms.src_lms WHERE product_src_id IS NOT NULL UNION SELECT product_src_id FROM sa_oms.src_oms WHERE product_src_id IS NOT NULL) p) as sa_count, (SELECT COUNT(*) FROM BL_3NF.ce_products_scd WHERE product_id != ''-1'' and is_active=''Y'') as nf3_count) x',
 'ce_products_scd', 'sa_lms.src_lms,sa_oms.src_oms', 'PASS', '3NF', 'Verify all SA products are loaded to 3NF'),

('customers_3nf_to_dm_completeness', 'COMPLETENESS',
 'SELECT CASE WHEN nf3_count = dm_count THEN ''PASS'' ELSE ''FAIL'' END FROM (SELECT (SELECT COUNT(*) FROM BL_3NF.ce_customers WHERE customer_id != ''-1'') as nf3_count, (SELECT COUNT(*) FROM BL_DM.DIM_CUSTOMERS WHERE customer_surr_id != ''-1'') as dm_count) x',
 'DIM_CUSTOMERS', 'ce_customers', 'PASS', 'DM', 'Verify all 3NF customers are loaded to DM'),

('order_lines_sa_to_3nf_completeness', 'COMPLETENESS',
 'SELECT CASE WHEN sa_count = nf3_count THEN ''PASS'' ELSE ''FAIL'' END FROM (SELECT (SELECT COUNT(DISTINCT CONCAT(order_src_id, ''|'', product_src_id, ''|'', customer_src_id)) FROM sa_oms.src_oms WHERE order_src_id IS NOT NULL) as sa_count, (SELECT COUNT(*) FROM BL_3NF.ce_order_lines) as nf3_count) x',
 'ce_order_lines', 'sa_oms.src_oms', 'PASS', '3NF', 'Verify all SA order lines are loaded to 3NF');

-- 5. Test Execution Function
CREATE OR REPLACE FUNCTION execute_data_quality_tests(
    p_test_category VARCHAR(50) DEFAULT NULL,
    p_test_layer VARCHAR(20) DEFAULT NULL
)
RETURNS TABLE(
    test_name VARCHAR(100),
    test_result VARCHAR(20),
    expected_value VARCHAR(50),
    actual_value VARCHAR(100),
    execution_time_ms INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    test_record RECORD;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    execution_time_ms INT;
    actual_result VARCHAR(100);
    test_result VARCHAR(20);
    error_message TEXT;
BEGIN
    -- Loop through active tests
    FOR test_record IN
        SELECT t.test_id, t.test_name, t.test_sql, t.expected_result
        FROM data_quality_tests t
        WHERE t.is_active = TRUE
          AND (p_test_category IS NULL OR t.test_category = p_test_category)
          AND (p_test_layer IS NULL OR t.test_layer = p_test_layer)
        ORDER BY t.test_category, t.test_name
    LOOP
        start_time := clock_timestamp();
        error_message := NULL;

        BEGIN
            -- Execute the test query and get result
            EXECUTE test_record.test_sql INTO actual_result;

            -- Determine test result
            IF actual_result = test_record.expected_result THEN
                test_result := 'PASS';
            ELSE
                test_result := 'FAIL';
            END IF;

        EXCEPTION WHEN OTHERS THEN
            actual_result := 'ERROR';
            test_result := 'FAIL';
            error_message := SQLERRM;
        END;

        end_time := clock_timestamp();
        execution_time_ms := EXTRACT(milliseconds FROM (end_time - start_time))::INT;

        -- Log result
        INSERT INTO data_quality_test_results
        (test_id, test_result, actual_value, expected_value, execution_time_ms, error_message)
        VALUES
        (test_record.test_id, test_result, actual_result, test_record.expected_result, execution_time_ms, error_message);

        -- Return result for this test
        RETURN QUERY SELECT
            test_record.test_name,
            test_result,
            test_record.expected_result,
            actual_result,
            execution_time_ms;
    END LOOP;

    RETURN;
END;
$$;

-- 6. Create a simple view for test results summary
CREATE OR REPLACE VIEW v_test_results_summary AS
SELECT
    execution_date::date as test_date,
    test_result,
    COUNT(*) as test_count
FROM data_quality_test_results
GROUP BY execution_date::date, test_result
ORDER BY test_date DESC, test_result;

-- 7. Usage  Queries

-- Execute all tests
SELECT * FROM execute_data_quality_tests();

-- Execute only duplicate tests
-- SELECT * FROM execute_data_quality_tests('DUPLICATES');

-- Execute only DM layer tests
-- SELECT * FROM execute_data_quality_tests(NULL, 'DM');

-- View recent test results
-- SELECT * FROM data_quality_test_results ORDER BY execution_date DESC LIMIT 20;

-- View test summary by date
SELECT * FROM v_test_results_summary;

-- Check which tests are configured
SELECT test_name, test_category, test_layer, is_active FROM data_quality_tests ORDER BY test_category, test_name;

