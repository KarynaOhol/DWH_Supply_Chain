-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - MASTER DIMENSION LOADER (FIXED)
-- File: 03_Master_Procedures/load_all_dm_dimensions.sql
-- Run as: dwh_cleansing_user
-- =====================================================

SELECT CURRENT_USER, SESSION_USER;

SET ROLE dwh_cleansing_user;
SET search_path = BL_CL, BL_3NF, BL_DM, public;

-- =====================================================
-- SECTION 1: MASTER DIMENSION CONFIGURATION TYPES
-- =====================================================

-- Master load configuration composite type
CREATE TYPE BL_CL.t_master_load_config AS (
    load_mode VARCHAR(20),              -- DELTA, FULL, SELECTIVE
    include_tables VARCHAR(1000),       -- ALL, or comma-separated table list
    parallel_execution BOOLEAN,         -- Enable parallel execution for independent dimensions
    stop_on_error BOOLEAN,             -- Stop entire process on first error
    validation_level VARCHAR(20),       -- STRICT, RELAXED, NONE
    max_parallel_jobs INTEGER,          -- Maximum concurrent dimension loads
    enable_logging BOOLEAN,             -- Enable detailed logging
    rollback_on_failure BOOLEAN        -- Rollback all changes on any failure
);

-- Dimension execution result
CREATE TYPE BL_CL.t_dimension_execution_result AS (
    dimension_name VARCHAR(100),
    execution_order INTEGER,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    execution_time_ms INTEGER,
    status VARCHAR(20),                 -- SUCCESS, WARNING, ERROR, SKIPPED
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_unchanged INTEGER,
    validation_errors INTEGER,
    error_message TEXT
);

-- Master execution summary
CREATE TYPE BL_CL.t_master_execution_summary AS (
    total_dimensions INTEGER,
    successful_loads INTEGER,
    failed_loads INTEGER,
    skipped_loads INTEGER,
    total_execution_time_ms INTEGER,
    total_records_processed INTEGER,
    total_records_inserted INTEGER,
    total_records_updated INTEGER,
    overall_status VARCHAR(20),
    start_time TIMESTAMP,
    end_time TIMESTAMP
);

-- =====================================================
-- SECTION 2: DIMENSION DEPENDENCY CONFIGURATION
-- =====================================================
--DROP FUNCTION IF EXISTS BL_CL.get_dimension_load_order();
-- Function to get dimension loading dependencies and order
CREATE OR REPLACE FUNCTION BL_CL.get_dimension_load_order()
RETURNS TABLE (
    load_group INTEGER,
    dimension_name TEXT,
    procedure_name TEXT,
    depends_on TEXT,
    can_run_parallel BOOLEAN
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH dimension_config AS (
        SELECT
            1 as load_group, 'Geography'::TEXT as dimension_name, 'load_dim_geographies'::TEXT as procedure_name,
            NULL::TEXT as depends_on, TRUE as can_run_parallel
        UNION ALL SELECT
            1, 'Warehouses'::TEXT, 'load_dim_warehouses'::TEXT,
            NULL::TEXT, TRUE
        UNION ALL SELECT
            1, 'Carriers'::TEXT, 'load_dim_carriers'::TEXT,
            NULL::TEXT, TRUE
        UNION ALL SELECT
            1, 'Order Statuses'::TEXT, 'load_dim_order_statuses'::TEXT,
            NULL::TEXT, TRUE
        UNION ALL SELECT
            1, 'Payment Methods'::TEXT, 'load_dim_payment_methods'::TEXT,
            NULL::TEXT, TRUE
        UNION ALL SELECT
            1, 'Shipping Modes'::TEXT, 'load_dim_shipping_modes'::TEXT,
            NULL::TEXT, TRUE
        UNION ALL SELECT
            1, 'Delivery Statuses'::TEXT, 'load_dim_delivery_statuses'::TEXT,
            NULL::TEXT, TRUE
        UNION ALL SELECT
            2, 'Products'::TEXT, 'load_dim_products_scd'::TEXT,
            'Geography'::TEXT, FALSE  -- SCD2 procedure, run separately
        UNION ALL SELECT
            2, 'Customers'::TEXT, 'load_dim_customers'::TEXT,
            'Geography'::TEXT, TRUE
        UNION ALL SELECT
            2, 'Sales Representatives'::TEXT, 'load_dim_sales_representatives'::TEXT,
            'Geography'::TEXT, TRUE
    )
    SELECT * FROM dimension_config
    ORDER BY load_group, dimension_name;
END $$;

-- =====================================================
-- SECTION 3: PARALLEL EXECUTION MANAGEMENT
-- =====================================================

-- Function to check if dimension procedure is currently running
CREATE OR REPLACE FUNCTION BL_CL.is_dimension_procedure_running(
    p_procedure_name VARCHAR(100)
) RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_lock_exists BOOLEAN := FALSE;
BEGIN
    -- Check if procedure lock exists
    SELECT EXISTS(
        SELECT 1 FROM BL_CL.mta_process_locks
        WHERE procedure_name = p_procedure_name
--         AND is_locked = TRUE
    ) INTO v_lock_exists;

    RETURN v_lock_exists;
END $$;

-- Function to wait for dimension to complete
CREATE OR REPLACE FUNCTION BL_CL.wait_for_dimension_completion(
    p_procedure_name VARCHAR(100),
    p_max_wait_seconds INTEGER DEFAULT 3600
) RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_wait_count INTEGER := 0;
    v_max_iterations INTEGER := p_max_wait_seconds / 5; -- Check every 5 seconds
BEGIN
    WHILE BL_CL.is_dimension_procedure_running(p_procedure_name) AND v_wait_count < v_max_iterations LOOP
        PERFORM pg_sleep(5);
        v_wait_count := v_wait_count + 1;
    END LOOP;

    RETURN NOT BL_CL.is_dimension_procedure_running(p_procedure_name);
END $$;

-- =====================================================
-- SECTION 4: MAIN MASTER LOADING PROCEDURE
-- =====================================================

-- MASTER PROCEDURE: Load All DM Layer Dimensions


CREATE OR REPLACE PROCEDURE BL_CL.load_all_dm_dimensions(
    p_config BL_CL.t_master_load_config DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    -- Configuration and tracking
    v_config BL_CL.t_master_load_config;
    v_master_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_execution_results BL_CL.t_dimension_execution_result[] := ARRAY[]::BL_CL.t_dimension_execution_result[];
    v_execution_result BL_CL.t_dimension_execution_result;
    v_summary BL_CL.t_master_execution_summary;

    -- Processing variables
    v_dimension_record RECORD;
    v_current_group INTEGER := 0;
    v_group_start_time TIMESTAMP;
    v_dimension_start_time TIMESTAMP;
    v_dimension_end_time TIMESTAMP;
    v_execution_time INTEGER;

    -- SQL execution
    v_sql TEXT;
    v_include_tables TEXT[];
    v_should_load_dimension BOOLEAN;
    v_actual_procedure_name TEXT;

    -- Counters
    v_total_dimensions INTEGER := 0;
    v_successful_loads INTEGER := 0;
    v_failed_loads INTEGER := 0;
    v_skipped_loads INTEGER := 0;
    v_total_processed INTEGER := 0;
    v_total_inserted INTEGER := 0;
    v_total_updated INTEGER := 0;

    -- Error handling
    v_error_occurred BOOLEAN := FALSE;
    v_error_message TEXT;
BEGIN
    -- Initialize configuration with defaults
    v_config := COALESCE(p_config, ROW(
        'DELTA',        -- load_mode
        'ALL',          -- include_tables
        TRUE,           -- parallel_execution
        FALSE,          -- stop_on_error
        'RELAXED',      -- validation_level
        3,              -- max_parallel_jobs
        TRUE,           -- enable_logging
        FALSE           -- rollback_on_failure
    )::BL_CL.t_master_load_config);

    -- Note: Savepoint handling removed - will be handled at transaction level if needed

    -- Parse include_tables if selective loading
    IF v_config.include_tables != 'ALL' THEN
        v_include_tables := string_to_array(replace(v_config.include_tables, ' ', ''), ',');
    END IF;

    -- Log master procedure start
    IF v_config.enable_logging THEN
        CALL BL_CL.log_procedure_event(
            'load_all_dm_dimensions', 'MULTIPLE', 'MULTIPLE', 'START', 0,
            FORMAT('Starting master dimension load - Mode: %s, Tables: %s, Parallel: %s',
                   v_config.load_mode, v_config.include_tables, v_config.parallel_execution)
        );
    END IF;

    -- MAIN PROCESSING LOOP: Process each load group
    FOR v_dimension_record IN
        SELECT * FROM BL_CL.get_dimension_load_order() ORDER BY load_group, dimension_name
    LOOP
        v_total_dimensions := v_total_dimensions + 1;

        -- Check if this dimension should be loaded (selective loading)
        v_should_load_dimension := TRUE;
        IF v_config.include_tables != 'ALL' THEN
            v_should_load_dimension := v_dimension_record.dimension_name = ANY(v_include_tables) OR
                                     v_dimension_record.procedure_name = ANY(v_include_tables);
        END IF;

        -- Skip if not in selective list
        IF NOT v_should_load_dimension THEN
            v_execution_result := ROW(
                v_dimension_record.dimension_name, v_dimension_record.load_group,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0,
                'SKIPPED', 0, 0, 0, 0, 0,
                'Dimension not included in selective load list'
            )::BL_CL.t_dimension_execution_result;

            v_execution_results := array_append(v_execution_results, v_execution_result);
            v_skipped_loads := v_skipped_loads + 1;
            CONTINUE;
        END IF;

        -- Wait for new load group (handle dependencies)
        IF v_current_group != v_dimension_record.load_group THEN
            v_current_group := v_dimension_record.load_group;
            v_group_start_time := CURRENT_TIMESTAMP;

            IF v_config.enable_logging THEN
                CALL BL_CL.log_procedure_event(
                    'load_all_dm_dimensions', 'MULTIPLE', 'MULTIPLE', 'INFO', 0,
                    FORMAT('Starting load group %s', v_current_group)
                );
            END IF;
        END IF;

        -- Determine actual procedure name based on load mode
        IF v_config.load_mode = 'FULL' THEN
            v_actual_procedure_name := v_dimension_record.procedure_name || '_full';
        ELSE  -- DELTA mode
            v_actual_procedure_name := v_dimension_record.procedure_name || '_delta';
        END IF;

        -- Execute dimension load
        v_dimension_start_time := CURRENT_TIMESTAMP;

        BEGIN
            -- Build dynamic procedure call to wrapper procedures (no parameters needed)
            v_sql := FORMAT('CALL BL_CL.%s()', v_actual_procedure_name);

            -- Execute the dimension load procedure
            EXECUTE v_sql;

            v_dimension_end_time := CURRENT_TIMESTAMP;
            v_execution_time := EXTRACT(EPOCH FROM (v_dimension_end_time - v_dimension_start_time)) * 1000;

            -- Record successful execution (simplified - in production you'd get actual counts)
            v_execution_result := ROW(
                v_dimension_record.dimension_name, v_dimension_record.load_group,
                v_dimension_start_time, v_dimension_end_time, v_execution_time,
                'SUCCESS', 0, 0, 0, 0, 0,
                FORMAT('Dimension loaded successfully using %s', v_actual_procedure_name)
            )::BL_CL.t_dimension_execution_result;

            v_successful_loads := v_successful_loads + 1;

        EXCEPTION WHEN OTHERS THEN
            v_dimension_end_time := CURRENT_TIMESTAMP;
            v_execution_time := EXTRACT(EPOCH FROM (v_dimension_end_time - v_dimension_start_time)) * 1000;
            v_error_message := SQLERRM;
            v_error_occurred := TRUE;

            -- Record failed execution
            v_execution_result := ROW(
                v_dimension_record.dimension_name, v_dimension_record.load_group,
                v_dimension_start_time, v_dimension_end_time, v_execution_time,
                'ERROR', 0, 0, 0, 0, 0,
                FORMAT('Failed calling %s: %s', v_actual_procedure_name, v_error_message)
            )::BL_CL.t_dimension_execution_result;

            v_failed_loads := v_failed_loads + 1;

            -- Log error
            IF v_config.enable_logging THEN
                CALL BL_CL.log_procedure_event(
                    'load_all_dm_dimensions', 'MULTIPLE', 'MULTIPLE', 'ERROR', 0,
                    FORMAT('Failed to load dimension %s using %s: %s', v_dimension_record.dimension_name, v_actual_procedure_name, v_error_message)
                );
            END IF;

            -- Stop on error if configured
            IF v_config.stop_on_error THEN
                EXIT;
            END IF;
        END;

        -- Add result to array
        v_execution_results := array_append(v_execution_results, v_execution_result);

        -- Log individual dimension completion
        IF v_config.enable_logging THEN
            CALL BL_CL.log_procedure_event(
                'load_all_dm_dimensions', 'MULTIPLE', 'MULTIPLE',
                v_execution_result.status, 0,
                FORMAT('Completed %s: %s (%s ms) using %s',
                       v_dimension_record.dimension_name,
                       v_execution_result.status,
                       v_execution_time,
                       v_actual_procedure_name)
            );
        END IF;
    END LOOP;

    -- Build execution summary
    v_summary := ROW(
        v_total_dimensions,
        v_successful_loads,
        v_failed_loads,
        v_skipped_loads,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_master_start_time)) * 1000,
        v_total_processed,
        v_total_inserted,
        v_total_updated,
        CASE
            WHEN v_failed_loads = 0 THEN 'SUCCESS'
            WHEN v_successful_loads > 0 THEN 'PARTIAL_SUCCESS'
            ELSE 'FAILED'
        END,
        v_master_start_time,
        CURRENT_TIMESTAMP
    )::BL_CL.t_master_execution_summary;

    -- Log completion
    IF v_config.enable_logging THEN
        CALL BL_CL.log_procedure_event(
            'load_all_dm_dimensions', 'MULTIPLE', 'MULTIPLE',
            v_summary.overall_status, v_total_dimensions,
            FORMAT('Master load completed - Success: %s, Failed: %s, Skipped: %s, Total Time: %s ms',
                   v_successful_loads, v_failed_loads, v_skipped_loads, v_summary.total_execution_time_ms)
        );
    END IF;

    -- Handle rollback on failure (note: simplified without savepoints)
    IF v_error_occurred AND v_config.rollback_on_failure THEN
        -- Note: In a real implementation, rollback would be handled at the calling transaction level
        RAISE EXCEPTION 'Master dimension load failed. Failed loads: %. Use transaction-level rollback if needed.', v_failed_loads;
    END IF;

    -- Output execution summary (for monitoring/reporting)
    RAISE NOTICE 'MASTER DIMENSION LOAD SUMMARY:';
    RAISE NOTICE 'Total Dimensions: %, Successful: %, Failed: %, Skipped: %',
                 v_total_dimensions, v_successful_loads, v_failed_loads, v_skipped_loads;
    RAISE NOTICE 'Overall Status: %, Total Execution Time: % ms',
                 v_summary.overall_status, v_summary.total_execution_time_ms;

EXCEPTION WHEN OTHERS THEN
    -- Handle unexpected errors
    IF v_config.enable_logging THEN
        CALL BL_CL.log_procedure_event(
            'load_all_dm_dimensions', 'MULTIPLE', 'MULTIPLE', 'ERROR', 0,
            FORMAT('Master dimension load failed with unexpected error: %s', SQLERRM)
        );
    END IF;

    RAISE;
END $$;

-- =====================================================
-- CONVENIENCE WRAPPER PROCEDURES
-- =====================================================

-- Delta load all dimensions
CREATE OR REPLACE PROCEDURE BL_CL.load_all_dimensions_delta()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL BL_CL.load_all_dm_dimensions(
        ROW('DELTA', 'ALL', TRUE, FALSE, 'RELAXED', 3, TRUE, FALSE)::BL_CL.t_master_load_config
    );
END $$;

-- Full reload all dimensions
CREATE OR REPLACE PROCEDURE BL_CL.load_all_dimensions_full()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL BL_CL.load_all_dm_dimensions(
        ROW('FULL', 'ALL', TRUE, FALSE, 'STRICT', 3, TRUE, FALSE)::BL_CL.t_master_load_config
    );
END $$;

-- Safe full reload (simplified without savepoints)
CREATE OR REPLACE PROCEDURE BL_CL.load_all_dimensions_full_safe()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL BL_CL.load_all_dm_dimensions(
        ROW('FULL', 'ALL', FALSE, TRUE, 'STRICT', 1, TRUE, FALSE)::BL_CL.t_master_load_config
    );
END $$;

-- Load specific dimensions only
CREATE OR REPLACE PROCEDURE BL_CL.load_dimensions_selective(
    p_dimension_list TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    CALL BL_CL.load_all_dm_dimensions(
        ROW('DELTA', p_dimension_list, FALSE, FALSE, 'RELAXED', 1, TRUE, FALSE)::BL_CL.t_master_load_config
    );
END $$;

COMMIT;