-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - DM LAYER PROCEDURES
-- File: 02_Dimension_Procedures/Business_Entities/load_dim_warehouses.sql
-- Purpose: Load WAREHOUSES dimension from BL_3NF to BL_DM
-- Run as: dwh_cleansing_user
-- =====================================================

SELECT CURRENT_USER, SESSION_USER;

SET ROLE dwh_cleansing_user;
SET search_path = BL_CL, BL_3NF, BL_DM, public;

-- =====================================================
-- SECTION 1: UTILITY FUNCTIONS FOR DYNAMIC SQL
-- =====================================================

-- Function to build dynamic column mapping based on source system
CREATE OR REPLACE FUNCTION BL_CL.get_warehouse_column_mapping(
    p_source_system VARCHAR(50)
) RETURNS TEXT
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_column_mapping TEXT;
BEGIN
    -- Dynamic column mapping based on source system
    CASE p_source_system
        WHEN '3NF_LAYER' THEN v_column_mapping :=
                'warehouse_src_id, warehouse_name, ''3NF_LAYER'' as source_system, ''CE_WAREHOUSES'' as source_entity, ta_update_dt';
        WHEN 'LMS' THEN v_column_mapping :=
                'warehouse_src_id, warehouse_name, ''3NF_LAYER'' as source_system, ''CE_WAREHOUSES'' as source_entity, ta_update_dt';
        WHEN 'ALL' THEN v_column_mapping :=
                'warehouse_src_id, warehouse_name, ''3NF_LAYER'' as source_system, ''CE_WAREHOUSES'' as source_entity, ta_update_dt';
        ELSE v_column_mapping :=
                'warehouse_src_id, warehouse_name, ''3NF_LAYER'' as source_system, ''CE_WAREHOUSES'' as source_entity, ta_update_dt';
        END CASE;

    RETURN v_column_mapping;
END
$$;

-- Function to build dynamic WHERE clause for incremental loading
CREATE OR REPLACE FUNCTION BL_CL.build_warehouse_where_clause(
    p_config BL_CL.t_dim_load_config,
    p_last_update_dt TIMESTAMP DEFAULT NULL
) RETURNS TEXT
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_where_clause TEXT := 'WHERE warehouse_id != -1';
BEGIN
    -- Add source system filter if specified (filter on 3NF source systems, not DM target)
    IF p_config.include_source_system != 'ALL' THEN
        v_where_clause := v_where_clause || ' AND source_system = ' || quote_literal(
                CASE p_config.include_source_system
                    WHEN '3NF_LAYER' THEN 'LMS' -- Map DM filter to 3NF source
                    ELSE p_config.include_source_system
                    END
                                                                       );
    END IF;

    -- Add incremental loading filter for delta mode
    IF p_config.load_mode = 'DELTA' AND p_last_update_dt IS NOT NULL THEN
        v_where_clause := v_where_clause || ' AND ta_update_dt > ' || quote_literal(p_last_update_dt);
    END IF;

    RETURN v_where_clause;
END
$$;

-- =====================================================
-- SECTION 2: DATA VALIDATION FUNCTION
-- =====================================================

-- Function to validate warehouse data quality
CREATE OR REPLACE FUNCTION BL_CL.validate_warehouse_data(
    p_config BL_CL.t_dim_load_config
) RETURNS BL_CL.t_dim_validation_result[]
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_validations    BL_CL.t_dim_validation_result[] := ARRAY []::BL_CL.t_dim_validation_result[];
    v_validation     BL_CL.t_dim_validation_result;
    v_sql            TEXT;
    v_total_count    INTEGER;
    v_failed_count   INTEGER;
    v_sample_records TEXT;
BEGIN
    -- Get total count for validation base
    EXECUTE FORMAT('SELECT COUNT(*) FROM %s WHERE warehouse_id != -1', p_config.source_table)
        INTO v_total_count;

    -- Validation 1: Missing or Invalid Warehouse Names
    v_sql := FORMAT(
            'SELECT COUNT(*), STRING_AGG(DISTINCT warehouse_src_id::TEXT, '', '') FROM %s WHERE warehouse_id != -1 AND (warehouse_name IS NULL OR warehouse_name = '''' OR warehouse_name = ''Unknown'')',
            p_config.source_table);
    EXECUTE v_sql INTO v_failed_count, v_sample_records;

    v_validation := ROW (
        'Missing or Invalid Warehouse Names',
        v_failed_count,
        v_total_count,
        ROUND((v_failed_count::DECIMAL / NULLIF(v_total_count, 0)) * 100, 2),
        CASE
            WHEN v_failed_count > (v_total_count * 0.05) THEN 'ERROR'
            WHEN v_failed_count > 0 THEN 'WARNING'
            ELSE 'INFO' END,
        COALESCE(v_sample_records, 'None'),
        CASE
            WHEN v_failed_count > 0 THEN 'Review warehouse name data quality'
            ELSE 'Warehouse name validation passed' END
        )::BL_CL.t_dim_validation_result;
    v_validations := array_append(v_validations, v_validation);

    -- Validation 2: Missing Warehouse Source IDs
    v_sql := FORMAT(
            'SELECT COUNT(*), STRING_AGG(DISTINCT warehouse_id::TEXT, '', '') FROM %s WHERE warehouse_id != -1 AND (warehouse_src_id IS NULL OR warehouse_src_id = '''')',
            p_config.source_table);
    EXECUTE v_sql INTO v_failed_count, v_sample_records;

    v_validation := ROW (
        'Missing Warehouse Source IDs',
        v_failed_count,
        v_total_count,
        ROUND((v_failed_count::DECIMAL / NULLIF(v_total_count, 0)) * 100, 2),
        CASE
            WHEN v_failed_count > (v_total_count * 0.01) THEN 'ERROR'
            WHEN v_failed_count > 0 THEN 'WARNING'
            ELSE 'INFO' END,
        COALESCE(v_sample_records, 'None'),
        CASE
            WHEN v_failed_count > 0 THEN 'Critical: Warehouse source IDs are required for dimension'
            ELSE 'Warehouse source ID validation passed' END
        )::BL_CL.t_dim_validation_result;
    v_validations := array_append(v_validations, v_validation);

    -- Validation 3: Duplicate Warehouse Source IDs
    v_sql := FORMAT(
            'SELECT COUNT(*) - COUNT(DISTINCT warehouse_src_id), STRING_AGG(DISTINCT warehouse_src_id::TEXT, '', '') FROM (SELECT warehouse_src_id FROM %s WHERE warehouse_id != -1 GROUP BY warehouse_src_id HAVING COUNT(*) > 1) dups',
            p_config.source_table);
    EXECUTE v_sql INTO v_failed_count, v_sample_records;

    v_validation := ROW (
        'Duplicate Warehouse Source IDs',
        v_failed_count,
        v_total_count,
        ROUND((v_failed_count::DECIMAL / NULLIF(v_total_count, 0)) * 100, 2),
        CASE
            WHEN v_failed_count > 0 THEN 'WARNING'
            ELSE 'INFO' END,
        COALESCE(v_sample_records, 'None'),
        CASE
            WHEN v_failed_count > 0 THEN 'Review duplicate warehouse source IDs - may cause UPSERT issues'
            ELSE 'Warehouse uniqueness validation passed' END
        )::BL_CL.t_dim_validation_result;
    v_validations := array_append(v_validations, v_validation);

    RETURN v_validations;
END
$$;

-- =====================================================
-- SECTION 3: MAIN DIMENSION LOADING PROCEDURE
-- =====================================================

-- MAIN PROCEDURE: Load DIM_WAREHOUSES using composite types, dynamic SQL, and UPSERT
CREATE OR REPLACE PROCEDURE BL_CL.load_dim_warehouses(
    p_config BL_CL.t_dim_load_config DEFAULT NULL
)
    LANGUAGE plpgsql
AS
$$
DECLARE
    -- Composite type variables
    v_config            BL_CL.t_dim_load_config;
    v_result            BL_CL.t_dim_load_result;
    v_validations       BL_CL.t_dim_validation_result[];
    v_validation        BL_CL.t_dim_validation_result;

    -- Processing variables
    v_start_time        TIMESTAMP := CURRENT_TIMESTAMP;
    v_execution_time    INTEGER;
    v_last_update_dt    TIMESTAMP;
    v_sql               TEXT;
    v_where_clause      TEXT;
    v_column_mapping    TEXT;

    -- Result tracking
    v_rows_inserted     INTEGER   := 0;
    v_rows_updated      INTEGER   := 0;
    v_rows_unchanged    INTEGER   := 0;
    v_total_processed   INTEGER   := 0;
    v_validation_errors INTEGER   := 0;
    v_business_errors   INTEGER   := 0;

    -- Dynamic SQL components
    v_source_table      TEXT;
    v_target_table      TEXT;
    v_business_key      TEXT;
    v_surrogate_key     TEXT;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_dim_warehouses') THEN
        RAISE EXCEPTION 'Procedure load_dim_warehouses is already running';
    END IF;

    -- Initialize configuration with defaults if not provided
    v_config := COALESCE(p_config, ROW (
        'BL_3NF.CE_WAREHOUSES', -- source_table
        'BL_DM.DIM_WAREHOUSES', -- target_table
        'WAREHOUSE_SRC_ID', -- business_key_column
        'WAREHOUSE_SURR_ID', -- surrogate_key_column
        'DELTA', -- load_mode
        '3NF_LAYER', -- include_source_system
        'STRICT', -- validation_level
        0, -- batch_size (0 = unlimited)
        TRUE -- enable_logging
        )::BL_CL.t_dim_load_config);

    -- Extract config values for easier use
    v_source_table := v_config.source_table;
    v_target_table := v_config.target_table;
    v_business_key := v_config.business_key_column;
    v_surrogate_key := v_config.surrogate_key_column;

    -- Log procedure start
    IF v_config.enable_logging THEN
        CALL BL_CL.log_procedure_event(
                'load_dim_warehouses',
                v_source_table,
                v_target_table,
                'START',
                0,
                FORMAT('Starting DIM_WAREHOUSES load - Mode: %s, Source System: %s, Validation: %s',
                       v_config.load_mode, v_config.include_source_system, v_config.validation_level)
             );
    END IF;

    -- STEP 1: Data Validation (if enabled)
    IF v_config.validation_level IN ('STRICT', 'RELAXED') THEN
        v_validations := BL_CL.validate_warehouse_data(v_config);

        -- Process validation results
        FOR i IN 1..array_length(v_validations, 1)
            LOOP
                v_validation := v_validations[i];

                IF v_validation.severity = 'ERROR' AND v_config.validation_level = 'STRICT' THEN
                    v_validation_errors := v_validation_errors + v_validation.failed_count;

                    IF v_config.enable_logging THEN
                        CALL BL_CL.log_procedure_event(
                                'load_dim_warehouses', v_source_table, v_target_table, 'ERROR',
                                0, FORMAT('Validation failed: %s - %s failed records',
                                          v_validation.validation_rule, v_validation.failed_count)
                             );
                    END IF;
                ELSIF v_validation.severity IN ('ERROR', 'WARNING') AND v_config.enable_logging THEN
                    CALL BL_CL.log_procedure_event(
                            'load_dim_warehouses', v_source_table, v_target_table, 'WARNING',
                            0, FORMAT('Validation warning: %s - %s failed records',
                                      v_validation.validation_rule, v_validation.failed_count)
                         );
                END IF;
            END LOOP;

        -- Stop processing if validation errors in STRICT mode
        IF v_validation_errors > 0 AND v_config.validation_level = 'STRICT' THEN
            RAISE EXCEPTION 'Data validation failed with % errors in STRICT mode', v_validation_errors;
        END IF;
    END IF;

    -- STEP 2: Get last successful load time for delta processing
    IF v_config.load_mode = 'DELTA' THEN
        v_last_update_dt := BL_CL.get_last_successful_load('load_dim_warehouses');
    END IF;

    -- STEP 3: Build dynamic SQL components
    v_column_mapping := BL_CL.get_warehouse_column_mapping(v_config.include_source_system);
    v_where_clause := BL_CL.build_warehouse_where_clause(v_config, v_last_update_dt);

    -- STEP 4: Execute UPSERT operation using dynamic SQL (clean VARCHAR assignment)
    v_sql := FORMAT('
        WITH source_data AS (
            SELECT %s
            FROM %s
            %s
        ),
        upsert_result AS (
            INSERT INTO %s (
                %s, WAREHOUSE_NAME, SOURCE_SYSTEM, SOURCE_ENTITY,
                TA_INSERT_DT, TA_UPDATE_DT
            )
            SELECT
                warehouse_src_id,
                COALESCE(warehouse_name, ''Unknown''),
                source_system,
                source_entity,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            FROM source_data
            ON CONFLICT (%s, SOURCE_SYSTEM)
            DO UPDATE SET
                WAREHOUSE_NAME = EXCLUDED.WAREHOUSE_NAME,
                TA_UPDATE_DT = CURRENT_TIMESTAMP
            WHERE (
                %s.WAREHOUSE_NAME != EXCLUDED.WAREHOUSE_NAME
            )
            RETURNING
                CASE WHEN xmax::text::int = 0 THEN 1 ELSE 0 END as inserted,
                CASE WHEN xmax::text::int > 0 THEN 1 ELSE 0 END as updated
        )
        SELECT
            COALESCE(SUM(inserted), 0) as total_inserted,
            COALESCE(SUM(updated), 0) as total_updated,
            COUNT(*) as total_processed
        FROM upsert_result',
                    v_column_mapping, -- Source columns
                    v_source_table, -- Source table
                    v_where_clause, -- WHERE clause
                    v_target_table, -- Target table
                    v_business_key, -- Business key column
                    v_business_key, -- Conflict resolution key
                    v_target_table -- Table alias for UPDATE condition
             );

    -- Execute the dynamic UPSERT
    EXECUTE v_sql INTO v_rows_inserted, v_rows_updated, v_total_processed;

    -- Calculate unchanged records
    v_rows_unchanged := v_total_processed - v_rows_inserted - v_rows_updated;

    -- STEP 5: Calculate execution time and build result
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    v_result := ROW (
        v_rows_inserted,
        v_rows_updated,
        0, -- rows_deleted (not applicable for UPSERT)
        v_rows_unchanged,
        v_validation_errors,
        v_business_errors,
        v_execution_time,
        v_total_processed,
        v_start_time,
        CURRENT_TIMESTAMP,
        CASE WHEN v_validation_errors = 0 AND v_business_errors = 0 THEN 'SUCCESS' ELSE 'WARNING' END,
        FORMAT('DIM_WAREHOUSES load completed - Mode: %s, Inserted: %s, Updated: %s, Unchanged: %s',
               v_config.load_mode, v_rows_inserted, v_rows_updated, v_rows_unchanged),
        NULL
        )::BL_CL.t_dim_load_result;

    -- Log successful completion
    IF v_config.enable_logging THEN
        CALL BL_CL.log_procedure_event(
                'load_dim_warehouses',
                v_source_table,
                v_target_table,
                v_result.status,
                v_total_processed,
                v_result.message,
                v_execution_time
             );
    END IF;

    -- Release procedure lock
    PERFORM BL_CL.release_procedure_lock('load_dim_warehouses');

EXCEPTION
    WHEN OTHERS THEN
        -- Release procedure lock on error
        PERFORM BL_CL.release_procedure_lock('load_dim_warehouses');

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

        IF v_config.enable_logging THEN
            CALL BL_CL.log_procedure_event(
                    'load_dim_warehouses',
                    COALESCE(v_source_table, 'Unknown'),
                    COALESCE(v_target_table, 'Unknown'),
                    'ERROR',
                    0,
                    SQLERRM,
                    v_execution_time,
                    SQLSTATE
                 );
        END IF;

        RAISE;
END
$$;

-- =====================================================
-- SECTION 4: CONVENIENCE WRAPPER PROCEDURES
-- =====================================================

-- Simple wrapper for default delta loading
CREATE OR REPLACE PROCEDURE BL_CL.load_dim_warehouses_delta()
    LANGUAGE plpgsql
AS
$$
BEGIN
    CALL BL_CL.load_dim_warehouses(
            ROW (
                'BL_3NF.CE_WAREHOUSES', 'BL_DM.DIM_WAREHOUSES',
                'WAREHOUSE_SRC_ID', 'WAREHOUSE_SURR_ID',
                'DELTA', '3NF_LAYER', 'RELAXED', 0, TRUE
                )::BL_CL.t_dim_load_config
         );
END
$$;

-- Simple wrapper for full reload
CREATE OR REPLACE PROCEDURE BL_CL.load_dim_warehouses_full()
    LANGUAGE plpgsql
AS
$$
BEGIN
    CALL BL_CL.load_dim_warehouses(
            ROW (
                'BL_3NF.CE_WAREHOUSES', 'BL_DM.DIM_WAREHOUSES',
                'WAREHOUSE_SRC_ID', 'WAREHOUSE_SURR_ID',
                'FULL', 'ALL', 'STRICT', 0, TRUE
                )::BL_CL.t_dim_load_config
         );
END
$$;

-- =====================================================
-- SECTION 5: VERIFICATION QUERIES
-- =====================================================

-- Verify procedure creation
SELECT routine_name,
       routine_type,
       data_type                      as return_type,
       routine_definition IS NOT NULL as has_definition
FROM information_schema.routines
WHERE routine_schema = 'bl_cl'
  AND routine_name LIKE '%warehouse%'
ORDER BY routine_name;

-- Test the utility functions
SELECT BL_CL.get_warehouse_column_mapping('3NF_LAYER') as column_mapping_test;

SELECT BL_CL.build_warehouse_where_clause(
               ROW ('BL_3NF.CE_WAREHOUSES', 'BL_DM.DIM_WAREHOUSES', 'WAREHOUSE_SRC_ID', 'WAREHOUSE_SURR_ID',
                   'DELTA', 'LMS', 'STRICT', 0, TRUE)::BL_CL.t_dim_load_config,
               '2024-01-01'::TIMESTAMP
       ) as where_clause_test;

COMMIT;

-- =====================================================
-- READY FOR TESTING
-- Usage Examples:

-- CALL bl_cl.load_dim_warehouses_full();
-- --
-- -- 1. Default delta load:
-- -- CALL BL_CL.load_dim_warehouses_delta();
-- --
-- -- 2. Full reload:
-- -- CALL BL_CL.load_dim_warehouses_full();
-- --
-- -- 3. Check results:
-- SELECT COUNT(*) FROM BL_DM.DIM_WAREHOUSES;
-- SELECT * FROM BL_DM.DIM_WAREHOUSES LIMIT 10;
-- =====================================================