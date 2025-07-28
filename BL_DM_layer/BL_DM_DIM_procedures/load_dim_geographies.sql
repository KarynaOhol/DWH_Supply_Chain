-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - DM LAYER PROCEDURES
-- File: 02_Dimension_Procedures/Geographic_Hierarchy/load_dim_geographies.sql
-- Purpose: Load GEOGRAPHIES dimension from BL_3NF to BL_DM with hierarchy flattening
-- Requirements: Composite Types ✅, Cursor FOR Loop ✅, Dynamic SQL ✅, UPSERT ✅
-- Technical Features: Hierarchy flattening, row-by-row processing, complex transformations
-- Run as: dwh_cleansing_user
-- =====================================================

SELECT CURRENT_USER, SESSION_USER;

SET ROLE dwh_cleansing_user;
SET search_path = BL_CL, BL_3NF, BL_DM, public;

-- =====================================================
-- SECTION 1: UTILITY FUNCTIONS FOR GEOGRAPHY HIERARCHY
-- =====================================================

-- Function to build geography hierarchy from 3NF normalized structure
CREATE OR REPLACE FUNCTION BL_CL.get_geography_hierarchy_data(
    p_config BL_CL.t_dim_load_config,
    p_last_update_dt TIMESTAMP DEFAULT NULL
) RETURNS SETOF BL_CL.t_geography_hierarchy
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_sql          TEXT;
    v_where_clause TEXT := 'WHERE g.geography_id != -1';
BEGIN
    -- Add source system filter
    IF p_config.include_source_system != 'ALL' THEN
        v_where_clause := v_where_clause || ' AND g.source_system = ' || quote_literal(
                CASE p_config.include_source_system
                    WHEN '3NF_LAYER' THEN 'LMS' -- Map DM filter to 3NF source
                    ELSE p_config.include_source_system
                    END
                                                                         );
    END IF;

    -- Add incremental loading filter for delta mode
    IF p_config.load_mode = 'DELTA' AND p_last_update_dt IS NOT NULL THEN
        v_where_clause := v_where_clause || ' AND g.ta_update_dt > ' || quote_literal(p_last_update_dt);
    END IF;

    -- Build dynamic query to get complete geography hierarchy
    v_sql := FORMAT('
        SELECT
            g.geography_src_id::VARCHAR(50) as geography_src_id,
            COALESCE(c.city_name, ''Unknown''::VARCHAR(100))::VARCHAR(100) as city_name,
            COALESCE(c.city_src_id, ''Unknown'')::VARCHAR(50) as city_src_id,
            COALESCE(s.state_name, ''Unknown''::VARCHAR(100))::VARCHAR(100) as state_name,
            COALESCE(s.state_src_id, ''Unknown'')::VARCHAR(50) as state_src_id,
            COALESCE(s.state_code, ''UNK''::VARCHAR(10))::VARCHAR(10) as state_code,
            COALESCE(co.country_name, ''Unknown''::VARCHAR(100))::VARCHAR(100) as country_name,
            COALESCE(co.country_src_id, ''Unknown'')::VARCHAR(50) as country_src_id,
            COALESCE(co.country_code, ''UNK''::VARCHAR(10))::VARCHAR(10) as country_code,
            COALESCE(r.region_name, ''Unknown''::VARCHAR(100))::VARCHAR(100) as region_name,
            COALESCE(r.region_src_id, ''Unknown'')::VARCHAR(50) as region_src_id,
            ''3NF_LAYER''::VARCHAR(50) as source_system,
            ''CE_GEOGRAPHIES''::VARCHAR(100) as source_entity
        FROM %s.CE_GEOGRAPHIES g
        LEFT JOIN %s.CE_CITIES c ON g.city_id = c.city_id
        LEFT JOIN %s.CE_STATES s ON c.state_id = s.state_id
        LEFT JOIN %s.CE_COUNTRIES co ON s.country_id = co.country_id
        LEFT JOIN %s.CE_REGIONS r ON co.region_id = r.region_id
        %s
        ORDER BY g.geography_src_id',
                    p_config.source_table,
                    p_config.source_table,
                    p_config.source_table,
                    p_config.source_table,
                    p_config.source_table,
                    v_where_clause
             );

    -- Replace schema references properly
    v_sql := REPLACE(v_sql, p_config.source_table || '.', 'BL_3NF.');

    RETURN QUERY EXECUTE v_sql;
END
$$;

-- Function to validate geography hierarchy completeness
CREATE OR REPLACE FUNCTION BL_CL.validate_geography_hierarchy(
    p_hierarchy BL_CL.t_geography_hierarchy
) RETURNS BL_CL.t_dim_validation_result
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_validation     BL_CL.t_dim_validation_result;
    v_missing_levels TEXT[]      := ARRAY []::TEXT[];
    v_severity       VARCHAR(10) := 'INFO';
    v_message        TEXT;
BEGIN
    -- Check for missing hierarchy levels
    -- FIXED: Updated validation logic to handle business key patterns
    IF p_hierarchy.city_name = 'Unknown' OR p_hierarchy.city_src_id IS NULL OR p_hierarchy.city_src_id = '' THEN
        v_missing_levels := array_append(v_missing_levels, 'City');
        v_severity := 'WARNING';
    END IF;

    IF p_hierarchy.state_name = 'Unknown' OR p_hierarchy.state_src_id IS NULL OR p_hierarchy.state_src_id = '' THEN
        v_missing_levels := array_append(v_missing_levels, 'State');
        v_severity := 'WARNING';
    END IF;

    IF p_hierarchy.country_name = 'Unknown' OR p_hierarchy.country_src_id IS NULL OR
       p_hierarchy.country_src_id = '' THEN
        v_missing_levels := array_append(v_missing_levels, 'Country');
        v_severity := 'ERROR'; -- Country is critical
    END IF;

    IF p_hierarchy.region_name = 'Unknown' OR p_hierarchy.region_src_id = '-1' THEN
        v_missing_levels := array_append(v_missing_levels, 'Region');
        v_severity := 'WARNING';
    END IF;

    -- Build validation result
    IF array_length(v_missing_levels, 1) > 0 THEN
        v_message := 'Missing hierarchy levels: ' || array_to_string(v_missing_levels, ', ');
    ELSE
        v_message := 'Complete geography hierarchy';
        v_severity := 'INFO';
    END IF;

    v_validation := ROW (
        'Geography Hierarchy Completeness',
        CASE WHEN array_length(v_missing_levels, 1) > 0 THEN 1 ELSE 0 END,
        1,
        CASE WHEN array_length(v_missing_levels, 1) > 0 THEN 100.00 ELSE 0.00 END,
        v_severity,
        p_hierarchy.geography_src_id,
        v_message
        )::BL_CL.t_dim_validation_result;

    RETURN v_validation;
END
$$;

-- Function to build dynamic UPSERT statement for geographies
CREATE OR REPLACE FUNCTION BL_CL.build_geography_upsert_sql(
    p_config BL_CL.t_dim_load_config
) RETURNS TEXT
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_upsert_sql TEXT;
BEGIN
    v_upsert_sql := FORMAT('
        INSERT INTO %s (
            GEOGRAPHY_SRC_ID, CITY_NAME, CITY_SRC_ID, STATE_NAME, STATE_SRC_ID, STATE_CODE,
            COUNTRY_NAME, COUNTRY_SRC_ID, COUNTRY_CODE, REGION_NAME, REGION_SRC_ID,
            SOURCE_SYSTEM, SOURCE_ENTITY, TA_INSERT_DT, TA_UPDATE_DT
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (GEOGRAPHY_SRC_ID, SOURCE_SYSTEM)
        DO UPDATE SET
            CITY_NAME = EXCLUDED.CITY_NAME,
            CITY_SRC_ID = EXCLUDED.CITY_SRC_ID,
            STATE_NAME = EXCLUDED.STATE_NAME,
            STATE_SRC_ID = EXCLUDED.STATE_SRC_ID,
            STATE_CODE = EXCLUDED.STATE_CODE,
            COUNTRY_NAME = EXCLUDED.COUNTRY_NAME,
            COUNTRY_SRC_ID = EXCLUDED.COUNTRY_SRC_ID,
            COUNTRY_CODE = EXCLUDED.COUNTRY_CODE,
            REGION_NAME = EXCLUDED.REGION_NAME,
            REGION_SRC_ID = EXCLUDED.REGION_SRC_ID,
            TA_UPDATE_DT = EXCLUDED.TA_UPDATE_DT
        WHERE (
            %s.CITY_NAME != EXCLUDED.CITY_NAME OR
            %s.STATE_NAME != EXCLUDED.STATE_NAME OR
            %s.COUNTRY_NAME != EXCLUDED.COUNTRY_NAME OR
            %s.REGION_NAME != EXCLUDED.REGION_NAME OR
            %s.STATE_CODE != EXCLUDED.STATE_CODE OR
            %s.COUNTRY_CODE != EXCLUDED.COUNTRY_CODE
        )',
                           p_config.target_table,
                           p_config.target_table, p_config.target_table, p_config.target_table,
                           p_config.target_table, p_config.target_table, p_config.target_table
                    );

    RETURN v_upsert_sql;
END
$$;

-- =====================================================
-- SECTION 2: MAIN DIMENSION LOADING PROCEDURE WITH CURSOR FOR LOOP
-- =====================================================
-- MAIN PROCEDURE: Load DIM_GEOGRAPHIES using cursor FOR loop for hierarchy processing
CREATE OR REPLACE PROCEDURE BL_CL.load_dim_geographies(
    p_config BL_CL.t_dim_load_config DEFAULT NULL
)
    LANGUAGE plpgsql
AS
$$
DECLARE
    -- Composite type variables
    v_config               BL_CL.t_dim_load_config;
    v_result               BL_CL.t_dim_load_result;
    v_validations          BL_CL.t_dim_validation_result[] := ARRAY []::BL_CL.t_dim_validation_result[];
    v_validation           BL_CL.t_dim_validation_result;

    -- Cursor FOR loop variables
    geography_rec          BL_CL.t_geography_hierarchy;

    -- Processing variables
    v_start_time           TIMESTAMP                       := CURRENT_TIMESTAMP;
    v_execution_time       INTEGER;
    v_last_update_dt       TIMESTAMP;
    v_upsert_sql           TEXT;
    v_batch_counter        INTEGER                         := 0;

    -- Result tracking
    v_rows_inserted        INTEGER                         := 0;
    v_rows_updated         INTEGER                         := 0;
    v_rows_unchanged       INTEGER                         := 0;
    v_total_processed      INTEGER                         := 0;
    v_validation_errors    INTEGER                         := 0;
    v_business_errors      INTEGER                         := 0;
    v_hierarchy_warnings   INTEGER                         := 0;
    v_total_actual_changes INTEGER                         := 0;

    -- Dynamic execution variables
    v_upsert_result        RECORD;
    v_affected_rows        INTEGER;
    v_record_exists        BOOLEAN;
    v_was_insert           BOOLEAN;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_dim_geographies') THEN
        RAISE EXCEPTION 'Procedure load_dim_geographies is already running';
    END IF;

    -- Initialize configuration with defaults
    v_config := COALESCE(p_config, ROW (
        'BL_3NF.CE_GEOGRAPHIES', -- source_table
        'BL_DM.DIM_GEOGRAPHIES', -- target_table
        'GEOGRAPHY_SRC_ID', -- business_key_column
        'GEOGRAPHY_SURR_ID', -- surrogate_key_column
        'DELTA', -- load_mode
        '3NF_LAYER', -- include_source_system
        'STRICT', -- validation_level
        1000, -- batch_size (for cursor processing)
        TRUE -- enable_logging
        )::BL_CL.t_dim_load_config);

    -- Log procedure start
    IF v_config.enable_logging THEN
        CALL BL_CL.log_procedure_event(
                'load_dim_geographies',
                v_config.source_table,
                v_config.target_table,
                'START',
                0,
                FORMAT(
                        'Starting DIM_GEOGRAPHIES load with CURSOR FOR LOOP - Mode: %s, Source System: %s, Batch Size: %s',
                        v_config.load_mode, v_config.include_source_system, v_config.batch_size)
             );
    END IF;

    -- Get last successful load time for delta processing
    IF v_config.load_mode = 'DELTA' THEN
        v_last_update_dt := BL_CL.get_last_successful_load('load_dim_geographies');
    END IF;

    -- Prepare dynamic UPSERT SQL
    v_upsert_sql := BL_CL.build_geography_upsert_sql(v_config);

    -- MAIN PROCESSING: Cursor FOR loop over geography hierarchy
    FOR geography_rec IN
        SELECT * FROM BL_CL.get_geography_hierarchy_data(v_config, v_last_update_dt)
        LOOP
            -- Increment counters
            v_total_processed := v_total_processed + 1;
            v_batch_counter := v_batch_counter + 1;

            -- STEP 1: Validate geography hierarchy (if validation enabled)
            IF v_config.validation_level IN ('STRICT', 'RELAXED') THEN
                v_validation := BL_CL.validate_geography_hierarchy(geography_rec);

                -- Track validation issues
                IF v_validation.severity = 'ERROR' THEN
                    v_validation_errors := v_validation_errors + 1;
                    v_validations := array_append(v_validations, v_validation);

                    -- Skip processing this record in STRICT mode
                    IF v_config.validation_level = 'STRICT' THEN
                        IF v_config.enable_logging THEN
                            CALL BL_CL.log_procedure_event(
                                    'load_dim_geographies', v_config.source_table, v_config.target_table, 'WARNING',
                                    0, FORMAT('Skipping geography %s due to validation error: %s',
                                              geography_rec.geography_src_id, v_validation.recommendation)
                                 );
                        END IF;
                        CONTINUE; -- Skip to next record
                    END IF;
                ELSIF v_validation.severity = 'WARNING' THEN
                    v_hierarchy_warnings := v_hierarchy_warnings + 1;
                END IF;
            END IF;

            -- STEP 2: Check if record exists before UPSERT
            SELECT EXISTS(SELECT 1
                          FROM BL_DM.DIM_GEOGRAPHIES
                          WHERE GEOGRAPHY_SRC_ID = geography_rec.geography_src_id
                            AND SOURCE_SYSTEM = geography_rec.source_system)
            INTO v_record_exists;

            -- Execute UPSERT for current geography record
            BEGIN
                EXECUTE v_upsert_sql USING
                    geography_rec.geography_src_id, -- $1
                    geography_rec.city_name, -- $2
                    geography_rec.city_src_id, -- $3
                    geography_rec.state_name, -- $4
                    geography_rec.state_src_id, -- $5
                    geography_rec.state_code, -- $6
                    geography_rec.country_name, -- $7
                    geography_rec.country_src_id, -- $8
                    geography_rec.country_code, -- $9
                    geography_rec.region_name, -- $10
                    geography_rec.region_src_id, -- $11
                    geography_rec.source_system, -- $12
                    geography_rec.source_entity, -- $13
                    CURRENT_TIMESTAMP, -- $14
                    CURRENT_TIMESTAMP; -- $15

                GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

                -- Determine if insert or update based on affected rows
                IF v_affected_rows > 0 THEN
                    v_was_insert := NOT v_record_exists;

                    IF v_was_insert THEN
                        v_rows_inserted := v_rows_inserted + 1;
                        v_total_actual_changes := v_total_actual_changes + 1;
                    ELSE
                        -- This was an update - but was there an actual change?
                        -- PostgreSQL UPSERT only increments ROW_COUNT if there was a real change
                        v_rows_updated := v_rows_updated + 1;
                        v_total_actual_changes := v_total_actual_changes + 1;
                    END IF;
                ELSE
                    -- No rows affected means no change was needed (record exists and is identical)
                    v_rows_unchanged := v_rows_unchanged + 1;
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    v_business_errors := v_business_errors + 1;

                    IF v_config.enable_logging THEN
                        CALL BL_CL.log_procedure_event(
                                'load_dim_geographies', v_config.source_table, v_config.target_table, 'ERROR',
                                0, FORMAT('Error processing geography %s: %s', geography_rec.geography_src_id, SQLERRM)
                             );
                    END IF;

                    -- In STRICT mode, re-raise the error; in RELAXED mode, continue
                    IF v_config.validation_level = 'STRICT' THEN
                        RAISE;
                    END IF;
            END;

            -- STEP 3: Batch processing checkpoint (if batch size specified)
            IF v_config.batch_size > 0 AND v_batch_counter >= v_config.batch_size THEN
                IF v_config.enable_logging THEN
                    CALL BL_CL.log_procedure_event(
                            'load_dim_geographies', v_config.source_table, v_config.target_table, 'INFO',
                            v_total_actual_changes,
                            FORMAT('Processed batch of %s records. Total processed: %s, Total changes: %s',
                                   v_batch_counter, v_total_processed, v_total_actual_changes)
                         );
                END IF;
                v_batch_counter := 0; -- Reset batch counter
            END IF;

        END LOOP;
    -- End of cursor FOR loop

    -- STEP 4: Calculate execution time and build result
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
        CASE
            WHEN v_validation_errors = 0 AND v_business_errors = 0 THEN 'SUCCESS'
            WHEN v_business_errors = 0 THEN 'WARNING'
            ELSE 'ERROR'
            END,
        FORMAT(
                'DIM_GEOGRAPHIES load completed using CURSOR FOR LOOP - Mode: %s, Processed: %s, Inserted: %s, Updated: %s, Warnings: %s, Errors: %s',
                v_config.load_mode, v_total_processed, v_rows_inserted, v_rows_updated, v_hierarchy_warnings,
                v_validation_errors + v_business_errors),
        CASE
            WHEN array_length(v_validations, 1) > 0 THEN
                'Validation issues: ' ||
                array_to_string(ARRAY(SELECT v.validation_rule FROM unnest(v_validations) v), ', ')
            ELSE NULL END
        )::BL_CL.t_dim_load_result;

    -- Log successful completion
    IF v_config.enable_logging THEN
        CALL BL_CL.log_procedure_event(
                'load_dim_geographies',
                v_config.source_table,
                v_config.target_table,
                v_result.status,
                v_total_actual_changes,
                v_result.message,
                v_execution_time
             );
    END IF;

    -- Release procedure lock
    PERFORM BL_CL.release_procedure_lock('load_dim_geographies');

EXCEPTION
    WHEN OTHERS THEN
        -- Release procedure lock on error
        PERFORM BL_CL.release_procedure_lock('load_dim_geographies');

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

        IF v_config.enable_logging THEN
            CALL BL_CL.log_procedure_event(
                    'load_dim_geographies',
                    COALESCE(v_config.source_table, 'Unknown'),
                    COALESCE(v_config.target_table, 'Unknown'),
                    'ERROR',
                    v_total_actual_changes,
                    SQLERRM,
                    v_execution_time,
                    SQLSTATE
                 );
        END IF;

        RAISE;
END
$$;

-- =====================================================
-- SECTION 3: CONVENIENCE WRAPPER PROCEDURES
-- =====================================================

-- Simple wrapper for default delta loading
CREATE OR REPLACE PROCEDURE BL_CL.load_dim_geographies_delta()
    LANGUAGE plpgsql
AS
$$
BEGIN
    CALL BL_CL.load_dim_geographies(
            ROW (
                'BL_3NF.CE_GEOGRAPHIES', 'BL_DM.DIM_GEOGRAPHIES',
                'GEOGRAPHY_SRC_ID', 'GEOGRAPHY_SURR_ID',
                'DELTA', '3NF_LAYER', 'RELAXED', 1000, TRUE
                )::BL_CL.t_dim_load_config
         );
END
$$;

-- Simple wrapper for full reload
CREATE OR REPLACE PROCEDURE BL_CL.load_dim_geographies_full()
    LANGUAGE plpgsql
AS
$$
BEGIN
    CALL BL_CL.load_dim_geographies(
            ROW (
                'BL_3NF.CE_GEOGRAPHIES', 'BL_DM.DIM_GEOGRAPHIES',
                'GEOGRAPHY_SRC_ID', 'GEOGRAPHY_SURR_ID',
                'FULL', 'ALL', 'STRICT', 500, TRUE
                )::BL_CL.t_dim_load_config
         );
END
$$;

-- =====================================================
-- SECTION 4: VERIFICATION QUERIES
-- =====================================================

-- Verify procedure creation
SELECT routine_name,
       routine_type,
       data_type                      as return_type,
       routine_definition IS NOT NULL as has_definition
FROM information_schema.routines
WHERE routine_schema = 'bl_cl'
  AND routine_name LIKE '%geograph%'
ORDER BY routine_name;

-- Test the hierarchy data function
SELECT
    geography_src_id, city_name, state_name, country_name, region_name
FROM BL_CL.get_geography_hierarchy_data(
    ROW('BL_3NF.CE_GEOGRAPHIES', 'BL_DM.DIM_GEOGRAPHIES', 'GEOGRAPHY_SRC_ID', 'GEOGRAPHY_SURR_ID',
        'FULL', 'ALL', 'STRICT', 0, TRUE)::BL_CL.t_dim_load_config,
    NULL
) LIMIT 5;

-- Test hierarchy validation function
SELECT BL_CL.validate_geography_hierarchy(
    ROW('New York|New York|USA',         -- geography_src_id
         'New York', 'NYC',  'New York', 'NY', 'NY',
         'USA', 'US', 'US',  'North America', 'NAM',
        '3NF_LAYER', -- source_system
            'CE_GEOGRAPHIES')::BL_CL.t_geography_hierarchy
);

COMMIT;

-- -- =====================================================
-- -- READY FOR TESTING
-- -- Usage Examples:
-- --
-- -- 1. Full geography load with cursor processing:
CALL BL_CL.load_dim_geographies_full();
-- --
-- -- 2. Delta load with custom batch size:
-- CALL BL_CL.load_dim_geographies(
--         ROW ('BL_3NF.CE_GEOGRAPHIES', 'BL_DM.DIM_GEOGRAPHIES',
--             'GEOGRAPHY_SRC_ID', 'GEOGRAPHY_SURR_ID',
--             'DELTA', '3NF_LAYER', 'RELAXED', 100, TRUE)::BL_CL.t_dim_load_config
--      );
--
-- -- Update a geography record in 3NF to trigger delta
-- UPDATE BL_3NF.CE_GEOGRAPHIES
-- SET ta_update_dt = CURRENT_TIMESTAMP
-- WHERE geography_id = (SELECT geography_id
--                       FROM BL_3NF.CE_GEOGRAPHIES
--                       WHERE geography_id != -1
--                       LIMIT 1);
--
-- Get a specific geography ID to work with
SELECT geography_id, geography_src_id, city_id, ta_update_dt
FROM BL_3NF.CE_GEOGRAPHIES
WHERE geography_id != -1
LIMIT 1;
--
-- Update the geography record itself (this will trigger delta)
UPDATE BL_3NF.CE_GEOGRAPHIES
SET geography_src_id = geography_src_id || ' UPDATED', -- Modify the source ID
    ta_update_dt     = CURRENT_TIMESTAMP
WHERE geography_id = 26095;
--  actual geography_id from above query
--
-- -- Check what changed in 3NF
-- SELECT geography_id, geography_src_id, ta_update_dt
-- FROM BL_3NF.CE_GEOGRAPHIES
-- WHERE geography_id = 26095;
--
-- --
-- -- 3. Test hierarchy flattening:
-- SELECT *
-- FROM BL_CL.get_geography_hierarchy_data(
--         ROW ('BL_3NF.CE_GEOGRAPHIES', 'BL_DM.DIM_GEOGRAPHIES',
--             'GEOGRAPHY_SRC_ID', 'GEOGRAPHY_SURR_ID',
--             'FULL', 'ALL', 'STRICT', 0, TRUE)::BL_CL.t_dim_load_config, NULL
--      )
-- LIMIT 10;
--
SELECT COUNT(*)
FROM BL_DM.DIM_GEOGRAPHIES;
SELECT city_name, state_name, country_name, region_name
FROM BL_DM.DIM_GEOGRAPHIES
WHERE geography_surr_id != -1
LIMIT 10;

select * from bl_dm.dim_geographies limit 5;
-- =====================================================