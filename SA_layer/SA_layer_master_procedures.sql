-- =====================================================
--  MASTER LOAD PROCEDURE
-- =====================================================

CREATE OR REPLACE FUNCTION BL_CL.LOAD_STAGING_DATA(
    p_source_system VARCHAR,
    p_file_path VARCHAR,
    p_load_type VARCHAR DEFAULT 'FULL' -- 'FULL' or 'INCREMENTAL'
) RETURNS INTEGER AS
$$
DECLARE
    v_load_id             INTEGER;
    v_ext_table_name      VARCHAR;
    v_target_schema       VARCHAR;
    v_target_table        VARCHAR;
    v_date_column         VARCHAR;
    v_ext_date_column     VARCHAR;
    v_last_load_date      DATE;
    v_records_processed   INTEGER := 0;
    v_records_inserted    INTEGER := 0;
    v_file_name           VARCHAR;
    v_sql                 TEXT;
    v_where_clause        TEXT    := '';
    v_procedure_name      VARCHAR := 'LOAD_STAGING_DATA';
    v_start_time          TIMESTAMP;
    v_execution_time_ms   INTEGER;
    v_lock_acquired       BOOLEAN := FALSE;
    v_lock_name           VARCHAR;
    v_full_ext_table_name VARCHAR;
    v_ext_schema          VARCHAR;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    v_lock_name := v_procedure_name || '_' || p_source_system;

    -- Validate input parameters
    IF p_source_system IS NULL OR p_file_path IS NULL THEN
        RAISE EXCEPTION 'Source system and file path cannot be null';
    END IF;

    IF p_load_type NOT IN ('FULL', 'INCREMENTAL') THEN
        RAISE EXCEPTION 'Load type must be FULL or INCREMENTAL, got: %', p_load_type;
    END IF;

    -- Construct external schema name
    v_ext_schema := 'sa_' || LOWER(p_source_system);

    -- Try to acquire lock using framework
    v_lock_acquired := BL_CL.acquire_procedure_lock(v_lock_name);

    IF NOT v_lock_acquired THEN
        RAISE EXCEPTION 'Could not acquire lock for % - procedure may already be running', v_lock_name;
    END IF;

    -- Extract filename from path
    v_file_name := SPLIT_PART(p_file_path, '/', -1);

    -- Log procedure start using framework
    CALL BL_CL.log_procedure_event(
            v_procedure_name,
            p_file_path,
            NULL,
            'START',
            0,
            FORMAT('Starting load for %s system, file: %s, load type: %s', p_source_system, v_file_name, p_load_type)
         );

    -- Get source system configuration
    SELECT TARGET_SCHEMA, TARGET_TABLE, DATE_COLUMN
    INTO v_target_schema, v_target_table, v_date_column
    FROM BL_CL.MTA_SOURCE_SYSTEMS
    WHERE SOURCE_SYSTEM = p_source_system
      AND ACTIVE_FLAG = TRUE;

    IF NOT FOUND THEN
        CALL BL_CL.log_procedure_event(
                v_procedure_name,
                p_file_path,
                NULL,
                'ERROR',
                0,
                FORMAT('Source system %s not configured or inactive', p_source_system)
             );
        RAISE EXCEPTION 'Source system % not configured or inactive', p_source_system;
    END IF;

    -- Create load control record
    INSERT INTO BL_CL.MTA_FILE_LOADS (SOURCE_SYSTEM, FILE_PATH, FILE_NAME, TARGET_SCHEMA, TARGET_TABLE,
                                      LOAD_TYPE, LOAD_STATUS, START_TIMESTAMP)
    VALUES (p_source_system, p_file_path, v_file_name, v_target_schema, v_target_table,
            p_load_type, 'PROCESSING', CURRENT_TIMESTAMP)
    RETURNING LOAD_ID INTO v_load_id;

    BEGIN
        -- Create dynamic external table
        CALL BL_CL.log_procedure_event(
                v_procedure_name, p_file_path, NULL, 'INFO', 0,
                FORMAT('Creating external table for file: %s', v_file_name)
             );

        -- Create external table and get table name
        v_ext_table_name := BL_CL.CREATE_DYNAMIC_EXTERNAL_TABLE(p_source_system, p_file_path, v_load_id::VARCHAR);

        -- Store the full qualified name for consistent reference
        v_full_ext_table_name := v_ext_schema || '.' || v_ext_table_name;

        -- MODIFIED: Only apply date filtering if DATE_COLUMN is configured AND load type is INCREMENTAL
        IF p_load_type = 'INCREMENTAL' AND v_date_column IS NOT NULL THEN
            CALL BL_CL.log_procedure_event(
                    v_procedure_name, p_file_path, NULL, 'INFO', 0,
                    'DATE_COLUMN is NULL - skipping date filtering, loading ALL records'
                 );
        END IF;

        -- Build and execute insert statement based on source system
        CALL BL_CL.log_procedure_event(
                v_procedure_name, p_file_path, NULL, 'INFO', 0,
                FORMAT('Building insert statement for source system: %s (no date filter)', p_source_system)
             );

        IF p_source_system = 'OMS' THEN
            v_sql := BL_CL.BUILD_OMS_INSERT_SQL(v_target_schema, v_target_table, v_ext_schema, v_ext_table_name, '');
        ELSIF p_source_system = 'LMS' THEN
            v_sql := BL_CL.BUILD_LMS_INSERT_SQL(v_target_schema, v_target_table, v_ext_schema, v_ext_table_name, '');
        ELSE
            RAISE EXCEPTION 'Unsupported source system: %', p_source_system;
        END IF;

        -- Execute the insert statement
        CALL BL_CL.log_procedure_event(
                v_procedure_name, p_file_path, NULL, 'INFO', 0,
                'Executing data insert operation (ALL RECORDS)'
             );

        EXECUTE v_sql;
        GET DIAGNOSTICS v_records_inserted = ROW_COUNT;

        -- Get total records processed from external table
        EXECUTE FORMAT('SELECT COUNT(*) FROM %s WHERE TransactionSK != ''TransactionSK''',
                       v_full_ext_table_name) INTO v_records_processed;

        -- Clean up external table
        CALL BL_CL.DROP_EXTERNAL_TABLE(v_full_ext_table_name);

        -- Update load status
        PERFORM BL_CL.UPDATE_LOAD_STATUS(v_load_id, 'COMPLETED', v_records_processed, v_records_inserted);

        -- Calculate execution time
        v_execution_time_ms := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

        -- Log success using framework
        CALL BL_CL.log_procedure_event(
                v_procedure_name,
                v_full_ext_table_name,
                FORMAT('%s.%s', v_target_schema, v_target_table),
                'SUCCESS',
                v_records_inserted,
                FORMAT('Successfully loaded %s records from %s (processed %s total)',
                       v_records_inserted, v_file_name, v_records_processed),
                v_execution_time_ms
             );

        -- Release lock
        PERFORM BL_CL.release_procedure_lock(v_lock_name);

        RETURN v_load_id;

    EXCEPTION
        WHEN OTHERS THEN
            -- Clean up external table on error
            BEGIN
                IF v_full_ext_table_name IS NOT NULL THEN
                    CALL BL_CL.DROP_EXTERNAL_TABLE(v_full_ext_table_name);
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    -- Ignore cleanup errors but log them
                    CALL BL_CL.log_procedure_event(
                            v_procedure_name, p_file_path, NULL, 'WARNING', 0,
                            FORMAT('Failed to cleanup external table: %s', SQLERRM)
                         );
            END;

            -- Update load status
            PERFORM BL_CL.UPDATE_LOAD_STATUS(v_load_id, 'FAILED', v_records_processed, 0, 0, SQLERRM);

            -- Calculate execution time
            v_execution_time_ms := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

            -- Log failure using framework
            CALL BL_CL.log_procedure_event(
                    v_procedure_name,
                    COALESCE(v_full_ext_table_name, p_file_path),
                    COALESCE(FORMAT('%s.%s', v_target_schema, v_target_table), 'UNKNOWN'),
                    'ERROR',
                    0,
                    FORMAT('Load failed for %s: %s', p_source_system, SQLERRM),
                    v_execution_time_ms,
                    SQLSTATE
                 );

            -- Release lock
            PERFORM BL_CL.release_procedure_lock(v_lock_name);

            RAISE EXCEPTION 'Load failed for %: %', p_source_system, SQLERRM;
    END;
END;
$$ LANGUAGE plpgsql;