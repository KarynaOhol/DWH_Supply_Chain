-- =====================================================
-- MASTER DATA PIPELINE ORCHESTRATOR
-- =====================================================

CREATE OR REPLACE PROCEDURE BL_CL.MASTER_DATA_PIPELINE(
    p_load_type VARCHAR DEFAULT 'INCREMENTAL',        -- 'FULL' or 'INCREMENTAL'
    p_source_systems TEXT[] DEFAULT ARRAY['OMS', 'LMS'], -- Which source systems to process
    p_layers TEXT[] DEFAULT ARRAY['SA', '3NF', 'DM'],     -- Which layers to process
    p_file_paths JSONB DEFAULT NULL,                   -- Optional: {'OMS': '/path/to/oms.csv', 'LMS': '/path/to/lms.csv'}
    p_error_strategy VARCHAR DEFAULT 'CONTINUE_ON_WARNING', -- 'FAIL_FAST' or 'CONTINUE_ON_WARNING'
    p_force_3nf_full BOOLEAN DEFAULT FALSE             -- Force full 3NF load even if incremental requested
) LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_execution_time INTEGER;
    v_procedure_name VARCHAR := 'MASTER_DATA_PIPELINE';
    v_sa_status TEXT := 'NOT_STARTED';
    v_3nf_status TEXT := 'NOT_STARTED';
    v_dm_status TEXT := 'NOT_STARTED';
    v_source_system TEXT;
    v_load_id INTEGER;
    v_file_path TEXT;
    v_default_file_paths JSONB;
    v_overall_status TEXT := 'SUCCESS';
    v_layer TEXT;
    v_should_continue BOOLEAN := TRUE;
BEGIN
    -- Validate parameters
    IF p_load_type NOT IN ('FULL', 'INCREMENTAL') THEN
        RAISE EXCEPTION 'Invalid load_type: %. Must be FULL or INCREMENTAL', p_load_type;
    END IF;

    IF p_error_strategy NOT IN ('FAIL_FAST', 'CONTINUE_ON_WARNING') THEN
        RAISE EXCEPTION 'Invalid error_strategy: %. Must be FAIL_FAST or CONTINUE_ON_WARNING', p_error_strategy;
    END IF;

    -- Check if procedure is already running
    IF BL_CL.is_procedure_running(v_procedure_name) THEN
        RAISE EXCEPTION 'Master data pipeline is already running';
    END IF;

    -- Set default file paths if not provided
    IF p_file_paths IS NULL THEN
        v_default_file_paths := jsonb_build_object(
            'OMS', '/var/lib/postgresql/16/main/source_system_1_oms_' || LOWER(p_load_type) || '.csv',
            'LMS', '/var/lib/postgresql/16/main/source_system_2_lms_' || LOWER(p_load_type) || '.csv'
        );
    ELSE
        v_default_file_paths := p_file_paths;
    END IF;

    -- Log pipeline start
    CALL BL_CL.log_procedure_event(
        v_procedure_name,
        array_to_string(p_source_systems, ','),
        array_to_string(p_layers, '->'),
        'START',
        0,
        FORMAT('Starting master pipeline: load_type=%s, systems=%s, layers=%s, error_strategy=%s',
               p_load_type, array_to_string(p_source_systems, ','),
               array_to_string(p_layers, ','), p_error_strategy)
    );

    -- =====================================================
    -- LAYER 1: STAGING AREA (SA) LOADING
    -- =====================================================
    IF 'SA' = ANY(p_layers) THEN
        BEGIN
            v_sa_status := 'PROCESSING';
            RAISE NOTICE '=== LAYER 1: STAGING AREA LOADING ===';

            -- Process each source system
            FOREACH v_source_system IN ARRAY p_source_systems LOOP
                -- Get file path for this source system
                v_file_path := v_default_file_paths ->> v_source_system;

                IF v_file_path IS NULL THEN
                    RAISE EXCEPTION 'No file path provided for source system: %', v_source_system;
                END IF;

                RAISE NOTICE 'Loading % from file: %', v_source_system, v_file_path;

                -- Call staging load procedure
                v_load_id := BL_CL.LOAD_STAGING_DATA(
                    v_source_system,
                    v_file_path,
                    p_load_type
                );

                RAISE NOTICE 'Successfully loaded % with load_id: %', v_source_system, v_load_id;
            END LOOP;

            v_sa_status := 'SUCCESS';
            RAISE NOTICE 'LAYER 1 COMPLETED: All staging loads successful';

        EXCEPTION WHEN OTHERS THEN
            v_sa_status := 'FAILED';
            v_overall_status := 'FAILED';

            CALL BL_CL.log_procedure_event(
                v_procedure_name,
                array_to_string(p_source_systems, ','),
                'SA_LAYER',
                'ERROR',
                0,
                FORMAT('SA Layer failed: %s', SQLERRM)
            );

            IF p_error_strategy = 'FAIL_FAST' THEN
                RAISE EXCEPTION 'SA Layer failed, stopping pipeline: %', SQLERRM;
            ELSE
                RAISE WARNING 'SA Layer failed, continuing with next layer: %', SQLERRM;
                v_should_continue := FALSE;
            END IF;
        END;
    ELSE
        v_sa_status := 'SKIPPED';
        RAISE NOTICE 'LAYER 1 SKIPPED: SA layer not requested';
    END IF;

    -- =====================================================
    -- LAYER 2: 3NF LOADING
    -- =====================================================
    IF '3NF' = ANY(p_layers) AND v_should_continue THEN
        BEGIN
            v_3nf_status := 'PROCESSING';
            RAISE NOTICE '=== LAYER 2: 3NF LOADING ===';

            -- Decide which 3NF procedure to call
            IF p_load_type = 'FULL' OR p_force_3nf_full THEN
                RAISE NOTICE 'Calling 3NF FULL load procedure';
                CALL BL_CL.load_bl_3nf_full();
            ELSE
                RAISE NOTICE 'Calling 3NF INCREMENTAL load procedure';
                -- Note: This procedure needs to be properly implemented
                CALL BL_CL.load_bl_3nf_incremental();
            END IF;

            v_3nf_status := 'SUCCESS';
            RAISE NOTICE 'LAYER 2 COMPLETED: 3NF load successful';

        EXCEPTION WHEN OTHERS THEN
            v_3nf_status := 'FAILED';

            CALL BL_CL.log_procedure_event(
                v_procedure_name,
                'SA_*',
                'BL_3NF',
                'ERROR',
                0,
                FORMAT('3NF Layer failed: %s', SQLERRM)
            );

            IF p_error_strategy = 'FAIL_FAST' THEN
                v_overall_status := 'FAILED';
                RAISE EXCEPTION '3NF Layer failed, stopping pipeline: %', SQLERRM;
            ELSE
                v_overall_status := 'WARNING';
                RAISE WARNING '3NF Layer failed, continuing with next layer: %', SQLERRM;
                v_should_continue := FALSE;
            END IF;
        END;
    ELSE
        v_3nf_status := 'SKIPPED';
        IF '3NF' = ANY(p_layers) THEN
            RAISE NOTICE 'LAYER 2 SKIPPED: 3NF layer requested but cannot proceed due to SA failure';
        ELSE
            RAISE NOTICE 'LAYER 2 SKIPPED: 3NF layer not requested';
        END IF;
    END IF;

    -- =====================================================
    -- LAYER 3: DATA MART (DM) LOADING
    -- =====================================================
    IF 'DM' = ANY(p_layers) AND v_should_continue THEN
        BEGIN
            v_dm_status := 'PROCESSING';
            RAISE NOTICE '=== LAYER 3: DATA MART LOADING ===';

            -- Call DM load with incremental flag
            CALL BL_CL.load_bl_dm_full(p_load_type = 'INCREMENTAL');

            v_dm_status := 'SUCCESS';
            RAISE NOTICE 'LAYER 3 COMPLETED: DM load successful';

        EXCEPTION WHEN OTHERS THEN
            v_dm_status := 'FAILED';

            CALL BL_CL.log_procedure_event(
                v_procedure_name,
                'BL_3NF',
                'BL_DM',
                'ERROR',
                0,
                FORMAT('DM Layer failed: %s', SQLERRM)
            );

            IF p_error_strategy = 'FAIL_FAST' THEN
                v_overall_status := 'FAILED';
                RAISE EXCEPTION 'DM Layer failed, stopping pipeline: %', SQLERRM;
            ELSE
                v_overall_status := 'WARNING';
                RAISE WARNING 'DM Layer failed: %', SQLERRM;
            END IF;
        END;
    ELSE
        v_dm_status := 'SKIPPED';
        IF 'DM' = ANY(p_layers) THEN
            RAISE NOTICE 'LAYER 3 SKIPPED: DM layer requested but cannot proceed due to previous failures';
        ELSE
            RAISE NOTICE 'LAYER 3 SKIPPED: DM layer not requested';
        END IF;
    END IF;

    -- =====================================================
    -- PIPELINE COMPLETION
    -- =====================================================
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log final status
    CALL BL_CL.log_procedure_event(
        v_procedure_name,
        array_to_string(p_source_systems, ','),
        array_to_string(p_layers, '->'),
        v_overall_status,
        CASE
            WHEN v_overall_status = 'SUCCESS' THEN array_length(p_layers, 1)
            ELSE 0
        END,
        FORMAT('Pipeline completed: SA=%s, 3NF=%s, DM=%s', v_sa_status, v_3nf_status, v_dm_status),
        v_execution_time
    );

    -- Final summary
    RAISE NOTICE '=== MASTER PIPELINE COMPLETED ===';
    RAISE NOTICE 'Overall Status: %', v_overall_status;
    RAISE NOTICE 'SA Layer: %', v_sa_status;
    RAISE NOTICE '3NF Layer: %', v_3nf_status;
    RAISE NOTICE 'DM Layer: %', v_dm_status;
    RAISE NOTICE 'Total Execution Time: % ms', v_execution_time;

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

        CALL BL_CL.log_procedure_event(
            v_procedure_name,
            array_to_string(p_source_systems, ','),
            array_to_string(p_layers, '->'),
            'ERROR',
            0,
            SQLERRM,
            v_execution_time,
            SQLSTATE
        );

        RAISE EXCEPTION 'Master pipeline failed: %', SQLERRM;
END;
$$;