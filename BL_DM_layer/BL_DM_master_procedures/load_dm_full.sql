-- =====================================================
--  ORCHESTRATION PROCEDURE FOR DM LAYER
-- =====================================================

-- Master procedure to load complete DM layer (dimensions + fact)
CREATE OR REPLACE PROCEDURE BL_CL.load_bl_dm_full(
    p_incremental BOOLEAN DEFAULT TRUE
)
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time        TIMESTAMP := CURRENT_TIMESTAMP;
    v_execution_time    INTEGER;
    v_dimensions_status TEXT      := 'NOT_STARTED';
    v_fact_status       TEXT      := 'NOT_STARTED';
BEGIN
    -- Check if procedure is already running
    IF BL_CL.is_procedure_running('load_bl_dm_full') THEN
        RAISE EXCEPTION 'Procedure load_bl_dm_full is already running';
    END IF;

-- Log complete DM ETL start
    CALL BL_CL.log_procedure_event(
            'load_bl_dm_full',
            'BL_3NF.*',
            'BL_DM.*',
            'START',
            0,
            FORMAT('Starting complete DM layer load - incremental: %s (dimensions: %s)',
                   p_incremental,
                   CASE WHEN p_incremental THEN 'DELTA' ELSE 'FULL' END)
         );

    -- -- PHASE 1: Load DM dimensions from 3NF
--     BEGIN
--         RAISE NOTICE 'PHASE 1: Starting DM dimension loading from 3NF...';
--         CALL BL_CL.load_all_dimensions_full();
--         v_dimensions_status := 'SUCCESS';
--         RAISE NOTICE 'PHASE 1: DM dimension loading completed successfully';
--     EXCEPTION
--         WHEN OTHERS THEN
--             v_dimensions_status := 'FAILED';
--             RAISE WARNING 'PHASE 1: DM dimension loading failed: %', SQLERRM;
--     END;
    -- PHASE 1: Load DM dimensions from 3NF (conditional based on incremental flag)
    BEGIN
        RAISE NOTICE 'PHASE 1: Starting DM dimension loading from 3NF...';

        IF p_incremental THEN
            RAISE NOTICE 'PHASE 1: Using DELTA load for dimensions';
            CALL BL_CL.load_all_dimensions_delta();
        ELSE
            RAISE NOTICE 'PHASE 1: Using FULL load for dimensions';
            CALL BL_CL.load_all_dimensions_full();
        END IF;

        v_dimensions_status := 'SUCCESS';
        RAISE NOTICE 'PHASE 1: DM dimension loading completed successfully';
    EXCEPTION
        WHEN OTHERS THEN
            v_dimensions_status := 'FAILED';
            RAISE WARNING 'PHASE 1: DM dimension loading failed: %', SQLERRM;
    END;

-- PHASE 2: Setup partitions if first run
    IF NOT p_incremental THEN
        BEGIN
            RAISE NOTICE 'PHASE 2: Setting up historical partitions...';
            CALL BL_CL.setup_historical_partitions();
            RAISE NOTICE 'PHASE 2: Historical partitions setup completed';
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'PHASE 2: Historical partition setup failed: %', SQLERRM;
        END;
    END IF;

-- PHASE 3: Load fact table with partitioning
    BEGIN
        RAISE NOTICE 'PHASE 3: Starting partitioned fact loading...';
        CALL BL_CL.load_fct_order_line_shipments_dd(p_incremental);
        v_fact_status := 'SUCCESS';
        RAISE NOTICE 'PHASE 3: Partitioned fact loading completed successfully';
    EXCEPTION
        WHEN OTHERS THEN
            v_fact_status := 'FAILED';
            RAISE WARNING 'PHASE 3: Partitioned fact loading failed: %', SQLERRM;
    END;

    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Determine overall status
    IF v_dimensions_status = 'SUCCESS' AND v_fact_status = 'SUCCESS' THEN
        CALL BL_CL.log_procedure_event(
                'load_bl_dm_full',
                'BL_3NF.*',
                'BL_DM.*',
                'SUCCESS',
                1,
                'Complete DM layer load completed successfully with partitioning',
                v_execution_time
             );
        RAISE NOTICE 'COMPLETE DM ETL SUCCESS: All dimensions and partitioned fact loaded in % ms', v_execution_time;
    ELSE
        CALL BL_CL.log_procedure_event(
                'load_bl_dm_full',
                'BL_3NF.*',
                'BL_DM.*',
                'WARNING',
                0,
                FORMAT('Partial DM load: dimensions=%s, fact=%s', v_dimensions_status, v_fact_status),
                v_execution_time
             );
        RAISE NOTICE 'PARTIAL DM ETL: Some components failed. Check logs for details.';
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

        CALL BL_CL.log_procedure_event(
                'load_bl_dm_full',
                'BL_3NF.*',
                'BL_DM.*',
                'ERROR',
                0,
                SQLERRM,
                v_execution_time,
                SQLSTATE
             );
        RAISE;
END
$$;