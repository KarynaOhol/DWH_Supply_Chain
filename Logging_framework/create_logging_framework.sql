-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - LOGGING FRAMEWORK
-- Purpose: Create centralized logging system for BL_CL procedures
-- Run as: dwh_cleansing_user
-- =====================================================

SELECT CURRENT_USER, SESSION_USER;

SET ROLE dwh_cleansing_user;
SET search_path = BL_CL, BL_3NF, SA_OMS, SA_LMS, public;

-- =====================================================
-- SECTION 1: CREATE CENTRALIZED LOGGING TABLE
-- =====================================================

-- Drop table if exists (for rerun capability)
DROP TABLE IF EXISTS BL_CL.MTA_PROCESS_LOG CASCADE;

-- Create centralized logging table
CREATE TABLE IF NOT EXISTS BL_CL.MTA_PROCESS_LOG
(
    log_id            SERIAL PRIMARY KEY,
    log_datetime      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    procedure_name    VARCHAR(100) NOT NULL,
    source_table      VARCHAR(100) NULL,     -- e.g., 'SA_OMS.SRC_OMS'
    target_table      VARCHAR(100) NULL,     -- e.g., 'BL_3NF.CE_CUSTOMERS'
    user_name         VARCHAR(100) NOT NULL DEFAULT CURRENT_USER,
    rows_affected     INTEGER      NOT NULL DEFAULT 0,
    status            VARCHAR(20)  NOT NULL, -- 'START', 'SUCCESS', 'ERROR', 'WARNING'
    message           TEXT         NULL,
    execution_time_ms INTEGER      NULL,     -- Execution time in milliseconds
    error_code        VARCHAR(50)  NULL,     -- SQLSTATE for errors

    -- Technical attributes
    ta_insert_dt      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IDX_MTA_PROCESS_LOG_DATETIME ON BL_CL.MTA_PROCESS_LOG (log_datetime);
CREATE INDEX IDX_MTA_PROCESS_LOG_PROCEDURE ON BL_CL.MTA_PROCESS_LOG (procedure_name);
CREATE INDEX IDX_MTA_PROCESS_LOG_STATUS ON BL_CL.MTA_PROCESS_LOG (status);
CREATE INDEX IDX_MTA_PROCESS_LOG_USER ON BL_CL.MTA_PROCESS_LOG (user_name);


-- =====================================================
-- PROPER CONCURRENCY CONTROL WITH LOCK TABLE
-- =====================================================
-- Create a dedicated process lock table
CREATE TABLE IF NOT EXISTS BL_CL.MTA_PROCESS_LOCKS
(
    procedure_name VARCHAR(100) PRIMARY KEY,
    lock_datetime  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    user_name      VARCHAR(100) NOT NULL DEFAULT CURRENT_USER,
    process_id     INTEGER               DEFAULT pg_backend_pid()
);
-- Create improved locking functions
CREATE OR REPLACE FUNCTION BL_CL.acquire_procedure_lock(p_procedure_name VARCHAR(100))
    RETURNS BOOLEAN
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_lock_acquired BOOLEAN := FALSE;
BEGIN
    -- Try to acquire lock
    BEGIN
        INSERT INTO BL_CL.MTA_PROCESS_LOCKS (procedure_name, lock_datetime, user_name, process_id)
        VALUES (p_procedure_name, CURRENT_TIMESTAMP, CURRENT_USER, pg_backend_pid());
        v_lock_acquired := TRUE;
    EXCEPTION
        WHEN unique_violation THEN
            -- Lock already exists, check if it's stale
            DELETE
            FROM BL_CL.MTA_PROCESS_LOCKS
            WHERE procedure_name = p_procedure_name
              AND lock_datetime < CURRENT_TIMESTAMP - INTERVAL '2 hours';

            -- Try again after cleanup
            BEGIN
                INSERT INTO BL_CL.MTA_PROCESS_LOCKS (procedure_name, lock_datetime, user_name, process_id)
                VALUES (p_procedure_name, CURRENT_TIMESTAMP, CURRENT_USER, pg_backend_pid());
                v_lock_acquired := TRUE;
            EXCEPTION
                WHEN unique_violation THEN
                    v_lock_acquired := FALSE;
            END;
    END;

    RETURN v_lock_acquired;
END
$$;

CREATE OR REPLACE FUNCTION BL_CL.release_procedure_lock(p_procedure_name VARCHAR(100))
    RETURNS BOOLEAN
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_deleted_count INTEGER;
BEGIN
    DELETE
    FROM BL_CL.MTA_PROCESS_LOCKS
    WHERE procedure_name = p_procedure_name
      AND user_name = CURRENT_USER;

    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN v_deleted_count > 0;
END
$$;

-- =====================================================
-- SECTION 2: CREATE LOGGING PROCEDURE
-- =====================================================

-- Drop procedure if exists (for rerun capability)
DROP PROCEDURE IF EXISTS BL_CL.log_procedure_event(VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, TEXT, INTEGER, VARCHAR);

-- Create centralized logging procedure
CREATE OR REPLACE PROCEDURE BL_CL.log_procedure_event(
    p_procedure_name VARCHAR(100),
    p_source_table VARCHAR(100) DEFAULT NULL,
    p_target_table VARCHAR(100) DEFAULT NULL,
    p_status VARCHAR(20) DEFAULT 'N/A',
    p_rows_affected INTEGER DEFAULT 0,
    p_message TEXT DEFAULT NULL,
    p_execution_time_ms INTEGER DEFAULT 0,
    p_error_code VARCHAR(50) DEFAULT NULL
)
    LANGUAGE plpgsql
AS
$$
BEGIN
    -- Insert log entry
    INSERT INTO BL_CL.MTA_PROCESS_LOG (procedure_name,
                                       source_table,
                                       target_table,
                                       user_name,
                                       rows_affected,
                                       status,
                                       message,
                                       execution_time_ms,
                                       error_code)
    VALUES (p_procedure_name,
            p_source_table,
            p_target_table,
            CURRENT_USER,
            p_rows_affected,
            p_status,
            p_message,
            p_execution_time_ms,
            p_error_code);


EXCEPTION
    WHEN OTHERS THEN
        -- Even if logging fails, don't break the main procedure
        -- Just output to console for debugging
        RAISE WARNING 'Logging failed for procedure %: %', p_procedure_name, SQLERRM;
END
$$;

-- =====================================================
-- SECTION 3: CREATE UTILITY LOGGING FUNCTIONS
-- =====================================================

-- Function to get last successful execution time for incremental loading
CREATE OR REPLACE FUNCTION BL_CL.get_last_successful_load(
    p_procedure_name VARCHAR(100)
)
    RETURNS TIMESTAMP
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_last_load_time TIMESTAMP;
BEGIN
    SELECT MAX(log_datetime)
    INTO v_last_load_time
    FROM BL_CL.MTA_PROCESS_LOG
    WHERE procedure_name = p_procedure_name
      AND status = 'SUCCESS';

    -- Return default date if no successful run found
    RETURN COALESCE(v_last_load_time, '1900-01-01 00:00:00'::TIMESTAMP);
END
$$;

-- Function to check if procedure is currently running (prevent parallel execution)
CREATE OR REPLACE FUNCTION BL_CL.is_procedure_running(p_procedure_name VARCHAR(100))
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_lock_exists BOOLEAN;
BEGIN
    -- Clean up stale locks first
    DELETE FROM BL_CL.MTA_PROCESS_LOCKS
    WHERE lock_datetime < CURRENT_TIMESTAMP - INTERVAL '2 hours';

    -- Check if lock exists
    SELECT EXISTS(
        SELECT 1 FROM BL_CL.MTA_PROCESS_LOCKS
        WHERE procedure_name = p_procedure_name
    ) INTO v_lock_exists;

    RETURN v_lock_exists;
END $$;

-- =====================================================
-- SECTION 4: CREATE MONITORING VIEWS
-- =====================================================

-- View for procedure execution summary
CREATE OR REPLACE VIEW BL_CL.VW_PROCEDURE_EXECUTION_SUMMARY AS
SELECT procedure_name,
       COUNT(*)                                                     as total_executions,
       COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END)               as successful_runs,
       COUNT(CASE WHEN status = 'ERROR' THEN 1 END)                 as failed_runs,
       MAX(CASE WHEN status = 'SUCCESS' THEN log_datetime END)      as last_successful_run,
       MAX(CASE WHEN status = 'ERROR' THEN log_datetime END)        as last_failed_run,
       AVG(CASE WHEN status = 'SUCCESS' THEN execution_time_ms END) as avg_execution_time_ms,
       MAX(CASE WHEN status = 'SUCCESS' THEN rows_affected END)     as max_rows_processed
FROM BL_CL.MTA_PROCESS_LOG
GROUP BY procedure_name
ORDER BY procedure_name;

-- View for recent execution status
CREATE OR REPLACE VIEW BL_CL.VW_RECENT_EXECUTIONS AS
SELECT log_datetime,
       procedure_name,
       source_table,
       target_table,
       user_name,
       rows_affected,
       status,
       execution_time_ms,
       message
FROM BL_CL.MTA_PROCESS_LOG
WHERE log_datetime >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY log_datetime DESC;

-- =====================================================
-- SECTION 5: TEST THE LOGGING FRAMEWORK
-- =====================================================

-- Test logging procedure
DO
$$
    DECLARE
        v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
        v_execution_time INTEGER;
    BEGIN
        -- Log start of test
        CALL BL_CL.log_procedure_event(
                'test_logging_framework',
                'N/A',
                'BL_CL.MTA_PROCESS_LOG',
                'START',
                0,
                'Testing logging framework'
             );

        -- Simulate some processing time
        PERFORM pg_sleep(1);

        -- Calculate execution time
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

        -- Log successful completion
        CALL BL_CL.log_procedure_event(
                'test_logging_framework',
                'N/A',
                'BL_CL.MTA_PROCESS_LOG',
                'SUCCESS',
                1,
                'Logging framework test completed successfully',
                v_execution_time
             );


        RAISE NOTICE 'Logging framework test completed in % ms', v_execution_time;

    END
$$;

-- =====================================================
-- SECTION 6: VERIFICATION QUERIES
-- =====================================================

-- Verify table creation
\d BL_CL.MTA_PROCESS_LOG

-- Verify test log entries
SELECT log_id,
       log_datetime,
       procedure_name,
       status,
       rows_affected,
       execution_time_ms,
       message
FROM BL_CL.MTA_PROCESS_LOG
WHERE procedure_name = 'test_logging_framework'
ORDER BY log_datetime;

-- Test utility functions
SELECT 'get_last_successful_load'                               as function_name,
       BL_CL.get_last_successful_load('test_logging_framework') as result;

SELECT 'is_procedure_running'                               as function_name,
       BL_CL.is_procedure_running('test_logging_framework') as result;

-- View monitoring views
SELECT *
FROM BL_CL.VW_PROCEDURE_EXECUTION_SUMMARY
WHERE procedure_name = 'test_logging_framework';

SELECT *
FROM BL_CL.VW_RECENT_EXECUTIONS
WHERE procedure_name = 'test_logging_framework';

-- Show all created objects in BL_CL schema
SELECT schemaname,
       tablename as object_name,
       'table'   as object_type
FROM pg_tables
WHERE schemaname = 'bl_cl'

UNION ALL

SELECT routine_schema,
       routine_name,
       routine_type
FROM information_schema.routines
WHERE routine_schema = 'bl_cl'

UNION ALL

SELECT schemaname,
       viewname,
       'view'
FROM pg_views
WHERE schemaname = 'bl_cl'

ORDER BY object_type, object_name;

COMMIT;
