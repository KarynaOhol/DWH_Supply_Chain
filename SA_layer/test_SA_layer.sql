

-- =====================================================
-- STEP 1: INITIAL FULL LOADS (First Time Setup)
-- =====================================================

-- Load OMS system - Full Load (Initial)
SELECT BL_CL.LOAD_STAGING_DATA(
    'OMS',                                                           -- source system
    '/var/lib/postgresql/16/main/source_system_1_oms_full.csv',   -- file path
    'FULL'                                                          -- load type
) as oms_load_id;

-- Load LMS system - Full Load (Initial)
SELECT BL_CL.LOAD_STAGING_DATA(
    'LMS',                                                           -- source system
    '/var/lib/postgresql/16/main/source_system_2_lms_full.csv',   -- file path
    'FULL'                                                          -- load type
) as lms_load_id;

-- =====================================================
-- STEP 2: INCREMENTAL LOADS (Subsequent Loads)
-- =====================================================

-- Load OMS system - Incremental Load
SELECT BL_CL.LOAD_STAGING_DATA(
    'OMS',                                                              -- source system
    '/var/lib/postgresql/16/main/source_system_1_oms_incremental.csv', -- incremental file
    'INCREMENTAL'                                                       -- load type
) as oms_incremental_load_id;

-- Load LMS system - Incremental Load
SELECT BL_CL.LOAD_STAGING_DATA(
    'LMS',                                                              -- source system
    '/var/lib/postgresql/16/main/source_system_2_lms_incremental.csv', -- incremental file
    'INCREMENTAL'                                                       -- load type
) as lms_incremental_load_id;

-- =====================================================
-- BATCH PROCESSING EXAMPLE
-- =====================================================

-- Process both systems in sequence with error handling
DO $$
DECLARE
    v_oms_load_id INTEGER;
    v_lms_load_id INTEGER;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    
    RAISE NOTICE 'Starting batch load process at %', v_start_time;
    
    -- Load OMS incremental
    BEGIN
        v_oms_load_id := BL_CL.LOAD_STAGING_DATA(
            'OMS', 
            '/var/lib/postgresql/16/main/source_system_1_oms_incremental.csv', 
            'INCREMENTAL'
        );
        RAISE NOTICE 'OMS incremental load completed successfully. Load ID: %', v_oms_load_id;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'OMS incremental load failed: %', SQLERRM;
    END;
    
    -- Load LMS incremental
    BEGIN
        v_lms_load_id := BL_CL.LOAD_STAGING_DATA(
            'LMS', 
            '/var/lib/postgresql/16/main/source_system_2_lms_incremental.csv', 
            'INCREMENTAL'
        );
        RAISE NOTICE 'LMS incremental load completed successfully. Load ID: %', v_lms_load_id;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'LMS incremental load failed: %', SQLERRM;
    END;
    
    v_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Batch load process completed at %. Total time: %', 
                 v_end_time, 
                 EXTRACT(EPOCH FROM (v_end_time - v_start_time)) || ' seconds';
END $$;

-- =====================================================
-- MONITORING AND VERIFICATION QUERIES
-- =====================================================

-- Check load status for recent loads
SELECT 
    LOAD_ID,
    SOURCE_SYSTEM,
    FILE_NAME,
    LOAD_TYPE,
    LOAD_STATUS,
    RECORDS_PROCESSED,
    RECORDS_INSERTED,
    START_TIMESTAMP,
    END_TIMESTAMP,
    EXTRACT(EPOCH FROM (END_TIMESTAMP - START_TIMESTAMP)) as duration_seconds,
    ERROR_MESSAGE
FROM BL_CL.MTA_FILE_LOADS 
WHERE START_TIMESTAMP >= CURRENT_DATE
ORDER BY START_TIMESTAMP DESC;

-- Check last load dates per source system
SELECT 
    SOURCE_SYSTEM,
    MAX(END_TIMESTAMP) as last_successful_load,
    SUM(CASE WHEN LOAD_STATUS = 'COMPLETED' THEN RECORDS_INSERTED ELSE 0 END) as total_records_today
FROM BL_CL.MTA_FILE_LOADS 
WHERE START_TIMESTAMP >= CURRENT_DATE
  AND LOAD_STATUS = 'COMPLETED'
GROUP BY SOURCE_SYSTEM;


SELECT 
    SOURCE_SYSTEM,
    FILE_NAME,
    ERROR_MESSAGE,
    START_TIMESTAMP
FROM BL_CL.MTA_FILE_LOADS 
WHERE LOAD_STATUS = 'FAILED'
  AND START_TIMESTAMP >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY START_TIMESTAMP DESC;


-- See the most recent records loaded (assuming they have ta_insert_dt populated)
SELECT
    transaction_src_id,
    order_src_id,
    customer_first_name,
    customer_last_name,
    product_name,
    sales_amount,
    order_dt,
    ta_insert_dt
FROM sa_oms.src_oms
WHERE ta_insert_dt >= '2025-07-24 17:24:40'  -- Use the start timestamp from your load
ORDER BY ta_insert_dt DESC
LIMIT 20;


select count(*) from sa_oms.src_oms;

select count(*) from sa_lms.src_lms;

