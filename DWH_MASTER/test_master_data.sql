--Full Pipeline Refresh (Clean Slate):
CALL BL_CL.FULL_PIPELINE_REFRESH();
-- Daily Incremental Load:
CALL BL_CL.DAILY_INCREMENTAL_PIPELINE();
-- Custom Pipeline with Specific File Paths:
CALL BL_CL.MASTER_DATA_PIPELINE(
        p_load_type := 'INCREMENTAL',
        p_file_paths := '{
          "OMS": "/custom/path/oms.csv",
          "LMS": "/custom/path/lms.csv"
        }'::JSONB
     );
-- Recovery from Failed 3NF Layer:
CALL BL_CL.RECOVERY_PIPELINE('3NF', 'INCREMENTAL');
--Test Single Source System:
CALL BL_CL.SINGLE_SYSTEM_PIPELINE('OMS', 'INCREMENTAL');

-- Custom call with only 3NF and DM layers
CALL BL_CL.MASTER_DATA_PIPELINE(
    p_load_type := 'INCREMENTAL',          -- or 'FULL'
    p_source_systems := ARRAY['OMS', 'LMS'],
    p_layers := ARRAY['3NF', 'DM'],        -- Only these layers
    p_error_strategy := 'CONTINUE_ON_WARNING'
);


-- Load only 3NF and DM layers (skip SA)
CALL BL_CL.ANALYTICAL_LAYERS_PIPELINE('INCREMENTAL');
CALL BL_CL.ANALYTICAL_LAYERS_PIPELINE('FULL');

CALL BL_CL.MASTER_DATA_PIPELINE(
        p_load_type := 'FULL',
        p_file_paths := '{
          "OMS": "/var/lib/postgresql/16/main/source_system_1_oms_incremental.csv",
          "LMS": "/var/lib/postgresql/16/main/source_system_2_lms_incremental.csv"
        }'::JSONB
     );


