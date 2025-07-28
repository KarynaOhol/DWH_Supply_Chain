-- =====================================================
-- CONVENIENCE WRAPPER PROCEDURES
-- =====================================================

-- Full pipeline refresh (everything from scratch)
CREATE OR REPLACE PROCEDURE BL_CL.FULL_PIPELINE_REFRESH()
LANGUAGE plpgsql AS $$
BEGIN
    CALL BL_CL.MASTER_DATA_PIPELINE(
        p_load_type := 'FULL',
        p_source_systems := ARRAY['OMS', 'LMS'],
        p_layers := ARRAY['SA', '3NF', 'DM'],
        p_error_strategy := 'FAIL_FAST'
    );
END;
$$;

-- Daily incremental pipeline
CREATE OR REPLACE PROCEDURE BL_CL.DAILY_INCREMENTAL_PIPELINE()
LANGUAGE plpgsql AS $$
BEGIN
    CALL BL_CL.MASTER_DATA_PIPELINE(
        p_load_type := 'INCREMENTAL',
        p_source_systems := ARRAY['OMS', 'LMS'],
        p_layers := ARRAY['SA', '3NF', 'DM'],
        p_error_strategy := 'CONTINUE_ON_WARNING'
    );
END;
$$;

-- Only staging layer (for testing or manual intervention)
CREATE OR REPLACE PROCEDURE BL_CL.STAGING_ONLY_PIPELINE(
    p_load_type VARCHAR DEFAULT 'INCREMENTAL'
)
LANGUAGE plpgsql AS $$
BEGIN
    CALL BL_CL.MASTER_DATA_PIPELINE(
        p_load_type := p_load_type,
        p_source_systems := ARRAY['OMS', 'LMS'],
        p_layers := ARRAY['SA'],
        p_error_strategy := 'FAIL_FAST'
    );
END;
$$;

-- 3NF and DM only (skip staging - useful when SA data already loaded)
CREATE OR REPLACE PROCEDURE BL_CL.ANALYTICAL_LAYERS_PIPELINE(
    p_load_type VARCHAR DEFAULT 'INCREMENTAL'
)
LANGUAGE plpgsql AS $$
BEGIN
    CALL BL_CL.MASTER_DATA_PIPELINE(
        p_load_type := p_load_type,
        p_source_systems := ARRAY['OMS', 'LMS'],
        p_layers := ARRAY['3NF', 'DM'],
        p_error_strategy := 'CONTINUE_ON_WARNING'
    );
END;
$$;

-- Single source system pipeline (useful for testing)
CREATE OR REPLACE PROCEDURE BL_CL.SINGLE_SYSTEM_PIPELINE(
    p_source_system VARCHAR,
    p_load_type VARCHAR DEFAULT 'INCREMENTAL'
)
LANGUAGE plpgsql AS $$
BEGIN
    CALL BL_CL.MASTER_DATA_PIPELINE(
        p_load_type := p_load_type,
        p_source_systems := ARRAY[p_source_system],
        p_layers := ARRAY['SA', '3NF', 'DM'],
        p_error_strategy := 'FAIL_FAST'
    );
END;
$$;

-- Recovery pipeline (start from specific layer)
CREATE OR REPLACE PROCEDURE BL_CL.RECOVERY_PIPELINE(
    p_start_layer VARCHAR, -- 'SA', '3NF', or 'DM'
    p_load_type VARCHAR DEFAULT 'INCREMENTAL'
)
LANGUAGE plpgsql AS $$
DECLARE
    v_layers TEXT[];
BEGIN
    -- Determine which layers to include based on start layer
    CASE p_start_layer
        WHEN 'SA' THEN v_layers := ARRAY['SA', '3NF', 'DM'];
        WHEN '3NF' THEN v_layers := ARRAY['3NF', 'DM'];
        WHEN 'DM' THEN v_layers := ARRAY['DM'];
        ELSE RAISE EXCEPTION 'Invalid start_layer: %. Must be SA, 3NF, or DM', p_start_layer;
    END CASE;

    CALL BL_CL.MASTER_DATA_PIPELINE(
        p_load_type := p_load_type,
        p_source_systems := ARRAY['OMS', 'LMS'],
        p_layers := v_layers,
        p_error_strategy := 'CONTINUE_ON_WARNING'
    );
END;
$$;