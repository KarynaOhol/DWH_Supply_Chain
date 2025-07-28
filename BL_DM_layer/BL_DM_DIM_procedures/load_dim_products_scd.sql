-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - DM LAYER PROCEDURES
-- File: 02_Dimension_Procedures/Product_Hierarchy/load_dim_products_scd.sql
-- Purpose: Load PRODUCTS SCD2 dimension from BL_3NF to BL_DM with advanced SCD2 processing
-- Requirements: Composite Types ✅, Cursor FOR Loop ✅, Dynamic SQL ✅, Cursor Variables ✅, UPSERT ✅
-- Technical Features: SCD2 change detection, cursor variables, complex hierarchy flattening
-- Run as: dwh_cleansing_user
-- =====================================================

SELECT CURRENT_USER, SESSION_USER;

SET ROLE dwh_cleansing_user;
SET search_path = BL_CL, BL_3NF, BL_DM, public;

-- =====================================================
-- SECTION 1: UTILITY FUNCTIONS FOR PRODUCT HIERARCHY AND SCD2
-- =====================================================

-- Function to build product hierarchy with category aggregation
CREATE OR REPLACE FUNCTION BL_CL.get_product_hierarchy_data(
    p_config BL_CL.t_dim_load_config,
    p_last_update_dt TIMESTAMP DEFAULT NULL
) RETURNS SETOF BL_CL.t_product_hierarchy
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_sql          TEXT;
    v_where_clause TEXT := 'WHERE p.product_id != -1';
BEGIN
    -- Add source system filter
    IF p_config.include_source_system != 'ALL' THEN
        v_where_clause := v_where_clause || ' AND p.source_system = ' || quote_literal(
                CASE p_config.include_source_system
                    WHEN '3NF_LAYER' THEN 'OMS' -- Map DM filter to 3NF source
                    ELSE p_config.include_source_system
                    END
                                                                         );
    END IF;

    -- Add incremental loading filter for delta mode
    IF p_config.load_mode = 'DELTA' AND p_last_update_dt IS NOT NULL THEN
        v_where_clause := v_where_clause || ' AND p.ta_update_dt > ' || quote_literal(p_last_update_dt);
    END IF;

    -- Build dynamic query for complete product hierarchy with category aggregation
    v_sql := FORMAT('
        SELECT
            p.product_src_id::VARCHAR(50) as product_src_id,
            COALESCE(p.product_name, ''Unknown''::VARCHAR(255))::VARCHAR(255) as product_name,
            COALESCE(b.brand_name, ''Unknown''::VARCHAR(100))::VARCHAR(100) as brand_name,
            COALESCE(b.brand_src_id, ''Unknown'')::VARCHAR(50) as brand_src_id,
            COALESCE(pc.category_name, ''Unknown''::VARCHAR(100))::VARCHAR(100) as primary_category_name,
            COALESCE(pc.category_src_id, ''Unknown'')::VARCHAR(100) as primary_category_src_id,
            COALESCE(d.department_name, ''Unknown''::VARCHAR(100))::VARCHAR(100) as department_name,
            COALESCE(d.department_src_id, ''Unknown'')::VARCHAR(50) as department_src_id,
            COALESCE(
                (SELECT STRING_AGG(DISTINCT cat.category_src_id::TEXT, ''|'' ORDER BY cat.category_src_id::TEXT)
                 FROM BL_3NF.CE_PRODUCT_CATEGORIES pcat
                 JOIN BL_3NF.CE_CATEGORIES cat ON pcat.category_id = cat.category_id
                 WHERE pcat.product_id = p.product_id),
                pc.category_src_id::TEXT
            )::TEXT as all_category_src_ids,
            COALESCE(
                (SELECT STRING_AGG(DISTINCT cat.category_name, ''|'' ORDER BY cat.category_name)
                 FROM BL_3NF.CE_PRODUCT_CATEGORIES pcat
                 JOIN BL_3NF.CE_CATEGORIES cat ON pcat.category_id = cat.category_id
                 WHERE pcat.product_id = p.product_id),
                pc.category_name
            )::TEXT as all_category_names,
            COALESCE(ps.status_name, ''Unknown''::VARCHAR(50))::VARCHAR(50) as status_name,
            COALESCE(ps.status_src_id, ''Unknown'')::VARCHAR(50) as status_src_id,
            ''3NF_LAYER''::VARCHAR(50) as source_system,
            ''CE_PRODUCTS_SCD''::VARCHAR(100) as source_entity,
            p.start_dt::DATE as effective_date
        FROM %s.CE_PRODUCTS_SCD p
        LEFT JOIN %s.CE_BRANDS b ON p.brand_id = b.brand_id
        LEFT JOIN %s.CE_CATEGORIES pc ON p.primary_category_id = pc.category_id
        LEFT JOIN %s.CE_DEPARTMENTS d ON pc.department_id = d.department_id
        LEFT JOIN %s.CE_PRODUCT_STATUSES ps ON p.status_id = ps.status_id
        %s
        ORDER BY p.product_src_id, p.start_dt DESC',
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

-- Function to detect SCD2 changes for a product
CREATE OR REPLACE FUNCTION BL_CL.detect_product_scd2_changes(
    p_new_product BL_CL.t_product_hierarchy,
    p_existing_product BL_CL.t_product_hierarchy
) RETURNS BL_CL.t_dim_scd2_change_record
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_change_record BL_CL.t_dim_scd2_change_record;
    v_changes       TEXT[]      := ARRAY []::TEXT[];
    v_change_type   VARCHAR(20) := 'UNCHANGED';
BEGIN
    -- Compare each attribute for changes
    IF COALESCE(p_new_product.product_name, '') != COALESCE(p_existing_product.product_name, '') THEN
        v_changes := array_append(v_changes, 'product_name');
        v_change_type := 'CHANGED';
    END IF;

    -- Compare brand_src_id (business key) instead of brand_name
    IF COALESCE(p_new_product.brand_src_id, '') != COALESCE(p_existing_product.brand_src_id, '') THEN
        v_changes := array_append(v_changes, 'brand_src_id');
        v_change_type := 'CHANGED';
    END IF;

    -- Also check brand name for display purposes
    IF COALESCE(p_new_product.brand_name, '') != COALESCE(p_existing_product.brand_name, '') THEN
        v_changes := array_append(v_changes, 'brand_name');
        v_change_type := 'CHANGED';
    END IF;

    -- Compare category_src_id (business key) instead of category name only
    IF COALESCE(p_new_product.primary_category_src_id, '') !=
       COALESCE(p_existing_product.primary_category_src_id, '') THEN
        v_changes := array_append(v_changes, 'primary_category_src_id');
        v_change_type := 'CHANGED';
    END IF;

    IF COALESCE(p_new_product.primary_category_name, '') != COALESCE(p_existing_product.primary_category_name, '') THEN
        v_changes := array_append(v_changes, 'primary_category_name');
        v_change_type := 'CHANGED';
    END IF;

    -- Compare department_src_id (business key)
    IF COALESCE(p_new_product.department_src_id, '') != COALESCE(p_existing_product.department_src_id, '') THEN
        v_changes := array_append(v_changes, 'department_src_id');
        v_change_type := 'CHANGED';
    END IF;

    IF COALESCE(p_new_product.department_name, '') != COALESCE(p_existing_product.department_name, '') THEN
        v_changes := array_append(v_changes, 'department_name');
        v_change_type := 'CHANGED';
    END IF;

    -- Compare status_src_id (business key)
    IF COALESCE(p_new_product.status_src_id, '') != COALESCE(p_existing_product.status_src_id, '') THEN
        v_changes := array_append(v_changes, 'status_src_id');
        v_change_type := 'CHANGED';
    END IF;

    IF COALESCE(p_new_product.status_name, '') != COALESCE(p_existing_product.status_name, '') THEN
        v_changes := array_append(v_changes, 'status_name');
        v_change_type := 'CHANGED';
    END IF;

    -- Compare all_category_src_ids (business keys) instead of names only
    IF COALESCE(p_new_product.all_category_src_ids, '') != COALESCE(p_existing_product.all_category_src_ids, '') THEN
        v_changes := array_append(v_changes, 'all_category_src_ids');
        v_change_type := 'CHANGED';
    END IF;

    IF COALESCE(p_new_product.all_category_names, '') != COALESCE(p_existing_product.all_category_names, '') THEN
        v_changes := array_append(v_changes, 'all_category_names');
        v_change_type := 'CHANGED';
    END IF;

    -- Build change record
    v_change_record := ROW (
        p_new_product.product_src_id, -- source_key
        v_change_type, -- change_type
        CASE
            WHEN array_length(v_changes, 1) > 0 THEN array_to_string(v_changes, ', ')
            ELSE 'No changes detected'
            END, -- change_reason
        p_new_product.effective_date, -- effective_date
        '9999-12-31'::DATE, -- expiration_date
        NULL, -- old_surrogate_key (will be set later)
        NULL, -- new_surrogate_key (will be set later)
        CASE
            WHEN array_length(v_changes, 1) > 0 THEN
                array_to_string(ARRAY(SELECT unnest(v_changes)), '|')
            ELSE NULL
            END, -- attribute_changes
        CASE
            WHEN array_length(v_changes, 1) > 3 THEN 'HIGH'
            WHEN array_length(v_changes, 1) > 1 THEN 'MEDIUM'
            WHEN array_length(v_changes, 1) = 1 THEN 'LOW'
            ELSE 'NONE'
            END -- confidence_level
        )::BL_CL.t_dim_scd2_change_record;

    RETURN v_change_record;
END
$$;

-- Function to build dynamic UPSERT statement for products SCD2
CREATE OR REPLACE FUNCTION BL_CL.build_product_scd2_upsert_sql(
    p_config BL_CL.t_dim_load_config,
    p_operation VARCHAR(20) -- 'INSERT_NEW', 'UPDATE_EXISTING', 'CLOSE_EXPIRED'
) RETURNS TEXT
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_sql TEXT;
BEGIN
    CASE p_operation
        WHEN 'INSERT_NEW' THEN v_sql := FORMAT('
                INSERT INTO %s (
                    PRODUCT_SRC_ID, PRODUCT_NAME, BRAND_NAME, BRAND_SRC_ID,
                    PRIMARY_CATEGORY_SRC_ID, PRIMARY_CATEGORY_NAME, DEPARTMENT_SRC_ID, DEPARTMENT_NAME,
                    ALL_CATEGORY_SRC_IDS, ALL_CATEGORY_NAMES, PRODUCT_STATUS_NAME, PRODUCT_STATUS_SRC_ID,
                    START_DT, END_DT, IS_ACTIVE, SOURCE_SYSTEM, SOURCE_ENTITY, TA_INSERT_DT, TA_UPDATE_DT
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)',
                                               p_config.target_table
                                        );

        WHEN 'CLOSE_EXPIRED' THEN v_sql := FORMAT('
                UPDATE %s
                SET END_DT = $1, IS_ACTIVE = ''N'', TA_UPDATE_DT = $2
                WHERE PRODUCT_SRC_ID = $3 AND IS_ACTIVE = ''Y'' AND SOURCE_SYSTEM = $4',
                                                  p_config.target_table
                                           );

        WHEN 'UPDATE_EXISTING' THEN v_sql := FORMAT('
                UPDATE %s
                SET PRODUCT_NAME = $1, BRAND_NAME = $2, BRAND_SRC_ID = $3,
                    PRIMARY_CATEGORY_NAME = $4, PRIMARY_CATEGORY_SRC_ID = $5,
                    DEPARTMENT_NAME = $6, DEPARTMENT_SRC_ID = $7,
                    ALL_CATEGORY_SRC_IDS = $8, ALL_CATEGORY_NAMES = $9,
                    PRODUCT_STATUS_NAME = $10, PRODUCT_STATUS_SRC_ID = $11,
                    TA_UPDATE_DT = $12
                WHERE PRODUCT_SRC_ID = $13 AND IS_ACTIVE = ''Y'' AND SOURCE_SYSTEM = $14',
                                                    p_config.target_table
                                             );

        ELSE RAISE EXCEPTION 'Unknown operation: %', p_operation;
        END CASE;

    RETURN v_sql;
END
$$;

-- =====================================================
-- SECTION 2: MAIN DIMENSION LOADING PROCEDURE WITH CURSOR VARIABLES
-- =====================================================

-- MAIN PROCEDURE: Load DIM_PRODUCTS_SCD using cursor variables for flexible SCD2 processing
CREATE OR REPLACE PROCEDURE BL_CL.load_dim_products_scd(
    p_config BL_CL.t_dim_load_config DEFAULT NULL
)
    LANGUAGE plpgsql
AS
$$
DECLARE
    -- Composite type variables
    v_config                BL_CL.t_dim_load_config;
    v_result                BL_CL.t_dim_load_result;
    v_scd2_changes          BL_CL.t_dim_scd2_change_record[] := ARRAY []::BL_CL.t_dim_scd2_change_record[];
    v_scd2_change           BL_CL.t_dim_scd2_change_record;

    -- CURSOR VARIABLES for different processing modes
    source_cursor           REFCURSOR;
    active_cursor           REFCURSOR;

    -- Processing variables
    v_start_time            TIMESTAMP                        := CURRENT_TIMESTAMP;
    v_execution_time        INTEGER;
    v_last_update_dt        TIMESTAMP;
    v_has_initial_data      BOOLEAN                          := FALSE;
    v_is_initial_load       BOOLEAN                          := FALSE;

    -- Cursor FOR loop variables
    product_rec             BL_CL.t_product_hierarchy;
    existing_product        BL_CL.t_product_hierarchy;

    -- Dynamic SQL variables
    v_insert_sql            TEXT;
    v_close_sql             TEXT;
    v_update_sql            TEXT;

    -- Result tracking
    v_rows_inserted         INTEGER                          := 0;
    v_rows_updated          INTEGER                          := 0;
    v_rows_closed           INTEGER                          := 0;
    v_total_processed       INTEGER                          := 0;
    v_scd2_changes_detected INTEGER                          := 0;
    v_validation_errors     INTEGER                          := 0;
    v_business_errors       INTEGER                          := 0;

    -- Variables for product processing
    v_current_product_id    TEXT                             := '';
    v_product_processed     BOOLEAN                          := FALSE;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_dim_products_scd') THEN
        RAISE EXCEPTION 'Procedure load_dim_products_scd is already running';
    END IF;

    -- Initialize configuration with defaults
    v_config := COALESCE(p_config, ROW (
        'BL_3NF.CE_PRODUCTS_SCD', -- source_table
        'BL_DM.DIM_PRODUCTS_SCD', -- target_table
        'PRODUCT_SRC_ID', -- business_key_column
        'PRODUCT_SURR_ID', -- surrogate_key_column
        'DELTA', -- load_mode
        '3NF_LAYER', -- include_source_system
        'STRICT', -- validation_level
        1000, -- batch_size
        TRUE -- enable_logging
        )::BL_CL.t_dim_load_config);

    -- Log procedure start
    IF v_config.enable_logging THEN
        CALL BL_CL.log_procedure_event(
                'load_dim_products_scd',
                v_config.source_table,
                v_config.target_table,
                'START',
                0,
                FORMAT('Starting DIM_PRODUCTS_SCD load with CORRECTED SCD2 - Mode: %s, Source System: %s',
                       v_config.load_mode, v_config.include_source_system)
             );
    END IF;

    -- STEP 1: Determine if this is initial or incremental load
    SELECT COUNT(*) > 0
    INTO v_has_initial_data
    FROM BL_DM.DIM_PRODUCTS_SCD
    WHERE source_system = '3NF_LAYER'
      AND product_surr_id != -1;

    v_is_initial_load := NOT v_has_initial_data;

    IF v_config.enable_logging THEN
        CALL BL_CL.log_procedure_event(
                'load_dim_products_scd', v_config.source_table, v_config.target_table, 'INFO',
                0, FORMAT('Load type determined: %s (has_initial_data: %s)',
                          CASE WHEN v_is_initial_load THEN 'INITIAL' ELSE 'INCREMENTAL' END, v_has_initial_data)
             );
    END IF;

    -- Get last successful load time for delta processing
    IF v_config.load_mode = 'DELTA' AND NOT v_is_initial_load THEN
        v_last_update_dt := BL_CL.get_last_successful_load('load_dim_products_scd');

        IF v_config.enable_logging THEN
            CALL BL_CL.log_procedure_event(
                    'load_dim_products_scd', v_config.source_table, v_config.target_table, 'INFO',
                    0, FORMAT('Delta load - last successful load: %s', v_last_update_dt)
                 );
        END IF;
    END IF;

    -- Prepare dynamic SQL statements
    v_insert_sql := BL_CL.build_product_scd2_upsert_sql(v_config, 'INSERT_NEW');
    v_close_sql := BL_CL.build_product_scd2_upsert_sql(v_config, 'CLOSE_EXPIRED');
    v_update_sql := BL_CL.build_product_scd2_upsert_sql(v_config, 'UPDATE_EXISTING');

    -- STEP 2: Open cursor based on load type
    IF v_is_initial_load THEN
        --         -- For initial load, get only the LATEST version of each product
--         OPEN source_cursor FOR
--             SELECT DISTINCT ON (product_src_id) *
--             FROM BL_CL.get_product_hierarchy_data(v_config, NULL)
--             ORDER BY product_src_id, effective_date DESC;

        -- for Initial load should get ALL historical versions
            OPEN source_cursor FOR
                SELECT * -- Get ALL versions, not just latest
                FROM BL_CL.get_product_hierarchy_data(v_config, NULL)
                ORDER BY product_src_id, effective_date ASC; -- Process chronologically

            IF v_config.enable_logging THEN
                CALL BL_CL.log_procedure_event(
                        'load_dim_products_scd', v_config.source_table, v_config.target_table, 'INFO',
                        0, 'INITIAL LOAD: Processing latest version of each product only'
                     );
            END IF;

        ELSIF v_config.load_mode = 'FULL' THEN
            -- For full reload, process with SCD2 logic but get all  records
            OPEN source_cursor FOR
                SELECT DISTINCT ON (product_src_id) *
                FROM BL_CL.get_product_hierarchy_data(v_config, NULL)
                WHERE product_src_id IN (SELECT product_src_id
                                         FROM BL_3NF.CE_PRODUCTS_SCD
                                         --WHERE is_active = 'Y'
                                         )
                ORDER BY product_src_id, effective_date DESC;

            IF v_config.enable_logging THEN
                CALL BL_CL.log_procedure_event(
                        'load_dim_products_scd', v_config.source_table, v_config.target_table, 'INFO',
                        0, 'FULL RELOAD: Processing all active products with SCD2 logic'
                     );
            END IF;

        ELSE -- DELTA load
        -- For delta load, get products that changed since last load
            OPEN source_cursor FOR
                SELECT DISTINCT ON (product_src_id) *
                FROM BL_CL.get_product_hierarchy_data(v_config, v_last_update_dt)
                ORDER BY product_src_id, effective_date DESC;

            IF v_config.enable_logging THEN
                CALL BL_CL.log_procedure_event(
                        'load_dim_products_scd', v_config.source_table, v_config.target_table, 'INFO',
                        0, FORMAT('DELTA LOAD: Processing products changed since %s', v_last_update_dt)
                     );
            END IF;
        END IF;

        -- STEP 3: Process each product with proper SCD2 logic
        LOOP
            FETCH source_cursor INTO product_rec;
            EXIT WHEN NOT FOUND;

            v_total_processed := v_total_processed + 1;
            v_product_processed := FALSE;

            BEGIN
                -- Check if product already exists in DM layer
                OPEN active_cursor FOR
                    SELECT product_src_id,
                           product_name,
                           brand_name,
                           brand_src_id,
                           primary_category_name,
                           primary_category_src_id,
                           department_name,
                           department_src_id,
                           all_category_src_ids,
                           all_category_names,
                           product_status_name   as status_name,
                           product_status_src_id as status_src_id,
                           source_system,
                           source_entity,
                           start_dt              as effective_date
                    FROM BL_DM.DIM_PRODUCTS_SCD
                    WHERE product_src_id = product_rec.product_src_id
                      AND is_active = 'Y'
                      AND source_system = '3NF_LAYER';

                FETCH active_cursor INTO existing_product;

                IF FOUND THEN
                    -- Product exists - check for changes
                    v_scd2_change := BL_CL.detect_product_scd2_changes(product_rec, existing_product);
                    v_scd2_changes := array_append(v_scd2_changes, v_scd2_change);

                    IF v_scd2_change.change_type = 'CHANGED' THEN
                        v_scd2_changes_detected := v_scd2_changes_detected + 1;

                        -- Close existing active record
                        EXECUTE v_close_sql USING
                            CURRENT_DATE,---CURRENT_DATE - 1, -- $1 end_dt (yesterday)
                            CURRENT_TIMESTAMP, -- $2 ta_update_dt
                            product_rec.product_src_id, -- $3 product_src_id
                            '3NF_LAYER'; -- $4 source_system

                        v_rows_closed := v_rows_closed + 1;

                        -- Insert new active record
                        EXECUTE v_insert_sql USING
                            product_rec.product_src_id, product_rec.product_name, product_rec.brand_name, product_rec.brand_src_id,
                            product_rec.primary_category_src_id, product_rec.primary_category_name,
                            product_rec.department_src_id, product_rec.department_name,
                            product_rec.all_category_src_ids, product_rec.all_category_names,
                            product_rec.status_name, product_rec.status_src_id,
                            CURRENT_DATE, '9999-12-31'::DATE, 'Y',
                            product_rec.source_system, product_rec.source_entity, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP;

                        v_rows_inserted := v_rows_inserted + 1;
                        v_product_processed := TRUE;

                        IF v_config.enable_logging THEN
                            CALL BL_CL.log_procedure_event(
                                    'load_dim_products_scd', v_config.source_table, v_config.target_table, 'INFO',
                                    0, FORMAT('SCD2 change detected for product %s: %s',
                                              product_rec.product_src_id, v_scd2_change.change_reason)
                                 );
                        END IF;
                    ELSE
                        -- No changes detected - skip
                        IF v_config.enable_logging THEN
                            CALL BL_CL.log_procedure_event(
                                    'load_dim_products_scd', v_config.source_table, v_config.target_table, 'DEBUG',
                                    0, FORMAT('No changes for product %s - skipping', product_rec.product_src_id)
                                 );
                        END IF;
                    END IF;
                ELSE
                    -- New product - insert
                    EXECUTE v_insert_sql USING
                        product_rec.product_src_id, product_rec.product_name, product_rec.brand_name, product_rec.brand_src_id,
                        product_rec.primary_category_src_id, product_rec.primary_category_name,
                        product_rec.department_src_id, product_rec.department_name,
                        product_rec.all_category_src_ids, product_rec.all_category_names,
                        product_rec.status_name, product_rec.status_src_id,
                        product_rec.effective_date, '9999-12-31'::DATE, 'Y',
                        product_rec.source_system, product_rec.source_entity, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP;

                    v_rows_inserted := v_rows_inserted + 1;
                    v_product_processed := TRUE;

                    IF v_config.enable_logging THEN
                        CALL BL_CL.log_procedure_event(
                                'load_dim_products_scd', v_config.source_table, v_config.target_table, 'INFO',
                                0, FORMAT('New product inserted: %s - %s', product_rec.product_src_id,
                                          product_rec.product_name)
                             );
                    END IF;
                END IF;

                CLOSE active_cursor;

            EXCEPTION
                WHEN OTHERS THEN
                    v_business_errors := v_business_errors + 1;
                    IF v_config.enable_logging THEN
                        CALL BL_CL.log_procedure_event(
                                'load_dim_products_scd', v_config.source_table, v_config.target_table, 'WARNING',
                                0, FORMAT('Error processing product %s: %s', product_rec.product_src_id, SQLERRM)
                             );
                    END IF;

                    -- Ensure cursor is closed on error
                    IF active_cursor IS NOT NULL THEN
                        CLOSE active_cursor;
                    END IF;
            END;
        END LOOP;

        CLOSE source_cursor;

        -- STEP 4: Calculate execution time and build result
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

        v_result := ROW (
            v_rows_inserted,
            v_rows_updated,
            v_rows_closed, -- Using closed count for "deleted"
            v_total_processed - v_rows_inserted - v_rows_updated - v_rows_closed,
            v_validation_errors,
            v_business_errors,
            v_execution_time,
            v_total_processed,
            v_start_time,
            CURRENT_TIMESTAMP,
            CASE
                WHEN v_business_errors = 0 THEN 'SUCCESS'
                WHEN v_business_errors < (v_total_processed * 0.1) THEN 'WARNING'
                ELSE 'ERROR'
                END,
            FORMAT(
                    'DIM_PRODUCTS_SCD load completed with CORRECTED SCD2 - Mode: %s (%s), Processed: %s, Inserted: %s, Closed: %s, SCD2 Changes: %s',
                    v_config.load_mode,
                    CASE WHEN v_is_initial_load THEN 'INITIAL' ELSE 'INCREMENTAL' END,
                    v_total_processed, v_rows_inserted, v_rows_closed, v_scd2_changes_detected),
            CASE
                WHEN array_length(v_scd2_changes, 1) > 0 THEN
                    FORMAT('SCD2 changes detected for %s products', v_scd2_changes_detected)
                ELSE NULL END
            )::BL_CL.t_dim_load_result;

        -- Log successful completion
        IF v_config.enable_logging THEN
            CALL BL_CL.log_procedure_event(
                    'load_dim_products_scd',
                    v_config.source_table,
                    v_config.target_table,
                    v_result.status,
                -- OLD: v_total_processed,  -- This was counting scanned records as "changes"
                    v_rows_inserted + v_rows_updated + v_rows_closed, -- FIXED: Only count actual changes
                    v_result.message,
                    v_execution_time
                 );
        END IF;

        -- Release procedure lock
        PERFORM BL_CL.release_procedure_lock('load_dim_products_scd');

        EXCEPTION
    WHEN OTHERS THEN
    -- Release procedure lock on error
    PERFORM BL_CL.release_procedure_lock('load_dim_products_scd');

    -- Close any open cursors
    IF source_cursor IS NOT NULL THEN
        CLOSE source_cursor;
    END IF;
    IF active_cursor IS NOT NULL THEN
        CLOSE active_cursor;
    END IF;

    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    IF v_config.enable_logging THEN
        CALL BL_CL.log_procedure_event(
                'load_dim_products_scd',
                COALESCE(v_config.source_table, 'Unknown'),
                COALESCE(v_config.target_table, 'Unknown'),
                'ERROR',
            -- OLD: v_total_processed,  -- This was counting scanned records as "changes"
                v_rows_inserted + v_rows_updated + v_rows_closed, -- FIXED: Only count actual changes
                SQLERRM,
                v_execution_time,
                SQLSTATE
             );
    END IF;

    RAISE;
END
$$;


CREATE OR REPLACE PROCEDURE BL_CL.load_dim_products_scd_enhanced(
    p_config BL_CL.t_dim_load_config,
    p_product_list TEXT[] DEFAULT NULL
)
    LANGUAGE plpgsql
AS
$$
DECLARE
    source_cursor           REFCURSOR;
    active_cursor           REFCURSOR;
    product_rec             BL_CL.t_product_hierarchy;
    existing_product        BL_CL.t_product_hierarchy;
    v_scd2_change           BL_CL.t_dim_scd2_change_record;
    v_insert_sql            TEXT;
    v_close_sql             TEXT;
    v_rows_inserted         INTEGER   := 0;
    v_rows_closed           INTEGER   := 0;
    v_total_processed       INTEGER   := 0;
    v_total_fetched         INTEGER   := 0;  -- NEW: Track what we actually fetch
    v_scd2_changes_detected INTEGER   := 0;
    v_start_time            TIMESTAMP := CURRENT_TIMESTAMP;
    v_debug_product_count   INTEGER   := 0;  -- NEW: Debug counter
BEGIN
    CALL BL_CL.log_procedure_event(
            'load_dim_products_scd_enhanced', p_config.source_table, p_config.target_table,
            'START', 0, FORMAT('Processing %s specific products: %s',
                              array_length(p_product_list, 1),
                              array_to_string(p_product_list, ', '))
         );

    -- Prepare SQL
    v_insert_sql := BL_CL.build_product_scd2_upsert_sql(p_config, 'INSERT_NEW');
    v_close_sql := BL_CL.build_product_scd2_upsert_sql(p_config, 'CLOSE_EXPIRED');

    -- DEBUG: First check how many products the hierarchy function will return
    SELECT COUNT(*)
    INTO v_debug_product_count
    FROM BL_CL.get_product_hierarchy_data(p_config, NULL)
    WHERE product_src_id = ANY (p_product_list);

    CALL BL_CL.log_procedure_event(
            'load_dim_products_scd_enhanced', p_config.source_table, p_config.target_table,
            'DEBUG', 0,
            FORMAT('Hierarchy function returned %s products for requested list of %s',
                   v_debug_product_count, array_length(p_product_list, 1))
         );

    -- FIXED: Open cursor for specific products only (no timestamp filtering for targeted products!)
    OPEN source_cursor FOR
        SELECT DISTINCT ON (product_src_id) *
        FROM BL_CL.get_product_hierarchy_data(p_config, NULL) -- NULL = no timestamp filter!
        WHERE product_src_id = ANY (p_product_list)
        ORDER BY product_src_id, effective_date DESC;

    -- Process each product
    LOOP
        FETCH source_cursor INTO product_rec;
        EXIT WHEN NOT FOUND;

        v_total_fetched := v_total_fetched + 1; -- Track what we actually fetch

        CALL BL_CL.log_procedure_event(
                'load_dim_products_scd_enhanced', p_config.source_table, p_config.target_table,
                'DEBUG', 0,
                FORMAT('Processing product %s/%s: %s - %s',
                       v_total_fetched, v_debug_product_count,
                       product_rec.product_src_id, product_rec.product_name)
             );

        v_total_processed := v_total_processed + 1;

        -- Get existing active product
        OPEN active_cursor FOR
            SELECT product_src_id,
                   product_name,
                   brand_name,
                   brand_src_id,
                   primary_category_name,
                   primary_category_src_id,
                   department_name,
                   department_src_id,
                   all_category_src_ids,
                   all_category_names,
                   product_status_name   as status_name,
                   product_status_src_id as status_src_id,
                   source_system,
                   source_entity,
                   start_dt              as effective_date
            FROM BL_DM.DIM_PRODUCTS_SCD
            WHERE product_src_id = product_rec.product_src_id
              AND is_active = 'Y'
              AND source_system = '3NF_LAYER';

        FETCH active_cursor INTO existing_product;

        IF FOUND THEN
            -- Existing product - check for changes
            CALL BL_CL.log_procedure_event(
                    'load_dim_products_scd_enhanced', p_config.source_table, p_config.target_table,
                    'DEBUG', 0,
                    FORMAT('Found existing product %s in DM, checking for changes', product_rec.product_src_id)
                 );

            v_scd2_change := BL_CL.detect_product_scd2_changes(product_rec, existing_product);

            IF v_scd2_change.change_type = 'CHANGED' THEN
                v_scd2_changes_detected := v_scd2_changes_detected + 1;

                -- Close existing record
                EXECUTE v_close_sql USING
                    CURRENT_DATE, -- End date is today
                    CURRENT_TIMESTAMP,
                    product_rec.product_src_id,
                    '3NF_LAYER';
                v_rows_closed := v_rows_closed + 1;

                -- Insert new record
                EXECUTE v_insert_sql USING
                    product_rec.product_src_id, product_rec.product_name, product_rec.brand_name, product_rec.brand_src_id,
                    product_rec.primary_category_src_id, product_rec.primary_category_name,
                    product_rec.department_src_id, product_rec.department_name,
                    product_rec.all_category_src_ids, product_rec.all_category_names,
                    product_rec.status_name, product_rec.status_src_id,
                    CURRENT_DATE, '9999-12-31'::DATE, 'Y',
                    product_rec.source_system, product_rec.source_entity, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP;
                v_rows_inserted := v_rows_inserted + 1;

                CALL BL_CL.log_procedure_event(
                        'load_dim_products_scd_enhanced', p_config.source_table, p_config.target_table,
                        'INFO', 0,
                        FORMAT('SCD2 change: %s (%s) -> %s',
                               product_rec.product_src_id,
                               v_scd2_change.change_reason,
                               product_rec.product_name)
                     );
            ELSE
                CALL BL_CL.log_procedure_event(
                        'load_dim_products_scd_enhanced', p_config.source_table, p_config.target_table,
                        'DEBUG', 0,
                        FORMAT('No changes for existing product: %s', product_rec.product_src_id)
                     );
            END IF;
        ELSE
            -- NEW PRODUCT - insert directly
            CALL BL_CL.log_procedure_event(
                    'load_dim_products_scd_enhanced', p_config.source_table, p_config.target_table,
                    'DEBUG', 0,
                    FORMAT('Product %s not found in DM - inserting as NEW', product_rec.product_src_id)
                 );

            EXECUTE v_insert_sql USING
                product_rec.product_src_id, product_rec.product_name, product_rec.brand_name, product_rec.brand_src_id,
                product_rec.primary_category_src_id, product_rec.primary_category_name,
                product_rec.department_src_id, product_rec.department_name,
                product_rec.all_category_src_ids, product_rec.all_category_names,
                product_rec.status_name, product_rec.status_src_id,
                product_rec.effective_date, '9999-12-31'::DATE, 'Y',
                product_rec.source_system, product_rec.source_entity, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP;
            v_rows_inserted := v_rows_inserted + 1;

            CALL BL_CL.log_procedure_event(
                    'load_dim_products_scd_enhanced', p_config.source_table, p_config.target_table,
                    'INFO', 0,
                    FORMAT('NEW PRODUCT inserted: %s - %s', product_rec.product_src_id, product_rec.product_name)
                 );
        END IF;

        CLOSE active_cursor;
    END LOOP;

    CLOSE source_cursor;

    CALL BL_CL.log_procedure_event(
            'load_dim_products_scd_enhanced', p_config.source_table, p_config.target_table,
            'SUCCESS', v_total_processed,
            FORMAT('Enhanced delta completed: %s fetched, %s processed, %s changes, %s closed, %s inserted',
                   v_total_fetched, v_total_processed, v_scd2_changes_detected, v_rows_closed, v_rows_inserted)
         );
END
$$;



-- =====================================================
-- SECTION 3: CONVENIENCE WRAPPER PROCEDURES
-- =====================================================

-- Simple wrapper for default delta loading
CREATE OR REPLACE PROCEDURE BL_CL.load_dim_products_scd_delta()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_config              BL_CL.t_dim_load_config;
    v_last_update_dt      TIMESTAMP;
    v_products_to_process TEXT[];
    product_id            TEXT;
    v_content_changes     INTEGER := 0;
    v_timestamp_changes   INTEGER := 0;
    v_new_products        INTEGER := 0;
BEGIN
    -- Initialize config
    v_config := ROW (
        'BL_3NF.CE_PRODUCTS_SCD', 'BL_DM.DIM_PRODUCTS_SCD',
        'PRODUCT_SRC_ID', 'PRODUCT_SURR_ID',
        'DELTA', '3NF_LAYER', 'STRICT', 1000, TRUE
        )::BL_CL.t_dim_load_config;

    -- Get last successful load
    v_last_update_dt := BL_CL.get_last_successful_load('load_dim_products_scd');

    -- FIXED: Enhanced detection that properly handles new products
    WITH content_based_changes AS (
        -- Products where current 3NF state differs from DM state (PRIORITY)
        SELECT DISTINCT
            p3nf.product_src_id,
            CASE
                WHEN pdm.product_src_id IS NULL THEN 'NEW_PRODUCT'
                ELSE 'CONTENT_MISMATCH'
            END as reason
        FROM BL_3NF.CE_PRODUCTS_SCD p3nf
        LEFT JOIN BL_DM.DIM_PRODUCTS_SCD pdm ON (
            p3nf.product_src_id = pdm.product_src_id
            AND pdm.is_active = 'Y'
            AND pdm.source_system = '3NF_LAYER'
        )
        LEFT JOIN BL_3NF.CE_BRANDS b ON p3nf.brand_id = b.brand_id
        LEFT JOIN BL_3NF.CE_CATEGORIES c ON p3nf.primary_category_id = c.category_id
        LEFT JOIN BL_3NF.CE_DEPARTMENTS d ON c.department_id = d.department_id
        LEFT JOIN BL_3NF.CE_PRODUCT_STATUSES ps ON p3nf.status_id = ps.status_id
        WHERE p3nf.is_active = 'Y'
          AND (
            pdm.product_src_id IS NULL -- NEW PRODUCT - This was missing!
            OR pdm.product_name != COALESCE(p3nf.product_name, 'Unknown') -- Name changed
            OR COALESCE(pdm.brand_name, '') != COALESCE(b.brand_name, 'Unknown') -- Brand changed
            OR COALESCE(pdm.brand_src_id, '') != COALESCE(b.brand_src_id, 'Unknown') -- Brand key changed
            OR COALESCE(pdm.primary_category_name, '') != COALESCE(c.category_name, 'Unknown') -- Category changed
            OR COALESCE(pdm.primary_category_src_id, '') != COALESCE(c.category_src_id, 'Unknown') -- Category key changed
            OR COALESCE(pdm.department_name, '') != COALESCE(d.department_name, 'Unknown') -- Department changed
            OR COALESCE(pdm.department_src_id, '') != COALESCE(d.department_src_id, 'Unknown') -- Department key changed
            OR COALESCE(pdm.product_status_name, '') != COALESCE(ps.status_name, 'Unknown') -- Status changed
            OR COALESCE(pdm.product_status_src_id, '') != COALESCE(ps.status_src_id, 'Unknown') -- Status key changed
        )),
         timestamp_based_changes AS (
             -- Products updated since last load (SECONDARY) - excluding those already found above
             SELECT DISTINCT p.product_src_id, 'TIMESTAMP_NEWER' as reason
             FROM BL_3NF.CE_PRODUCTS_SCD p
             WHERE p.is_active = 'Y'
               AND p.ta_update_dt > v_last_update_dt
               AND p.product_src_id NOT IN (SELECT product_src_id FROM content_based_changes))
    SELECT
        ARRAY_AGG(DISTINCT product_src_id ORDER BY product_src_id),
        COUNT(CASE WHEN reason = 'CONTENT_MISMATCH' THEN 1 END),
        COUNT(CASE WHEN reason = 'TIMESTAMP_NEWER' THEN 1 END),
        COUNT(CASE WHEN reason = 'NEW_PRODUCT' THEN 1 END)
    INTO v_products_to_process, v_content_changes, v_timestamp_changes, v_new_products
    FROM (
        SELECT product_src_id, reason FROM content_based_changes
        UNION ALL
        SELECT product_src_id, reason FROM timestamp_based_changes
    ) all_changes;

    -- Log what we found with detailed breakdown including new products
    CALL BL_CL.log_procedure_event(
            'load_dim_products_scd_smart_delta', 'BL_3NF.CE_PRODUCTS_SCD', 'BL_DM.DIM_PRODUCTS_SCD',
            'INFO', 0,
            FORMAT('Enhanced delta detected %s products: %s (New: %s, Content: %s, Timestamp: %s)',
                   COALESCE(array_length(v_products_to_process, 1), 0),
                   COALESCE(array_to_string(v_products_to_process, ', '), 'NONE'),
                   v_new_products,
                   v_content_changes,
                   v_timestamp_changes)
         );

    -- If we have products to process, run a targeted load
    IF v_products_to_process IS NOT NULL AND array_length(v_products_to_process, 1) > 0 THEN
        -- Call the main procedure, but we'll modify it to use our enhanced detection
        CALL BL_CL.load_dim_products_scd_enhanced(v_config, v_products_to_process);
    ELSE
        CALL BL_CL.log_procedure_event(
                'load_dim_products_scd_smart_delta', 'BL_3NF.CE_PRODUCTS_SCD', 'BL_DM.DIM_PRODUCTS_SCD',
                'SUCCESS', 0, 'No products require processing - delta load skipped'
             );
    END IF;
END
$$;


-- Simple wrapper for full reload
CREATE OR REPLACE PROCEDURE BL_CL.load_dim_products_scd_full()
    LANGUAGE plpgsql
AS
$$
BEGIN
    CALL BL_CL.load_dim_products_scd(
            ROW (
                'BL_3NF.CE_PRODUCTS_SCD', 'BL_DM.DIM_PRODUCTS_SCD',
                'PRODUCT_SRC_ID', 'PRODUCT_SURR_ID',
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
  AND routine_name LIKE '%product%'
ORDER BY routine_name;

-- Test the product hierarchy data function
-- Test the hierarchy data function
SELECT product_src_id, product_name, brand_src_id, primary_category_src_id, department_src_id
FROM BL_CL.get_product_hierarchy_data(
        ROW ('BL_3NF.CE_PRODUCTS_SCD', 'BL_DM.DIM_PRODUCTS_SCD',
            'PRODUCT_SRC_ID', 'PRODUCT_SURR_ID',
            'FULL', 'ALL', 'STRICT', 0, TRUE)::BL_CL.t_dim_load_config,
        NULL
     )
LIMIT 5;

-- Test SCD2 change detection with business keys
SELECT BL_CL.detect_product_scd2_changes(
               ROW ('PROD001', 'Product A', 'Brand X', 'BRAND_001',
                   'Electronics', 'CAT_001', 'Technology', 'DEPT_001',
                   'CAT_001|CAT_002', 'Electronics|Gadgets', 'Active', 'STATUS_001',
                   '3NF_LAYER', 'CE_PRODUCTS_SCD', CURRENT_DATE)::BL_CL.t_product_hierarchy,
               ROW ('PROD001', 'Product A', 'Brand Y', 'BRAND_002',
                   'Electronics', 'CAT_001', 'Technology', 'DEPT_001',
                   'CAT_001', 'Electronics', 'Active', 'STATUS_001',
                   '3NF_LAYER', 'CE_PRODUCTS_SCD', CURRENT_DATE)::BL_CL.t_product_hierarchy
       );

-- Test dynamic SQL builders
SELECT BL_CL.build_product_scd2_upsert_sql(
               ROW ('BL_3NF.CE_PRODUCTS_SCD', 'BL_DM.DIM_PRODUCTS_SCD', 'PRODUCT_SRC_ID', 'PRODUCT_SURR_ID',
                   'DELTA', '3NF_LAYER', 'STRICT', 0, TRUE)::BL_CL.t_dim_load_config,
               'INSERT_NEW'
       ) as insert_sql_sample;

COMMIT;

-- =====================================================
-- READY FOR TESTING
-- Usage Examples:
--
-- -- 1. Initial/Full product load with cursor variables:
CALL BL_CL.load_dim_products_scd_full();
--
select count(*)
from bl_dm.dim_products_scd;
-- --
-- SELECT product_src_id, product_name, start_dt, end_dt, is_active, brand_name
-- FROM BL_DM.DIM_PRODUCTS_SCD
-- WHERE product_surr_id != -1
-- ORDER BY product_src_id, start_dt;
-- --
-- -- -- 3. Test change detection with product 1014
-- UPDATE BL_3NF.CE_PRODUCTS_SCD
-- SET product_name = 'UPDATED',
--     ta_update_dt = CURRENT_TIMESTAMP
-- WHERE product_src_id = '1014'
--   AND is_active = 'Y';
--
-- UPDATE sa_oms.src_oms
-- set product_name = 'O''Brien Men''s Neoprene Life Vest',
--     ta_update_dt = CURRENT_TIMESTAMP
-- WHERE product_src_id = '1014';
--
-- select product_src_id, product_name, count(transaction_src_id) as count
-- from sa_oms.src_oms
-- WHERE product_src_id = '1014'
-- group by product_src_id, product_name
-- ;

--O'Brien Men's Neoprene Life Vest
-- -- 4. Run incremental load
-- CALL BL_CL.load_dim_products_scd_delta();
--
-- 2. Incremental SCD2 load with change detection:
-- CALL BL_CL.load_dim_products_scd_delta();
--
-- 3. Custom configuration with specific batch size:
-- CALL BL_CL.load_dim_products_scd(
--     ROW('BL_3NF.CE_PRODUCTS_SCD', 'BL_DM.DIM_PRODUCTS_SCD',
--         'PRODUCT_SRC_ID', 'PRODUCT_SURR_ID',
--         'DELTA', '3NF_LAYER', 'RELAXED', 100, TRUE)::BL_CL.t_dim_load_config
-- );
--
-- 4. Test hierarchy data extraction:
-- SELECT * FROM BL_CL.get_product_hierarchy_data(
--     ROW('BL_3NF.CE_PRODUCTS_SCD', 'BL_DM.DIM_PRODUCTS_SCD',
--         'PRODUCT_SRC_ID', 'PRODUCT_SURR_ID',
--         'FULL', 'ALL', 'STRICT', 0, TRUE)::BL_CL.t_dim_load_config, NULL
-- ) WHERE product_src_id = '1014';
--
-- 5. Check SCD2 versions:
-- SELECT product_src_id, product_name, start_dt, end_dt, is_active
-- FROM BL_DM.DIM_PRODUCTS_SCD
-- WHERE product_src_id = '1014'
-- ORDER BY start_dt;
-- =====================================================