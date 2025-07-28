-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - DIMENSION PROCEDURES
-- Purpose: Create all 20 dimension loading procedures
-- Run as: dwh_cleansing_user
-- Dependencies: Functions, Logging framework, BL_3NF tables
-- =====================================================
SET ROLE dwh_cleansing_user;
-- Set search path to work in BL_CL schema
SET search_path = BL_CL, BL_3NF, SA_OMS, SA_LMS, public;

-- =====================================================
-- SECTION 1: GEOGRAPHIC HIERARCHY PROCEDURES (5)
-- Load in dependency order: Regions → Countries → States → Cities → Geographies
-- =====================================================

-- 1. PROCEDURE: Load CE_REGIONS
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_regions()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_regions') THEN
        RAISE EXCEPTION 'Procedure load_ce_regions is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_regions', 'MANUAL', 'BL_3NF.CE_REGIONS', 'START', 0, 'Starting regions load'
         );

    -- Load standard regions (manual reference data)
    INSERT INTO BL_3NF.CE_REGIONS (region_src_id, region_name, region_code, source_system, source_entity, ta_insert_dt,
                                   ta_update_dt)
    VALUES ('North America', 'North America', 'NAM', 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
           ('South America', 'South America', 'SAM', 'MANUAL', 'DEFAULT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ON CONFLICT (region_src_id, source_system) DO NOTHING;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_regions', 'MANUAL', 'BL_3NF.CE_REGIONS', 'SUCCESS',
            v_rows_affected, 'Regions load completed successfully', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_regions');
EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_regions');
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_regions', 'MANUAL', 'BL_3NF.CE_REGIONS', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- 2. PROCEDURE: Load CE_COUNTRIES
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_countries()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_countries') THEN
        RAISE EXCEPTION 'Procedure load_ce_countries is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_countries', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_COUNTRIES', 'START', 0, 'Starting countries load'
         );

    -- Load countries from LMS staging data
    INSERT INTO BL_3NF.CE_COUNTRIES (country_src_id, country_name, country_code, region_id, source_system,
                                     source_entity, ta_insert_dt, ta_update_dt)
    SELECT DISTINCT COALESCE(l.destination_country, 'Unknown')      as country_src_id,
                    COALESCE(l.destination_country, 'Unknown')      as country_name,
                    LEFT(COALESCE(l.destination_country, 'UNK'), 3) as country_code,
                    CASE
                        WHEN UPPER(l.destination_country) IN ('USA', 'EE. UU.', 'UNITED STATES', 'US') THEN
                            (SELECT region_id FROM BL_3NF.CE_REGIONS WHERE region_name = 'North America' LIMIT 1)
                        WHEN UPPER(l.destination_country) IN ('PUERTO RICO', 'PR') THEN
                            (SELECT region_id FROM BL_3NF.CE_REGIONS WHERE region_name = 'South America' LIMIT 1)
                        ELSE
                            (SELECT region_id FROM BL_3NF.CE_REGIONS WHERE region_name = 'Unknown' LIMIT 1)
                        END                                         as region_id,
                    'LMS'                                           as source_system,
                    'SRC_LMS'                                       as source_entity,
                    CURRENT_TIMESTAMP                               as ta_insert_dt,
                    CURRENT_TIMESTAMP                               as ta_update_dt
    FROM SA_LMS.SRC_LMS l
    WHERE l.destination_country IS NOT NULL
      AND l.destination_country != ''
    ON CONFLICT (country_src_id, source_system) DO UPDATE SET country_name = EXCLUDED.country_name,
                                                              country_code = EXCLUDED.country_code,
                                                              region_id    = EXCLUDED.region_id,
                                                              ta_update_dt = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_countries', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_COUNTRIES', 'SUCCESS',
            v_rows_affected, 'Countries load completed successfully', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_countries');

EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_countries');
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_countries', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_COUNTRIES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- 3. PROCEDURE: Load CE_STATES
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_states()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_states') THEN
        RAISE EXCEPTION 'Procedure load_ce_states is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_states', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_STATES', 'START', 0, 'Starting states load'
         );

    -- Load states from LMS staging data
    INSERT INTO BL_3NF.CE_STATES (state_src_id, state_name, state_code, country_id, source_system, source_entity,
                                  ta_insert_dt, ta_update_dt)
    SELECT DISTINCT COALESCE(l.destination_state, 'Unknown')      as state_src_id,
                    COALESCE(l.destination_state, 'Unknown')      as state_name,
                    LEFT(COALESCE(l.destination_state, 'UNK'), 3) as state_code,
                    COALESCE(c.country_id, -1)                    as country_id,
                    'LMS'                                         as source_system,
                    'SRC_LMS'                                     as source_entity,
                    CURRENT_TIMESTAMP                             as ta_insert_dt,
                    CURRENT_TIMESTAMP                             as ta_update_dt
    FROM SA_LMS.SRC_LMS l
             LEFT JOIN BL_3NF.CE_COUNTRIES c ON c.country_src_id = l.destination_country AND c.source_system = 'LMS'
    WHERE l.destination_state IS NOT NULL
      AND l.destination_state != ''
    ON CONFLICT (state_src_id, country_id, source_system) DO UPDATE SET state_name   = EXCLUDED.state_name,
                                                                        state_code   = EXCLUDED.state_code,
                                                                        ta_update_dt = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_states', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_STATES', 'SUCCESS',
            v_rows_affected, 'States load completed successfully', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_states');

EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_states');
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_states', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_STATES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- 4. PROCEDURE: Load CE_CITIES
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_cities()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_cities') THEN
        RAISE EXCEPTION 'Procedure load_ce_cities is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_cities', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_CITIES', 'START', 0, 'Starting cities load'
         );

    -- Load cities from LMS staging data
    INSERT INTO BL_3NF.CE_CITIES (city_src_id, city_name, city_code, state_id, source_system, source_entity,
                                  ta_insert_dt, ta_update_dt)
    SELECT DISTINCT COALESCE(l.destination_city, 'Unknown')      as city_src_id,
                    COALESCE(l.destination_city, 'Unknown')      as city_name,
                    LEFT(COALESCE(l.destination_city, 'UNK'), 3) as city_code,
                    COALESCE(s.state_id, -1)                     as state_id,
                    'LMS'                                        as source_system,
                    'SRC_LMS'                                    as source_entity,
                    CURRENT_TIMESTAMP                            as ta_insert_dt,
                    CURRENT_TIMESTAMP                            as ta_update_dt
    FROM SA_LMS.SRC_LMS l
             LEFT JOIN BL_3NF.CE_STATES s ON s.state_src_id = l.destination_state AND s.source_system = 'LMS'
    WHERE l.destination_city IS NOT NULL
      AND l.destination_city != ''
    ON CONFLICT (city_src_id, state_id, source_system) DO UPDATE SET city_name    = EXCLUDED.city_name,
                                                                     city_code    = EXCLUDED.city_code,
                                                                     ta_update_dt = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_cities', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_CITIES', 'SUCCESS',
            v_rows_affected, 'Cities load completed successfully', v_execution_time
         );

    PERFORM BL_CL.release_procedure_lock('load_ce_cities');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_cities');
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_cities', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_CITIES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- 5. PROCEDURE: Load CE_GEOGRAPHIES (Uses FOR LOOP function )
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_geographies()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_geographies') THEN
        RAISE EXCEPTION 'Procedure load_ce_geographies is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_geographies', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_GEOGRAPHIES', 'START', 0,
            'Starting geographies load using FOR LOOP function'
         );

    -- Use FOR LOOP function to process geographies (TASK REQUIREMENT)
    v_rows_affected := BL_CL.process_geography_batch();

    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_geographies', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_GEOGRAPHIES', 'SUCCESS',
            v_rows_affected, 'Geographies load completed successfully using FOR LOOP function', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_geographies');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_geographies');
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_geographies', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_GEOGRAPHIES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- =====================================================
-- SECTION 2: PRODUCT HIERARCHY PROCEDURES (7)
-- Load in dependency order: Departments → Categories → Brands → Statuses → Products → Bridge Tables
-- =====================================================

-- 6. PROCEDURE: Load CE_DEPARTMENTS
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_departments()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_departments') THEN
        RAISE EXCEPTION 'Procedure load_ce_departments is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_departments', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_DEPARTMENTS', 'START', 0, 'Starting departments load'
         );

    -- Load departments from OMS staging data
    INSERT INTO BL_3NF.CE_DEPARTMENTS (department_src_id, department_name, source_system, source_entity, ta_insert_dt,
                                       ta_update_dt)
    SELECT DISTINCT COALESCE(o.department_src_id, 'Unknown') as department_src_id,
                    COALESCE(o.department_name, 'Unknown')   as department_name,
                    'OMS'                                    as source_system,
                    'SRC_OMS'                                as source_entity,
                    CURRENT_TIMESTAMP                        as ta_insert_dt,
                    CURRENT_TIMESTAMP                        as ta_update_dt
    FROM SA_OMS.SRC_OMS o
    WHERE o.department_src_id IS NOT NULL
      AND o.department_src_id != ''
    ON CONFLICT (department_src_id, source_system) DO UPDATE SET department_name = EXCLUDED.department_name,
                                                                 ta_update_dt    = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_departments', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_DEPARTMENTS', 'SUCCESS',
            v_rows_affected, 'Departments load completed successfully', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_departments');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_departments');
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_departments', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_DEPARTMENTS', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- 7. PROCEDURE: Load CE_CATEGORIES
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_categories()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_categories') THEN
        RAISE EXCEPTION 'Procedure load_ce_categories is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_categories', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_CATEGORIES', 'START', 0, 'Starting categories load'
         );

    -- Load categories from OMS staging data
    INSERT INTO BL_3NF.CE_CATEGORIES (category_src_id, category_name, category_code, department_id, source_system,
                                      source_entity, ta_insert_dt, ta_update_dt)
    SELECT DISTINCT COALESCE(o.product_category_src_id, 'Unknown') as category_src_id,
                    COALESCE(o.product_category, 'Unknown')        as category_name,
                    LEFT(COALESCE(o.product_category, 'UNK'), 10)  as category_code,
                    COALESCE(d.department_id, -1)                  as department_id,
                    'OMS'                                          as source_system,
                    'SRC_OMS'                                      as source_entity,
                    CURRENT_TIMESTAMP                              as ta_insert_dt,
                    CURRENT_TIMESTAMP                              as ta_update_dt
    FROM SA_OMS.SRC_OMS o
             LEFT JOIN BL_3NF.CE_DEPARTMENTS d ON d.department_src_id = o.department_src_id AND d.source_system = 'OMS'
    WHERE o.product_category_src_id IS NOT NULL
      AND o.product_category_src_id != ''
    ON CONFLICT (category_src_id, source_system) DO UPDATE SET category_name = EXCLUDED.category_name,
                                                               category_code = EXCLUDED.category_code,
                                                               department_id = EXCLUDED.department_id,
                                                               ta_update_dt  = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_categories', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_CATEGORIES', 'SUCCESS',
            v_rows_affected, 'Categories load completed successfully', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_categories');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_categories');

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_categories', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_CATEGORIES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- 8. PROCEDURE: Load CE_BRANDS
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_brands()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_brands') THEN
        RAISE EXCEPTION 'Procedure load_ce_brands is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_brands', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_BRANDS', 'START', 0, 'Starting brands load'
         );

    -- Load brands from OMS staging data
    INSERT INTO BL_3NF.CE_BRANDS (brand_src_id, brand_name, source_system, source_entity, ta_insert_dt, ta_update_dt)
    SELECT DISTINCT COALESCE(o.product_brand, 'Unknown') as brand_src_id,
                    COALESCE(o.product_brand, 'Unknown') as brand_name,
                    'OMS'                                as source_system,
                    'SRC_OMS'                            as source_entity,
                    CURRENT_TIMESTAMP                    as ta_insert_dt,
                    CURRENT_TIMESTAMP                    as ta_update_dt
    FROM SA_OMS.SRC_OMS o
    WHERE o.product_brand IS NOT NULL
      AND o.product_brand != ''
    ON CONFLICT (brand_src_id, source_system) DO UPDATE SET brand_name   = EXCLUDED.brand_name,
                                                            ta_update_dt = CURRENT_TIMESTAMP;
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_brands', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_BRANDS', 'SUCCESS',
            v_rows_affected, 'Brands load completed successfully', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_brands');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_brands');

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_brands', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_BRANDS', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- 9. PROCEDURE: Load CE_PRODUCT_STATUSES (Uses MERGE )
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_product_statuses()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
    v_updated_count  INTEGER   := 0;
    v_inserted_count INTEGER   := 0;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_product_statuses') THEN
        RAISE EXCEPTION 'Procedure load_ce_product_statuses is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_product_statuses', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_PRODUCT_STATUSES', 'START', 0,
            'Starting product statuses load using MERGE'
         );

    -- Use MERGE for SCD Type 1 behavior (TASK REQUIREMENT)
    WITH staging_data AS (SELECT DISTINCT COALESCE(product_status, 'Unknown') as status_src_id,
                                          COALESCE(product_status, 'Unknown') as status_name,
                                          'OMS'                               as source_system,
                                          'SRC_OMS'                           as source_entity,
                                          CURRENT_TIMESTAMP                   as ta_insert_dt,
                                          CURRENT_TIMESTAMP                   as ta_update_dt
                          FROM SA_OMS.SRC_OMS
                          WHERE product_status IS NOT NULL
                            AND product_status != '')
    INSERT
    INTO BL_3NF.CE_PRODUCT_STATUSES (status_src_id, status_name, source_system, source_entity, ta_insert_dt,
                                     ta_update_dt)
    SELECT status_src_id, status_name, source_system, source_entity, ta_insert_dt, ta_update_dt
    FROM staging_data s
    WHERE NOT EXISTS (SELECT 1
                      FROM BL_3NF.CE_PRODUCT_STATUSES ps
                      WHERE ps.status_src_id = s.status_src_id
                        AND ps.source_system = s.source_system)
    ON CONFLICT (status_src_id, source_system) DO UPDATE SET status_name  = EXCLUDED.status_name,
                                                             ta_update_dt = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_product_statuses', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_PRODUCT_STATUSES', 'SUCCESS',
            v_rows_affected, 'Product statuses load completed successfully using MERGE approach', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_product_statuses');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_product_statuses');

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_product_statuses', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_PRODUCT_STATUSES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- 10. PROCEDURE: Load CE_PRODUCTS_SCD (SCD Type 2 with staging function)
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_products_scd()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time         TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected      INTEGER   := 0;
    v_execution_time     INTEGER;
    v_updated_count      INTEGER   := 0;
    v_inserted_count     INTEGER   := 0;
    v_dataset_start_date DATE      := '2023-01-01';
    v_has_initial_data   BOOLEAN;
    v_staging_count      INTEGER;
    v_existing_count     INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_products_scd') THEN
        RAISE EXCEPTION 'Procedure load_ce_products_scd is already running';
    END IF;

    -- Count staging products
    SELECT COUNT(*) INTO v_staging_count FROM BL_CL.get_staging_products();

    -- Check if we have initial load data (records with historical start date)
    SELECT COUNT(*) > 0
    INTO v_has_initial_data
    FROM BL_3NF.CE_PRODUCTS_SCD
    WHERE start_dt = v_dataset_start_date
      AND source_system = 'OMS'
      AND product_id != -1;
    -- Exclude default record

    -- Count existing business records
    SELECT COUNT(*)
    INTO v_existing_count
    FROM BL_3NF.CE_PRODUCTS_SCD
    WHERE is_active = 'Y'
      AND source_system = 'OMS'
      AND product_id != -1;

    -- Log procedure start with clear detection logic
    CALL BL_CL.log_procedure_event(
            'load_ce_products_scd', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_PRODUCTS_SCD', 'START', 0,
            FORMAT('Products SCD2 load - Historical data exists: %s, Existing records: %s, Staging: %s',
                   v_has_initial_data, v_existing_count, v_staging_count)
         );

    -- STEP 1: Handle Initial Load (if no historical data exists)
    IF NOT v_has_initial_data THEN
        -- This is initial load - insert all staging products with historical date
        INSERT INTO BL_3NF.CE_PRODUCTS_SCD (product_src_id, product_name, brand_id, primary_category_id, status_id,
                                            start_dt, end_dt, is_active, source_system, source_entity,
                                            ta_insert_dt, ta_update_dt)
        SELECT src.product_src_id,
               src.product_name,
               COALESCE(b.brand_id, -1)    as brand_id,
               COALESCE(c.category_id, -1) as primary_category_id,
               COALESCE(ps.status_id, -1)  as status_id,
               v_dataset_start_date        as start_dt, -- Historical date marker
               '9999-12-31'::DATE          as end_dt,
               'Y'                         as is_active,
               src.source_system,
               src.source_entity,
               CURRENT_TIMESTAMP           as ta_insert_dt,
               CURRENT_TIMESTAMP           as ta_update_dt
        FROM BL_CL.get_staging_products() src
                 LEFT JOIN BL_3NF.CE_BRANDS b
                           ON b.brand_src_id = src.product_brand AND b.source_system = 'OMS'
                 LEFT JOIN BL_3NF.CE_CATEGORIES c
                           ON c.category_src_id = src.product_category_src_id AND c.source_system = 'OMS'
                 LEFT JOIN BL_3NF.CE_PRODUCT_STATUSES ps
                           ON ps.status_src_id = src.product_status AND ps.source_system = 'OMS'
        WHERE NOT EXISTS (SELECT 1
                          FROM BL_3NF.CE_PRODUCTS_SCD cp
                          WHERE cp.product_src_id = src.product_src_id
                            AND cp.source_system = 'OMS'
                            AND cp.product_id != -1);

        GET DIAGNOSTICS v_inserted_count = ROW_COUNT;

    ELSE
        -- STEP 2: Handle Incremental Load (historical data exists)

        -- Create temp table for products that need updates
        CREATE TEMP TABLE temp_products_to_update AS
        SELECT DISTINCT p.product_src_id
        FROM BL_3NF.CE_PRODUCTS_SCD p
                 INNER JOIN BL_CL.get_staging_products() src ON p.product_src_id = src.product_src_id
                 LEFT JOIN BL_3NF.CE_BRANDS b
                           ON b.brand_src_id = src.product_brand AND b.source_system = 'OMS'
                 LEFT JOIN BL_3NF.CE_CATEGORIES c
                           ON c.category_src_id = src.product_category_src_id AND c.source_system = 'OMS'
                 LEFT JOIN BL_3NF.CE_PRODUCT_STATUSES ps
                           ON ps.status_src_id = src.product_status AND ps.source_system = 'OMS'
        WHERE p.is_active = 'Y'
          AND p.source_system = 'OMS'
          AND p.product_id != -1
          AND (
            COALESCE(p.product_name, '') != COALESCE(src.product_name, '') OR
            COALESCE(p.brand_id, -1) != COALESCE(b.brand_id, -1) OR
            COALESCE(p.primary_category_id, -1) != COALESCE(c.category_id, -1) OR
            COALESCE(p.status_id, -1) != COALESCE(ps.status_id, -1)
            );

        -- Close changed records
        UPDATE BL_3NF.CE_PRODUCTS_SCD
        SET end_dt       = CURRENT_DATE - 1,
            is_active    = 'N',
            ta_update_dt = CURRENT_TIMESTAMP
        WHERE product_src_id IN (SELECT product_src_id FROM temp_products_to_update)
          AND is_active = 'Y'
          AND source_system = 'OMS'
          AND product_id != -1;

        GET DIAGNOSTICS v_updated_count = ROW_COUNT;

        -- Insert new versions of changed records + completely new products
        INSERT INTO BL_3NF.CE_PRODUCTS_SCD (product_src_id, product_name, brand_id, primary_category_id, status_id,
                                            start_dt, end_dt, is_active, source_system, source_entity,
                                            ta_insert_dt, ta_update_dt)
        SELECT src.product_src_id,
               src.product_name,
               COALESCE(b.brand_id, -1)    as brand_id,
               COALESCE(c.category_id, -1) as primary_category_id,
               COALESCE(ps.status_id, -1)  as status_id,
               CURRENT_DATE                as start_dt, -- Current date for incremental changes
               '9999-12-31'::DATE          as end_dt,
               'Y'                         as is_active,
               src.source_system,
               src.source_entity,
               CURRENT_TIMESTAMP           as ta_insert_dt,
               CURRENT_TIMESTAMP           as ta_update_dt
        FROM BL_CL.get_staging_products() src
                 LEFT JOIN BL_3NF.CE_BRANDS b
                           ON b.brand_src_id = src.product_brand AND b.source_system = 'OMS'
                 LEFT JOIN BL_3NF.CE_CATEGORIES c
                           ON c.category_src_id = src.product_category_src_id AND c.source_system = 'OMS'
                 LEFT JOIN BL_3NF.CE_PRODUCT_STATUSES ps
                           ON ps.status_src_id = src.product_status AND ps.source_system = 'OMS'
        WHERE (
                  -- Changed products
                  src.product_src_id IN (SELECT product_src_id FROM temp_products_to_update)
                      OR
                      -- Completely new products (never existed before)
                  NOT EXISTS (SELECT 1
                              FROM BL_3NF.CE_PRODUCTS_SCD cp
                              WHERE cp.product_src_id = src.product_src_id
                                AND cp.source_system = 'OMS'
                                AND cp.product_id != -1)
                  );

        GET DIAGNOSTICS v_inserted_count = ROW_COUNT;

        -- Clean up temp table
        DROP TABLE temp_products_to_update;
    END IF;

    v_rows_affected := v_updated_count + v_inserted_count;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_products_scd', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_PRODUCTS_SCD', 'SUCCESS',
            v_rows_affected,
            FORMAT('Products SCD2 completed - Mode: %s, Updated: %s, Inserted: %s',
                   CASE WHEN NOT v_has_initial_data THEN 'INITIAL' ELSE 'INCREMENTAL' END,
                   v_updated_count, v_inserted_count),
            v_execution_time
         );

    PERFORM BL_CL.release_procedure_lock('load_ce_products_scd');

EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_products_scd');
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_products_scd', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_PRODUCTS_SCD', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        RAISE;
END
$$;

-- 11. PROCEDURE: Load CE_BRAND_CATEGORIES (Bridge table)
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_brand_categories()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_brand_categories') THEN
        RAISE EXCEPTION 'Procedure load_ce_brand_categories is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_brand_categories', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_BRAND_CATEGORIES', 'START', 0,
            'Starting brand-categories bridge load'
         );

    -- Load brand-category relationships with strength calculation
    INSERT INTO BL_3NF.CE_BRAND_CATEGORIES (brand_id, category_id, relationship_strength, product_count, source_system,
                                            source_entity, ta_insert_dt, ta_update_dt)
    WITH brand_category_stats AS (SELECT b.brand_id,
                                         c.category_id,
                                         COUNT(DISTINCT o.product_src_id) as unique_products,
                                         ROW_NUMBER() OVER (
                                             PARTITION BY b.brand_id
                                             ORDER BY COUNT(DISTINCT o.product_src_id) DESC, COUNT(*) DESC
                                             )                            as strength_rank
                                  FROM SA_OMS.SRC_OMS o
                                           JOIN BL_3NF.CE_BRANDS b
                                                ON b.brand_src_id = o.product_brand AND b.source_system = 'OMS'
                                           JOIN BL_3NF.CE_CATEGORIES c
                                                ON c.category_src_id = o.product_category_src_id AND
                                                   c.source_system = 'OMS'
                                  WHERE o.product_brand IS NOT NULL
                                    AND o.product_category_src_id IS NOT NULL
                                  GROUP BY b.brand_id, c.category_id)
    SELECT bcs.brand_id,
           bcs.category_id,
           CASE
               WHEN bcs.strength_rank = 1 THEN 1 -- Primary category
               WHEN bcs.strength_rank <= 3 THEN 2 -- Secondary categories
               ELSE 3 -- Minor categories
               END             as relationship_strength,
           bcs.unique_products as product_count,
           'OMS'               as source_system,
           'SRC_OMS'           as source_entity,
           CURRENT_TIMESTAMP   as ta_insert_dt,
           CURRENT_TIMESTAMP   as ta_update_dt
    FROM brand_category_stats bcs
    WHERE NOT EXISTS (SELECT 1
                      FROM BL_3NF.CE_BRAND_CATEGORIES bc
                      WHERE bc.brand_id = bcs.brand_id
                        AND bc.category_id = bcs.category_id
                        AND bc.source_system = 'OMS');

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_brand_categories', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_BRAND_CATEGORIES', 'SUCCESS',
            v_rows_affected, 'Brand-categories bridge load completed successfully', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_brand_categories');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_brand_categories');

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_brand_categories', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_BRAND_CATEGORIES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- 12. PROCEDURE: Load CE_PRODUCT_CATEGORIES (Bridge table)
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_product_categories()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_product_categories') THEN
        RAISE EXCEPTION 'Procedure load_ce_product_categories is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_product_categories', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_PRODUCT_CATEGORIES', 'START', 0,
            'Starting product-categories bridge load'
         );

    -- Load product-category relationships
    INSERT INTO BL_3NF.CE_PRODUCT_CATEGORIES (product_id, category_id, is_primary, source_system, source_entity,
                                              ta_insert_dt, ta_update_dt)
    SELECT DISTINCT p.product_id,
                    c.category_id,
                    CASE WHEN c.category_id = p.primary_category_id THEN 'Y' ELSE 'N' END as is_primary,
                    'OMS'                                                                 as source_system,
                    'SRC_OMS'                                                             as source_entity,
                    CURRENT_TIMESTAMP                                                     as ta_insert_dt,
                    CURRENT_TIMESTAMP                                                     as ta_update_dt
    FROM SA_OMS.SRC_OMS o
             JOIN BL_3NF.CE_PRODUCTS_SCD p ON p.product_src_id = o.product_src_id
        AND p.source_system = 'OMS' AND p.is_active = 'Y'
             JOIN BL_3NF.CE_CATEGORIES c ON c.category_src_id = o.product_category_src_id AND c.source_system = 'OMS'
    WHERE o.product_src_id IS NOT NULL
      AND o.product_category_src_id IS NOT NULL
      AND NOT EXISTS (SELECT 1
                      FROM BL_3NF.CE_PRODUCT_CATEGORIES pc
                      WHERE pc.product_id = p.product_id
                        AND pc.category_id = c.category_id
                        AND pc.source_system = 'OMS');

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_product_categories', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_PRODUCT_CATEGORIES', 'SUCCESS',
            v_rows_affected, 'Product-categories bridge load completed successfully', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_product_categories');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_product_categories');

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_product_categories', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_PRODUCT_CATEGORIES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- =====================================================
-- SECTION 3: BUSINESS ENTITY PROCEDURES (4)
-- =====================================================

-- 13. PROCEDURE: Load CE_CUSTOMERS (Uses original  deduplication logic: Customer dimension should have exactly one row per unique customer, regardless of how many order lines they have.
-- -- Load customers with consistent attributes using MIN/MAX f )
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_customers()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_customers') THEN
        RAISE EXCEPTION 'Procedure load_ce_customers is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_customers', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_CUSTOMERS', 'START', 0,
            'Starting customers load with proven deduplication'
         );

    -- Load customers with proven deduplication logic (handles multi-gender issue)
    INSERT INTO BL_3NF.CE_CUSTOMERS (customer_src_id, customer_first_name, customer_last_name, customer_gender,
                                     customer_year_of_birth, customer_email, customer_segment, source_system,
                                     source_entity, ta_insert_dt, ta_update_dt)
    SELECT customer_src_id,
           customer_first_name,
           customer_last_name,
           customer_gender,
           customer_year_of_birth,
           customer_email,
           customer_segment,
           source_system,
           source_entity,
           ta_insert_dt,
           ta_update_dt
    FROM (SELECT customer_src_id,
                 customer_first_name,
                 customer_last_name,
                 customer_gender,
                 customer_year_of_birth,
                 customer_email,
                 customer_segment,
                 source_system,
                 source_entity,
                 ta_insert_dt,
                 ta_update_dt,
                 ROW_NUMBER() OVER (
                     PARTITION BY customer_src_id
                     ORDER BY gender_count DESC, customer_gender
                     ) as rn
          FROM (SELECT COALESCE(customer_src_id, 'Unknown')                 as customer_src_id,
                       MAX(COALESCE(customer_first_name, 'Unknown'))        as customer_first_name,
                       MAX(COALESCE(customer_last_name, 'Unknown'))         as customer_last_name,
                       COALESCE(customer_gender, 'U')                       as customer_gender,
                       MIN(COALESCE(
                               CASE
                                   WHEN customer_year_of_birth ~ '^[0-9]+$'
                                       THEN customer_year_of_birth::INTEGER
                                   ELSE NULL END,
                               1900))                                       as customer_year_of_birth,
                       MAX(COALESCE(customer_email, 'unknown@unknown.com')) as customer_email,
                       MAX(COALESCE(customer_segment, 'Unknown'))           as customer_segment,
                       COUNT(*)                                             as gender_count,
                       'OMS'                                                as source_system,
                       'SRC_OMS'                                            as source_entity,
                       CURRENT_TIMESTAMP                                    as ta_insert_dt,
                       CURRENT_TIMESTAMP                                    as ta_update_dt
                FROM SA_OMS.SRC_OMS
                WHERE customer_src_id IS NOT NULL
                  AND customer_src_id != ''
                GROUP BY customer_src_id, customer_gender) gender_counts) ranked
    WHERE rn = 1
    ON CONFLICT (customer_src_id, source_system) DO UPDATE SET customer_first_name    = EXCLUDED.customer_first_name,
                                                               customer_last_name     = EXCLUDED.customer_last_name,
                                                               customer_gender        = EXCLUDED.customer_gender,
                                                               customer_year_of_birth = EXCLUDED.customer_year_of_birth,
                                                               customer_email         = EXCLUDED.customer_email,
                                                               customer_segment       = EXCLUDED.customer_segment,
                                                               ta_update_dt           = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_customers', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_CUSTOMERS', 'SUCCESS',
            v_rows_affected, 'Customers load completed with proven deduplication logic ', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_customers');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_customers');

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                ' load_ce_customers ', ' SA_OMS.SRC_OMS ', ' BL_3NF.CE_CUSTOMERS ', ' ERROR ',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- 14. PROCEDURE: Load CE_SALES_REPRESENTATIVES
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_sales_representatives()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_sales_representatives') THEN
        RAISE EXCEPTION 'Procedure load_ce_sales_representatives is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_sales_representatives', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_SALES_REPRESENTATIVES', 'START', 0,
            'Starting sales representatives load'
         );

    -- Load sales representatives from OMS staging data
    INSERT INTO BL_3NF.CE_SALES_REPRESENTATIVES (sales_rep_src_id, sales_rep_name, source_system,
                                                 source_entity, ta_insert_dt, ta_update_dt)
    SELECT DISTINCT COALESCE(sales_rep_src_id, 'Unknown')                       as sales_rep_src_id,
                    CONCAT('Sales Rep ', COALESCE(sales_rep_src_id, 'Unknown')) as sales_rep_name,

                    'OMS'                                                       as source_system,
                    'SRC_OMS'                                                   as source_entity,
                    CURRENT_TIMESTAMP                                           as ta_insert_dt,
                    CURRENT_TIMESTAMP                                           as ta_update_dt
    FROM SA_OMS.SRC_OMS
    WHERE sales_rep_src_id IS NOT NULL
      AND sales_rep_src_id != ''
    ON CONFLICT (sales_rep_src_id, source_system) DO UPDATE SET sales_rep_name = EXCLUDED.sales_rep_name,
                                                                ta_update_dt   = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_sales_representatives', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_SALES_REPRESENTATIVES', 'SUCCESS',
            v_rows_affected, 'Sales representatives load completed successfully', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_sales_representatives');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_sales_representatives');

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_sales_representatives', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_SALES_REPRESENTATIVES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- 15. PROCEDURE: Load CE_WAREHOUSES
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_warehouses()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_warehouses') THEN
        RAISE EXCEPTION 'Procedure load_ce_warehouses is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_warehouses', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_WAREHOUSES', 'START', 0, 'Starting warehouses load'
         );

    -- Load warehouses from LMS staging data
    INSERT INTO BL_3NF.CE_WAREHOUSES (warehouse_src_id, warehouse_name, source_system, source_entity, ta_insert_dt,
                                      ta_update_dt)
    SELECT DISTINCT COALESCE(warehouse_src_id, 'Unknown')                       as warehouse_src_id,
                    CONCAT('Warehouse ', COALESCE(warehouse_src_id, 'Unknown')) as warehouse_name,
                    'LMS'                                                       as source_system,
                    'SRC_LMS'                                                   as source_entity,
                    CURRENT_TIMESTAMP                                           as ta_insert_dt,
                    CURRENT_TIMESTAMP                                           as ta_update_dt
    FROM SA_LMS.SRC_LMS
    WHERE warehouse_src_id IS NOT NULL
      AND warehouse_src_id != ''
    ON CONFLICT (warehouse_src_id, source_system) DO UPDATE SET warehouse_name = EXCLUDED.warehouse_name,
                                                                ta_update_dt   = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_warehouses', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_WAREHOUSES', 'SUCCESS',
            v_rows_affected, 'Warehouses load completed successfully', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_warehouses');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_warehouses');

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_warehouses', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_WAREHOUSES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- 16. PROCEDURE: Load CE_CARRIERS
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_carriers()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_carriers') THEN
        RAISE EXCEPTION 'Procedure load_ce_carriers is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_carriers', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_CARRIERS', 'START', 0, 'Starting carriers load'
         );

    -- Load carriers from LMS staging data
    INSERT INTO BL_3NF.CE_CARRIERS (carrier_src_id, carrier_name, carrier_type, source_system, source_entity,
                                    ta_insert_dt, ta_update_dt)
    SELECT DISTINCT COALESCE(carrier_src_id, 'Unknown') as carrier_src_id,
                    COALESCE(carrier_src_id, 'Unknown') as carrier_name,
                    'Standard'                          as carrier_type,
                    'LMS'                               as source_system,
                    'SRC_LMS'                           as source_entity,
                    CURRENT_TIMESTAMP                   as ta_insert_dt,
                    CURRENT_TIMESTAMP                   as ta_update_dt
    FROM SA_LMS.SRC_LMS
    WHERE carrier_src_id IS NOT NULL
      AND carrier_src_id != ''
    ON CONFLICT (carrier_src_id, source_system) DO UPDATE SET carrier_name = EXCLUDED.carrier_name,
                                                              carrier_type = EXCLUDED.carrier_type,
                                                              ta_update_dt = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_carriers', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_CARRIERS', 'SUCCESS',
            v_rows_affected, 'Carriers load completed successfully', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_carriers');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_carriers');

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_carriers', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_CARRIERS', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- =====================================================
-- SECTION 4: OPERATIONAL DIMENSION PROCEDURES (4)
-- =====================================================

-- 17. PROCEDURE: Load CE_ORDER_STATUSES (Uses MERGE )
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_order_statuses()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_order_statuses') THEN
        RAISE EXCEPTION 'Procedure load_ce_order_statuses is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_order_statuses', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_ORDER_STATUSES', 'START', 0,
            'Starting order statuses load using MERGE'
         );

    -- Use MERGE approach for SCD Type 1 behavior
    INSERT INTO BL_3NF.CE_ORDER_STATUSES (order_status_src_id, order_status, source_system, source_entity, ta_insert_dt,
                                          ta_update_dt)
    SELECT DISTINCT COALESCE(order_status, 'Unknown') as order_status_src_id,
                    COALESCE(order_status, 'Unknown') as order_status,
                    'OMS'                             as source_system,
                    'SRC_OMS'                         as source_entity,
                    CURRENT_TIMESTAMP                 as ta_insert_dt,
                    CURRENT_TIMESTAMP                 as ta_update_dt
    FROM SA_OMS.SRC_OMS
    WHERE order_status IS NOT NULL
      AND order_status != ''
    ON CONFLICT (order_status_src_id, source_system) DO UPDATE SET order_status = EXCLUDED.order_status,
                                                                   ta_update_dt = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_order_statuses', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_ORDER_STATUSES', 'SUCCESS',
            v_rows_affected, 'Order statuses load completed successfully using MERGE approach', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_order_statuses');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_order_statuses');

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_order_statuses', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_ORDER_STATUSES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- 18. PROCEDURE: Load CE_PAYMENT_METHODS
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_payment_methods()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_payment_methods') THEN
        RAISE EXCEPTION 'Procedure load_ce_payment_methods is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_payment_methods', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_PAYMENT_METHODS', 'START', 0,
            'Starting payment methods load'
         );

    -- Load payment methods from OMS staging data
    INSERT INTO BL_3NF.CE_PAYMENT_METHODS (payment_method_src_id, payment_method, source_system, source_entity,
                                           ta_insert_dt, ta_update_dt)
    SELECT DISTINCT COALESCE(payment_method, 'Unknown') as payment_method_src_id,
                    COALESCE(payment_method, 'Unknown') as payment_method,
                    'OMS'                               as source_system,
                    'SRC_OMS'                           as source_entity,
                    CURRENT_TIMESTAMP                   as ta_insert_dt,
                    CURRENT_TIMESTAMP                   as ta_update_dt
    FROM SA_OMS.SRC_OMS
    WHERE payment_method IS NOT NULL
      AND payment_method != ''
    ON CONFLICT (payment_method_src_id, source_system) DO UPDATE SET payment_method = EXCLUDED.payment_method,
                                                                     ta_update_dt   = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_payment_methods', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_PAYMENT_METHODS', 'SUCCESS',
            v_rows_affected, 'Payment methods load completed successfully', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_payment_methods');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_payment_methods');

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_payment_methods', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_PAYMENT_METHODS', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- 19. PROCEDURE: Load CE_SHIPPING_MODES
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_shipping_modes()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_shipping_modes') THEN
        RAISE EXCEPTION 'Procedure load_ce_shipping_modes is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_shipping_modes', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_SHIPPING_MODES', 'START', 0,
            'Starting shipping modes load'
         );

    -- Load shipping modes from LMS staging data
    INSERT INTO BL_3NF.CE_SHIPPING_MODES (shipping_mode_src_id, shipping_mode, source_system, source_entity,
                                          ta_insert_dt, ta_update_dt)
    SELECT DISTINCT COALESCE(shipping_mode, 'Unknown') as shipping_mode_src_id,
                    COALESCE(shipping_mode, 'Unknown') as shipping_mode,
                    'LMS'                              as source_system,
                    'SRC_LMS'                          as source_entity,
                    CURRENT_TIMESTAMP                  as ta_insert_dt,
                    CURRENT_TIMESTAMP                  as ta_update_dt
    FROM SA_LMS.SRC_LMS
    WHERE shipping_mode IS NOT NULL
      AND shipping_mode != ''
    ON CONFLICT (shipping_mode_src_id, source_system) DO UPDATE SET shipping_mode = EXCLUDED.shipping_mode,
                                                                    ta_update_dt  = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_shipping_modes', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_SHIPPING_MODES', 'SUCCESS',
            v_rows_affected, 'Shipping modes load completed successfully', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_shipping_modes');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_shipping_modes');

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_shipping_modes', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_SHIPPING_MODES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- 20. PROCEDURE: Load CE_DELIVERY_STATUSES
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_delivery_statuses()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Check if procedure is already running
    IF NOT BL_CL.acquire_procedure_lock('load_ce_delivery_statuses') THEN
        RAISE EXCEPTION 'Procedure load_ce_delivery_statuses is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_delivery_statuses', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_DELIVERY_STATUSES', 'START', 0,
            'Starting delivery statuses load'
         );

    -- Load delivery statuses from LMS staging data
    INSERT INTO BL_3NF.CE_DELIVERY_STATUSES (delivery_status_src_id, delivery_status, source_system, source_entity,
                                             ta_insert_dt, ta_update_dt)
    SELECT DISTINCT COALESCE(delivery_status, 'Unknown') as delivery_status_src_id,
                    COALESCE(delivery_status, 'Unknown') as delivery_status,
                    'LMS'                                as source_system,
                    'SRC_LMS'                            as source_entity,
                    CURRENT_TIMESTAMP                    as ta_insert_dt,
                    CURRENT_TIMESTAMP                    as ta_update_dt
    FROM SA_LMS.SRC_LMS
    WHERE delivery_status IS NOT NULL
      AND delivery_status != ''
    ON CONFLICT (delivery_status_src_id, source_system) DO UPDATE SET delivery_status = EXCLUDED.delivery_status,
                                                                      ta_update_dt    = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
            'load_ce_delivery_statuses', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_DELIVERY_STATUSES', 'SUCCESS',
            v_rows_affected, 'Delivery statuses load completed successfully', v_execution_time
         );

    --COMMIT;
    PERFORM BL_CL.release_procedure_lock('load_ce_delivery_statuses');


EXCEPTION
    WHEN OTHERS THEN
        PERFORM BL_CL.release_procedure_lock('load_ce_delivery_statuses');

        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_delivery_statuses', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_DELIVERY_STATUSES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        --ROLLBACK;
        RAISE;
END
$$;

-- Verify all procedures were created
SELECT routine_name,
       routine_type,
       routine_definition IS NOT NULL as has_definition
FROM information_schema.routines
WHERE routine_schema = 'bl_cl'
  AND routine_type = 'PROCEDURE'
  AND routine_name LIKE 'load_ce_%'
ORDER BY routine_name;

-- Check recent procedure executions
SELECT procedure_name,
       target_table,
       status,
       rows_affected,
       execution_time_ms,
       message
FROM BL_CL.MTA_PROCESS_LOG
WHERE procedure_name LIKE 'load_ce_%'
  AND log_datetime >= CURRENT_DATE
ORDER BY log_datetime DESC
LIMIT 10;

-- Verify dimension data was loaded by test
SELECT 'CE_REGIONS' as table_name, COUNT(*) as total_records, COUNT(*) - 1 as business_records
FROM BL_3NF.CE_REGIONS
WHERE region_id != -1;

--COMMIT;