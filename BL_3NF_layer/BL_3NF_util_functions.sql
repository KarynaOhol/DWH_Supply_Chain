-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - UTILITY FUNCTIONS
-- Purpose: Create utility functions for 3NF data loading
-- Run as: dwh_cleansing_user
-- =====================================================

SELECT CURRENT_USER, SESSION_USER;

SET ROLE dwh_cleansing_user;
SET search_path = BL_CL, BL_3NF, SA_OMS, SA_LMS, public;

-- =====================================================
-- SECTION 1: TABLE-RETURNING FUNCTIONS
-- =====================================================

-- Note: Customer staging function removed - will use original proven INSERT logic with deduplication

-- Function returns table for staging geographic data
CREATE OR REPLACE FUNCTION BL_CL.get_staging_geographies()
    RETURNS TABLE
            (
                destination_city    VARCHAR(100),
                destination_state   VARCHAR(100),
                destination_country VARCHAR(100),
                geography_key       VARCHAR(255),
                source_system       VARCHAR(50),
                source_entity       VARCHAR(100),
                ta_insert_dt        TIMESTAMPTZ,
                ta_update_dt        TIMESTAMPTZ
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        SELECT DISTINCT COALESCE(l.destination_city, 'Unknown')::VARCHAR(100),
                        COALESCE(l.destination_state, 'Unknown')::VARCHAR(100),
                        COALESCE(l.destination_country, 'Unknown')::VARCHAR(100),
                        CONCAT(
                                COALESCE(l.destination_city, 'Unknown'), '|',
                                COALESCE(l.destination_state, 'Unknown'), '|',
                                COALESCE(l.destination_country, 'Unknown')
                        )::VARCHAR(255)         as geography_key,
                        'LMS'::VARCHAR(50)      as source_system,
                        'SRC_LMS'::VARCHAR(100) as source_entity,
                        CURRENT_TIMESTAMP       as ta_insert_dt,
                        CURRENT_TIMESTAMP       as ta_update_dt
        FROM SA_LMS.SRC_LMS l
        WHERE l.destination_city IS NOT NULL
          AND l.destination_state IS NOT NULL
          AND l.destination_country IS NOT NULL;
END
$$;

-- Function returns table for staging products (for SCD2)
CREATE OR REPLACE FUNCTION BL_CL.get_staging_products()
    RETURNS TABLE
            (
                product_src_id          VARCHAR(50),
                product_name            VARCHAR(255),
                product_brand           VARCHAR(100),
                product_category_src_id VARCHAR(50),
                product_status          VARCHAR(50),
                source_system           VARCHAR(50),
                source_entity           VARCHAR(100),
                latest_order_date       VARCHAR(20),
                ta_insert_dt            TIMESTAMPTZ,
                ta_update_dt            TIMESTAMPTZ
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        WITH product_latest AS (SELECT o.product_src_id,
                                       o.product_name,
                                       o.product_brand,
                                       o.product_category_src_id,
                                       o.product_status,
                                       o.order_dt,
                                       o.transaction_src_id, -- Add this for deterministic tie-breaking
                                       ROW_NUMBER() OVER (
                                           PARTITION BY o.product_src_id
                                           -- FIXED: Use business date, not technical timestamps
                                           ORDER BY o.order_dt DESC, o.transaction_src_id DESC
                                           ) as rn
                                FROM SA_OMS.SRC_OMS o
                                WHERE o.product_src_id IS NOT NULL
                                  AND o.product_src_id != '')
        SELECT pl.product_src_id::VARCHAR(50),
               COALESCE(pl.product_name, 'Unknown')::VARCHAR(255),
               COALESCE(pl.product_brand, 'Unknown')::VARCHAR(100),
               COALESCE(pl.product_category_src_id, 'Unknown')::VARCHAR(50),
               COALESCE(pl.product_status, 'Unknown')::VARCHAR(50),
               'OMS'::VARCHAR(50)       as source_system,
               'SRC_OMS'::VARCHAR(100)  as source_entity,
               pl.order_dt::VARCHAR(20) as latest_order_date,
               CURRENT_TIMESTAMP        as ta_insert_dt,
               CURRENT_TIMESTAMP        as ta_update_dt
        FROM product_latest pl
        WHERE pl.rn = 1;
END
$$;

-- =====================================================
-- SECTION 2: FOR LOOP PROCESSING FUNCTIONS
-- =====================================================

-- Note: Customer processing function removed - will use original proven INSERT logic with ROW_NUMBER() deduplication

-- Function that processes geographic hierarchy using FOR LOOP
CREATE OR REPLACE FUNCTION BL_CL.process_geography_batch()
    RETURNS INTEGER
    LANGUAGE plpgsql
AS
$$
DECLARE
    geo_rec         RECORD;
    processed_count INTEGER := 0;
    v_region_id     BIGINT;
    v_country_id    BIGINT;
    v_state_id      BIGINT;
    v_city_id       BIGINT;
    v_geography_id  BIGINT;
BEGIN
    -- FOR LOOP over geographic staging data
    FOR geo_rec IN
        SELECT DISTINCT destination_city,
                        destination_state,
                        destination_country,
                        geography_key,
                        source_system,
                        source_entity,
                        ta_insert_dt,
                        ta_update_dt
        FROM BL_CL.get_staging_geographies()
        LOOP
            -- Process Country (assuming regions are pre-loaded)
            SELECT country_id
            INTO v_country_id
            FROM BL_3NF.CE_COUNTRIES
            WHERE country_src_id = geo_rec.destination_country
              AND source_system = geo_rec.source_system;

            -- Process State
            SELECT state_id
            INTO v_state_id
            FROM BL_3NF.CE_STATES
            WHERE state_src_id = geo_rec.destination_state
              AND source_system = geo_rec.source_system;

            -- Process City
            SELECT city_id
            INTO v_city_id
            FROM BL_3NF.CE_CITIES
            WHERE city_src_id = geo_rec.destination_city
              AND source_system = geo_rec.source_system;

            -- Process Geography (final level)
            SELECT geography_id
            INTO v_geography_id
            FROM BL_3NF.CE_GEOGRAPHIES
            WHERE geography_src_id = geo_rec.geography_key
              AND source_system = geo_rec.source_system;

            IF v_geography_id IS NULL AND v_city_id IS NOT NULL THEN
                INSERT INTO BL_3NF.CE_GEOGRAPHIES (geography_src_id,
                                                   city_id,
                                                   source_system,
                                                   source_entity)
                VALUES (geo_rec.geography_key,
                        v_city_id,
                        geo_rec.source_system,
                        geo_rec.source_entity);

                processed_count := processed_count + 1;
            END IF;
        END LOOP;

    RETURN processed_count;
END
$$;

-- =====================================================
-- SECTION 3: UTILITY FUNCTIONS FOR DIMENSION LOOKUPS
-- =====================================================

-- Generic function to get dimension ID by source ID
CREATE OR REPLACE FUNCTION BL_CL.get_dimension_id(
    p_table_name VARCHAR(100),
    p_src_id_column VARCHAR(100),
    p_src_id_value VARCHAR(100),
    p_source_system VARCHAR(50) DEFAULT NULL
)
    RETURNS BIGINT
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_sql       TEXT;
    v_result    BIGINT;
    v_id_column VARCHAR(100);
BEGIN
    -- Construct ID column name (remove CE_ prefix and add _ID)
    v_id_column := LOWER(REPLACE(p_table_name, 'CE_', '')) || '_id';

    -- Build dynamic SQL
    v_sql := FORMAT('SELECT %I FROM BL_3NF.%I WHERE %I = $1',
                    v_id_column, p_table_name, p_src_id_column);

    -- Add source system filter if provided
    IF p_source_system IS NOT NULL THEN
        v_sql := v_sql || ' AND source_system = $2';
        EXECUTE v_sql INTO v_result USING p_src_id_value, p_source_system;
    ELSE
        EXECUTE v_sql INTO v_result USING p_src_id_value;
    END IF;

    -- Return -1 if not found (default value)
    RETURN COALESCE(v_result, -1);
END
$$;

-- Specific lookup functions for common dimensions
CREATE OR REPLACE FUNCTION BL_CL.get_customer_id(
    p_customer_src_id VARCHAR(50),
    p_source_system VARCHAR(50) DEFAULT 'OMS'
)
    RETURNS BIGINT
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_customer_id BIGINT;
BEGIN
    SELECT customer_id
    INTO v_customer_id
    FROM BL_3NF.CE_CUSTOMERS
    WHERE customer_src_id = p_customer_src_id
      AND source_system = p_source_system;

    RETURN COALESCE(v_customer_id, -1);
END
$$;

CREATE OR REPLACE FUNCTION BL_CL.get_product_id(
    p_product_src_id VARCHAR(50),
    p_effective_date DATE DEFAULT CURRENT_DATE,
    p_source_system VARCHAR(50) DEFAULT 'OMS'
)
    RETURNS BIGINT
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_product_id BIGINT;
BEGIN
    -- Get active product for the effective date (SCD2)
    SELECT product_id
    INTO v_product_id
    FROM BL_3NF.CE_PRODUCTS_SCD
    WHERE product_src_id = p_product_src_id
      AND source_system = p_source_system
      AND is_active = 'Y'
      AND start_dt <= p_effective_date
      AND end_dt >= p_effective_date;

    RETURN COALESCE(v_product_id, -1);
END
$$;


-- =====================================================
-- SECTION 3.1: IMPROVED SCD LOOKUP FUNCTION
-- =====================================================
-- TODO: implement for loading historical orders for whom product information was updated
-- Create a robust SCD lookup function
CREATE OR REPLACE FUNCTION BL_CL.get_product_id_robust(
    p_product_src_id VARCHAR(50),
    p_order_date DATE,
    p_source_system VARCHAR(10) DEFAULT 'OMS'
) RETURNS INTEGER
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_product_id INTEGER;
BEGIN
    -- First try: Exact date range match with active record
    SELECT product_id
    INTO v_product_id
    FROM BL_3NF.CE_PRODUCTS_SCD
    WHERE product_src_id = p_product_src_id
      AND source_system = p_source_system
      AND is_active = 'Y'
      AND p_order_date BETWEEN start_dt AND end_dt
    LIMIT 1;

    -- Second try: Any active record for this product (fallback)
    IF v_product_id IS NULL THEN
        SELECT product_id
        INTO v_product_id
        FROM BL_3NF.CE_PRODUCTS_SCD
        WHERE product_src_id = p_product_src_id
          AND source_system = p_source_system
          AND is_active = 'Y'
        ORDER BY start_dt DESC -- Get the most recent version
        LIMIT 1;
    END IF;

    -- Third try: Any record for this product (emergency fallback)
    IF v_product_id IS NULL THEN
        SELECT product_id
        INTO v_product_id
        FROM BL_3NF.CE_PRODUCTS_SCD
        WHERE product_src_id = p_product_src_id
          AND source_system = p_source_system
        ORDER BY start_dt DESC
        LIMIT 1;
    END IF;

    RETURN COALESCE(v_product_id, -1);
END
$$;

-- =====================================================
-- SECTION 4: DATA VALIDATION FUNCTIONS
-- =====================================================

-- Function to validate staging data quality
CREATE OR REPLACE FUNCTION BL_CL.validate_staging_data(
    p_source_table VARCHAR(100)
)
    RETURNS TABLE
            (
                validation_rule       VARCHAR(100),
                failed_count          INTEGER,
                sample_failed_records TEXT
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    IF p_source_table = 'SA_OMS.SRC_OMS' THEN
        RETURN QUERY
            -- Check for missing customer IDs
            SELECT 'Missing Customer IDs'::VARCHAR(100),
                   COUNT(*)::INTEGER,
                   STRING_AGG(DISTINCT transaction_src_id, ', ')::TEXT
            FROM SA_OMS.SRC_OMS
            WHERE customer_src_id IS NULL
               OR customer_src_id = ''

            UNION ALL

            -- Check for missing product IDs
            SELECT 'Missing Product IDs'::VARCHAR(100),
                   COUNT(*)::INTEGER,
                   STRING_AGG(DISTINCT transaction_src_id, ', ')::TEXT
            FROM SA_OMS.SRC_OMS
            WHERE product_src_id IS NULL
               OR product_src_id = ''

            UNION ALL

            -- Check for invalid dates
            SELECT 'Invalid Order Dates'::VARCHAR(100),
                   COUNT(*)::INTEGER,
                   STRING_AGG(DISTINCT order_dt, ', ')::TEXT
            FROM SA_OMS.SRC_OMS
            WHERE order_dt IS NULL
               OR order_dt = '';

    ELSIF p_source_table = 'SA_LMS.SRC_LMS' THEN
        RETURN QUERY
            -- Check for missing shipment IDs
            SELECT 'Missing Shipment IDs'::VARCHAR(100),
                   COUNT(*)::INTEGER,
                   STRING_AGG(DISTINCT transaction_src_id, ', ')::TEXT
            FROM SA_LMS.SRC_LMS
            WHERE shipment_src_id IS NULL
               OR shipment_src_id = ''

            UNION ALL

            -- Check for missing geographic data
            SELECT 'Missing Geographic Data'::VARCHAR(100),
                   COUNT(*)::INTEGER,
                   STRING_AGG(DISTINCT shipment_src_id, ', ')::TEXT
            FROM SA_LMS.SRC_LMS
            WHERE destination_city IS NULL
               OR destination_state IS NULL
               OR destination_country IS NULL;
    END IF;
END
$$;

-- =====================================================
-- SECTION 5: TEST FUNCTIONS
-- =====================================================

-- Test all functions
DO
$$
    DECLARE
        v_result      INTEGER;
        v_customer_id BIGINT;
        v_product_id  BIGINT;
    BEGIN
        RAISE NOTICE 'Testing table-returning functions...';

        -- Test geography staging function
        SELECT COUNT(*) INTO v_result FROM BL_CL.get_staging_geographies();
        RAISE NOTICE 'Staging geographies found: %', v_result;

        -- Test product staging function
        SELECT COUNT(*) INTO v_result FROM BL_CL.get_staging_products();
        RAISE NOTICE 'Staging products found: %', v_result;

        RAISE NOTICE 'Testing lookup functions...';

        -- Test dimension lookup (should return -1 for non-existent)
        v_customer_id := BL_CL.get_customer_id('TEST_CUSTOMER', 'OMS');
        RAISE NOTICE 'Test customer lookup result: %', v_customer_id;

        v_product_id := BL_CL.get_product_id('TEST_PRODUCT', CURRENT_DATE, 'OMS');
        RAISE NOTICE 'Test product lookup result: %', v_product_id;

        RAISE NOTICE 'All function tests completed successfully!';
    END
$$;

-- =====================================================
-- SECTION 6: VERIFICATION QUERIES
-- =====================================================

-- Show all created functions
SELECT routine_name,
       routine_type,
       data_type                      as return_type,
       routine_definition IS NOT NULL as has_definition
FROM information_schema.routines
WHERE routine_schema = 'bl_cl'
    AND routine_name LIKE '%staging%'
   OR routine_name LIKE '%process%'
   OR routine_name LIKE '%get_%'
ORDER BY routine_name;

-- Sample data from table-returning functions
SELECT 'Sample Geographies' as data_type, destination_country, destination_state, destination_city
FROM BL_CL.get_staging_geographies()
LIMIT 5;

SELECT 'Sample Products' as data_type, product_src_id, product_name, product_brand
FROM BL_CL.get_staging_products()
LIMIT 5;

COMMIT;