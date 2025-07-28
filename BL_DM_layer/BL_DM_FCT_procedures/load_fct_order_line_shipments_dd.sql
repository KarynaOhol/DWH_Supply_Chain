-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - DM LAYER FACT LOADING
-- Purpose: Create partitioned fact table loading with rolling window management
-- Target: FCT_ORDER_LINE_SHIPMENTS_DD in BL_DM
-- Partition Strategy: Monthly partitions on ORDER_DATE (EVENT_DT)
-- Rolling Window: Keep last 3 months active + all historical static
-- =====================================================

SET ROLE dwh_cleansing_user;
SET search_path = BL_CL, BL_DM, BL_3NF, public;

-- =====================================================
-- SECTION 1: PARTITION MANAGEMENT UTILITIES
-- =====================================================

-- Function to create partition name from date
CREATE OR REPLACE FUNCTION BL_CL.get_partition_name(p_table_name TEXT, p_date DATE)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN FORMAT('%s_%s', p_table_name, TO_CHAR(p_date, 'YYYYMM'));
END $$;

-- Function to get partition bounds for a month
CREATE OR REPLACE FUNCTION BL_CL.get_partition_bounds(p_date DATE)
RETURNS TABLE(start_date DATE, end_date DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        DATE_TRUNC('month', p_date)::DATE AS start_date,
        (DATE_TRUNC('month', p_date) + INTERVAL '1 month')::DATE AS end_date;
END $$;

-- Function to check if partition exists
CREATE OR REPLACE FUNCTION BL_CL.partition_exists(p_partition_name TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_exists BOOLEAN;
BEGIN
    -- Check in pg_class for both regular tables and partitions
    SELECT EXISTS(
        SELECT 1
        FROM pg_inherits i
        JOIN pg_class parent ON i.inhparent = parent.oid
        JOIN pg_class child ON i.inhrelid = child.oid
        JOIN pg_namespace pn ON parent.relnamespace = pn.oid
        JOIN pg_namespace cn ON child.relnamespace = cn.oid
        WHERE LOWER(pn.nspname) = 'bl_dm'
        AND LOWER(parent.relname) = 'fct_order_line_shipments_dd'
        AND LOWER(cn.nspname) = 'bl_dm'
        AND (LOWER(child.relname) = LOWER(p_partition_name)
             OR UPPER(child.relname) = UPPER(p_partition_name)
             OR child.relname = p_partition_name)
    ) INTO v_exists;

    -- If not found via inheritance, check pg_tables as backup
    IF NOT v_exists THEN
        SELECT EXISTS(
            SELECT 1 FROM pg_tables
            WHERE LOWER(schemaname) = 'bl_dm'
            AND (LOWER(tablename) = LOWER(p_partition_name)
                 OR UPPER(tablename) = UPPER(p_partition_name)
                 OR tablename = p_partition_name)
        ) INTO v_exists;
    END IF;

    RETURN v_exists;
END $$;

-- =====================================================
-- SECTION 2: PARTITION CREATION PROCEDURE
-- =====================================================

-- Procedure to create a partition for a specific month
CREATE OR REPLACE PROCEDURE BL_CL.create_fact_partition(
    p_partition_date DATE,
    p_attach BOOLEAN DEFAULT TRUE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_partition_name TEXT;
    v_start_date DATE;
    v_end_date DATE;
    v_sql TEXT;
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_execution_time INTEGER;
    v_exists_check BOOLEAN;
    v_actual_partition_name TEXT;
BEGIN
    -- Acquire lock for partition operations
    IF NOT BL_CL.acquire_procedure_lock('create_fact_partition') THEN
        RAISE EXCEPTION 'Another partition operation is in progress';
    END IF;

    -- Log start
    CALL BL_CL.log_procedure_event(
        'create_fact_partition',
        NULL,
        'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
        'START',
        0,
        FORMAT('Creating partition for %s', p_partition_date)
    );

    -- Get partition details
    v_partition_name := BL_CL.get_partition_name('FCT_ORDER_LINE_SHIPMENTS_DD', p_partition_date);
    SELECT start_date, end_date INTO v_start_date, v_end_date
    FROM BL_CL.get_partition_bounds(p_partition_date);

    -- Check if partition exists
    --  Direct inheritance check (most accurate)
    SELECT EXISTS(
        SELECT 1
        FROM pg_inherits i
        JOIN pg_class parent ON i.inhparent = parent.oid
        JOIN pg_class child ON i.inhrelid = child.oid
        JOIN pg_namespace pn ON parent.relnamespace = pn.oid
        JOIN pg_namespace cn ON child.relnamespace = cn.oid
        WHERE LOWER(pn.nspname) = 'bl_dm'
        AND LOWER(parent.relname) = 'fct_order_line_shipments_dd'
        AND LOWER(cn.nspname) = 'bl_dm'
        AND LOWER(child.relname) = LOWER(v_partition_name)
    ) INTO v_exists_check;

    -- Get the actual partition name if it exists
    IF v_exists_check THEN
        SELECT child.relname INTO v_actual_partition_name
        FROM pg_inherits i
        JOIN pg_class parent ON i.inhparent = parent.oid
        JOIN pg_class child ON i.inhrelid = child.oid
        JOIN pg_namespace pn ON parent.relnamespace = pn.oid
        JOIN pg_namespace cn ON child.relnamespace = cn.oid
        WHERE LOWER(pn.nspname) = 'bl_dm'
        AND LOWER(parent.relname) = 'fct_order_line_shipments_dd'
        AND LOWER(cn.nspname) = 'bl_dm'
        AND LOWER(child.relname) = LOWER(v_partition_name)
        LIMIT 1;
    END IF;

    -- Check if partition already exists
    IF v_exists_check THEN
        RAISE NOTICE 'Partition % (actual name: %) already exists and is attached, skipping creation',
                     v_partition_name, v_actual_partition_name;

        -- Log skip event
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
            'create_fact_partition',
            NULL,
            FORMAT('BL_DM.%s', v_actual_partition_name),
            'SKIPPED',
            0,
            FORMAT('Partition already exists and attached for %s', p_partition_date),
            v_execution_time
        );

        PERFORM BL_CL.release_procedure_lock('create_fact_partition');
        RETURN;
    END IF;

    -- Create the partition table with error handling
    v_sql := FORMAT('
        CREATE TABLE BL_DM.%I PARTITION OF BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
        FOR VALUES FROM (%L) TO (%L)',
        v_partition_name, v_start_date, v_end_date);

    BEGIN
        EXECUTE v_sql;
        RAISE NOTICE 'Created partition table: % for dates % to %', v_partition_name, v_start_date, v_end_date;
    EXCEPTION
        WHEN duplicate_table THEN
            RAISE NOTICE 'Partition % already exists (caught during creation), continuing...', v_partition_name;
            PERFORM BL_CL.release_procedure_lock('create_fact_partition');
            RETURN;
        WHEN OTHERS THEN
            -- Check if the error is because partition already exists with different case
            IF SQLERRM LIKE '%already exists%' THEN
                RAISE NOTICE 'Partition % already exists (different case), skipping...', v_partition_name;
                PERFORM BL_CL.release_procedure_lock('create_fact_partition');
                RETURN;
            ELSE
                RAISE EXCEPTION 'Failed to create partition %: %', v_partition_name, SQLERRM;
            END IF;
    END;

    -- Create indexes on the partition
    BEGIN
        v_sql := FORMAT('CREATE INDEX IF NOT EXISTS idx_%s_event_dt ON BL_DM.%I (EVENT_DT)',
                        LOWER(v_partition_name), v_partition_name);
        EXECUTE v_sql;

        v_sql := FORMAT('CREATE INDEX IF NOT EXISTS idx_%s_customer_surr_id ON BL_DM.%I (CUSTOMER_SURR_ID)',
                        LOWER(v_partition_name), v_partition_name);
        EXECUTE v_sql;

        v_sql := FORMAT('CREATE INDEX IF NOT EXISTS idx_%s_product_surr_id ON BL_DM.%I (PRODUCT_SURR_ID)',
                        LOWER(v_partition_name), v_partition_name);
        EXECUTE v_sql;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Failed to create some indexes for partition %: %', v_partition_name, SQLERRM;
    END;

    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log success
    CALL BL_CL.log_procedure_event(
        'create_fact_partition',
        NULL,
        FORMAT('BL_DM.%s', v_partition_name),
        'SUCCESS',
        1,
        FORMAT('Successfully created partition for %s', p_partition_date),
        v_execution_time
    );

    -- Release lock
    PERFORM BL_CL.release_procedure_lock('create_fact_partition');

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

        CALL BL_CL.log_procedure_event(
            'create_fact_partition',
            NULL,
            'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
            'ERROR',
            0,
            SQLERRM,
            v_execution_time,
            SQLSTATE
        );

        PERFORM BL_CL.release_procedure_lock('create_fact_partition');
        RAISE;
END $$;

-- =====================================================
-- SECTION 3: ROLLING WINDOW MANAGEMENT
-- =====================================================

-- Procedure to manage rolling window (detach old partitions)
CREATE OR REPLACE PROCEDURE BL_CL.manage_rolling_window(
    p_months_to_keep INTEGER DEFAULT 3
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_cutoff_date DATE;
    v_partition_record RECORD;
    v_detached_count INTEGER := 0;
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_execution_time INTEGER;
    v_sql TEXT;
BEGIN
    -- Acquire lock
    IF NOT BL_CL.acquire_procedure_lock('manage_rolling_window') THEN
        RAISE EXCEPTION 'Another rolling window operation is in progress';
    END IF;

    -- Log start
    CALL BL_CL.log_procedure_event(
        'manage_rolling_window',
        NULL,
        'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
        'START',
        0,
        FORMAT('Managing rolling window, keeping last %s months', p_months_to_keep)
    );

    -- Calculate cutoff date (only detach partitions older than this)
    v_cutoff_date := DATE_TRUNC('month', CURRENT_DATE - (p_months_to_keep || ' months')::INTERVAL);

    RAISE NOTICE 'Rolling window cutoff date: %', v_cutoff_date;

    -- Find partitions to detach (only future partitions, keep all historical 2023-2025)
    FOR v_partition_record IN
        SELECT
            schemaname,
            tablename,
            -- Extract date from partition name pattern FCT_ORDER_LINE_SHIPMENTS_DD_YYYYMM
            TO_DATE(RIGHT(tablename, 6), 'YYYYMM') as partition_date
        FROM pg_tables
        WHERE schemaname = 'bl_dm'
        AND tablename LIKE 'FCT_ORDER_LINE_SHIPMENTS_DD_%'  -- UPPERCASE pattern
        AND tablename ~ 'FCT_ORDER_LINE_SHIPMENTS_DD_[0-9]{6}$'  --  UPPERCASE regex
        AND TO_DATE(RIGHT(tablename, 6), 'YYYYMM') < v_cutoff_date
        AND TO_DATE(RIGHT(tablename, 6), 'YYYYMM') >= '2025-01-01'::DATE  -- Only manage 2025+ partitions
        ORDER BY tablename
    LOOP
        BEGIN
            -- Detach the partition
            v_sql := FORMAT('ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD DETACH PARTITION BL_DM.%I',
                           v_partition_record.tablename);
            EXECUTE v_sql;

            RAISE NOTICE 'Detached partition: % (date: %)',
                        v_partition_record.tablename,
                        v_partition_record.partition_date;

            v_detached_count := v_detached_count + 1;

        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to detach partition %: %',
                             v_partition_record.tablename, SQLERRM;
        END;
    END LOOP;

    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log completion
    CALL BL_CL.log_procedure_event(
        'manage_rolling_window',
        NULL,
        'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
        'SUCCESS',
        v_detached_count,
        FORMAT('Detached %s old partitions, cutoff date: %s', v_detached_count, v_cutoff_date),
        v_execution_time
    );

    -- Release lock
    PERFORM BL_CL.release_procedure_lock('manage_rolling_window');

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

        CALL BL_CL.log_procedure_event(
            'manage_rolling_window',
            NULL,
            'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
            'ERROR',
            0,
            SQLERRM,
            v_execution_time,
            SQLSTATE
        );

        PERFORM BL_CL.release_procedure_lock('manage_rolling_window');
        RAISE;
END $$;

-- =====================================================
-- SECTION 4: HISTORICAL PARTITIONS SETUP
-- =====================================================

-- Procedure to create all historical partitions (2023-2025)
CREATE OR REPLACE PROCEDURE BL_CL.setup_historical_partitions()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_date DATE := '2023-01-01';
    v_end_date DATE := '2025-12-31';
    v_current_date DATE;
    v_created_count INTEGER := 0;
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_execution_time INTEGER;
BEGIN
    -- Log start
    CALL BL_CL.log_procedure_event(
        'setup_historical_partitions',
        NULL,
        'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
        'START',
        0,
        FORMAT('Creating historical partitions from %s to %s', v_start_date, v_end_date)
    );

    -- Table should already be partitioned, just create the partitions
    RAISE NOTICE 'Creating partitions for partitioned table...';

    -- Create partitions for each month
    v_current_date := v_start_date;
    WHILE v_current_date <= v_end_date LOOP
        BEGIN
            CALL BL_CL.create_fact_partition(v_current_date, TRUE);
            v_created_count := v_created_count + 1;
            RAISE NOTICE 'Created historical partition for %', TO_CHAR(v_current_date, 'YYYY-MM');
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to create partition for %: %', v_current_date, SQLERRM;
        END;

        -- Move to next month
        v_current_date := v_current_date + INTERVAL '1 month';
    END LOOP;

    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log completion
    CALL BL_CL.log_procedure_event(
        'setup_historical_partitions',
        NULL,
        'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
        'SUCCESS',
        v_created_count,
        FORMAT('Created %s historical partitions', v_created_count),
        v_execution_time
    );

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

        CALL BL_CL.log_procedure_event(
            'setup_historical_partitions',
            NULL,
            'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
            'ERROR',
            0,
            SQLERRM,
            v_execution_time,
            SQLSTATE
        );
        RAISE;
END $$;

-- =====================================================
-- SECTION 5: MAIN FACT TABLE LOADING PROCEDURE
-- =====================================================

-- Main procedure to load fact table with partition management
CREATE OR REPLACE PROCEDURE BL_CL.load_fct_order_line_shipments_dd(
    p_incremental BOOLEAN DEFAULT TRUE,
    p_target_date DATE DEFAULT NULL  -- For specific date loading
) -- Uses VARCHAR business key joins to get BIGINT surrogate keys
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_execution_time INTEGER;
    v_last_update_dt TIMESTAMP;
    v_deleted_count INTEGER := 0;
    v_inserted_count INTEGER := 0;
    v_missing_customers INTEGER := 0;
    v_missing_products INTEGER := 0;
    v_missing_dates INTEGER := 0;
    v_business_records INTEGER := 0;
    v_target_partitions TEXT[] := '{}';
    v_partition_name TEXT;
    v_min_order_date DATE;
    v_max_order_date DATE;
    v_current_month DATE;
BEGIN
    -- Acquire procedure lock
    IF NOT BL_CL.acquire_procedure_lock('load_fct_order_line_shipments_dd') THEN
        RAISE EXCEPTION 'Procedure load_fct_order_line_shipments_dd is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
        'load_fct_order_line_shipments_dd',
        'BL_3NF.CE_*',
        'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
        'START',
        0,
        FORMAT('Starting fact load - incremental: %s, target_date: %s', p_incremental, p_target_date)
    );

    -- Determine what data to load
    IF p_incremental THEN
        -- Get last successful update timestamp for delta loading
        v_last_update_dt := BL_CL.get_last_successful_load('load_fct_order_line_shipments_dd');
        RAISE NOTICE 'Processing incremental load since: %', v_last_update_dt;
    ELSE
        -- Full reload
        v_last_update_dt := '1900-01-01'::TIMESTAMP;
        RAISE NOTICE 'Processing full reload of all data';
    END IF;

    -- Get date range of data to be processed
    SELECT
        COALESCE(MIN(o.ORDER_DATE), CURRENT_DATE),
        COALESCE(MAX(o.ORDER_DATE), CURRENT_DATE)
    INTO v_min_order_date, v_max_order_date
    FROM BL_3NF.CE_ORDER_LINES ol
    JOIN BL_3NF.CE_ORDERS o ON ol.ORDER_ID = o.ORDER_ID
    JOIN BL_3NF.CE_SHIPMENT_LINES sl ON ol.ORDER_LINE_ID = sl.ORDER_LINE_ID
    JOIN BL_3NF.CE_SHIPMENTS s ON sl.SHIPMENT_ID = s.SHIPMENT_ID
    WHERE (NOT p_incremental OR
           s.TA_UPDATE_DT > v_last_update_dt OR
           ol.TA_UPDATE_DT > v_last_update_dt OR
           sl.TA_UPDATE_DT > v_last_update_dt)
    AND (p_target_date IS NULL OR o.ORDER_DATE = p_target_date);

    RAISE NOTICE 'Data date range: % to %', v_min_order_date, v_max_order_date;

    -- Create/ensure partitions exist for the date range
    v_current_month := DATE_TRUNC('month', v_min_order_date);
    WHILE v_current_month <= DATE_TRUNC('month', v_max_order_date) LOOP
        v_partition_name := BL_CL.get_partition_name('FCT_ORDER_LINE_SHIPMENTS_DD', v_current_month);

        IF NOT BL_CL.partition_exists(v_partition_name) THEN
            CALL BL_CL.create_fact_partition(v_current_month, TRUE);
            RAISE NOTICE 'Created missing partition: %', v_partition_name;
        END IF;

        v_target_partitions := v_target_partitions || v_partition_name;
        v_current_month := v_current_month + INTERVAL '1 month';
    END LOOP;

    -- Delete existing records for updated shipments (delta approach)
    IF p_incremental THEN
        DELETE FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
        WHERE ORDER_LINE_SHIPMENT_SURR_ID > 0
        AND (SHIPMENT_SRC_ID, ORDER_LINE_SRC_ID) IN (
            SELECT DISTINCT s.SHIPMENT_ID, ol.ORDER_LINE_ID
            FROM BL_3NF.CE_SHIPMENTS s
            JOIN BL_3NF.CE_SHIPMENT_LINES sl ON s.SHIPMENT_ID = sl.SHIPMENT_ID
            JOIN BL_3NF.CE_ORDER_LINES ol ON sl.ORDER_LINE_ID = ol.ORDER_LINE_ID
            WHERE s.TA_UPDATE_DT > v_last_update_dt
               OR ol.TA_UPDATE_DT > v_last_update_dt
               OR sl.TA_UPDATE_DT > v_last_update_dt
        );
    ELSE
        -- Full reload - truncate all partitions
        EXECUTE FORMAT('TRUNCATE TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD');
    END IF;

    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % existing fact records', v_deleted_count;

    -- MAIN FACT LOADING QUERY - CORRECTED WITH VARCHAR BUSINESS KEY JOINS
    INSERT INTO BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD (
        EVENT_DT,
        CUSTOMER_SURR_ID,
        PRODUCT_SURR_ID,
        SALES_REP_SURR_ID,
        ORDER_DT_SURR_ID,
        SHIP_DT_SURR_ID,
        DELIVERY_DT_SURR_ID,
        CUSTOMER_GEOGRAPHY_SURR_ID,
        WAREHOUSE_SURR_ID,
        CARRIER_SURR_ID,
        PAYMENT_METHOD_SURR_ID,
        ORDER_STATUS_SURR_ID,
        SHIPPING_MODE_SURR_ID,
        DELIVERY_STATUS_SURR_ID,
        ORDER_SRC_ID,
        ORDER_LINE_SRC_ID,
        SHIPMENT_SRC_ID,
        SHIPMENT_LINE_SRC_ID,
        DELIVERY_SRC_ID,
        UNIT_PRICE_ACT,
        UNIT_COST_ACT,
        LINE_TOTAL_ACT,
        ORDERED_QUANTITY_CNT,
        ORDER_PROFIT_ACT,
        SHIPPED_QUANTITY_CNT,
        SHIPPING_COST_ACT,
        ALLOCATED_SHIPPING_COST_ACT,
        DELIVERY_DAYS_CNT,
        ORDER_TO_SHIP_DAYS_CNT,
        ON_TIME_DELIVERY_FLAG,
        LATE_DELIVERY_FLAG,
        SHIPPED_SALES_AMOUNT_ACT,
        UNSHIPPED_QUANTITY_CNT,
        FILL_RATE_PCT,
        TA_INSERT_DT,
        TA_UPDATE_DT
    )
    WITH shipment_totals AS (
        SELECT s.SHIPMENT_ID,
               s.SHIPPING_COST,
               SUM(ol.LINE_TOTAL) as TOTAL_SHIPMENT_VALUE
        FROM BL_3NF.CE_SHIPMENTS s
        JOIN BL_3NF.CE_SHIPMENT_LINES sl ON s.SHIPMENT_ID = sl.SHIPMENT_ID
        JOIN BL_3NF.CE_ORDER_LINES ol ON sl.ORDER_LINE_ID = ol.ORDER_LINE_ID
        WHERE (NOT p_incremental OR
               s.TA_UPDATE_DT > v_last_update_dt OR
               ol.TA_UPDATE_DT > v_last_update_dt OR
               sl.TA_UPDATE_DT > v_last_update_dt)
        AND (p_target_date IS NULL OR EXISTS(
            SELECT 1 FROM BL_3NF.CE_ORDERS o2
            WHERE o2.ORDER_ID = ol.ORDER_ID AND o2.ORDER_DATE = p_target_date
        ))
        GROUP BY s.SHIPMENT_ID, s.SHIPPING_COST
    ),
    fact_base AS (
        SELECT
            -- EVENT_DT is ORDER_DATE for partitioning
            o.ORDER_DATE as EVENT_DT,

            -- Natural Keys for degenerate dimensions
            o.ORDER_ID as ORDER_SRC_ID,
            ol.ORDER_LINE_ID as ORDER_LINE_SRC_ID,
            s.SHIPMENT_ID as SHIPMENT_SRC_ID,
            sl.SHIPMENT_LINE_ID as SHIPMENT_LINE_SRC_ID,
            d.DELIVERY_ID as DELIVERY_SRC_ID,

            -- Core business data
            o.ORDER_DATE,
            s.SHIP_DATE,
            d.DELIVERY_DATE,
            ol.QUANTITY as ORDERED_QUANTITY,
            ol.UNIT_PRICE,
            ol.UNIT_COST,
            ol.LINE_TOTAL,
            sl.SHIPPED_QUANTITY,
            s.SHIPPING_COST,
            st.TOTAL_SHIPMENT_VALUE,
            d.DELIVERY_DAYS,
            d.ON_TIME_DELIVERY,

            -- VARCHAR BUSINESS KEYS for dimension lookups
            c.customer_src_id as CUSTOMER_SRC_ID,          -- VARCHAR(50) business key
            p.product_src_id as PRODUCT_SRC_ID,            -- VARCHAR(50) business key
            sr.sales_rep_src_id as SALES_REP_SRC_ID,       -- VARCHAR(50) business key
            g.geography_src_id as GEOGRAPHY_SRC_ID,        -- VARCHAR(50) business key
            w.warehouse_src_id as WAREHOUSE_SRC_ID,        -- VARCHAR(50) business key
            car.carrier_src_id as CARRIER_SRC_ID,          -- VARCHAR(50) business key
            pm.payment_method_src_id as PAYMENT_METHOD_SRC_ID,  -- VARCHAR(50) business key
            os.order_status_src_id as ORDER_STATUS_SRC_ID, -- VARCHAR(50) business key
            sm.shipping_mode_src_id as SHIPPING_MODE_SRC_ID,    -- VARCHAR(50) business key
            ds.delivery_status_src_id as DELIVERY_STATUS_SRC_ID -- VARCHAR(50) business key

        FROM BL_3NF.CE_ORDER_LINES ol
        JOIN BL_3NF.CE_ORDERS o ON ol.ORDER_ID = o.ORDER_ID
        JOIN BL_3NF.CE_SHIPMENT_LINES sl ON ol.ORDER_LINE_ID = sl.ORDER_LINE_ID
        JOIN BL_3NF.CE_SHIPMENTS s ON sl.SHIPMENT_ID = s.SHIPMENT_ID
        LEFT JOIN BL_3NF.CE_DELIVERIES d ON s.SHIPMENT_ID = d.SHIPMENT_ID
        LEFT JOIN shipment_totals st ON s.SHIPMENT_ID = st.SHIPMENT_ID

        -- Join to dimension source tables to get VARCHAR business keys
        LEFT JOIN BL_3NF.CE_CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
        LEFT JOIN BL_3NF.CE_PRODUCTS_SCD p ON ol.PRODUCT_ID = p.PRODUCT_ID
            --AND p.IS_ACTIVE = 'Y'
            AND o.ORDER_DATE BETWEEN p.START_DT AND p.END_DT
        LEFT JOIN BL_3NF.CE_SALES_REPRESENTATIVES sr ON o.SALES_REP_ID = sr.SALES_REP_ID
        LEFT JOIN BL_3NF.CE_GEOGRAPHIES g ON s.GEOGRAPHY_ID = g.GEOGRAPHY_ID
        LEFT JOIN BL_3NF.CE_WAREHOUSES w ON s.WAREHOUSE_ID = w.WAREHOUSE_ID
        LEFT JOIN BL_3NF.CE_CARRIERS car ON s.CARRIER_ID = car.CARRIER_ID
        LEFT JOIN BL_3NF.CE_PAYMENT_METHODS pm ON o.PAYMENT_METHOD_ID = pm.PAYMENT_METHOD_ID
        LEFT JOIN BL_3NF.CE_ORDER_STATUSES os ON o.ORDER_STATUS_ID = os.ORDER_STATUS_ID
        LEFT JOIN BL_3NF.CE_SHIPPING_MODES sm ON s.SHIPPING_MODE_ID = sm.SHIPPING_MODE_ID
        LEFT JOIN BL_3NF.CE_DELIVERY_STATUSES ds ON d.DELIVERY_STATUS_ID = ds.DELIVERY_STATUS_ID

        WHERE ol.ORDER_LINE_ID > 0
        AND s.SHIPMENT_ID > 0
        AND (NOT p_incremental OR
             s.TA_UPDATE_DT > v_last_update_dt OR
             ol.TA_UPDATE_DT > v_last_update_dt OR
             sl.TA_UPDATE_DT > v_last_update_dt)
        AND (p_target_date IS NULL OR o.ORDER_DATE = p_target_date)
    )
    SELECT
        fb.EVENT_DT,

        -- DIMENSION SURROGATE KEY LOOKUPS - VARCHAR business key joins returning BIGINT surrogate keys
        COALESCE(dc.customer_surr_id, -1) as CUSTOMER_SURR_ID,
        COALESCE(dp.product_surr_id, -1) as PRODUCT_SURR_ID,
        COALESCE(dsr.sales_rep_surr_id, -1) as SALES_REP_SURR_ID,
        COALESCE(dto.dt_surr_id, -1) as ORDER_DT_SURR_ID,
        COALESCE(dts.dt_surr_id, -1) as SHIP_DT_SURR_ID,
        COALESCE(dtd.dt_surr_id, -1) as DELIVERY_DT_SURR_ID,
        COALESCE(dg.geography_surr_id, -1) as CUSTOMER_GEOGRAPHY_SURR_ID,
        COALESCE(dw.warehouse_surr_id, -1) as WAREHOUSE_SURR_ID,
        COALESCE(dcar.carrier_surr_id, -1) as CARRIER_SURR_ID,
        COALESCE(dpm.payment_method_surr_id, -1) as PAYMENT_METHOD_SURR_ID,
        COALESCE(dos.order_status_surr_id, -1) as ORDER_STATUS_SURR_ID,
        COALESCE(dsm.shipping_mode_surr_id, -1) as SHIPPING_MODE_SURR_ID,
        COALESCE(dds.delivery_status_surr_id, -1) as DELIVERY_STATUS_SURR_ID,

        -- Degenerate Dimensions
        fb.ORDER_SRC_ID,
        fb.ORDER_LINE_SRC_ID,
        fb.SHIPMENT_SRC_ID,
        fb.SHIPMENT_LINE_SRC_ID,
        fb.DELIVERY_SRC_ID,

        -- Sales Measures
        COALESCE(fb.UNIT_PRICE, 0) as UNIT_PRICE_ACT,
        fb.UNIT_COST as UNIT_COST_ACT,
        COALESCE(fb.LINE_TOTAL, 0) as LINE_TOTAL_ACT,
        COALESCE(fb.ORDERED_QUANTITY, 0) as ORDERED_QUANTITY_CNT,
        CASE
            WHEN fb.UNIT_COST IS NOT NULL AND fb.UNIT_PRICE IS NOT NULL
                THEN (fb.UNIT_PRICE - fb.UNIT_COST) * fb.SHIPPED_QUANTITY
            ELSE NULL
        END as ORDER_PROFIT_ACT,

        -- Shipment Measures
        COALESCE(fb.SHIPPED_QUANTITY, 0) as SHIPPED_QUANTITY_CNT,
        fb.SHIPPING_COST as SHIPPING_COST_ACT,
        CASE
            WHEN fb.TOTAL_SHIPMENT_VALUE > 0 AND fb.SHIPPING_COST IS NOT NULL
                THEN fb.SHIPPING_COST * (fb.LINE_TOTAL / fb.TOTAL_SHIPMENT_VALUE)
            ELSE NULL
        END as ALLOCATED_SHIPPING_COST_ACT,

        -- Delivery Measures
        fb.DELIVERY_DAYS as DELIVERY_DAYS_CNT,
        CASE
            WHEN fb.ORDER_DATE IS NOT NULL AND fb.SHIP_DATE IS NOT NULL
                THEN fb.SHIP_DATE - fb.ORDER_DATE
            ELSE NULL
        END as ORDER_TO_SHIP_DAYS_CNT,
        CASE
            WHEN fb.ON_TIME_DELIVERY IS TRUE THEN 1::DECIMAL
            WHEN fb.ON_TIME_DELIVERY IS FALSE THEN 0::DECIMAL
            ELSE NULL
        END as ON_TIME_DELIVERY_FLAG,
        CASE
            WHEN fb.ON_TIME_DELIVERY IS FALSE THEN 1::DECIMAL
            WHEN fb.ON_TIME_DELIVERY IS TRUE THEN 0::DECIMAL
            ELSE NULL
        END as LATE_DELIVERY_FLAG,

        -- Calculated Measures
        COALESCE(fb.SHIPPED_QUANTITY, 0) * COALESCE(fb.UNIT_PRICE, 0) as SHIPPED_SALES_AMOUNT_ACT,
        COALESCE(fb.ORDERED_QUANTITY, 0) - COALESCE(fb.SHIPPED_QUANTITY, 0) as UNSHIPPED_QUANTITY_CNT,
        CASE
            WHEN fb.ORDERED_QUANTITY > 0
                THEN (fb.SHIPPED_QUANTITY::DECIMAL / fb.ORDERED_QUANTITY * 100)
            ELSE NULL
        END as FILL_RATE_PCT,

        -- Technical Attributes
        CURRENT_TIMESTAMP as TA_INSERT_DT,
        CURRENT_TIMESTAMP as TA_UPDATE_DT

    FROM fact_base fb
    -- DIMENSION LOOKUPS USING VARCHAR BUSINESS KEYS → RETURNING BIGINT SURROGATE KEYS
    LEFT JOIN BL_DM.DIM_CUSTOMERS dc
        ON fb.CUSTOMER_SRC_ID = dc.customer_src_id           -- VARCHAR = VARCHAR
        AND dc.SOURCE_SYSTEM = '3NF_LAYER'
    LEFT JOIN BL_DM.DIM_PRODUCTS_SCD dp
        ON fb.PRODUCT_SRC_ID = dp.product_src_id             -- VARCHAR = VARCHAR
        AND dp.SOURCE_SYSTEM = '3NF_LAYER'
        AND fb.ORDER_DATE BETWEEN dp.START_DT AND dp.END_DT
        --AND dp.IS_ACTIVE = 'Y'
    LEFT JOIN BL_DM.DIM_SALES_REPRESENTATIVES dsr
        ON fb.SALES_REP_SRC_ID = dsr.sales_rep_src_id        -- VARCHAR = VARCHAR
        AND dsr.SOURCE_SYSTEM = '3NF_LAYER'
    LEFT JOIN BL_DM.DIM_GEOGRAPHIES dg
        ON fb.GEOGRAPHY_SRC_ID = dg.geography_src_id         -- VARCHAR = VARCHAR
        AND dg.SOURCE_SYSTEM = '3NF_LAYER'
    LEFT JOIN BL_DM.DIM_WAREHOUSES dw
        ON fb.WAREHOUSE_SRC_ID = dw.warehouse_src_id         -- VARCHAR = VARCHAR
        AND dw.SOURCE_SYSTEM = '3NF_LAYER'
    LEFT JOIN BL_DM.DIM_CARRIERS dcar
        ON fb.CARRIER_SRC_ID = dcar.carrier_src_id           -- VARCHAR = VARCHAR
        AND dcar.SOURCE_SYSTEM = '3NF_LAYER'
    LEFT JOIN BL_DM.DIM_PAYMENT_METHODS dpm
        ON fb.PAYMENT_METHOD_SRC_ID = dpm.payment_method_src_id  -- VARCHAR = VARCHAR
        AND dpm.SOURCE_SYSTEM = '3NF_LAYER'
    LEFT JOIN BL_DM.DIM_ORDER_STATUSES dos
        ON fb.ORDER_STATUS_SRC_ID = dos.order_status_src_id  -- VARCHAR = VARCHAR
        AND dos.SOURCE_SYSTEM = '3NF_LAYER'
    LEFT JOIN BL_DM.DIM_SHIPPING_MODES dsm
        ON fb.SHIPPING_MODE_SRC_ID = dsm.shipping_mode_src_id    -- VARCHAR = VARCHAR
        AND dsm.SOURCE_SYSTEM = '3NF_LAYER'
    LEFT JOIN BL_DM.DIM_DELIVERY_STATUSES dds
        ON fb.DELIVERY_STATUS_SRC_ID = dds.delivery_status_src_id -- VARCHAR = VARCHAR
        AND dds.SOURCE_SYSTEM = '3NF_LAYER'

    -- TIME DIMENSION LOOKUPS USING DIRECT DATE MATCHING → RETURNING BIGINT SURROGATE KEYS
    LEFT JOIN BL_DM.DIM_TIME_DAY dto ON fb.ORDER_DATE = dto.calendar_dt      -- DATE = DATE
    LEFT JOIN BL_DM.DIM_TIME_DAY dts ON fb.SHIP_DATE = dts.calendar_dt       -- DATE = DATE
    LEFT JOIN BL_DM.DIM_TIME_DAY dtd ON fb.DELIVERY_DATE = dtd.calendar_dt;  -- DATE = DATE

    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
    RAISE NOTICE 'Inserted % new fact records', v_inserted_count;

    -- Data Quality Checks
    SELECT COUNT(*) INTO v_missing_customers
    FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    WHERE CUSTOMER_SURR_ID = -1 AND TA_UPDATE_DT >= v_start_time;

    SELECT COUNT(*) INTO v_missing_products
    FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    WHERE PRODUCT_SURR_ID = -1 AND TA_UPDATE_DT >= v_start_time;

    SELECT COUNT(*) INTO v_missing_dates
    FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    WHERE (ORDER_DT_SURR_ID = -1 OR SHIP_DT_SURR_ID = -1) AND TA_UPDATE_DT >= v_start_time;

    SELECT COUNT(*) INTO v_business_records
    FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    WHERE ORDER_LINE_SHIPMENT_SURR_ID > 0;

    -- Data Quality Warnings
    IF v_missing_customers > 0 THEN
        RAISE WARNING 'WARNING: % records have missing customer lookups (-1)', v_missing_customers;
    END IF;

    IF v_missing_products > 0 THEN
        RAISE WARNING 'WARNING: % records have missing product lookups (-1)', v_missing_products;
    END IF;

    IF v_missing_dates > 0 THEN
        RAISE WARNING 'WARNING: % records have missing date lookups (-1)', v_missing_dates;
    END IF;

    -- Manage rolling window (detach old partitions)
    IF p_incremental THEN
        CALL BL_CL.manage_rolling_window(3); -- Keep last 3 months
    END IF;

    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    -- Log successful completion
    CALL BL_CL.log_procedure_event(
        'load_fct_order_line_shipments_dd',
        'BL_3NF.CE_*',
        'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
        'SUCCESS',
        v_inserted_count,
        FORMAT('Load completed: %s deleted, %s inserted, %s total records. DQ: %s missing customers, %s missing products, %s missing dates. Partitions: %s',
               v_deleted_count, v_inserted_count, v_business_records,
               v_missing_customers, v_missing_products, v_missing_dates,
               array_to_string(v_target_partitions, ',')),
        v_execution_time
    );

    RAISE NOTICE 'FACT LOAD COMPLETED: % deleted, % inserted, % total business records in % ms',
        v_deleted_count, v_inserted_count, v_business_records, v_execution_time;

    -- Release lock
    PERFORM BL_CL.release_procedure_lock('load_fct_order_line_shipments_dd');

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

        CALL BL_CL.log_procedure_event(
            'load_fct_order_line_shipments_dd',
            'BL_3NF.CE_*',
            'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
            'ERROR',
            0,
            SQLERRM,
            v_execution_time,
            SQLSTATE
        );

        PERFORM BL_CL.release_procedure_lock('load_fct_order_line_shipments_dd');
        RAISE;
END $$;

-- =====================================================
-- SECTION 6: MONITORING AND UTILITY PROCEDURES
-- =====================================================

-- Procedure to get partition information
CREATE OR REPLACE PROCEDURE BL_CL.show_partition_info()
LANGUAGE plpgsql
AS $$
DECLARE
    v_partition_record RECORD;
BEGIN
    RAISE NOTICE 'FACT TABLE PARTITION INFORMATION:';
    RAISE NOTICE '=====================================';

    FOR v_partition_record IN
        SELECT
            schemaname,
            tablename,
            CASE
                WHEN tablename ~ 'fct_order_line_shipments_dd_[0-9]{6}$' THEN
                    TO_DATE(RIGHT(tablename, 6), 'YYYYMM')
                ELSE NULL
            END as partition_date,
            pg_size_pretty(pg_total_relation_size('bl_dm.' || tablename)) as size
        FROM pg_tables
        WHERE schemaname = 'bl_dm'
        AND (tablename = 'fct_order_line_shipments_dd' OR
             tablename LIKE 'fct_order_line_shipments_dd_%')
        ORDER BY tablename
    LOOP
        IF v_partition_record.partition_date IS NOT NULL THEN
            RAISE NOTICE 'Partition: % | Date: % | Size: %',
                        v_partition_record.tablename,
                        v_partition_record.partition_date,
                        v_partition_record.size;
        ELSE
            RAISE NOTICE 'Main Table: % | Size: %',
                        v_partition_record.tablename,
                        v_partition_record.size;
        END IF;
    END LOOP;
END $$;

-- Function to get record count by partition
CREATE OR REPLACE FUNCTION BL_CL.get_partition_counts()
RETURNS TABLE(partition_name TEXT, record_count BIGINT, min_date DATE, max_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_partition_record RECORD;
    v_sql TEXT;
    v_count BIGINT;
    v_min_date DATE;
    v_max_date DATE;
BEGIN
    FOR v_partition_record IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'bl_dm'
        AND tablename LIKE 'fct_order_line_shipments_dd_%'
        AND tablename ~ 'fct_order_line_shipments_dd_[0-9]{6}$'
        ORDER BY tablename
    LOOP
        -- Get count and date range for each partition
        v_sql := FORMAT('SELECT COUNT(*), MIN(EVENT_DT), MAX(EVENT_DT) FROM BL_DM.%I',
                       v_partition_record.tablename);
        EXECUTE v_sql INTO v_count, v_min_date, v_max_date;

        partition_name := v_partition_record.tablename;
        record_count := v_count;
        min_date := v_min_date;
        max_date := v_max_date;
        RETURN NEXT;
    END LOOP;
    RETURN;
END $$;

-- =====================================================
-- SECTION 7: TESTING AND VERIFICATION
-- =====================================================

-- Test procedure for partition functionality
CREATE OR REPLACE PROCEDURE BL_CL.test_partition_functionality()
LANGUAGE plpgsql
AS $$
DECLARE
    v_test_date DATE := '2024-06-15';
    v_partition_name TEXT;
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_execution_time INTEGER;
BEGIN
    CALL BL_CL.log_procedure_event(
        'test_partition_functionality',
        NULL,
        'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
        'START',
        0,
        'Testing partition creation and management'
    );

    -- Test partition creation
    RAISE NOTICE 'Testing partition creation for date: %', v_test_date;
    CALL BL_CL.create_fact_partition(v_test_date, TRUE);

    v_partition_name := BL_CL.get_partition_name('FCT_ORDER_LINE_SHIPMENTS_DD', v_test_date);

    IF BL_CL.partition_exists(v_partition_name) THEN
        RAISE NOTICE 'SUCCESS: Partition % created successfully', v_partition_name;
    ELSE
        RAISE EXCEPTION 'FAILED: Partition % was not created', v_partition_name;
    END IF;

    -- Test rolling window management
    RAISE NOTICE 'Testing rolling window management...';
    CALL BL_CL.manage_rolling_window(3);

    -- Show partition information
    CALL BL_CL.show_partition_info();

    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    CALL BL_CL.log_procedure_event(
        'test_partition_functionality',
        NULL,
        'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
        'SUCCESS',
        1,
        'Partition functionality test completed successfully',
        v_execution_time
    );

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

        CALL BL_CL.log_procedure_event(
            'test_partition_functionality',
            NULL,
            'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
            'ERROR',
            0,
            SQLERRM,
            v_execution_time,
            SQLSTATE
        );
        RAISE;
END $$;



-- =====================================================
-- SECTION 8: FINAL VERIFICATION QUERIES
-- =====================================================

-- List all created procedures
SELECT routine_name, routine_type, routine_definition IS NOT NULL as has_definition
FROM information_schema.routines
WHERE routine_schema = 'bl_cl'
AND routine_name LIKE '%fct%' OR routine_name LIKE '%partition%' OR routine_name = 'load_bl_dm_full'
ORDER BY routine_name;

-- Show utility functions
SELECT routine_name, routine_type
FROM information_schema.routines
WHERE routine_schema = 'bl_cl'
AND routine_type = 'FUNCTION'
AND (routine_name LIKE '%partition%' OR routine_name LIKE '%fct%')
ORDER BY routine_name;

COMMIT;