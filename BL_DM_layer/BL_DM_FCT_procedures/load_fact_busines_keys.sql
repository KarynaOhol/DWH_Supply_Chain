TRUNCATE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD;

ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    ALTER COLUMN ORDER_SRC_ID TYPE VARCHAR(50),
    ALTER COLUMN ORDER_LINE_SRC_ID TYPE VARCHAR(50),
    ALTER COLUMN SHIPMENT_SRC_ID TYPE VARCHAR(50),
    ALTER COLUMN SHIPMENT_LINE_SRC_ID TYPE VARCHAR(50),
    ALTER COLUMN DELIVERY_SRC_ID TYPE VARCHAR(50);


-- =====================================================
-- FIXED FACT TABLE LOADING PROCEDURE FOR INDEPENDENT SHIPMENT LINES
-- Purpose: Load FCT_ORDER_LINE_SHIPMENTS_DD with corrected JOIN logic
-- Grain: One row per ORDER_LINE (includes both shipped and unshipped)
-- Business Logic: shipment_src_id = order_src_id, shipment_line_src_id = order_line_src_id
-- =====================================================

CREATE OR REPLACE PROCEDURE BL_CL.load_fct_order_line_shipments_dd(
    p_incremental BOOLEAN DEFAULT TRUE,
    p_target_date DATE DEFAULT NULL
)
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time        TIMESTAMP := CURRENT_TIMESTAMP;
    v_execution_time    INTEGER;
    v_last_update_dt    TIMESTAMP;
    v_deleted_count     INTEGER   := 0;
    v_inserted_count    INTEGER   := 0;
    v_missing_customers INTEGER   := 0;
    v_missing_products  INTEGER   := 0;
    v_missing_dates     INTEGER   := 0;
    v_business_records  INTEGER   := 0;
    v_target_partitions TEXT[]    := '{}';
    v_partition_name    TEXT;
    v_min_order_date    DATE;
    v_max_order_date    DATE;
    v_current_month     DATE;
    v_shipped_count     INTEGER   := 0;
    v_unshipped_count   INTEGER   := 0;
BEGIN
    -- Acquire procedure lock
    IF NOT BL_CL.acquire_procedure_lock('load_fct_order_line_shipments_dd_fixed') THEN
        RAISE EXCEPTION 'Procedure load_fct_order_line_shipments_dd_fixed is already running';
    END IF;

    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_fct_order_line_shipments_dd_fixed',
            'BL_3NF.CE_*',
            'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
            'START',
            0,
            FORMAT('Starting FIXED fact load with independent shipment lines - incremental: %s, target_date: %s', p_incremental, p_target_date)
         );

    -- Determine what data to load
    IF p_incremental THEN
        v_last_update_dt := BL_CL.get_last_successful_load('load_fct_order_line_shipments_dd_fixed');
        RAISE NOTICE 'Processing incremental load since: %', v_last_update_dt;
    ELSE
        v_last_update_dt := '1900-01-01'::TIMESTAMP;
        RAISE NOTICE 'Processing full reload of all data';
    END IF;

    -- Get date range of data to be processed (FIXED: no longer requires shipment join)
    SELECT COALESCE(MIN(o.ORDER_DATE), CURRENT_DATE),
           COALESCE(MAX(o.ORDER_DATE), CURRENT_DATE)
    INTO v_min_order_date, v_max_order_date
    FROM BL_3NF.CE_ORDER_LINES ol
             JOIN BL_3NF.CE_ORDERS o ON ol.ORDER_ID = o.ORDER_ID
    WHERE (NOT p_incremental OR ol.TA_UPDATE_DT > v_last_update_dt)
      AND (p_target_date IS NULL OR o.ORDER_DATE = p_target_date);

    RAISE NOTICE 'Data date range: % to %', v_min_order_date, v_max_order_date;

    -- Create/ensure partitions exist for the date range
    v_current_month := DATE_TRUNC('month', v_min_order_date);
    WHILE v_current_month <= DATE_TRUNC('month', v_max_order_date)
        LOOP
            v_partition_name := BL_CL.get_partition_name('FCT_ORDER_LINE_SHIPMENTS_DD', v_current_month);

            IF NOT BL_CL.partition_exists(v_partition_name) THEN
                CALL BL_CL.create_fact_partition(v_current_month, TRUE);
                RAISE NOTICE 'Created missing partition: %', v_partition_name;
            END IF;

            v_target_partitions := v_target_partitions || v_partition_name;
            v_current_month := v_current_month + INTERVAL '1 month';
        END LOOP;

    -- Delete existing records for updated order lines (FIXED: using ORDER_LINE business keys)
    IF p_incremental THEN
        DELETE
        FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
        WHERE ORDER_LINE_SRC_ID IN (
            SELECT ol.ORDER_LINE_SRC_ID
            FROM BL_3NF.CE_ORDER_LINES ol
            WHERE ol.TA_UPDATE_DT > v_last_update_dt
        );
    ELSE
        -- Full reload - truncate all partitions
        EXECUTE FORMAT('TRUNCATE TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD');
    END IF;

    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % existing fact records', v_deleted_count;

    -- =====================================================
    -- MAIN FACT LOADING QUERY - FIXED FOR INDEPENDENT SHIPMENT LINES
    -- =====================================================
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
        -- BUSINESS KEYS as degenerate dimensions
        ORDER_SRC_ID,
        ORDER_LINE_SRC_ID,
        SHIPMENT_SRC_ID,
        SHIPMENT_LINE_SRC_ID,
        DELIVERY_SRC_ID,
        -- Measures
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
        -- Calculate shipping cost allocation denominator (FIXED: handles NULLs)
        SELECT s.SHIPMENT_ID,
               s.SHIPPING_COST,
               SUM(COALESCE(sl.shipped_quantity, 0) *
                   COALESCE((SELECT ol2.unit_price
                            FROM BL_3NF.CE_ORDER_LINES ol2
                            WHERE ol2.order_line_src_id = sl.shipment_line_src_id
                            LIMIT 1), 0)) as TOTAL_SHIPMENT_VALUE
        FROM BL_3NF.CE_SHIPMENTS s
        LEFT JOIN BL_3NF.CE_SHIPMENT_LINES sl ON s.SHIPMENT_ID = sl.SHIPMENT_ID
        WHERE (NOT p_incremental OR s.TA_UPDATE_DT > v_last_update_dt OR COALESCE(sl.TA_UPDATE_DT, '1900-01-01') > v_last_update_dt)
          AND (p_target_date IS NULL OR s.ship_date = p_target_date)
        GROUP BY s.SHIPMENT_ID, s.SHIPPING_COST
    ),
    latest_product_scd AS (
        -- Get the latest active product record for each product at order date
        SELECT p.PRODUCT_ID,
               p.PRODUCT_SRC_ID,
               order_products.ORDER_DATE,
               ROW_NUMBER() OVER (
                   PARTITION BY p.PRODUCT_ID, order_products.ORDER_DATE
                   ORDER BY p.START_DT DESC, p.TA_UPDATE_DT DESC
                   ) as rn
        FROM BL_3NF.CE_PRODUCTS_SCD p
                 JOIN (SELECT DISTINCT o.ORDER_DATE, ol.PRODUCT_ID
                       FROM BL_3NF.CE_ORDERS o
                                JOIN BL_3NF.CE_ORDER_LINES ol ON o.ORDER_ID = ol.ORDER_ID
                       WHERE (NOT p_incremental OR ol.TA_UPDATE_DT > v_last_update_dt)
                         AND (p_target_date IS NULL OR o.ORDER_DATE = p_target_date)) order_products
                      ON p.PRODUCT_ID = order_products.PRODUCT_ID
        WHERE order_products.ORDER_DATE BETWEEN p.START_DT AND p.END_DT
    ),
    fact_base AS (
        SELECT
            -- EVENT_DT is ORDER_DATE for partitioning
            o.ORDER_DATE as EVENT_DT,

            -- BUSINESS KEYS for degenerate dimensions (VARCHAR)
            o.ORDER_SRC_ID,                                           -- VARCHAR business key from OMS
            ol.ORDER_LINE_SRC_ID,                                     -- VARCHAR business key from OMS
            COALESCE(s.SHIPMENT_SRC_ID, 'NOT_SHIPPED') as SHIPMENT_SRC_ID,    -- VARCHAR business key from LMS or default
            COALESCE(sl.shipment_line_src_id, 'NOT_SHIPPED') as SHIPMENT_LINE_SRC_ID, -- VARCHAR business key from LMS or default
            COALESCE(d.SHIPMENT_SRC_ID, s.SHIPMENT_SRC_ID, 'NOT_DELIVERED') as DELIVERY_SRC_ID, -- Use delivery or shipment src_id

            -- Core business data
            o.ORDER_DATE,
            s.SHIP_DATE,
            d.DELIVERY_DATE,
            ol.QUANTITY as ORDERED_QUANTITY,
            ol.UNIT_PRICE,
            ol.UNIT_COST,
            ol.LINE_TOTAL,
            COALESCE(sl.SHIPPED_QUANTITY, 0) as SHIPPED_QUANTITY,     -- Default to 0 if not shipped
            s.SHIPPING_COST,
            st.TOTAL_SHIPMENT_VALUE,
            d.DELIVERY_DAYS,
            d.ON_TIME_DELIVERY,

            -- BUSINESS KEYS for dimension lookups (VARCHAR)
            c.CUSTOMER_SRC_ID,        -- VARCHAR business key
            lps.PRODUCT_SRC_ID,       -- VARCHAR business key from latest SCD
            sr.SALES_REP_SRC_ID,      -- VARCHAR business key
            g.GEOGRAPHY_SRC_ID,       -- VARCHAR business key
            w.WAREHOUSE_SRC_ID,       -- VARCHAR business key
            car.CARRIER_SRC_ID,       -- VARCHAR business key
            pm.PAYMENT_METHOD_SRC_ID, -- VARCHAR business key
            os.ORDER_STATUS_SRC_ID,   -- VARCHAR business key
            sm.SHIPPING_MODE_SRC_ID,  -- VARCHAR business key
            ds.DELIVERY_STATUS_SRC_ID -- VARCHAR business key

        FROM BL_3NF.CE_ORDER_LINES ol
                 JOIN BL_3NF.CE_ORDERS o ON ol.ORDER_ID = o.ORDER_ID

                 -- FIXED: Independent shipment lines join using business keys
                 LEFT JOIN BL_3NF.CE_SHIPMENT_LINES sl ON
                     sl.shipment_line_src_id = ol.order_line_src_id  -- Direct business key match
                     AND sl.source_system = 'LMS'

                 -- FIXED: Shipment join using business rule: shipment_src_id = order_src_id
                 LEFT JOIN BL_3NF.CE_SHIPMENTS s ON
                     s.shipment_src_id = o.order_src_id  -- shipment_src_id = order_src_id
                     AND s.source_system = 'LMS'

                 LEFT JOIN BL_3NF.CE_DELIVERIES d ON s.SHIPMENT_ID = d.SHIPMENT_ID
                 LEFT JOIN shipment_totals st ON s.SHIPMENT_ID = st.SHIPMENT_ID

                 -- SCD Product join using latest_product_scd CTE
                 LEFT JOIN latest_product_scd lps ON ol.PRODUCT_ID = lps.PRODUCT_ID
                     AND o.ORDER_DATE = lps.ORDER_DATE
                     AND lps.rn = 1

                 -- Join to dimension source tables to get VARCHAR business keys
                 LEFT JOIN BL_3NF.CE_CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
                 LEFT JOIN BL_3NF.CE_SALES_REPRESENTATIVES sr ON o.SALES_REP_ID = sr.SALES_REP_ID
                 LEFT JOIN BL_3NF.CE_GEOGRAPHIES g ON s.GEOGRAPHY_ID = g.GEOGRAPHY_ID
                 LEFT JOIN BL_3NF.CE_WAREHOUSES w ON s.WAREHOUSE_ID = w.WAREHOUSE_ID
                 LEFT JOIN BL_3NF.CE_CARRIERS car ON s.CARRIER_ID = car.CARRIER_ID
                 LEFT JOIN BL_3NF.CE_PAYMENT_METHODS pm ON o.PAYMENT_METHOD_ID = pm.PAYMENT_METHOD_ID
                 LEFT JOIN BL_3NF.CE_ORDER_STATUSES os ON o.ORDER_STATUS_ID = os.ORDER_STATUS_ID
                 LEFT JOIN BL_3NF.CE_SHIPPING_MODES sm ON s.SHIPPING_MODE_ID = sm.SHIPPING_MODE_ID
                 LEFT JOIN BL_3NF.CE_DELIVERY_STATUSES ds ON d.DELIVERY_STATUS_ID = ds.DELIVERY_STATUS_ID

        WHERE ol.ORDER_LINE_ID > 0
          -- FIXED: Handle NULLs in incremental logic
          AND (NOT p_incremental OR
               ol.TA_UPDATE_DT > v_last_update_dt OR
               COALESCE(s.TA_UPDATE_DT, '1900-01-01') > v_last_update_dt OR
               COALESCE(sl.TA_UPDATE_DT, '1900-01-01') > v_last_update_dt)
          AND (p_target_date IS NULL OR o.ORDER_DATE = p_target_date)
    )
    SELECT fb.EVENT_DT,

           -- DIMENSION SURROGATE KEY LOOKUPS (VARCHAR business key → BIGINT surrogate key)
           COALESCE(dc.CUSTOMER_SURR_ID, -1)                                   as CUSTOMER_SURR_ID,
           COALESCE(dp.PRODUCT_SURR_ID, -1)                                    as PRODUCT_SURR_ID,
           COALESCE(dsr.SALES_REP_SURR_ID, -1)                                 as SALES_REP_SURR_ID,
           COALESCE(dto.DT_SURR_ID, -1)                                        as ORDER_DT_SURR_ID,
           COALESCE(dts.DT_SURR_ID, -1)                                        as SHIP_DT_SURR_ID,
           COALESCE(dtd.DT_SURR_ID, -1)                                        as DELIVERY_DT_SURR_ID,
           COALESCE(dg.GEOGRAPHY_SURR_ID, -1)                                  as CUSTOMER_GEOGRAPHY_SURR_ID,
           COALESCE(dw.WAREHOUSE_SURR_ID, -1)                                  as WAREHOUSE_SURR_ID,
           COALESCE(dcar.CARRIER_SURR_ID, -1)                                  as CARRIER_SURR_ID,
           COALESCE(dpm.PAYMENT_METHOD_SURR_ID, -1)                            as PAYMENT_METHOD_SURR_ID,
           COALESCE(dos.ORDER_STATUS_SURR_ID, -1)                              as ORDER_STATUS_SURR_ID,
           COALESCE(dsm.SHIPPING_MODE_SURR_ID, -1)                             as SHIPPING_MODE_SURR_ID,
           COALESCE(dds.DELIVERY_STATUS_SURR_ID, -1)                           as DELIVERY_STATUS_SURR_ID,

           -- BUSINESS KEYS as Degenerate Dimensions (VARCHAR)
           fb.ORDER_SRC_ID,
           fb.ORDER_LINE_SRC_ID,
           fb.SHIPMENT_SRC_ID,
           fb.SHIPMENT_LINE_SRC_ID,
           fb.DELIVERY_SRC_ID,

           -- Sales Measures
           COALESCE(fb.UNIT_PRICE, 0)                                          as UNIT_PRICE_ACT,
           fb.UNIT_COST                                                        as UNIT_COST_ACT,
           COALESCE(fb.LINE_TOTAL, 0)                                          as LINE_TOTAL_ACT,
           COALESCE(fb.ORDERED_QUANTITY, 0)                                    as ORDERED_QUANTITY_CNT,
           CASE
               WHEN fb.UNIT_COST IS NOT NULL AND fb.UNIT_PRICE IS NOT NULL
                   THEN (fb.UNIT_PRICE - fb.UNIT_COST) * fb.SHIPPED_QUANTITY
               ELSE NULL
               END                                                             as ORDER_PROFIT_ACT,

           -- Shipment Measures (FIXED: handle NULLs for unshipped orders)
           COALESCE(fb.SHIPPED_QUANTITY, 0)                                    as SHIPPED_QUANTITY_CNT,
           fb.SHIPPING_COST                                                    as SHIPPING_COST_ACT,
           CASE
               WHEN fb.TOTAL_SHIPMENT_VALUE > 0 AND fb.SHIPPING_COST IS NOT NULL AND fb.LINE_TOTAL IS NOT NULL
                   THEN fb.SHIPPING_COST * (fb.LINE_TOTAL / fb.TOTAL_SHIPMENT_VALUE)
               ELSE NULL
               END                                                             as ALLOCATED_SHIPPING_COST_ACT,

           -- Delivery Measures (FIXED: handle NULLs for undelivered orders)
           fb.DELIVERY_DAYS                                                    as DELIVERY_DAYS_CNT,
           CASE
               WHEN fb.ORDER_DATE IS NOT NULL AND fb.SHIP_DATE IS NOT NULL
                   THEN fb.SHIP_DATE - fb.ORDER_DATE
               ELSE NULL
               END                                                             as ORDER_TO_SHIP_DAYS_CNT,
           CASE
               WHEN fb.ON_TIME_DELIVERY IS TRUE THEN 1::DECIMAL
               WHEN fb.ON_TIME_DELIVERY IS FALSE THEN 0::DECIMAL
               ELSE NULL
               END                                                             as ON_TIME_DELIVERY_FLAG,
           CASE
               WHEN fb.ON_TIME_DELIVERY IS FALSE THEN 1::DECIMAL
               WHEN fb.ON_TIME_DELIVERY IS TRUE THEN 0::DECIMAL
               ELSE NULL
               END                                                             as LATE_DELIVERY_FLAG,

           -- Calculated Measures (FIXED: handle unshipped orders)
           COALESCE(fb.SHIPPED_QUANTITY, 0) * COALESCE(fb.UNIT_PRICE, 0)       as SHIPPED_SALES_AMOUNT_ACT,
           COALESCE(fb.ORDERED_QUANTITY, 0) - COALESCE(fb.SHIPPED_QUANTITY, 0) as UNSHIPPED_QUANTITY_CNT,
           CASE
               WHEN fb.ORDERED_QUANTITY > 0
                   THEN (COALESCE(fb.SHIPPED_QUANTITY, 0)::DECIMAL / fb.ORDERED_QUANTITY * 100)
               ELSE NULL
               END                                                             as FILL_RATE_PCT,

           -- Technical Attributes
           CURRENT_TIMESTAMP                                                   as TA_INSERT_DT,
           CURRENT_TIMESTAMP                                                   as TA_UPDATE_DT

    FROM fact_base fb
             -- DIMENSION LOOKUPS USING VARCHAR BUSINESS KEYS → RETURNING BIGINT SURROGATE KEYS
             LEFT JOIN BL_DM.DIM_CUSTOMERS dc
                       ON fb.CUSTOMER_SRC_ID = dc.CUSTOMER_SRC_ID
                           AND dc.SOURCE_SYSTEM = '3NF_LAYER'
             LEFT JOIN BL_DM.DIM_PRODUCTS_SCD dp
                       ON fb.PRODUCT_SRC_ID = dp.PRODUCT_SRC_ID
                           AND dp.SOURCE_SYSTEM = '3NF_LAYER'
                           AND fb.ORDER_DATE BETWEEN dp.START_DT AND dp.END_DT
             LEFT JOIN BL_DM.DIM_SALES_REPRESENTATIVES dsr
                       ON fb.SALES_REP_SRC_ID = dsr.SALES_REP_SRC_ID
                           AND dsr.SOURCE_SYSTEM = '3NF_LAYER'
             LEFT JOIN BL_DM.DIM_GEOGRAPHIES dg
                       ON fb.GEOGRAPHY_SRC_ID = dg.GEOGRAPHY_SRC_ID
                           AND dg.SOURCE_SYSTEM = '3NF_LAYER'
             LEFT JOIN BL_DM.DIM_WAREHOUSES dw
                       ON fb.WAREHOUSE_SRC_ID = dw.WAREHOUSE_SRC_ID
                           AND dw.SOURCE_SYSTEM = '3NF_LAYER'
             LEFT JOIN BL_DM.DIM_CARRIERS dcar
                       ON fb.CARRIER_SRC_ID = dcar.CARRIER_SRC_ID
                           AND dcar.SOURCE_SYSTEM = '3NF_LAYER'
             LEFT JOIN BL_DM.DIM_PAYMENT_METHODS dpm
                       ON fb.PAYMENT_METHOD_SRC_ID = dpm.PAYMENT_METHOD_SRC_ID
                           AND dpm.SOURCE_SYSTEM = '3NF_LAYER'
             LEFT JOIN BL_DM.DIM_ORDER_STATUSES dos
                       ON fb.ORDER_STATUS_SRC_ID = dos.ORDER_STATUS_SRC_ID
                           AND dos.SOURCE_SYSTEM = '3NF_LAYER'
             LEFT JOIN BL_DM.DIM_SHIPPING_MODES dsm
                       ON fb.SHIPPING_MODE_SRC_ID = dsm.SHIPPING_MODE_SRC_ID
                           AND dsm.SOURCE_SYSTEM = '3NF_LAYER'
             LEFT JOIN BL_DM.DIM_DELIVERY_STATUSES dds
                       ON fb.DELIVERY_STATUS_SRC_ID = dds.DELIVERY_STATUS_SRC_ID
                           AND dds.SOURCE_SYSTEM = '3NF_LAYER'

             -- TIME DIMENSION LOOKUPS USING DIRECT DATE MATCHING
             LEFT JOIN BL_DM.DIM_TIME_DAY dto ON fb.ORDER_DATE = dto.CALENDAR_DT
             LEFT JOIN BL_DM.DIM_TIME_DAY dts ON fb.SHIP_DATE = dts.CALENDAR_DT
             LEFT JOIN BL_DM.DIM_TIME_DAY dtd ON fb.DELIVERY_DATE = dtd.CALENDAR_DT;

    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
    RAISE NOTICE 'Inserted % new fact records', v_inserted_count;

    -- Count shipped vs unshipped order lines
    SELECT COUNT(*) INTO v_shipped_count
    FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    WHERE SHIPMENT_LINE_SRC_ID != 'NOT_SHIPPED'
      AND TA_UPDATE_DT >= v_start_time;

    SELECT COUNT(*) INTO v_unshipped_count
    FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    WHERE SHIPMENT_LINE_SRC_ID = 'NOT_SHIPPED'
      AND TA_UPDATE_DT >= v_start_time;

    -- Data Quality Checks
    SELECT COUNT(*) INTO v_missing_customers
    FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    WHERE CUSTOMER_SURR_ID = -1 AND TA_UPDATE_DT >= v_start_time;

    SELECT COUNT(*) INTO v_missing_products
    FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    WHERE PRODUCT_SURR_ID = -1 AND TA_UPDATE_DT >= v_start_time;

    SELECT COUNT(*) INTO v_missing_dates
    FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD
    WHERE ORDER_DT_SURR_ID = -1 AND TA_UPDATE_DT >= v_start_time;

    SELECT COUNT(*) INTO v_business_records
    FROM BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD;

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
            'load_fct_order_line_shipments_dd_fixed',
            'BL_3NF.CE_*',
            'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
            'SUCCESS',
            v_inserted_count,
            FORMAT(
                    'FIXED Load completed: %s deleted, %s inserted (%s shipped + %s unshipped), %s total records. DQ: %s missing customers, %s missing products, %s missing dates. Expected: CE_ORDER_LINES count. Partitions: %s',
                    v_deleted_count, v_inserted_count, v_shipped_count, v_unshipped_count, v_business_records,
                    v_missing_customers, v_missing_products, v_missing_dates,
                    array_to_string(v_target_partitions, ',')),
            v_execution_time
         );

    RAISE NOTICE 'FIXED FACT LOAD COMPLETED: % deleted, % inserted (% shipped + % unshipped), % total business records in % ms',
        v_deleted_count, v_inserted_count, v_shipped_count, v_unshipped_count, v_business_records, v_execution_time;
    RAISE NOTICE 'EXPECTED RESULT: Fact table should have exactly CE_ORDER_LINES count (includes shipped and unshipped order lines)';

    -- Release lock
    PERFORM BL_CL.release_procedure_lock('load_fct_order_line_shipments_dd_fixed');

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

        CALL BL_CL.log_procedure_event(
                'load_fct_order_line_shipments_dd_fixed',
                'BL_3NF.CE_*',
                'BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD',
                'ERROR',
                0,
                SQLERRM,
                v_execution_time,
                SQLSTATE
             );

        PERFORM BL_CL.release_procedure_lock('load_fct_order_line_shipments_dd_fixed');
        RAISE;
END
$$;

call bl_cl.load_all_dm_dimensions();

call bl_cl.load_fct_order_line_shipments_dd(false);