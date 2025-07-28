-- =====================================================
-- FIXED SUPPLY CHAIN DATA WAREHOUSE - IDEMPOTENT FACT PROCEDURES
-- Purpose: Fix foreign key constraints and order lines logic
-- =====================================================

SET ROLE dwh_cleansing_user;
SET search_path = BL_CL, BL_3NF, SA_OMS, SA_LMS, public;


-- =====================================================
-- SECTION 2: CORRECTED ORDER LINES LOGIC
-- =====================================================
-- PROCEDURE: Load CE_ORDERS with UPSERT
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_orders_idempotent()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_orders_idempotent', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_ORDERS', 'START', 0,
            'Starting idempotent orders load using business keys'
         );

    INSERT INTO BL_3NF.CE_ORDERS (order_src_id, customer_id, sales_rep_id, order_status_id, payment_method_id,
                                  order_date, order_total, event_dt, source_system, source_entity)
    SELECT grouped_oms.order_src_id,
           COALESCE(c.customer_id, -1)        as customer_id,
           COALESCE(sr.sales_rep_id, -1)      as sales_rep_id,
           COALESCE(os.order_status_id, -1)   as order_status_id,
           COALESCE(pm.payment_method_id, -1) as payment_method_id,
           CASE
               WHEN grouped_oms.order_dt ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                   THEN grouped_oms.order_dt::DATE
               ELSE '1900-01-01'::DATE
               END                            as order_date,
           CASE
               WHEN grouped_oms.order_total ~ '^[0-9]+\.?[0-9]*$'
                   THEN grouped_oms.order_total::DECIMAL(15, 2)
               ELSE 0
               END                            as order_total,
           CASE
               WHEN grouped_oms.order_dt ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                   THEN grouped_oms.order_dt::DATE
               ELSE '1900-01-01'::DATE
               END                            as event_dt,
           'OMS'                              as source_system,
           'SRC_OMS'                          as source_entity
    FROM (SELECT order_src_id,
                 MIN(order_dt)         as order_dt,
                 MIN(customer_src_id)  as customer_src_id,
                 MIN(sales_rep_src_id) as sales_rep_src_id,
                 MIN(order_status)     as order_status,
                 MIN(payment_method)   as payment_method,
                 MIN(order_total)      as order_total
          FROM SA_OMS.SRC_OMS
          WHERE order_src_id IS NOT NULL
            AND order_src_id != ''
          GROUP BY order_src_id) grouped_oms
             LEFT JOIN BL_3NF.CE_CUSTOMERS c
                       ON c.customer_src_id = grouped_oms.customer_src_id
                           AND c.source_system = 'OMS'
             LEFT JOIN BL_3NF.CE_SALES_REPRESENTATIVES sr
                       ON sr.sales_rep_src_id = grouped_oms.sales_rep_src_id
                           AND sr.source_system = 'OMS'
             LEFT JOIN BL_3NF.CE_ORDER_STATUSES os
                       ON os.order_status_src_id = grouped_oms.order_status
                           AND os.source_system = 'OMS'
             LEFT JOIN BL_3NF.CE_PAYMENT_METHODS pm
                       ON pm.payment_method_src_id = grouped_oms.payment_method
                           AND pm.source_system = 'OMS'
    ON CONFLICT (order_src_id, source_system) DO NOTHING;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    CALL BL_CL.log_procedure_event(
            'load_ce_orders_idempotent', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_ORDERS', 'SUCCESS',
            v_rows_affected, FORMAT('Orders loaded: %s new records', v_rows_affected), v_execution_time
         );

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_orders_idempotent', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_ORDERS', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        RAISE;
END
$$;


-- PROCEDURE: Load CE_ORDER_LINES with CORRECT distinct combination logic
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_order_lines_idempotent()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    CALL BL_CL.log_procedure_event(
            'load_ce_order_lines_idempotent', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_ORDER_LINES', 'START', 0,
            'Starting idempotent order lines load with CORRECT distinct combination logic'
         );

    -- FIXED: Create order lines based on distinct combinations of order|product|customer
    INSERT INTO BL_3NF.CE_ORDER_LINES (order_line_src_id, order_id, product_id, quantity, unit_price, unit_cost,
                                       line_total, source_system, source_entity)
    SELECT
           -- Create synthetic order_line_src_id from the distinct combination
           CONCAT(grouped_oms.order_src_id,'|',grouped_oms.product_src_id,'|',grouped_oms.customer_src_id) as order_line_src_id,
           COALESCE(o.order_id, -1)   as order_id,
           COALESCE(p.product_id, -1) as product_id,
           grouped_oms.total_quantity as quantity,
           grouped_oms.avg_unit_price as unit_price,
           grouped_oms.avg_unit_cost  as unit_cost,
           grouped_oms.total_sales_amount as line_total,
           'OMS'                      as source_system,
           'SRC_OMS'                  as source_entity
    FROM (
        -- Group by the distinct combination and aggregate quantities/amounts
        SELECT order_src_id,
               customer_src_id,
               product_src_id,
               SUM(CASE WHEN quantity ~ '^[0-9]+$' THEN quantity::INTEGER ELSE 0 END) as total_quantity,
               AVG(CASE WHEN unit_price ~ '^[0-9]+\.?[0-9]*$' THEN unit_price::DECIMAL(15, 2) ELSE 0 END) as avg_unit_price,
               AVG(CASE WHEN unit_cost ~ '^[0-9]+\.?[0-9]*$' THEN unit_cost::DECIMAL(15, 2) ELSE NULL END) as avg_unit_cost,
               SUM(CASE WHEN sales_amount ~ '^[0-9]+\.?[0-9]*$' THEN sales_amount::DECIMAL(15, 2) ELSE 0 END) as total_sales_amount,
               MIN(order_dt) as order_dt  -- For date-based product SCD lookup
        FROM SA_OMS.SRC_OMS
        WHERE order_src_id IS NOT NULL AND order_src_id != ''
          AND customer_src_id IS NOT NULL AND customer_src_id != ''
          AND product_src_id IS NOT NULL AND product_src_id != ''
        GROUP BY order_src_id, customer_src_id, product_src_id
    ) grouped_oms
    LEFT JOIN BL_3NF.CE_ORDERS o
              ON o.order_src_id = grouped_oms.order_src_id
                  AND o.source_system = 'OMS'
    LEFT JOIN BL_3NF.CE_PRODUCTS_SCD p
              ON p.product_src_id = grouped_oms.product_src_id
                  AND p.source_system = 'OMS'
                  AND CASE
                          WHEN grouped_oms.order_dt ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                              THEN grouped_oms.order_dt::DATE
                          ELSE '1900-01-01'::DATE
                     END BETWEEN p.start_dt AND p.end_dt
    ON CONFLICT (order_line_src_id, source_system) DO NOTHING;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    CALL BL_CL.log_procedure_event(
            'load_ce_order_lines_idempotent', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_ORDER_LINES', 'SUCCESS',
            v_rows_affected, FORMAT('Order lines loaded: %s new distinct combinations', v_rows_affected), v_execution_time
         );

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_order_lines_idempotent', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_ORDER_LINES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        RAISE;
END
$$;

-- PROCEDURE: Load CE_TRANSACTIONS with UPSERT
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_transactions_idempotent()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    -- Log procedure start
    CALL BL_CL.log_procedure_event(
            'load_ce_transactions_idempotent', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_TRANSACTIONS', 'START', 0,
            'Starting idempotent transactions load using business keys'
         );

    INSERT INTO BL_3NF.CE_TRANSACTIONS (transaction_src_id, order_id, sales_amount, transaction_date, event_dt,
                                        source_system, source_entity)
    SELECT oms.transaction_src_id,
           COALESCE(o.order_id, -1) as order_id,
           CASE
               WHEN oms.sales_amount ~ '^[0-9]+\.?[0-9]*$' THEN oms.sales_amount::DECIMAL(15, 2)
               ELSE 0
               END                  as sales_amount,
           CASE
               WHEN oms.order_dt ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN oms.order_dt::DATE
               ELSE '1900-01-01'::DATE
               END                  as transaction_date,
           CASE
               WHEN oms.order_dt ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN oms.order_dt::DATE
               ELSE '1900-01-01'::DATE
               END                  as event_dt,
           'OMS'                    as source_system,
           'SRC_OMS'                as source_entity
    FROM SA_OMS.SRC_OMS oms
             LEFT JOIN BL_3NF.CE_ORDERS o
                       ON o.order_src_id = oms.order_src_id
                           AND o.source_system = 'OMS'
    WHERE oms.transaction_src_id IS NOT NULL
      AND oms.transaction_src_id != ''
    ON CONFLICT (transaction_src_id, source_system) DO NOTHING;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    CALL BL_CL.log_procedure_event(
            'load_ce_transactions_idempotent', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_TRANSACTIONS', 'SUCCESS',
            v_rows_affected, FORMAT('Transactions loaded: %s new records', v_rows_affected), v_execution_time
         );

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_transactions_idempotent', 'SA_OMS.SRC_OMS', 'BL_3NF.CE_TRANSACTIONS', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        RAISE;
END
$$;

-- =====================================================
-- SECTION 3: FIXED SHIPMENT PROCEDURES WITH PROPER FOREIGN KEY MAPPINGS
-- =====================================================

--- PROCEDURE: Load CE_SHIPMENTS with FIXED order mapping
CREATE OR REPLACE PROCEDURE BL_CL.load_ce_shipments_idempotent()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    CALL BL_CL.log_procedure_event(
            'load_ce_shipments_idempotent', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_SHIPMENTS', 'START', 0,
            'Starting idempotent shipments load with CORRECTED source table reference'
         );

    INSERT INTO BL_3NF.CE_SHIPMENTS (shipment_src_id, order_id, geography_id, shipping_mode_id, warehouse_id,
                                     carrier_id, ship_date, shipping_cost, event_dt, source_system, source_entity)
    SELECT DISTINCT ON (lms.shipment_src_id)
           lms.shipment_src_id,
           COALESCE(o.order_id, -1)          as order_id,
           COALESCE(g.geography_id, -1)      as geography_id,
           COALESCE(sm.shipping_mode_id, -1) as shipping_mode_id,
           COALESCE(w.warehouse_id, -1)      as warehouse_id,
           COALESCE(car.carrier_id, -1)      as carrier_id,
           CASE
               WHEN lms.ship_dt ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                   THEN lms.ship_dt::DATE
               ELSE '1900-01-01'::DATE
               END                           as ship_date,
           CASE
               WHEN lms.shipping_cost ~ '^[0-9]+\.?[0-9]*$'
                   THEN lms.shipping_cost::DECIMAL(15, 2)
               ELSE NULL
               END                           as shipping_cost,
           CASE
               WHEN lms.ship_dt ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                   THEN lms.ship_dt::DATE
               ELSE '1900-01-01'::DATE
               END                           as event_dt,
           'LMS'                             as source_system,
           'SRC_LMS'                         as source_entity
    FROM SA_LMS.SRC_LMS lms  -- FIXED: Use correct source table instead of BL_CL.clean_lms_data
    -- Business rule: shipment_src_id = order_src_id (linking LMS shipments to OMS orders)
    LEFT JOIN BL_3NF.CE_ORDERS o
              ON o.order_src_id = lms.shipment_src_id
                  AND o.source_system = 'OMS'
    LEFT JOIN BL_3NF.CE_GEOGRAPHIES g
              ON g.geography_src_id = CONCAT(
                      COALESCE(lms.destination_city, 'Unknown'), '|',
                      COALESCE(lms.destination_state, 'Unknown'), '|',
                      COALESCE(lms.destination_country, 'Unknown')
                                      ) AND g.source_system = 'LMS'
    LEFT JOIN BL_3NF.CE_SHIPPING_MODES sm
              ON sm.shipping_mode_src_id = lms.shipping_mode
                  AND sm.source_system = 'LMS'
    LEFT JOIN BL_3NF.CE_WAREHOUSES w
              ON w.warehouse_src_id = lms.warehouse_src_id
                  AND w.source_system = 'LMS'
    LEFT JOIN BL_3NF.CE_CARRIERS car
              ON car.carrier_src_id = lms.carrier_src_id
                  AND car.source_system = 'LMS'
    WHERE lms.shipment_src_id IS NOT NULL
      AND lms.shipment_src_id != ''
    ORDER BY lms.shipment_src_id, lms.transaction_src_id
    ON CONFLICT (shipment_src_id, source_system) DO NOTHING;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    CALL BL_CL.log_procedure_event(
            'load_ce_shipments_idempotent', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_SHIPMENTS', 'SUCCESS',
            v_rows_affected, FORMAT('Shipments loaded: %s new records with corrected source table reference', v_rows_affected), v_execution_time
         );

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_shipments_idempotent', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_SHIPMENTS', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        RAISE;
END
$$;


CREATE OR REPLACE PROCEDURE BL_CL.load_ce_shipment_lines_idempotent()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    CALL BL_CL.log_procedure_event(
        'load_ce_shipment_lines_independent', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_SHIPMENT_LINES', 'START', 0,
        'Starting INDEPENDENT shipment lines load - no order line dependencies'
    );

    INSERT INTO BL_3NF.CE_SHIPMENT_LINES (
        shipment_line_src_id,
        shipment_id,
        product_id,
        customer_id,
        shipped_quantity,
        source_system,
        source_entity
    )
    SELECT
        -- Simple business key: shipment + product + customer
        CONCAT(
            grouped_lms.shipment_src_id, '|',
            grouped_lms.product_src_id, '|',
            grouped_lms.customer_src_id
        ) as shipment_line_src_id,

        COALESCE(s.shipment_id, -1) as shipment_id,
        COALESCE(p.product_id, -1) as product_id,
        COALESCE(c.customer_id, -1) as customer_id,
        grouped_lms.total_shipped_quantity,
        'LMS' as source_system,
        'SRC_LMS' as source_entity

    FROM (
        -- Aggregate by natural business key combination
        SELECT
            shipment_src_id,
            product_src_id,
            customer_src_id,
            SUM(CASE
                WHEN shipped_quantity ~ '^[0-9]+$'
                THEN shipped_quantity::INTEGER
                ELSE 0
            END) as total_shipped_quantity,
            MIN(ship_dt) as ship_dt  -- For SCD product lookup
        FROM SA_LMS.SRC_LMS
        WHERE shipment_src_id IS NOT NULL AND shipment_src_id != ''
          AND product_src_id IS NOT NULL AND product_src_id != ''
          AND customer_src_id IS NOT NULL AND customer_src_id != ''
        GROUP BY shipment_src_id, product_src_id, customer_src_id
    ) grouped_lms

    -- Join to dimension tables INDEPENDENTLY
    LEFT JOIN BL_3NF.CE_SHIPMENTS s
        ON s.shipment_src_id = grouped_lms.shipment_src_id
        AND s.source_system = 'LMS'

    LEFT JOIN BL_3NF.CE_PRODUCTS_SCD p
    ON p.product_src_id = grouped_lms.product_src_id
    AND p.source_system = 'OMS'
    AND CASE
        WHEN grouped_lms.ship_dt ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        THEN grouped_lms.ship_dt::DATE
        ELSE '1900-01-01'::DATE
    END BETWEEN p.start_dt AND p.end_dt

    LEFT JOIN BL_3NF.CE_CUSTOMERS c
        ON c.customer_src_id = grouped_lms.customer_src_id
        AND c.source_system = 'OMS'  -- Customers come from OMS system

    ON CONFLICT (shipment_line_src_id, source_system) DO NOTHING;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    CALL BL_CL.log_procedure_event(
        'load_ce_shipment_lines_independent', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_SHIPMENT_LINES', 'SUCCESS',
        v_rows_affected,
        FORMAT('Independent shipment lines loaded: %s distinct combinations', v_rows_affected),
        v_execution_time
    );

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
            'load_ce_shipment_lines_independent', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_SHIPMENT_LINES', 'ERROR',
            0, SQLERRM, v_execution_time, SQLSTATE
        );
        RAISE;
END
$$;

CREATE OR REPLACE PROCEDURE BL_CL.load_ce_deliveries_idempotent()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time     TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_affected  INTEGER   := 0;
    v_execution_time INTEGER;
BEGIN
    CALL BL_CL.log_procedure_event(
            'load_ce_deliveries_idempotent', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_DELIVERIES', 'START', 0,
            'Starting deliveries load with CORRECTED shipment-level grain'
         );

    -- Clear existing data for clean reload
    TRUNCATE TABLE BL_3NF.CE_DELIVERIES;

    INSERT INTO BL_3NF.CE_DELIVERIES (
        shipment_src_id,
        shipment_id,
        delivery_status_id,
        delivery_date,
        delivery_days,
        on_time_delivery,
        event_dt,
        source_system,
        source_entity,
        ta_insert_dt,
        ta_update_dt
    )
    SELECT DISTINCT
           lms.shipment_src_id,                    -- Business key (shipment level)
           COALESCE(s.shipment_id, -1)         as shipment_id,
           COALESCE(ds.delivery_status_id, -1) as delivery_status_id,

           -- Aggregate delivery metrics at shipment level
           MAX(CASE
               WHEN lms.delivery_dt ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
               THEN lms.delivery_dt::DATE
               ELSE NULL
               END)                            as delivery_date,

           MAX(CASE
               WHEN lms.delivery_days ~ '^[0-9]+$'
               THEN lms.delivery_days::INTEGER
               ELSE NULL
               END)                            as delivery_days,

           -- Use ANY_VALUE or MAX for on_time_delivery (shipment-level measure)
           BOOL_OR(CASE
               WHEN UPPER(lms.on_time_delivery) IN ('TRUE', 'T', '1', 'YES', 'Y') THEN TRUE
               WHEN UPPER(lms.on_time_delivery) IN ('FALSE', 'F', '0', 'NO', 'N') THEN FALSE
               ELSE NULL
               END)                            as on_time_delivery,

           COALESCE(
                   MAX(CASE
                       WHEN lms.delivery_dt ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                           THEN lms.delivery_dt::DATE
                       ELSE NULL
                       END),
                   MAX(CASE
                       WHEN lms.ship_dt ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                       THEN lms.ship_dt::DATE
                       ELSE '1900-01-01'::DATE
                       END)
           )                                   as event_dt,

           'LMS'                               as source_system,
           'SRC_LMS'                           as source_entity,
           CURRENT_TIMESTAMP                   as ta_insert_dt,
           CURRENT_TIMESTAMP                   as ta_update_dt

    FROM SA_LMS.SRC_LMS lms
    LEFT JOIN BL_3NF.CE_SHIPMENTS s
              ON s.shipment_src_id = lms.shipment_src_id
                  AND s.source_system = 'LMS'
    LEFT JOIN BL_3NF.CE_DELIVERY_STATUSES ds
              ON ds.delivery_status_src_id = lms.delivery_status
                  AND ds.source_system = 'LMS'
    WHERE lms.shipment_src_id IS NOT NULL
      AND lms.shipment_src_id != ''

    -- GROUP BY SHIPMENT LEVEL (not order line level)
    GROUP BY lms.shipment_src_id,
             s.shipment_id,
             ds.delivery_status_id;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;

    CALL BL_CL.log_procedure_event(
            'load_ce_deliveries_idempotent', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_DELIVERIES', 'SUCCESS',
            v_rows_affected,
            FORMAT('Deliveries loaded: %s shipment-level records (expected: 187,449)', v_rows_affected),
            v_execution_time
         );

    RAISE NOTICE 'CE_DELIVERIES loaded with shipment-level grain: % records', v_rows_affected;

EXCEPTION
    WHEN OTHERS THEN
        v_execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time)) * 1000;
        CALL BL_CL.log_procedure_event(
                'load_ce_deliveries_idempotent', 'SA_LMS.SRC_LMS', 'BL_3NF.CE_DELIVERIES', 'ERROR',
                0, SQLERRM, v_execution_time, SQLSTATE
             );
        RAISE;
END
$$;
-- =====================================================
-- SECTION 5: VALIDATION QUERIES
-- =====================================================

-- Check order lines count matches expected distinct combinations
SELECT 'Order Lines Count Check' as check_type,
       (SELECT COUNT(*) FROM BL_3NF.CE_ORDER_LINES WHERE source_system = 'OMS') as actual_count,
       (SELECT COUNT(DISTINCT CONCAT(order_src_id,'|',product_src_id,'|',customer_src_id))
        FROM SA_OMS.SRC_OMS
        WHERE order_src_id IS NOT NULL AND order_src_id != ''
          AND customer_src_id IS NOT NULL AND customer_src_id != ''
          AND product_src_id IS NOT NULL AND product_src_id != '') as expected_count;

-- Check shipment lines count matches expected distinct combinations
SELECT 'Shipment Lines Count Check' as check_type,
       (SELECT COUNT(*) FROM BL_3NF.CE_SHIPMENT_LINES WHERE source_system = 'LMS') as actual_count,
       (SELECT COUNT(DISTINCT CONCAT(shipment_src_id,'|',product_src_id,'|',customer_src_id))
        FROM SA_LMS.SRC_LMS
        WHERE shipment_src_id IS NOT NULL AND shipment_src_id != ''
          AND product_src_id IS NOT NULL AND product_src_id != ''
          AND customer_src_id IS NOT NULL AND customer_src_id != '') as expected_count;

-- Check for any remaining foreign key constraint violations
SELECT 'Orders without valid customer' as issue,
       COUNT(*) as count
FROM BL_3NF.CE_ORDERS
WHERE customer_id = -1
UNION ALL
SELECT 'Shipments without valid order' as issue,
       COUNT(*) as count
FROM BL_3NF.CE_SHIPMENTS
WHERE order_id = -1
UNION ALL
SELECT 'Shipment lines without valid shipment' as issue,
       COUNT(*) as count
FROM BL_3NF.CE_SHIPMENT_LINES
WHERE shipment_id = -1;