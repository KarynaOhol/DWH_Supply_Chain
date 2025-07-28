-- =====================================================
-- MASTER STAGING PROCEDURE FRAMEWORK
-- Purpose: Dynamic file loading with incremental support
-- =====================================================

--- =====================================================
-- IMPROVED EXTERNAL TABLE CREATION FUNCTION
-- =====================================================

CREATE OR REPLACE FUNCTION BL_CL.CREATE_DYNAMIC_EXTERNAL_TABLE(
    p_source_system VARCHAR,
    p_file_path VARCHAR,
    p_table_suffix VARCHAR DEFAULT 'TEMP'
) RETURNS VARCHAR AS
$$
DECLARE
    v_ext_table_name  VARCHAR;
    v_target_schema   VARCHAR;
    v_sql             TEXT;
    v_procedure_name  VARCHAR := 'CREATE_DYNAMIC_EXTERNAL_TABLE';
    v_full_table_name VARCHAR;
BEGIN
    -- Construct schema name with lowercase
    v_target_schema := 'sa_' || LOWER(p_source_system);
    v_ext_table_name := 'ext_' || LOWER(p_source_system) || '_' || p_table_suffix;
    v_full_table_name := v_target_schema || '.' || v_ext_table_name;

    -- Log start
    CALL BL_CL.log_procedure_event(
            v_procedure_name,
            p_file_path,
            v_full_table_name,
            'START',
            0,
            'Creating external table for ' || p_source_system
         );

    -- Drop existing external table if exists using consistent naming
    CALL BL_CL.DROP_EXTERNAL_TABLE(v_full_table_name);

    -- Create external table based on source system
    IF p_source_system = 'OMS' THEN
        v_sql := FORMAT('
            CREATE FOREIGN TABLE %I.%I (
                TransactionSK VARCHAR(50),
                OrderID VARCHAR(50),
                OrderItemID VARCHAR(50),
                CustomerID VARCHAR(50),
                CustomerFirstName VARCHAR(100),
                CustomerLastName VARCHAR(100),
                CustomerGender VARCHAR(10),
                CustomerYearOfBirth VARCHAR(10),
                CustomerEmail VARCHAR(255),
                CustomerSegment VARCHAR(50),
                ProductID VARCHAR(50),
                ProductName VARCHAR(255),
                ProductBrand VARCHAR(100),
                ProductStatus VARCHAR(50),
                ProductCategoryID VARCHAR(50),
                ProductCategory VARCHAR(100),
                DepartmentID VARCHAR(50),
                Department VARCHAR(100),
                SalesRepID VARCHAR(50),
                SalesAmount VARCHAR(20),
                Quantity VARCHAR(10),
                OrderTotal VARCHAR(20),
                UnitPrice VARCHAR(20),
                OrderStatus VARCHAR(50),
                PaymentMethod VARCHAR(100),
                SourceSystem VARCHAR(50),
                OrderDate VARCHAR(20),
                UnitCost VARCHAR(20),
                TotalCost VARCHAR(20),
                OrderYear VARCHAR(10),
                OrderMonth VARCHAR(10),
                OrderQuarter VARCHAR(10),
                OrderDayOfWeek VARCHAR(10),
                OrderWeekOfYear VARCHAR(10)
            ) SERVER file_server
            OPTIONS (
                filename %L,
                format ''csv'',
                header ''true'',
                delimiter '','',
                null ''''
            )', v_target_schema, v_ext_table_name, p_file_path);

    ELSIF p_source_system = 'LMS' THEN
        v_sql := FORMAT('
            CREATE FOREIGN TABLE %I.%I (
                TransactionSK VARCHAR(50),
                ShipmentID VARCHAR(50),
                CustomerID VARCHAR(50),
                ProductID VARCHAR(50),
                ProductName VARCHAR(255),
                ShippedQuantity VARCHAR(10),
                DestinationCity VARCHAR(100),
                DestinationState VARCHAR(100),
                DestinationCountry VARCHAR(100),
                ShippingMode VARCHAR(50),
                DeliveryStatus VARCHAR(50),
                WarehouseID VARCHAR(50),
                CarrierID VARCHAR(50),
                ShippingCost VARCHAR(20),
                DeliveryDays VARCHAR(10),
                OnTimeDelivery VARCHAR(10),
                SourceSystem VARCHAR(50),
                OrderDate VARCHAR(20),
                ShipDate VARCHAR(20),
                OrderToShipDays VARCHAR(10),
                DeliveryDate VARCHAR(20)
            ) SERVER file_server
            OPTIONS (
                filename %L,
                format ''csv'',
                header ''true'',
                delimiter '','',
                null ''''
            )', v_target_schema, v_ext_table_name, p_file_path);
    ELSE
        RAISE EXCEPTION 'Unsupported source system: %', p_source_system;
    END IF;

    EXECUTE v_sql;

    -- Verify the table was created by checking if it exists
    PERFORM 1
    FROM information_schema.tables
    WHERE table_schema = v_target_schema
      AND table_name = v_ext_table_name
      AND table_type = 'FOREIGN TABLE';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Failed to create external table %', v_full_table_name;
    END IF;

    -- Log success
    CALL BL_CL.log_procedure_event(
            v_procedure_name,
            p_file_path,
            v_full_table_name,
            'SUCCESS',
            1,
            'External table created successfully'
         );

    -- Return the external table name
    RETURN v_ext_table_name;

EXCEPTION
    WHEN OTHERS THEN
        -- Log error using your framework
        CALL BL_CL.log_procedure_event(
                v_procedure_name,
                p_file_path,
                v_full_table_name,
                'ERROR',
                0,
                SQLERRM,
                0,
                SQLSTATE
             );
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- CENTRALIZED TABLE CLEANUP FUNCTION
-- =====================================================

CREATE OR REPLACE PROCEDURE BL_CL.DROP_EXTERNAL_TABLE(
    p_full_table_name VARCHAR
) AS
$$
DECLARE
    v_schema_name  VARCHAR;
    v_table_name   VARCHAR;
    v_table_exists BOOLEAN;
BEGIN
    -- Parse schema and table name from full name
    v_schema_name := SPLIT_PART(p_full_table_name, '.', 1);
    v_table_name := SPLIT_PART(p_full_table_name, '.', 2);

    -- Check if table exists before attempting to drop
    SELECT EXISTS(SELECT 1
                  FROM information_schema.tables
                  WHERE table_schema = v_schema_name
                    AND table_name = v_table_name
                    AND table_type = 'FOREIGN TABLE')
    INTO v_table_exists;

    IF v_table_exists THEN
        EXECUTE FORMAT('DROP FOREIGN TABLE %I.%I', v_schema_name, v_table_name);
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        -- Log but don't fail - cleanup is best effort
        RAISE WARNING 'Failed to drop external table %: %', p_full_table_name, SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- UPDATED SQL BUILDER FUNCTIONS
-- =====================================================

CREATE OR REPLACE FUNCTION BL_CL.BUILD_OMS_INSERT_SQL(
    p_target_schema VARCHAR,
    p_target_table VARCHAR,
    p_ext_schema VARCHAR,
    p_ext_table_name VARCHAR,
    p_where_clause VARCHAR DEFAULT ''
) RETURNS TEXT AS
$$
BEGIN
    RETURN FORMAT('
        INSERT INTO %I.%I (
            TRANSACTION_SRC_ID, ORDER_SRC_ID, ORDER_ITEM_SRC_ID, CUSTOMER_SRC_ID,
            CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME, CUSTOMER_GENDER, CUSTOMER_YEAR_OF_BIRTH,
            CUSTOMER_EMAIL, CUSTOMER_SEGMENT, PRODUCT_SRC_ID, PRODUCT_NAME, PRODUCT_BRAND,
            PRODUCT_STATUS, PRODUCT_CATEGORY_SRC_ID, PRODUCT_CATEGORY, DEPARTMENT_SRC_ID,
            DEPARTMENT_NAME, SALES_REP_SRC_ID, SALES_AMOUNT, QUANTITY, ORDER_TOTAL,
            UNIT_PRICE, ORDER_STATUS, PAYMENT_METHOD, SOURCE_SYSTEM, ORDER_DT,
            UNIT_COST, TOTAL_COST, ORDER_YEAR, ORDER_MONTH, ORDER_QUARTER,
            ORDER_DAY_OF_WEEK, ORDER_WEEK_OF_YEAR
        )
        SELECT
            TransactionSK, OrderID, OrderItemID, CustomerID, CustomerFirstName,
            CustomerLastName, CustomerGender, CustomerYearOfBirth, CustomerEmail,
            CustomerSegment, ProductID, ProductName, ProductBrand, ProductStatus,
            ProductCategoryID, ProductCategory, DepartmentID, Department, SalesRepID,
            SalesAmount, Quantity, OrderTotal, UnitPrice, OrderStatus, PaymentMethod,
            SourceSystem, OrderDate, UnitCost, TotalCost, OrderYear, OrderMonth,
            OrderQuarter, OrderDayOfWeek, OrderWeekOfYear
        FROM %I.%I
        WHERE TransactionSK != ''TransactionSK''
          AND TransactionSK IS NOT NULL
          AND TRIM(TransactionSK) != '''' %s',
                  p_target_schema, p_target_table, p_ext_schema, p_ext_table_name, COALESCE(p_where_clause, ''));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION BL_CL.BUILD_LMS_INSERT_SQL(
    p_target_schema VARCHAR,
    p_target_table VARCHAR,
    p_ext_schema VARCHAR,
    p_ext_table_name VARCHAR,
    p_where_clause VARCHAR DEFAULT ''
) RETURNS TEXT AS
$$
BEGIN
    RETURN FORMAT('
        INSERT INTO %I.%I (
            TRANSACTION_SRC_ID, SHIPMENT_SRC_ID, CUSTOMER_SRC_ID, PRODUCT_SRC_ID,
            PRODUCT_NAME, SHIPPED_QUANTITY, DESTINATION_CITY, DESTINATION_STATE,
            DESTINATION_COUNTRY, SHIPPING_MODE, DELIVERY_STATUS, WAREHOUSE_SRC_ID,
            CARRIER_SRC_ID, SHIPPING_COST, DELIVERY_DAYS, ON_TIME_DELIVERY,
            SOURCE_SYSTEM, ORDER_DT, SHIP_DT, ORDER_TO_SHIP_DAYS, DELIVERY_DT
        )
        SELECT
            TransactionSK, ShipmentID, CustomerID, ProductID, ProductName,
            ShippedQuantity, DestinationCity, DestinationState, DestinationCountry,
            ShippingMode, DeliveryStatus, WarehouseID, CarrierID, ShippingCost,
            DeliveryDays, OnTimeDelivery, SourceSystem, OrderDate, ShipDate,
            OrderToShipDays, DeliveryDate
        FROM %I.%I
        WHERE TransactionSK != ''TransactionSK''
          AND TransactionSK IS NOT NULL
          AND TRIM(TransactionSK) != '''' %s',
                  p_target_schema, p_target_table, p_ext_schema, p_ext_table_name, COALESCE(p_where_clause, ''));
END;
$$ LANGUAGE plpgsql;