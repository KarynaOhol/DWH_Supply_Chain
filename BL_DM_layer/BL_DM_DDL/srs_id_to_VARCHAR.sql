-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - DM LAYER SCHEMA FIXES
-- File: BL_CL_DM_Layer_Implementation/01_Foundation/alter_src_ids_to_varchar.sql
-- Purpose: Change all SRC_ID columns from BIGINT to VARCHAR to match 3NF layer
-- Reason: Maintain data type consistency between layers (3NF uses VARCHAR)
-- Run as: dwh_cleansing_user (or postgres superuser)
-- =====================================================

\c dwh_dev_pgsql;

SET search_path = BL_DM, BL_3NF, public;

-- =====================================================
-- SECTION 1: ALTER DIM_CUSTOMERS
-- =====================================================

-- Drop foreign key constraints temporarily (if any reference this)
-- ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD DROP CONSTRAINT IF EXISTS FK_FCT_CUSTOMER;

-- Alter customer source ID to VARCHAR
ALTER TABLE BL_DM.DIM_CUSTOMERS 
    ALTER COLUMN CUSTOMER_SRC_ID TYPE VARCHAR(50);

-- Recreate foreign key constraint
-- ALTER TABLE BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD 
--     ADD CONSTRAINT FK_FCT_CUSTOMER FOREIGN KEY (CUSTOMER_SURR_ID) REFERENCES DIM_CUSTOMERS (CUSTOMER_SURR_ID);

-- =====================================================
-- SECTION 2: ALTER DIM_PRODUCTS_SCD
-- =====================================================

-- Alter product source ID to VARCHAR
ALTER TABLE BL_DM.DIM_PRODUCTS_SCD 
    ALTER COLUMN BRAND_SRC_ID TYPE VARCHAR(50);

ALTER TABLE BL_DM.DIM_PRODUCTS_SCD 
    ALTER COLUMN PRIMARY_CATEGORY_SRC_ID TYPE VARCHAR(50);

ALTER TABLE BL_DM.DIM_PRODUCTS_SCD 
    ALTER COLUMN DEPARTMENT_SRC_ID TYPE VARCHAR(50);

ALTER TABLE BL_DM.DIM_PRODUCTS_SCD 
    ALTER COLUMN PRODUCT_STATUS_SRC_ID TYPE VARCHAR(50);

-- =====================================================
-- SECTION 3: ALTER DIM_GEOGRAPHIES
-- =====================================================

-- Alter geography hierarchy source IDs to VARCHAR
ALTER TABLE BL_DM.DIM_GEOGRAPHIES 
    ALTER COLUMN GEOGRAPHY_SRC_ID TYPE VARCHAR(50);

ALTER TABLE BL_DM.DIM_GEOGRAPHIES 
    ALTER COLUMN CITY_SRC_ID TYPE VARCHAR(50);

ALTER TABLE BL_DM.DIM_GEOGRAPHIES 
    ALTER COLUMN STATE_SRC_ID TYPE VARCHAR(50);

ALTER TABLE BL_DM.DIM_GEOGRAPHIES 
    ALTER COLUMN COUNTRY_SRC_ID TYPE VARCHAR(50);

ALTER TABLE BL_DM.DIM_GEOGRAPHIES 
    ALTER COLUMN REGION_SRC_ID TYPE VARCHAR(50);

-- =====================================================
-- SECTION 4: ALTER DIM_SALES_REPRESENTATIVES
-- =====================================================

-- Alter sales rep source ID to VARCHAR
ALTER TABLE BL_DM.DIM_SALES_REPRESENTATIVES 
    ALTER COLUMN SALES_REP_SRC_ID TYPE VARCHAR(50);

-- =====================================================
-- SECTION 5: ALTER DIM_WAREHOUSES
-- =====================================================

-- Alter warehouse source ID to VARCHAR
ALTER TABLE BL_DM.DIM_WAREHOUSES 
    ALTER COLUMN WAREHOUSE_SRC_ID TYPE VARCHAR(50);

-- =====================================================
-- SECTION 6: ALTER DIM_CARRIERS
-- =====================================================

-- Alter carrier source ID to VARCHAR
ALTER TABLE BL_DM.DIM_CARRIERS 
    ALTER COLUMN CARRIER_SRC_ID TYPE VARCHAR(50);

-- =====================================================
-- SECTION 7: ALTER DIM_ORDER_STATUSES
-- =====================================================

-- Alter order status source ID to VARCHAR
ALTER TABLE BL_DM.DIM_ORDER_STATUSES 
    ALTER COLUMN ORDER_STATUS_SRC_ID TYPE VARCHAR(50);

-- =====================================================
-- SECTION 8: ALTER DIM_PAYMENT_METHODS
-- =====================================================

-- Alter payment method source ID to VARCHAR
ALTER TABLE BL_DM.DIM_PAYMENT_METHODS 
    ALTER COLUMN PAYMENT_METHOD_SRC_ID TYPE VARCHAR(50);

-- =====================================================
-- SECTION 9: ALTER DIM_SHIPPING_MODES
-- =====================================================

-- Alter shipping mode source ID to VARCHAR
ALTER TABLE BL_DM.DIM_SHIPPING_MODES 
    ALTER COLUMN SHIPPING_MODE_SRC_ID TYPE VARCHAR(50);

-- =====================================================
-- SECTION 10: ALTER DIM_DELIVERY_STATUSES
-- =====================================================

-- Alter delivery status source ID to VARCHAR
ALTER TABLE BL_DM.DIM_DELIVERY_STATUSES 
    ALTER COLUMN DELIVERY_STATUS_SRC_ID TYPE VARCHAR(50);


-- =====================================================
-- SECTION 11: VERIFICATION QUERIES
-- =====================================================

-- Verify the changes
SELECT 
    table_name,
    column_name,
    data_type,
    character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'bl_dm'
  AND column_name LIKE '%_src_id'
  AND table_name LIKE 'dim_%'
ORDER BY table_name, column_name;
