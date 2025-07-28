-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - DM LAYER PRIVILEGES
-- Purpose: Grant privileges for BL_DM layer operations
-- Run as: postgres superuser
-- Dependencies: dwh_cleansing_role must exist from 3NF layer setup
-- =====================================================

\c dwh_dev_pgsql;

-- =====================================================
-- SECTION 1: VERIFY EXISTING ROLE AND USER
-- =====================================================

-- Verify that dwh_cleansing_role exists from 3NF setup
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dwh_cleansing_role') THEN
        RAISE EXCEPTION 'dwh_cleansing_role does not exist. Please run 3NF layer BL_3NF_grant_privileges.sql first.';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dwh_cleansing_user') THEN
        RAISE EXCEPTION 'dwh_cleansing_user does not exist. Please run 3NF layer BL_3NF_grant_privileges.sql first.';
    END IF;

    RAISE NOTICE 'Existing cleansing role and user verified successfully.';
END $$;

-- Set search path to include all required schemas
SET search_path = BL_CL, BL_3NF, BL_DM, SA_OMS, SA_LMS, public;

-- =====================================================
-- SECTION 2: GRANT PRIVILEGES ON BL_DM SCHEMA
-- =====================================================

-- Grant usage on BL_DM schema
GRANT USAGE ON SCHEMA BL_DM TO dwh_cleansing_role;

-- Grant all privileges on BL_DM tables and sequences
GRANT ALL ON ALL TABLES IN SCHEMA BL_DM TO dwh_cleansing_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA BL_DM TO dwh_cleansing_role;

-- Grant default privileges for future BL_DM objects
ALTER DEFAULT PRIVILEGES IN SCHEMA BL_DM
    GRANT ALL ON TABLES TO dwh_cleansing_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA BL_DM
    GRANT ALL ON SEQUENCES TO dwh_cleansing_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA BL_DM
    GRANT ALL ON FUNCTIONS TO dwh_cleansing_role;

-- Allow user to create objects in BL_DM schema
GRANT CREATE ON SCHEMA BL_DM TO dwh_cleansing_user;


-- =====================================================
-- SECTION 4: SPECIFIC SEQUENCE PRIVILEGES FOR DM LAYER
-- =====================================================

-- Grant usage and update on specific DM sequences for dimension loading
GRANT USAGE, SELECT, UPDATE ON SEQUENCE BL_DM.SEQ_DIM_CUSTOMERS TO dwh_cleansing_role;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE BL_DM.SEQ_DIM_PRODUCTS_SCD TO dwh_cleansing_role;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE BL_DM.SEQ_DIM_GEOGRAPHIES TO dwh_cleansing_role;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE BL_DM.SEQ_DIM_SALES_REPRESENTATIVES TO dwh_cleansing_role;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE BL_DM.SEQ_DIM_WAREHOUSES TO dwh_cleansing_role;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE BL_DM.SEQ_DIM_CARRIERS TO dwh_cleansing_role;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE BL_DM.SEQ_DIM_ORDER_STATUSES TO dwh_cleansing_role;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE BL_DM.SEQ_DIM_PAYMENT_METHODS TO dwh_cleansing_role;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE BL_DM.SEQ_DIM_SHIPPING_MODES TO dwh_cleansing_role;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE BL_DM.SEQ_DIM_DELIVERY_STATUSES TO dwh_cleansing_role;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE BL_DM.SEQ_FCT_ORDER_LINE_SHIPMENTS TO dwh_cleansing_role;


-- =====================================================
-- SECTION 5: SPECIFIC TABLE PRIVILEGES FOR DM LAYER
-- =====================================================

-- Ensure specific privileges on key DM tables
GRANT SELECT, INSERT, UPDATE, DELETE ON BL_DM.DIM_CUSTOMERS TO dwh_cleansing_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON BL_DM.DIM_PRODUCTS_SCD TO dwh_cleansing_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON BL_DM.DIM_GEOGRAPHIES TO dwh_cleansing_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON BL_DM.DIM_SALES_REPRESENTATIVES TO dwh_cleansing_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON BL_DM.DIM_WAREHOUSES TO dwh_cleansing_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON BL_DM.DIM_CARRIERS TO dwh_cleansing_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON BL_DM.DIM_ORDER_STATUSES TO dwh_cleansing_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON BL_DM.DIM_PAYMENT_METHODS TO dwh_cleansing_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON BL_DM.DIM_SHIPPING_MODES TO dwh_cleansing_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON BL_DM.DIM_DELIVERY_STATUSES TO dwh_cleansing_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON BL_DM.DIM_TIME_DAY TO dwh_cleansing_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON BL_DM.FCT_ORDER_LINE_SHIPMENTS_DD TO dwh_cleansing_role;

-- =====================================================
-- SECTION : VERIFICATION QUERIES
-- =====================================================

-- Verify BL_DM schema privileges
SELECT
    n.nspname as schema_name,
    r.rolname as grantee,
    CASE
        WHEN has_schema_privilege(r.rolname, n.nspname, 'USAGE') THEN 'USAGE'
        ELSE 'NO ACCESS'
    END as usage_privilege,
    CASE
        WHEN has_schema_privilege(r.rolname, n.nspname, 'CREATE') THEN 'CREATE'
        ELSE 'NO CREATE'
    END as create_privilege
FROM pg_namespace n
CROSS JOIN pg_roles r
WHERE r.rolname = 'dwh_cleansing_role'
  AND n.nspname IN ('bl_cl', 'bl_3nf', 'bl_dm', 'sa_oms', 'sa_lms')
ORDER BY n.nspname;

-- Verify specific table privileges on DM dimension tables
SELECT
    table_schema,
    table_name,
    string_agg(privilege_type, ', ' ORDER BY privilege_type) as privileges
FROM information_schema.table_privileges
WHERE grantee = 'dwh_cleansing_role'
  AND table_schema = 'bl_dm'
  AND table_name LIKE 'dim_%'
GROUP BY table_schema, table_name
ORDER BY table_name;


-- Test basic access to key tables
SELECT 'BL_DM dimension table access test' as test_description,
       COUNT(*) as dim_customers_count
FROM BL_DM.DIM_CUSTOMERS;


-- Show summary of granted privileges
SELECT
    'SUMMARY: DM Layer Privileges' as summary,
    COUNT(CASE WHEN table_schema = 'bl_dm' THEN 1 END) as dm_tables_granted,
    COUNT(CASE WHEN table_schema = 'bl_3nf' THEN 1 END) as nf3_tables_granted,
    COUNT(CASE WHEN table_schema = 'bl_cl' THEN 1 END) as cl_tables_granted
FROM information_schema.table_privileges
WHERE grantee = 'dwh_cleansing_role';

COMMIT;