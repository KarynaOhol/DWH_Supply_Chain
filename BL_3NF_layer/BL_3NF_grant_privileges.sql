-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - CLEANSING LAYER
-- Purpose: Create BL_CL schema and grant privileges
-- =====================================================

\c dwh_dev_pgsql;

-- =====================================================
-- SECTION 1: CREATE BL_CL SCHEMA
-- =====================================================

CREATE SCHEMA IF NOT EXISTS BL_CL;

-- Set search path to include all required schemas
SET search_path = BL_CL, BL_3NF, SA_OMS, SA_LMS, public;

-- =====================================================
-- SECTION 2: CREATE ROLE FOR CLEANSING LAYER
-- =====================================================

-- Create role for cleansing layer operations
DROP ROLE IF EXISTS dwh_cleansing_role;
CREATE ROLE dwh_cleansing_role;

-- =====================================================
-- SECTION 3: GRANT PRIVILEGES TO BL_CL SCHEMA
-- =====================================================

-- Grant all privileges on BL_CL schema to the role
GRANT ALL ON SCHEMA BL_CL TO dwh_cleansing_role;
GRANT ALL ON ALL TABLES IN SCHEMA BL_CL TO dwh_cleansing_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA BL_CL TO dwh_cleansing_role;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA BL_CL TO dwh_cleansing_role;


-- Grant default privileges for future objects in BL_CL
ALTER DEFAULT PRIVILEGES IN SCHEMA BL_CL
    GRANT ALL ON TABLES TO dwh_cleansing_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA BL_CL
    GRANT ALL ON SEQUENCES TO dwh_cleansing_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA BL_CL
    GRANT ALL ON FUNCTIONS TO dwh_cleansing_role;


-- =====================================================
-- SECTION 4: GRANT READ PRIVILEGES ON STAGING SCHEMAS
-- =====================================================

-- Grant usage on staging schemas
GRANT USAGE ON SCHEMA SA_OMS TO dwh_cleansing_role;
GRANT USAGE ON SCHEMA SA_LMS TO dwh_cleansing_role;

-- Grant select privileges on all staging tables
GRANT SELECT ON ALL TABLES IN SCHEMA SA_OMS TO dwh_cleansing_role;
GRANT SELECT ON ALL TABLES IN SCHEMA SA_LMS TO dwh_cleansing_role;

-- Grant default select privileges for future staging tables
ALTER DEFAULT PRIVILEGES IN SCHEMA SA_OMS
    GRANT SELECT ON TABLES TO dwh_cleansing_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA SA_LMS
    GRANT SELECT ON TABLES TO dwh_cleansing_role;

-- =====================================================
-- SECTION 5: GRANT WRITE PRIVILEGES ON 3NF SCHEMA
-- =====================================================

-- Grant usage on 3NF schema
GRANT USAGE ON SCHEMA BL_3NF TO dwh_cleansing_role;

-- Grant all privileges on 3NF tables and sequences
GRANT ALL ON ALL TABLES IN SCHEMA BL_3NF TO dwh_cleansing_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA BL_3NF TO dwh_cleansing_role;

-- Grant default privileges for future 3NF objects
ALTER DEFAULT PRIVILEGES IN SCHEMA BL_3NF
    GRANT ALL ON TABLES TO dwh_cleansing_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA BL_3NF
    GRANT ALL ON SEQUENCES TO dwh_cleansing_role;

-- =====================================================
-- SECTION 6: CREATE DATABASE USER AND ASSIGN ROLE
-- =====================================================

-- Create user for cleansing operations
DROP USER IF EXISTS dwh_cleansing_user;
CREATE USER dwh_cleansing_user WITH PASSWORD 'clean_123';

-- Assign role to user
GRANT dwh_cleansing_role TO dwh_cleansing_user;

-- Allow user to create objects in assigned schemas
GRANT CREATE ON SCHEMA BL_CL TO dwh_cleansing_user;

-- =====================================================
-- SECTION 7: ADDITIONAL SYSTEM PRIVILEGES
-- =====================================================

-- Grant temporary table creation privileges (for ETL operations)
GRANT TEMPORARY ON DATABASE dwh_dev_pgsql TO dwh_cleansing_role;

-- Grant connect privilege to database
GRANT CONNECT ON DATABASE dwh_dev_pgsql TO dwh_cleansing_role;

-- =====================================================
-- SECTION 8: VERIFICATION QUERIES
-- =====================================================

-- Verify role creation
SELECT rolname, rolcanlogin, rolcreaterole, rolcreatedb
FROM pg_roles
WHERE rolname = 'dwh_cleansing_role';

-- Verify user creation and role assignment
SELECT r.rolname as role_name,
       m.rolname as member_name
FROM pg_roles r
         JOIN pg_auth_members am ON r.oid = am.roleid
         JOIN pg_roles m ON am.member = m.oid
WHERE r.rolname = 'dwh_cleansing_role';

-- Verify schema privileges
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
  AND n.nspname IN ('bl_cl', 'bl_3nf', 'sa_oms', 'sa_lms')
ORDER BY n.nspname;

-- Verify table privileges
SELECT table_schema,
       table_name,
       privilege_type,
       grantee
FROM information_schema.table_privileges
WHERE grantee = 'dwh_cleansing_role'
ORDER BY table_schema, table_name, privilege_type;


COMMIT;
