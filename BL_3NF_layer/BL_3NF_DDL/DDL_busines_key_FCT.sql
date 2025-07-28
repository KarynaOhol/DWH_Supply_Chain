-- =====================================================
-- SUPPLY CHAIN DATA WAREHOUSE - ADD BUSINESS KEYS TO FACT TABLES
-- Purpose: Add business keys and unique constraints for idempotent loading
-- Run as: dwh_cleansing_user
-- Dependencies: 3NF schema already exists
-- =====================================================

\c dwh_dev_pgsql;
-- SET ROLE dwh_cleansing_user;
SET search_path = BL_3NF, BL_CL, public;

-- =====================================================
-- SECTION 1: ADD BUSINESS KEYS TO FACT TABLES
-- =====================================================

-- 1. CE_ORDERS - Add business keys
ALTER TABLE BL_3NF.CE_ORDERS ADD COLUMN IF NOT EXISTS order_src_id VARCHAR(50);
ALTER TABLE BL_3NF.CE_ORDERS ADD COLUMN IF NOT EXISTS source_system VARCHAR(50) DEFAULT 'OMS';
ALTER TABLE BL_3NF.CE_ORDERS ADD COLUMN IF NOT EXISTS source_entity VARCHAR(100) DEFAULT 'SRC_OMS';

-- 2. CE_ORDER_LINES - Add business keys
ALTER TABLE BL_3NF.CE_ORDER_LINES ADD COLUMN IF NOT EXISTS order_line_src_id VARCHAR(50);
ALTER TABLE BL_3NF.CE_ORDER_LINES ADD COLUMN IF NOT EXISTS source_system VARCHAR(50) DEFAULT 'OMS';
ALTER TABLE BL_3NF.CE_ORDER_LINES ADD COLUMN IF NOT EXISTS source_entity VARCHAR(100) DEFAULT 'SRC_OMS';

-- 3. CE_TRANSACTIONS - Add business keys
ALTER TABLE BL_3NF.CE_TRANSACTIONS ADD COLUMN IF NOT EXISTS transaction_src_id VARCHAR(50);
ALTER TABLE BL_3NF.CE_TRANSACTIONS ADD COLUMN IF NOT EXISTS source_system VARCHAR(50) DEFAULT 'OMS';
ALTER TABLE BL_3NF.CE_TRANSACTIONS ADD COLUMN IF NOT EXISTS source_entity VARCHAR(100) DEFAULT 'SRC_OMS';

-- 4. CE_SHIPMENTS - Add business keys
ALTER TABLE BL_3NF.CE_SHIPMENTS ADD COLUMN IF NOT EXISTS shipment_src_id VARCHAR(50);
ALTER TABLE BL_3NF.CE_SHIPMENTS ADD COLUMN IF NOT EXISTS source_system VARCHAR(50) DEFAULT 'LMS';
ALTER TABLE BL_3NF.CE_SHIPMENTS ADD COLUMN IF NOT EXISTS source_entity VARCHAR(100) DEFAULT 'SRC_LMS';

-- 5. CE_SHIPMENT_LINES - Add business keys (composite)
ALTER TABLE BL_3NF.CE_SHIPMENT_LINES ADD COLUMN IF NOT EXISTS shipment_src_id VARCHAR(50);
ALTER TABLE BL_3NF.CE_SHIPMENT_LINES ADD COLUMN IF NOT EXISTS order_line_src_id VARCHAR(50);
ALTER TABLE BL_3NF.CE_SHIPMENT_LINES ADD COLUMN IF NOT EXISTS source_system VARCHAR(50) DEFAULT 'LMS';
ALTER TABLE BL_3NF.CE_SHIPMENT_LINES ADD COLUMN IF NOT EXISTS source_entity VARCHAR(100) DEFAULT 'SRC_LMS';

-- 6. CE_DELIVERIES - Add business keys (composite)
ALTER TABLE BL_3NF.CE_DELIVERIES ADD COLUMN IF NOT EXISTS shipment_src_id VARCHAR(50);
ALTER TABLE BL_3NF.CE_DELIVERIES ADD COLUMN IF NOT EXISTS order_line_src_id VARCHAR(50);
ALTER TABLE BL_3NF.CE_DELIVERIES ADD COLUMN IF NOT EXISTS source_system VARCHAR(50) DEFAULT 'LMS';
ALTER TABLE BL_3NF.CE_DELIVERIES ADD COLUMN IF NOT EXISTS source_entity VARCHAR(100) DEFAULT 'SRC_LMS';

-- =====================================================
-- SECTION 2: CREATE UNIQUE CONSTRAINTS FOR IDEMPOTENCY
-- =====================================================

-- Drop existing constraints if they exist (for rerun capability)
ALTER TABLE BL_3NF.CE_ORDERS DROP CONSTRAINT IF EXISTS uk_orders_business_key;
ALTER TABLE BL_3NF.CE_ORDER_LINES DROP CONSTRAINT IF EXISTS uk_order_lines_business_key;
ALTER TABLE BL_3NF.CE_TRANSACTIONS DROP CONSTRAINT IF EXISTS uk_transactions_business_key;
ALTER TABLE BL_3NF.CE_SHIPMENTS DROP CONSTRAINT IF EXISTS uk_shipments_business_key;
ALTER TABLE BL_3NF.CE_SHIPMENT_LINES DROP CONSTRAINT IF EXISTS uk_shipment_lines_business_key;
ALTER TABLE BL_3NF.CE_DELIVERIES DROP CONSTRAINT IF EXISTS uk_deliveries_business_key;

-- 1. CE_ORDERS unique constraint
ALTER TABLE BL_3NF.CE_ORDERS
ADD CONSTRAINT uk_orders_business_key
UNIQUE (order_src_id, source_system);

-- 2. CE_ORDER_LINES unique constraint
ALTER TABLE BL_3NF.CE_ORDER_LINES
ADD CONSTRAINT uk_order_lines_business_key
UNIQUE (order_line_src_id, source_system);

-- 3. CE_TRANSACTIONS unique constraint
ALTER TABLE BL_3NF.CE_TRANSACTIONS
ADD CONSTRAINT uk_transactions_business_key
UNIQUE (transaction_src_id, source_system);

-- 4. CE_SHIPMENTS unique constraint
ALTER TABLE BL_3NF.CE_SHIPMENTS
ADD CONSTRAINT uk_shipments_business_key
UNIQUE (shipment_src_id, source_system);

-- 5. CE_SHIPMENT_LINES composite unique constraint
ALTER TABLE BL_3NF.CE_SHIPMENT_LINES
ADD CONSTRAINT uk_shipment_lines_business_key
UNIQUE (shipment_src_id, order_line_src_id, source_system);

-- 6. CE_DELIVERIES composite unique constraint
ALTER TABLE BL_3NF.CE_DELIVERIES
ADD CONSTRAINT uk_deliveries_business_key
UNIQUE (shipment_src_id, order_line_src_id, source_system);

-- =====================================================
-- SECTION 3: CREATE INDEXES FOR PERFORMANCE
-- =====================================================

-- Business key indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_orders_business_key ON BL_3NF.CE_ORDERS (order_src_id, source_system);
CREATE INDEX IF NOT EXISTS idx_order_lines_business_key ON BL_3NF.CE_ORDER_LINES (order_line_src_id, source_system);
CREATE INDEX IF NOT EXISTS idx_transactions_business_key ON BL_3NF.CE_TRANSACTIONS (transaction_src_id, source_system);
CREATE INDEX IF NOT EXISTS idx_shipments_business_key ON BL_3NF.CE_SHIPMENTS (shipment_src_id, source_system);
CREATE INDEX IF NOT EXISTS idx_shipment_lines_business_key ON BL_3NF.CE_SHIPMENT_LINES (shipment_src_id, order_line_src_id, source_system);
CREATE INDEX IF NOT EXISTS idx_deliveries_business_key ON BL_3NF.CE_DELIVERIES (shipment_src_id, order_line_src_id, source_system);

-- Source system indexes for filtering
CREATE INDEX IF NOT EXISTS idx_orders_source_system ON BL_3NF.CE_ORDERS (source_system);
CREATE INDEX IF NOT EXISTS idx_order_lines_source_system ON BL_3NF.CE_ORDER_LINES (source_system);
CREATE INDEX IF NOT EXISTS idx_transactions_source_system ON BL_3NF.CE_TRANSACTIONS (source_system);
CREATE INDEX IF NOT EXISTS idx_shipments_source_system ON BL_3NF.CE_SHIPMENTS (source_system);
CREATE INDEX IF NOT EXISTS idx_shipment_lines_source_system ON BL_3NF.CE_SHIPMENT_LINES (source_system);
CREATE INDEX IF NOT EXISTS idx_deliveries_source_system ON BL_3NF.CE_DELIVERIES (source_system);

-- =====================================================
-- SECTION 4: VERIFICATION QUERIES
-- =====================================================

-- Verify columns were added
SELECT
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'bl_3nf'
  AND table_name IN ('ce_orders', 'ce_order_lines', 'ce_transactions', 'ce_shipments', 'ce_shipment_lines', 'ce_deliveries')
  AND column_name IN ('order_src_id', 'order_line_src_id', 'transaction_src_id', 'shipment_src_id', 'source_system', 'source_entity')
ORDER BY table_name, column_name;

-- Verify unique constraints
SELECT
    tc.table_name,
    tc.constraint_name,
    tc.constraint_type,
    STRING_AGG(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as columns
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
WHERE tc.table_schema = 'bl_3nf'
  AND tc.constraint_type = 'UNIQUE'
  AND tc.constraint_name LIKE '%business_key%'
GROUP BY tc.table_name, tc.constraint_name, tc.constraint_type
ORDER BY tc.table_name;

-- Verify indexes
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'bl_3nf'
  AND (indexname LIKE '%business_key%' OR indexname LIKE '%source_system%')
ORDER BY tablename, indexname;

-- Show current row counts (should be same as before)
SELECT 'CE_ORDERS' as table_name, COUNT(*) as record_count FROM BL_3NF.CE_ORDERS
UNION ALL
SELECT 'CE_ORDER_LINES', COUNT(*) FROM BL_3NF.CE_ORDER_LINES
UNION ALL
SELECT 'CE_TRANSACTIONS', COUNT(*) FROM BL_3NF.CE_TRANSACTIONS
UNION ALL
SELECT 'CE_SHIPMENTS', COUNT(*) FROM BL_3NF.CE_SHIPMENTS
UNION ALL
SELECT 'CE_SHIPMENT_LINES', COUNT(*) FROM BL_3NF.CE_SHIPMENT_LINES
UNION ALL
SELECT 'CE_DELIVERIES', COUNT(*) FROM BL_3NF.CE_DELIVERIES
ORDER BY table_name;

COMMIT;