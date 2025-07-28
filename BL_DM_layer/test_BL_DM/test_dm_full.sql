--===================================
-- Complete DM layer load
--===================================
TRUNCATE bl_dm.fct_order_line_shipments_dd;

CALL BL_CL.cleanup_all_dimension_tables();



CALL BL_CL.load_bl_dm_full(TRUE);