-- =============================================================
-- Create DB2 explain tables for the test schema.
-- These are required for EXPLAIN PLAN FOR to populate results.
-- Run with: db2 -tvf explain_setup.sql
-- =============================================================

CALL SYSPROC.SYSINSTALLOBJECTS('EXPLAIN', 'C', CAST(NULL AS VARCHAR(128)), CAST(NULL AS VARCHAR(128)));
