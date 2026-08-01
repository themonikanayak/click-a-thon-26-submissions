-- #####################################################################
-- reset.sql — DROP every object the build creates, so `--build` can
-- recreate them from a clean slate (the "drop the tables and recreate"
-- flow). Fully IDEMPOTENT: every statement is DROP ... IF EXISTS, so
-- running it on an empty database is a harmless no-op.
--
--   python migrations/run_sql.py --reset --build
--
-- This is DESTRUCTIVE (drops data). It is NOT part of the ordered
-- migrations applied by `--migrate`; it only runs when you pass --reset.
-- Drop order respects dependencies: MVs → view → dictionary → tables →
-- functions (a function referenced by an MV can't be dropped first, and
-- cfg_gap_timeout_seconds depends on the two knob functions).
-- #####################################################################

-- A. MATERIALIZED VIEWS (depend on tables + the cfg_/norm_ functions) -----------
DROP VIEW IF EXISTS sonyliv_concurrency.mv_incoming_to_raw;
DROP VIEW IF EXISTS sonyliv_concurrency.mv_session_intervals;
DROP VIEW IF EXISTS sonyliv_concurrency.concurrency_hot_abs_mv;

-- B. SERVING VIEW ---------------------------------------------------------------
DROP VIEW IF EXISTS sonyliv_concurrency.concurrency_now;

-- C. DICTIONARY -----------------------------------------------------------------
DROP DICTIONARY IF EXISTS sonyliv_concurrency.content_dict;

-- D. TABLES ---------------------------------------------------------------------
DROP TABLE IF EXISTS sonyliv_concurrency.events_incoming;
DROP TABLE IF EXISTS sonyliv_concurrency.events_raw;
DROP TABLE IF EXISTS sonyliv_concurrency.content_dim;
DROP TABLE IF EXISTS sonyliv_concurrency.session_intervals;
DROP TABLE IF EXISTS sonyliv_concurrency.concurrency_cold_abs;
DROP TABLE IF EXISTS sonyliv_concurrency.concurrency_hot_abs;
DROP TABLE IF EXISTS sonyliv_concurrency.concurrency_ext_abs;
DROP TABLE IF EXISTS sonyliv_concurrency.concurrency_sa_abs;   -- session-aware comparison table
DROP TABLE IF EXISTS sonyliv_concurrency.concurrency_si_abs;   -- session-independent comparison table

-- E. CONFIG UDFs (server-wide; recreated by 00_config.sql) -------------------------
-- Dependent function first (it references the two knob functions).
DROP FUNCTION IF EXISTS cfg_gap_timeout_seconds;
DROP FUNCTION IF EXISTS cfg_bucket_seconds;
DROP FUNCTION IF EXISTS cfg_heartbeat_seconds;
DROP FUNCTION IF EXISTS cfg_missing_heartbeat_buffer_seconds;
DROP FUNCTION IF EXISTS cfg_hot_window_seconds;
DROP FUNCTION IF EXISTS norm_lang;
DROP FUNCTION IF EXISTS norm_dim;
