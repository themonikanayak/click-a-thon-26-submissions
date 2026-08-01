-- [WRITE — setup] step 00 of the offline pipeline. Tunable knobs (SQL UDFs); run FIRST, re-run after any change.
-- #####################################################################
-- 00_config.sql — TUNABLE KNOBS, one place. Run this ONCE before the build
-- files (01_schema.sql / 03_backfill.sql / approach_*.sql / 06_verify.sql),
-- and re-run it whenever you change a value, then re-run the build.
--
--   clickhouse client --host <h> --user default --secure --queries-file 00_config.sql
--
-- WHY SQL UDFs (not a config table): these knobs are used inside
-- toStartOfInterval(...) and INTERVAL expressions, which ClickHouse requires
-- to be CONSTANT. A UDF body is inlined at query-analysis time, so
-- `cfg_bucket_seconds()` folds to the literal `60` and satisfies that
-- constant requirement. A scalar subquery `(SELECT ... FROM config)` would
-- NOT fold to a constant and would fail there. UDFs are server-wide objects
-- (not per-database), hence the `cfg_` prefix to avoid collisions.
-- #####################################################################

-- Drop dependents BEFORE leaves (a function referenced by another can't be
-- dropped first), then recreate leaves BEFORE the derived one.
DROP FUNCTION IF EXISTS cfg_gap_timeout_seconds;
DROP FUNCTION IF EXISTS cfg_bucket_seconds;
DROP FUNCTION IF EXISTS cfg_heartbeat_seconds;
DROP FUNCTION IF EXISTS cfg_missing_heartbeat_buffer_seconds;
DROP FUNCTION IF EXISTS cfg_hot_window_seconds;
DROP FUNCTION IF EXISTS norm_lang;
DROP FUNCTION IF EXISTS norm_dim;

-- ---------------------------------------------------------------------
-- KNOB 1 — TIME BUCKET GRANULARITY  (was hardcoded 1 minute)
-- The width, in seconds, of one concurrency bucket. Every place that used
-- toStartOfMinute() + "per minute" now uses toStartOfInterval(.., this).
--   60   = 1-minute buckets (default, matches the original design)
--   300  = 5-minute buckets   ·  30 = 30-second buckets  ·  3600 = hourly
-- NOTE: the serving column is still named `minute`; with a non-60 value it
-- holds the start of a bucket of this many seconds. Pick a divisor/multiple
-- of your reporting grain so hour/day roll-ups stay clean.
-- ---------------------------------------------------------------------
CREATE FUNCTION cfg_bucket_seconds AS () -> 60;

-- ---------------------------------------------------------------------
-- KNOB 2 — HEARTBEAT CADENCE
-- Expected seconds between heartbeats while actively watching. Used as the
-- grace tail added after the last/only active event so a lone heartbeat
-- still occupies its bucket (seg_end = last_event + this).
-- ---------------------------------------------------------------------
CREATE FUNCTION cfg_heartbeat_seconds AS () -> 60;

-- ---------------------------------------------------------------------
-- KNOB 3 — MISSING-HEARTBEAT BUFFER  (the "instead of 90s" knob)
-- Extra seconds tolerated ON TOP of the heartbeat cadence before a silence
-- is treated as the viewer going inactive. This is the slack for dropped or
-- late heartbeats: raise it to bridge more missing beats, lower it to cut
-- inactive time more aggressively.
--   30 = tolerate ~half a missed beat of slack (default; 60 + 30 = old 90s)
--   90 = tolerate roughly one whole extra missed heartbeat
-- ---------------------------------------------------------------------
CREATE FUNCTION cfg_missing_heartbeat_buffer_seconds AS () -> 30;

-- ---------------------------------------------------------------------
-- DERIVED — GAP TIMEOUT  (was hardcoded 90s)
-- Max silence, in seconds, bridged between two consecutive active events.
-- A gap LONGER than this splits the active stretch (viewer inactive between).
-- = heartbeat cadence + missing-heartbeat buffer, so tuning either knob above
-- moves this automatically. Default 60 + 30 = 90 (preserves prior behavior).
-- ---------------------------------------------------------------------
CREATE FUNCTION cfg_gap_timeout_seconds AS () -> cfg_heartbeat_seconds() + cfg_missing_heartbeat_buffer_seconds();

-- ---------------------------------------------------------------------
-- KNOB 4 — HOT→COLD ROLLOVER WINDOW  (was hardcoded 10 min literals)
-- How many seconds of the most-recent buckets stay in the HOT tier
-- (mutable, REPLACE-recomputed each cycle) before they roll over into the
-- frozen COLD tier. A bucket rolls to cold once it is older than
-- now() - this many seconds; the serving view (concurrency_now) reads hot
-- only for minutes newer than max(cold minute), so the tiers stay disjoint.
--   600  = 10 minutes (default; matches the original hardcoded window)
--   Wider  = more correctness slack for late/out-of-order data (bigger hot
--            recompute); Narrower = faster hot reads, less late-data tolerance.
-- Size to p99 heartbeat/ingest lag (measure via ClickStack). This IS UDF-able
-- because it sits inside toIntervalSecond(...) (a constant-folding UDF), unlike
-- the REFRESH cadence which must stay a literal (see below). The derivation
-- lookback (01_schema.sql D2, INTERVAL 20 MINUTE) MUST stay larger than this.
-- ---------------------------------------------------------------------
CREATE FUNCTION cfg_hot_window_seconds AS () -> 600;

-- =====================================================================
-- DIMENSION NORMALIZATION  (applied at INGEST — the edge, per PLAN §9)
-- The raw data has case/format-split dimension values that would fragment
-- filters if left as-is (verified on ch-hackathon-raw-data.csv):
--   audio_language: hin / HIN / hin-hindi (same lang, 3 strings); unk / UNK
--   subtitle_language: UNK / unk / UND; off / OFF; NON / non; '' (empty)
--   app_version / player_version: some '' (empty)
-- Normalizing here (in mv_incoming_to_raw + the batch load) keeps events_raw
-- canonical so every downstream filter is consistent. Only the four EXTENDED
-- dims are normalized; the locked core content dims (video_type/category)
-- are left as-is to avoid diverging from the benchmark's ground-truth keys.
-- If the benchmark expects raw values, relax these two functions in one place.
-- =====================================================================

-- Language dims: lowercase + trim, collapse a "hin-hindi"-style suffix to its
-- prefix, and fold empty / und / unknown into a single 'unk' bucket.
CREATE FUNCTION norm_lang AS (x) ->
    multiIf(x = '' OR lower(trim(x)) IN ('und', 'unknown', 'unk'), 'unk',
            splitByChar('-', lower(trim(x)))[1]);

-- Generic dim: fold empty into 'unk', otherwise keep the value verbatim
-- (version strings are meaningful; only the blanks need a home).
CREATE FUNCTION norm_dim AS (x) -> if(trim(x) = '', 'unk', x);

-- ---------------------------------------------------------------------
-- Sanity: echo the resolved values + a couple of normalization examples.
-- ---------------------------------------------------------------------
SELECT cfg_bucket_seconds()                    AS bucket_seconds,
       cfg_heartbeat_seconds()                 AS heartbeat_seconds,
       cfg_missing_heartbeat_buffer_seconds()  AS missing_heartbeat_buffer_seconds,
       cfg_gap_timeout_seconds()               AS gap_timeout_seconds,
       cfg_hot_window_seconds()                AS hot_window_seconds,
       norm_lang('HIN')                        AS ex_hin,          -- -> hin
       norm_lang('hin-hindi')                  AS ex_hin_hindi,    -- -> hin
       norm_lang('')                           AS ex_empty_lang,   -- -> unk
       norm_dim('')                            AS ex_empty_dim;    -- -> unk

-- =====================================================================
-- OTHER TUNING KNOBS (NOT UDF-able — documented here, set where they live)
--   These sit in `REFRESH EVERY` / `INTERVAL` literals that ClickHouse will
--   not accept a function for, so change them at the source file:
--   * Derivation lookback (recently-active sessions) ... 01_schema.sql D2
--       `INTERVAL 20 MINUTE`  — must exceed cfg_hot_window_seconds().
--   * Refresh cadence ........................... 01_schema.sql D2/D3
--       `REFRESH EVERY 1 MINUTE` / `EVERY 30 SECOND`.
--   * events_raw retention ...................... 01_schema.sql
--       `TTL ... + INTERVAL 30 DAY`.
--   (The HOT→COLD rollover window is now the UDF cfg_hot_window_seconds() above —
--    it lives inside toIntervalSecond(...), so it folds to a constant.)
-- =====================================================================
