-- #####################################################################
-- approach_session_aware.sql — SESSION-AWARE approach, standalone & comparable.
--
-- WHY this file exists: the problem asks us to compare two approaches to
-- concurrency (session-aware vs session-independent) head-to-head. The
-- real serving path (schema.sql) mixes the session-aware approach with
-- hot/cold TIERING, which is an orthogonal freshness/compaction concern
-- and would pollute a 1:1 diff. This file re-materializes the SAME
-- session-aware numbers into their own full-history table, with no tier
-- split, so it can be joined minute-for-minute against the
-- session-independent approach's table (a separate file) to see exactly
-- where/why the two methods disagree.
--
-- Reads sonyliv_concurrency.session_intervals — assumes backfill_history.sql
-- (the state machine) has already been run so intervals are populated.
-- Does NOT touch events_raw, does NOT reimplement hot/cold tiers.
-- #####################################################################

-- =====================================================================
-- A. TABLE — same shape/engine/order as concurrency_cold_abs (schema.sql),
--    so the two approaches' outputs are byte-for-byte comparable.
-- =====================================================================
TRUNCATE TABLE IF EXISTS sonyliv_concurrency.concurrency_sa_abs;

CREATE TABLE IF NOT EXISTS sonyliv_concurrency.concurrency_sa_abs
( country LowCardinality(String), platform LowCardinality(String), video_type LowCardinality(String),
  category LowCardinality(String), minute DateTime('UTC'), content_id UInt64, concurrent UInt32 )
ENGINE = MergeTree ORDER BY (country, platform, video_type, category, minute, content_id);

-- =====================================================================
-- B. POPULATE — full history, no watermark (this table exists purely for
--    comparison, not for live serving, so there's no hot/cold split here).
-- =====================================================================
INSERT INTO sonyliv_concurrency.concurrency_sa_abs
SELECT country, platform, video_type, category, minute, content_id,
       -- once-per-minute dedupe: a session that is active for part of a
       -- minute (or has >1 truly-active interval touching the same minute)
       -- must still count as ONE concurrent viewer in that minute.
       toUInt32(uniqExact(video_session_id)) AS concurrent
FROM (
  -- Expand each truly-active [active_start, active_end) interval to every
  -- minute it touches. This is the session-aware core: active_end/active_start
  -- already come from the state machine, so paused/backgrounded time is
  -- excluded — a session only "occupies" the minutes it was really watching.
  -- half-open end: subtract 1ms before flooring so an interval ending exactly
  -- on a minute boundary doesn't spuriously claim that next minute.
  SELECT video_session_id, country, platform, video_type, category, content_id,
         -- configurable bucket (config.sql): start-of-bucket + N buckets
         toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds()))
           + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
  FROM sonyliv_concurrency.session_intervals FINAL
  ARRAY JOIN range(0, toUInt64(dateDiff('second',
                 toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds())),
                 toStartOfInterval(active_end - INTERVAL 1 MILLISECOND, toIntervalSecond(cfg_bucket_seconds())))
                 / cfg_bucket_seconds()) + 1) AS number
  WHERE active_end > active_start
)
GROUP BY country, platform, video_type, category, minute, content_id;

-- =====================================================================
-- C. SMOKE TEST
-- =====================================================================
SELECT count() AS rows FROM sonyliv_concurrency.concurrency_sa_abs;

SELECT max(c) AS peak_concurrency, argMax(minute, c) AS peak_minute
FROM (SELECT minute, sum(concurrent) AS c FROM sonyliv_concurrency.concurrency_sa_abs GROUP BY minute);
