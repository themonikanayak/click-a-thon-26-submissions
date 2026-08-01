-- #####################################################################
-- approach_session_independent.sql — SESSION-INDEPENDENT approach,
-- standalone & comparable to approach_session_aware.sql.
--
-- WHY this file exists: same head-to-head comparison as
-- approach_session_aware.sql, but for the SESSION-INDEPENDENT method:
-- derive the SAME foreground-only active definition per event, WITHOUT
-- reconstructing per-session intervals (no island-merge, no interval_idx).
-- This isolates "does interval reconstruction matter for the final count?"
-- as the only difference between the two files — everything upstream of
-- island-merging (the transition state machine, the 90s gap / 60s grace
-- segment rule) is copied verbatim from backfill_history.sql so both
-- approaches share one active definition and can only diverge on the
-- island-merge step itself.
--
-- Reads sonyliv_concurrency.events_raw directly (does NOT read
-- session_intervals — that table is the session-aware path's output).
-- The naive "any heartbeat landed in this minute" rule is REJECTED here
-- (PLAN.md §2/§8): it would count paused/backgrounded minutes as active.
-- This file still computes per-event foreground STATE (via the same
-- state-changing-event carry-forward used by the state machine) — it
-- just stops before compacting consecutive active spans into islands.
-- #####################################################################

TRUNCATE TABLE IF EXISTS sonyliv_concurrency.concurrency_si_abs;

-- =====================================================================
-- A. TABLE — identical shape/engine/order to concurrency_sa_abs /
--    concurrency_cold_abs, so the two approaches are byte-for-byte
--    comparable minute-for-minute.
-- =====================================================================
CREATE TABLE IF NOT EXISTS sonyliv_concurrency.concurrency_si_abs
( country LowCardinality(String), platform LowCardinality(String), video_type LowCardinality(String),
  category LowCardinality(String), minute DateTime('UTC'), content_id UInt64, concurrent UInt32 )
ENGINE = MergeTree ORDER BY (country, platform, video_type, category, minute, content_id);

-- =====================================================================
-- B. POPULATE — same active definition as the state machine
--    (backfill_history.sql), but STOP at `segments` (per-event active
--    stretches) instead of merging them into `islands`/interval_idx.
-- =====================================================================
INSERT INTO sonyliv_concurrency.concurrency_si_abs
SELECT country, platform, video_type, category, minute, content_id,
       -- once-per-minute dedupe (PLAN §2): uniqExact collapses however many
       -- active SEGMENTS a session has touching this minute (e.g. two short
       -- plays either side of a pause) down to ONE concurrent viewer. This
       -- is also *why* skipping island-merge is safe here — see section C.
       toUInt32(uniqExact(video_session_id)) AS concurrent
FROM
(
  WITH
  -- ---- copied verbatim from backfill_history.sql: same active definition ----
  per_event AS (
    SELECT video_session_id AS sid, event_timestamp AS ts, content_id, platform, country,
      multiIf(event_type IN ('VideoPlay','AppForegrounded') OR event IN ('resume','speed-resume','AdResume'),  1,
              event_type IN ('AppBackgrounded','VideoSessionEnd','VideoError') OR event IN ('pause','speed-pause','AdPause'), -1,
              0) AS transition
    FROM sonyliv_concurrency.events_raw
  ),
  collapsed AS (                                    -- one row per (session, ms); deactivate wins
    SELECT sid, ts,
           if(min(transition) < 0, toInt8(-1), toInt8(max(transition))) AS transition,
           any(content_id) AS content_id, any(platform) AS platform, any(country) AS country
    FROM per_event GROUP BY sid, ts
  ),
  stated AS (
    SELECT sid, ts, content_id, platform, country,
      argMax(transition, if(transition!=0, ts, toDateTime64('1970-01-01 00:00:00',3,'UTC')))
        OVER (PARTITION BY sid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS state_sign,
      row_number() OVER (PARTITION BY sid ORDER BY ts) AS rn,
      count()      OVER (PARTITION BY sid)             AS n,
      leadInFrame(ts) OVER (PARTITION BY sid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS next_ts
    FROM collapsed
  ),
  -- `segments` = per-event active stretches (90s gap closes a stretch at
  -- last_seen+60s grace; a gap <=90s bridges straight to the next event),
  -- identical to backfill_history.sql. THIS IS THE STOPPING POINT:
  -- session-aware keeps going from here (builds `islands`, merging
  -- consecutive segments per session into one interval via interval_idx);
  -- session-independent expands straight from `segments` below — no
  -- island/interval_idx construction at all.
  segments AS (
    SELECT sid, content_id, platform, country, ts AS seg_start,
      -- grace tail + gap timeout now come from config.sql (was +60s / <=90s):
      multiIf(rn=n, addSeconds(ts, cfg_heartbeat_seconds()),
              dateDiff('second', ts, next_ts) <= cfg_gap_timeout_seconds(), next_ts,
              addSeconds(ts, cfg_heartbeat_seconds())) AS seg_end
    FROM stated WHERE state_sign = 1
  )
  -- ---- expand each active SEGMENT (not island) straight to minutes ----
  -- Same half-open-end idiom as backfill_history.sql: subtract 1ms before
  -- flooring so a segment ending exactly on a minute boundary doesn't
  -- spuriously claim that next minute.
  SELECT sid AS video_session_id, country, platform,
         dictGet('sonyliv_concurrency.content_dict','video_type', content_id) AS video_type,
         dictGet('sonyliv_concurrency.content_dict','category',   content_id) AS category,
         content_id,
         -- configurable bucket (config.sql): start-of-bucket + N buckets
         toStartOfInterval(seg_start, toIntervalSecond(cfg_bucket_seconds()))
           + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
  FROM segments
  ARRAY JOIN range(0, toUInt64(dateDiff('second',
                 toStartOfInterval(seg_start, toIntervalSecond(cfg_bucket_seconds())),
                 toStartOfInterval(seg_end - INTERVAL 1 MILLISECOND, toIntervalSecond(cfg_bucket_seconds())))
                 / cfg_bucket_seconds()) + 1) AS number
  WHERE seg_end > seg_start
)
GROUP BY country, platform, video_type, category, minute, content_id;

-- =====================================================================
-- C. WHY SESSION-INDEPENDENT == SESSION-AWARE HERE
-- =====================================================================
-- Both paths share ONE active definition (identical per_event/collapsed/
-- stated/segments CTEs, same 90s-gap / 60s-grace rule) and differ only in
-- whether consecutive active segments for a session get compacted into one
-- `island` (session-aware) before counting. Island-merging is a pure
-- COMPACTION of a session's own active time, not a change to WHICH minutes
-- are active: if segment A and segment B both touch minute m for the same
-- session, so does their merged island, and vice versa. Because the
-- per-minute metric is uniqExact(video_session_id) — a set membership test,
-- not a segment count — a session touching minute m via 1 unmerged segment
-- or via N unmerged segments contributes exactly 1, identical to
-- contributing via 1 merged island. So skipping island/interval_idx
-- construction changes nothing about the final counts; it only removes
-- work the session-independent approach doesn't need to do.

-- =====================================================================
-- D. SMOKE TEST
-- =====================================================================
SELECT count() AS rows FROM sonyliv_concurrency.concurrency_si_abs;

SELECT max(c) AS peak_concurrency, argMax(minute, c) AS peak_minute
FROM (SELECT minute, sum(concurrent) AS c FROM sonyliv_concurrency.concurrency_si_abs GROUP BY minute);
