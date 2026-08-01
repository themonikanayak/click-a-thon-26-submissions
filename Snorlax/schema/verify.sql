-- #####################################################################
-- 05_verification.sql — correctness checks.
-- #####################################################################

-- ---------------------------------------------------------------------
-- A) SERVING == BRUTE FORCE. The tiered serving (cold+hot) must equal an
--    independent per-minute explosion from session_intervals (distinct
--    sessions per minute). Expect ZERO mismatched rows.
-- ---------------------------------------------------------------------
WITH
serving AS (
  SELECT minute, sum(concurrent) AS c
  FROM sonyliv_concurrency.concurrency_now GROUP BY minute
),
reference AS (
  SELECT minute, uniqExact(video_session_id) AS c
  FROM (
    SELECT video_session_id,
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
  GROUP BY minute
)
SELECT s.minute, s.c AS serving_c, r.c AS reference_c
FROM serving s FULL JOIN reference r USING (minute)
WHERE s.c != r.c
ORDER BY minute;                 -- <-- zero rows = correct

-- ---------------------------------------------------------------------
-- B) PER-SESSION DEDUPE probe: sessions with >1 active interval in the
--    same minute must still count once (handled by uniqExact). List them.
-- ---------------------------------------------------------------------
SELECT video_session_id, toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds())) AS minute, count() AS intervals_in_minute
FROM sonyliv_concurrency.session_intervals FINAL
GROUP BY video_session_id, minute
HAVING intervals_in_minute > 1
ORDER BY intervals_in_minute DESC LIMIT 20;

-- ---------------------------------------------------------------------
-- C) PAUSE-CORRECTNESS: our foreground-only count vs the NAIVE
--    "heartbeat-present in minute" rule (PLAN2). Naive counts paused
--    time as active -> it should be consistently HIGHER. The gap is the
--    overcount we avoid. (Total + a few worst minutes.)
-- ---------------------------------------------------------------------
WITH
ours AS (
  SELECT minute, sum(concurrent) AS c FROM sonyliv_concurrency.concurrency_now GROUP BY minute
),
naive AS (   -- "active iff any heartbeat in the minute" (ignores pause)
  SELECT toStartOfInterval(event_timestamp, toIntervalSecond(cfg_bucket_seconds())) AS minute,
         uniqExact(video_session_id) AS c
  FROM sonyliv_concurrency.events_raw
  WHERE event_type = 'VideoHeartbeat'
  GROUP BY minute
)
SELECT
  sum(n.c) AS naive_total_session_minutes,
  sum(o.c) AS ours_total_session_minutes,
  sum(n.c) - sum(o.c) AS overcount_avoided,
  round(100.0*(sum(n.c)-sum(o.c))/sum(n.c), 1) AS pct_overcount_avoided
FROM naive n LEFT JOIN ours o USING (minute);

-- ---------------------------------------------------------------------
-- D) Cold/Hot split sanity: tiers must be disjoint by minute.
-- ---------------------------------------------------------------------
SELECT
  (SELECT max(minute) FROM sonyliv_concurrency.concurrency_cold_abs) AS cold_max_minute,
  (SELECT min(minute) FROM sonyliv_concurrency.concurrency_hot_abs)  AS hot_min_minute,
  (SELECT count() FROM (
      SELECT minute FROM sonyliv_concurrency.concurrency_cold_abs
      INTERSECT
      SELECT minute FROM sonyliv_concurrency.concurrency_hot_abs)) AS overlapping_minutes;  -- expect 0
