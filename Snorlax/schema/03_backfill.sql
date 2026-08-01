-- [WRITE — build] step 03 of the offline pipeline. One-shot backfill of session_intervals + cold/hot from static data.
-- #####################################################################
-- 03_build.sql — one-shot build (offline test / history backfill).
--   events_raw -> session_intervals (state machine) -> cold_abs + hot_abs
-- Watermark: as_of = max(event_timestamp), HOT_WINDOW = cfg_hot_window_seconds()
--   (00_config.sql, default 600s = 10 min) -> last window HOT, rest COLD (both tiers).
--
-- Review fixes: events collapsed per (session, ms) with deterministic priority
-- (deactivate > reactivate > neutral) — fixes tied-timestamp nondeterminism;
-- content enriched via LEFT JOIN content_dim (no dictionary — a Cloud dictionary
-- reload is node-local, so a stale replica can silently serve wrong dims);
-- session_intervals is one row per session (Array of interval tuples) so a
-- re-derivation with fewer islands than before can't leave stale rows behind.
-- #####################################################################

TRUNCATE TABLE sonyliv_concurrency.session_intervals;
TRUNCATE TABLE sonyliv_concurrency.concurrency_cold_abs;
TRUNCATE TABLE sonyliv_concurrency.concurrency_hot_abs;

-- ---------------------------------------------------------------------
-- STATE MACHINE -> session_intervals (all sessions)
-- ---------------------------------------------------------------------
-- Column order MUST match session_intervals: video_session_id, user_id, intervals,
-- is_provisional, content_id, platform, country, video_type, category, title, version.
-- Content dims (incl. title) via LEFT JOIN content_dim, not dictGet (stale-replica fix).
INSERT INTO sonyliv_concurrency.session_intervals
SELECT sess.video_session_id AS video_session_id,
       sess.user_id AS user_id,
       sess.intervals AS intervals,
       0 AS is_provisional,
       sess.content_id AS content_id, sess.platform AS platform, sess.country AS country,
       cd.video_type AS video_type, cd.category AS category, cd.title AS title,
       toUnixTimestamp64Milli(now64(3)) AS version
FROM
(
  WITH
  per_event AS (
    SELECT video_session_id AS sid, user_id, event_timestamp AS ts, content_id, platform, country,
      -- ACTIVATE (foreground-only). VideoSessionStart SEEDS the session as active from the start — a
      -- session is watching until a pause/bg/error/end stops it — so active heartbeats BEFORE the first
      -- explicit VideoPlay aren't dropped, and a session that never emits an explicit Play still counts.
      multiIf(event_type IN ('VideoSessionStart','VideoPlay','AppForegrounded') OR event IN ('resume','speed-resume','AdResume'),  1,
              -- DEACTIVATE (foreground-only). PAUSE has no coarse event_type in the raw feed — it rides in
              -- the `event` column ("the actual event", dataset_details.md: pause/speed-pause/AdPause). We
              -- also match the VideoPause/AdBreakStart pause-family event_types directly, so a pause is caught by
              -- event_type OR event and a paused-but-heartbeating session can't leak in as active (GAP #2).
              event_type IN ('AppBackgrounded','VideoSessionEnd','VideoError','VideoPause','AdBreakStart') OR event IN ('pause','speed-pause','AdPause'), -1,
              0) AS transition
    FROM sonyliv_concurrency.events_raw
  ),
  collapsed AS (                                    -- one row per (session, ms); deactivate wins
    SELECT sid, ts,
           if(min(transition) < 0, toInt8(-1), toInt8(max(transition))) AS transition,
           any(user_id) AS user_id, any(content_id) AS content_id, any(platform) AS platform, any(country) AS country
    FROM per_event GROUP BY sid, ts
  ),
  stated AS (
    SELECT sid, ts, user_id, content_id, platform, country,
      argMax(transition, if(transition!=0, ts, toDateTime64('1970-01-01 00:00:00',3,'UTC')))
        OVER (PARTITION BY sid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS state_sign,
      row_number() OVER (PARTITION BY sid ORDER BY ts) AS rn,
      count()      OVER (PARTITION BY sid)             AS n,
      leadInFrame(ts) OVER (PARTITION BY sid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS next_ts
    FROM collapsed
  ),
  segments AS (
    SELECT sid, user_id, content_id, platform, country, ts AS seg_start,
      -- grace tail + gap timeout now come from 00_config.sql (was +60s / <=90s):
      multiIf(rn=n, addSeconds(ts, cfg_heartbeat_seconds()),
              dateDiff('second', ts, next_ts) <= cfg_gap_timeout_seconds(), next_ts,
              addSeconds(ts, cfg_heartbeat_seconds())) AS seg_end
    FROM stated WHERE state_sign = 1
  ),
  islands AS (
    SELECT *, if(seg_start > max(seg_end) OVER (PARTITION BY sid ORDER BY seg_start
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 1, 0) AS new_island
    FROM segments
  ),
  per_island AS (
    SELECT sid, island_id, min(seg_start) AS istart, max(seg_end) AS iend,
           any(user_id) AS user_id,
           any(content_id) AS content_id, any(platform) AS platform, any(country) AS country
    FROM (SELECT *, sum(new_island) OVER (PARTITION BY sid ORDER BY seg_start
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id FROM islands)
    GROUP BY sid, island_id
    HAVING iend > istart
  )
  SELECT sid AS video_session_id, any(user_id) AS user_id,
         arraySort(iv -> iv.1, groupArray((istart, iend))) AS intervals,
         any(content_id) AS content_id, any(platform) AS platform, any(country) AS country
  FROM per_island
  GROUP BY sid
) AS sess
LEFT JOIN sonyliv_concurrency.content_dim FINAL AS cd USING (content_id);

-- watermark (HOT_WINDOW = cfg_hot_window_seconds(), 00_config.sql; default 600 = old 10 min)
CREATE TEMPORARY TABLE _wm AS
SELECT toStartOfInterval(max(event_timestamp), toIntervalSecond(cfg_bucket_seconds())) - toIntervalSecond(cfg_hot_window_seconds()) AS wm
FROM sonyliv_concurrency.events_raw;

-- ---------------------------------------------------------------------
-- ABSOLUTE per (dims, minute): expand intervals -> minutes, count distinct
-- sessions (once-per-minute dedupe), split by watermark.
-- ---------------------------------------------------------------------
INSERT INTO sonyliv_concurrency.concurrency_cold_abs
SELECT country, platform, video_type, category, minute, content_id,
       toUInt32(uniqExact(video_session_id)) AS concurrent,
       toUInt32(uniqExact(user_id))          AS concurrent_users
FROM (
  SELECT video_session_id, user_id, country, platform, video_type, category, content_id,
         -- configurable bucket (00_config.sql): start-of-bucket + N buckets
         toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds()))
           + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
  FROM (
    -- unpack session_intervals' one-row-per-session Array(Tuple(...)) first
    -- (ghost-interval fix, review #7).
    SELECT video_session_id, user_id, country, platform, video_type, category, content_id,
           iv.1 AS active_start, iv.2 AS active_end
    FROM sonyliv_concurrency.session_intervals FINAL
    ARRAY JOIN intervals AS iv
    WHERE iv.2 > iv.1
  )
  ARRAY JOIN range(0, toUInt64(dateDiff('second',
                 toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds())),
                 toStartOfInterval(active_end - INTERVAL 1 MILLISECOND, toIntervalSecond(cfg_bucket_seconds())))
                 / cfg_bucket_seconds()) + 1) AS number
)
WHERE minute <= (SELECT wm FROM _wm)
GROUP BY country, platform, video_type, category, minute, content_id;

INSERT INTO sonyliv_concurrency.concurrency_hot_abs
SELECT country, platform, video_type, category, minute, content_id,
       toUInt32(uniqExact(video_session_id)) AS concurrent,
       toUInt32(uniqExact(user_id))          AS concurrent_users
FROM (
  SELECT video_session_id, user_id, country, platform, video_type, category, content_id,
         -- configurable bucket (00_config.sql): start-of-bucket + N buckets
         toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds()))
           + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
  FROM (
    -- unpack session_intervals' one-row-per-session Array(Tuple(...)) first
    -- (ghost-interval fix, review #7).
    SELECT video_session_id, user_id, country, platform, video_type, category, content_id,
           iv.1 AS active_start, iv.2 AS active_end
    FROM sonyliv_concurrency.session_intervals FINAL
    ARRAY JOIN intervals AS iv
    WHERE iv.2 > iv.1
  )
  ARRAY JOIN range(0, toUInt64(dateDiff('second',
                 toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds())),
                 toStartOfInterval(active_end - INTERVAL 1 MILLISECOND, toIntervalSecond(cfg_bucket_seconds())))
                 / cfg_bucket_seconds()) + 1) AS number
)
WHERE minute > (SELECT wm FROM _wm)
GROUP BY country, platform, video_type, category, minute, content_id;

DROP TEMPORARY TABLE _wm;

-- sanity + smoke test
SELECT 'sessions_with_intervals' AS t, count() AS rows FROM sonyliv_concurrency.session_intervals FINAL
UNION ALL SELECT 'total_intervals', sum(length(intervals)) FROM sonyliv_concurrency.session_intervals FINAL
UNION ALL SELECT 'cold_abs', count() FROM sonyliv_concurrency.concurrency_cold_abs
UNION ALL SELECT 'hot_abs',  count() FROM sonyliv_concurrency.concurrency_hot_abs;

SELECT max(c) AS peak_concurrency, argMax(minute, c) AS peak_minute
FROM (SELECT minute, sum(concurrent) AS c FROM sonyliv_concurrency.concurrency_now GROUP BY minute);
