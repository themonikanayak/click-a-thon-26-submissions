-- #####################################################################
-- 001_foreground_state_fixes.sql — two corrections to the foreground-only
-- active definition, applied to a live mv_session_intervals. Both are already
-- baked into 01_schema.sql D2 / 03_backfill.sql / 04_approaches.sql
-- (so --reset --build / --all pick them up); this migration applies them to an
-- ALREADY-deployed live MV. Idempotent: DROP VIEW IF EXISTS + CREATE.
--
-- (1) PAUSE is an explicit deactivation (GAP_ANALYSIS #2). Pause has no coarse
--     event_type in the SonyLIV feed — it rides in the `event` column ("the actual
--     event", dataset_details.md): pause / speed-pause / AdPause. Heartbeats are
--     neutral and never reset a deactivated state, so a paused-but-still-heartbeating
--     session is excluded (the gap rule alone would miss it). We also match the
--     VideoPause / AdBreakStart pause-family event_types directly, so a pause is
--     caught by event_type OR event even if it arrives with a blank/unknown `event`.
--     (Seek/buffering = `event`='speed-pause' is the separate buffering toggle, §9.)
--
-- (2) VideoSessionStart SEEDS the session as ACTIVE (GAP_ANALYSIS #2a). The state
--     machine used to seed every session inactive until the first explicit +1 event,
--     silently dropping (a) sessions whose first state-changing event is a
--     deactivation — with active heartbeats before it — to zero, and (b) the active
--     heartbeats before a late VideoPlay. A session is now active from its start
--     until a pause/bg/error/end stops it. Trade-off pinned to the benchmark key
--     (like the buffering toggle): a session that starts but never truly plays counts
--     ~1 minute (the [start, first-play) window).
--
-- ⚠ KEEP IN SYNC: this is a verbatim copy of 01_schema.sql section D2
-- (mv_session_intervals). An MV can only be replaced wholesale, so if D2 changes
-- (e.g. the ongoing user-level-concurrency columns user_id / title), regenerate
-- this body from D2. The next scheduled refresh repopulates session_intervals; no
-- data is dropped or double-counted.
-- #####################################################################

DROP VIEW IF EXISTS sonyliv_concurrency.mv_session_intervals;
CREATE MATERIALIZED VIEW sonyliv_concurrency.mv_session_intervals
REFRESH EVERY 30 SECOND TO sonyliv_concurrency.session_intervals EMPTY AS
-- Column order MUST match session_intervals (positional MV insert):
-- video_session_id, user_id, interval_idx, active_start, active_end, is_provisional,
-- content_id, platform, country, video_type, category, title, version.
SELECT video_session_id, user_id, interval_idx, active_start, active_end,
       toUInt8(active_end >= now() - toIntervalSecond(cfg_gap_timeout_seconds())) AS is_provisional,
       content_id, platform, country,
       dictGet('sonyliv_concurrency.content_dict','video_type', content_id) AS video_type,
       dictGet('sonyliv_concurrency.content_dict','category',   content_id) AS category,
       dictGet('sonyliv_concurrency.content_dict','title',      content_id) AS title,
       toUnixTimestamp64Milli(now64(3)) AS version
FROM
(
  WITH
  recent AS (
    SELECT video_session_id FROM sonyliv_concurrency.events_raw
    GROUP BY video_session_id HAVING max(event_timestamp) >= now() - INTERVAL 20 MINUTE ),
  per_event AS (
    SELECT video_session_id AS sid, user_id, event_timestamp AS ts, content_id, platform, country,
      -- ACTIVATE (foreground-only). VideoSessionStart SEEDS the session as active from the start — a
      -- session is watching until a pause/bg/error/end stops it — so active heartbeats BEFORE the first
      -- explicit VideoPlay aren't dropped, and a session that never emits an explicit Play still counts.
      multiIf(event_type IN ('VideoSessionStart','VideoPlay','AppForegrounded') OR event IN ('resume','speed-resume','AdResume'), 1,
              -- DEACTIVATE (foreground-only). PAUSE has no coarse event_type in the raw feed — it rides in
              -- the `event` column ("the actual event", dataset_details.md: pause/speed-pause/AdPause). We
              -- also match the VideoPause/AdBreakStart pause-family event_types directly, so a pause is caught by
              -- event_type OR event and a paused-but-heartbeating session can't leak in as active (GAP #2).
              event_type IN ('AppBackgrounded','VideoSessionEnd','VideoError','VideoPause','AdBreakStart') OR event IN ('pause','speed-pause','AdPause'), -1,
              0) AS transition
    FROM sonyliv_concurrency.events_raw
    WHERE video_session_id IN (SELECT video_session_id FROM recent) ),
  collapsed AS (
    SELECT sid, ts, if(min(transition) < 0, toInt8(-1), toInt8(max(transition))) AS transition,
           any(user_id) AS user_id, any(content_id) AS content_id, any(platform) AS platform, any(country) AS country
    FROM per_event GROUP BY sid, ts ),
  stated AS (
    SELECT sid, ts, user_id, content_id, platform, country,
      argMax(transition, if(transition!=0, ts, toDateTime64('1970-01-01 00:00:00',3,'UTC')))
        OVER (PARTITION BY sid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS state_sign,
      row_number() OVER (PARTITION BY sid ORDER BY ts) AS rn,
      count()      OVER (PARTITION BY sid)             AS n,
      leadInFrame(ts) OVER (PARTITION BY sid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS next_ts
    FROM collapsed ),
  segments AS (
    SELECT sid, user_id, content_id, platform, country, ts AS seg_start,
      multiIf(rn=n, addSeconds(ts, cfg_heartbeat_seconds()), dateDiff('second', ts, next_ts) <= cfg_gap_timeout_seconds(), next_ts, addSeconds(ts, cfg_heartbeat_seconds())) AS seg_end
    FROM stated WHERE state_sign = 1 ),
  islands AS (
    SELECT *, if(seg_start > max(seg_end) OVER (PARTITION BY sid ORDER BY seg_start
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 1, 0) AS new_island
    FROM segments )
  SELECT sid AS video_session_id, any(user_id) AS user_id, toUInt16(island_id) AS interval_idx,
         min(seg_start) AS active_start, max(seg_end) AS active_end,
         any(content_id) AS content_id, any(platform) AS platform, any(country) AS country
  FROM (SELECT *, sum(new_island) OVER (PARTITION BY sid ORDER BY seg_start
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id FROM islands)
  GROUP BY sid, island_id HAVING active_end > active_start
);
