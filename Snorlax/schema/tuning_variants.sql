-- [READ — serve] not a pipeline step; offline knob-sweep exploration (run explicitly).
-- #####################################################################
-- tuning_variants.sql — offline knob-sweep for the three unhedged

-- This does NOT change production defaults in 01_schema.sql/03_backfill.sql
-- — run it once the real benchmark answers are available, compare each of
-- the 8 combinations against them, and only then hand-pin the winning
-- combo back into the production files.
--
-- Usage (repeat for all 8 combinations of 0/1, 0/60, 90/120):
--   clickhouse client --param_foreground_resumes=1 --param_grace_seconds=60 \
--     --param_gap_seconds=90 --queries-file tuning_variants.sql > variant_1_60_90.tsv
-- #####################################################################

WITH
  {foreground_resumes:UInt8} AS foreground_resumes,   -- 1 = AppForegrounded resumes playback, 0 = it doesn't
  {grace_seconds:UInt32}     AS grace_seconds,         -- extend the last active event by this many seconds
  {gap_seconds:UInt32}       AS gap_seconds,           -- heartbeat silence beyond this many seconds ends a stretch
  transitions AS (
    SELECT video_session_id AS sid, event_timestamp AS ts, content_id, platform, country,
      multiIf(event_type = 'VideoPlay' OR event IN ('resume','speed-resume','AdResume'), 1,
              foreground_resumes = 1 AND event_type = 'AppForegrounded', 1,
              event_type IN ('AppBackgrounded','VideoSessionEnd','VideoError') OR event IN ('pause','speed-pause','AdPause'), -1,
              0) AS transition
    FROM sonyliv_concurrency.events_raw
  ),
  collapsed AS (
    SELECT sid, ts, if(min(transition) < 0, toInt8(-1), toInt8(max(transition))) AS transition,
           any(content_id) AS content_id, any(platform) AS platform, any(country) AS country
    FROM transitions GROUP BY sid, ts
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
  segments AS (
    SELECT sid, content_id, platform, country, ts AS seg_start,
      multiIf(rn = n, ts + toIntervalSecond(grace_seconds),
              dateDiff('second', ts, next_ts) <= gap_seconds, next_ts,
              ts + toIntervalSecond(grace_seconds)) AS seg_end
    FROM stated WHERE state_sign = 1
  ),
  islands AS (
    SELECT *, if(seg_start > max(seg_end) OVER (PARTITION BY sid ORDER BY seg_start
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 1, 0) AS new_island
    FROM segments
  ),
  per_island AS (
    SELECT sid, island_id, min(seg_start) AS active_start, max(seg_end) AS active_end,
           any(content_id) AS content_id, any(platform) AS platform, any(country) AS country
    FROM (SELECT *, sum(new_island) OVER (PARTITION BY sid ORDER BY seg_start
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id FROM islands)
    GROUP BY sid, island_id
    HAVING active_end > active_start
  )
SELECT country, platform, cd.video_type AS video_type, cd.category AS category, minute, content_id,
       toUInt32(uniqExact(video_session_id)) AS concurrent
FROM (
  SELECT video_session_id, country, platform, content_id,
         toStartOfMinute(active_start) + INTERVAL number MINUTE AS minute
  FROM per_island
  ARRAY JOIN range(0, toUInt64(dateDiff('minute',
                 toStartOfMinute(active_start),
                 toStartOfMinute(active_end - INTERVAL 1 MILLISECOND)) + 1)) AS number
) AS x
LEFT JOIN sonyliv_concurrency.content_dim FINAL AS cd USING (content_id)
GROUP BY country, platform, video_type, category, minute, content_id
ORDER BY minute;
