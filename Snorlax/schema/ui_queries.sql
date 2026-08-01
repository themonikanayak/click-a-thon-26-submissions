-- #####################################################################
-- ui_queries.sql — live insight / dashboard queries.
-- Concurrency reads the absolute serving view: filter -> sum -> max/avg.
-- ALL params are lenient strings; EMPTY '' = "all":
--   from/to '' -> full data range · dim filters '' -> all · content_id '' -> all
-- Pass every param (missing = error). Run one query at a time via --query,
-- or the whole file with --queries-file (same params applied to each).
-- #####################################################################

-- =====================================================================
-- 0) FILTER DROPDOWNS + time bounds (no params)
-- =====================================================================
SELECT DISTINCT platform   FROM sonyliv_concurrency.concurrency_now ORDER BY platform;
SELECT DISTINCT country    FROM sonyliv_concurrency.concurrency_now ORDER BY country;
SELECT DISTINCT video_type FROM sonyliv_concurrency.concurrency_now ORDER BY video_type;
SELECT DISTINCT category   FROM sonyliv_concurrency.concurrency_now ORDER BY category;
SELECT content_id, title FROM sonyliv_concurrency.content_dim ORDER BY title LIMIT 1000;
SELECT min(minute) AS min_ts, max(minute) AS max_ts FROM sonyliv_concurrency.concurrency_now;

-- =====================================================================
-- 1) CONCURRENCY CURVE (minute grain, filtered)
-- =====================================================================
WITH
  coalesce(parseDateTimeBestEffortOrNull({from:String},'UTC'), (SELECT min(minute) FROM sonyliv_concurrency.concurrency_now)) AS from_ts,
  coalesce(parseDateTimeBestEffortOrNull({to:String},'UTC'),   (SELECT max(minute) FROM sonyliv_concurrency.concurrency_now)) AS to_ts
SELECT minute, sum(concurrent) AS concurrency
FROM sonyliv_concurrency.concurrency_now
WHERE minute BETWEEN from_ts AND to_ts
  AND (platform  = {platform:String}   OR {platform:String}   = '')
  AND (country   = {country:String}     OR {country:String}    = '')
  AND (video_type= {video_type:String}  OR {video_type:String} = '')
  AND (category  = {category:String}    OR {category:String}   = '')
  AND (content_id= toUInt64OrZero({content_id:String}) OR toUInt64OrZero({content_id:String}) = 0)
GROUP BY minute ORDER BY minute;

-- =====================================================================
-- 2) KPI TILES — peak / avg / current concurrency (avg over full range)
-- =====================================================================
WITH
  coalesce(parseDateTimeBestEffortOrNull({from:String},'UTC'), (SELECT min(minute) FROM sonyliv_concurrency.concurrency_now)) AS from_ts,
  coalesce(parseDateTimeBestEffortOrNull({to:String},'UTC'),   (SELECT max(minute) FROM sonyliv_concurrency.concurrency_now)) AS to_ts,
  curve AS (
    SELECT minute, sum(concurrent) AS c
    FROM sonyliv_concurrency.concurrency_now
    WHERE minute BETWEEN from_ts AND to_ts
      AND (platform = {platform:String} OR {platform:String} = '')
    GROUP BY minute
  )
SELECT max(c) AS peak_concurrency, argMax(minute, c) AS peak_minute,
       -- avg = sum / (#buckets in range), empty buckets counted as 0. Denominator
       -- is buckets (config.sql), not minutes, so it stays correct if the bucket
       -- width changes: dateDiff(seconds)/bucket_seconds + 1.
       round(sum(c) / (dateDiff('second', from_ts, to_ts) / cfg_bucket_seconds() + 1), 1) AS avg_concurrency,
       anyLast(c) AS last_minute_concurrency
FROM curve;

-- KPI tiles — sessions started / ended (from events_raw, dup-safe) ----------
WITH
  coalesce(parseDateTimeBestEffortOrNull({from:String},'UTC'), (SELECT min(event_timestamp) FROM sonyliv_concurrency.events_raw)) AS from_ts,
  coalesce(parseDateTimeBestEffortOrNull({to:String},'UTC'),   (SELECT max(event_timestamp) FROM sonyliv_concurrency.events_raw)) AS to_ts
SELECT
  uniqExactIf(video_session_id, event_type = 'VideoSessionStart') AS sessions_started,
  uniqExactIf(video_session_id, event_type = 'VideoSessionEnd')   AS sessions_ended
FROM sonyliv_concurrency.events_raw
WHERE event_timestamp BETWEEN from_ts AND to_ts
  AND (platform = {platform:String} OR {platform:String} = '');

-- new users in the range (users whose FIRST-EVER event falls in the window)
WITH
  coalesce(parseDateTimeBestEffortOrNull({from:String},'UTC'), (SELECT min(event_timestamp) FROM sonyliv_concurrency.events_raw)) AS from_ts,
  coalesce(parseDateTimeBestEffortOrNull({to:String},'UTC'),   (SELECT max(event_timestamp) FROM sonyliv_concurrency.events_raw)) AS to_ts
SELECT count() AS new_users
FROM ( SELECT user_id, min(event_timestamp) AS first_ts
       FROM sonyliv_concurrency.events_raw GROUP BY user_id )
WHERE first_ts BETWEEN from_ts AND to_ts;

-- =====================================================================
-- 3) NEW USERS vs DROPS per minute (arrivals / departures)
-- =====================================================================
WITH
  coalesce(parseDateTimeBestEffortOrNull({from:String},'UTC'), (SELECT min(event_timestamp) FROM sonyliv_concurrency.events_raw)) AS from_ts,
  coalesce(parseDateTimeBestEffortOrNull({to:String},'UTC'),   (SELECT max(event_timestamp) FROM sonyliv_concurrency.events_raw)) AS to_ts
SELECT minute,
       uniqExactIf(video_session_id, event_type='VideoSessionStart') AS sessions_started,
       uniqExactIf(video_session_id, event_type='VideoSessionEnd')   AS sessions_ended
FROM (
  SELECT toStartOfInterval(event_timestamp, toIntervalSecond(cfg_bucket_seconds())) AS minute, video_session_id, event_type
  FROM sonyliv_concurrency.events_raw
  WHERE event_timestamp BETWEEN from_ts AND to_ts
    AND (platform = {platform:String} OR {platform:String} = '')
)
GROUP BY minute ORDER BY minute;

-- =====================================================================
-- 4) DIMENSION BREAKDOWN — peak per dimension (own peak minute)
-- =====================================================================
WITH
  coalesce(parseDateTimeBestEffortOrNull({from:String},'UTC'), (SELECT min(minute) FROM sonyliv_concurrency.concurrency_now)) AS from_ts,
  coalesce(parseDateTimeBestEffortOrNull({to:String},'UTC'),   (SELECT max(minute) FROM sonyliv_concurrency.concurrency_now)) AS to_ts
SELECT platform, max(c) AS peak, argMax(minute, c) AS peak_minute
FROM (
  SELECT platform, minute, sum(concurrent) AS c
  FROM sonyliv_concurrency.concurrency_now
  WHERE minute BETWEEN from_ts AND to_ts
  GROUP BY platform, minute
)
GROUP BY platform ORDER BY peak DESC;

-- Top content by peak concurrency (title via dictionary)
WITH
  coalesce(parseDateTimeBestEffortOrNull({from:String},'UTC'), (SELECT min(minute) FROM sonyliv_concurrency.concurrency_now)) AS from_ts,
  coalesce(parseDateTimeBestEffortOrNull({to:String},'UTC'),   (SELECT max(minute) FROM sonyliv_concurrency.concurrency_now)) AS to_ts
SELECT content_id,
       dictGetOrDefault('sonyliv_concurrency.content_dict','title', content_id, concat('Unknown (', toString(content_id), ')')) AS title,
       max(c) AS peak, argMax(minute, c) AS peak_minute
FROM (
  SELECT content_id, minute, sum(concurrent) AS c
  FROM sonyliv_concurrency.concurrency_now
  WHERE minute BETWEEN from_ts AND to_ts
  GROUP BY content_id, minute
)
GROUP BY content_id ORDER BY peak DESC LIMIT 20;

-- =====================================================================
-- 5) HOUR / DAY grain — from minute concurrency
-- =====================================================================
WITH
  coalesce(parseDateTimeBestEffortOrNull({from:String},'UTC'), (SELECT min(minute) FROM sonyliv_concurrency.concurrency_now)) AS from_ts,
  coalesce(parseDateTimeBestEffortOrNull({to:String},'UTC'),   (SELECT max(minute) FROM sonyliv_concurrency.concurrency_now)) AS to_ts,
  curve AS (
    SELECT minute, sum(concurrent) AS c
    FROM sonyliv_concurrency.concurrency_now
    WHERE minute BETWEEN from_ts AND to_ts
    GROUP BY minute
  )
SELECT toStartOfHour(minute) AS hour,        -- toStartOfDay for day grain
       max(c) AS peak_concurrency, round(avg(c),1) AS avg_concurrency
FROM curve GROUP BY hour ORDER BY hour;

-- =====================================================================
-- 6) LATENCY / "what it reads" — evidence for the latency badge & judges
-- =====================================================================
SELECT query_duration_ms, read_rows, formatReadableSize(read_bytes) AS read_bytes
FROM system.query_log
WHERE type='QueryFinish'
  AND hasAny(tables, ['sonyliv_concurrency.concurrency_cold_abs','sonyliv_concurrency.concurrency_hot_abs',
                      'sonyliv_concurrency.concurrency_ext_abs'])
ORDER BY event_time DESC LIMIT 5;

-- =====================================================================
-- 7) EXTENDED DRILL-DOWN — curve + peak/avg filtered on ANY dim, including the
--    drill-down dims (app_version, audio_language, subtitle_language,
--    player_version). Reads concurrency_ext_abs (approach_extended_dims.sql),
--    NOT the lean core tiers. EMPTY '' = "all" for every filter. Language values
--    are normalized (config.sql), so pass e.g. 'hin' (not 'HIN'/'hin-hindi').
-- =====================================================================
WITH
  coalesce(parseDateTimeBestEffortOrNull({from:String},'UTC'), (SELECT min(minute) FROM sonyliv_concurrency.concurrency_ext_abs)) AS from_ts,
  coalesce(parseDateTimeBestEffortOrNull({to:String},'UTC'),   (SELECT max(minute) FROM sonyliv_concurrency.concurrency_ext_abs)) AS to_ts,
  curve AS (
    SELECT minute, sum(concurrent) AS c
    FROM sonyliv_concurrency.concurrency_ext_abs
    WHERE minute BETWEEN from_ts AND to_ts
      AND (platform          = {platform:String}          OR {platform:String}          = '')
      AND (country           = {country:String}           OR {country:String}           = '')
      AND (video_type        = {video_type:String}        OR {video_type:String}        = '')
      AND (category          = {category:String}          OR {category:String}          = '')
      AND (app_version       = {app_version:String}       OR {app_version:String}       = '')
      AND (audio_language    = {audio_language:String}    OR {audio_language:String}    = '')
      AND (subtitle_language = {subtitle_language:String} OR {subtitle_language:String} = '')
      AND (player_version    = {player_version:String}    OR {player_version:String}    = '')
      AND (content_id = toUInt64OrZero({content_id:String}) OR toUInt64OrZero({content_id:String}) = 0)
    GROUP BY minute
  )
SELECT max(c) AS peak_concurrency, argMax(minute, c) AS peak_minute,
       -- avg over #buckets in range, empty buckets = 0 (bucket width from config.sql)
       round(sum(c) / (dateDiff('second', from_ts, to_ts) / cfg_bucket_seconds() + 1), 1) AS avg_concurrency
FROM curve;

-- Distinct drill-down dim values (dropdowns for the extended filters)
SELECT DISTINCT app_version       FROM sonyliv_concurrency.concurrency_ext_abs ORDER BY app_version;
SELECT DISTINCT audio_language    FROM sonyliv_concurrency.concurrency_ext_abs ORDER BY audio_language;
SELECT DISTINCT subtitle_language FROM sonyliv_concurrency.concurrency_ext_abs ORDER BY subtitle_language;
SELECT DISTINCT player_version    FROM sonyliv_concurrency.concurrency_ext_abs ORDER BY player_version;
