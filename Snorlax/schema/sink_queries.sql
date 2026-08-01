-- #####################################################################
-- sink_queries.sql — analytics that read the PHYSICAL sink tables
-- directly, bypassing the concurrency_now view / MVs.
--
-- Serving = concurrency_cold_abs (durable, SharedReplacingMergeTree ->
-- ALWAYS FINAL to dedup the APPENDed compaction output) UNION the live
-- tail of concurrency_hot_abs (SharedMergeTree, whole-table replace on
-- each refresh -> NO FINAL). See "reusable serving pattern" below.
--
-- Rule of thumb: use `concurrency_cold_abs FINAL` for anything
-- historical (all closed buckets land in cold); add the cold UNION hot
-- tail only when the last ~10 live minutes matter (query 1).
--
-- Measures: concurrent = distinct SESSIONS, concurrent_users = distinct
-- USERS. Both exact per cell. Summing concurrent across content is exact
-- (session is 1:1 with content); summing concurrent_users across dims is
-- an UPPER BOUND (a user may appear on >1 content). ext/sa/si are plain
-- SharedMergeTree -> no FINAL.
-- #####################################################################

-- =====================================================================
-- Reusable serving pattern (what concurrency_now does, inlined)
-- =====================================================================
SELECT * FROM sonyliv_concurrency.concurrency_cold_abs FINAL
UNION ALL
SELECT * FROM sonyliv_concurrency.concurrency_hot_abs
WHERE minute > coalesce((SELECT max(minute) FROM sonyliv_concurrency.concurrency_cold_abs), toDateTime(0));

-- =====================================================================
-- 1) Latest snapshot — top live content right now (cold UNION hot tail)
-- =====================================================================
WITH serving AS (
    SELECT content_id, minute, concurrent, concurrent_users
    FROM sonyliv_concurrency.concurrency_cold_abs FINAL
    UNION ALL
    SELECT content_id, minute, concurrent, concurrent_users
    FROM sonyliv_concurrency.concurrency_hot_abs
    WHERE minute > coalesce((SELECT max(minute) FROM sonyliv_concurrency.concurrency_cold_abs), toDateTime(0))
)
SELECT content_id,
       dictGet('sonyliv_concurrency.content_dict', 'title', content_id) AS title,
       sum(concurrent)       AS sessions,
       sum(concurrent_users) AS users
FROM serving
WHERE minute = (SELECT max(minute) FROM serving)
GROUP BY content_id
ORDER BY sessions DESC
LIMIT 20;

-- =====================================================================
-- 2) Per-content leaderboard — all-time peak
-- =====================================================================
SELECT content_id,
       dictGet('sonyliv_concurrency.content_dict', 'title', content_id) AS title,
       max(concurrent)            AS peak_sessions,
       max(concurrent_users)      AS peak_users,
       argMax(minute, concurrent) AS peak_minute
FROM sonyliv_concurrency.concurrency_cold_abs FINAL
GROUP BY content_id
ORDER BY peak_sessions DESC
LIMIT 25;

-- =====================================================================
-- 3) Concurrency time series for one title
-- =====================================================================
SELECT minute,
       sum(concurrent)       AS sessions,
       sum(concurrent_users) AS users
FROM sonyliv_concurrency.concurrency_cold_abs FINAL
WHERE content_id = 2078157818          -- swap in your content_id
GROUP BY minute
ORDER BY minute;

-- =====================================================================
-- 4) Platform-wide total concurrency over time
--    concurrent: exact.  concurrent_users: upper bound (see header).
-- =====================================================================
SELECT minute,
       sum(concurrent)       AS total_sessions,
       sum(concurrent_users) AS total_users_upper_bound
FROM sonyliv_concurrency.concurrency_cold_abs FINAL
GROUP BY minute
ORDER BY minute;

-- =====================================================================
-- 5) Peak by dimension — swap platform for country/category/video_type
-- =====================================================================
SELECT platform,
       max(concurrent)            AS peak_sessions,
       argMax(minute, concurrent) AS peak_minute
FROM sonyliv_concurrency.concurrency_cold_abs FINAL
GROUP BY platform
ORDER BY peak_sessions DESC;

-- =====================================================================
-- 6) Filtered slice — e.g. live sports on iPhone
-- =====================================================================
SELECT minute, sum(concurrent) AS sessions
FROM sonyliv_concurrency.concurrency_cold_abs FINAL
WHERE video_type = 'live'
  AND category   = 'sports'
  AND platform   = 'IPHONE'
GROUP BY minute
ORDER BY minute;

-- =====================================================================
-- 7) Extended drill-down (audio / subtitle / app / player version)
--    concurrency_ext_abs is SharedMergeTree -> no FINAL.
-- =====================================================================
SELECT audio_language, subtitle_language, app_version, player_version,
       max(concurrent) AS peak_sessions
FROM sonyliv_concurrency.concurrency_ext_abs
WHERE content_id = 2078157818
GROUP BY audio_language, subtitle_language, app_version, player_version
ORDER BY peak_sessions DESC
LIMIT 20;

-- =====================================================================
-- 8) Parity check — session-aware vs session-independent sink tables.
--    Expect 0 rows: the two approaches must agree per (dims, minute).
-- =====================================================================
SELECT sa.country, sa.platform, sa.video_type, sa.category, sa.minute, sa.content_id,
       sa.concurrent AS sa_c, si.concurrent AS si_c
FROM sonyliv_concurrency.concurrency_sa_abs sa
FULL JOIN sonyliv_concurrency.concurrency_si_abs si
  USING (country, platform, video_type, category, minute, content_id)
WHERE sa.concurrent != si.concurrent
   OR sa.concurrent IS NULL
   OR si.concurrent IS NULL
LIMIT 100;

-- =====================================================================
-- 9) Platform peak-concurrency day-by-day
-- =====================================================================
SELECT toDate(minute) AS day,
       platform,
       max(concurrent) AS peak_sessions
FROM sonyliv_concurrency.concurrency_cold_abs FINAL
GROUP BY day, platform
ORDER BY day, peak_sessions DESC;

-- =====================================================================
-- 10) Global peak minute (single busiest moment)
-- =====================================================================
SELECT minute, sum(concurrent) AS total_sessions
FROM sonyliv_concurrency.concurrency_cold_abs FINAL
GROUP BY minute
ORDER BY total_sessions DESC
LIMIT 1;
