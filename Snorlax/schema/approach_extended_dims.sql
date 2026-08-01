-- #####################################################################
-- approach_extended_dims.sql — EXTENDED drill-down aggregate.
--
-- WHY this file exists: the core serving key is intentionally lean
-- (country, platform, video_type, category, content_id) so the common-case
-- dashboard stays fast (PLAN §9 Fix #7). But the problem lists more filter
-- dims — app_version, player_version, audio_language, subtitle_language —
-- which the core key can't answer. This builds the SEPARATE, full-dimension
-- table `concurrency_ext_abs` that drill-down queries read.
--
-- Approach: reuse the session-aware truly-active intervals (session_intervals)
-- as the source of "who is active when", and enrich each session with its 4
-- extended dims via a BUILD-TIME join to a tiny per-session lookup (one row per
-- session). The extended dim values were already normalized at ingest
-- (config.sql: norm_lang / norm_dim), so no cleanup is needed here.
--
-- PREREQ run order:
--   config.sql -> schema.sql -> load events -> backfill_history.sql
--   -> approach_session_aware.sql -> THIS FILE.
-- (session_intervals must be populated; concurrency_sa_abs is used for the
--  cross-check at the bottom.)
-- #####################################################################

-- =====================================================================
-- A. TABLE — full dim set. Core dims form the ORDER BY PREFIX (so a core-only
--    filter still hits the leading key), then extended dims low->high card.
--    Same shape as the DDL in schema.sql; repeated here for standalone runs.
-- =====================================================================
TRUNCATE TABLE IF EXISTS sonyliv_concurrency.concurrency_ext_abs;

CREATE TABLE IF NOT EXISTS sonyliv_concurrency.concurrency_ext_abs
( country LowCardinality(String), platform LowCardinality(String), video_type LowCardinality(String),
  category LowCardinality(String), subtitle_language LowCardinality(String),
  audio_language LowCardinality(String), player_version LowCardinality(String),
  app_version LowCardinality(String), minute DateTime('UTC'), content_id UInt64, concurrent UInt32 )
ENGINE = MergeTree
ORDER BY (country, platform, video_type, category, subtitle_language, audio_language, player_version, app_version, minute, content_id);

-- =====================================================================
-- B. POPULATE — expand truly-active intervals to buckets, enrich with the
--    per-session extended dims, count distinct sessions per full-dim bucket.
-- =====================================================================
INSERT INTO sonyliv_concurrency.concurrency_ext_abs
WITH per_session_dims AS (
    -- one row per session: its extended dims (already normalized in events_raw).
    -- any() attributes a session to a single value, exactly as the core path does
    -- for platform — a session that switches audio/device mid-stream (rare) is
    -- attributed to one; documented caveat, consistent with the core model.
    SELECT video_session_id,
           any(app_version)       AS app_version,
           any(player_version)    AS player_version,
           any(audio_language)    AS audio_language,
           any(subtitle_language) AS subtitle_language
    FROM sonyliv_concurrency.events_raw
    GROUP BY video_session_id
)
SELECT country, platform, video_type, category,
       subtitle_language, audio_language, player_version, app_version,
       minute, content_id,
       -- once-per-(full-dim)-bucket dedupe, same as the core tables.
       toUInt32(uniqExact(video_session_id)) AS concurrent
FROM (
    SELECT iv.video_session_id           AS video_session_id,
           iv.country                    AS country,
           iv.platform                   AS platform,
           iv.video_type                 AS video_type,
           iv.category                   AS category,
           iv.content_id                 AS content_id,
           iv.minute                     AS minute,
           d.app_version                 AS app_version,
           d.player_version              AS player_version,
           d.audio_language              AS audio_language,
           d.subtitle_language           AS subtitle_language
    FROM (
        -- session-aware expansion: each [active_start, active_end) -> its buckets
        -- (identical idiom to approach_session_aware.sql; bucket from config.sql,
        -- half-open end via -1ms so a boundary-aligned end doesn't claim the next).
        SELECT video_session_id, country, platform, video_type, category, content_id,
               toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds()))
                 + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
        FROM sonyliv_concurrency.session_intervals FINAL
        ARRAY JOIN range(0, toUInt64(dateDiff('second',
                       toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds())),
                       toStartOfInterval(active_end - INTERVAL 1 MILLISECOND, toIntervalSecond(cfg_bucket_seconds())))
                       / cfg_bucket_seconds()) + 1) AS number
        WHERE active_end > active_start
    ) AS iv
    -- build-time enrichment join (one row per session), NOT a serving-time join.
    INNER JOIN per_session_dims AS d ON iv.video_session_id = d.video_session_id
)
GROUP BY country, platform, video_type, category,
         subtitle_language, audio_language, player_version, app_version,
         minute, content_id;

-- =====================================================================
-- C. CROSS-CHECK — collapsing the extended table over the 4 drill-down dims
--    MUST reproduce the core session-aware counts exactly: any() gives each
--    session a single extended tuple, so summing the sub-cells telescopes back
--    to the core distinct-session count. Expect ZERO mismatched rows.
-- =====================================================================
WITH
ext_collapsed AS (
    SELECT country, platform, video_type, category, minute, content_id,
           sum(concurrent) AS c
    FROM sonyliv_concurrency.concurrency_ext_abs
    GROUP BY country, platform, video_type, category, minute, content_id
)
SELECT sa.country, sa.platform, sa.video_type, sa.category, sa.minute, sa.content_id,
       sa.concurrent AS core_sa, e.c AS ext_rolled_up
FROM sonyliv_concurrency.concurrency_sa_abs sa
FULL JOIN ext_collapsed e
  USING (country, platform, video_type, category, minute, content_id)
WHERE coalesce(sa.concurrent, 0) != coalesce(e.c, 0)
ORDER BY minute, country, platform
LIMIT 100;                        -- <-- ZERO rows = extended is a faithful superset

-- =====================================================================
-- D. SMOKE TEST
-- =====================================================================
SELECT count() AS rows FROM sonyliv_concurrency.concurrency_ext_abs;

-- peak concurrency for a drill-down combo (example: Android phone + Hindi audio)
SELECT max(c) AS peak_concurrency, argMax(minute, c) AS peak_minute
FROM (
    SELECT minute, sum(concurrent) AS c
    FROM sonyliv_concurrency.concurrency_ext_abs
    WHERE platform = 'ANDROID_PHONE' AND audio_language = 'hin'
    GROUP BY minute
);
