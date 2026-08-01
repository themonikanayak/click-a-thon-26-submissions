-- [WRITE — build] step 04 of the offline pipeline. Populates the three comparison tables.
-- #####################################################################
-- 04_approaches.sql — INSERT-only jobs for the three comparison tables:
--   session-aware (concurrency_sa_abs), session-independent
--   (concurrency_si_abs), and extended-dims (concurrency_ext_abs).
--
-- This file holds ONLY the population (TRUNCATE + INSERT) logic for those
-- three tables. Their DDL now lives in 01_schema.sql (single source of
-- objects for the whole pipeline) and their smoke-test / cross-check
-- SELECTs now live in 05_compare.sql (single place to eyeball results and
-- assert the approaches agree).
--
-- PREREQ run order:
--   00_config.sql -> 01_schema.sql -> 02_seed.sql -> 03_backfill.sql
--   -> THIS FILE -> 05_compare.sql.
-- #####################################################################

-- #####################################################################
-- SECTION 1 — SESSION-AWARE (concurrency_sa_abs)
-- #####################################################################
-- #####################################################################
-- 04_approaches.sql — SESSION-AWARE approach, standalone & comparable.
--
-- WHY this file exists: the problem asks us to compare two approaches to
-- concurrency (session-aware vs session-independent) head-to-head. The
-- real serving path (01_schema.sql) mixes the session-aware approach with
-- hot/cold TIERING, which is an orthogonal freshness/compaction concern
-- and would pollute a 1:1 diff. This file re-materializes the SAME
-- session-aware numbers into their own full-history table, with no tier
-- split, so it can be joined minute-for-minute against the
-- session-independent approach's table (a separate file) to see exactly
-- where/why the two methods disagree.
--
-- Reads sonyliv_concurrency.session_intervals — assumes 03_backfill.sql
-- (the state machine) has already been run so intervals are populated.
-- Does NOT touch events_raw, does NOT reimplement hot/cold tiers.
-- #####################################################################

TRUNCATE TABLE IF EXISTS sonyliv_concurrency.concurrency_sa_abs;

-- =====================================================================
-- B. POPULATE — full history, no watermark (this table exists purely for
--    comparison, not for live serving, so there's no hot/cold split here).
-- =====================================================================
INSERT INTO sonyliv_concurrency.concurrency_sa_abs
SELECT country, platform, video_type, category, minute, content_id,
       -- once-per-minute dedupe: a session that is active for part of a
       -- minute (or has >1 truly-active interval touching the same minute)
       -- must still count as ONE concurrent viewer in that minute.
       toUInt32(uniqExact(video_session_id)) AS concurrent,
       toUInt32(uniqExact(user_id))          AS concurrent_users
FROM (
  -- Expand each truly-active [active_start, active_end) interval to every
  -- minute it touches. This is the session-aware core: active_end/active_start
  -- already come from the state machine, so paused/backgrounded time is
  -- excluded — a session only "occupies" the minutes it was really watching.
  -- half-open end: subtract 1ms before flooring so an interval ending exactly
  -- on a minute boundary doesn't spuriously claim that next minute.
  SELECT video_session_id, user_id, country, platform, video_type, category, content_id,
         -- configurable bucket (00_config.sql): start-of-bucket + N buckets
         toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds()))
           + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
  FROM (
    -- unpack session_intervals' one-row-per-session Array(Tuple(...)) first —
    -- FINAL no longer exposes active_start/active_end as plain columns.
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
GROUP BY country, platform, video_type, category, minute, content_id;

-- #####################################################################
-- SECTION 2 — SESSION-INDEPENDENT (concurrency_si_abs)
-- #####################################################################
-- #####################################################################
-- 04_approaches.sql — SESSION-INDEPENDENT approach,
-- standalone & comparable to 04_approaches.sql.
--
-- WHY this file exists: same head-to-head comparison as
-- 04_approaches.sql, but for the SESSION-INDEPENDENT method:
-- derive the SAME foreground-only active definition per event, WITHOUT
-- reconstructing per-session intervals (no island-merge, no interval_idx).
-- This isolates "does interval reconstruction matter for the final count?"
-- as the only difference between the two files — everything upstream of
-- island-merging (the transition state machine, the 90s gap / 60s grace
-- segment rule) is copied verbatim from 03_backfill.sql so both
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
-- B. POPULATE — same active definition as the state machine
--    (03_backfill.sql), but STOP at `segments` (per-event active
--    stretches) instead of merging them into `islands`/interval_idx.
-- =====================================================================
INSERT INTO sonyliv_concurrency.concurrency_si_abs
SELECT country, platform, video_type, category, minute, content_id,
       -- once-per-minute dedupe (PLAN §2): uniqExact collapses however many
       -- active SEGMENTS a session has touching this minute (e.g. two short
       -- plays either side of a pause) down to ONE concurrent viewer. This
       -- is also *why* skipping island-merge is safe here — see section C.
       toUInt32(uniqExact(video_session_id)) AS concurrent,
       toUInt32(uniqExact(user_id))          AS concurrent_users
FROM
(
  WITH
  -- ---- copied verbatim from 03_backfill.sql: same active definition ----
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
  -- `segments` = per-event active stretches (90s gap closes a stretch at
  -- last_seen+60s grace; a gap <=90s bridges straight to the next event),
  -- identical to 03_backfill.sql. THIS IS THE STOPPING POINT:
  -- session-aware keeps going from here (builds `islands`, merging
  -- consecutive segments per session into one interval via interval_idx);
  -- session-independent expands straight from `segments` below — no
  -- island/interval_idx construction at all.
  segments AS (
    SELECT sid, user_id, content_id, platform, country, ts AS seg_start,
      -- grace tail + gap timeout now come from 00_config.sql (was +60s / <=90s):
      multiIf(rn=n, addSeconds(ts, cfg_heartbeat_seconds()),
              dateDiff('second', ts, next_ts) <= cfg_gap_timeout_seconds(), next_ts,
              addSeconds(ts, cfg_heartbeat_seconds())) AS seg_end
    FROM stated WHERE state_sign = 1
  )
  -- ---- expand each active SEGMENT (not island) straight to minutes ----
  -- Same half-open-end idiom as 03_backfill.sql: subtract 1ms before
  -- flooring so a segment ending exactly on a minute boundary doesn't
  -- spuriously claim that next minute.
  SELECT sid AS video_session_id, user_id, country, platform,
         dictGet('sonyliv_concurrency.content_dict','video_type', content_id) AS video_type,
         dictGet('sonyliv_concurrency.content_dict','category',   content_id) AS category,
         content_id,
         -- configurable bucket (00_config.sql): start-of-bucket + N buckets
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

-- #####################################################################
-- SECTION 3 — EXTENDED-DIMS (concurrency_ext_abs)
-- #####################################################################
-- #####################################################################
-- 04_approaches.sql — EXTENDED drill-down aggregate.
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
-- (00_config.sql: norm_lang / norm_dim), so no cleanup is needed here.
--
-- PREREQ run order:
--   00_config.sql -> 01_schema.sql -> load events -> 03_backfill.sql
--   -> 04_approaches.sql -> THIS FILE.
-- (session_intervals must be populated; concurrency_sa_abs is used for the
--  cross-check at the bottom.)
-- #####################################################################

TRUNCATE TABLE IF EXISTS sonyliv_concurrency.concurrency_ext_abs;

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
       minute, content_id, title,
       -- once-per-(full-dim)-bucket dedupe, same as the core tables.
       toUInt32(uniqExact(video_session_id)) AS concurrent,
       toUInt32(uniqExact(user_id))          AS concurrent_users
FROM (
    SELECT iv.video_session_id           AS video_session_id,
           iv.user_id                    AS user_id,
           iv.country                    AS country,
           iv.platform                   AS platform,
           iv.video_type                 AS video_type,
           iv.category                   AS category,
           iv.content_id                 AS content_id,
           iv.title                      AS title,
           iv.minute                     AS minute,
           d.app_version                 AS app_version,
           d.player_version              AS player_version,
           d.audio_language              AS audio_language,
           d.subtitle_language           AS subtitle_language
    FROM (
        -- session-aware expansion: each [active_start, active_end) -> its buckets
        -- (identical idiom to 04_approaches.sql; bucket from 00_config.sql,
        -- half-open end via -1ms so a boundary-aligned end doesn't claim the next).
        SELECT video_session_id, user_id, country, platform, video_type, category, content_id, title,
               toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds()))
                 + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
        FROM (
          -- unpack session_intervals' one-row-per-session Array(Tuple(...)) first —
          -- FINAL no longer exposes active_start/active_end as plain columns.
          SELECT video_session_id, user_id, country, platform, video_type, category, content_id, title,
                 iv.1 AS active_start, iv.2 AS active_end
          FROM sonyliv_concurrency.session_intervals FINAL
          ARRAY JOIN intervals AS iv
          WHERE iv.2 > iv.1
        )
        ARRAY JOIN range(0, toUInt64(dateDiff('second',
                       toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds())),
                       toStartOfInterval(active_end - INTERVAL 1 MILLISECOND, toIntervalSecond(cfg_bucket_seconds())))
                       / cfg_bucket_seconds()) + 1) AS number
    ) AS iv
    -- build-time enrichment join (one row per session), NOT a serving-time join.
    INNER JOIN per_session_dims AS d ON iv.video_session_id = d.video_session_id
)
GROUP BY country, platform, video_type, category,
         subtitle_language, audio_language, player_version, app_version,
         minute, content_id, title;
