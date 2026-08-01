-- 002_session_intervals_append_and_platform_per_interval.sql
-- #####################################################################
-- Fixes bugs on LIVE ClickHouse Cloud, found by benchmark/benchmark.py
-- (an independent raw-events oracle) failing all 11 checks:
--
-- BUG 1 (critical, data loss) — mv_session_intervals had no APPEND on its
-- `REFRESH ... TO session_intervals` clause. A refreshable MV without APPEND
-- fully REPLACES its target table on every cycle (same footgun the project
-- already documented on the sibling mv_cold_compaction). This view's query
-- is scoped to a 20-minute recency window, so every 30s it wiped
-- session_intervals down to just "sessions active in the last 20 min",
-- destroying 03_backfill.sql's full-history backfill and every session from
-- the historical CSV seed batch. Confirmed live: session_intervals held
-- ~32,774 rows vs 42,990+ distinct sessions in events_raw, a gap that
-- reproduced on days-old, fully-settled data (nothing to do with ingest
-- lag) and matched benchmark check B8's ~10% undercount in foreground
-- session-minutes (208,406 reference vs 188,059 serving).
--
-- BUG 2 (correctness) — platform AND user_id were each collapsed to ONE
-- value per whole session (`any(...) ... GROUP BY sid`), then applied to
-- every minute the session was active. ~95/42,990 sessions (0.9%,
-- plan/PLAN.md line 96) genuinely span >1 platform (a device switch
-- mid-session), and ~120/42,990 span >1 user_id — collapsing them produced
-- mismatched (dims, minute) cells against the benchmark's reference,
-- cascading into every peak/breakdown/user-count check (B0, B2-B5, B9, B10).
--
-- FIX: `session_intervals.intervals` now carries platform AND user_id
-- INSIDE each interval tuple `(active_start, active_end, platform,
-- user_id)` instead of as session-level columns, and the state machine's
-- island-merge now also starts a new island on a platform OR user_id change
-- (not just a time gap) so every island has exactly one, unambiguous
-- platform/user_id by construction. The MV gets APPEND. Every downstream
-- reader of session_intervals (concurrency_hot_abs_mv, mv_cold_compaction;
-- also 03_backfill.sql and 04_approaches.sql, updated alongside this
-- migration) now reads platform/user_id off the tuple (`iv.3`/`iv.4`)
-- instead of plain session_intervals.platform/user_id columns.
--
-- Idempotent: DROP IF EXISTS + CREATE is safe to re-run any number of times
-- (same convention as 001_fix_content_dict_complex_key.sql). Dropping
-- session_intervals here is safe — it's a rebuildable cache (3-day TTL,
-- not a source of truth); events_raw retains 30 days of history.
--
-- REQUIRED AFTER APPLYING: re-run the full backfill (03_backfill.sql +
-- 04_approaches.sql, e.g. `python run_sql.py --all`, or at minimum those two
-- files) ONCE. This migration only fixes the going-forward behavior — the
-- live MV (even with APPEND) only ever repopulates its own 20-minute
-- recency window, so it cannot by itself resurrect the sessions BUG 1
-- already wiped, or retroactively split existing sessions' platform/user_id
-- per interval. The one-shot backfill re-derives everything from events_raw
-- under the corrected logic and restores both. NOTE: only re-run
-- 03_backfill.sql/04_approaches.sql, NOT 02_seed.sql — events_raw already
-- retains the full historical batch (30-day TTL), so re-seeding is not
-- needed to recover BUG 1's data loss.
-- #####################################################################

DROP VIEW IF EXISTS sonyliv_concurrency.mv_cold_compaction;
DROP VIEW IF EXISTS sonyliv_concurrency.concurrency_hot_abs_mv;
DROP VIEW IF EXISTS sonyliv_concurrency.mv_session_intervals;
DROP TABLE IF EXISTS sonyliv_concurrency.session_intervals;

-- ---------------------------------------------------------------------
-- session_intervals — platform/user_id now ride inside the interval tuple.
-- ---------------------------------------------------------------------
CREATE TABLE sonyliv_concurrency.session_intervals
(
    video_session_id String,
    intervals Array(Tuple(active_start DateTime64(3,'UTC'), active_end DateTime64(3,'UTC'), platform LowCardinality(String), user_id String)),
    is_provisional UInt8 DEFAULT 0,
    content_id Int64, country LowCardinality(String),
    video_type LowCardinality(String), category LowCardinality(String),
    title String, version UInt64
)
ENGINE = ReplacingMergeTree(version) ORDER BY video_session_id
TTL toDateTime(version/1000) + INTERVAL 3 DAY;

-- ---------------------------------------------------------------------
-- mv_session_intervals — APPEND added; island boundary also on platform/
-- user_id change; intervals carry (istart, iend, platform, user_id).
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW sonyliv_concurrency.mv_session_intervals
REFRESH EVERY 30 SECOND APPEND TO sonyliv_concurrency.session_intervals EMPTY AS
SELECT sess.video_session_id AS video_session_id,
       sess.intervals AS intervals,
       toUInt8(sess.last_active_end >= now() - toIntervalSecond(cfg_gap_timeout_seconds())) AS is_provisional,
       sess.content_id AS content_id, sess.country AS country,
       cd.video_type AS video_type, cd.category AS category, cd.title AS title,
       toUnixTimestamp64Milli(now64(3)) AS version
FROM
(
  WITH
  recent AS (
    SELECT video_session_id FROM sonyliv_concurrency.session_last_seen
    WHERE last_ts >= now() - INTERVAL 20 MINUTE ),
  per_event AS (
    SELECT video_session_id AS sid, user_id, event_timestamp AS ts, content_id, platform, country,
      multiIf(event_type IN ('VideoSessionStart','VideoPlay','AppForegrounded') OR event IN ('resume','speed-resume','AdResume'), 1,
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
      multiIf(rn=n, addSeconds(ts, cfg_heartbeat_seconds()),
              dateDiff('second', ts, next_ts) <= cfg_gap_timeout_seconds(), next_ts,
              addSeconds(ts, cfg_heartbeat_seconds())) AS seg_end
    FROM stated WHERE state_sign = 1 ),
  islands AS (
    SELECT *, if(seg_start > max(seg_end) OVER (PARTITION BY sid ORDER BY seg_start
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
               OR platform != lagInFrame(platform) OVER (PARTITION BY sid ORDER BY seg_start
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
               OR user_id != lagInFrame(user_id) OVER (PARTITION BY sid ORDER BY seg_start
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 1, 0) AS new_island
    FROM segments ),
  per_island AS (
    SELECT sid, island_id, min(seg_start) AS istart, max(seg_end) AS iend,
           any(user_id) AS user_id,
           any(content_id) AS content_id, any(platform) AS platform, any(country) AS country
    FROM (SELECT *, sum(new_island) OVER (PARTITION BY sid ORDER BY seg_start
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id FROM islands)
    GROUP BY sid, island_id HAVING iend > istart )
  SELECT sid AS video_session_id,
         arraySort(iv -> iv.1, groupArray((istart, iend, platform, user_id))) AS intervals,
         max(iend) AS last_active_end,
         any(content_id) AS content_id, any(country) AS country
  FROM per_island
  GROUP BY sid
) AS sess
LEFT JOIN sonyliv_concurrency.content_dim AS cd FINAL USING (content_id);

-- ---------------------------------------------------------------------
-- concurrency_hot_abs_mv (D3) — platform/user_id read off the interval tuple.
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW sonyliv_concurrency.concurrency_hot_abs_mv
REFRESH EVERY 30 SECOND DEPENDS ON sonyliv_concurrency.mv_session_intervals
TO sonyliv_concurrency.concurrency_hot_abs EMPTY AS
SELECT country, platform, video_type, category, minute, content_id,
       toUInt32(uniqExact(video_session_id)) AS concurrent,
       toUInt32(uniqExact(user_id))          AS concurrent_users
FROM (
  SELECT video_session_id, user_id, country, platform, video_type, category, content_id,
         toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds()))
           + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
  FROM
  (
    SELECT video_session_id, country, video_type, category, content_id,
           iv.1 AS active_start, iv.2 AS active_end, iv.3 AS platform, iv.4 AS user_id
    FROM sonyliv_concurrency.session_intervals FINAL
    ARRAY JOIN intervals AS iv
    WHERE iv.2 > iv.1
      AND iv.2 >= toStartOfInterval(now(), toIntervalSecond(cfg_bucket_seconds())) - INTERVAL 10 MINUTE
  )
  ARRAY JOIN range(0, toUInt64(dateDiff('second',
                 toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds())),
                 toStartOfInterval(active_end - INTERVAL 1 MILLISECOND, toIntervalSecond(cfg_bucket_seconds())))
                 / cfg_bucket_seconds()) + 1) AS number
)
WHERE minute > toStartOfInterval(now(), toIntervalSecond(cfg_bucket_seconds())) - toIntervalSecond(cfg_hot_window_seconds())
GROUP BY country, platform, video_type, category, minute, content_id;

-- ---------------------------------------------------------------------
-- mv_cold_compaction (D4) — platform/user_id read off the interval tuple.
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW sonyliv_concurrency.mv_cold_compaction
REFRESH EVERY 1 MINUTE DEPENDS ON sonyliv_concurrency.concurrency_hot_abs_mv
APPEND
TO sonyliv_concurrency.concurrency_cold_abs EMPTY AS
SELECT country, platform, video_type, category, minute, content_id,
       toUInt32(uniqExact(video_session_id)) AS concurrent,
       toUInt32(uniqExact(user_id))          AS concurrent_users
FROM (
  SELECT video_session_id, user_id, country, platform, video_type, category, content_id,
         toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds()))
           + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
  FROM (
    SELECT video_session_id, country, video_type, category, content_id,
           iv.1 AS active_start, iv.2 AS active_end, iv.3 AS platform, iv.4 AS user_id
    FROM sonyliv_concurrency.session_intervals FINAL
    ARRAY JOIN intervals AS iv
    WHERE iv.2 > iv.1
      AND iv.2 <= toStartOfInterval(now(), toIntervalSecond(cfg_bucket_seconds())) - INTERVAL 10 MINUTE
  )
  ARRAY JOIN range(0, toUInt64(dateDiff('second',
                 toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds())),
                 toStartOfInterval(active_end - INTERVAL 1 MILLISECOND, toIntervalSecond(cfg_bucket_seconds())))
                 / cfg_bucket_seconds()) + 1) AS number
)
WHERE minute <= toStartOfInterval(now(), toIntervalSecond(cfg_bucket_seconds())) - INTERVAL 10 MINUTE
  AND minute >  coalesce((SELECT max(minute) FROM sonyliv_concurrency.concurrency_cold_abs), toDateTime(0))
GROUP BY country, platform, video_type, category, minute, content_id;
