-- [WRITE — build] step 01 of the offline pipeline; also the live-app schema.
-- #####################################################################
-- 01_schema.sql — EVERYTHING the live app needs: tables + dictionary + view + MVs.
-- This is the SINGLE source of DDL: every table / MV / dictionary is created
-- here (incl. the comparison-only concurrency_sa_abs / concurrency_si_abs /
-- concurrency_ext_abs, which 04_approaches.sql only TRUNCATEs + INSERTs into).
-- Run once. Then point ClickPipes (Redpanda → events_incoming) and read
-- live insights with ui_queries.sql.
--
--   clickhouse client --host <h> --user default --secure --queries-file 01_schema.sql
--
-- Flow: Redpanda → ClickPipes → events_incoming ─(MV)→ events_raw
--        ─(MV: state machine)→ session_intervals ─(MV: hot)→ concurrency_hot_abs
--        + cold compaction → concurrency_cold_abs ; concurrency_now = union.
--
-- ===== LIVE-TRAFFIC TUNING (this is a continuous, high-throughput service) =====
-- INGEST (throughput):
--   * events_incoming = Null → no landing storage; MV converts on insert.
--   * ClickPipes: use LARGE batches (e.g. 100k rows / few seconds) — few big
--     inserts >> many small ones. Enable async inserts on the pipe if available.
--   * events_raw partitioned monthly + 30d TTL so raw never grows unbounded
--     (the aggregates live in cold_abs).
-- COMPUTE (bounded, never rescans history):
--   * Derivation window = 20 min; hot window = freeze horizon = cfg_hot_window_seconds() (default 600s = 10 min).
--     Recompute cost ∝ (window × ACTIVE sessions), independent of TOTAL history —
--     but see the open caveat below re: per-session cost within that window.
--     Keep windows as TIGHT as p99 heartbeat lag allows (measure via ClickStack).
--   * Refresh cadence: derivation 30 s · hot 30 s · compaction ~1 min.
--     Tightened from 1 min so an open session's state reaches concurrency_now
--     within ~30-60s end-to-end. Raise cadence back if refresh duration
--     approaches the interval (watch via system.view_refreshes / query_log).
--   * Cold is append-only forward-fill → finalized minutes are never recomputed.
--   * D3/D4 filter session_intervals to the hot/freeze window BEFORE the
--     ARRAY JOIN expansion (not after) — provably equivalent, avoids expanding
--     the table's full retained history (3-day TTL) every cycle.
-- SERVE (fast under write load):
--   * Serving tables are absolute per (dims,minute) → queries are filter→sum→max/avg,
--     tiny reads, no cumsum. content_dict avoids JOINs on the hot path.
-- KNOWN OPEN CAVEAT (documented, not yet fixed — see PLAN.md §9):
--   * mv_session_intervals re-derives a session's FULL event history on every
--     refresh (bounded by session activity, not by the 20-min window itself),
--     so cost per active session grows with session DURATION, not just count.
--     For hours-long live-sport sessions this is the next real bottleneck.
--     Fix path: an incremental per-session cursor (carry watching-state +
--     last-processed-ts forward, process only NEW events each cycle) instead
--     of full re-derivation — a genuine architecture change, not applied here.
-- SCALE-OUT (100×): shard by video_session_id; size the service to peak concurrency
--   not event volume; if a single refresh can't finish in its interval, split the
--   hot window across parallel refreshes or adopt the incremental-cursor fix above.
-- ===============================================================================
-- #####################################################################

CREATE DATABASE IF NOT EXISTS sonyliv_concurrency;

-- =====================================================================
-- A. TABLES
-- =====================================================================

-- ClickPipes landing (Redpanda JSON; ms epochs). Transient.
-- PRODUCER: one JSON message per event (JSONEachRow), keys = these columns,
--   Redpanda key = video_session_id. Do NOT pre-convert timestamps (send ms ints).
-- CLICKPIPES: Source=Kafka(Redpanda) · Topic=sonyliv.events · Format=JSONEachRow
--   · Destination=sonyliv_concurrency.events_incoming · map by field name.
CREATE TABLE IF NOT EXISTS sonyliv_concurrency.events_incoming
(
    content_id Int64, video_session_id String, user_id String,
    event_type LowCardinality(String), event LowCardinality(String), event_timestamp UInt64,
    platform LowCardinality(String), app_version LowCardinality(String), country LowCardinality(String),
    audio_language LowCardinality(String), subtitle_language LowCardinality(String),
    player_version LowCardinality(String), session_start_epoch UInt64
)
ENGINE = Null;   -- MV consumes each insert; nothing stored (leaner). To DEBUG the
                 -- ClickPipes mapping, temporarily use: ENGINE = MergeTree ORDER BY tuple().

-- Canonical typed events.
CREATE TABLE IF NOT EXISTS sonyliv_concurrency.events_raw
(
    -- content_id is Int64 (not UInt64): the catalog carries a negative placeholder
    -- ID (review #1), and content_dim/content_dict are Int64 — keep them aligned.
    video_session_id String, user_id String, content_id Int64,
    -- event_type is LowCardinality(String), NOT Enum8: the dataset lists only the
    -- CURRENT 7 types (VideoSessionStart/VideoPlay/VideoHeartbeat/AppBackgrounded/
    -- AppForegrounded/VideoSessionEnd/VideoError) and warns more may appear. A strict
    -- Enum8 would REJECT any unseen-day event_type on ingest; String tolerates new
    -- values at ~zero cost (LowCardinality keeps the dictionary encoding).
    event_type LowCardinality(String),
    event LowCardinality(String),
    event_timestamp DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1)),
    session_start_epoch DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1)),
    platform LowCardinality(String), app_version LowCardinality(String), country LowCardinality(String),
    audio_language LowCardinality(String), subtitle_language LowCardinality(String), player_version LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (video_session_id, event_timestamp)   -- session-contiguous: derivation is a streaming per-session read
PARTITION BY toYYYYMM(event_timestamp)         -- monthly (lifecycle only; bounded partition count)
TTL toDateTime(event_timestamp) + INTERVAL 30 DAY;  -- aggregates live in cold_abs; bound raw growth

-- Content metadata.
CREATE TABLE IF NOT EXISTS sonyliv_concurrency.content_dim
( content_id Int64, title String, video_type LowCardinality(String), category LowCardinality(String) )
ENGINE = ReplacingMergeTree ORDER BY content_id;

-- Truly-active intervals (state-machine output). One row per session — the
-- FULL current set of that session's islands lives in `intervals`, so a
-- refresh that re-derives fewer islands than before simply ships a shorter
-- array; ReplacingMergeTree(version) on a single-column key (video_session_id)
-- means the whole prior row is superseded, so stale islands can never survive
-- under FINAL (unlike a per-interval key, where a shrunk island count leaves
-- orphaned high-index rows behind).
CREATE TABLE IF NOT EXISTS sonyliv_concurrency.session_intervals
(
    -- One row per session (array of intervals). user_id + title are 1:1 with the
    -- session/content so they ride as plain columns: user_id enables user-level
    -- concurrency, title makes the content name a keyed dim without cardinality cost.
    video_session_id String, user_id String,
    intervals Array(Tuple(active_start DateTime64(3,'UTC'), active_end DateTime64(3,'UTC'))),
    is_provisional UInt8 DEFAULT 0,
    content_id Int64, platform LowCardinality(String), country LowCardinality(String),
    video_type LowCardinality(String), category LowCardinality(String),
    title String, version UInt64
)
ENGINE = ReplacingMergeTree(version) ORDER BY video_session_id
TTL toDateTime(version/1000) + INTERVAL 3 DAY;   -- bound growth; cold_abs already holds the durable aggregate

-- Serving tiers: ABSOLUTE concurrency per (dims, minute).
-- cold = ReplacingMergeTree so a re-fired/retried compaction can't double-count
-- a minute (one row per (dims,minute) key; read with FINAL). hot is REPLACE-
-- recomputed wholesale, so plain MergeTree is fine there.
-- concurrent      = distinct SESSIONS active in the (dims, minute)  = uniqExact(video_session_id)
-- concurrent_users = distinct USERS    active in the (dims, minute)  = uniqExact(user_id)
-- Both are exact per cell and for fixed-dimension peak/avg. NOTE: summing
-- concurrent_users ACROSS dim rows can overcount a user watching >1 content/dim
-- (the same additivity caveat the session count assumes; a session has ~one
-- dim-tuple, a user may span several) — for exact cross-dim user rollups switch
-- to uniqExactState + AggregatingMergeTree.
CREATE TABLE IF NOT EXISTS sonyliv_concurrency.concurrency_cold_abs
( country LowCardinality(String), platform LowCardinality(String), video_type LowCardinality(String),
  category LowCardinality(String), minute DateTime('UTC'), content_id Int64,
  concurrent UInt32, concurrent_users UInt32 )
ENGINE = ReplacingMergeTree ORDER BY (country, platform, video_type, category, minute, content_id);

CREATE TABLE IF NOT EXISTS sonyliv_concurrency.concurrency_hot_abs
( country LowCardinality(String), platform LowCardinality(String), video_type LowCardinality(String),
  category LowCardinality(String), minute DateTime('UTC'), content_id Int64,
  concurrent UInt32, concurrent_users UInt32 )
ENGINE = MergeTree ORDER BY (country, platform, video_type, category, minute, content_id);

-- EXTENDED drill-down serving table: the 4 high-cardinality dims
-- (app_version, player_version, audio_language, subtitle_language) IN ADDITION to
-- the core dims. Kept SEPARATE from the lean core tiers (PLAN §9 Fix #7) so the
-- common-case dashboard stays fast on the small core key, while drill-down
-- queries that filter on device/language read this. Absolute per (dims, minute),
-- same model. Populated by 04_approaches.sql from session_intervals
-- (offline/scheduled; no live hot/cold tier for the extended path in this version).
-- ORDER BY keeps the core key as its PREFIX (country,platform,video_type,category)
-- then adds the extended dims low→high cardinality, so a core-only filter still
-- uses the leading key, and this table is a clean superset of the core key.
-- content_id is Int64 (not UInt64) — same negative-sentinel fix as every other
-- content_id column (review #1): the catalog has a negative placeholder ID.
-- title sits next to content_id in the key: it is 1:1 with content_id, so it adds
-- no cardinality but makes the content name a first-class drill-down dimension
-- here (core tiers still resolve title via dictGet(content_id) at query time).
CREATE TABLE IF NOT EXISTS sonyliv_concurrency.concurrency_ext_abs
( country LowCardinality(String), platform LowCardinality(String), video_type LowCardinality(String),
  category LowCardinality(String), subtitle_language LowCardinality(String),
  audio_language LowCardinality(String), player_version LowCardinality(String),
  app_version LowCardinality(String), minute DateTime('UTC'), content_id Int64, title String,
  concurrent UInt32, concurrent_users UInt32 )
ENGINE = MergeTree
ORDER BY (country, platform, video_type, category, subtitle_language, audio_language, player_version, app_version, minute, content_id, title);

-- COMPARISON-ONLY serving tables (no hot/cold tiering) — populated by
-- 04_approaches.sql (TRUNCATE + INSERT), read/asserted by 05_compare.sql.
-- Same shape/engine/ORDER BY as concurrency_cold_abs so the two approaches'
-- outputs are byte-for-byte comparable minute-for-minute. DDL lives HERE (not in
-- 04) so 01_schema.sql stays the single source of every object.
--   * concurrency_sa_abs — SESSION-AWARE (expanded from session_intervals).
--   * concurrency_si_abs — SESSION-INDEPENDENT (per-event state, no interval merge).
CREATE TABLE IF NOT EXISTS sonyliv_concurrency.concurrency_sa_abs
( country LowCardinality(String), platform LowCardinality(String), video_type LowCardinality(String),
  category LowCardinality(String), minute DateTime('UTC'), content_id Int64,
  concurrent UInt32, concurrent_users UInt32 )
ENGINE = MergeTree ORDER BY (country, platform, video_type, category, minute, content_id);

CREATE TABLE IF NOT EXISTS sonyliv_concurrency.concurrency_si_abs
( country LowCardinality(String), platform LowCardinality(String), video_type LowCardinality(String),
  category LowCardinality(String), minute DateTime('UTC'), content_id Int64,
  concurrent UInt32, concurrent_users UInt32 )
ENGINE = MergeTree ORDER BY (country, platform, video_type, category, minute, content_id);

-- One row per session, incrementally kept up to date (fed by mv_session_last_seen
-- below). Lets D2's "which sessions are recent" lookup read a tiny per-session
-- table instead of full-scanning events_raw every 30s (recompute cost O(active
-- sessions), matching the design doc's claim rather than O(history)).
CREATE TABLE IF NOT EXISTS sonyliv_concurrency.session_last_seen
( video_session_id String, last_ts SimpleAggregateFunction(max, DateTime64(3,'UTC')) )
ENGINE = AggregatingMergeTree ORDER BY video_session_id;

-- Tiny distinct-value table for cheap UI filter dropdowns (fed by mv_dim_values
-- below), so populating a dropdown doesn't force a cold FINAL + hot union scan
-- of concurrency_now.
CREATE TABLE IF NOT EXISTS sonyliv_concurrency.dim_values
( dim LowCardinality(String), value LowCardinality(String) )
ENGINE = ReplacingMergeTree ORDER BY (dim, value);

-- =====================================================================
-- B. DICTIONARY (content enrichment via dictGet)
-- =====================================================================
DROP DICTIONARY IF EXISTS sonyliv_concurrency.content_dict;
CREATE DICTIONARY sonyliv_concurrency.content_dict
( content_id Int64, title String, video_type String, category String )
PRIMARY KEY content_id
SOURCE(CLICKHOUSE( USER 'default' PASSWORD '' DB 'sonyliv_concurrency' TABLE 'content_dim' ))
LAYOUT(HASHED()) LIFETIME(MIN 600 MAX 1200);

-- =====================================================================
-- C. SERVING VIEW (what UI reads) — cold ∪ hot, disjoint by minute
-- =====================================================================
CREATE OR REPLACE VIEW sonyliv_concurrency.concurrency_now AS
SELECT country, platform, video_type, category, minute, content_id, concurrent, concurrent_users
FROM sonyliv_concurrency.concurrency_cold_abs FINAL          -- dedup ReplacingMergeTree
UNION ALL
SELECT country, platform, video_type, category, minute, content_id, concurrent, concurrent_users
FROM sonyliv_concurrency.concurrency_hot_abs
-- coalesce: an empty cold_abs (pure-live deployment, nothing compacted yet) must
-- not turn into `minute > NULL`, which would silently filter out every hot row.
WHERE minute > coalesce((SELECT max(minute) FROM sonyliv_concurrency.concurrency_cold_abs), toDateTime(0));

-- =====================================================================
-- D. MATERIALIZED VIEWS (populate the tables)
-- =====================================================================

-- D1. events_incoming → events_raw (cast ms → DateTime64). Incremental.
CREATE MATERIALIZED VIEW IF NOT EXISTS sonyliv_concurrency.mv_incoming_to_raw
TO sonyliv_concurrency.events_raw AS
SELECT video_session_id, user_id, content_id, event_type, event,
       fromUnixTimestamp64Milli(event_timestamp, 'UTC')     AS event_timestamp,
       fromUnixTimestamp64Milli(session_start_epoch, 'UTC') AS session_start_epoch,
       -- column order MUST match events_raw (positional MV insert):
       -- platform, app_version, country, audio_language, subtitle_language, player_version.
       -- normalize the 4 extended dims at the edge (00_config.sql) so events_raw is
       -- canonical and drill-down filters don't fragment (hin/HIN/hin-hindi):
       platform,
       norm_dim(app_version)        AS app_version,
       country,
       norm_lang(audio_language)    AS audio_language,
       norm_lang(subtitle_language) AS subtitle_language,
       norm_dim(player_version)     AS player_version
FROM sonyliv_concurrency.events_incoming;

-- D1b. events_raw → session_last_seen (incremental, keeps D2's recency lookup tiny).
CREATE MATERIALIZED VIEW IF NOT EXISTS sonyliv_concurrency.mv_session_last_seen
TO sonyliv_concurrency.session_last_seen AS
SELECT video_session_id, max(event_timestamp) AS last_ts
FROM sonyliv_concurrency.events_raw
GROUP BY video_session_id;

-- D1c. events_raw → dim_values (incremental, feeds cheap UI dropdowns).
CREATE MATERIALIZED VIEW IF NOT EXISTS sonyliv_concurrency.mv_dim_values
TO sonyliv_concurrency.dim_values AS
SELECT 'platform' AS dim, platform AS value FROM sonyliv_concurrency.events_raw
UNION ALL
SELECT 'country' AS dim, country AS value FROM sonyliv_concurrency.events_raw;

-- D2. events_raw → session_intervals (state machine, recent 20-min window).
-- Events collapsed per (session, ms): deactivate > reactivate > neutral (determinism).
-- Refresh every 30s (tightened from 1min) so an open session's state reaches
-- concurrency_now within ~30-60s end-to-end instead of ~60-90s.
-- Recency lookup reads session_last_seen (O(active sessions)), not a full
-- events_raw scan. Content enrichment is a LEFT JOIN against content_dim, not
-- dictGet — a ClickHouse Cloud dictionary reload is node-local, so a stale
-- replica can silently serve wrong video_type/category (LEFT so sessions with
-- an unrecognized content_id still count, just with empty dims).
DROP VIEW IF EXISTS sonyliv_concurrency.mv_session_intervals;
CREATE MATERIALIZED VIEW sonyliv_concurrency.mv_session_intervals
REFRESH EVERY 30 SECOND TO sonyliv_concurrency.session_intervals EMPTY AS
-- Column order MUST match session_intervals (positional MV insert):
-- video_session_id, user_id, intervals, is_provisional, content_id, platform,
-- country, video_type, category, title, version. Content dims (incl. title) come
-- from a LEFT JOIN content_dim, NOT dictGet — a Cloud dictionary reload is
-- node-local, so a stale replica could otherwise serve wrong dims.
SELECT sess.video_session_id AS video_session_id,
       sess.user_id AS user_id,
       sess.intervals AS intervals,
       -- provisional threshold tied to the same gap timeout (00_config.sql) used to
       -- close a stretch, so tuning that one knob keeps this consistent too.
       toUInt8(sess.last_active_end >= now() - toIntervalSecond(cfg_gap_timeout_seconds())) AS is_provisional,
       sess.content_id AS content_id, sess.platform AS platform, sess.country AS country,
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
      -- grace tail + gap timeout come from 00_config.sql (was hardcoded +60s / <=90s);
      -- already uses dateDiff('second', ...) for an unambiguous unit on the gap test.
      multiIf(rn=n, addSeconds(ts, cfg_heartbeat_seconds()),
              dateDiff('second', ts, next_ts) <= cfg_gap_timeout_seconds(), next_ts,
              addSeconds(ts, cfg_heartbeat_seconds())) AS seg_end
    FROM stated WHERE state_sign = 1 ),
  islands AS (
    SELECT *, if(seg_start > max(seg_end) OVER (PARTITION BY sid ORDER BY seg_start
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 1, 0) AS new_island
    FROM segments ),
  per_island AS (
    SELECT sid, island_id, min(seg_start) AS istart, max(seg_end) AS iend,
           any(user_id) AS user_id,
           any(content_id) AS content_id, any(platform) AS platform, any(country) AS country
    FROM (SELECT *, sum(new_island) OVER (PARTITION BY sid ORDER BY seg_start
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id FROM islands)
    GROUP BY sid, island_id HAVING iend > istart )
  -- Collapse a session's islands into one array-typed row — see the
  -- session_intervals table comment: this is what makes a shrunk island
  -- count (fewer islands than the previous refresh) unable to leave stale
  -- rows behind under FINAL.
  SELECT sid AS video_session_id, any(user_id) AS user_id,
         arraySort(iv -> iv.1, groupArray((istart, iend))) AS intervals,
         max(iend) AS last_active_end,
         any(content_id) AS content_id, any(platform) AS platform, any(country) AS country
  FROM per_island
  GROUP BY sid
) AS sess
LEFT JOIN sonyliv_concurrency.content_dim FINAL AS cd USING (content_id);

-- D3. session_intervals → concurrency_hot_abs (recent minutes, absolute). 30s REPLACE.
DROP VIEW IF EXISTS sonyliv_concurrency.concurrency_hot_abs_mv;
CREATE MATERIALIZED VIEW sonyliv_concurrency.concurrency_hot_abs_mv
REFRESH EVERY 30 SECOND DEPENDS ON sonyliv_concurrency.mv_session_intervals
TO sonyliv_concurrency.concurrency_hot_abs EMPTY AS
-- Perf fix: filter to the hot window BEFORE the minute-expansion ARRAY JOIN, not
-- after. An interval whose active_end is before the window can never produce a
-- minute inside it, so this is provably equivalent to the old filter — just
-- avoids expanding (and scanning) session_intervals' entire history every 30s.
SELECT country, platform, video_type, category, minute, content_id,
       toUInt32(uniqExact(video_session_id)) AS concurrent,
       toUInt32(uniqExact(user_id))          AS concurrent_users
FROM (
  SELECT video_session_id, user_id, country, platform, video_type, category, content_id,
         -- configurable bucket (00_config.sql): start-of-bucket + N buckets
         toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds()))
           + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
  FROM
  (
    -- unpack session_intervals' one-row-per-session Array(Tuple(...)) first
    -- (ghost-interval fix, review #7) — session_intervals FINAL no longer has
    -- active_start/active_end as plain columns.
    SELECT video_session_id, user_id, country, platform, video_type, category, content_id,
           iv.1 AS active_start, iv.2 AS active_end
    FROM sonyliv_concurrency.session_intervals FINAL
    ARRAY JOIN intervals AS iv
    WHERE iv.2 > iv.1
      AND iv.2 >= toStartOfInterval(now(), toIntervalSecond(cfg_bucket_seconds())) - INTERVAL 10 MINUTE   -- prune BEFORE expanding
  )
  ARRAY JOIN range(0, toUInt64(dateDiff('second',
                 toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds())),
                 toStartOfInterval(active_end - INTERVAL 1 MILLISECOND, toIntervalSecond(cfg_bucket_seconds())))
                 / cfg_bucket_seconds()) + 1) AS number
)
-- HOT window from 00_config.sql (cfg_hot_window_seconds, default 600 = old 10 min).
WHERE minute > toStartOfInterval(now(), toIntervalSecond(cfg_bucket_seconds())) - toIntervalSecond(cfg_hot_window_seconds())
GROUP BY country, platform, video_type, category, minute, content_id;

-- D4. COLD compaction — now a real scheduled job (previously a commented-out
-- INSERT block wired up nowhere, meaning a pure-live deployment never
-- populated cold_abs and concurrency_now silently served only the hot tier's
-- last 10 minutes). DEPENDS ON D3 so it runs after session_intervals/hot_abs
-- are current for this cycle. Same pre-ARRAY-JOIN pruning as D3.
-- concurrency_cold_abs is ReplacingMergeTree so a retried/overlapping run
-- can't double-count; the forward-fill guard keeps it idempotent and
-- append-only (never touches already-frozen minutes), coalesced against an
-- empty cold_abs on the very first run. Emits BOTH measures (sessions + users)
-- to match concurrency_cold_abs / the hot tier.
DROP VIEW IF EXISTS sonyliv_concurrency.mv_cold_compaction;
CREATE MATERIALIZED VIEW sonyliv_concurrency.mv_cold_compaction
REFRESH EVERY 1 MINUTE DEPENDS ON sonyliv_concurrency.concurrency_hot_abs_mv
TO sonyliv_concurrency.concurrency_cold_abs EMPTY AS
-- Bucket-aware to match D3 (00_config.sql cfg_bucket_seconds()) — hot and cold
-- must bucket identically or concurrency_now's cold/hot union misaligns.
SELECT country, platform, video_type, category, minute, content_id,
       toUInt32(uniqExact(video_session_id)) AS concurrent,
       toUInt32(uniqExact(user_id))          AS concurrent_users
FROM (
  SELECT video_session_id, user_id, country, platform, video_type, category, content_id,
         toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds()))
           + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
  FROM (
    SELECT video_session_id, user_id, country, platform, video_type, category, content_id,
           iv.1 AS active_start, iv.2 AS active_end
    FROM sonyliv_concurrency.session_intervals FINAL
    ARRAY JOIN intervals AS iv
    WHERE iv.2 > iv.1
      AND iv.2 <= toStartOfInterval(now(), toIntervalSecond(cfg_bucket_seconds())) - INTERVAL 10 MINUTE          -- only fully-aged intervals
  )
  ARRAY JOIN range(0, toUInt64(dateDiff('second',
                 toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds())),
                 toStartOfInterval(active_end - INTERVAL 1 MILLISECOND, toIntervalSecond(cfg_bucket_seconds())))
                 / cfg_bucket_seconds()) + 1) AS number
)
WHERE minute <= toStartOfInterval(now(), toIntervalSecond(cfg_bucket_seconds())) - INTERVAL 10 MINUTE
  AND minute >  coalesce((SELECT max(minute) FROM sonyliv_concurrency.concurrency_cold_abs), toDateTime(0))  -- forward-fill only
GROUP BY country, platform, video_type, category, minute, content_id;

SHOW TABLES FROM sonyliv_concurrency;
