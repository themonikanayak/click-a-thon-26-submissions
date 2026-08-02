-- #####################################################################
-- 002_incremental_session_intervals.sql — an INSERT-TRIGGERED (incremental)
-- equivalent of 01_schema.sql D2 (mv_session_intervals), which is REFRESHABLE
-- (REFRESH EVERY 30 SECOND) and therefore does NOT fire on insert and is gated
-- to now()-INTERVAL 20 MINUTE (so stale/historical timestamps yield zero rows).
--
-- WHY A SINGLE INCREMENTAL MV CAN'T DO THIS DIRECTLY
-- An incremental MV sees only the just-inserted block, but D2's state machine is
-- inherently cross-row / cross-block:
--   (1) heartbeat state inheritance — a neutral VideoHeartbeat inherits active/
--       paused from the last non-neutral event (needs LOOK-BACK), and
--   (2) silent-gap island splitting — dateDiff(ts, next_ts) <= gap_timeout needs
--       the NEXT event (needs LOOK-AHEAD), which hasn't arrived at insert time.
-- A block is not guaranteed to hold a session's whole history, so neither can be
-- computed per-block.
--
-- THE FAITHFUL INSERT-TRIGGERED SHAPE (accumulator + read-time view)
--   * session_accum        — AggregatingMergeTree, one row per session holding a
--                            groupArray of that session's events.
--   * mv_session_accum      — INCREMENTAL MV on events_raw: fires on EVERY insert
--                            (direct INSERT INTO events_raw *and* the chained
--                            mv_incoming_to_raw path), appending events to the
--                            accumulator. This is the "insert trigger".
--   * session_intervals_live — a plain VIEW that reconstructs EXACTLY the same
--                            intervals as D2 by running the identical state machine
--                            over each session's accumulated events at read time.
--                            No now() gate, so backdated/historical data works too.
-- Result: intervals are always current the instant an insert lands (visible on the
-- next read of the view), with numbers identical to D2 — the cost moves from a
-- scheduled recompute to read-time window-function work.
--
-- NON-DESTRUCTIVE: this coexists with the refreshable mv_session_intervals /
-- session_intervals table; it does not drop or alter them. To serve the hot/cold
-- tiers from this path instead, point D3/D4's `FROM session_intervals FINAL` at
-- `FROM session_intervals_live` (same output columns + intervals array) — out of
-- scope here (those two MVs are refreshable and would need their own conversion).
--
-- Idempotent: CREATE TABLE IF NOT EXISTS / DROP VIEW IF EXISTS + CREATE /
-- CREATE OR REPLACE VIEW. Safe to re-run.
-- #####################################################################

-- ---------------------------------------------------------------------
-- A. ACCUMULATOR — one row per session, groupArray of its events.
-- Tuple fields are stored as plain String (not LowCardinality): LowCardinality
-- inside an AggregateFunction state has known engine quirks, and the read view
-- re-derives dims via a LEFT JOIN anyway. last_ts is a SimpleAggregateFunction so
-- the 3-day TTL can bound growth exactly like session_intervals.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sonyliv_concurrency.session_accum
(
    video_session_id String,
    events AggregateFunction(groupArray,
        Tuple(DateTime64(3, 'UTC'),   -- event_timestamp
              String,                 -- event_type
              String,                 -- event
              String,                 -- user_id
              Int64,                  -- content_id
              String,                 -- platform
              String)),               -- country
    last_ts SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
ORDER BY video_session_id
TTL toDateTime(last_ts) + INTERVAL 3 DAY;   -- bound growth; live view re-reads current sessions

-- ---------------------------------------------------------------------
-- B. INCREMENTAL MV — THE INSERT TRIGGER. Fires on every insert into events_raw
-- (direct seed inserts AND the mv_incoming_to_raw → events_raw live path). GROUP
-- BY per session produces a partial groupArrayState per inserted block; the
-- AggregatingMergeTree engine merges those partials across blocks over time.
-- ---------------------------------------------------------------------
DROP VIEW IF EXISTS sonyliv_concurrency.mv_session_accum;
CREATE MATERIALIZED VIEW sonyliv_concurrency.mv_session_accum
TO sonyliv_concurrency.session_accum AS
SELECT video_session_id,
       -- toString() folds LowCardinality → String to match the state tuple above:
       groupArrayState((event_timestamp,
                        toString(event_type),
                        toString(event),
                        user_id,
                        content_id,
                        toString(platform),
                        toString(country))) AS events,
       max(event_timestamp) AS last_ts
FROM sonyliv_concurrency.events_raw
GROUP BY video_session_id;

-- ---------------------------------------------------------------------
-- C. READ-TIME VIEW — reconstructs intervals with the SAME state machine as D2
-- (per_event → collapsed → stated → segments → islands → per_island → arrays).
-- Sourced from the accumulator (all sessions, no now()-INTERVAL 20 MINUTE gate),
-- so historical/backdated events are handled too. Output columns + order match
-- the session_intervals table so this is a drop-in source for D3/D4.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW sonyliv_concurrency.session_intervals_live AS
SELECT sess.video_session_id AS video_session_id,
       sess.user_id AS user_id,
       sess.intervals AS intervals,
       -- same provisional rule as D2 (still active within one gap timeout of now):
       toUInt8(sess.last_active_end >= now() - toIntervalSecond(cfg_gap_timeout_seconds())) AS is_provisional,
       sess.content_id AS content_id, sess.platform AS platform, sess.country AS country,
       cd.video_type AS video_type, cd.category AS category, cd.title AS title,
       toUnixTimestamp64Milli(now64(3)) AS version
FROM
(
  WITH
  -- Merge each session's partial groupArray states, then explode back to per-event
  -- rows. Merged-array order is unspecified, but every step below re-sorts by ts
  -- (collapse groups by (sid, ts); window functions ORDER BY ts), so order is safe.
  exploded AS (
    SELECT video_session_id AS sid,
           ev.1 AS ts, ev.2 AS event_type, ev.3 AS event, ev.4 AS user_id,
           ev.5 AS content_id, ev.6 AS platform, ev.7 AS country
    FROM
    (
      SELECT video_session_id, groupArrayMerge(events) AS evs
      FROM sonyliv_concurrency.session_accum
      GROUP BY video_session_id
    )
    ARRAY JOIN evs AS ev ),
  per_event AS (
    SELECT sid, user_id, ts, content_id, platform, country,
      -- ACTIVATE (foreground-only). VideoSessionStart SEEDS the session as active from the start — a
      -- session is watching until a pause/bg/error/end stops it — so active heartbeats BEFORE the first
      -- explicit VideoPlay aren't dropped, and a session that never emits an explicit Play still counts.
      multiIf(event_type IN ('VideoSessionStart','VideoPlay','AppForegrounded') OR event IN ('resume','speed-resume','AdResume'), 1,
              -- DEACTIVATE (foreground-only). PAUSE has no coarse event_type in the raw feed — it rides in
              -- the `event` column (dataset_details.md: pause/speed-pause/AdPause). We also match the
              -- VideoPause/AdBreakStart pause-family event_types directly, so a paused-but-heartbeating
              -- session can't leak in as active (GAP #2).
              event_type IN ('AppBackgrounded','VideoSessionEnd','VideoError','VideoPause','AdBreakStart') OR event IN ('pause','speed-pause','AdPause'), -1,
              0) AS transition
    FROM exploded ),
  collapsed AS (                                    -- one row per (session, ms); deactivate wins
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
      -- grace tail + gap timeout from 00_config.sql (cfg_heartbeat_seconds / cfg_gap_timeout_seconds):
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
  -- Collapse a session's islands into one array-typed row (matches session_intervals).
  SELECT sid AS video_session_id, any(user_id) AS user_id,
         arraySort(iv -> iv.1, groupArray((istart, iend))) AS intervals,
         max(iend) AS last_active_end,
         any(content_id) AS content_id, any(platform) AS platform, any(country) AS country
  FROM per_island
  GROUP BY sid
) AS sess
LEFT JOIN sonyliv_concurrency.content_dim FINAL AS cd USING (content_id);

-- ---------------------------------------------------------------------
-- D. Smoke test — after any INSERT INTO events_raw, this reflects it immediately
-- (no SYSTEM REFRESH / no 30s wait). Compare against the refreshable path.
-- ---------------------------------------------------------------------
SELECT count() AS sessions, sum(length(intervals)) AS total_intervals
FROM sonyliv_concurrency.session_intervals_live;
