-- [READ — validate] step 06 of the offline pipeline. Serving == brute-force + oracle; correctness invariants.
-- #####################################################################
-- 05_verification.sql — correctness checks.
-- #####################################################################

-- ---------------------------------------------------------------------
-- A) SERVING == BRUTE FORCE. The tiered serving (cold+hot) must equal an
--    independent per-minute explosion from session_intervals (distinct
--    sessions per minute). Expect ZERO mismatched rows.
--    NOTE: this only checks the expand+aggregate step — session_intervals is
--    both the reference's and serving's shared input, so it CANNOT catch a
--    bug in the state machine itself. See check A2 below for that.
-- ---------------------------------------------------------------------
WITH
serving AS (
  SELECT minute, sum(concurrent) AS c
  FROM sonyliv_concurrency.concurrency_now GROUP BY minute
),
reference AS (
  SELECT minute, uniqExact(video_session_id) AS c
  FROM (
    SELECT video_session_id,
           -- configurable bucket (00_config.sql): start-of-bucket + N buckets
           toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds()))
             + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
    FROM (
      -- unpack session_intervals' one-row-per-session Array(Tuple(...)) first
      -- (ghost-interval fix, review #7).
      SELECT video_session_id, iv.1 AS active_start, iv.2 AS active_end
      FROM sonyliv_concurrency.session_intervals FINAL
      ARRAY JOIN intervals AS iv
      WHERE iv.2 > iv.1
    )
    ARRAY JOIN range(0, toUInt64(dateDiff('second',
                   toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds())),
                   toStartOfInterval(active_end - INTERVAL 1 MILLISECOND, toIntervalSecond(cfg_bucket_seconds())))
                   / cfg_bucket_seconds()) + 1) AS number
  )
  GROUP BY minute
)
SELECT s.minute, s.c AS serving_c, r.c AS reference_c
FROM serving s FULL JOIN reference r USING (minute)
WHERE s.c != r.c
ORDER BY minute;                 -- <-- zero rows = correct

-- ---------------------------------------------------------------------
-- A2) INDEPENDENT ORACLE — re-derives active intervals straight from
--     events_raw using a structurally different technique from
--     01_schema.sql/03_backfill.sql's window-function pipeline (argMax()
--     OVER, row_number() OVER, leadInFrame() OVER, sum() OVER), so a bug in
--     one implementation is very unlikely to reproduce identically in the
--     other. This is the highest-leverage check in this file — check A above
--     can only validate the expand+aggregate step, not the state machine.
--
--     Technique: per-session sorted arrays + arrayFill() to forward-fill the
--     watching state (stand-in for argMax() OVER) + direct array-index
--     lookahead ts_arr[i+1] (stand-in for leadInFrame() OVER). Segments are
--     NOT merged into islands (unlike the production pipeline) — for a
--     per-minute-membership oracle, "does ANY segment cover minute m" is
--     mathematically equivalent to "does the merged island set cover minute
--     m", so skipping the merge step removes an entire class of shared bugs
--     the two implementations could otherwise accidentally share.
--
--     Reads the SAME 00_config.sql knobs (cfg_heartbeat_seconds, cfg_gap_timeout_
--     seconds, cfg_bucket_seconds) and the same deactivate>reactivate>neutral
--     collapse priority as the production state machine, so tuning a knob
--     doesn't make this oracle go stale and report false mismatches. Those
--     knob VALUES are themselves under test elsewhere (see the
--     VideoSessionStart / tuning_variants checks below), not by this oracle.
-- ---------------------------------------------------------------------
WITH
transitions AS (
  SELECT video_session_id AS sid, event_timestamp AS ts,
         multiIf(event_type IN ('VideoPlay','AppForegrounded') OR event IN ('resume','speed-resume','AdResume'), 1,
                 event_type IN ('AppBackgrounded','VideoSessionEnd','VideoError') OR event IN ('pause','speed-pause','AdPause'), -1,
                 0) AS transition
  FROM sonyliv_concurrency.events_raw
),
collapsed AS (
  SELECT sid, ts, if(min(transition) < 0, -1, max(transition)) AS transition
  FROM transitions GROUP BY sid, ts
),
per_session AS (
  SELECT sid,
         arrayMap(p -> p.1, arraySort(x -> x.1, groupArray((ts, transition)))) AS ts_arr,
         arrayMap(p -> p.2, arraySort(x -> x.1, groupArray((ts, transition)))) AS tr_arr
  FROM collapsed
  GROUP BY sid
),
stated AS (
  SELECT sid, ts_arr, arrayFill(x -> x != 0, tr_arr) AS state_arr, length(ts_arr) AS n
  FROM per_session
),
exploded AS (
  SELECT sid, ts_arr, state_arr, n, arrayJoin(arrayEnumerate(ts_arr)) AS i
  FROM stated
),
active_events AS (
  SELECT sid, ts_arr[i] AS seg_start,
         if(i = n, addSeconds(ts_arr[i], cfg_heartbeat_seconds()),
            if(dateDiff('second', ts_arr[i], ts_arr[i+1]) <= cfg_gap_timeout_seconds(), ts_arr[i+1],
               addSeconds(ts_arr[i], cfg_heartbeat_seconds()))) AS seg_end
  FROM exploded
  WHERE state_arr[i] = 1
),
oracle_session_minutes AS (
  SELECT DISTINCT sid AS video_session_id,
         toStartOfInterval(seg_start, toIntervalSecond(cfg_bucket_seconds()))
           + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
  FROM active_events
  ARRAY JOIN range(0, toUInt64(dateDiff('second',
                 toStartOfInterval(seg_start, toIntervalSecond(cfg_bucket_seconds())),
                 toStartOfInterval(seg_end - INTERVAL 1 MILLISECOND, toIntervalSecond(cfg_bucket_seconds())))
                 / cfg_bucket_seconds()) + 1) AS number
  WHERE seg_end > seg_start
),
reference_session_minutes AS (
  SELECT DISTINCT video_session_id,
         toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds()))
           + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
  FROM (
    SELECT video_session_id, iv.1 AS active_start, iv.2 AS active_end
    FROM sonyliv_concurrency.session_intervals FINAL
    ARRAY JOIN intervals AS iv
    WHERE iv.2 > iv.1
  )
  ARRAY JOIN range(0, toUInt64(dateDiff('second',
                 toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds())),
                 toStartOfInterval(active_end - INTERVAL 1 MILLISECOND, toIntervalSecond(cfg_bucket_seconds())))
                 / cfg_bucket_seconds()) + 1) AS number
)
SELECT
  (SELECT count() FROM oracle_session_minutes AS o
     LEFT ANTI JOIN reference_session_minutes AS r
     ON o.video_session_id = r.video_session_id AND o.minute = r.minute) AS oracle_only_count,
  (SELECT count() FROM reference_session_minutes AS r
     LEFT ANTI JOIN oracle_session_minutes AS o
     ON r.video_session_id = o.video_session_id AND r.minute = o.minute) AS reference_only_count;
  -- both 0 = correct (Nirad's bar: N/N identical, interval-by-interval)

-- ---------------------------------------------------------------------
-- B) PER-SESSION DEDUPE probe: a session's own array can still list >1
--    interval landing in the same minute; uniqExact upstream handles the
--    once-per-minute dedupe regardless. List any for a sanity spot check.
-- ---------------------------------------------------------------------
SELECT video_session_id, toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds())) AS minute, count() AS intervals_in_minute
FROM (
  SELECT video_session_id, iv.1 AS active_start
  FROM sonyliv_concurrency.session_intervals FINAL
  ARRAY JOIN intervals AS iv
)
GROUP BY video_session_id, minute
HAVING intervals_in_minute > 1
ORDER BY intervals_in_minute DESC LIMIT 20;

-- ---------------------------------------------------------------------
-- E) VideoSessionStart lead-in check: VideoSessionStart is currently neutral
--    (transition=0) in the state machine, so any time between a session's
--    VideoSessionStart and its first VideoPlay/resume is dropped as inactive.
--    If a meaningful fraction of sessions heartbeat/lead-in before their
--    first VideoPlay, VideoSessionStart likely belongs in the +1 branch
--    instead (verify against the real dataset before changing production).
-- ---------------------------------------------------------------------
SELECT
  count() AS sessions_with_start_and_play,
  countIf(gap_seconds > 0) AS sessions_with_lead_in,
  round(100.0 * countIf(gap_seconds > 0) / count(), 1) AS pct_with_lead_in,
  round(avgIf(gap_seconds, gap_seconds > 0), 1) AS avg_lead_in_seconds
FROM (
  SELECT video_session_id,
         dateDiff('second',
           minIf(event_timestamp, event_type = 'VideoSessionStart'),
           minIf(event_timestamp, event_type = 'VideoPlay' OR event IN ('resume','speed-resume','AdResume'))
         ) AS gap_seconds
  FROM sonyliv_concurrency.events_raw
  GROUP BY video_session_id
  HAVING countIf(event_type = 'VideoSessionStart') > 0
     AND countIf(event_type = 'VideoPlay' OR event IN ('resume','speed-resume','AdResume')) > 0
);

-- ---------------------------------------------------------------------
-- C) PAUSE-CORRECTNESS: our foreground-only count vs the NAIVE
--    "heartbeat-present in minute" rule (PLAN2). Naive counts paused
--    time as active -> it should be consistently HIGHER. The gap is the
--    overcount we avoid. (Total + a few worst minutes.)
-- ---------------------------------------------------------------------
WITH
ours AS (
  SELECT minute, sum(concurrent) AS c FROM sonyliv_concurrency.concurrency_now GROUP BY minute
),
naive AS (   -- "active iff any heartbeat in the minute" (ignores pause)
  SELECT toStartOfInterval(event_timestamp, toIntervalSecond(cfg_bucket_seconds())) AS minute,
         uniqExact(video_session_id) AS c
  FROM sonyliv_concurrency.events_raw
  WHERE event_type = 'VideoHeartbeat'
  GROUP BY minute
)
SELECT
  sum(n.c) AS naive_total_session_minutes,
  sum(o.c) AS ours_total_session_minutes,
  sum(n.c) - sum(o.c) AS overcount_avoided,
  round(100.0*(sum(n.c)-sum(o.c))/sum(n.c), 1) AS pct_overcount_avoided
FROM naive n LEFT JOIN ours o USING (minute);

-- ---------------------------------------------------------------------
-- D) Cold/Hot split sanity: tiers must be disjoint by minute.
-- ---------------------------------------------------------------------
SELECT
  (SELECT max(minute) FROM sonyliv_concurrency.concurrency_cold_abs) AS cold_max_minute,
  (SELECT min(minute) FROM sonyliv_concurrency.concurrency_hot_abs)  AS hot_min_minute,
  (SELECT count() FROM (
      SELECT minute FROM sonyliv_concurrency.concurrency_cold_abs
      INTERSECT
      SELECT minute FROM sonyliv_concurrency.concurrency_hot_abs)) AS overlapping_minutes;  -- expect 0

-- ---------------------------------------------------------------------
-- E) USER-CONCURRENCY == BRUTE FORCE, at the FULL CELL grain. Distinct-user
--    counts are NOT summable across dims (a user can span content/dims — see
--    concurrency_cold_abs comment), so we check per (dims, minute) CELL, where
--    no cross-dim summation happens: serving concurrent_users must equal an
--    independent per-cell uniqExact(user_id). Expect ZERO mismatched rows.
-- ---------------------------------------------------------------------
WITH
serving AS (
  SELECT country, platform, video_type, category, minute, content_id, concurrent_users AS u
  FROM sonyliv_concurrency.concurrency_now
),
reference AS (
  SELECT country, platform, video_type, category, minute, content_id,
         uniqExact(user_id) AS u
  FROM (
    SELECT user_id, country, platform, video_type, category, content_id,
           toStartOfInterval(active_start, toIntervalSecond(cfg_bucket_seconds()))
             + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
    FROM (
      -- unpack session_intervals' one-row-per-session Array(Tuple(...)) first.
      SELECT user_id, country, platform, video_type, category, content_id,
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
  GROUP BY country, platform, video_type, category, minute, content_id
)
SELECT s.minute, s.content_id, s.u AS serving_users, r.u AS reference_users
FROM serving s FULL JOIN reference r
  USING (country, platform, video_type, category, minute, content_id)
WHERE coalesce(s.u, 0) != coalesce(r.u, 0)
ORDER BY minute, content_id
LIMIT 100;                        -- <-- zero rows = user concurrency correct

-- ---------------------------------------------------------------------
-- F) SESSION-START SEEDING (GAP_ANALYSIS #2a). A session activated ONLY by
--    VideoSessionStart (has a start + heartbeats but NO explicit VideoPlay/
--    AppForegrounded/resume) must still produce an active interval. Under the
--    OLD logic (inactive until the first explicit +1) these were dropped to
--    ZERO. Scoped to the MV's 20-min recency window so it matches what
--    session_intervals currently holds. `missing_expect_0` = 0 confirms the fix.
--    (Reads session_intervals + events_raw directly — independent of hot/cold.)
-- ---------------------------------------------------------------------
WITH
recent AS (
  SELECT video_session_id FROM sonyliv_concurrency.events_raw
  GROUP BY video_session_id HAVING max(event_timestamp) >= now() - INTERVAL 20 MINUTE ),
start_only AS (   -- start + heartbeat, but no explicit activation event
  SELECT video_session_id FROM sonyliv_concurrency.events_raw
  WHERE video_session_id IN (SELECT video_session_id FROM recent)
  GROUP BY video_session_id
  HAVING sum(event_type = 'VideoSessionStart') > 0
     AND sum(event_type = 'VideoHeartbeat')    > 0
     AND sum((event_type IN ('VideoPlay','AppForegrounded'))
             OR (event IN ('resume','speed-resume','AdResume'))) = 0 ),
have_interval AS (
  SELECT DISTINCT video_session_id FROM sonyliv_concurrency.session_intervals FINAL )
SELECT
  (SELECT count() FROM start_only)                                                                       AS start_only_sessions,
  (SELECT count() FROM start_only WHERE video_session_id IN     (SELECT video_session_id FROM have_interval)) AS with_active_interval,
  (SELECT count() FROM start_only WHERE video_session_id NOT IN (SELECT video_session_id FROM have_interval)) AS missing_expect_0;

-- F-sample: a few start-only sessions, showing the interval begins at the
-- session's first event (VideoSessionStart) — i.e. the start seeded activity.
WITH
recent AS (
  SELECT video_session_id FROM sonyliv_concurrency.events_raw
  GROUP BY video_session_id HAVING max(event_timestamp) >= now() - INTERVAL 20 MINUTE ),
start_only AS (
  SELECT video_session_id FROM sonyliv_concurrency.events_raw
  WHERE video_session_id IN (SELECT video_session_id FROM recent)
  GROUP BY video_session_id
  HAVING sum(event_type = 'VideoSessionStart') > 0
     AND sum(event_type = 'VideoHeartbeat')    > 0
     AND sum((event_type IN ('VideoPlay','AppForegrounded'))
             OR (event IN ('resume','speed-resume','AdResume'))) = 0 ),
first_ev AS (
  SELECT video_session_id, min(event_timestamp) AS session_start_ts
  FROM sonyliv_concurrency.events_raw GROUP BY video_session_id ),
ivl AS (
  -- unpack the Array(Tuple(...)) so count() is the true interval count and
  -- min/max span the session (FINAL has no plain active_start/active_end).
  SELECT video_session_id, min(iv.1) AS active_start, max(iv.2) AS active_end, count() AS intervals
  FROM sonyliv_concurrency.session_intervals FINAL
  ARRAY JOIN intervals AS iv
  GROUP BY video_session_id )
SELECT so.video_session_id, f.session_start_ts, i.active_start, i.active_end, i.intervals
FROM start_only so
INNER JOIN first_ev f USING (video_session_id)
INNER JOIN ivl      i USING (video_session_id)
ORDER BY f.session_start_ts DESC
LIMIT 10;

-- ---------------------------------------------------------------------
-- G) PAUSE EXCLUSION (GAP_ANALYSIS #2). Invariant: no instant carrying a
--    DEACTIVATED foreground state (paused / backgrounded / errored / ended —
--    including a heartbeat that lands during a pause) may fall INSIDE an active
--    interval. We recompute per-event carried state with the SAME classifier as
--    the state machine, take the instants where state = -1, and count how many
--    lie within [active_start, active_end) of the same session.
--    `inside_active_interval_expect_0` = 0 confirms paused time is excluded.
-- ---------------------------------------------------------------------
WITH
recent AS (
  SELECT video_session_id FROM sonyliv_concurrency.events_raw
  GROUP BY video_session_id HAVING max(event_timestamp) >= now() - INTERVAL 20 MINUTE ),
per_event AS (
  SELECT video_session_id AS sid, event_timestamp AS ts,
    multiIf((event_type IN ('VideoSessionStart','VideoPlay','AppForegrounded'))
              OR (event IN ('resume','speed-resume','AdResume')), 1,
            (event_type IN ('AppBackgrounded','VideoSessionEnd','VideoError','VideoPause','AdBreakStart'))
              OR (event IN ('pause','speed-pause','AdPause')), -1,
            0) AS transition
  FROM sonyliv_concurrency.events_raw
  WHERE video_session_id IN (SELECT video_session_id FROM recent) ),
collapsed AS (
  SELECT sid, ts, if(min(transition) < 0, toInt8(-1), toInt8(max(transition))) AS transition
  FROM per_event GROUP BY sid, ts ),
stated AS (
  SELECT sid, ts,
    argMax(transition, if(transition!=0, ts, toDateTime64('1970-01-01 00:00:00',3,'UTC')))
      OVER (PARTITION BY sid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS state_sign
  FROM collapsed ),
paused AS ( SELECT sid, ts FROM stated WHERE state_sign = -1 ),
ivl AS ( SELECT video_session_id AS sid, iv.1 AS active_start, iv.2 AS active_end
         FROM sonyliv_concurrency.session_intervals FINAL
         ARRAY JOIN intervals AS iv )
SELECT
  (SELECT count()        FROM paused) AS deactivated_instants,
  (SELECT uniqExact(sid) FROM paused) AS sessions_with_paused_state,
  count()                             AS inside_active_interval_expect_0
FROM paused p
INNER JOIN ivl i ON i.sid = p.sid
WHERE i.active_start <= p.ts AND p.ts < i.active_end;

-- G-sample: sessions that BOTH paused and have active intervals (the scenario
-- is exercised); the summary above proves none of the paused instants are
-- counted as active.
WITH
recent AS (
  SELECT video_session_id FROM sonyliv_concurrency.events_raw
  GROUP BY video_session_id HAVING max(event_timestamp) >= now() - INTERVAL 20 MINUTE ),
per_event AS (
  SELECT video_session_id AS sid, event_timestamp AS ts,
    multiIf((event_type IN ('VideoSessionStart','VideoPlay','AppForegrounded'))
              OR (event IN ('resume','speed-resume','AdResume')), 1,
            (event_type IN ('AppBackgrounded','VideoSessionEnd','VideoError','VideoPause','AdBreakStart'))
              OR (event IN ('pause','speed-pause','AdPause')), -1,
            0) AS transition
  FROM sonyliv_concurrency.events_raw
  WHERE video_session_id IN (SELECT video_session_id FROM recent) ),
collapsed AS (
  SELECT sid, ts, if(min(transition) < 0, toInt8(-1), toInt8(max(transition))) AS transition
  FROM per_event GROUP BY sid, ts ),
stated AS (
  SELECT sid, ts,
    argMax(transition, if(transition!=0, ts, toDateTime64('1970-01-01 00:00:00',3,'UTC')))
      OVER (PARTITION BY sid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS state_sign
  FROM collapsed ),
pcnt AS ( SELECT sid, count() AS paused_instants FROM stated WHERE state_sign = -1 GROUP BY sid ),
icnt AS ( SELECT video_session_id AS sid, count() AS active_intervals,
                 min(iv.1) AS first_active, max(iv.2) AS last_active
          FROM sonyliv_concurrency.session_intervals FINAL
          ARRAY JOIN intervals AS iv GROUP BY sid )
SELECT p.sid, p.paused_instants, i.active_intervals, i.first_active, i.last_active
FROM pcnt p INNER JOIN icnt i USING (sid)
ORDER BY p.paused_instants DESC
LIMIT 10;
