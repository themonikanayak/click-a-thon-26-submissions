-- [READ — validate] step 05 of the offline pipeline. Asserts the approaches agree; no writes.
-- #####################################################################
-- 05_compare.sql — SESSION-AWARE vs SESSION-INDEPENDENT.
--
-- The problem statement (and README_START_HERE) require BOTH a session-aware
-- and a session-independent concurrency view, and a COMPARISON to validate
-- accuracy and argue trade-offs. This file is that comparison.
--
--   session-aware        -> concurrency_sa_abs   (04_approaches.sql)
--                           from session_intervals (state machine merges active
--                           segments into per-session islands, then expands).
--   session-independent  -> concurrency_si_abs   (04_approaches.sql)
--                           per-event foreground state expanded directly to
--                           minutes, NO per-session interval reconstruction.
--
-- Both share ONE active definition (90s gap / 60s grace, pause/bg/error cut) and
-- both dedupe to distinct sessions per (dims, minute), so they MUST agree.
--
-- PREREQ run order:
--   01_schema.sql -> (load events) -> 03_backfill.sql
--   -> 04_approaches.sql -> 04_approaches.sql -> this file.
-- Expect ZERO mismatched rows in checks A/B/C.
-- #####################################################################

-- ---------------------------------------------------------------------
-- A) FULL-DIMENSION EQUALITY: the two aggregate tables must match row-for-row
--    on every (dims, minute) cell. FULL JOIN surfaces cells present in one but
--    not the other (as NULL -> 0) as well as value disagreements.
-- ---------------------------------------------------------------------
SELECT
    coalesce(sa.country, si.country)         AS country,
    coalesce(sa.platform, si.platform)       AS platform,
    coalesce(sa.video_type, si.video_type)   AS video_type,
    coalesce(sa.category, si.category)       AS category,
    coalesce(sa.minute, si.minute)           AS minute,
    coalesce(sa.content_id, si.content_id)   AS content_id,
    sa.concurrent       AS session_aware,        si.concurrent       AS session_independent,
    sa.concurrent_users AS session_aware_users,  si.concurrent_users AS session_independent_users
FROM sonyliv_concurrency.concurrency_sa_abs sa
FULL JOIN sonyliv_concurrency.concurrency_si_abs si
  USING (country, platform, video_type, category, minute, content_id)
-- both the session count AND the user count must agree per cell:
WHERE coalesce(sa.concurrent, 0)       != coalesce(si.concurrent, 0)
   OR coalesce(sa.concurrent_users, 0) != coalesce(si.concurrent_users, 0)
ORDER BY minute, country, platform
LIMIT 100;                        -- <-- ZERO rows = the two approaches agree exactly

-- ---------------------------------------------------------------------
-- B) PER-MINUTE TOTALS, three-way: session-aware vs session-independent vs the
--    live tiered serving (concurrency_now). All three curves must coincide.
--    (concurrency_now is cold∪hot from 01_schema.sql; sa/si are single full-history
--    tables — totals per minute are the common denominator.)
-- ---------------------------------------------------------------------
WITH
sa  AS (SELECT minute, sum(concurrent) AS c FROM sonyliv_concurrency.concurrency_sa_abs GROUP BY minute),
si  AS (SELECT minute, sum(concurrent) AS c FROM sonyliv_concurrency.concurrency_si_abs GROUP BY minute),
now AS (SELECT minute, sum(concurrent) AS c FROM sonyliv_concurrency.concurrency_now    GROUP BY minute)
SELECT
    coalesce(sa.minute, si.minute, now.minute) AS minute,
    sa.c  AS session_aware,
    si.c  AS session_independent,
    now.c AS serving_now
FROM sa
FULL JOIN si  USING (minute)
FULL JOIN now USING (minute)
WHERE coalesce(sa.c, 0) != coalesce(si.c, 0)
   OR coalesce(sa.c, 0) != coalesce(now.c, 0)
ORDER BY minute
LIMIT 100;                        -- <-- ZERO rows = all three curves coincide

-- ---------------------------------------------------------------------
-- C) DIMENSION-FILTERED spot check: peak concurrency per filter combo must be
--    identical across approaches (the peak minute can differ per combo — the
--    exact non-additivity the problem calls out — so we compare per combo).
-- ---------------------------------------------------------------------
WITH
sa AS (
  SELECT country, platform, max(c) AS peak, argMax(minute, c) AS peak_minute
  FROM (SELECT country, platform, minute, sum(concurrent) AS c
        FROM sonyliv_concurrency.concurrency_sa_abs GROUP BY country, platform, minute)
  GROUP BY country, platform ),
si AS (
  SELECT country, platform, max(c) AS peak, argMax(minute, c) AS peak_minute
  FROM (SELECT country, platform, minute, sum(concurrent) AS c
        FROM sonyliv_concurrency.concurrency_si_abs GROUP BY country, platform, minute)
  GROUP BY country, platform )
SELECT sa.country, sa.platform,
       sa.peak AS sa_peak, si.peak AS si_peak,
       sa.peak_minute AS sa_peak_minute, si.peak_minute AS si_peak_minute
FROM sa FULL JOIN si USING (country, platform)
WHERE coalesce(sa.peak, 0) != coalesce(si.peak, 0)
ORDER BY country, platform
LIMIT 100;                        -- <-- ZERO rows = per-combo peaks agree

-- ---------------------------------------------------------------------
-- D) SUMMARY: one-line scorecard. mismatches_* must all be 0; totals equal.
-- ---------------------------------------------------------------------
SELECT
  (SELECT count() FROM sonyliv_concurrency.concurrency_sa_abs) AS sa_rows,
  (SELECT count() FROM sonyliv_concurrency.concurrency_si_abs) AS si_rows,
  (SELECT sum(concurrent)       FROM sonyliv_concurrency.concurrency_sa_abs) AS sa_session_minutes,
  (SELECT sum(concurrent)       FROM sonyliv_concurrency.concurrency_si_abs) AS si_session_minutes,
  (SELECT sum(concurrent_users) FROM sonyliv_concurrency.concurrency_sa_abs) AS sa_user_minutes,
  (SELECT sum(concurrent_users) FROM sonyliv_concurrency.concurrency_si_abs) AS si_user_minutes,
  (SELECT count() FROM (
      SELECT country, platform, video_type, category, minute, content_id
      FROM sonyliv_concurrency.concurrency_sa_abs
      FULL JOIN sonyliv_concurrency.concurrency_si_abs
        USING (country, platform, video_type, category, minute, content_id)
      WHERE coalesce(concurrency_sa_abs.concurrent, 0)
         != coalesce(concurrency_si_abs.concurrent, 0)
         OR coalesce(concurrency_sa_abs.concurrent_users, 0)
         != coalesce(concurrency_si_abs.concurrent_users, 0)
  )) AS cell_mismatches;          -- <-- expect 0 (sessions AND users)

-- =====================================================================
-- EXTENDED ROLL-UP CROSS-CHECK (moved from 04_approaches.sql §C)
-- collapsing the extended table over the 4 drill-down dims MUST reproduce
-- the core session-aware counts exactly: any() gives each session a single
-- extended tuple, so summing the sub-cells telescopes back to the core
-- distinct-session count. Expect ZERO mismatched rows.
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
-- SMOKE COUNTS / PEAKS (moved from 04_approaches.sql §C,
-- 04_approaches.sql §D, 04_approaches.sql §D)
-- =====================================================================
SELECT count() AS rows FROM sonyliv_concurrency.concurrency_sa_abs;

SELECT max(c) AS peak_concurrency, argMax(minute, c) AS peak_minute
FROM (SELECT minute, sum(concurrent) AS c FROM sonyliv_concurrency.concurrency_sa_abs GROUP BY minute);

SELECT count() AS rows FROM sonyliv_concurrency.concurrency_si_abs;

SELECT max(c) AS peak_concurrency, argMax(minute, c) AS peak_minute
FROM (SELECT minute, sum(concurrent) AS c FROM sonyliv_concurrency.concurrency_si_abs GROUP BY minute);

SELECT count() AS rows FROM sonyliv_concurrency.concurrency_ext_abs;

-- peak concurrency for a drill-down combo (example: Android phone + Hindi audio)
SELECT max(c) AS peak_concurrency, argMax(minute, c) AS peak_minute
FROM (
    SELECT minute, sum(concurrent) AS c
    FROM sonyliv_concurrency.concurrency_ext_abs
    WHERE platform = 'ANDROID_PHONE' AND audio_language = 'hin'
    GROUP BY minute
);
