-- #####################################################################
-- compare_approaches.sql — SESSION-AWARE vs SESSION-INDEPENDENT.
--
-- The problem statement (and README_START_HERE) require BOTH a session-aware
-- and a session-independent concurrency view, and a COMPARISON to validate
-- accuracy and argue trade-offs. This file is that comparison.
--
--   session-aware        -> concurrency_sa_abs   (approach_session_aware.sql)
--                           from session_intervals (state machine merges active
--                           segments into per-session islands, then expands).
--   session-independent  -> concurrency_si_abs   (approach_session_independent.sql)
--                           per-event foreground state expanded directly to
--                           minutes, NO per-session interval reconstruction.
--
-- Both share ONE active definition (90s gap / 60s grace, pause/bg/error cut) and
-- both dedupe to distinct sessions per (dims, minute), so they MUST agree.
--
-- PREREQ run order:
--   schema.sql -> (load events) -> backfill_history.sql
--   -> approach_session_aware.sql -> approach_session_independent.sql -> this file.
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
    sa.concurrent AS session_aware,
    si.concurrent AS session_independent
FROM sonyliv_concurrency.concurrency_sa_abs sa
FULL JOIN sonyliv_concurrency.concurrency_si_abs si
  USING (country, platform, video_type, category, minute, content_id)
WHERE coalesce(sa.concurrent, 0) != coalesce(si.concurrent, 0)
ORDER BY minute, country, platform
LIMIT 100;                        -- <-- ZERO rows = the two approaches agree exactly

-- ---------------------------------------------------------------------
-- B) PER-MINUTE TOTALS, three-way: session-aware vs session-independent vs the
--    live tiered serving (concurrency_now). All three curves must coincide.
--    (concurrency_now is cold∪hot from schema.sql; sa/si are single full-history
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
  (SELECT sum(concurrent) FROM sonyliv_concurrency.concurrency_sa_abs) AS sa_session_minutes,
  (SELECT sum(concurrent) FROM sonyliv_concurrency.concurrency_si_abs) AS si_session_minutes,
  (SELECT count() FROM (
      SELECT country, platform, video_type, category, minute, content_id
      FROM sonyliv_concurrency.concurrency_sa_abs
      FULL JOIN sonyliv_concurrency.concurrency_si_abs
        USING (country, platform, video_type, category, minute, content_id)
      WHERE coalesce(concurrency_sa_abs.concurrent, 0)
         != coalesce(concurrency_si_abs.concurrent, 0)
  )) AS cell_mismatches;          -- <-- expect 0
