#!/usr/bin/env python3
"""
benchmark.py — CORRECTNESS harness for the Snorlax concurrency serving layer.

The judges score us on one thing above all: do our reported concurrency numbers
match the ground truth? This script answers that WITHOUT the private answer key,
by computing an INDEPENDENT ground truth straight from the raw event table and
checking the serving layer against it, question by question.

For each benchmark question (see BENCHMARK_QUERIES.md):
  * REFERENCE ("actual")  — computed from sonyliv_concurrency.events_raw ONLY,
    reconstructing foreground-only active minutes from scratch. It reads NO
    pipeline output (no session_intervals, no concurrency_* tables), so it is a
    genuine oracle, not the system checking itself.
  * SERVING  ("reported") — the number a dashboard would actually read, from the
    materialized serving layer (concurrency_now = cold ∪ hot, or the extended
    drill-down table concurrency_ext_abs).
The two are computed by the SAME outer query with only the source swapped, so a
disagreement is a real correctness defect, not a query artifact. Both sides call
the same config UDFs (cfg_bucket_seconds / cfg_gap_timeout_seconds / ...), so the
reference can never drift from the pipeline's tunable knobs.

Ground-truth active definition (identical to 01_schema.sql D2 / backfill /
04_approaches.sql): a session is foreground-active from its start
until a pause / background / error / end; heartbeats extend activity within the
gap timeout; paused and backgrounded time is EXCLUDED.

Connection comes from the SAME env as the producer (Snorlax/producer/.env),
exactly like migrations/run_sql.py:
  CLICKHOUSE_HOST      (required)
  CLICKHOUSE_PASSWORD  (required)
  CLICKHOUSE_PORT      (default 8443)
  CLICKHOUSE_USER      (default 'default')
  CLICKHOUSE_SECURE    (default 'true')
  CLICKHOUSE_DATABASE  (default 'sonyliv_concurrency')

Usage:
    python benchmark.py                 # run all checks, print a PASS/FAIL table
    python benchmark.py --grace-seconds 120
                                        # ignore the last 120s on BOTH sides (skip
                                        # the still-provisional hot edge on live data)
    python benchmark.py --since 2026-08-01T12:00:00 --until 2026-08-01T13:00:00
                                        # restrict the compared window (both sides)
    python benchmark.py --samples 30    # show up to N sample mismatched cells
    python benchmark.py --no-extended   # skip the extended drill-down checks

Exit code = number of FAILED checks (0 = everything correct), so it drops into CI
or a pre-submission gate. WARN (e.g. a tie-broken peak minute) does not fail.

NOTE: run this AFTER the pipeline has fully caught up with events_raw (offline
backfill, or `--grace-seconds` past p99 ingest lag on live data). A settled
serving layer must match the reference to the row; the reconciliation check
(B0) is the definitive verdict.
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import clickhouse_connect
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths & env — mirror migrations/run_sql.py so credentials come from one place.
# ---------------------------------------------------------------------------
HERE = Path(__file__).resolve().parent          # .../Snorlax/benchmark
SNORLAX = HERE.parent                            # .../Snorlax
PRODUCER_ENV = SNORLAX / "producer" / ".env"

load_dotenv(PRODUCER_ENV)
load_dotenv()

HOST = os.environ["CLICKHOUSE_HOST"]
PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
PORT = int(os.getenv("CLICKHOUSE_PORT", "8443"))
USER = os.getenv("CLICKHOUSE_USER", "default")
SECURE = os.getenv("CLICKHOUSE_SECURE", "true").lower() in ("1", "true", "yes")
DATABASE = os.getenv("CLICKHOUSE_DATABASE", "sonyliv_concurrency")

DB = "sonyliv_concurrency"
AVG_TOL = 1e-6          # both sides use the identical avg formula → expect exact
SERVING_CORE = f"{DB}.concurrency_now"
SERVING_EXT = f"{DB}.concurrency_ext_abs"

# ===========================================================================
# REFERENCE ("actual") — foreground-only concurrency computed from events_raw
# ALONE. This is a verbatim reuse of the active definition in
# 04_approaches.sql (per_event → collapsed → stated → segments),
# expanded to buckets. It touches no pipeline output, so it is an independent
# oracle for the serving layer. Config UDFs keep it in lock-step with the knobs.
# ===========================================================================

# One row per (session, user, dims, content_id, active-bucket) — the atoms every
# concurrency count is a distinct-count over.
_ACTIVE_MINUTES = f"""
(
  WITH
  per_event AS (
    SELECT video_session_id AS sid, user_id, event_timestamp AS ts, content_id, platform, country,
      multiIf(event_type IN ('VideoSessionStart','VideoPlay','AppForegrounded')
                OR event IN ('resume','speed-resume','AdResume'), 1,
              event_type IN ('AppBackgrounded','VideoSessionEnd','VideoError','VideoPause','AdBreakStart')
                OR event IN ('pause','speed-pause','AdPause'), -1,
              0) AS transition
    FROM {DB}.events_raw
  ),
  collapsed AS (
    SELECT sid, ts, if(min(transition) < 0, toInt8(-1), toInt8(max(transition))) AS transition,
           any(user_id) AS user_id, any(content_id) AS content_id,
           any(platform) AS platform, any(country) AS country
    FROM per_event GROUP BY sid, ts
  ),
  stated AS (
    SELECT sid, ts, user_id, content_id, platform, country,
      argMax(transition, if(transition!=0, ts, toDateTime64('1970-01-01 00:00:00',3,'UTC')))
        OVER (PARTITION BY sid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS state_sign,
      row_number() OVER (PARTITION BY sid ORDER BY ts) AS rn,
      count()      OVER (PARTITION BY sid)             AS n,
      leadInFrame(ts) OVER (PARTITION BY sid ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS next_ts
    FROM collapsed
  ),
  segments AS (
    SELECT sid, user_id, content_id, platform, country, ts AS seg_start,
      multiIf(rn=n, addSeconds(ts, cfg_heartbeat_seconds()),
              dateDiff('second', ts, next_ts) <= cfg_gap_timeout_seconds(), next_ts,
              addSeconds(ts, cfg_heartbeat_seconds())) AS seg_end
    FROM stated WHERE state_sign = 1
  )
  SELECT sid AS video_session_id, user_id, country, platform,
         dictGet('{DB}.content_dict','video_type', content_id) AS video_type,
         dictGet('{DB}.content_dict','category',   content_id) AS category,
         content_id,
         toStartOfInterval(seg_start, toIntervalSecond(cfg_bucket_seconds()))
           + toIntervalSecond(number * cfg_bucket_seconds()) AS minute
  FROM segments
  ARRAY JOIN range(0, toUInt64(dateDiff('second',
                 toStartOfInterval(seg_start, toIntervalSecond(cfg_bucket_seconds())),
                 toStartOfInterval(seg_end - INTERVAL 1 MILLISECOND, toIntervalSecond(cfg_bucket_seconds())))
                 / cfg_bucket_seconds()) + 1) AS number
  WHERE seg_end > seg_start
)
"""

# Aggregated to the SAME grain/columns as concurrency_now.
REF_CORE = f"""
(
  SELECT country, platform, video_type, category, minute, content_id,
         toUInt32(uniqExact(video_session_id)) AS concurrent,
         toUInt32(uniqExact(user_id))          AS concurrent_users
  FROM {_ACTIVE_MINUTES}
  GROUP BY country, platform, video_type, category, minute, content_id
)
"""

# Extended reference: enrich each active bucket with the session's 4 drill-down
# dims (any() per session, exactly as 04_approaches.sql does) + title,
# then aggregate to the extended key. Matches concurrency_ext_abs cell-for-cell.
REF_EXT = f"""
(
  WITH per_session_dims AS (
    SELECT video_session_id,
           any(app_version)       AS app_version,
           any(player_version)    AS player_version,
           any(audio_language)    AS audio_language,
           any(subtitle_language) AS subtitle_language
    FROM {DB}.events_raw
    GROUP BY video_session_id
  )
  SELECT m.country, m.platform, m.video_type, m.category,
         d.subtitle_language, d.audio_language, d.player_version, d.app_version,
         m.minute, m.content_id,
         dictGet('{DB}.content_dict','title', m.content_id) AS title,
         toUInt32(uniqExact(m.video_session_id)) AS concurrent,
         toUInt32(uniqExact(m.user_id))          AS concurrent_users
  FROM {_ACTIVE_MINUTES} AS m
  INNER JOIN per_session_dims AS d ON m.video_session_id = d.video_session_id
  GROUP BY m.country, m.platform, m.video_type, m.category,
           d.subtitle_language, d.audio_language, d.player_version, d.app_version,
           m.minute, m.content_id, title
)
"""


# ===========================================================================
# Source wrapping — apply the shared time window to BOTH sides identically, so
# reference and serving always compare over the exact same minute domain.
# ===========================================================================
def _time_predicate(since: str | None, until: str | None, grace: int) -> str:
    """Build a `minute` predicate shared by reference and serving (or '')."""
    preds: list[str] = []
    if since:
        preds.append(f"minute >= parseDateTimeBestEffort('{since}', 'UTC')")
    if until:
        preds.append(f"minute <= parseDateTimeBestEffort('{until}', 'UTC')")
    if grace > 0:
        preds.append(f"minute <= now() - toIntervalSecond({grace})")
    return " AND ".join(preds)


def _wrap(src: str, tp: str) -> str:
    """Wrap a source table/subquery with the shared time predicate."""
    return f"(SELECT * FROM {src} WHERE {tp})" if tp else src


# ===========================================================================
# Query helpers
# ===========================================================================
def q_one(client, sql: str) -> tuple:
    """Run a query expected to return a single row; return it (or an empty tuple)."""
    rows = client.query(sql).result_rows
    return tuple(rows[0]) if rows else ()


def q_all(client, sql: str) -> list[tuple]:
    """Run a query returning many rows; return them as a list of tuples."""
    return [tuple(r) for r in client.query(sql).result_rows]


# ===========================================================================
# Check model
# ===========================================================================
@dataclass
class Result:
    """Outcome of one benchmark check."""
    cid: str
    title: str
    status: str          # PASS | FAIL | WARN | SKIP
    detail: str
    warns: list[str] = field(default_factory=list)


@dataclass
class Check:
    """A single benchmark question and how to verify it (actual vs reported)."""
    cid: str
    title: str
    run: Callable[["Ctx"], Result]


@dataclass
class Ctx:
    """Everything a check needs at run time."""
    client: object
    tp: str              # shared time predicate ('' = whole dataset)
    samples: int
    ref_core: str
    ref_ext: str
    serving_core: str
    serving_ext: str


# --- reusable SQL templates (source-swappable) -----------------------------
def _tpl_peakavg(src: str, metric: str, where: str = "") -> str:
    """Peak + peak-bucket + range-average of `metric` at bucket grain over `src`.

    avg = sum / (#buckets in [min,max]) with empty buckets counted as 0, matching
    ui_queries.sql KPI tiles. Both sides use this identical formula.
    """
    w = f"WHERE {where}" if where else ""
    return f"""
    SELECT toUInt64(max(c)) AS peak,
           toString(argMax(minute, c)) AS peak_minute,
           round(sum(c) / (dateDiff('second', min(minute), max(minute))
                 / cfg_bucket_seconds() + 1), 6) AS avg_over_range
    FROM (SELECT minute, sum({metric}) AS c FROM {src} {w} GROUP BY minute)
    """


def _tpl_breakdown(src: str, keyexpr: str, metric: str = "concurrent") -> str:
    """Per-key peak of `metric` (each key finds its OWN peak bucket)."""
    return f"""
    SELECT toString(k) AS k, toUInt64(max(c)) AS peak, toString(argMax(minute, c)) AS peak_minute
    FROM (SELECT {keyexpr} AS k, minute, sum({metric}) AS c FROM {src} GROUP BY k, minute)
    GROUP BY k ORDER BY k
    """


# --- comparators -----------------------------------------------------------
def _cmp_peakavg(cid: str, title: str, ref_row: tuple, serv_row: tuple) -> Result:
    """Compare a (peak, peak_minute, avg) reference vs serving row."""
    if not ref_row and not serv_row:
        return Result(cid, title, "SKIP", "no data on either side")
    r_peak, r_min, r_avg = ref_row or (None, None, None)
    s_peak, s_min, s_avg = serv_row or (None, None, None)
    warns: list[str] = []
    ok = True
    if r_peak != s_peak:
        ok = False
    if r_avg is None or s_avg is None or abs(float(r_avg) - float(s_avg)) > AVG_TOL:
        ok = False
    if r_min != s_min:
        warns.append(f"peak-bucket differs (actual {r_min} vs reported {s_min}); "
                     "harmless if peak values tie")
    detail = (f"peak actual={r_peak} reported={s_peak} @ {s_min} | "
              f"avg actual={r_avg} reported={s_avg}")
    return Result(cid, title, "PASS" if ok else "FAIL", detail, warns)


def _cmp_breakdown(cid: str, title: str, ref_rows: list[tuple],
                   serv_rows: list[tuple]) -> Result:
    """Compare per-key peaks. Peak value is the hard assertion; peak bucket soft."""
    ref = {k: (peak, mn) for k, peak, mn in ref_rows}
    serv = {k: (peak, mn) for k, peak, mn in serv_rows}
    keys = sorted(set(ref) | set(serv))
    bad: list[str] = []
    warns: list[str] = []
    for k in keys:
        rp = ref.get(k)
        sp = serv.get(k)
        if rp is None:
            bad.append(f"{k}: reported by serving (peak {sp[0]}) but absent from reference")
            continue
        if sp is None:
            bad.append(f"{k}: present in reference (peak {rp[0]}) but missing from serving")
            continue
        if rp[0] != sp[0]:
            bad.append(f"{k}: peak actual={rp[0]} reported={sp[0]}")
        elif rp[1] != sp[1]:
            warns.append(f"{k}: peak-bucket differs ({rp[1]} vs {sp[1]})")
    if not keys:
        return Result(cid, title, "SKIP", "no keys on either side")
    if bad:
        return Result(cid, title, "FAIL",
                      f"{len(bad)}/{len(keys)} keys disagree: " + "; ".join(bad[:6]), warns)
    return Result(cid, title, "PASS", f"{len(keys)} keys agree on peak", warns)


# ===========================================================================
# The checks (see BENCHMARK_QUERIES.md for the question each one answers)
# ===========================================================================
def _chk_reconcile_core(ctx: Ctx) -> Result:
    """B0 — the definitive verdict: every (dims, bucket) cell in the serving
    layer equals the raw-events reference for BOTH sessions and users."""
    ref = _wrap(ctx.ref_core, ctx.tp)
    serv = _wrap(ctx.serving_core, ctx.tp)
    join = f"""
    FROM {ref} AS r
    FULL JOIN {serv} AS s
      USING (country, platform, video_type, category, minute, content_id)
    WHERE coalesce(r.concurrent,0)       != coalesce(s.concurrent,0)
       OR coalesce(r.concurrent_users,0) != coalesce(s.concurrent_users,0)
    """
    n = q_one(ctx.client, f"SELECT count() {join}")
    mismatches = int(n[0]) if n else 0
    if mismatches == 0:
        return Result("B0", "Cell-grain reconciliation (sessions & users)", "PASS",
                      "0 mismatched (dims,bucket) cells")
    samples = q_all(ctx.client, f"""
        SELECT toString(minute) AS minute, content_id, platform,
               coalesce(r.concurrent,0) AS actual_sessions, coalesce(s.concurrent,0) AS reported_sessions,
               coalesce(r.concurrent_users,0) AS actual_users, coalesce(s.concurrent_users,0) AS reported_users
        {join}
        ORDER BY minute, content_id LIMIT {ctx.samples}
    """)
    lines = [f"    {r}" for r in samples]
    return Result("B0", "Cell-grain reconciliation (sessions & users)", "FAIL",
                  f"{mismatches} mismatched cells; first {len(samples)}:\n" + "\n".join(lines))


def _chk_peak_sessions(ctx: Ctx) -> Result:
    """B1 — global peak & average concurrency (sessions), minute grain."""
    ref = q_one(ctx.client, _tpl_peakavg(_wrap(ctx.ref_core, ctx.tp), "concurrent"))
    serv = q_one(ctx.client, _tpl_peakavg(_wrap(ctx.serving_core, ctx.tp), "concurrent"))
    return _cmp_peakavg("B1", "Global peak & avg concurrency (sessions)", ref, serv)


def _chk_peak_users(ctx: Ctx) -> Result:
    """B2 — global peak & average USER concurrency, minute grain."""
    ref = q_one(ctx.client, _tpl_peakavg(_wrap(ctx.ref_core, ctx.tp), "concurrent_users"))
    serv = q_one(ctx.client, _tpl_peakavg(_wrap(ctx.serving_core, ctx.tp), "concurrent_users"))
    return _cmp_peakavg("B2", "Global peak & avg concurrency (users)", ref, serv)


def _chk_by_platform(ctx: Ctx) -> Result:
    """B3 — peak per platform (dimension filter friendliness + own peak minute)."""
    ref = q_all(ctx.client, _tpl_breakdown(_wrap(ctx.ref_core, ctx.tp), "platform"))
    serv = q_all(ctx.client, _tpl_breakdown(_wrap(ctx.serving_core, ctx.tp), "platform"))
    return _cmp_breakdown("B3", "Peak concurrency per platform", ref, serv)


def _chk_by_platform_country(ctx: Ctx) -> Result:
    """B4 — peak per (platform, country): the non-additivity scenario — a combo's
    peak minute can differ from either dimension alone."""
    key = "concat(platform, ' | ', country)"
    ref = q_all(ctx.client, _tpl_breakdown(_wrap(ctx.ref_core, ctx.tp), key))
    serv = q_all(ctx.client, _tpl_breakdown(_wrap(ctx.serving_core, ctx.tp), key))
    return _cmp_breakdown("B4", "Peak concurrency per platform × country", ref, serv)


def _chk_by_content(ctx: Ctx) -> Result:
    """B5 — peak per content_id (top-content leaderboard)."""
    ref = q_all(ctx.client, _tpl_breakdown(_wrap(ctx.ref_core, ctx.tp), "content_id"))
    serv = q_all(ctx.client, _tpl_breakdown(_wrap(ctx.serving_core, ctx.tp), "content_id"))
    return _cmp_breakdown("B5", "Peak concurrency per content", ref, serv)


def _chk_hour_grain(ctx: Ctx) -> Result:
    """B6 — hour-grain peak (max minute within each hour)."""
    ref = q_all(ctx.client, _tpl_breakdown(_wrap(ctx.ref_core, ctx.tp), "toStartOfHour(minute)"))
    serv = q_all(ctx.client, _tpl_breakdown(_wrap(ctx.serving_core, ctx.tp), "toStartOfHour(minute)"))
    return _cmp_breakdown("B6", "Peak concurrency per hour", ref, serv)


def _chk_day_grain(ctx: Ctx) -> Result:
    """B7 — day-grain peak (max minute within each day)."""
    ref = q_all(ctx.client, _tpl_breakdown(_wrap(ctx.ref_core, ctx.tp), "toStartOfDay(minute)"))
    serv = q_all(ctx.client, _tpl_breakdown(_wrap(ctx.serving_core, ctx.tp), "toStartOfDay(minute)"))
    return _cmp_breakdown("B7", "Peak concurrency per day", ref, serv)


def _chk_foreground_only(ctx: Ctx) -> Result:
    """B8 — foreground-only correctness: serving must equal the foreground
    reference (paused/backgrounded excluded) AND be strictly below the naive
    "any heartbeat in the minute" count (which overcounts inactive time)."""
    ref_total = q_one(ctx.client,
                      f"SELECT sum(concurrent) FROM {_wrap(ctx.ref_core, ctx.tp)}")
    serv_total = q_one(ctx.client,
                       f"SELECT sum(concurrent) FROM {_wrap(ctx.serving_core, ctx.tp)}")
    # naive: a session counts in every bucket it has ANY heartbeat in (ignores pause/bg)
    naive_where = ["event_type = 'VideoHeartbeat'"]
    naive_tp = ctx.tp.replace("minute", "bucket") if ctx.tp else ""
    naive_inner = f"""
        SELECT toStartOfInterval(event_timestamp, toIntervalSecond(cfg_bucket_seconds())) AS bucket,
               uniqExact(video_session_id) AS c
        FROM {DB}.events_raw
        WHERE {' AND '.join(naive_where)}
        GROUP BY bucket
    """
    if naive_tp:
        naive_inner = f"SELECT bucket, c FROM ({naive_inner}) WHERE {naive_tp}"
    naive_total = q_one(ctx.client, f"SELECT sum(c) FROM ({naive_inner})")

    r = int(ref_total[0]) if ref_total and ref_total[0] is not None else 0
    s = int(serv_total[0]) if serv_total and serv_total[0] is not None else 0
    nv = int(naive_total[0]) if naive_total and naive_total[0] is not None else 0
    if r == 0 and s == 0:
        return Result("B8", "Foreground-only exclusion", "SKIP", "no data")
    ok = (r == s) and (nv >= s)
    pct = round(100.0 * (nv - s) / nv, 1) if nv else 0.0
    detail = (f"serving session-buckets={s}, reference={r} "
              f"({'match' if r == s else 'MISMATCH'}); "
              f"naive(heartbeat-in-bucket)={nv}, overcount avoided={nv - s} ({pct}%)")
    return Result("B8", "Foreground-only exclusion (vs naive)",
                  "PASS" if ok else "FAIL", detail)


def _chk_reconcile_ext(ctx: Ctx) -> Result:
    """B9 — extended drill-down: concurrency_ext_abs equals the raw-events
    extended reference cell-for-cell (app/player version, audio/subtitle lang)."""
    if not _table_exists(ctx.client, "concurrency_ext_abs"):
        return Result("B9", "Extended drill-down reconciliation", "SKIP",
                      "concurrency_ext_abs not built (run 04_approaches.sql)")
    key = ("country, platform, video_type, category, subtitle_language, "
           "audio_language, player_version, app_version, minute, content_id, title")
    ref = _wrap(ctx.ref_ext, ctx.tp)
    serv = _wrap(ctx.serving_ext, ctx.tp)
    join = f"""
    FROM {ref} AS r
    FULL JOIN {serv} AS s USING ({key})
    WHERE coalesce(r.concurrent,0)       != coalesce(s.concurrent,0)
       OR coalesce(r.concurrent_users,0) != coalesce(s.concurrent_users,0)
    """
    n = q_one(ctx.client, f"SELECT count() {join}")
    mismatches = int(n[0]) if n else 0
    if mismatches == 0:
        return Result("B9", "Extended drill-down reconciliation", "PASS",
                      "0 mismatched extended cells")
    samples = q_all(ctx.client, f"""
        SELECT toString(minute) AS minute, content_id, platform, audio_language, app_version,
               coalesce(r.concurrent,0) AS actual, coalesce(s.concurrent,0) AS reported
        {join} ORDER BY minute, content_id LIMIT {ctx.samples}
    """)
    lines = [f"    {r}" for r in samples]
    return Result("B9", "Extended drill-down reconciliation", "FAIL",
                  f"{mismatches} mismatched extended cells; first {len(samples)}:\n"
                  + "\n".join(lines))


def _chk_ext_rollup(ctx: Ctx) -> Result:
    """B10 — the extended table rolls back up to the core counts exactly
    (collapsing the 4 drill-down dims must reproduce concurrency_now)."""
    if not _table_exists(ctx.client, "concurrency_ext_abs"):
        return Result("B10", "Extended → core roll-up", "SKIP", "concurrency_ext_abs not built")
    serv = _wrap(ctx.serving_core, ctx.tp)
    ext = _wrap(ctx.serving_ext, ctx.tp)
    join = f"""
    FROM {serv} AS c
    FULL JOIN (
        SELECT country, platform, video_type, category, minute, content_id, sum(concurrent) AS c
        FROM {ext}
        GROUP BY country, platform, video_type, category, minute, content_id
    ) AS e USING (country, platform, video_type, category, minute, content_id)
    WHERE coalesce(c.concurrent, 0) != coalesce(e.c, 0)
    """
    n = q_one(ctx.client, f"SELECT count() {join}")
    mismatches = int(n[0]) if n else 0
    status = "PASS" if mismatches == 0 else "FAIL"
    return Result("B10", "Extended → core roll-up", status,
                  f"{mismatches} cells where ext rolled-up != core")


def _table_exists(client, name: str) -> bool:
    row = q_one(client, f"EXISTS TABLE {DB}.{name}")
    return bool(row and int(row[0]) == 1)


CORE_CHECKS: list[Check] = [
    Check("B0", "Cell-grain reconciliation", _chk_reconcile_core),
    Check("B1", "Global peak & avg (sessions)", _chk_peak_sessions),
    Check("B2", "Global peak & avg (users)", _chk_peak_users),
    Check("B3", "Peak per platform", _chk_by_platform),
    Check("B4", "Peak per platform × country", _chk_by_platform_country),
    Check("B5", "Peak per content", _chk_by_content),
    Check("B6", "Peak per hour", _chk_hour_grain),
    Check("B7", "Peak per day", _chk_day_grain),
    Check("B8", "Foreground-only exclusion", _chk_foreground_only),
]
EXT_CHECKS: list[Check] = [
    Check("B9", "Extended drill-down reconciliation", _chk_reconcile_ext),
    Check("B10", "Extended → core roll-up", _chk_ext_rollup),
]


# ===========================================================================
# Runner
# ===========================================================================
_STATUS_STYLE = {"PASS": "PASS", "FAIL": "FAIL", "WARN": "WARN", "SKIP": "SKIP"}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Correctness benchmark: raw-events reference vs serving layer.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--since", help="ISO ts; compare only buckets >= this (both sides).")
    parser.add_argument("--until", help="ISO ts; compare only buckets <= this (both sides).")
    parser.add_argument("--grace-seconds", type=int, default=0,
                        help="Ignore the last N seconds on both sides (skip provisional hot edge).")
    parser.add_argument("--samples", type=int, default=20,
                        help="Max sample mismatched cells to print for reconciliation checks.")
    parser.add_argument("--no-extended", action="store_true",
                        help="Skip the extended drill-down checks (B9/B10).")
    args = parser.parse_args()

    client = clickhouse_connect.get_client(
        host=HOST, port=PORT, username=USER, password=PASSWORD, secure=SECURE,
        database=DATABASE, session_id=f"snorlax-benchmark-{os.getpid()}",
    )
    print(f"connected to {HOST}:{PORT} → {DATABASE}", file=sys.stderr)

    tp = _time_predicate(args.since, args.until, args.grace_seconds)
    if tp:
        print(f"comparing window: {tp}", file=sys.stderr)

    ctx = Ctx(
        client=client, tp=tp, samples=args.samples,
        ref_core=REF_CORE, ref_ext=REF_EXT,
        serving_core=SERVING_CORE, serving_ext=SERVING_EXT,
    )

    checks = list(CORE_CHECKS) + ([] if args.no_extended else EXT_CHECKS)
    results: list[Result] = []
    try:
        for chk in checks:
            try:
                results.append(chk.run(ctx))
            except Exception as exc:  # noqa: BLE001 — one bad check shouldn't sink the run
                results.append(Result(chk.cid, chk.title, "FAIL", f"query error: {exc}"))
    finally:
        client.close()

    # ---- report -----------------------------------------------------------
    print("\n" + "=" * 78)
    print("  SNORLAX CONCURRENCY BENCHMARK — actual (raw) vs reported (serving)")
    print("=" * 78)
    fails = warns = 0
    for r in results:
        if r.status == "FAIL":
            fails += 1
        if r.warns:
            warns += len(r.warns)
        print(f"\n[{_STATUS_STYLE.get(r.status, r.status):4}] {r.cid}  {r.title}")
        print(f"       {r.detail}")
        for w in r.warns:
            print(f"       warn: {w}")

    n_pass = sum(1 for r in results if r.status == "PASS")
    n_skip = sum(1 for r in results if r.status == "SKIP")
    print("\n" + "-" * 78)
    print(f"  {n_pass} passed · {fails} failed · {n_skip} skipped · {warns} warning(s)")
    print("-" * 78)
    if fails:
        print("  RESULT: FAIL — the serving layer disagrees with the raw-events "
              "ground truth.")
    else:
        print("  RESULT: PASS — reported concurrency matches the raw-events "
              "ground truth.")
    return fails


if __name__ == "__main__":
    raise SystemExit(main())
