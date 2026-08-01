# Benchmark & correctness query set

The questions our concurrency system is evaluated on, derived directly from
[`SonyLiv/PROBLEM_STATEMENT.md`](../../SonyLiv/PROBLEM_STATEMENT.md), plus how each
one is **automatically verified for correctness** by
[`benchmark.py`](benchmark.py).

## How correctness is checked (no answer key needed)

The private ground-truth answer key stays with the judges, so we build our own
oracle. For every question, `benchmark.py` computes the answer **twice**:

| Side | Source | Meaning |
|---|---|---|
| **actual** (reference) | `events_raw` **only** | Foreground-only concurrency reconstructed from raw events from scratch — reads no pipeline output (`session_intervals` / `concurrency_*`), so it is an independent oracle. |
| **reported** (serving) | `concurrency_now` (cold ∪ hot) / `concurrency_ext_abs` | The number a dashboard actually reads from the materialized serving layer. |

Both sides are produced by the **same outer query with only the source table
swapped**, and both call the same config UDFs (`cfg_bucket_seconds`,
`cfg_gap_timeout_seconds`, …), so any disagreement is a genuine correctness
defect, not a query artifact or a knob drift. The reference reconstructs the
identical foreground-only active definition used by the pipeline
(`01_schema.sql` D2 / `03_backfill.sql` / `04_approaches.sql`):

> A session is foreground-active from its start until a pause / background /
> error / end. Heartbeats extend activity within `cfg_gap_timeout_seconds()`.
> Paused and backgrounded time is **excluded**. Per `(dims, bucket)` the count is
> `uniqExact(video_session_id)` (sessions) and `uniqExact(user_id)` (users).

Run it after the pipeline has caught up with `events_raw` (offline backfill, or
`--grace-seconds` past ingest lag on live data). A settled serving layer must
match the reference to the row.

## The benchmark questions

Each row maps to a problem-statement requirement and a check id in `benchmark.py`.
The **headline check is B0**: exhaustive cell-grain reconciliation. B1–B10 are the
specific business questions dashboards ask; they would all pass automatically once
B0 passes, but are checked explicitly because they mirror the evaluated query set
(peak/average at minute/hour/day grain, with dimension filters).

| id | Question | Problem-statement tie-in | Verification |
|---|---|---|---|
| **B0** | Does every `(country, platform, video_type, category, minute, content_id)` cell — sessions **and** users — match ground truth? | "Correct … matches the ground truth on the benchmark queries" | FULL JOIN reference vs serving; **0 mismatched cells** required. Definitive. |
| **B1** | What is the **peak** and **average** concurrency (distinct sessions) over the range, at minute grain, and in which minute does the peak fall? | "accurate minute-wise peak and average concurrency" | `max`/`argMax`/range-avg over the per-minute curve; actual == reported. |
| **B2** | Same as B1 but **distinct users** (`concurrent_users`). | user-level concurrency (dataset `user_id`) | Peak/avg of `concurrent_users`; actual == reported. |
| **B3** | Peak concurrency **per platform** (each platform at its own peak minute). | "filter-friendly across … platform"; non-additivity scenario | Per-key peak breakdown; every platform's peak agrees. |
| **B4** | Peak concurrency **per platform × country** — the combo's peak minute can differ from either dimension alone. | the explicit scenario in §"peak and average concurrency" (peaks at different minutes per dim combo) | Per-combo peak breakdown; every combo agrees. |
| **B5** | Peak concurrency **per content** (top-content leaderboard). | "filter-friendly across … content" | Per-`content_id` peak breakdown agrees. |
| **B6** | Peak concurrency **per hour** (max minute within each hour). | "peak … at minute/hour/day grain" | Per-hour peak breakdown agrees. |
| **B7** | Peak concurrency **per day**. | "peak … at minute/hour/day grain" | Per-day peak breakdown agrees. |
| **B8** | Does the count truly **exclude backgrounded / paused / heartbeat-missing** time? | "Foreground-only means foreground-only … overcounting backgrounded time is the failure mode" | serving total == foreground reference, **and** strictly below the naive "any heartbeat in the minute" count; reports the overcount avoided. |
| **B9** | Do **drill-down** filters (app_version, player_version, audio_language, subtitle_language) report correct concurrency? | "filter-friendly across common business dimensions"; dataset drill-down dims | FULL JOIN extended reference vs `concurrency_ext_abs`; **0 mismatched cells**. |
| **B10** | Does the extended table **roll back up** to the core counts exactly? | design consistency / trade-off defense | Collapse the 4 drill-down dims; must reproduce `concurrency_now` per core cell. |

## Query shapes

**Peak / average at a grain (B1, B2, B6, B7)** — read the serving layer, never raw
history:

```sql
SELECT toUInt64(max(c))                       AS peak_concurrency,
       argMax(minute, c)                      AS peak_minute,
       round(sum(c) / (dateDiff('second', min(minute), max(minute))
             / cfg_bucket_seconds() + 1), 1)  AS avg_concurrency
FROM (SELECT minute, sum(concurrent) AS c
      FROM sonyliv_concurrency.concurrency_now
      GROUP BY minute);
```

**Per-dimension peak, each at its own peak minute (B3, B4, B5)** — the
non-additivity the problem calls out:

```sql
SELECT platform, max(c) AS peak, argMax(minute, c) AS peak_minute
FROM (SELECT platform, minute, sum(concurrent) AS c
      FROM sonyliv_concurrency.concurrency_now GROUP BY platform, minute)
GROUP BY platform ORDER BY peak DESC;
```

**Drill-down (B9)** — reads the extended serving table:

```sql
SELECT max(c) AS peak, argMax(minute, c) AS peak_minute
FROM (SELECT minute, sum(concurrent) AS c
      FROM sonyliv_concurrency.concurrency_ext_abs
      WHERE platform = 'ANDROID_PHONE' AND audio_language = 'hin'
      GROUP BY minute);
```

These are the same queries `schema/ui_queries.sql` serves to the dashboard; the
benchmark just runs each one against both the serving layer and the raw-events
reference and asserts they agree.

## Running the benchmark

```sh
cd Snorlax/benchmark
python -m venv .venv && source .venv/bin/activate      # or reuse producer/.venv
pip install -r requirements.txt
python benchmark.py                                     # all checks; exit code = #failures
```

Useful flags:

- `--grace-seconds N` — ignore the last `N` seconds on both sides (skip the
  still-provisional hot edge when running against live traffic).
- `--since <ISO> --until <ISO>` — restrict the compared window (both sides).
- `--samples N` — how many sample mismatched cells to print on failure.
- `--no-extended` — skip B9/B10 if `concurrency_ext_abs` isn't built.

Exit code is the number of failed checks (`0` = the serving layer matches the
raw-events ground truth), so it plugs straight into CI or a pre-submission gate.
