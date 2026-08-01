# ClickHouse SQL Best Practices — Snorlax / SonyLIV concurrency

This doc is referenced by `CLAUDE.md` and applies to **all** SQL in `Snorlax/`.
Engine: **ClickHouse Cloud**. Database: **`sonyliv_concurrency`**. The design is
absolute concurrency per `(dims, minute)` served by `concurrency_now` (cold ∪ hot).
When writing or editing SQL, match the conventions already established in
`schema/01_schema.sql` and `schema/04_approaches.sql` (the session-aware and
session-independent INSERT jobs). `schema/` is a numbered read/write pipeline:
`00`-`04` are the WRITE/build steps, `05`-`06` are READ/validate, and
`ui_queries.sql` + `tuning_variants.sql` are ad-hoc READ tools.

---

## 1. Formatting & style

- **Uppercase all SQL keywords** (`SELECT`, `CREATE TABLE IF NOT EXISTS`, `ORDER BY`,
  `ARRAY JOIN`, `GROUP BY`). Functions stay lowercase (`toStartOfMinute`, `uniqExact`).
- **One clause per line** for anything non-trivial — put `FROM`, `WHERE`, `GROUP BY`,
  `ORDER BY` on their own lines so a diff shows exactly which clause changed.
- **CTEs (`WITH ... AS`) over nested subqueries.** The state machine reads as a
  pipeline: `per_event → collapsed → stated → segments → islands`. Do not inline
  these as stacked subqueries; name each step after what it produces.
- **Meaningful aliases** that describe the row grain: `sid`, `ts`, `seg_start`,
  `seg_end`, `active_start`, `active_end`, `state_sign`, `new_island`. Avoid `t1`/`a`/`b`.
- **Comment every non-obvious query with a one-line purpose header.** Every file in
  `schema/` opens with a `-- ####…` banner stating what it does and its run order;
  every CTE and every subtle idiom (half-open end, once-per-minute dedupe, the
  `(session, ms)` collapse) carries an inline `--` explanation. Keep this up.
- **Trailing commas**: put the comma at the end of the line (leading item first),
  as the existing DDL and `SELECT` lists do. Keep column lists in a stable order so
  serving tables stay byte-for-byte comparable across files.

---

## 2. Schema / DDL

- **`CREATE TABLE IF NOT EXISTS`** always — scripts must be safe to re-run (see §7).
  Use `TRUNCATE TABLE IF EXISTS` before a full rebuild of a comparison table
  (`concurrency_sa_abs`, `concurrency_si_abs`).
- **Always specify `ENGINE` and `ORDER BY`.** Never rely on defaults.
- **Order key low→high cardinality.** Serving tables use
  `ORDER BY (country, platform, video_type, category, minute, content_id)` —
  low-card dims first, `minute` before the high-cardinality `content_id`. New
  serving/comparison tables MUST reuse this exact key so outputs stay comparable.
- **`LowCardinality(String)`** for finite string domains (`country`, `platform`,
  `video_type`, `category`, `app_version`, …) and **`Enum8`** for closed sets —
  e.g. `event_type Enum8('VideoSessionStart'=1, …, 'VideoError'=7)`.
- **`DateTime64(3,'UTC')`** for event timestamps, with
  `CODEC(DoubleDelta, ZSTD(1))` (time series compresses well delta-of-delta):
  ```sql
  event_timestamp DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1))
  ```
  Minute buckets in serving tables are `DateTime('UTC')` (second precision is enough).
- **TTL + monthly partitioning are for lifecycle only, not query pruning.**
  `events_raw` uses `PARTITION BY toYYYYMM(event_timestamp)` and
  `TTL toDateTime(event_timestamp) + INTERVAL 30 DAY` to bound raw growth (aggregates
  live in `concurrency_cold_abs`). Do **not** add daily partitions to shape queries.
- **`ReplacingMergeTree` for idempotent / retry-safe writes.** `concurrency_cold_abs`
  is `ReplacingMergeTree` so a re-fired compaction can't double-count a minute (one
  row per `(dims, minute)` key); `session_intervals` uses `ReplacingMergeTree(version)`
  with a `version` column. **Always read these with `FINAL`** (the `concurrency_now`
  view does: `FROM …concurrency_cold_abs FINAL`).
- **Plain `MergeTree` when the table is rebuilt wholesale** — `concurrency_hot_abs`
  is REPLACE-recomputed every 30s, so it needs no dedup engine.
- **`Null` engine for landing tables consumed by an MV.** `events_incoming` is
  `ENGINE = Null` — the MV `mv_incoming_to_raw` converts each insert; nothing is
  stored. (Swap to `MergeTree ORDER BY tuple()` only to debug ClickPipes mapping.)

---

## 3. Determinism

- **Same-millisecond collapse is mandatory.** ~29% of events share a timestamp and
  tie order is engine-unstable, so results would differ between local and Cloud.
  Collapse to **one row per `(session, ms)`** with priority
  **deactivate > reactivate > neutral** before running the state machine:
  ```sql
  -- collapsed: one row per (sid, ts); a same-ms pause beats a same-ms resume/heartbeat
  SELECT sid, ts,
         if(min(transition) < 0, toInt8(-1), toInt8(max(transition))) AS transition,
         any(content_id) AS content_id, any(platform) AS platform, any(country) AS country
  FROM per_event
  GROUP BY sid, ts
  ```
  This also stops a neutral heartbeat from cancelling a pause at the same instant.
- Any new logic that depends on event ordering must be reproducible: sort by
  `event_timestamp`, break ties deterministically, and never depend on insert order.

---

## 4. Query performance

- **Read the serving layer, never raw events.** Dashboards/KPIs read
  `concurrency_now` (or `concurrency_cold_abs`/`concurrency_hot_abs`), not `events_raw`.
- **`filter → sum → max/avg`.** Counts are **absolute** per `(dims, minute)` —
  **no cumsum, no carry-in, no base-term.** Concurrency at minute *m* is
  `sum(concurrent)` over matching dim rows; peak is `max` over a range; average is
  `sum(concurrent) / #minutes`.
- **`uniqExact` for distinct-session counts.** `toUInt32(uniqExact(video_session_id))`
  gives the once-per-minute dedupe for free — a set membership test, so it makes
  interval-merging (session-aware) vs not-merging (session-independent) irrelevant.
  Use `uniqExact` (exact), not `uniq`, for correctness against ground truth.
- **Prove reads via `system.query_log`.** Verify a query touches few rows with
  `read_rows` (and latency) from `system.query_log` — this is the M2 acceptance
  evidence, not a guess.
- **Avoid JOINs on the hot path.** Enrich `content_id → video_type/category` with the
  `content_dict` dictionary via `dictGet('sonyliv_concurrency.content_dict', …)`,
  as the MVs and the session-independent job in `04_approaches.sql` do. Reserve `content_dim` (the
  dictionary source) for a JOIN fallback only.

---

## 5. Materialized views

- **Incremental MV** for cheap per-insert transforms: `mv_incoming_to_raw`
  is `CREATE MATERIALIZED VIEW IF NOT EXISTS … TO events_raw AS SELECT …` — fires on
  every insert into `events_incoming`.
- **Refreshable MV** for periodic recompute: `REFRESH EVERY 1 MINUTE`
  (`mv_session_intervals`) and `REFRESH EVERY 30 SECOND` (`concurrency_hot_abs_mv`).
  Use `DEPENDS ON` to order refreshes (hot depends on `mv_session_intervals`).
- **`TO <table>`** to target an explicit table, and **`EMPTY AS`** so the MV is
  created without an immediate backfill run (the schedule populates it).
- **Bounded windows** so recompute cost is independent of history: derivation reads a
  20-min window, hot recomputes the last 10 min
  (`WHERE minute > toStartOfMinute(now()) - INTERVAL 10 MINUTE`). Recompute cost
  ∝ (window × active sessions), never total history. Keep windows as tight as p99
  heartbeat lag allows. Drop-and-recreate refreshable MVs with
  `DROP VIEW IF EXISTS …` first so edits are re-runnable.

---

## 6. Correctness & verification

- **Always keep a brute-force reference that must match with 0 mismatches.**
  `schema/05_compare.sql` asserts session-aware == session-independent (and
  both == `concurrency_now`) per `(dims, minute)`. Any new metric needs an independent
  reference query and an expected-zero-diff assertion (PLAN §8).
- **UTC everywhere, one `toTimeZone` at the edge.** All minute buckets are `'UTC'`;
  never bake a local timezone into the model. Convert to IST (or whatever the
  benchmark expects) only in the final presentation query.
- **Half-open interval end.** When expanding an interval to minutes, subtract 1ms
  before flooring so an interval ending exactly on a minute boundary doesn't claim the
  next minute: `toStartOfInterval(active_end - INTERVAL 1 MILLISECOND, …)`.
- **Dedup keys for retried loads.** `events_raw` is plain `MergeTree`, so a re-run
  duplicates rows — dedup by
  `(video_session_id, event_timestamp, event_type, event)` or reload cleanly before
  a scored run (PLAN §9, Fix #10).

---

## 7. Idempotency & ops

- **Every script is safe to re-run.** Use `CREATE … IF NOT EXISTS`,
  `DROP … IF EXISTS`, `TRUNCATE TABLE IF EXISTS`, `CREATE OR REPLACE VIEW`, and
  `ReplacingMergeTree` + `FINAL` for retry-safe writes. Re-running must not
  double-count or error.
- **Document run order in the file header.** Each `schema/*.sql` opens with a banner
  giving its place in the pipeline (`schema/` is a numbered read/write pipeline:
  `00`-`04` WRITE/build, `05`-`06` READ/validate). The canonical offline order is:
  `00_config.sql` → `01_schema.sql` → load events → `03_backfill.sql` →
  `04_approaches.sql` (one file: session-aware + session-independent +
  extended-dims INSERT jobs; DDL lives in `01_schema.sql`) →
  `05_compare.sql` → `06_verify.sql`. Keep new files consistent with this.
- **Dimension normalization lives at the edge.** The 4 extended drill-down dims
  (`app_version`, `player_version`, `audio_language`, `subtitle_language`) are
  normalized in `00_config.sql` (`norm_lang`/`norm_dim`) and applied ONCE at ingest
  (`mv_incoming_to_raw`), never re-derived downstream. The
  extended serving table `concurrency_ext_abs` is kept separate from the lean core
  tiers (PLAN §9 Fix #7); a core-only query must not pay for the extra dims.
- **Config over magic numbers.** Gap/grace/bucket constants come from `00_config.sql`
  helpers (`cfg_gap_timeout_seconds()`, `cfg_heartbeat_seconds()`,
  `cfg_bucket_seconds()`), not inline literals — change the knob in one place.
- **SQL is not yet executed on live ClickHouse** — expect minor engine fixes on first
  run (window frames, `leadInFrame`, refreshable-MV syntax, `ARRAY JOIN range()` on
  long intervals). Verify on Cloud before relying on any result.
