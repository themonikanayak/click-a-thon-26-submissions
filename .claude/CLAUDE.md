Use simpler subagents as much as possible

## Git operations — never run them directly
Do NOT execute any git operation yourself (no commits, branches, merges, pushes,
tags, resets, etc.). Instead, write the exact git command(s) to a temporary file
(e.g. `git-commands.sh`) and ask the user to execute them. Once the user confirms
they have run the commands, delete the temporary file.

# Snorlax submission — SonyLIV foreground-only concurrency

Active code lives in `Snorlax/`. Do NOT put implementation code in `SonyLiv/`
(that folder holds the problem statement, design docs, and GAP_ANALYSIS.md only).

## Coding standards (read before writing code)
Follow the language best-practices docs in `.claude/best-practices/` — consult the
relevant one before writing or reviewing code, and keep them current when a new
convention is adopted:
- SQL / ClickHouse → [`.claude/best-practices/sql.md`](best-practices/sql.md)
- Python → [`.claude/best-practices/python.md`](best-practices/python.md)
- Java (future JVM components) → [`.claude/best-practices/java.md`](best-practices/java.md)

## Schema changes — migrations only, idempotent
Do NOT create new stray `.sql` files for schema/behavior changes. Add every change
as an idempotent migration under `Snorlax/migrations/` (`NNN_short_description.sql`,
applied in order by `--migrate`). Editing an existing `schema/*.sql` file in place
is fine; *new* objects/changes belong in a migration. Every migration must be safe
to re-run (`CREATE ... IF NOT EXISTS`, `DROP ... IF EXISTS`, `CREATE OR REPLACE`,
`ALTER ... ADD COLUMN IF NOT EXISTS`, `ReplacingMergeTree` + `FINAL`).

Run/rebuild SQL with `Snorlax/migrations/run_sql.py` (reuses `producer/.env`):
- `python run_sql.py --reset --build` — drop everything, then recreate the schema.
- `python run_sql.py --build` / `--all` / `--migrate` — recreate structure / full
  offline pipeline / apply ordered migrations.
- `python run_sql.py -i` (interactive REPL) · `-c "SQL"` (inline) · `file.sql ...`.
`migrations/reset.sql` drops all objects and runs only via `--reset` (never `--migrate`).

## Keep docs current
`Snorlax/README.md` is the submission's user-facing doc — update it whenever the
project's behavior, run steps, or layout change. Reflect any new locked decisions
here in this CLAUDE.md too, and keep the run order in both files in sync with
`plan/PLAN.md`.

- ClickHouse is the engine. Database: `sonyliv_concurrency`.
- Design: absolute concurrency per `(dims, minute)` (no cumsum / carry-in).
  Serving = `concurrency_cold_abs` ∪ `concurrency_hot_abs` via the
  `concurrency_now` view. See `Snorlax/plan/PLAN.md` for the locked decisions
  (90s gap / 60s grace, overlap minute semantics, once-per-minute dedupe).

## Two concurrency approaches (the problem requires both + a comparison)
- **Session-aware** — reconstruct per-session truly-active intervals via the
  state machine (`session_intervals`), expand to minutes, count distinct
  sessions. Files: `schema/01_schema.sql`, `schema/03_backfill.sql`;
  standalone comparable table `concurrency_sa_abs` populated in
  `schema/04_approaches.sql` (DDL in `01_schema.sql`).
- **Session-independent** — derive per-event foreground state directly (no
  per-session interval reconstruction), expand active segments to minutes,
  count distinct sessions. Table `concurrency_si_abs` populated in
  `schema/04_approaches.sql` (DDL in `01_schema.sql`).
- **Comparison** — `schema/05_compare.sql` asserts the two agree
  (and match `concurrency_now`) per `(dims, minute)`; expect zero mismatches.

Both share ONE active definition; `uniqExact` per minute makes interval-merging
(session-aware) vs not-merging (session-independent) irrelevant to the count.

## Tunable knobs
`schema/00_config.sql` is the single place for tunable parameters, exposed as SQL
UDFs (they inline to constants, so they work inside `toStartOfInterval`/`INTERVAL`
where a config table would fail the constant requirement). Run it FIRST and
re-run after any change, then rebuild.
- `cfg_bucket_seconds()` — time-bucket width (was fixed 1 min = 60). Change for
  30s / 5-min / hourly buckets. Serving column is still named `minute`.
- `cfg_heartbeat_seconds()` + `cfg_missing_heartbeat_buffer_seconds()` — the
  gap tolerance. Derived `cfg_gap_timeout_seconds()` = heartbeat + buffer
  (default 60 + 30 = old 90s). Raise the buffer to bridge more missing beats.
- `cfg_hot_window_seconds()` — the HOT→COLD rollover window (default 600 = old
  10 min): buckets newer than `now() - this` serve from hot, older roll to cold.
  Used in 01_schema.sql D3/D4 and backfill (was a hardcoded `INTERVAL 10 MINUTE`).
Non-UDF-able knobs (derivation lookback, refresh cadence, TTL) are documented in
`00_config.sql` and set at their source literals.

## Dimensions
Serving splits into two keyed tables:
- **Core** (lean, fast, live hot/cold): `(country, platform, video_type, category,
  minute, content_id)` — `concurrency_cold_abs`/`hot_abs` → `concurrency_now`.
- **Extended** (drill-down, offline/scheduled build): core key + `app_version,
  player_version, audio_language, subtitle_language` → `concurrency_ext_abs`,
  populated in `04_approaches.sql` (DDL in `01_schema.sql`). Kept separate per
  PLAN §9 Fix #7 so the core path stays fast. It rolls back up to the core
  counts exactly (cross-check in `05_compare.sql`).
The 4 extended dims are **normalized at ingest** (`00_config.sql` `norm_lang` /
`norm_dim`): `hin/HIN/hin-hindi → hin`, empty → `unk`. Core content dims
(`video_type`/`category`) are left as-is to avoid ground-truth divergence.
Note: `country` is single-valued (`india`) in the sample data.

### Measures & extra dims
- Every aggregate stores TWO measures: `concurrent` = distinct SESSIONS
  (`uniqExact(video_session_id)`) and `concurrent_users` = distinct USERS
  (`uniqExact(user_id)`). Both are exact per cell and for fixed-dimension
  peak/avg; summing `concurrent_users` ACROSS dims can overcount a user on >1
  content (06_verify.sql check E asserts users at cell grain, not summed).
  `user_id` is carried through `session_intervals`.
- `title` is exposed as a keyed dimension in `session_intervals` and
  `concurrency_ext_abs` (1:1 with `content_id`, so no cardinality cost); core
  tiers still resolve it via `dictGet('content_dict','title', content_id)`.
- `event_type` is `LowCardinality(String)` (NOT `Enum8`) so an unseen-day
  event type can't reject ingestion.

## Run order (offline / backfill)
`schema/` is a numbered read/write pipeline: `00`-`04` are the WRITE/build
steps, `05`-`06` are READ/validate, and `ui_queries.sql` + `tuning_variants.sql`
are ad-hoc READ tools (unnumbered, not part of the pipeline).

`00_config.sql` → `01_schema.sql` → load events (`02_seed.sql`) →
`03_backfill.sql` → `04_approaches.sql` (one file: session-aware +
session-independent + extended-dims INSERT jobs; their DDL lives in
`01_schema.sql`) → `05_compare.sql` → `06_verify.sql`.

SQL not yet executed on a live ClickHouse — expect minor engine fixes on first run.
