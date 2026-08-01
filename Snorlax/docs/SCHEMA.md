# Snorlax schema reference

Everything here is transcribed directly from the actual DDL in
`Snorlax/schema/01_schema.sql` (the single source of every table / view /
dictionary / materialized view — `04_approaches.sql` only `TRUNCATE`s +
`INSERT`s into three of these tables, it does not define them). Database:
`sonyliv_concurrency`.

## 1. Config UDFs (`schema/00_config.sql`)

ClickHouse requires `toStartOfInterval(...)` / `INTERVAL` arguments to be
constant, and a scalar subquery against a config table wouldn't fold to a
constant there — so knobs are SQL functions, which do inline to a literal:

| Function | Returns | Used for |
|---|---|---|
| `cfg_bucket_seconds()` | `60` | Width of one concurrency bucket (the `minute` column). |
| `cfg_heartbeat_seconds()` | `60` | Expected seconds between heartbeats; grace tail after the last active event. |
| `cfg_missing_heartbeat_buffer_seconds()` | `30` | Extra slack tolerated on top of the heartbeat cadence. |
| `cfg_gap_timeout_seconds()` | `heartbeat + buffer` = `90` | Max silence bridged between two active events before a stretch is split. |
| `cfg_hot_window_seconds()` | `600` | How many trailing seconds stay in the HOT tier before rolling to COLD. |
| `norm_lang(x)` | string | Lowercase/trim, collapse `hin-hindi`→`hin`, fold empty/und/unknown→`unk`. |
| `norm_dim(x)` | string | Fold empty string → `unk`, otherwise pass through verbatim. |

Normalization (`norm_lang`/`norm_dim`) is applied only to the 4 **extended**
dims (`app_version`, `player_version`, `audio_language`, `subtitle_language`)
at ingest, inside `mv_incoming_to_raw`. Core content dims (`video_type`,
`category`) are left as-is.

## 2. Tables

### Ingest

**`events_incoming`** — ClickPipes landing table (Redpanda → JSONEachRow).
```sql
ENGINE = Null
```
Nothing is stored — `mv_incoming_to_raw` converts every insert on the fly.
Columns: `content_id Int64, video_session_id String, user_id String,
event_type LowCardinality(String), event LowCardinality(String),
event_timestamp UInt64, platform, app_version, country, audio_language,
subtitle_language, player_version LowCardinality(String),
session_start_epoch UInt64`. Timestamps arrive as **millisecond epoch ints**.

**`events_raw`** — canonical typed events.
```sql
ENGINE = MergeTree
ORDER BY (video_session_id, event_timestamp)
PARTITION BY toYYYYMM(event_timestamp)
TTL toDateTime(event_timestamp) + INTERVAL 30 DAY
```
Same columns as `events_incoming` but `event_timestamp`/`session_start_epoch`
are `DateTime64(3,'UTC')`, and the 4 extended dims are normalized. `content_id`
is `Int64` (not `UInt64`) because the catalog carries a negative placeholder
ID. `event_type` is `LowCardinality(String)`, deliberately **not** `Enum8`,
so an unseen-day event type can't be rejected on ingest. `ORDER BY
(video_session_id, event_timestamp)` makes the state-machine derivation a
streaming per-session read. 30-day TTL bounds raw growth — durable
aggregates live in `concurrency_cold_abs`.

### Reference

**`content_dim`** — content metadata, loaded by `02_seed.sql` (or the CSV
batch load).
```sql
ENGINE = ReplacingMergeTree ORDER BY content_id
( content_id Int64, title String, video_type LowCardinality(String), category LowCardinality(String) )
```

**`content_dict`** — `Dictionary` wrapping `content_dim` for O(1)
`dictGet(...)` lookups on the hot path (avoids a JOIN).
```sql
PRIMARY KEY content_id
SOURCE(CLICKHOUSE(... TABLE 'content_dim'))
LAYOUT(HASHED()) LIFETIME(MIN 600 MAX 1200)
```
Note: `mv_session_intervals` (the live path) deliberately does **not** use
this dictionary for `video_type`/`category`/`title` — it `LEFT JOIN`s
`content_dim FINAL` directly, because a ClickHouse Cloud dictionary reload is
node-local and a stale replica could otherwise serve wrong dims for a
correctness-critical column. `content_dict` is still used for display-only
reads (e.g. `ui_queries.sql` top-content-by-title) and by the
session-independent comparison job in `04_approaches.sql`.

### Intermediate (state-machine output)

**`session_intervals`** — one row per session, holding the *full* current
array of that session's truly-active `[start, end)` islands.
```sql
ENGINE = ReplacingMergeTree(version) ORDER BY video_session_id
TTL toDateTime(version/1000) + INTERVAL 3 DAY
( video_session_id String, user_id String,
  intervals Array(Tuple(active_start DateTime64(3,'UTC'), active_end DateTime64(3,'UTC'))),
  is_provisional UInt8 DEFAULT 0,
  content_id Int64, platform, country, video_type, category LowCardinality(String),
  title String, version UInt64 )
```
Keying on `video_session_id` alone (not per-interval) means a refresh that
derives *fewer* islands than last time simply replaces the whole row under
`ReplacingMergeTree` + `FINAL` — no orphaned stale rows are possible for a
shrunk interval count. `is_provisional` flags a session whose last active
end is within `cfg_gap_timeout_seconds()` of `now()` (i.e. it might still be
open).

**`session_last_seen`** — one row per session, incrementally maintained.
```sql
ENGINE = AggregatingMergeTree ORDER BY video_session_id
( video_session_id String, last_ts SimpleAggregateFunction(max, DateTime64(3,'UTC')) )
```
Exists purely so the state machine's "which sessions are recent" lookup
(`D2`'s `recent` CTE) reads this tiny table instead of scanning all of
`events_raw` every 30 seconds — recompute cost becomes O(active sessions),
not O(history).

**`dim_values`** — tiny distinct-value table for cheap UI dropdowns.
```sql
ENGINE = ReplacingMergeTree ORDER BY (dim, value)
( dim LowCardinality(String), value LowCardinality(String) )
```

### Serving — core (live hot/cold)

**`concurrency_cold_abs`** — frozen, append-only per `(dims, minute)`.
```sql
ENGINE = ReplacingMergeTree
ORDER BY (country, platform, video_type, category, minute, content_id)
( ..., concurrent UInt32, concurrent_users UInt32 )
```
`ReplacingMergeTree` so a retried/overlapping compaction run can't
double-count a minute; always read with `FINAL`.

**`concurrency_hot_abs`** — recent minutes, wholesale-recomputed every 30s.
```sql
ENGINE = MergeTree
ORDER BY (country, platform, video_type, category, minute, content_id)
```
Plain `MergeTree` is fine here because the refresh job `TRUNCATE`-and-rebuilds
this table's hot window wholesale (via `REFRESH ... EMPTY AS ...`), so there
is never a duplicate to replace.

**`concurrency_now`** (VIEW) — what the UI actually reads:
```sql
SELECT ... FROM concurrency_cold_abs FINAL
UNION ALL
SELECT ... FROM concurrency_hot_abs
WHERE minute > coalesce((SELECT max(minute) FROM concurrency_cold_abs), toDateTime(0))
```
The `coalesce` guards a pure-live deployment where `cold_abs` is still empty
— without it, `minute > NULL` would silently filter out every hot row.

Both `concurrent` (`uniqExact(video_session_id)`) and `concurrent_users`
(`uniqExact(user_id)`) are exact per cell and for fixed-dimension peak/avg.
Summing `concurrent_users` **across** dim rows can overcount a user watching
more than one piece of content at once — documented, not a bug.

### Serving — extended (drill-down, offline/scheduled build)

**`concurrency_ext_abs`** — core key + `app_version, player_version,
audio_language, subtitle_language, title`.
```sql
ENGINE = MergeTree
ORDER BY (country, platform, video_type, category, subtitle_language,
          audio_language, player_version, app_version, minute, content_id, title)
```
Kept as a **separate** table (not merged into the core tiers) so the common
dashboard path stays fast on the lean core key. `ORDER BY` puts the core key
as a prefix so a core-only filter still uses the leading key. Populated by
`04_approaches.sql` §3 from `session_intervals` (offline/scheduled — there is
no live hot/cold tier for the extended path).

### Comparison-only (no tiering; populated by `04_approaches.sql`)

**`concurrency_sa_abs`** (session-aware) and **`concurrency_si_abs`**
(session-independent) — identical shape/engine/`ORDER BY` to
`concurrency_cold_abs`, so the two approaches' full-history output is
byte-for-byte comparable minute-for-minute. Cross-checked by
`schema/05_compare.sql`.

## 3. Materialized views (populate the tables)

| MV | Refresh | `TO` table | Reads |
|---|---|---|---|
| `mv_incoming_to_raw` | incremental (per insert) | `events_raw` | `events_incoming` |
| `mv_session_last_seen` | incremental | `session_last_seen` | `events_raw` |
| `mv_dim_values` | incremental | `dim_values` | `events_raw` |
| `mv_session_intervals` | `REFRESH EVERY 30 SECOND` | `session_intervals` | `events_raw` (scoped to sessions seen in the last 20 min via `session_last_seen`) |
| `concurrency_hot_abs_mv` | `REFRESH EVERY 30 SECOND DEPENDS ON mv_session_intervals` | `concurrency_hot_abs` | `session_intervals FINAL` (last `cfg_hot_window_seconds()` = 10 min) |
| `mv_cold_compaction` | `REFRESH EVERY 1 MINUTE DEPENDS ON concurrency_hot_abs_mv` | `concurrency_cold_abs` | `session_intervals FINAL` (everything older than 10 min, forward-fill only) |

`mv_session_intervals` and the two REFRESH MVs are created with `EMPTY AS`,
i.e. they populate on their own schedule rather than back-filling
immediately — that's what `03_backfill.sql` / `02_seed.sql`'s
`SYSTEM REFRESH VIEW ... SYSTEM WAIT VIEW ...` calls are for in an offline
build.

## 4. File → object map

| File | Kind | Creates / populates |
|---|---|---|
| `00_config.sql` | WRITE (setup) | UDFs: `cfg_*`, `norm_lang`, `norm_dim` |
| `01_schema.sql` | WRITE (build) | **All** tables, the dictionary, `concurrency_now` view, all materialized views |
| `02_seed.sql` | WRITE (seed) | Synthetic smoke-test rows into `content_dim` + `events_raw` (Section A, active); or CSV batch load (Section B, commented out) |
| `03_backfill.sql` | WRITE (build) | One-shot: `events_raw` → `session_intervals` → `concurrency_cold_abs` + `concurrency_hot_abs`, split by a watermark |
| `04_approaches.sql` | WRITE (build) | `TRUNCATE` + `INSERT` into `concurrency_sa_abs`, `concurrency_si_abs`, `concurrency_ext_abs` only (DDL lives in `01_schema.sql`) |
| `05_compare.sql` | READ (validate) | Asserts `concurrency_sa_abs` ≡ `concurrency_si_abs` ≡ `concurrency_now` totals; extended roll-up cross-check |
| `06_verify.sql` | READ (validate) | Serving-vs-brute-force, an independently-implemented oracle, pause-exclusion and session-start-seeding invariants |
| `ui_queries.sql` | READ (ad hoc) | Dashboard queries against `concurrency_now` / `concurrency_ext_abs` |
| `tuning_variants.sql` | READ (ad hoc) | Sensitivity checks for the `00_config.sql` knobs |
| `migrations/001_foreground_state_fixes.sql` | migration | Re-applies the pause-deactivation + `VideoSessionStart`-seeds-active fix to an already-deployed `mv_session_intervals` |
| `migrations/reset.sql` | reset (only via `--reset`) | Drops every object |
