# SonyLIV Foreground-Only Concurrency — FINAL PLAN

## 1. Problem
Answer **"how many sessions are truly watching at each minute?"** — excluding paused, backgrounded, and heartbeat-missing time — with dashboard-grade latency, dimension filters, and incremental updates for still-open sessions. ClickHouse is the engine. Judged on correctness (vs a private key), query speed (what it *reads*), incremental updates, design reasoning, and a sealed **"unseen day"** run through our pipeline.

## 2. Decisions (locked)
- **Deployment: LIVE streaming service (primary).** A CSV-reader service tails file(s) → publishes JSON events to **Redpanda** → **ClickPipes** consumes → `events_incoming` → MV → `events_raw` (continuous). Serving runs on a `now()`-based watermark (hot every 30s, cold compaction every ~1 min) → **near-real-time insights**. Batch CSV load (`02_load`) and one-shot build (`03_build`) remain as a **history backfill / offline fallback**, not the main path.
- **Metric:** concurrent **sessions** only (distinct `video_session_id`). User-level deferred.
- **Minute semantics:** **overlap** — a session counts in minute *m* if truly-active for *any* part of *m*. (Single toggle; pin to ground truth.)
- **Per-session dedupe:** a session counts **once per minute** regardless of how many active stretches it has in it.
- **Infra:** ClickHouse Cloud, `default` user. DB = `sonyliv_concurrency`.
- **Integration:** ClickStack committed; LibreChat+MCP / Langfuse = stretch.
- **Event defaults:** buffering = active; ads (`AdPause/AdResume`) = pause/resume; offline/cast = excluded.
- **Gap/grace:** heartbeat silence > **90s** ends a stretch; grace = **60s**.

## 3. Core logic
**Step 1 — active-interval state machine** (per session, ordered by time). Hold `watching` bool; change only on transition:
- **+1 start:** `VideoSessionStart`, `VideoPlay`, `resume`, `speed-resume`, `AppForegrounded` (`AdResume`). **`VideoSessionStart` seeds the session as active from the start** — heartbeats only fire while watching, so active heartbeats *before* the first explicit `VideoPlay` must be kept, and a session that never emits an explicit `Play` (or whose first state-changing event is a pause/background) must not drop to zero. Trade-off (pin to the key, like buffering): a session that starts but never truly plays counts ~1 min (`[start, first-play)`).
- **−1 stop:** `pause`, `speed-pause`, `AppBackgrounded`, `VideoSessionEnd`, `VideoError` (`AdPause`), plus the `VideoPause` / `AdBreakStart` event_types. **Pause has no coarse `event_type`** — it rides in the `event` column ("the actual event", `dataset_details.md`); we match it by `event_type` **OR** `event` so a paused-but-still-heartbeating session is excluded by *state* (the 90s gap rule alone would miss it). Seek/buffering (`speed-pause`) is the separate buffering-active toggle (§9), left on the `event` value. [GAP_ANALYSIS #2]
- Heartbeats → no change, refresh `last_seen`. Silence > 90s → close at `last_seen+60s`, reopen next event. Still-open → extend to `last_seen+60s`, provisional. Repeats ignored.
- **Determinism (fix):** events are first **collapsed per `(session, millisecond)`** with priority **deactivate > reactivate > neutral**. ~29% of events share a timestamp and tie order is engine-unstable → this collapse guarantees identical results locally and on Cloud, and stops a neutral heartbeat from ever cancelling a pause at the same instant.
→ produces truly-active intervals `[active_start, active_end)`.

**Step 2 — absolute per (dims, minute).** Expand each session's intervals to the minutes they cover and count **distinct sessions** per `(dims, minute)` (`uniqExact` → the once-per-minute dedupe, for free). Store this absolute count in the serving tables. *(Verified on the data: a session has one content_id/country — only 1/0 sessions span each, genuinely negligible — so absolute counts are **additive across those dims**. platform and user_id are NOT session-level constants (~95 and ~120 sessions respectively span >1 value, e.g. a device switch mid-session): they ride per-interval instead, with an island boundary forced on either changing, so each interval's platform/user_id is exact rather than approximated — see §9 "Per-interval platform/user_id".)*

**Step 3 — serve.** Concurrency(*m*, filters) = `sum(concurrent)` over matching dim rows at minute *m*. Peak = `max` over the range; average = `sum(concurrent) / (#minutes in range)` (zero minutes included in the denominator); hour/day = aggregate the minute values. **No cumulative sum, no carry-in, no base-term** — absolute counts are directly summable. Peak per dimension combo falls out of filter+group at query time.

**Edge cases:** missed beat <90s → stay; long silence → gap excluded; **start with no explicit `VideoPlay` → active from `VideoSessionStart`** (seeded active; not 0); out-of-order → sort by event time; duplicates → ignored; end≤start/orphans → dropped; bot 301-session user → flag.

## 4. Architecture — HOT/COLD tiering (absolute, pitfall-fixed)
Both tiers store **absolute concurrency per `(dims, minute)`** (instantaneous counts are additive across dims). This makes every query `filter → sum → max/avg` — the simplest correct form, **no cumsum / carry-in / base-term anywhere**. (We rejected PLAN2's active definition on correctness — its "heartbeat-in-minute" rule overcounted ~50% of paused windows, verified — but kept its absolute-serving idea.) Split by a minute watermark:

- **COLD** (`minute ≤ watermark`) = absolute per `(dims, minute)`, frozen. Direct read.
- **HOT** (`minute > watermark`) = absolute per `(dims, minute)` for recent minutes, **REPLACE-recomputed** every ~30s from the recently-active sessions (bounded work). *(Fix #1: REPLACE — not append-only — sidesteps the open-session "moving −1" trap; no per-run bookkeeping.)*
- **Serving VIEW `concurrency_now`** = cold ∪ hot, with **hot read only for `minute > max(cold minute)`**. *(Fix #5: a minute is hidden from hot the instant it lands in cold → cold/hot disjoint even mid-compaction, no double-count race.)*
- **Compaction (hot→cold):** freezing a minute = copy its (already-absolute) hot rows into `cold_abs`; the derived watermark advances and the view stops reading them from hot. *(Fix #2: no snapshot layer — absolute cold needs none. Fix #3: hot cleanup is lazy hourly TTL/partition, never per-minute.)*
- **Replay/`as_of`** *(Fix #6):* watermark = `as_of − hot_window` (default `now()`; set `as_of = max(event_timestamp)` on the static set to exercise hot). Plain static load with `now()` → everything cold (correct).
- **Watermark width = the one knob:** size to p99 heartbeat lag via ClickStack. Wide hot = correctness under late data; narrow hot = faster reads.
- **Scale caveat (honest):** the absolute *build* expands intervals → minutes (a per-session-minute **intermediate** before the `uniqExact` group-by). The stored output is compact (minutes × dim-combos, independent of session count), but that intermediate is bounded by total active session-minutes. Fine at sample size and for the small hot window; for extreme scale (multi-hour sessions at 100×) switch the **cold build** to delta→cumsum-per-combo (2 rows/run → cumsum over a dense per-combo minute axis), which avoids per-session expansion. Query side is unchanged.

## 5. Tables & views (7 tables + 1 dict + 1 view + 1 Null landing)
*(Updated post-review — see §13. `session_intervals` is now one row per session; two small tables added for cheap recency/dropdown lookups; `content_dim` is now a plain JOIN on the correctness-critical path, dictionary kept only for display.)*
| Object | Engine | Role |
|---|---|---|
| `events_incoming` | MergeTree (+2d TTL) | **ClickPipes landing** from Redpanda (JSON, ms epochs); `mv_incoming_to_raw` casts → `events_raw` |
| `events_raw` | MergeTree, `ORDER BY (session, ts)` | **canonical typed events**; fed by streaming MV [07] + batch load [02]; dup-tolerant (state machine collapses per (session,ms) & ignores repeats); `content_id` is `Int64` (catalog has a negative sentinel) |
| `content_dim` + `content_dict` | ReplacingMergeTree + **Dictionary** | metadata enrichment; state machine (D2) and backfill use a **LEFT JOIN content_dim FINAL**, not `dictGet` — a Cloud dictionary reload is node-local, so a stale replica can silently serve wrong `video_type`/`category`. The dictionary is kept only for the UI's display-only title lookup, where staleness doesn't affect correctness |
| `session_intervals` | ReplacingMergeTree(version), `ORDER BY video_session_id` | truly-active intervals from the §3 state machine — **one row per session**, holding all current islands as `Array(Tuple(active_start, active_end))`. A refresh that re-derives fewer islands than before ships a shorter array; keying on `video_session_id` alone means the whole prior row is superseded, so stale islands can't survive under `FINAL` (the old per-`interval_idx`-row key could leak ghost rows) |
| `session_last_seen` | AggregatingMergeTree | one row per session (`last_ts`), fed incrementally off `events_raw`; lets D2's "which sessions are recent" check read a tiny table instead of full-scanning `events_raw` every 30s |
| `dim_values` | ReplacingMergeTree | tiny `(dim, value)` table fed off `events_raw`, so populating UI filter dropdowns doesn't force a cold `FINAL` + hot union scan of `concurrency_now` |
| `concurrency_cold_abs` | MergeTree, `ORDER BY (country,platform,video_type,category,minute,content_id)` | COLD: absolute `concurrent` per `(dims, minute)`, `minute ≤ watermark`; `content_id` is `Int64` |
| `concurrency_hot_abs` | MergeTree, same ORDER BY | HOT: absolute per `(dims, minute)`, `minute > watermark`; 30s REPLACE-recompute |
| `concurrency_now` | VIEW | `cold_abs` ∪ (`hot_abs` WHERE `minute > coalesce(max(cold minute), toDateTime(0))`) — coalesced so an empty cold tier (pure-live deployment) doesn't silently hide every hot row |

**Removed as unnecessary:** `events_stg` (batch uses a TEMPORARY staging table), `kpi_minute` + `user_first_seen` (**KPIs computed at query time** from `events_raw` with `uniqExact` — dup-safe, nothing to keep in sync).

**Best-practice choices:** serving `ORDER BY` low→high cardinality (minute before high-card content_id); Enum8 + LowCardinality; no daily partitioning; **dictionary** source `USER 'default'` (reload after loading `content_dim`; Cloud caveat noted). Serving tables are plain MergeTree — one row per `(dims, minute)` written once per build/refresh.

## 6. Files (`SonyLiv/solution/`)
`schema/` is a numbered read/write pipeline: `00`-`04` are the WRITE/build steps,
`05`-`06` are READ/validate, and `ui_queries.sql` + `tuning_variants.sql` are
ad-hoc READ tools.

| File | Purpose |
|---|---|
| `PLAN.md` | the single design/requirements doc (this file) |
| **`sql/00_config.sql`** | **tunable knobs (SQL UDFs): bucket width, heartbeat/gap buffer, dim normalization**. Run FIRST; re-run after any change. |
| **`sql/01_schema.sql`** | **tables + dictionary + `concurrency_now` view + all MVs** (ingestion, live derivation, hot, **and cold compaction — now a real `REFRESH EVERY` MV, not a manual step**) + `concurrency_ext_abs` DDL + the DDL for `concurrency_sa_abs`/`concurrency_si_abs`. Run once. |
| **`sql/ui_queries.sql`** | **dashboard / insight queries** (filter→sum→max/avg; lenient string params; identical 5-dim filter block on every query; `WITH FILL` densification; extended drill-down query reads `concurrency_ext_abs`) |
| `sql/02_seed.sql` | placeholder data (mapping table + dictionary + sample sessions) to smoke-test / seed the offline build |
| `sql/03_backfill.sql` | *(optional)* one-shot build of cold/hot from static data |
| `sql/06_verify.sql` | serving == brute force; **independent raw→intervals oracle (structurally different derivation, catches state-machine bugs the brute-force check can't)**; pause-correctness; disjoint tiers; VideoSessionStart lead-in diagnostic — all bucket/knob-aware (`00_config.sql`) |
| `sql/tuning_variants.sql` | **offline knob-sweep** for the foreground-resume/grace/gap coin-flips — run once real benchmark answers exist, don't hardcode a guess |
| **`sql/04_approaches.sql`** | one file with the INSERT jobs for both required approaches + extended dims: session-aware comparable table `concurrency_sa_abs` (from `session_intervals`), session-independent table `concurrency_si_abs` (per-event state, no interval reconstruction), and **extended drill-down table `concurrency_ext_abs`** (core + app/player/audio/subtitle dims); rolls up to core |
| **`sql/05_compare.sql`** | asserts session-aware == session-independent == `concurrency_now` (0 mismatches), plus the extended→core roll-up cross-check and per-approach smoke queries |

**Live:** `01_schema.sql` → point ClickPipes (Redpanda→`events_incoming`) + start producer (MVs auto-populate, including cold compaction) → `ui_queries.sql`. **Smoke test:** `01_schema.sql` → `02_seed.sql` → `ui_queries.sql`. **Offline:** `00_config.sql` → `01_schema.sql` → `02_seed.sql` → `03_backfill.sql` → `04_approaches.sql` → `05_compare.sql` → `06_verify.sql` (or `run_sql.py --all`).

## 7. 24-hour scope
- **Must:** model + hot/cold serving + correctness (vs brute force) + unseen-day runbook.
- **Then:** minimal dashboard (curve + filters + KPIs) + ClickStack.
- **Stretch:** LibreChat+MCP chat, Langfuse, drill-down / engagement / QoE panes, incident RCA, capacity.
- **Guardrail:** if core isn't validated by ~h12, cut all stretch; a correct, fast, evidenced core wins.

## 8. Verification (`sql/06_verify.sql`)
Brute-force reference (per-minute explosion counting distinct sessions, incl. pause+resume-in-a-minute) must equal `concurrency_now` totals per minute — **expect 0 mismatches**. This only checks the expand+aggregate step, though — it shares `session_intervals` as input with serving, so it can't catch a bug in the state machine itself. **Post-review fix:** added an independent oracle that re-derives active intervals straight from `events_raw` using a structurally different technique (arrays + `arrayFill` + index-lookahead, not the production pipeline's window functions), diffed against serving via `ANTI JOIN` at (session, minute) grain. Also compare vs the naive "heartbeat-in-minute" rule to quantify the paused-time overcount we avoid, and run the `VideoSessionStart` lead-in check before deciding whether it should be a +1 start. Prove latency + `read_rows` via `system.query_log`. Test late-heartbeat/open-session update (live path). Produce unseen-day answers + query-log evidence.


## 9. Open knobs & guardrails
**Live-traffic tuning (see `01_schema.sql` header):** `events_incoming = Null` (no landing) · large ClickPipes batches + async inserts · `events_raw` monthly-partitioned + 30d TTL · **bounded windows** (derivation 20 min, hot 10 min, freeze 10 min — recompute ∝ window × active sessions, tighten to p99 lag) · cold append-only forward-fill (finalized minutes never recomputed) · `cold_abs` = ReplacingMergeTree (retry-safe) read `FINAL` · scale-out by sharding on `video_session_id`, size to peak concurrency not event volume.
**Validate vs judges' key:** 90s gap / 60s grace · overlap vs point-sample minute semantics · buffering active/inactive (highest-weight toggle, ~132K events) · ads counted · hot-window width (from measured p99 lag).
**Guardrails (lower-severity fixes):**
- **Timezone:** all minute buckets are UTC; pin to whatever the benchmark uses (India-facing dashboards may expect IST) — one `toTimeZone` at the edge, not in the model. *(Fix #9)*
- **Ingest dedup:** `events_raw` is plain MergeTree, so a retried load duplicates rows. Dedup by `(video_session_id, event_timestamp, event_type, event)` or reload cleanly before the unseen-day run. *(Fix #10)*
- **Cold key cardinality:** keep only core dims `(country, platform, video_type, category, content_id)` in `cold_abs`/`hot_abs`; put high-card drill-down dims (`app_version, player_version, audio_language, subtitle_language`) in a **separate extended path**, not the core key. *(Fix #7)*
- **Per-interval platform/user_id (fixed, was a gap):** ~95 sessions (0.9%) span >1 platform and ~120 span >1 user_id (e.g. a device switch mid-session) — `session_intervals.intervals` originally collapsed both to one value per whole session via `any(...)`, which the independent `benchmark/benchmark.py` oracle caught as ~7,180+ mismatched (dims, minute) cells. **Fix:** platform and user_id now ride *inside* each interval tuple `(active_start, active_end, platform, user_id)` instead of as session-level columns, and the state machine's island-merge forces a new island on a platform OR user_id change (not just a time gap), so every interval's platform/user_id is exact by construction, not approximated. See `schema/migrations/002_session_intervals_append_and_platform_per_interval.sql`.
- **`mv_session_intervals` missing `APPEND` (fixed, was critical data loss):** the refreshable MV populating `session_intervals` had no `APPEND` on its `REFRESH ... TO session_intervals` clause, so every 30s cycle fully REPLACED the table with only sessions active in the last 20 minutes — silently destroying the one-shot historical backfill and the entire seeded CSV batch (~13,397/42,990+ sessions, ~24%) within ~30s of the view being created. Caught by `benchmark/benchmark.py` check B8 (~10% undercount in total foreground session-minutes) reproducing on days-old, fully-settled data. Fixed alongside the platform/user_id change in the same migration.
- **Replay:** pass `as_of = max(event_timestamp)` on the static set to exercise hot/cold; default `now()` for live. *(Fix #6)*
- **Real-time perf pass (applied):** `session_intervals` given a 3-day TTL (was unbounded); the hot MV and the compaction template now filter to their time window **before** the `ARRAY JOIN` expansion instead of after (provably equivalent — a row outside the window can't produce a minute inside it — but avoids re-expanding the table's full retained history every cycle); derivation cadence tightened 1min→30s for lower end-to-end latency.
- **Known open architecture limitation (not fixed — flagged as a known limit):** `mv_session_intervals` re-derives each active session's **full event history** on every refresh, so per-session cost scales with **session duration**, not just the 20-min activity window. Fine at hackathon scale; for hours-long live-sport sessions at real scale, the correct fix is an **incremental per-session cursor** (carry `watching` state + `last_processed_ts` forward, process only new events each cycle, keep a stable monotonic `interval_idx`) instead of full re-derivation — a genuine architecture change (an incremental retract/re-assert model is this same idea), not implemented here due to time/testing-risk trade-off. This also removes a related edge case: today's positional `interval_idx` (assigned by island order within each re-derivation run) could theoretically leave a stale row un-superseded if the island count for a session ever *shrinks* between runs (e.g. late out-of-order data bridges two previously-separate islands) — rare, but a real correctness edge case the cursor design would eliminate by construction.

## 10. Status & next step
**Decision locked:** our active-interval logic + PLAN2's serving tier (this §4). PLAN2 as written is rejected on correctness (pause overcount).

**All 10 pitfalls are resolved AND the SQL is now rewritten to match** (§6 file list). Final implementation choice: both tiers store **absolute** concurrency per `(dims, minute)` (not deltas), so queries are `filter → sum → max/avg` with **no cumsum and no carry-in** — the simplest correct form. Cold/hot disjointness is enforced by the serving view (`hot WHERE minute > max(cold minute)`), giving race-free compaction.

**Final-review outcome:** (a) reconciled this doc to the implemented **absolute-both** design; (b) fixed the live-path aged-minute bug (30-min derivation window ≫ 10-min freeze horizon); (c) validated additive-across-dims (1/0/95 sessions span content/country/platform); (d) **fixed same-ms nondeterminism** by collapsing per `(session, ms)` (deactivate>reactivate>neutral) — a same-timestamp tie-breaking pitfall seen in comparable builds; (e) **trimmed the schema** to 5 tables + dict + view (dropped `events_stg`, `kpi_minute`, `user_first_seen`); **KPIs now computed at query time** (`uniqExact`, dup-safe); (f) **kept the ClickHouse dictionary** for enrichment (with a Cloud reload caveat + JOIN fallback noted).

**Static path is correct-by-design; verified only once `05` runs (0 mismatches).** Remaining honest caveats: SQL **not yet executed** on ClickHouse (expect minor engine fixes — window frames, `leadInFrame`, refreshable-MV `SYSTEM REFRESH`, `ARRAY JOIN range()` on multi-hour intervals); ground-truth semantics toggles (overlap, buffering) pinned only on the real benchmark; live path (`06_live`) needs a live run to confirm the buffer/compaction cadence.

**Next:** connect to CH Cloud → `01_tables` → `02_load` → `03_build` → `05_verification` (must show 0 mismatches) → `04_ui_queries`; then ClickStack + dashboard.

## 11. Success metrics (acceptance)
| # | Metric | Target |
|---|---|---|
| M1 | Benchmark answers vs private ground truth | Match; foreground-only (no paused/background overcount) |
| M2 | Minute-grain filtered query latency | Dashboard-grade; reads serving layer not `events_raw` (proven via `query_log`) |
| M3 | Update handling | Late heartbeat / open session reflected incrementally, no rebuild |
| M4 | Scale | Serving size ∝ minutes × dim-combos, independent of event volume |
| M5 | Unseen day | Answers + latencies + pipeline evidence produced |
| M6 | Integration | ClickStack observing ingestion lag + query latency |

## 12. Team split (4 people)
Freeze two contracts at hour 0 (run `01_tables.sql`) so all tracks parallelize: **ingestion** → `events_raw`; **serving** → read only `concurrency_now` + `kpi_minute` (never raw).

| Track | Owner | Scope | Tools |
|---|---|---|---|
| **A — Model & serving** *(critical path)* | strongest SQL | `01_tables`/`03_build`/`05_verification`; state machine → absolute cold/hot; benchmark runner; verification == 0 mismatches | ClickHouse SQL |
| **B — Live pipeline & unseen-day** | infra/ops | **CSV-reader → Redpanda producer service** (JSONEachRow, key=session_id); **ClickPipes** (Kafka→`events_incoming`, `07`); live serving jobs (`06_live`: derivation MV + hot MV + cold-compaction scheduler); rehearsed **unseen-day runbook** | Redpanda, ClickPipes, `clickhouse-client` |
| **C — Dashboard** | frontend | curve + filters + KPI tiles + latency badge from `concurrency_now` (`04_ui_queries`); stretch: drill-down, replay | React (Recharts) or Streamlit |
| **D — Integrations & obs.** | integrations/LLM | **ClickStack** (committed; tune watermark from p99 lag); stretch: LibreChat+CH MCP chat, Langfuse, decline alert | ClickStack, LibreChat, MCP, Langfuse |

**Timeline:** h0–2 contracts+scaffolds · h2–8 build in parallel · h8–12 **verification green** + compaction + real-data dashboard · h12–18 tune + stretch · h18–22 **dry-run unseen-day runbook** · h22–24 write-up + demo.
**Unseen-day drill:** B points the CSV-reader service at the sealed file → it streams through Redpanda → ClickPipes → `events_raw`; live serving updates itself → A runs the benchmark on the live serving layer + captures answers/latency/`query_log` evidence → C shows the dashboard filling in real time, D shows ClickStack confirming ingest lag held.
**Guardrail:** if A isn't verified by ~h12, cut all stretch; A+B focus on the unseen-day runbook.

---

## 13. Competitive-review hardening pass (applied)

`review/COMPETITIVE_REVIEW.md` audited this plan + `sql/*.sql` against the other three
SonyLIV-concurrency submissions and found our serving architecture strongest of the
four, but our correctness *evidence* weakest — the axis judging weights hardest ("no
pipeline evidence, no credit"). The following P0/P1 fixes from that review are now
implemented in `sql/*.sql` (schema/views/strategy only — pipeline execution, ClickStack,
dashboard UI, and the sealed-run harness are separate, still-open workstreams, not
covered by this pass):

**Fixed:**
- `content_id` changed `UInt64` → `Int64` everywhere — the catalog's negative sentinel
  (`-987654322`) would otherwise abort the load or wrap into a garbage huge number,
  silently breaking joins/filters on that content.
- `session_intervals` restructured from one-row-per-interval to **one row per session**
  (`Array(Tuple(active_start, active_end))`, `ORDER BY video_session_id`) — closes a
  ghost-interval risk where a session re-deriving fewer islands than a previous refresh
  left stale high-`interval_idx` rows behind under `FINAL`.
- Content enrichment moved from `dictGet` to a **LEFT JOIN content_dim FINAL** on the
  state-machine path (D2 + backfill) — a ClickHouse Cloud dictionary reload is
  node-local, so a stale replica could silently serve wrong `video_type`/`category`.
  Dictionary kept only for the UI's display-only title lookup.
- Hour/day average bug fixed: `ui_queries.sql` query 5 now densifies zero-activity
  minutes with `WITH FILL` before averaging — previously, minutes absent from the data
  were skipped instead of counted as zero, biasing the average high.
- `(next_ts - ts) <= 90` (ambiguous unit on `DateTime64` subtraction) replaced with
  `dateDiff('second', ts, next_ts) <= 90` in the gap test.
- `concurrency_now` view now coalesces `max(minute) FROM concurrency_cold_abs` against
  `toDateTime(0)` — an empty cold tier (pure-live deployment, nothing compacted yet)
  previously turned into `minute > NULL`, silently hiding every hot row.
- Cold compaction (D4) converted from a commented-out manual INSERT into a real
  `REFRESH EVERY 1 MINUTE` materialized view, `DEPENDS ON` the hot MV — previously
  nothing populated `concurrency_cold_abs` in pure-live mode.
- Added `session_last_seen` (tiny incremental table) so D2's "which sessions are
  recent" check no longer full-scans `events_raw` every 30s — recompute cost is now
  O(active sessions), matching this doc's original claim rather than O(history).
- Added `dim_values` (tiny incremental table) so UI filter dropdowns read a small table
  instead of forcing a cold `FINAL` + hot union scan of `concurrency_now`.
- `ui_queries.sql`: the same 5-dim filter block now applies to every query (previously
  the KPI/breakdown queries filtered on platform only, or nothing, while the curve
  filtered all five — so KPI tiles could disagree with the filtered chart); content_id
  param parsing switched `toUInt64OrZero` → `toInt64OrZero` (same negative-sentinel bug,
  UI-side).
- `06_verify.sql`: added the independent raw→intervals oracle described in §8, plus a
  `VideoSessionStart` lead-in diagnostic and a documented cross-combo double-count note.

**Shipped as tooling, not silently changed (need real data to resolve):**
- `sql/tuning_variants.sql` — parameterized sweep for the foreground-resume / grace /
  gap knobs (§9's "one knob" and the AppForegrounded reactivation question). Run once
  benchmark answers exist; don't hand-pin a guess into production first.
- `VideoSessionStart` transition classification (currently neutral in `multiIf`) — the
  `06_verify.sql` lead-in check tells you whether it should be a +1 start; not flipped
  blind.

**Still open (out of scope for this pass — separate workstreams):** actually running
the Redpanda→ClickPipes pipeline end-to-end on Cloud; ClickStack integration; dashboard
UI/visuals; the sealed-run harness (checksums + clean git tree + `query_log.tsv`
packaging).

---
