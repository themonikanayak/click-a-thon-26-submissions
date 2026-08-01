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
- **+1 start:** `VideoPlay`, `resume`, `speed-resume`, `AppForegrounded` (`AdResume`).
- **−1 stop:** `pause`, `speed-pause`, `AppBackgrounded`, `VideoSessionEnd`, `VideoError` (`AdPause`).
- Heartbeats → no change, refresh `last_seen`. Silence > 90s → close at `last_seen+60s`, reopen next event. Still-open → extend to `last_seen+60s`, provisional. Repeats ignored.
- **Determinism (fix):** events are first **collapsed per `(session, millisecond)`** with priority **deactivate > reactivate > neutral**. ~29% of events share a timestamp and tie order is engine-unstable → this collapse guarantees identical results locally and on Cloud, and stops a neutral heartbeat from ever cancelling a pause at the same instant.
→ produces truly-active intervals `[active_start, active_end)`.

**Step 2 — absolute per (dims, minute).** Expand each session's intervals to the minutes they cover and count **distinct sessions** per `(dims, minute)` (`uniqExact` → the once-per-minute dedupe, for free). Store this absolute count in the serving tables. *(Verified on the data: a session has one content_id/country and ~one platform (only 1/0/95 sessions span each), so its dim-tuple is effectively constant → absolute counts are **additive across dims**.)*

**Step 3 — serve.** Concurrency(*m*, filters) = `sum(concurrent)` over matching dim rows at minute *m*. Peak = `max` over the range; average = `sum(concurrent) / (#minutes in range)` (zero minutes included in the denominator); hour/day = aggregate the minute values. **No cumulative sum, no carry-in, no base-term** — absolute counts are directly summable. Peak per dimension combo falls out of filter+group at query time.

**Edge cases:** missed beat <90s → stay; long silence → gap excluded; no-play → 0 minutes; out-of-order → sort by event time; duplicates → ignored; end≤start/orphans → dropped; bot 301-session user → flag.

## 4. Architecture — HOT/COLD tiering (absolute, pitfall-fixed)
Both tiers store **absolute concurrency per `(dims, minute)`** (instantaneous counts are additive across dims). This makes every query `filter → sum → max/avg` — the simplest correct form, **no cumsum / carry-in / base-term anywhere**. (We rejected PLAN2's active definition on correctness — its "heartbeat-in-minute" rule overcounted ~50% of paused windows, verified — but kept its absolute-serving idea.) Split by a minute watermark:

- **COLD** (`minute ≤ watermark`) = absolute per `(dims, minute)`, frozen. Direct read.
- **HOT** (`minute > watermark`) = absolute per `(dims, minute)` for recent minutes, **REPLACE-recomputed** every ~30s from the recently-active sessions (bounded work). *(Fix #1: REPLACE — not append-only — sidesteps the open-session "moving −1" trap; no per-run bookkeeping.)*
- **Serving VIEW `concurrency_now`** = cold ∪ hot, with **hot read only for `minute > max(cold minute)`**. *(Fix #5: a minute is hidden from hot the instant it lands in cold → cold/hot disjoint even mid-compaction, no double-count race.)*
- **Compaction (hot→cold):** freezing a minute = copy its (already-absolute) hot rows into `cold_abs`; the derived watermark advances and the view stops reading them from hot. *(Fix #2: no snapshot layer — absolute cold needs none. Fix #3: hot cleanup is lazy hourly TTL/partition, never per-minute.)*
- **Replay/`as_of`** *(Fix #6):* watermark = `as_of − hot_window` (default `now()`; set `as_of = max(event_timestamp)` on the static set to exercise hot). Plain static load with `now()` → everything cold (correct).
- **Watermark width = the one knob:** size to p99 heartbeat lag via ClickStack. Wide hot = correctness under late data; narrow hot = faster reads.
- **Scale caveat (honest):** the absolute *build* expands intervals → minutes (a per-session-minute **intermediate** before the `uniqExact` group-by). The stored output is compact (minutes × dim-combos, independent of session count), but that intermediate is bounded by total active session-minutes. Fine at sample size and for the small hot window; for extreme scale (multi-hour sessions at 100×) switch the **cold build** to delta→cumsum-per-combo (2 rows/run → cumsum over a dense per-combo minute axis), which avoids per-session expansion. Query side is unchanged.

## 5. Tables & views (lean — 5 tables + 1 dict + 1 view + 1 Null landing)
| Object | Engine | Role |
|---|---|---|
| `events_incoming` | MergeTree (+2d TTL) | **ClickPipes landing** from Redpanda (JSON, ms epochs); `mv_incoming_to_raw` casts → `events_raw` |
| `events_raw` | MergeTree, `ORDER BY (session, ts)` | **canonical typed events**; fed by streaming MV [07] + batch load [02]; dup-tolerant (state machine collapses per (session,ms) & ignores repeats) |
| `content_dim` + `content_dict` | ReplacingMergeTree + **Dictionary** | metadata enrichment via `dictGet` |
| `session_intervals` | ReplacingMergeTree(version) | truly-active intervals from the §3 state machine |
| `concurrency_cold_abs` | MergeTree, `ORDER BY (country,platform,video_type,category,minute,content_id)` | COLD: absolute `concurrent` per `(dims, minute)`, `minute ≤ watermark` |
| `concurrency_hot_abs` | MergeTree, same ORDER BY | HOT: absolute per `(dims, minute)`, `minute > watermark`; 30s REPLACE-recompute |
| `concurrency_now` | VIEW | `cold_abs` ∪ (`hot_abs` WHERE `minute > max(cold minute)`) |

**Removed as unnecessary:** `events_stg` (batch uses a TEMPORARY staging table), `kpi_minute` + `user_first_seen` (**KPIs computed at query time** from `events_raw` with `uniqExact` — dup-safe, nothing to keep in sync).

**Best-practice choices:** serving `ORDER BY` low→high cardinality (minute before high-card content_id); Enum8 + LowCardinality; no daily partitioning; **dictionary** source `USER 'default'` (reload after loading `content_dim`; Cloud caveat noted). Serving tables are plain MergeTree — one row per `(dims, minute)` written once per build/refresh.

## 6. Files (`SonyLiv/solution/`)
| File | Purpose |
|---|---|
| `PLAN.md` | the single design/requirements doc (this file) |
| **`sql/config.sql`** | **tunable knobs (SQL UDFs): bucket width, heartbeat/gap buffer, dim normalization**. Run FIRST; re-run after any change. |
| **`sql/schema.sql`** | **tables + dictionary + `concurrency_now` view + all MVs** (ingestion, live derivation, hot) + `concurrency_ext_abs` DDL. Run once. |
| **`sql/ui_queries.sql`** | **dashboard / insight queries** (filter→sum→max/avg; lenient string params) |
| `sql/seed_sample.sql` | placeholder data (mapping table + dictionary + sample sessions) to smoke-test |
| `sql/load_sample_csv.sql` | *(optional)* batch-load the provided CSVs → `events_raw` |
| `sql/backfill_history.sql` | *(optional)* one-shot build of cold/hot from static data |
| `sql/verify.sql` | *(optional)* serving == brute force; pause-correctness; disjoint tiers |
| **`sql/approach_session_aware.sql`** | session-aware comparable table `concurrency_sa_abs` (from `session_intervals`) |
| **`sql/approach_session_independent.sql`** | session-independent table `concurrency_si_abs` (per-event state, no interval reconstruction) |
| **`sql/approach_extended_dims.sql`** | **extended drill-down table `concurrency_ext_abs`** (core + app/player/audio/subtitle dims); rolls up to core |
| **`sql/compare_approaches.sql`** | asserts session-aware == session-independent == `concurrency_now` (0 mismatches) |

**Live:** `schema.sql` → point ClickPipes (Redpanda→`events_incoming`) + start producer (MVs auto-populate) → schedule cold-compaction (comment in `schema.sql` D4) → `ui_queries.sql`. **Smoke test:** `schema.sql` → `seed_sample.sql` → `ui_queries.sql`. **Offline w/ CSVs:** `schema.sql` → `load_sample_csv.sql` → `backfill_history.sql` → `verify.sql`.

## 7. 24-hour scope
- **Must:** model + hot/cold serving + correctness (vs brute force) + unseen-day runbook.
- **Then:** minimal dashboard (curve + filters + KPIs) + ClickStack.
- **Stretch:** LibreChat+MCP chat, Langfuse, drill-down / engagement / QoE panes, incident RCA, capacity.
- **Guardrail:** if core isn't validated by ~h12, cut all stretch; a correct, fast, evidenced core wins.

## 8. Verification (`sql/05_verification.sql`)
Brute-force reference (per-minute explosion counting distinct sessions, incl. pause+resume-in-a-minute) must equal `concurrency_now` totals per minute — **expect 0 mismatches**. Also compare vs the naive "heartbeat-in-minute" rule to quantify the paused-time overcount we avoid. Prove latency + `read_rows` via `system.query_log`. Test late-heartbeat/open-session update (live path). Produce unseen-day answers + query-log evidence.

## 9. Open knobs & guardrails
**Live-traffic tuning (see `schema.sql` header):** `events_incoming = Null` (no landing) · large ClickPipes batches + async inserts · `events_raw` monthly-partitioned + 30d TTL · **bounded windows** (derivation 20 min, hot 10 min, freeze 10 min — recompute ∝ window × active sessions, tighten to p99 lag) · cold append-only forward-fill (finalized minutes never recomputed) · `cold_abs` = ReplacingMergeTree (retry-safe) read `FINAL` · scale-out by sharding on `video_session_id`, size to peak concurrency not event volume.
**Validate vs judges' key:** 90s gap / 60s grace · overlap vs point-sample minute semantics · buffering active/inactive (highest-weight toggle, ~132K events) · ads counted · hot-window width (from measured p99 lag).
**Guardrails (lower-severity fixes):**
- **Timezone:** all minute buckets are UTC; pin to whatever the benchmark uses (India-facing dashboards may expect IST) — one `toTimeZone` at the edge, not in the model. *(Fix #9)*
- **Ingest dedup:** `events_raw` is plain MergeTree, so a retried load duplicates rows. Dedup by `(video_session_id, event_timestamp, event_type, event)` or reload cleanly before the unseen-day run. *(Fix #10)*
- **Cold key cardinality:** keep only core dims `(country, platform, video_type, category, content_id)` in `cold_abs`/`hot_abs`; put high-card drill-down dims (`app_version, player_version, audio_language, subtitle_language`) in a **separate extended path**, not the core key. *(Fix #7)*
- **Multi-platform sessions:** ~95 sessions (0.9%) span >1 platform; `any(platform)` attributes them to one. Negligible; note it.
- **Replay:** pass `as_of = max(event_timestamp)` on the static set to exercise hot/cold; default `now()` for live. *(Fix #6)*
- **Real-time perf pass (applied):** `session_intervals` given a 3-day TTL (was unbounded); the hot MV and the compaction template now filter to their time window **before** the `ARRAY JOIN` expansion instead of after (provably equivalent — a row outside the window can't produce a minute inside it — but avoids re-expanding the table's full retained history every cycle); derivation cadence tightened 1min→30s for lower end-to-end latency.
- **Known open architecture limitation (not fixed — flagged, like Phoenix's "known limits"):** `mv_session_intervals` re-derives each active session's **full event history** on every refresh, so per-session cost scales with **session duration**, not just the 20-min activity window. Fine at hackathon scale; for hours-long live-sport sessions at real scale, the correct fix is an **incremental per-session cursor** (carry `watching` state + `last_processed_ts` forward, process only new events each cycle, keep a stable monotonic `interval_idx`) instead of full re-derivation — a genuine architecture change (Phoenix's "retraction" model is this same idea), not implemented here due to time/testing-risk trade-off. This also removes a related edge case: today's positional `interval_idx` (assigned by island order within each re-derivation run) could theoretically leave a stale row un-superseded if the island count for a session ever *shrinks* between runs (e.g. late out-of-order data bridges two previously-separate islands) — rare, but a real correctness edge case the cursor design would eliminate by construction.

## 10. Status & next step
**Decision locked:** our active-interval logic + PLAN2's serving tier (this §4). PLAN2 as written is rejected on correctness (pause overcount).

**All 10 pitfalls are resolved AND the SQL is now rewritten to match** (§6 file list). Final implementation choice: both tiers store **absolute** concurrency per `(dims, minute)` (not deltas), so queries are `filter → sum → max/avg` with **no cumsum and no carry-in** — the simplest correct form. Cold/hot disjointness is enforced by the serving view (`hot WHERE minute > max(cold minute)`), giving race-free compaction.

**Final-review outcome:** (a) reconciled this doc to the implemented **absolute-both** design; (b) fixed the live-path aged-minute bug (30-min derivation window ≫ 10-min freeze horizon); (c) validated additive-across-dims (1/0/95 sessions span content/country/platform); (d) **fixed same-ms nondeterminism** by collapsing per `(session, ms)` (deactivate>reactivate>neutral) — the bug the Phoenix build hit; (e) **trimmed the schema** to 5 tables + dict + view (dropped `events_stg`, `kpi_minute`, `user_first_seen`); **KPIs now computed at query time** (`uniqExact`, dup-safe); (f) **kept the ClickHouse dictionary** for enrichment (with a Cloud reload caveat + JOIN fallback noted).

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
