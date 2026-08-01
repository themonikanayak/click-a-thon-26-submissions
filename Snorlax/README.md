# Snorlax

Trial run - hello there!

## Project
**Foreground-Only Concurrency for SonyLIV** — how many sessions are *truly watching*
each minute, excluding paused, backgrounded, and heartbeat-missing time, at
dashboard-grade latency.

## Team Members
- Name (GitHub handle) <!-- TODO: fill in team roster -->

## What it does
Streaming video platforms need an accurate, real-time answer to *"how many people
are actually watching right now?"* — not just how many sessions exist. A session
that is paused, backgrounded, mid-ad, or silently abandoned should **not** count
as active.

Snorlax computes **foreground-only concurrency**: the number of distinct sessions
that are genuinely in active playback per `(dimensions, minute)`, where dimensions
are `country`, `platform`, `video_type`, `category`, and `content_id`. It answers:

- Concurrency at any minute, filtered by any dimension combination.
- Peak / average concurrency over an arbitrary range.
- Incremental, near-real-time updates for still-open ("live") sessions.

It handles the hard edge cases explicitly: paused / backgrounded gaps, ad breaks,
seek/buffering stalls, playback errors that recover, silently abandoned sessions
(heartbeat gaps), late-arriving / out-of-order heartbeats, and long-lived live
sessions still open past the window.

## How we built it
**Engine:** ClickHouse Cloud (database `sonyliv_concurrency`).

**Pipeline (live path):**

```
CSV/producer → Redpanda → ClickPipes → events_incoming ─(MV)→ events_raw
   ─(MV: state machine)→ session_intervals ─(MV: hot 30s)→ concurrency_hot_abs
   + cold compaction (~1 min)             → concurrency_cold_abs
   concurrency_now  =  cold ∪ hot  (disjoint by minute)
```

**Core design decisions** (see [`plan/PLAN.md`](plan/PLAN.md) for the full reasoning):

- **Absolute concurrency per `(dims, minute)`** — no cumulative sum, no carry-in,
  no base term. Every query is `filter → sum → max/avg`, the simplest correct form.
  Instantaneous counts are additive across dimensions (verified on the data).
- **One active definition, a state machine per session.** `VideoPlay`,
  `AppForegrounded`, `resume`, `speed-resume`, `AdResume` activate; `pause`,
  `speed-pause`, `AppBackgrounded`, `VideoSessionEnd`, `VideoError`, `AdPause`
  deactivate. Heartbeat silence **> 90s** closes a stretch (**60s** grace);
  it reopens on the next active event.
- **Determinism fix.** ~29% of events share a millisecond timestamp and engine tie
  order is unstable, so events are first collapsed per `(session, ms)` with priority
  **deactivate > reactivate > neutral** — identical results locally and on Cloud.
- **Overlap minute semantics + once-per-minute dedupe.** A session counts in a
  minute if active for any part of it; `uniqExact` counts each session once per
  minute regardless of how many active stretches it has.
- **HOT/COLD tiering.** Recent minutes are REPLACE-recomputed every ~30s (bounded
  work); finalized minutes are frozen into cold storage. The serving view reads hot
  only for `minute > max(cold minute)`, so the tiers stay disjoint even mid-compaction.

**Two approaches + a comparison** (the problem asks for both):

- **Session-aware** — reconstruct per-session truly-active intervals via the state
  machine (`session_intervals`), expand to minutes, count distinct sessions.
- **Session-independent** — derive per-event foreground state directly (no interval
  reconstruction), expand active segments to minutes, count distinct sessions.
- **Comparison** — `schema/compare_approaches.sql` asserts the two agree (and match
  `concurrency_now`) per `(dims, minute)`; expect **zero mismatches**.

Both share one active definition; because we count distinct sessions per minute with
`uniqExact`, interval-merging (session-aware) vs not-merging (session-independent) is
irrelevant to the result — a nice cross-check.

**Producer:** a Python simulator (`producer/produce_events.py`) streams realistic
video-session events (including every edge case above) into ClickHouse via
server-side async inserts. It scales out to high volume with a pool of worker
threads (`PRODUCER_THREADS`) and, if one process can't saturate the endpoint,
forked instances (`PRODUCER_PROCESSES`) from a single command.

**Coding standards:** SQL / Python / Java best practices are documented in
[`.claude/best-practices/`](../.claude/best-practices/) and referenced from
`.claude/CLAUDE.md`.

## How to run it

### 1. Set tunable knobs, then build the schema
`schema/config.sql` is the single place for tunable parameters (bucket width,
heartbeat/gap tolerance), exposed as SQL UDFs. **Run it first**, then the schema:
```bash
clickhouse client --host <host> --user default --secure --queries-file Snorlax/schema/config.sql
clickhouse client --host <host> --user default --secure --queries-file Snorlax/schema/schema.sql
```
`schema.sql` creates all tables, the `content_dict` dictionary, the
`concurrency_now` serving view, and the materialized views. Re-run `config.sql`
after any knob change, then rebuild.

### 2. Get data in
**Live (primary path):** point ClickPipes at your Redpanda topic
(`Redpanda → events_incoming`, `JSONEachRow`, key = `video_session_id`), then run
the producer:
```bash
cd Snorlax/producer
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # then fill in your ClickHouse credentials
python produce_events.py    # Ctrl-C to stop (flushes buffered rows first)

# High volume: EVENTS_PER_SECOND is the PER-WORKER rate (0 = unthrottled).
# Aggregate ≈ EVENTS_PER_SECOND × PRODUCER_THREADS × PRODUCER_PROCESSES.
EVENTS_PER_SECOND=0 PRODUCER_THREADS=16 PRODUCER_PROCESSES=4 python produce_events.py
```

Scale-out knobs (all read from `.env` / the environment):

| Var | Default | Meaning |
| --- | --- | --- |
| `PRODUCER_THREADS` | `8` | Worker threads per process (each owns its own client, session pool, batch). |
| `PRODUCER_PROCESSES` | `1` | Forked producer processes from one command; total workers = threads × processes. |
| `EVENTS_PER_SECOND` | `200` | **Per-worker** target rate; `0` = unthrottled (max speed). |
| `MAX_CONCURRENT_SESSIONS` | `500` | Live-session pool size **per worker**. |
| `WAIT_FOR_ASYNC_INSERT` | `1` | `0` trades durable ack for more throughput (landing table is append-only + deduped downstream). |

You can also just run `python produce_events.py` in several shells (or across
machines) for the same effect as `PRODUCER_PROCESSES`.

**Offline / backfill (fallback):** load static events, then build the serving
tables. Run in this order:
```
schema.sql
  → load_sample_csv.sql   (or seed_sample.sql for a smoke test)
  → backfill_history.sql
  → approach_session_aware.sql
  → approach_session_independent.sql
  → compare_approaches.sql   (expect 0 mismatches)
  → verify.sql
```

### 3. Query it
Dashboard / insight queries live in `schema/ui_queries.sql` (filter → sum →
max/avg against `concurrency_now`).

> Note: the SQL is designed correct-by-construction but has not yet been executed
> end-to-end on a live ClickHouse instance — expect minor engine fixes (window
> frames, `leadInFrame`, refreshable-MV refresh, `ARRAY JOIN range()` on multi-hour
> intervals) on first run.

### 4. Dashboard & Insights Copilot (Streamlit)
A Streamlit dashboard renders the concurrency curve, KPIs, and breakdowns from
`concurrency_now`, plus an **Insights Copilot** chat grounded in the current
filters. The Copilot routes through a **local LibreChat on Docker** (which runs
the model on Ollama), with a direct-Ollama fallback.

```bash
cd Snorlax/sonyliv-dashboard-py
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py        # http://localhost:8501
```
- Dashboard details & the Copilot's LibreChat wiring: [`sonyliv-dashboard-py/README.md`](sonyliv-dashboard-py/README.md).
- Running LibreChat locally on Docker (agent + API key setup): [`librechat-setup/README.md`](librechat-setup/README.md).

## Repository layout
```
Snorlax/
  README.md              ← this file
  plan/PLAN.md           ← the single design + decisions doc
  producer/              ← Python event producer (streams into ClickHouse)
  schema/                ← ClickHouse DDL, MVs, backfill, approaches, comparison, verify
  sonyliv-dashboard-py/  ← Streamlit dashboard + Insights Copilot (see its README)
  librechat-setup/       ← LibreChat config + local-Docker run guide (see its README)
```
The problem statement and design background live in `SonyLiv/` (docs only — no code).

## Demo
_Link to a live demo or video, if available._
