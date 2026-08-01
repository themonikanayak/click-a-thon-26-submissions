#!/usr/bin/env python3
"""
Continuously stream dummy video-session events into ClickHouse Cloud — at volume.

Uses server-side ASYNC INSERTS: rows are batched client-side, then handed to
ClickHouse with async_insert=1 so the server coalesces them into larger parts.
This is the recommended pattern for high-frequency small inserts on Cloud.

High-volume design
------------------
Throughput scales along two axes, both env-tunable:
  * PRODUCER_THREADS   worker threads per process. Each worker owns its own
    ClickHouse client (clickhouse_connect clients are NOT thread-safe), its own
    pool of live sessions, and its own batch — so workers never share mutable
    state on the hot path. Inserts are network-bound, so threads scale well
    despite the GIL.
  * PRODUCER_PROCESSES  OS processes forked from one command (multiprocessing).
    Use this to get past the GIL for row generation and to open more independent
    connections when a single process can't saturate the endpoint. Total
    concurrency = PRODUCER_PROCESSES × PRODUCER_THREADS workers.

EVENTS_PER_SECOND is the PER-WORKER target rate; set it to 0 for unthrottled
max-speed production. Aggregate target ≈ EVENTS_PER_SECOND × PRODUCER_THREADS ×
PRODUCER_PROCESSES.

Feeds sonyliv_concurrency.events_incoming (the ClickPipes landing table); the
mv_incoming_to_raw materialized view fans it out to events_raw automatically.

Every event is stamped with the current wall-clock time (ts = now), so the
concurrency curve builds in near real time. The stream deliberately injects the
edge cases the foreground-only concurrency model has to get right:
  * backgrounded / paused periods that must be excluded from active time
  * ad breaks and seek/buffering stalls (deactivate then reactivate)
  * playback errors that recover
  * silently abandoned sessions (heartbeat gap, no end event)
  * late-arriving / out-of-order heartbeats
  * long-lived "live" sessions still open past the window

Usage:
    cp .env.example .env      # then fill in your credentials
    pip install -r requirements.txt
    python produce_events.py

    # Crank the volume (per-worker rate, 16 threads, 4 processes):
    EVENTS_PER_SECOND=0 PRODUCER_THREADS=16 PRODUCER_PROCESSES=4 python produce_events.py

Or start several independent instances by hand (equivalent to PRODUCER_PROCESSES,
handy across machines): run `python produce_events.py` in multiple shells.

Stop with Ctrl-C (every worker flushes its buffered rows first).
"""
from __future__ import annotations

import multiprocessing
import os
import random
import signal
import string
import sys
import threading
import time
import uuid

import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config (from .env)
# ---------------------------------------------------------------------------
HOST = os.environ["CLICKHOUSE_HOST"]
PORT = int(os.getenv("CLICKHOUSE_PORT", "8443"))
USER = os.getenv("CLICKHOUSE_USER", "default")
PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
SECURE = os.getenv("CLICKHOUSE_SECURE", "true").lower() in ("1", "true", "yes")
DATABASE = os.getenv("CLICKHOUSE_DATABASE", "sonyliv_concurrency")
TABLE = os.getenv("CLICKHOUSE_TABLE", "events_incoming")

# Per-worker target rate; 0 = unthrottled (produce as fast as the client allows).
EVENTS_PER_SECOND = int(os.getenv("EVENTS_PER_SECOND", "200"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
FLUSH_INTERVAL_SEC = float(os.getenv("FLUSH_INTERVAL_SEC", "1.0"))
# Live-session pool size, PER WORKER.
MAX_CONCURRENT_SESSIONS = int(os.getenv("MAX_CONCURRENT_SESSIONS", "500"))

# Scale-out knobs.
PRODUCER_THREADS = int(os.getenv("PRODUCER_THREADS", "8"))
PRODUCER_PROCESSES = int(os.getenv("PRODUCER_PROCESSES", "1"))

# Durable ack per async insert. Drop to 0 for max throughput at the cost of
# at-most-once ack semantics (the landing table is append-only and the pipeline
# dedupes once-per-minute downstream, so retried/best-effort batches are safe).
WAIT_FOR_ASYNC_INSERT = int(os.getenv("WAIT_FOR_ASYNC_INSERT", "1"))

# ---------------------------------------------------------------------------
# Edge-case knobs (the hard cases the concurrency model must handle correctly).
# See SonyLiv/PROBLEM_STATEMENT.md — foreground-only concurrency must exclude
# backgrounded / paused / heartbeat-missing periods.
# ---------------------------------------------------------------------------
# Fraction of events that arrive out-of-order (stamped a few seconds in the past),
# simulating late-arriving heartbeats on flaky networks.
LATE_ARRIVAL_PROB = float(os.getenv("LATE_ARRIVAL_PROB", "0.03"))
LATE_ARRIVAL_MAX_SEC = float(os.getenv("LATE_ARRIVAL_MAX_SEC", "15"))
# Fraction of "live" sessions that never voluntarily end — they stay open past the
# window (still-open sessions whose active ranges keep growing).
MARATHON_FRACTION = float(os.getenv("MARATHON_FRACTION", "0.05"))

# Column order must match the INSERT column list below.
COLUMNS = [
    "content_id", "video_session_id", "user_id", "event_type", "event",
    "event_timestamp", "platform", "app_version", "country",
    "audio_language", "subtitle_language", "player_version", "session_start_epoch",
]

# ---------------------------------------------------------------------------
# Dummy-data dimensions
# ---------------------------------------------------------------------------
CONTENT_IDS = [1001, 1002, 1003, 2001, 2002, 3001, 3002, 3003, 3004]
PLATFORMS = ["android", "ios", "web", "tv", "firetv"]
APP_VERSIONS = ["6.10.1", "6.11.0", "6.12.0", "7.0.0"]
COUNTRIES = ["IN", "US", "GB", "AE", "AU", "CA", "SG"]
AUDIO_LANGS = ["hin", "eng", "tam", "tel", "mar"]
SUBTITLE_LANGS = ["", "eng", "hin"]
PLAYER_VERSIONS = ["exo-2.18", "exo-2.19", "avplayer-1.2", "shaka-4.3"]


def _rand_id(n: int = 12) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


# Inactive sub-states and the (event_type, event) pair that re-activates each one.
# The activate/deactivate vocabulary matches the pipeline's state machine
# (see 01_schema.sql: VideoPlay/AppForegrounded/resume/speed-resume/AdResume = +1;
#  AppBackgrounded/VideoSessionEnd/VideoError/pause/speed-pause/AdPause = -1).
INACTIVE_STATES = {
    "backgrounded": ("AppForegrounded", "resume"),      # app sent to background
    "paused":       ("VideoPlay",       "resume"),       # user hit pause
    "ad":           ("VideoPlay",       "AdResume"),      # mid-roll ad break
    "seeking":      ("VideoPlay",       "speed-resume"),  # seek / buffering stall
    "errored":      ("VideoPlay",       "resume"),        # playback error, recovered
}


class Session:
    """A single simulated viewing session that walks through a lifecycle."""

    def __init__(self, now_ms: int):
        self.sid = str(uuid.uuid4())
        self.user_id = "u_" + _rand_id(10)
        self.content_id = random.choice(CONTENT_IDS)
        self.platform = random.choice(PLATFORMS)
        self.app_version = random.choice(APP_VERSIONS)
        self.country = random.choice(COUNTRIES)
        self.audio = random.choice(AUDIO_LANGS)
        self.subtitle = random.choice(SUBTITLE_LANGS)
        self.player = random.choice(PLAYER_VERSIONS)
        self.start_epoch = now_ms
        self.state = "start"  # start -> playing -> (inactive sub-states) -> ended
        # "Live" marathon sessions never voluntarily end; they stay open past the
        # window so the model must handle still-open sessions.
        self.marathon = random.random() < MARATHON_FRACTION

    def _ts(self, now_ms: int) -> int:
        """Event time — 'now', occasionally stamped a few seconds in the past to
        simulate late-arriving / out-of-order heartbeats."""
        if random.random() < LATE_ARRIVAL_PROB:
            return now_ms - random.randint(1000, int(LATE_ARRIVAL_MAX_SEC * 1000))
        return now_ms

    def _row(self, event_type: str, event: str, now_ms: int) -> list:
        return [
            self.content_id, self.sid, self.user_id, event_type, event,
            self._ts(now_ms), self.platform, self.app_version, self.country,
            self.audio, self.subtitle, self.player, self.start_epoch,
        ]

    def next_event(self, now_ms: int) -> tuple[list | None, bool]:
        """Produce the next event for this session.

        Returns (row, finished). row may be None when the session is *abandoned*
        silently (no VideoSessionEnd) — this is the heartbeat-gap edge case: the
        active interval must be capped by the watermark, not left open forever.
        """
        if self.state == "start":
            # A real session opens with VideoSessionStart immediately followed by
            # VideoPlay (see 02_seed.sql / dataset_details.md). Emit both, in
            # order, so generated data matches that convention. (The pipeline also
            # seeds a session active from VideoSessionStart, so it's correct even
            # if a real feed omits/delays VideoPlay — but the producer shouldn't
            # rely on that.)
            self.state = "started"
            return self._row("VideoSessionStart", "start", now_ms), False

        if self.state == "started":
            self.state = "playing"
            return self._row("VideoPlay", "Play", now_ms), False

        if self.state == "playing":
            roll = random.random()
            if roll < 0.62:
                return self._row("VideoHeartbeat", "heartbeat", now_ms), False
            if roll < 0.67:
                # Redundant play/keepalive; stays active.
                return self._row("VideoPlay", "resume", now_ms), False
            if roll < 0.76:
                self.state = "backgrounded"
                return self._row("AppBackgrounded", "pause", now_ms), False
            if roll < 0.83:
                self.state = "paused"
                return self._row("VideoPause", "pause", now_ms), False
            if roll < 0.89:
                self.state = "ad"
                return self._row("AdBreakStart", "AdPause", now_ms), False
            if roll < 0.93:
                self.state = "seeking"
                return self._row("VideoSeek", "speed-pause", now_ms), False
            if roll < 0.96:
                self.state = "errored"
                return self._row("VideoError", "error", now_ms), False
            if not self.marathon and roll < 0.98:
                # Abandoned: app killed / crashed with no end event (heartbeat gap).
                return None, True
            if self.marathon:
                # Live sessions keep going instead of ending.
                return self._row("VideoHeartbeat", "heartbeat", now_ms), False
            return self._row("VideoSessionEnd", "end", now_ms), True

        if self.state in INACTIVE_STATES:
            event_type, event = INACTIVE_STATES[self.state]
            # Marathon sessions always come back; others sometimes end while paused.
            if self.marathon or random.random() < 0.75:
                self.state = "playing"
                return self._row(event_type, event, now_ms), False
            if random.random() < 0.5:
                # Backgrounded/paused then silently abandoned — no end event.
                return None, True
            return self._row("VideoSessionEnd", "end", now_ms), True

        return self._row("VideoSessionEnd", "end", now_ms), True


def _worker(worker_id: str, stop: threading.Event, stat: dict) -> None:
    """Generate and insert events for one worker until `stop` is set.

    Owns its own ClickHouse client, session pool, and batch — no shared mutable
    state with other workers on the hot path. Publishes progress into `stat`
    (its own dict slot), which the monitor thread reads without locking.
    """
    client = clickhouse_connect.get_client(
        host=HOST, port=PORT, username=USER, password=PASSWORD, secure=SECURE,
    )
    # async_insert=1  : server buffers rows and flushes into big parts.
    # wait_for_async_insert : block until flushed (durable ack) when 1.
    settings = {
        "async_insert": 1,
        "wait_for_async_insert": WAIT_FOR_ASYNC_INSERT,
    }

    sessions: list[Session] = []
    batch: list[list] = []
    last_flush = time.time()
    interval = 1.0 / EVENTS_PER_SECOND if EVENTS_PER_SECOND > 0 else 0

    def flush() -> None:
        nonlocal batch, last_flush
        if not batch:
            return
        client.insert(
            TABLE, batch, column_names=COLUMNS,
            database=DATABASE, settings=settings,
        )
        stat["total"] += len(batch)
        batch = []
        last_flush = time.time()

    try:
        while not stop.is_set():
            now_ms = int(time.time() * 1000)

            # Top up the pool of live sessions.
            while len(sessions) < MAX_CONCURRENT_SESSIONS:
                sessions.append(Session(now_ms))

            # Advance one random session.
            s = random.choice(sessions)
            row, finished = s.next_event(now_ms)
            if row is not None:  # None => silently abandoned (heartbeat-gap case)
                batch.append(row)
            if finished:
                sessions.remove(s)

            stat["live"] = len(sessions)

            # Flush on size or time.
            if len(batch) >= BATCH_SIZE or (time.time() - last_flush) >= FLUSH_INTERVAL_SEC:
                flush()

            if interval:
                time.sleep(interval)
    finally:
        flush()
        client.close()


def _run_process(proc_id: int) -> None:
    """Run PRODUCER_THREADS worker threads in this process until interrupted.

    Installs its own SIGINT/SIGTERM handlers (each forked process receives the
    terminal's Ctrl-C independently) that flip a shared Event so every worker
    flushes and exits cleanly.
    """
    stop = threading.Event()

    def _stop(*_) -> None:
        stop.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    tag = f"proc {proc_id}" if PRODUCER_PROCESSES > 1 else "producer"
    print(
        f"[{tag}] connected to {HOST}:{PORT} → {DATABASE}.{TABLE} | "
        f"{PRODUCER_THREADS} threads × "
        f"{'unthrottled' if EVENTS_PER_SECOND == 0 else f'{EVENTS_PER_SECOND}/s'} each",
        file=sys.stderr,
    )

    stats = [{"total": 0, "live": 0} for _ in range(PRODUCER_THREADS)]
    threads = [
        threading.Thread(
            target=_worker,
            args=(f"{proc_id}.{i}", stop, stats[i]),
            name=f"worker-{proc_id}.{i}",
            daemon=True,
        )
        for i in range(PRODUCER_THREADS)
    ]
    for t in threads:
        t.start()

    # Monitor: aggregate this process's workers and print one line periodically.
    start = time.time()
    last_total = 0
    last_t = start
    while any(t.is_alive() for t in threads) and not stop.is_set():
        stop.wait(2.0)
        now = time.time()
        total = sum(s["total"] for s in stats)
        live = sum(s["live"] for s in stats)
        rate = (total - last_total) / max(now - last_t, 1e-9)
        last_total, last_t = total, now
        print(
            f"[{tag}] inserted {total:>12,} rows | {rate:>10,.0f} rows/s | "
            f"live sessions {live:>6}",
            file=sys.stderr,
        )

    for t in threads:
        t.join()
    total = sum(s["total"] for s in stats)
    print(f"[{tag}] flushed. Total inserted: {total:,} rows.", file=sys.stderr)


def main() -> int:
    if PRODUCER_PROCESSES <= 1:
        _run_process(0)
        return 0

    # Fork independent producer processes; each runs its own thread pool and
    # handles its own graceful shutdown on the shared Ctrl-C.
    ctx = multiprocessing.get_context("spawn")
    procs = [
        ctx.Process(target=_run_process, args=(i,), name=f"producer-{i}")
        for i in range(PRODUCER_PROCESSES)
    ]
    print(
        f"Starting {PRODUCER_PROCESSES} processes × {PRODUCER_THREADS} threads "
        f"= {PRODUCER_PROCESSES * PRODUCER_THREADS} workers",
        file=sys.stderr,
    )
    for p in procs:
        p.start()
    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        # Children receive the same SIGINT from the terminal and shut down
        # gracefully; just wait for them.
        for p in procs:
            p.join()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
