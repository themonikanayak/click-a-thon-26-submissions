"""ClickHouse Cloud connection for the dashboard.

Credentials are read from a `.env` file using the SAME keys as the producer
(`producer/produce_events.py`). To avoid duplicating secrets, the loader looks
for, in order:

    1. sonyliv-dashboard-py/.env          (this app's own env, if you made one)
    2. ../producer/.env                    (reuse the producer's working creds)

so `cp` of credentials is never required — point the app at the producer's
existing `.env` and it just works.

The client is cached as a Streamlit resource (one connection per session, reused
across reruns) since `clickhouse_connect` clients are cheap to hold open.
"""

from __future__ import annotations

import guardrails  # noqa: F401  (must run first — strips Bloomberg proxy env vars)

import os
from pathlib import Path

import clickhouse_connect
import pandas as pd
import streamlit as st
from clickhouse_connect.driver.client import Client
from dotenv import load_dotenv

_HERE = Path(__file__).resolve().parent
_ENV_CANDIDATES = [_HERE / ".env", _HERE.parent / "producer" / ".env"]


def _load_env() -> Path | None:
    """Load the first `.env` that exists; return which one (for the UI)."""
    for candidate in _ENV_CANDIDATES:
        if candidate.is_file():
            load_dotenv(candidate)
            return candidate
    return None


@st.cache_resource(show_spinner=False)
def get_client() -> tuple[Client, str]:
    """Return a live ClickHouse client and the host it connected to.

    Raises RuntimeError with an actionable message if no env / host is found.
    """
    env_path = _load_env()
    host = os.getenv("CLICKHOUSE_HOST")
    if not host:
        looked = "\n  - ".join(str(p) for p in _ENV_CANDIDATES)
        raise RuntimeError(
            "CLICKHOUSE_HOST is not set. Create a .env (see .env.example) or "
            f"reuse the producer's. Looked for:\n  - {looked}"
        )
    client = clickhouse_connect.get_client(
        host=host,
        port=int(os.getenv("CLICKHOUSE_PORT", "8443")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        secure=os.getenv("CLICKHOUSE_SECURE", "true").lower() in ("1", "true", "yes"),
        connect_timeout=15,
        send_receive_timeout=30,
        # Keep dashboard queries snappy and bounded (mirrors the React app).
        settings={"max_execution_time": 30},
    )
    _ = env_path  # loaded above; kept for clarity
    return client, host


def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Run a parameterized query and return a pandas DataFrame.

    Uses ClickHouse server-side named params ({name:Type}) — same convention as
    the original `lib/queries.ts`.
    """
    client, _ = get_client()
    return client.query_df(sql, parameters=params or {})
