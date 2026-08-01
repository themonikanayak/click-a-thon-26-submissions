"""Errors pane — data layer.

STATUS: PLACEHOLDER. There is no errors table in ClickHouse Cloud yet, so these
functions return deterministic *sample* data so the UI can be built now. The
intended SQL (to run once the schema exists) is kept alongside each function as
a constant; swap the sample return for `query_df(SQL, f)` when the table lands.

Likely real source: `sonyliv_concurrency.events_raw` filtered to error events
(event_type = 'VideoError'), or a dedicated `errors` fact table. Columns assumed:
error_timestamp, error_code, error_message, platform, content_id, video_session_id.
"""

from __future__ import annotations

import zlib
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from config import DB

# ---------------------------------------------------------------------------
# Intended SQL (NOT executed yet — placeholders for the final schema).
# ---------------------------------------------------------------------------
SQL_ERRORS_OVER_TIME = f"""
-- TODO(real schema): errors per minute in range
SELECT toStartOfMinute(error_timestamp) AS ts, count() AS errors
FROM {DB}.errors
WHERE error_timestamp BETWEEN {{from:String}} AND {{to:String}}
GROUP BY ts ORDER BY ts"""

SQL_ERRORS_BY_TYPE = f"""
SELECT error_code AS error_type, count() AS errors
FROM {DB}.errors
WHERE error_timestamp BETWEEN {{from:String}} AND {{to:String}}
GROUP BY error_type ORDER BY errors DESC"""

SQL_ERRORS_BY_PLATFORM = f"""
SELECT platform, count() AS errors
FROM {DB}.errors
WHERE error_timestamp BETWEEN {{from:String}} AND {{to:String}}
GROUP BY platform ORDER BY errors DESC"""

SQL_TOP_ERROR_MESSAGES = f"""
SELECT error_message AS message, count() AS count,
       toString(max(error_timestamp)) AS last_seen
FROM {DB}.errors
WHERE error_timestamp BETWEEN {{from:String}} AND {{to:String}}
GROUP BY message ORDER BY count DESC LIMIT 15"""

IS_SAMPLE = True  # flip to False once wired to real queries

ERROR_TYPES = [
    "PLAYBACK_STALL",
    "DRM_LICENSE",
    "MANIFEST_404",
    "DECODER_INIT",
    "NETWORK_TIMEOUT",
    "AD_LOAD_FAIL",
]
ERROR_MESSAGES = {
    "PLAYBACK_STALL": "Rebuffering exceeded threshold (player stalled)",
    "DRM_LICENSE": "Widevine license acquisition failed (403)",
    "MANIFEST_404": "HLS manifest not found for content variant",
    "DECODER_INIT": "Hardware decoder failed to initialize",
    "NETWORK_TIMEOUT": "Segment download timed out after 3 retries",
    "AD_LOAD_FAIL": "VAST ad tag failed to load",
}
PLATFORMS = ["ANDROID_PHONE", "IPHONE", "WEB", "TV", "FIRETV"]


# ---------------------------------------------------------------------------
# Sample-data helpers (deterministic: same filters → same numbers).
# ---------------------------------------------------------------------------
def _rng(f: dict) -> np.random.Generator:
    seed = zlib.crc32(repr(sorted(f.items())).encode()) & 0xFFFFFFFF
    return np.random.default_rng(seed)


def _time_axis(f: dict, points: int = 48) -> pd.DatetimeIndex:
    def _p(v, default):
        try:
            return datetime.strptime(v[:19], "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return default

    end = _p(f.get("to", ""), datetime.now())
    start = _p(f.get("from", ""), end - timedelta(hours=24))
    if end <= start:
        end = start + timedelta(hours=24)
    return pd.date_range(start, end, periods=points)


def get_errors_over_time(f: dict) -> pd.DataFrame:
    rng = _rng(f)
    ts = _time_axis(f)
    n = len(ts)
    base = rng.integers(4, 14, n).astype(float)
    # inject a spike (incident) in a random middle window
    s = rng.integers(n // 4, n // 2)
    base[s : s + max(2, n // 12)] += rng.integers(30, 70)
    return pd.DataFrame({"ts": ts, "errors": base.astype(int)})


def get_errors_by_type(f: dict) -> pd.DataFrame:
    rng = _rng(f)
    counts = sorted(rng.integers(40, 400, len(ERROR_TYPES)), reverse=True)
    return pd.DataFrame({"error_type": ERROR_TYPES, "errors": counts}).sort_values(
        "errors", ascending=False, ignore_index=True
    )


def get_errors_by_platform(f: dict) -> pd.DataFrame:
    rng = _rng(f)
    counts = rng.integers(30, 350, len(PLATFORMS))
    return pd.DataFrame({"platform": PLATFORMS, "errors": counts}).sort_values(
        "errors", ascending=False, ignore_index=True
    )


def get_top_error_messages(f: dict) -> pd.DataFrame:
    rng = _rng(f)
    end = _time_axis(f)[-1]
    rows = []
    for et in ERROR_TYPES:
        rows.append(
            {
                "error_type": et,
                "message": ERROR_MESSAGES[et],
                "count": int(rng.integers(40, 400)),
                "last_seen": (
                    end - timedelta(minutes=int(rng.integers(0, 120)))
                ).strftime("%Y-%m-%d %H:%M"),
            }
        )
    return pd.DataFrame(rows).sort_values("count", ascending=False, ignore_index=True)


def get_error_kpis(f: dict) -> dict:
    by_type = get_errors_by_type(f)
    over_time = get_errors_over_time(f)
    total = int(by_type["errors"].sum())
    # placeholder "sessions" denominator for an error-rate %
    rng = _rng(f)
    sessions = int(rng.integers(40_000, 120_000))
    return {
        "total_errors": total,
        "error_rate_pct": round(100 * total / sessions, 2),
        "distinct_types": int(by_type.shape[0]),
        "top_type": by_type.iloc[0]["error_type"],
        "peak_errors_min": int(over_time["errors"].max()),
    }
