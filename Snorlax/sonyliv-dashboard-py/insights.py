"""Insights pane — derived user & content analytics (data layer).

STATUS: PLACEHOLDER. These derive from JOINING two datasets — the events fact
(`events_raw`) with the content dimension (`content_dim`), and a future users
dataset. No joined/aggregated tables exist yet, so these return deterministic
*sample* data. The intended SQL is kept as a constant per function; swap the
sample return for `query_df(SQL, f)` once the schema is final.

Intended joins (final schema):
  events_raw e  JOIN content_dim c  ON e.content_id = c.content_id
  events_raw e  JOIN users d        ON e.user_id    = d.user_id
"""

from __future__ import annotations

import zlib

import numpy as np
import pandas as pd

from config import DB

# ---------------------------------------------------------------------------
# Intended SQL (NOT executed yet — placeholders for the final schema).
# ---------------------------------------------------------------------------
SQL_TOP_CONTENT = f"""
-- TODO(real schema): top content by unique viewers (events ⋈ content)
SELECT c.title AS title, c.category AS category,
       uniqExact(e.user_id) AS unique_viewers,
       round(avg(e.watch_seconds) / 60, 1) AS avg_watch_min
FROM {DB}.events_raw AS e
JOIN {DB}.content_dim AS c ON e.content_id = c.content_id
WHERE e.event_timestamp BETWEEN {{from:String}} AND {{to:String}}
GROUP BY title, category ORDER BY unique_viewers DESC LIMIT 12"""

SQL_ENGAGEMENT_BY_CATEGORY = f"""
SELECT c.category AS category,
       uniqExact(e.user_id) AS viewers,
       round(avg(e.watch_seconds) / 60, 1) AS avg_watch_min
FROM {DB}.events_raw AS e
JOIN {DB}.content_dim AS c ON e.content_id = c.content_id
WHERE e.event_timestamp BETWEEN {{from:String}} AND {{to:String}}
GROUP BY category ORDER BY viewers DESC"""

SQL_USER_SEGMENTS = f"""
-- TODO(real schema): user segments (events ⋈ users)
SELECT segment, uniqExact(user_id) AS users
FROM {DB}.users
GROUP BY segment ORDER BY users DESC"""

IS_SAMPLE = True  # flip to False once wired to real queries

CATEGORIES = ["Drama", "Sports", "Movies", "Reality", "News", "Kids"]
TITLES = [
    ("Scam 1992", "Drama"),
    ("India vs Australia — Live", "Sports"),
    ("Rocket Boys", "Drama"),
    ("The Kapil Sharma Show", "Reality"),
    ("Gullak", "Drama"),
    ("UEFA Champions League", "Sports"),
    ("Maharani", "Drama"),
    ("Shark Tank India", "Reality"),
    ("Avatar: The Way of Water", "Movies"),
    ("Undekhi", "Drama"),
    ("WWE Raw", "Sports"),
    ("Peppa Pig", "Kids"),
]
SEGMENTS = ["New", "Returning", "Power viewer", "At-risk"]


def _rng(f: dict) -> np.random.Generator:
    seed = zlib.crc32(("insights" + repr(sorted(f.items()))).encode()) & 0xFFFFFFFF
    return np.random.default_rng(seed)


def get_top_content(f: dict) -> pd.DataFrame:
    rng = _rng(f)
    viewers = sorted(rng.integers(2_000, 90_000, len(TITLES)), reverse=True)
    watch = np.round(rng.uniform(8, 55, len(TITLES)), 1)
    df = pd.DataFrame(
        {
            "title": [t for t, _ in TITLES],
            "category": [c for _, c in TITLES],
            "unique_viewers": viewers,
            "avg_watch_min": watch,
        }
    )
    return df.sort_values("unique_viewers", ascending=False, ignore_index=True)


def get_engagement_by_category(f: dict) -> pd.DataFrame:
    rng = _rng(f)
    viewers = rng.integers(10_000, 200_000, len(CATEGORIES))
    watch = np.round(rng.uniform(10, 48, len(CATEGORIES)), 1)
    return pd.DataFrame(
        {"category": CATEGORIES, "viewers": viewers, "avg_watch_min": watch}
    ).sort_values("viewers", ascending=False, ignore_index=True)


def get_user_segments(f: dict) -> pd.DataFrame:
    rng = _rng(f)
    users = rng.integers(15_000, 120_000, len(SEGMENTS))
    return pd.DataFrame({"segment": SEGMENTS, "users": users}).sort_values(
        "users", ascending=False, ignore_index=True
    )


def get_insight_kpis(f: dict) -> dict:
    seg = get_user_segments(f)
    cat = get_engagement_by_category(f)
    total_users = int(seg["users"].sum())
    returning = int(
        seg.loc[seg["segment"].isin(["Returning", "Power viewer"]), "users"].sum()
    )
    watch_hours = int((cat["viewers"] * cat["avg_watch_min"]).sum() / 60)
    return {
        "total_users": total_users,
        "returning_pct": round(100 * returning / total_users, 1),
        "avg_watch_min": round(float(cat["avg_watch_min"].mean()), 1),
        "watch_hours": watch_hours,
        "top_category": cat.iloc[0]["category"],
    }
