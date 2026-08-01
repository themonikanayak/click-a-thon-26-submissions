"""SQL for the dashboard, ported from the React app's `lib/queries.ts`.

All queries use ClickHouse server-side named params ({name:Type}). Empty string
'' means "all" for a dimension filter and "full range" for from/to — the same
convention as the original UI and `schema/ui_queries.sql`.

Note vs the React version: the curve/KPI queries alias the time column to `ts`
(not `minute`) to avoid an alias-vs-column collision that ClickHouse rejects
(`NO_COMMON_TYPE`) when a String alias shadows the DateTime `minute` in WHERE.
"""

from __future__ import annotations

import pandas as pd

from clickhouse_client import query_df
from config import DB

# --- Filter dropdowns + time bounds ------------------------------------------
_Q_DISTINCT = (
    "SELECT DISTINCT {col} AS v FROM " + DB + ".concurrency_now "
    "WHERE {col} != '' ORDER BY v"
)
Q_CONTENTS = (
    f"SELECT toString(content_id) AS content_id, title "
    f"FROM {DB}.content_dim ORDER BY title LIMIT 1000"
)
Q_BOUNDS = (
    f"SELECT toString(min(minute)) AS min_ts, toString(max(minute)) AS max_ts "
    f"FROM {DB}.concurrency_now"
)

# Shared time-window CTE + lenient dimension WHERE fragment.
_RANGE_CTE = f"""
  coalesce(parseDateTimeBestEffortOrNull({{from:String}}, 'UTC'),
           (SELECT min(minute) FROM {DB}.concurrency_now)) AS from_ts,
  coalesce(parseDateTimeBestEffortOrNull({{to:String}}, 'UTC'),
           (SELECT max(minute) FROM {DB}.concurrency_now)) AS to_ts"""

_WHERE = """
  minute BETWEEN from_ts AND to_ts
  AND (platform   = {platform:String}   OR {platform:String}   = '')
  AND (country    = {country:String}     OR {country:String}    = '')
  AND (video_type = {video_type:String}  OR {video_type:String} = '')
  AND (category   = {category:String}    OR {category:String}   = '')
  AND (content_id = toUInt64OrZero({content_id:String})
       OR toUInt64OrZero({content_id:String}) = 0)"""

Q_CURVE = f"""
WITH {_RANGE_CTE}
SELECT toString(minute) AS ts, toUInt32(sum(concurrent)) AS concurrency
FROM {DB}.concurrency_now
WHERE {_WHERE}
GROUP BY minute
ORDER BY minute"""

# Full stat set for the selected filters. The per-minute `curve` CTE sums
# concurrency across all dims first, then aggregates over minutes.
# `current` = concurrency at the latest minute in range (argMax is deterministic).
Q_STATS = f"""
WITH {_RANGE_CTE},
curve AS (
  SELECT minute, sum(concurrent) AS c
  FROM {DB}.concurrency_now
  WHERE {_WHERE}
  GROUP BY minute
)
SELECT
  toUInt32(ifNull(max(c), 0))                                  AS peak_concurrency,
  toString(argMax(minute, c))                                 AS peak_minute,
  toUInt32(ifNull(argMax(c, minute), 0))                      AS last_minute_concurrency,
  round(ifNull(avg(c), 0), 1)                                 AS avg_concurrency,
  toUInt32(ifNull(min(c), 0))                                 AS min_concurrency,
  toUInt32(ifNull(quantile(0.95)(c), 0))                      AS p95_concurrency,
  toUInt32(count())                                           AS active_minutes,
  toUInt32(ifNull(sum(c), 0))                                 AS total_session_minutes
FROM curve"""

# Dimensions the UI can break peak concurrency down by. `content` joins titles.
BREAKDOWN_DIMS = ["platform", "video_type", "category"]


def _breakdown_sql(dim: str) -> str:
    """Peak / average concurrency grouped by one dimension, honoring filters."""
    return f"""
WITH {_RANGE_CTE},
per AS (
  SELECT {dim} AS name, minute, sum(concurrent) AS c
  FROM {DB}.concurrency_now
  WHERE {_WHERE}
  GROUP BY name, minute
)
SELECT name,
       toUInt32(max(c))   AS peak,
       round(avg(c), 1)   AS avg
FROM per
WHERE name != ''
GROUP BY name
ORDER BY peak DESC, name
LIMIT 50"""


_Q_TOP_CONTENT = f"""
WITH {_RANGE_CTE},
per AS (
  SELECT content_id, minute, sum(concurrent) AS c
  FROM {DB}.concurrency_now
  WHERE {_WHERE}
  GROUP BY content_id, minute
)
SELECT toString(per.content_id) AS content_id,
       any(cd.title)            AS title,
       toUInt32(max(c))         AS peak,
       round(avg(c), 1)         AS avg
FROM per
LEFT JOIN {DB}.content_dim AS cd ON per.content_id = cd.content_id
GROUP BY per.content_id
ORDER BY peak DESC, content_id
LIMIT 15"""

# Empty filter set: everything, full range.
EMPTY_FILTERS: dict[str, str] = {
    "from": "",
    "to": "",
    "platform": "",
    "country": "",
    "video_type": "",
    "category": "",
    "content_id": "",
}


def get_filter_options() -> dict:
    """Dropdown values + time bounds for the filter bar."""

    def col(name: str) -> list[str]:
        df = query_df(_Q_DISTINCT.format(col=name))
        return df["v"].tolist() if not df.empty else []

    bounds_df = query_df(Q_BOUNDS)
    bounds = (
        bounds_df.iloc[0].to_dict()
        if not bounds_df.empty
        else {"min_ts": None, "max_ts": None}
    )
    contents_df = query_df(Q_CONTENTS)
    return {
        "platforms": col("platform"),
        "countries": col("country"),
        "video_types": col("video_type"),
        "categories": col("category"),
        "contents": contents_df,  # DataFrame[content_id, title]
        "bounds": bounds,
    }


def get_curve(filters: dict) -> pd.DataFrame:
    """Per-minute concurrency curve. Columns: ts (str), concurrency (int)."""
    return query_df(Q_CURVE, filters)


def get_stats(filters: dict) -> dict:
    """Full stat set (peak/current/avg/min/p95/active-minutes/total) for the
    current filter set."""
    df = query_df(Q_STATS, filters)
    if df.empty:
        return {
            "peak_concurrency": 0,
            "peak_minute": None,
            "last_minute_concurrency": 0,
            "avg_concurrency": 0,
            "min_concurrency": 0,
            "p95_concurrency": 0,
            "active_minutes": 0,
            "total_session_minutes": 0,
        }
    return df.iloc[0].to_dict()


def get_breakdown(filters: dict, dim: str) -> pd.DataFrame:
    """Peak/avg concurrency grouped by one dimension, honoring filters."""
    return query_df(_breakdown_sql(dim), filters)


def get_top_content(filters: dict) -> pd.DataFrame:
    """Top content by peak concurrency (with titles), honoring filters."""
    return query_df(_Q_TOP_CONTENT, filters)
