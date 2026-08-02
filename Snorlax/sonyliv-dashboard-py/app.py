"""SonyLIV — Viewing Concurrency & Insights dashboard (Streamlit).

Three panes:
  1. Concurrency — truly-active sessions per minute (REAL data, concurrency_now).
  2. Errors      — playback-error metrics (PLACEHOLDER sample data, see errors.py).
  3. Insights    — derived user & content analytics from joined datasets
                   (PLACEHOLDER sample data, see insights.py).

Plus an "✨ Copilot" tab that embeds LibreChat (see ../librechat-setup/) — an
Ollama agent that queries ClickHouse directly via the ClickHouse MCP server.

ClickHouse-inspired theme (config.py / ui.py). Run:
    pip install -r requirements.txt
    streamlit run app.py
"""

from __future__ import annotations

import guardrails  # noqa: F401  (must run first — strips Bloomberg proxy env vars)

from datetime import date, datetime, time

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

import errors as errmod
import insights as insmod
import queries
import ui
from config import DB, LIBRECHAT_EMBED_URL, REFRESH_MS

st.set_page_config(page_title="SonyLIV — Viewing Concurrency", layout="wide")

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:  # pragma: no cover
    st_autorefresh = None

DANGER = "#ff6b6b"
DANGER_FILL = "rgba(255,107,107,0.14)"


# ===========================================================================
# Pane 1 — Concurrency (real data)
# ===========================================================================
def render_concurrency(time_filter: dict) -> None:
    try:
        opts = queries.get_filter_options()
    except Exception as e:  # noqa: BLE001
        st.error(f"⚠ Failed to load filters: {e}")
        return

    f = {**queries.EMPTY_FILTERS, **time_filter}

    # Dimension filters (concurrency-specific columns).
    r = st.columns([1, 1, 1, 1, 1.6])

    def sel(container, label: str, values: list[str]) -> str:
        choice = container.selectbox(label, ["All", *values], index=0)
        return "" if choice == "All" else choice

    f["platform"] = sel(r[0], "Platform", opts["platforms"])
    f["country"] = sel(r[1], "Country", opts["countries"])
    f["video_type"] = sel(r[2], "Video type", opts["video_types"])
    f["category"] = sel(r[3], "Category", opts["categories"])

    contents = opts["contents"]
    labels, ids = ["All content"], [""]
    if not contents.empty:
        for _, row in contents.iterrows():
            labels.append(row["title"] or row["content_id"])
            ids.append(row["content_id"])
    idx = r[4].selectbox("Content", range(len(labels)), format_func=lambda i: labels[i])
    f["content_id"] = ids[idx]

    try:
        stats = queries.get_stats(f)
        curve = queries.get_curve(f)
    except Exception as e:  # noqa: BLE001
        st.error(f"⚠ Query failed: {e}")
        return

    avg = stats["avg_concurrency"]
    avg_str = f"{avg:,.1f}" if pd.notna(avg) else "—"

    ui.tiles_row(
        st.columns(3),
        [
            ui.kpi_tile(
                "Peak concurrency",
                ui.fmt(stats["peak_concurrency"]),
                f"at {ui.pretty_minute(stats['peak_minute'])}",
                "accent",
                peak=True,
            ),
            ui.kpi_tile(
                "Current concurrency",
                ui.fmt(stats["last_minute_concurrency"]),
                "latest minute in range",
                "accent2",
            ),
            ui.kpi_tile("Average concurrency", avg_str, "mean over active minutes"),
        ],
    )
    st.write("")
    ui.tiles_row(
        st.columns(3),
        [
            ui.kpi_tile(
                "Min concurrency",
                ui.fmt(stats["min_concurrency"]),
                "lowest active minute",
            ),
            ui.kpi_tile(
                "P95 concurrency",
                ui.fmt(stats["p95_concurrency"]),
                "95th-percentile minute",
            ),
            ui.kpi_tile(
                "Active minutes",
                ui.fmt(stats["active_minutes"]),
                f"{ui.fmt(stats['total_session_minutes'])} session-minutes total",
            ),
        ],
    )
    st.write("")

    st.markdown("**Concurrency curve (per minute)**")
    if curve.empty:
        st.info("No data for the selected filters.")
    else:
        st.plotly_chart(
            ui.time_area(curve, "ts", "concurrency", "concurrent"),
            width="stretch",
            config={"displayModeBar": False},
        )

    st.write("")
    st.markdown(
        "**Breakdown by dimension** — peak / avg concurrency for the current filters"
    )
    labels_map = {
        "platform": "Platform",
        "video_type": "Video type",
        "category": "Category",
    }
    for col, dim in zip(
        st.columns(len(queries.BREAKDOWN_DIMS)), queries.BREAKDOWN_DIMS
    ):
        with col:
            st.caption(labels_map[dim])
            bdf = queries.get_breakdown(f, dim)
            if bdf.empty:
                st.info("No data.")
            else:
                st.dataframe(
                    bdf.rename(
                        columns={"name": labels_map[dim], "peak": "Peak", "avg": "Avg"}
                    ),
                    hide_index=True,
                    width="stretch",
                )

    st.write("")
    st.markdown("**Top content by peak concurrency**")
    tc = queries.get_top_content(f)
    if tc.empty:
        st.info("No content for the selected filters.")
    else:
        st.dataframe(
            tc.rename(
                columns={
                    "content_id": "Content ID",
                    "title": "Title",
                    "peak": "Peak",
                    "avg": "Avg",
                }
            ),
            hide_index=True,
            width="stretch",
        )


# ===========================================================================
# Pane 2 — Errors (placeholder sample data)
# ===========================================================================
def render_errors(time_filter: dict) -> None:
    if errmod.IS_SAMPLE:
        st.caption(
            "⚠ Sample data (placeholder) — swap errors.py functions for real "
            "queries once the errors table lands."
        )
    k = errmod.get_error_kpis(time_filter)
    ui.tiles_row(
        st.columns(4),
        [
            ui.kpi_tile(
                "Total errors",
                ui.fmt(k["total_errors"]),
                f"top type: {k['top_type']}",
                "danger",
            ),
            ui.kpi_tile(
                "Error rate", f"{k['error_rate_pct']:.2f}%", "errors / sessions"
            ),
            ui.kpi_tile(
                "Peak errors / bucket",
                ui.fmt(k["peak_errors_min"]),
                "worst time bucket",
            ),
            ui.kpi_tile(
                "Distinct error types",
                ui.fmt(k["distinct_types"]),
                "unique error codes",
            ),
        ],
    )
    st.write("")

    st.markdown("**Errors over time**")
    st.plotly_chart(
        ui.time_area(
            errmod.get_errors_over_time(time_filter),
            "ts",
            "errors",
            "errors",
            color=DANGER,
            fill=DANGER_FILL,
        ),
        width="stretch",
        config={"displayModeBar": False},
    )

    st.write("")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Errors by type**")
        st.plotly_chart(
            ui.bar(
                errmod.get_errors_by_type(time_filter),
                "error_type",
                "errors",
                "errors",
                color=DANGER,
            ),
            width="stretch",
            config={"displayModeBar": False},
        )
    with c2:
        st.markdown("**Errors by platform**")
        st.plotly_chart(
            ui.bar(
                errmod.get_errors_by_platform(time_filter),
                "platform",
                "errors",
                "errors",
            ),
            width="stretch",
            config={"displayModeBar": False},
        )

    st.write("")
    st.markdown("**Top error messages**")
    st.dataframe(
        errmod.get_top_error_messages(time_filter).rename(
            columns={
                "error_type": "Type",
                "message": "Message",
                "count": "Count",
                "last_seen": "Last seen",
            }
        ),
        hide_index=True,
        width="stretch",
    )


# ===========================================================================
# Pane 3 — Insights (placeholder sample data; joined users × content)
# ===========================================================================
def render_insights(time_filter: dict) -> None:
    if insmod.IS_SAMPLE:
        st.caption(
            "⚠ Sample data (placeholder) — derived from events ⋈ content ⋈ users; "
            "swap insights.py functions for real joins once the schema is final."
        )
    k = insmod.get_insight_kpis(time_filter)
    ui.tiles_row(
        st.columns(4),
        [
            ui.kpi_tile(
                "Unique users",
                ui.fmt(k["total_users"]),
                f"top category: {k['top_category']}",
                "accent",
            ),
            ui.kpi_tile(
                "Returning users",
                f"{k['returning_pct']:.1f}%",
                "returning + power viewers",
                "accent2",
            ),
            ui.kpi_tile(
                "Avg watch time",
                f"{k['avg_watch_min']:.1f} min",
                "per viewer, across categories",
            ),
            ui.kpi_tile(
                "Total watch hours", ui.fmt(k["watch_hours"]), "content-hours in range"
            ),
        ],
    )
    st.write("")

    st.markdown("**Top content by unique viewers** (events ⋈ content)")
    top = insmod.get_top_content(time_filter)
    st.plotly_chart(
        ui.bar(top.head(10), "title", "unique_viewers", "viewers"),
        width="stretch",
        config={"displayModeBar": False},
    )

    st.write("")
    c1, c2 = st.columns([1.3, 1])
    with c1:
        st.markdown("**Engagement by category**")
        st.plotly_chart(
            ui.bar(
                insmod.get_engagement_by_category(time_filter),
                "category",
                "viewers",
                "viewers",
            ),
            width="stretch",
            config={"displayModeBar": False},
        )
    with c2:
        st.markdown("**User segments**")
        st.plotly_chart(
            ui.donut(insmod.get_user_segments(time_filter), "segment", "users"),
            width="stretch",
            config={"displayModeBar": False},
        )

    st.write("")
    st.markdown("**Content leaderboard**")
    st.dataframe(
        top.rename(
            columns={
                "title": "Title",
                "category": "Category",
                "unique_viewers": "Unique viewers",
                "avg_watch_min": "Avg watch (min)",
            }
        ),
        hide_index=True,
        width="stretch",
    )


# ===========================================================================
# Pane 4 — Insights Copilot (embedded LibreChat agent + ClickHouse MCP)
# ===========================================================================
_COPILOT_HEIGHT = 760


def render_copilot() -> None:
    st.caption(
        "Ask the **Insights Copilot** about the live concurrency data — a local "
        "Ollama agent (via LibreChat) that queries ClickHouse directly through the "
        "ClickHouse MCP server. See `../librechat-setup/` for setup."
    )
    st.markdown(
        f'<a class="mono" href="{LIBRECHAT_EMBED_URL}" target="_blank" '
        f'rel="noopener">↗ Open the Copilot in a new tab</a> '
        "<span class='dash-sub'>(use this if the embed below stays blank — some "
        "browsers block framing / third-party cookies)</span>",
        unsafe_allow_html=True,
    )
    st.write("")
    components.iframe(LIBRECHAT_EMBED_URL, height=_COPILOT_HEIGHT, scrolling=True)


# ===========================================================================
# App shell
# ===========================================================================
def main() -> None:
    ui.inject_css()

    try:
        from clickhouse_client import get_client

        get_client()
    except Exception as e:  # noqa: BLE001
        st.error(f"⚠ Could not connect to ClickHouse: {e}")
        st.stop()

    # ---- Header --------------------------------------------------------------
    left, right = st.columns([3, 2])
    with left:
        st.markdown(
            f'<div class="brand">{ui.CH_LOGO}<div>'
            f'<p class="dash-title">SonyLIV — Viewing Concurrency</p>'
            f'<div class="dash-sub">Concurrency · errors · insights · '
            f'ClickHouse Cloud · <span class="mono">{DB}</span></div>'
            f"</div></div>",
            unsafe_allow_html=True,
        )
    with right:
        c1, c2 = st.columns([1.4, 1])
        auto = c1.toggle("Auto-refresh (30s)", value=True)
        refresh = c2.button("Refresh", width="stretch", type="primary")

    if auto and st_autorefresh is not None:
        st_autorefresh(interval=REFRESH_MS, key="auto")
    if refresh:
        st.rerun()

    dot = "live" if auto else ""
    st.markdown(
        f'<div class="dash-sub"><span class="dot {dot}"></span>'
        f'{"Live" if auto else "Paused"} · last updated '
        f'{datetime.now().strftime("%H:%M:%S")}</div>',
        unsafe_allow_html=True,
    )
    st.write("")

    # ---- Global time window (applies to all panes) ---------------------------
    today = date.today()
    d1, t1, d2, t2 = st.columns([1.3, 1, 1.3, 1])
    from_date = d1.date_input("From date", value=today)
    from_time = t1.time_input("From time", value=time(0, 0))
    to_date = d2.date_input("To date", value=today)
    to_time = t2.time_input("To time", value=time(23, 59))
    time_filter = {
        "from": datetime.combine(from_date, from_time).strftime("%Y-%m-%d %H:%M:%S"),
        "to": datetime.combine(to_date, to_time).strftime("%Y-%m-%d %H:%M:%S"),
    }
    st.write("")

    # ---- Dashboard tabs ------------------------------------------------------
    tab_conc, tab_err, tab_ins, tab_ai = st.tabs(
        ["📈 Concurrency", "🚨 Errors", "🧭 Insights", "✨ Copilot"]
    )
    with tab_conc:
        render_concurrency(time_filter)
    with tab_err:
        render_errors(time_filter)
    with tab_ins:
        render_insights(time_filter)
    with tab_ai:
        render_copilot()


if __name__ == "__main__":
    main()
