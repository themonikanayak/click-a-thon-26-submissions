"""Shared UI helpers — theme CSS, KPI tiles, and reusable chart builders.

Kept separate from the data modules (queries/errors/insights) and the app shell
(app.py) so styling and chart construction live in one place. Palette comes from
config.py (ClickHouse-inspired yellow-on-near-black).
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from config import ACCENT, ACCENT_2, ACCENT_FILL, BORDER, MUTED, PANEL, PANEL_2, TEXT

# ClickHouse-style columnar bar mark (three tall bars + one short) in brand yellow.
CH_LOGO = f"""
<svg width="30" height="30" viewBox="0 0 48 48" fill="none" xmlns="http://www.w3.org/2000/svg">
  <rect x="4"  y="6"  width="8" height="36" rx="2" fill="{ACCENT}"/>
  <rect x="16" y="6"  width="8" height="36" rx="2" fill="{ACCENT}"/>
  <rect x="28" y="6"  width="8" height="36" rx="2" fill="{ACCENT}"/>
  <rect x="40" y="18" width="8" height="24" rx="2" fill="{ACCENT}"/>
</svg>"""


def inject_css() -> None:
    st.markdown(
        f"""
        <style>
          /* clear Streamlit's fixed top toolbar so the header isn't covered */
          header[data-testid="stHeader"] {{ background: transparent; height: 0; }}
          .block-container {{ padding-top: 4.5rem; max-width: 1250px; }}
          #MainMenu, footer {{ visibility: hidden; }}
          /* header / brand */
          .brand {{ display:flex; align-items:center; gap:12px; }}
          .brand svg {{ flex:0 0 auto; }}
          .dash-title {{ font-size: 23px; font-weight: 800; margin: 0;
                         letter-spacing: -0.2px; }}
          .dash-sub {{ color: {MUTED}; font-size: 13px; margin-top: 2px; }}
          .mono {{ font-family: ui-monospace, Menlo, Consolas, monospace;
                   font-variant-numeric: tabular-nums; }}
          /* KPI tiles */
          .tile {{ background: linear-gradient(180deg, {PANEL}, {PANEL_2});
                   border: 1px solid {BORDER}; border-radius: 14px;
                   padding: 18px 20px; box-shadow: 0 6px 24px rgba(0,0,0,0.35); }}
          .tile.peak {{ border-color: {ACCENT};
                        box-shadow: 0 0 0 1px {ACCENT}33, 0 6px 24px rgba(0,0,0,0.35); }}
          .k-label {{ font-size: 12px; text-transform: uppercase;
                      letter-spacing: 0.7px; color: {MUTED}; }}
          .k-value {{ font-size: 38px; font-weight: 800; margin-top: 8px;
                      line-height: 1; color: {TEXT}; }}
          .k-value.accent {{ color: {ACCENT}; }}
          .k-value.accent2 {{ color: {ACCENT_2}; }}
          .k-value.danger {{ color: #ff6b6b; }}
          .k-sub {{ font-size: 12px; color: {MUTED}; margin-top: 8px; }}
          /* live dot */
          .dot {{ display:inline-block; width:8px; height:8px; border-radius:50%;
                  background:{MUTED}; margin-right:6px; }}
          .dot.live {{ background:{ACCENT}; box-shadow:0 0 0 3px {ACCENT}2e; }}
          /* primary (Refresh) button → ClickHouse yellow with dark text */
          .stButton > button[kind="primary"] {{ background:{ACCENT}; color:#141414;
                      border:0; font-weight:700; }}
          /* tabs → yellow active accent */
          .stTabs [data-baseweb="tab-list"] {{ gap: 6px; }}
          .stTabs [data-baseweb="tab"] {{ font-weight: 600; }}
          .stTabs [aria-selected="true"] {{ color: {ACCENT} !important; }}
          .stTabs [data-baseweb="tab-highlight"] {{ background-color: {ACCENT}; }}
          div[data-testid="stMetricValue"] {{ font-variant-numeric: tabular-nums; }}
        </style>
        """,
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------
def fmt(n) -> str:
    """Thousands-separated integer, or an em-dash for missing values."""
    return f"{int(n):,}" if pd.notna(n) else "—"


def pretty_minute(ts) -> str:
    if not ts or pd.isna(ts):
        return "—"
    return str(ts).replace("T", " ")[:16] + " UTC"


def kpi_tile(
    label: str, value: str, sub: str, accent: str = "", peak: bool = False
) -> str:
    """Return the HTML for one KPI tile (render with st.markdown, unsafe_allow_html)."""
    tile_cls = "tile peak" if peak else "tile"
    val_cls = f"k-value {accent}".strip()
    return (
        f'<div class="{tile_cls}"><div class="k-label">{label}</div>'
        f'<div class="{val_cls} mono">{value}</div>'
        f'<div class="k-sub">{sub}</div></div>'
    )


def tiles_row(cols, tiles: list[str]) -> None:
    """Render a row of KPI tiles into the given Streamlit columns."""
    for col, html in zip(cols, tiles):
        col.markdown(html, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Charts (Plotly, transparent background, ClickHouse palette)
# ---------------------------------------------------------------------------
def _base_layout(fig: go.Figure, height: int = 380) -> go.Figure:
    fig.update_layout(
        height=height,
        margin=dict(l=8, r=16, t=8, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=MUTED, size=12),
        showlegend=False,
    )
    return fig


def time_area(
    df: pd.DataFrame,
    ts_col: str,
    y_col: str,
    label: str,
    color: str = ACCENT,
    fill: str = ACCENT_FILL,
) -> go.Figure:
    """Filled area chart over a time axis."""
    x = pd.to_datetime(df[ts_col])
    fig = go.Figure(
        go.Scatter(
            x=x,
            y=df[y_col],
            mode="lines",
            line=dict(color=color, width=2, shape="spline"),
            fill="tozeroy",
            fillcolor=fill,
            hovertemplate=f"%{{x|%Y-%m-%d %H:%M}} UTC<br><b>%{{y:,}}</b> {label}<extra></extra>",
        )
    )
    _base_layout(fig)
    fig.update_layout(
        xaxis=dict(showgrid=False, tickformat="%H:%M", color=MUTED),
        yaxis=dict(
            gridcolor=BORDER,
            griddash="dot",
            rangemode="tozero",
            color=MUTED,
            tickformat=",",
        ),
    )
    return fig


def bar(
    df: pd.DataFrame,
    cat_col: str,
    val_col: str,
    label: str,
    color: str = ACCENT,
    horizontal: bool = True,
) -> go.Figure:
    """Categorical bar chart. Horizontal by default (nice for ranked lists)."""
    if horizontal:
        d = df.iloc[::-1]  # largest at top
        fig = go.Figure(
            go.Bar(
                x=d[val_col],
                y=d[cat_col].astype(str),
                orientation="h",
                marker=dict(color=color),
                hovertemplate=f"%{{y}}<br><b>%{{x:,}}</b> {label}<extra></extra>",
            )
        )
        _base_layout(fig, height=max(220, 40 * len(df) + 60))
        fig.update_layout(
            xaxis=dict(gridcolor=BORDER, griddash="dot", color=MUTED, tickformat=","),
            yaxis=dict(color=TEXT),
        )
    else:
        fig = go.Figure(
            go.Bar(
                x=df[cat_col].astype(str),
                y=df[val_col],
                marker=dict(color=color),
                hovertemplate=f"%{{x}}<br><b>%{{y:,}}</b> {label}<extra></extra>",
            )
        )
        _base_layout(fig)
        fig.update_layout(
            xaxis=dict(color=TEXT),
            yaxis=dict(gridcolor=BORDER, griddash="dot", color=MUTED, tickformat=","),
        )
    return fig


def donut(df: pd.DataFrame, name_col: str, val_col: str) -> go.Figure:
    """Donut chart — used for share/segment breakdowns."""
    palette = [ACCENT, ACCENT_2, "#7c9cff", "#ff8fab", "#5eead4", "#c4b5fd"]
    fig = go.Figure(
        go.Pie(
            labels=df[name_col].astype(str),
            values=df[val_col],
            hole=0.62,
            marker=dict(colors=palette[: len(df)], line=dict(color=PANEL, width=2)),
            textinfo="label+percent",
            textfont=dict(color=TEXT, size=12),
            hovertemplate="%{label}<br><b>%{value:,}</b> (%{percent})<extra></extra>",
        )
    )
    _base_layout(fig, height=320)
    fig.update_layout(showlegend=False)
    return fig
