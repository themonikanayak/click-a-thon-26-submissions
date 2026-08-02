# SonyLIV Concurrency Dashboard (Python / Streamlit)

A Python dashboard for **live viewing-concurrency insights** served from
ClickHouse Cloud (`sonyliv_concurrency`). It reads the `concurrency_now` serving
view and renders a filtered per-minute concurrency curve plus peak / current /
average KPI tiles, with auto-refresh for still-open sessions.

This is a Python (Streamlit) port of the original Next.js dashboard — same data,
no Node/JS toolchain required — restyled to take inspiration from ClickHouse's
brand (signature yellow `#FAFF69` on near-black, with a columnar bar logo mark).

## Architecture

```
Streamlit app  ──►  clickhouse_client  ──►  ClickHouse Cloud
 filters/charts      cached CH client        concurrency_now + content_dim
 + ✨ Copilot tab    parameterized SQL
      └── iframe ──►  LibreChat (:3080) ──►  Ollama agent ──MCP──► ClickHouse Cloud
```

| File | Responsibility |
|---|---|
| `app.py` | App shell: header, global filters, 4 tabs (incl. embedded Copilot) |
| `ui.py` | Shared UI: theme CSS, KPI tiles, chart builders |
| `queries.py` | Concurrency SQL (**real** — `concurrency_now`) |
| `errors.py` | Errors pane data — placeholder SQL + sample data |
| `insights.py` | Insights pane data — placeholder joins + sample data |
| `clickhouse_client.py` | `.env` loading + cached ClickHouse client |
| `guardrails.py` | Strips Bloomberg-proxy env vars before any outbound HTTP call |
| `config.py` | DB name, refresh cadence, embedded LibreChat URL, theme palette |
| `.streamlit/config.toml` | ClickHouse-inspired theme (yellow on near-black) |

### Panes
1. **📈 Concurrency** — real data from `concurrency_now`: peak/current/avg/min/p95
   stats, per-minute curve, per-dimension breakdowns, top content.
2. **🚨 Errors** — playback-error metrics (over time, by type, by platform, top
   messages). **Placeholder sample data** — intended SQL lives in `errors.py`
   (`SQL_*` constants); swap in real queries once an errors table exists.
3. **🧭 Insights** — derived user & content analytics from joined datasets
   (top content, engagement by category, user segments). **Placeholder sample
   data** — intended joins live in `insights.py`.
4. **✨ Copilot** — an **embedded LibreChat** chat (iframe) whose agent queries
   the live database itself. See below.

### Insights Copilot (✨) — embedded LibreChat + ClickHouse MCP
The Copilot is a **fourth tab** that embeds the LibreChat web UI in an iframe
(`config.LIBRECHAT_EMBED_URL`, default `http://localhost:3080`) — not a native
Streamlit chat. Inside LibreChat, a local **Ollama** agent (`qwen2.5-coder:7b`,
no paid key) is wired to a **ClickHouse MCP server**, so it answers by running
read-only `SELECT`s against `sonyliv_concurrency` directly — grounded by the
schema/query guidelines in
[`../librechat-setup/agent-guidelines.md`](../librechat-setup/agent-guidelines.md).

Full setup (Docker compose, model pull, MCP wiring, agent creation) lives in
**[`../librechat-setup/README.md`](../librechat-setup/README.md)**. If LibreChat
isn't running, the tab shows an "Open in a new tab" link and the iframe stays
blank — the three data tabs still work independently.

All dashboard queries use ClickHouse named params (`{name:Type}`); empty string
`''` means **all** for a dimension and **full range** for `from`/`to`.

## Setup

1. **Install** (Python 3.10+):
   ```bash
   python3 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. **Credentials** — no setup needed if `../producer/.env` exists: the app
   reuses it automatically. To use separate credentials instead:
   ```bash
   cp .env.example .env    # then fill it in
   ```
   Keys match `producer/produce_events.py`: `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`,
   `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_SECURE`,
   `CLICKHOUSE_DATABASE`. `.env` is gitignored.
3. **(Optional) Insights Copilot** — bring up LibreChat + Ollama + the ClickHouse
   MCP server (Docker) so the **✨ Copilot** tab works. One-time walkthrough in
   [`../librechat-setup/README.md`](../librechat-setup/README.md). You can skip
   this and still run the three data tabs; the Copilot tab just stays blank.
   Override the embed URL with `LIBRECHAT_URL` if LibreChat isn't on `:3080`.
4. **Run**:
   ```bash
   streamlit run app.py      # http://localhost:8501
   ```

## Features

- **Filters**: platform, country, video type, category, content, and a from/to
  time window (blank = full data range).
- **KPI tiles**: peak concurrency (+ its minute), current concurrency (latest
  minute in range), average concurrency over the range.
- **Concurrency curve**: per-minute Plotly area chart, filter-reactive.
- **Live updates**: auto-refresh every 30s (toggle) + manual refresh + a
  "last updated" stamp.
- **✨ Insights Copilot**: embedded LibreChat agent that queries ClickHouse via
  MCP — see [Insights Copilot](#insights-copilot--embedded-librechat--clickhouse-mcp) above.

## Stack

Python · Streamlit · `clickhouse-connect` · Plotly · pandas · embedded LibreChat
(local Ollama `qwen2.5-coder:7b`) + ClickHouse MCP server — all local, no paid API.
