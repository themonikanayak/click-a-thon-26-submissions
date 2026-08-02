"""Static configuration for the SonyLIV concurrency dashboard.

Single place for the database name, refresh cadence, the embedded LibreChat URL,
and the theme palette.
The palette takes inspiration from ClickHouse's brand: signature bright yellow
(#FAFF69) on a near-black canvas, with an amber secondary accent.
"""

from __future__ import annotations

import os

DB = "sonyliv_concurrency"

# Auto-refresh cadence for still-open ("live") sessions.
REFRESH_MS = 30_000

# --- Insights Copilot (embedded LibreChat) -----------------------------------
# The "✨ Copilot" tab embeds LibreChat in an iframe. LibreChat runs a local
# Ollama agent wired to the ClickHouse MCP server, so it queries the live
# concurrency data directly (see ../librechat-setup/). Published on the host at
# :3080 by LibreChat's docker compose.
LIBRECHAT_EMBED_URL = os.environ.get("LIBRECHAT_URL", "http://localhost:3080")

# --- ClickHouse-inspired palette ---------------------------------------------
BG = "#161619"  # near-black canvas
PANEL = "#1f1f24"  # cards / panels
PANEL_2 = "#26262d"  # gradient bottom / inputs
BORDER = "#33333d"
TEXT = "#f4f4f2"  # warm white
MUTED = "#a0a0ab"
ACCENT = "#faff69"  # ClickHouse yellow — primary (peak, chart, live)
ACCENT_2 = "#ffb454"  # amber — secondary (current)
DANGER = "#ff6b6b"

# Yellow at low opacity for the chart's area fill.
ACCENT_FILL = "rgba(250, 255, 105, 0.14)"
