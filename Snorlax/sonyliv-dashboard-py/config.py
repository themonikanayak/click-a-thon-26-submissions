"""Static configuration for the SonyLIV concurrency dashboard.

Single place for the database name, refresh cadence, assistant model, and the
theme palette.
The palette takes inspiration from ClickHouse's brand: signature bright yellow
(#FAFF69) on a near-black canvas, with an amber secondary accent.
"""

from __future__ import annotations

import os

DB = "sonyliv_concurrency"

# Auto-refresh cadence for still-open ("live") sessions.
REFRESH_MS = 30_000

# --- Insights Copilot backend ------------------------------------------------
# Primary path: the dashboard calls our LOCAL LibreChat's OpenAI-compatible
# "remote agents" API. LibreChat then runs the turn against its own Ollama
# endpoint, so the model is local Ollama but every request flows through — and
# is logged/managed by — LibreChat (see librechat-setup/ for the config + the
# dashboard README → "Connect to LibreChat" for the one-time setup).
#   POST {LIBRECHAT_URL}/chat/completions   (header: Authorization: Bearer <key>)
#   body.model = LIBRECHAT_AGENT_ID  (a LibreChat agent wired to the Ollama model)
# LibreChat's API is published on the host at :3080 by its docker compose.
LIBRECHAT_URL = os.environ.get("LIBRECHAT_URL", "http://localhost:3080/api/agents/v1")
LIBRECHAT_API_KEY = os.environ.get("LIBRECHAT_API_KEY", "")
LIBRECHAT_AGENT_ID = os.environ.get("LIBRECHAT_AGENT_ID", "")

# Fallback path: talk to Ollama directly (the same model LibreChat uses) when
# LibreChat isn't configured/reachable, so the panel still works for a demo.
# The dashboard runs on the host (not in LibreChat's compose network), so it
# reaches Ollama via its published port on localhost rather than the `ollama`
# service DNS name LibreChat's container uses. Set
# ASSISTANT_ALLOW_OLLAMA_FALLBACK=0 to require LibreChat (no direct-Ollama path).
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2:3b")
ALLOW_OLLAMA_FALLBACK = os.environ.get(
    "ASSISTANT_ALLOW_OLLAMA_FALLBACK", "1"
).lower() in ("1", "true", "yes")

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
