"""Insights Copilot — the dashboard's context-aware chat panel.

Primary path: call our LOCAL LibreChat's OpenAI-compatible "remote agents" API
(`POST {LIBRECHAT_URL}/chat/completions`, `Authorization: Bearer <key>`,
`model` = a LibreChat agent id). LibreChat runs the turn against its configured
Ollama endpoint, so the underlying model is local Ollama, but every request
flows through — and is logged/managed by — LibreChat at :3080.

Fallback path: if LibreChat isn't configured (no API key / agent id) or is
unreachable, and `ASSISTANT_ALLOW_OLLAMA_FALLBACK` is on (default), talk to
Ollama directly with the same model so the panel still works for a demo.

No paid API key required either way — the model is the local Ollama one.
"""

from __future__ import annotations

import guardrails  # noqa: F401  (must run first — strips Bloomberg proxy env vars)

import json
import urllib.error
import urllib.request

from config import (
    ALLOW_OLLAMA_FALLBACK,
    LIBRECHAT_AGENT_ID,
    LIBRECHAT_API_KEY,
    LIBRECHAT_URL,
    OLLAMA_MODEL,
    OLLAMA_URL,
)

SYSTEM_PROMPT = (
    "You are the Insights Copilot embedded in the SonyLIV viewing-concurrency "
    "dashboard. Answer questions using the supplied dashboard context. Do not "
    "invent metrics that aren't present, and clearly say so when the context "
    "doesn't contain what's needed to answer."
)

_TIMEOUT = 180  # seconds — local models can be slow on first token


def _build_messages(context: dict, history: list[dict]) -> list[dict]:
    """System prompt (with the current dashboard context) + recent history."""
    return [
        {
            "role": "system",
            "content": f"{SYSTEM_PROMPT}\n\nDashboard context:\n"
            f"{json.dumps(context, default=str)}",
        },
        *history[-8:],
    ]


def _post_json(url: str, payload: dict, headers: dict[str, str]) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
        return json.loads(resp.read())


def _librechat_configured() -> bool:
    return bool(LIBRECHAT_API_KEY and LIBRECHAT_AGENT_ID)


def _ask_librechat(messages: list[dict]) -> str:
    """OpenAI-compatible call to LibreChat's remote-agents endpoint."""
    body = _post_json(
        f"{LIBRECHAT_URL}/chat/completions",
        {
            "model": LIBRECHAT_AGENT_ID,
            "messages": messages,
            "stream": False,
            "temperature": 0.2,
        },
        {"Authorization": f"Bearer {LIBRECHAT_API_KEY}"},
    )
    return body["choices"][0]["message"]["content"]


def _ask_ollama(messages: list[dict]) -> str:
    """Direct call to Ollama's native chat API (fallback)."""
    body = _post_json(
        f"{OLLAMA_URL}/api/chat",
        {
            "model": OLLAMA_MODEL,
            "messages": messages,
            "stream": False,
            "options": {"temperature": 0.2},
        },
        {},
    )
    return body["message"]["content"]


def _librechat_hint(err: object) -> str:
    return (
        f"⚠ Could not reach LibreChat at `{LIBRECHAT_URL}`: {err}. Check that "
        "LibreChat is running (`docker compose up -d` in `LibreChat/`), that "
        "remote agents are enabled, and that `LIBRECHAT_API_KEY` / "
        "`LIBRECHAT_AGENT_ID` are correct — see the dashboard README "
        "→ 'Connect to LibreChat'."
    )


def _ollama_hint(err: object) -> str:
    return (
        f"⚠ Could not reach Ollama at `{OLLAMA_URL}`: {err}. Is it running? "
        f"(`docker compose up -d ollama` in `LibreChat/`, then "
        f"`docker compose exec ollama ollama pull {OLLAMA_MODEL}`)"
    )


def ask_assistant(context: dict, history: list[dict]) -> str:
    """Send the conversation + current dashboard context, return the reply text.

    `history` is the full `st.session_state.chat_messages` list (role/content
    dicts), already including the latest user message. Routes through LibreChat
    when configured, otherwise (or on failure) falls back to Ollama.
    """
    messages = _build_messages(context, history)

    # --- Primary: LibreChat remote-agents API --------------------------------
    if _librechat_configured():
        try:
            return _ask_librechat(messages)
        except Exception as e:  # noqa: BLE001
            if not ALLOW_OLLAMA_FALLBACK:
                return _librechat_hint(getattr(e, "reason", e))
            # else: fall through to the Ollama fallback below.
    elif not ALLOW_OLLAMA_FALLBACK:
        return (
            "⚠ Insights Copilot isn't wired to LibreChat yet. Set "
            "`LIBRECHAT_API_KEY` and `LIBRECHAT_AGENT_ID` (see the dashboard "
            "README → 'Connect to LibreChat'), or allow the Ollama fallback "
            "with `ASSISTANT_ALLOW_OLLAMA_FALLBACK=1`."
        )

    # --- Fallback: direct Ollama ---------------------------------------------
    try:
        return _ask_ollama(messages)
    except urllib.error.URLError as e:
        return _ollama_hint(e.reason)
    except Exception as e:  # noqa: BLE001
        return f"⚠ Assistant request failed: {e}"
