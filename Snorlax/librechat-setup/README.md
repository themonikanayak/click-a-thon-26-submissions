# LibreChat setup — local Docker + dashboard wiring

Run [LibreChat](https://github.com/danny-avila/LibreChat) locally on Docker with
a **local Ollama** model, then point the SonyLIV dashboard's **Insights Copilot**
at it. The dashboard calls LibreChat's OpenAI-compatible *remote agents* API, and
LibreChat runs each turn against Ollama — so the model is local (no paid key) but
every request flows through LibreChat.

```
Streamlit dashboard  ──POST /api/agents/v1/chat/completions──►  LibreChat (:3080)  ──►  Ollama (:11434)
 (host, :8501)          Authorization: Bearer <agent API key>     docker compose        llama3.2:3b
                        model = <agent id>
```

LibreChat itself is **not vendored** in git — clone it separately and drop the
two config files from this folder into its root.

---

## 1. Run LibreChat on Docker

1. **Clone LibreChat** (once), next to `Snorlax/` or wherever you like:
   ```bash
   git clone https://github.com/danny-avila/LibreChat.git
   cd LibreChat
   ```
2. **Copy the config** from this folder into the LibreChat root:
   ```bash
   cp /path/to/Snorlax/librechat-setup/docker-compose.override.yaml .
   cp /path/to/Snorlax/librechat-setup/librechat.yaml .
   ```
   - `docker-compose.override.yaml` adds an **`ollama`** service (published on
     `:11434`), mounts `librechat.yaml`, and blanks any `*_PROXY` vars so
     outbound calls don't try to use a corporate proxy.
   - `librechat.yaml` defines the **Ollama** custom endpoint
     (`http://ollama:11434/v1/`, model `llama3.2:3b`) and **enables remote
     agents** (`interface.remoteAgents`).
3. **Create a `.env`** in the LibreChat root (holds secrets/host settings, so it
   is not committed here). Start from LibreChat's own template and generate the
   required secrets:
   ```bash
   cp .env.example .env
   # Set at minimum (see .env.example for the rest):
   #   HOST=0.0.0.0
   #   PORT=3080
   #   ALLOW_REGISTRATION=true          # so you can create the first (admin) user
   #   JWT_SECRET / JWT_REFRESH_SECRET / CREDS_KEY / CREDS_IV / MEILI_MASTER_KEY
   ```
   Generate the secrets with LibreChat's helper: <https://www.librechat.ai/toolkit/creds_generator>
4. **Start the stack:**
   ```bash
   docker compose up -d
   ```
   LibreChat comes up at **http://localhost:3080**.
5. **Pull the model** into the Ollama container (first run only):
   ```bash
   docker compose exec ollama ollama pull llama3.2:3b
   ```
6. **Sanity-check the model endpoint** (optional):
   ```bash
   curl http://localhost:11434/api/tags        # should list llama3.2:3b
   ```

Open http://localhost:3080, **register the first user** (the first account
becomes an **admin**, which is what grants the remote-agents permission), pick
the **Ollama** endpoint, and confirm you can chat. That proves LibreChat → Ollama
works before you wire the dashboard in.

---

## 2. Remote agents — create the agent + API key (one time)

The dashboard needs (a) an **agent id** and (b) an **agent API key**. Both are
created from the LibreChat UI once remote agents are enabled (already done in
`librechat.yaml`; re-run `docker compose up -d` if you enabled it after first
boot).

1. **Create an agent** wired to Ollama:
   - In LibreChat, open the **Agents** builder.
   - Set **Endpoint = Ollama**, **Model = `llama3.2:3b`**.
   - (Optional) give it the Insights Copilot instructions; the dashboard also
     sends its own system prompt + live context each turn.
   - Save, then copy the **agent id** (the `agent_...` id in the URL / agent
     details). This is the `model` the dashboard sends.
2. **Create an API key** for the remote-agents API:
   - Go to the **API keys** section (available once `remoteAgents` is enabled),
     create a key, and copy it. You only see the full key once.

> Prefer the raw API? With an admin JWT you can `POST /api/apiKeys` and
> `GET /api/agents/v1/models` (lists agents as models). The UI is simpler.

---

## 3. Point the dashboard at LibreChat

In `Snorlax/sonyliv-dashboard-py/`, set three env vars (e.g. in that app's
`.env`), then run the dashboard — full steps in
[`../sonyliv-dashboard-py/README.md`](../sonyliv-dashboard-py/README.md)
→ **Connect to LibreChat**:

```bash
LIBRECHAT_URL=http://localhost:3080/api/agents/v1   # default; usually leave as-is
LIBRECHAT_API_KEY=<the agent API key from step 2>
LIBRECHAT_AGENT_ID=<the agent id from step 2>
```

With those set, the Insights Copilot routes through LibreChat. If they're unset
(or LibreChat is unreachable), it falls back to calling Ollama directly at
`:11434` so the panel still works — set `ASSISTANT_ALLOW_OLLAMA_FALLBACK=0` to
require LibreChat.

---

## Troubleshooting

- **`Could not reach LibreChat` in the panel** — is `docker compose up -d` up? Is
  `LIBRECHAT_AGENT_ID` a real agent id and `LIBRECHAT_API_KEY` the current key?
- **401 / permission errors** — the API key's user must have the remote-agents
  permission. The first-registered user is admin (has it by default); confirm
  `interface.remoteAgents.use: true` is in `librechat.yaml` and the stack was
  restarted after adding it.
- **Empty replies / model errors** — pull the model:
  `docker compose exec ollama ollama pull llama3.2:3b`.
- **Proxy failures** — the override blanks `*_PROXY`; if you still see proxy
  errors, confirm `NO_PROXY` includes `localhost,127.0.0.1,ollama`.

## Current state / stretch

- Local Ollama (`llama3.2:3b`) wired as the model endpoint; dashboard Copilot
  routes through LibreChat's remote-agents API.
- ClickHouse **MCP** integration (so LibreChat agents can query the concurrency
  data directly) is not yet configured — still a stretch item.
