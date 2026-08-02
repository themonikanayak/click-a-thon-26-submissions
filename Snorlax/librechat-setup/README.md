# LibreChat setup — local Docker + ClickHouse MCP + dashboard embed

Run [LibreChat](https://github.com/danny-avila/LibreChat) locally on Docker with
a **local Ollama** model and a **ClickHouse MCP server**, then embed it as the
**✨ Copilot** tab of the SonyLIV dashboard. The agent answers viewing-concurrency
questions by running read-only SQL against `sonyliv_concurrency` **itself** (via
MCP) — no paid key, everything local.

```
Dashboard :8501  ──iframe──►  LibreChat :3080
                                 └─ Agent (model = qwen2.5-coder:7b @ Ollama :11434)
                                      ├─ Instructions = agent-guidelines.md
                                      └─ MCP tools ──► mcp-clickhouse :8000
                                                          └─(proxy :443, verify off)─► ClickHouse Cloud
```

LibreChat is **not vendored** in git — clone it separately and drop this folder's
config files into its root.

## Files in this folder
| File | Purpose |
|---|---|
| `docker-compose.override.yaml` | Adds `ollama` + `mcp-clickhouse` services, mounts `librechat.yaml`, blanks the corporate proxy on `api`, and routes the two internet-facing services through the proxy. |
| `librechat.yaml` | Ollama custom endpoint (`qwen2.5-coder:7b`), `mcpServers.clickhouse`, remote-agents enabled. |
| `agent-guidelines.md` | Paste into the agent's **Instructions** — schema + how to query correctly. |

---

## 1. Run the stack on Docker
1. **Clone LibreChat** and copy the two config files into its root:
   ```bash
   git clone https://github.com/danny-avila/LibreChat.git && cd LibreChat
   cp /path/to/Snorlax/librechat-setup/docker-compose.override.yaml .
   cp /path/to/Snorlax/librechat-setup/librechat.yaml .
   ```
2. **Create `.env`** in the LibreChat root (from `.env.example`) and set:
   ```bash
   HOST=0.0.0.0
   PORT=3080
   ALLOW_REGISTRATION=true            # to create the first (admin) user
   # secrets — generate via https://www.librechat.ai/toolkit/creds_generator
   JWT_SECRET=... ; JWT_REFRESH_SECRET=... ; CREDS_KEY=... ; CREDS_IV=... ; MEILI_MASTER_KEY=...
   # --- consumed by docker-compose.override.yaml (mcp-clickhouse) ---
   CLICKHOUSE_PASSWORD=<your ClickHouse Cloud password>
   MCP_AUTH_TOKEN=<any long random string>     # bearer token LibreChat↔MCP
   ```
   `.env` holds secrets — it is **not** committed.
3. **Start it:**
   ```bash
   docker compose up -d          # LibreChat :3080, Ollama :11434, mcp-clickhouse :8000
   docker compose ps             # all three should be Up
   ```
4. **Pull the model** (first run only). Ollama needs internet egress; the override
   already routes it through the proxy:
   ```bash
   docker compose exec ollama ollama pull qwen2.5-coder:7b
   ```

## 2. Verify the MCP server reaches ClickHouse
The MCP server connects to ClickHouse Cloud through the corporate proxy on **port
443** (8443 is blocked) with **`CLICKHOUSE_VERIFY=false`** (the proxy intercepts
TLS with its own CA). Confirm it's serving and can query:
```bash
# reachable on the host (port is published for this check):
curl -s -H "Authorization: Bearer $MCP_AUTH_TOKEN" http://localhost:8000/mcp -o /dev/null -w "%{http_code}\n"
# and from inside the network the way LibreChat reaches it:
docker compose logs mcp-clickhouse   # should show it bound to 0.0.0.0:8000, no ClickHouse errors
```
A `run_query` of `SELECT count() FROM sonyliv_concurrency.concurrency_now` should
return ~89k (matches the dashboard).

## 3. Create the agent (one-time, in the LibreChat UI)
1. Open http://localhost:3080, **register the first user** (becomes admin) — this
   grants agent + MCP permissions.
2. **Agents builder → new agent:**
   - Endpoint = **Ollama**, Model = **`qwen2.5-coder:7b`**.
   - **Instructions** = paste the full contents of `agent-guidelines.md`.
   - **Tools** = enable the **clickhouse** MCP tools (`list_tables`, `run_query`, …).
   - Save.
3. Chat with the agent (e.g. *"What was the peak concurrency and at what minute?"*).
   It should call `run_query` and answer from live data.

## 4. Embed in the dashboard
The dashboard's **✨ Copilot** tab already iframes `http://localhost:3080`
(`config.LIBRECHAT_EMBED_URL`, override with `LIBRECHAT_URL`). If the iframe stays
blank, use the tab's "Open in a new tab" link — some browsers block cross-origin
framing / third-party cookies for `localhost:8501 → :3080`. To allow framing you
can put LibreChat behind a small reverse proxy that sets
`Content-Security-Policy: frame-ancestors 'self' http://localhost:8501` and drops
`X-Frame-Options`.

---

## Proxy / egress notes (restricted networks)
- Only the corporate proxy (`http://proxy.bloomberg.com:81`) can reach the
  internet; direct egress is blocked. The override sets `HTTPS_PROXY` on the
  `ollama` and `mcp-clickhouse` services and **pins the proxy IP** via
  `extra_hosts: ["proxy.bloomberg.com:69.191.241.9"]` (in-container DNS may not
  resolve corporate hosts). If the proxy IP changes, re-resolve it and update the
  override.
- **Fallback if in-container egress fails:** run `mcp-clickhouse` on the **host**
  instead (the proven path) and point `librechat.yaml`'s `mcpServers.clickhouse.url`
  at `http://host.docker.internal:8000/mcp`.

## Troubleshooting
- **Agent can't see the clickhouse tools** — confirm `mcpServers` is in the mounted
  `librechat.yaml` and the stack was restarted; check `docker compose logs api` for
  MCP connection errors. The `mcpServers` schema differs across LibreChat versions
  — older builds use `type: sse` with a `/sse` URL instead of `streamable-http /mcp`.
- **MCP → ClickHouse errors** — verify `CLICKHOUSE_PORT=443`, `CLICKHOUSE_VERIFY=false`,
  and the proxy env/`extra_hosts` on the `mcp-clickhouse` service.
- **Empty replies / model errors** — `docker compose exec ollama ollama pull qwen2.5-coder:7b`.
- **`mcp/clickhouse` image not found** — build it instead (see the commented
  `pip install mcp-clickhouse` alternative in `docker-compose.override.yaml`).

## Current state / stretch
- Local Ollama (`qwen2.5-coder:7b`) + ClickHouse MCP wired; the dashboard Copilot
  is the embedded LibreChat UI.
- Read-only MCP (`CLICKHOUSE_ALLOW_WRITE_ACCESS=false`). Write access is a
  deliberate non-goal for the demo.
