# migrations/

Schema changes and the SQL runner. **Going forward, do not add new stray `.sql`
files elsewhere** — put every schema change here as an idempotent migration.

## Convention (read before adding SQL)

- **No new ad-hoc files.** New schema/behavior changes go into a migration here,
  not a fresh file scattered across the repo. Editing an existing `schema/*.sql`
  file in place is fine; *new* objects/changes belong in a numbered migration.
- **Idempotent.** Every migration must be safe to run any number of times: use
  `CREATE ... IF NOT EXISTS`, `DROP ... IF EXISTS`, `CREATE OR REPLACE VIEW`,
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, and `ReplacingMergeTree` + `FINAL`.
  Re-running must never double-count or error.
- **Ordered & additive.** Name migrations `NNN_short_description.sql`
  (`001_...`, `002_...`). `--migrate` applies them in filename order.
- **`reset.sql` is special** — it DROPs every object (the "drop and recreate"
  flow) and is *not* part of `--migrate`. It only runs when you pass `--reset`.

## The runner — `run_sql.py`

Connects to ClickHouse Cloud using the **same** `.env` as the producer
(`../producer/.env`: `CLICKHOUSE_HOST`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_PORT`,
`CLICKHOUSE_USER`, `CLICKHOUSE_SECURE`, `CLICKHOUSE_DATABASE`). Use the producer's
virtualenv (it already has `clickhouse-connect` + `python-dotenv`):

```bash
cd Snorlax/producer
source .venv/bin/activate          # or: python -m venv .venv && pip install -r requirements.txt
cd ../migrations
```

| Command | What it does |
| --- | --- |
| `python run_sql.py --reset --build` | **Drop everything, then recreate the schema** (config.sql + schema.sql). |
| `python run_sql.py --build` | Recreate the schema structure only (idempotent). |
| `python run_sql.py --all` | Full offline pipeline: config → schema → seed → backfill → approaches → compare → verify. |
| `python run_sql.py --migrate` | Apply the ordered migrations (`NNN_*.sql`) in this dir. |
| `python run_sql.py -i` | Interactive REPL (end statements with `;`, `\q` / Ctrl-D to quit). |
| `python run_sql.py -c "SELECT 1"` | Run one inline statement. |
| `python run_sql.py ../schema/config.sql ../schema/schema.sql` | Run specific file(s) in order. |

All statements in a run share one client + session (so the `TEMPORARY TABLE` in
`backfill_history.sql` survives across statements). The run **stops at the first
error** unless you pass `--continue-on-error`.
