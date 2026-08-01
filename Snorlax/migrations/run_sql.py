#!/usr/bin/env python3
"""
Execute SQL against ClickHouse Cloud — files, inline commands, or an interactive
REPL. The one tool for building/rebuilding the Snorlax schema and for ad-hoc
queries. Reuses the producer's connection convention (clickhouse_connect + .env).

Connection comes from the SAME env as the producer (Snorlax/producer/.env):
  CLICKHOUSE_HOST      (required)
  CLICKHOUSE_PASSWORD  (required)
  CLICKHOUSE_PORT      (default 8443)
  CLICKHOUSE_USER      (default 'default')
  CLICKHOUSE_SECURE    (default 'true')
  CLICKHOUSE_DATABASE  (default 'sonyliv_concurrency')

Usage:
    # Interactive REPL (end statements with ';', Ctrl-D or \\q to quit):
    python run_sql.py -i

    # Drop everything and recreate the schema from a clean slate:
    python run_sql.py --reset --build

    # Full offline pipeline (config → schema → seed → backfill → approaches → compare → verify):
    python run_sql.py --all

    # Apply the ordered idempotent migrations in this directory:
    python run_sql.py --migrate

    # Run specific .sql file(s), in order:
    python run_sql.py ../schema/00_config.sql ../schema/01_schema.sql

    # One-off inline command:
    python run_sql.py -c "SELECT count() FROM sonyliv_concurrency.events_raw"

All statements in a run share ONE client + session, so cross-statement session
state (e.g. the TEMPORARY TABLE in 03_backfill.sql) survives. By default the
run STOPS at the first error; pass --continue-on-error to keep going.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import clickhouse_connect
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths — this script lives in Snorlax/migrations/.
# ---------------------------------------------------------------------------
HERE = Path(__file__).resolve().parent          # .../Snorlax/migrations
SNORLAX = HERE.parent                            # .../Snorlax
SCHEMA_DIR = SNORLAX / "schema"
PRODUCER_ENV = SNORLAX / "producer" / ".env"

# Load the producer's .env first (the canonical credentials), then any .env in
# the current dir as an override-free fallback (load_dotenv never overrides).
load_dotenv(PRODUCER_ENV)
load_dotenv()

# ---------------------------------------------------------------------------
# Connection config (from env) — fail fast on required vars.
# ---------------------------------------------------------------------------
HOST = os.environ["CLICKHOUSE_HOST"]
PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
PORT = int(os.getenv("CLICKHOUSE_PORT", "8443"))
USER = os.getenv("CLICKHOUSE_USER", "default")
SECURE = os.getenv("CLICKHOUSE_SECURE", "true").lower() in ("1", "true", "yes")
DATABASE = os.getenv("CLICKHOUSE_DATABASE", "sonyliv_concurrency")

# Statements whose leading keyword means "return a result set to print" (vs. DDL
# / INSERT, which we run as a command and just acknowledge).
READ_KEYWORDS = {"SELECT", "WITH", "SHOW", "DESC", "DESCRIBE", "EXPLAIN", "EXISTS"}

# The canonical offline build order (see README / CLAUDE.md). `--build` runs just
# the structural pair; `--all` runs the whole pipeline including seed + verify.
BUILD_FILES = ["00_config.sql", "01_schema.sql"]
PIPELINE_FILES = [
    "00_config.sql",
    "01_schema.sql",
    "02_seed.sql",
    "03_backfill.sql",
    "04_approaches.sql",
    "05_compare.sql",
    "06_verify.sql",
]


def split_sql(sql: str) -> list[str]:
    """Split a SQL script into individual statements on top-level semicolons.

    Ignores semicolons inside single-quoted strings, backtick identifiers, and
    `--` line / `/* */` block comments — so it survives the comment-heavy schema
    files and string literals like 'sonyliv_concurrency.content_dict'.
    """
    statements: list[str] = []
    buf: list[str] = []
    i, n = 0, len(sql)
    in_line_comment = in_block_comment = in_string = in_backtick = False
    while i < n:
        c = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if in_line_comment:
            buf.append(c)
            if c == "\n":
                in_line_comment = False
        elif in_block_comment:
            buf.append(c)
            if c == "*" and nxt == "/":
                buf.append(nxt)
                i += 2
                in_block_comment = False
                continue
        elif in_string:
            buf.append(c)
            if c == "\\" and nxt:          # backslash escape inside a string
                buf.append(nxt)
                i += 2
                continue
            if c == "'":
                in_string = False
        elif in_backtick:
            buf.append(c)
            if c == "`":
                in_backtick = False
        elif c == "-" and nxt == "-":
            in_line_comment = True
            buf.append(c)
        elif c == "/" and nxt == "*":
            in_block_comment = True
            buf.append(c)
            buf.append(nxt)
            i += 2
            continue
        elif c == "'":
            in_string = True
            buf.append(c)
        elif c == "`":
            in_backtick = True
            buf.append(c)
        elif c == ";":
            stmt = "".join(buf).strip()
            if stmt and _leading_keyword(stmt):   # skip comment-only / blank segments
                statements.append(stmt)
            buf = []
        else:
            buf.append(c)
        i += 1
    tail = "".join(buf).strip()
    if tail and _leading_keyword(tail):           # trailing comment block is not a statement
        statements.append(tail)
    return statements


def _leading_keyword(stmt: str) -> str:
    """First SQL keyword of a statement, uppercased, skipping leading comments."""
    i, n = 0, len(stmt)
    while i < n:
        if stmt[i].isspace():
            i += 1
        elif stmt.startswith("--", i):
            j = stmt.find("\n", i)
            i = n if j == -1 else j + 1
        elif stmt.startswith("/*", i):
            j = stmt.find("*/", i)
            i = n if j == -1 else j + 2
        else:
            break
    word = []
    while i < n and (stmt[i].isalpha() or stmt[i] == "_"):
        word.append(stmt[i])
        i += 1
    return "".join(word).upper()


def _short(stmt: str, width: int = 90) -> str:
    """One-line preview of a statement for logging."""
    flat = " ".join(stmt.split())
    return flat if len(flat) <= width else flat[: width - 1] + "…"


def _print_result(result) -> None:
    """Pretty-print a query result set to stdout (tab-separated)."""
    cols = result.column_names
    rows = result.result_rows
    if not rows:
        print("(0 rows)")
        return
    print("\t".join(str(c) for c in cols))
    for row in rows:
        print("\t".join("" if v is None else str(v) for v in row))
    print(f"({len(rows)} row{'s' if len(rows) != 1 else ''})")


def run_statement(client, stmt: str) -> None:
    """Execute one statement, routing reads to query() and everything else to
    command(). Errors propagate to the caller (which decides whether to stop)."""
    if _leading_keyword(stmt) in READ_KEYWORDS:
        _print_result(client.query(stmt))
    else:
        client.command(stmt)
        print(f"  ok  {_short(stmt)}", file=sys.stderr)


def run_script(client, path: Path, continue_on_error: bool) -> int:
    """Run every statement in a .sql file, in order. Returns count of errors."""
    print(f"\n=== {path} ===", file=sys.stderr)
    errors = 0
    for stmt in split_sql(path.read_text()):
        try:
            run_statement(client, stmt)
        except Exception as exc:  # noqa: BLE001 — surface any engine error verbatim
            errors += 1
            print(f"  ERR {_short(stmt)}\n      {exc}", file=sys.stderr)
            if not continue_on_error:
                raise
    return errors


def interactive(client) -> None:
    """A minimal REPL: accumulate lines until a ';' terminates the buffer, then
    execute. `\\q` / `quit` / `exit` (or Ctrl-D) leaves."""
    print(
        "Interactive ClickHouse SQL. End statements with ';'. "
        "\\q or Ctrl-D to quit.",
        file=sys.stderr,
    )
    buf: list[str] = []
    while True:
        try:
            line = input("snorlax> " if not buf else "     ..> ")
        except EOFError:
            print(file=sys.stderr)
            break
        if not buf and line.strip().lower() in ("\\q", "\\quit", "quit", "exit"):
            break
        buf.append(line)
        if not line.rstrip().endswith(";"):
            continue
        for stmt in split_sql("\n".join(buf)):
            try:
                run_statement(client, stmt)
            except Exception as exc:  # noqa: BLE001 — keep the REPL alive on error
                print(f"ERROR: {exc}", file=sys.stderr)
        buf = []


def _resolve(files: list[str], base: Path) -> list[Path]:
    """Resolve script names against `base`, erroring on any missing file."""
    resolved = []
    for name in files:
        p = Path(name)
        if not p.is_absolute():
            p = base / name
        if not p.is_file():
            sys.exit(f"error: SQL file not found: {p}")
        resolved.append(p)
    return resolved


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Execute SQL against ClickHouse Cloud (files / inline / REPL).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("files", nargs="*", help="SQL file(s) to run, in order.")
    parser.add_argument("-c", "--command", help="Run a single inline SQL statement.")
    parser.add_argument("-i", "--interactive", action="store_true", help="Start a REPL.")
    parser.add_argument("--reset", action="store_true",
                        help="Run migrations/reset.sql first (drops all objects).")
    parser.add_argument("--build", action="store_true",
                        help="Recreate the schema structure (00_config.sql + 01_schema.sql).")
    parser.add_argument("--all", action="store_true",
                        help="Run the full offline pipeline (build → seed → backfill → "
                             "approaches → compare → verify).")
    parser.add_argument("--migrate", action="store_true",
                        help="Apply ordered idempotent migrations (NNN_*.sql) in this dir.")
    parser.add_argument("--continue-on-error", action="store_true",
                        help="Keep going after a failing statement (default: stop).")
    args = parser.parse_args()

    did_something = any([args.reset, args.build, args.all, args.migrate,
                         args.files, args.command, args.interactive])
    if not did_something:
        parser.print_help()
        return 0

    # One client + session for the whole run, so TEMPORARY TABLEs and other
    # session state persist across the split statements (e.g. backfill's _wm).
    client = clickhouse_connect.get_client(
        host=HOST, port=PORT, username=USER, password=PASSWORD, secure=SECURE,
        database=DATABASE, session_id=f"snorlax-runner-{os.getpid()}",
    )
    print(f"connected to {HOST}:{PORT} → {DATABASE}", file=sys.stderr)

    errors = 0
    try:
        if args.reset:
            errors += run_script(client, HERE / "reset.sql", args.continue_on_error)
        if args.build:
            for p in _resolve(BUILD_FILES, SCHEMA_DIR):
                errors += run_script(client, p, args.continue_on_error)
        if args.all:
            for p in _resolve(PIPELINE_FILES, SCHEMA_DIR):
                errors += run_script(client, p, args.continue_on_error)
        if args.migrate:
            migrations = sorted(
                p for p in HERE.glob("*.sql") if p.name != "reset.sql"
            )
            if not migrations:
                print("no migrations to apply (migrations/NNN_*.sql)", file=sys.stderr)
            for p in migrations:
                errors += run_script(client, p, args.continue_on_error)
        if args.files:
            for p in _resolve(args.files, Path.cwd()):
                errors += run_script(client, p, args.continue_on_error)
        if args.command:
            try:
                run_statement(client, args.command)
            except Exception as exc:  # noqa: BLE001
                errors += 1
                print(f"ERROR: {exc}", file=sys.stderr)
                if not args.continue_on_error:
                    raise
        if args.interactive:
            interactive(client)
    finally:
        client.close()

    if errors:
        print(f"\ndone with {errors} error(s).", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
