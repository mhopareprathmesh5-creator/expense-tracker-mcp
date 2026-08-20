# expense-tracker-mcp

A remote [MCP](https://modelcontextprotocol.io) server for tracking personal
expenses, backed by Postgres, designed to be driven by **two different
clients**: Claude as a connector, and a custom LangGraph agent.

Log an expense by saying "spent 450 on groceries today", then ask "what did I
spend on food this month?" — and get the same answer from either client,
because the state lives in a database rather than in a chat session.

```
Claude (connector) ─┐
                    ├─► expense-tracker-mcp ─► Neon Postgres
LangGraph agent ────┘        (FastMCP)
```

## Status

| Phase | | |
|---|---|---|
| 1 | Server foundation — typed tools, Postgres, category validation | **works locally** |
| 2 | LangGraph client — terminal, `create_react_agent`, checkpointed memory | not started |
| 3 | Streamlit frontend on top of the working agent | not started |
| 4 | OAuth 2.1, queries scoped to the authenticated user | not started |

Phase 1 is verified end to end against a real Neon database. Deployment is the
next step.

## Tools

| Tool | Purpose |
|---|---|
| `list_categories` | The valid taxonomy, so the model can look it up instead of guessing. |
| `add_expense` | Record one expense. Validates the category before writing. |
| `list_expenses` | Individual rows, newest first. Optional date range and category filters. |
| `summarize` | Totals over a date range, grouped by category — or by subcategory when you filter to one category. |

The taxonomy is also published as a resource, `expenses://categories`. That
duplication is deliberate, and testing against Claude is what put it there:
resources are the *correct* MCP primitive for read-only reference data, but a
client only reads one when a **user** attaches it — models are handed tools,
not resources. Asked "what categories can I use?", Claude reported the
taxonomy as unavailable and offered to write a junk row so it could read the
valid values off the rejection error. The tool is what the model can actually
reach; the resource remains for clients that browse resources directly.

Categories are a fixed two-level taxonomy defined in
[`categories.json`](categories.json) — 20 categories, each with subcategories.
Anything outside it is rejected with the valid values included in the error, so
the model can correct itself in one round trip.

## Running it locally

**Prerequisites:** Python 3.10+, [uv](https://docs.astral.sh/uv/), and a
[Neon](https://neon.tech) account (the free tier is enough).

```bash
git clone https://github.com/<your-username>/expense-tracker-mcp
cd expense-tracker-mcp
uv sync
```

**Configure the database.** Copy the example file and fill in your Neon
connection string:

```bash
cp .env.example .env      # PowerShell: Copy-Item .env.example .env
```

Two things matter about that string:

- Use the **pooled** connection — the host contains `-pooler`.
- **Strip the `?sslmode=require&channel_binding=require` query string.**
  asyncpg doesn't accept libpq's query parameters and will raise
  `invalid dsn: invalid connection option "sslmode"`. TLS is requested
  explicitly in code instead. (The server strips these defensively too, so a
  raw pasted string still works.)

**Create the table.** Run [`schema.sql`](schema.sql) once, in the Neon SQL
Editor or any Postgres client. Every statement is idempotent.

**Start the server:**

```bash
uv run python main.py            # http://127.0.0.1:8000/mcp
```

**Or explore it interactively** with the MCP Inspector (needs Node):

```bash
uv run fastmcp dev inspector main.py
```

A browser `GET` on `/mcp` returns **406 Not Acceptable**. That's correct, not a
failure — MCP requires `POST` with
`Accept: application/json, text/event-stream`.

## Deploying

Built for [Prefect Horizon](https://horizon.prefect.io) (formerly FastMCP
Cloud). Point it at this repo with entrypoint `main.py:mcp` and set
`DATABASE_URL` in the environment variables. Deployed servers get a
`*.fastmcp.app` URL, which can be added directly to Claude as a connector.

Note there is deliberately **no `.python-version` file**. Horizon builds with
`UV_PROJECT_ENVIRONMENT=/usr/local`, a system Python prefix rather than a
virtualenv; a version pin makes uv reject it, download a managed CPython, and
fail trying to recreate a non-venv directory. The `requires-python = ">=3.10"`
floor in `pyproject.toml` is sufficient.

## Design decisions

**Money is `NUMERIC(12,2)`, never a float.** Binary floating point cannot
represent `0.1` exactly, so summing float amounts accumulates error and totals
drift by cents. Amounts are `Decimal` in Python, `NUMERIC` in Postgres, and
cross the wire as *strings* — JSON numbers are IEEE-754 doubles, so
serialising as a float would reintroduce the drift at the very last step.
`450.55 + 120.45` returns exactly `571.00`.

**The connection pool is created lazily, never at import.** Connecting at
import time turns a transient database problem into a failed *deploy*; a lazy
pool turns it into one failed tool call the caller can retry. Schema creation
is likewise a separate one-time script, not something the server does on boot.

**Every parameter is annotated.** FastMCP builds the JSON schema the model sees
from type hints, so `date: date` reaches the model as
`{"type": "string", "format": "date"}` and `amount` carries
`exclusiveMinimum: 0`. Untyped parameters measurably degrade tool-calling
accuracy — and invalid input is rejected by schema validation before the tool
body runs at all.

**Every tool returns a dict, on success and on failure alike**, with an `ok`
key. A tool that returns a list on success and a dict on error forces every
caller to type-check before using the result.

**`user_id` exists from day one**, defaulted and currently unused; phase 4
scopes every query by it. Adding a `NOT NULL` column to a populated table later
is a migration — adding it now is free. It is deliberately *not* a tool
parameter: if the model could choose the `user_id`, any client could read
anyone's expenses just by asking.

**Logging goes to stderr.** Over the stdio transport, stdout *is* the JSON-RPC
channel, and a stray `print()` corrupts the protocol stream.

## Not implemented yet

Honest limitations rather than oversights:

- **No edit or delete tools.** Correcting a mis-logged expense means going to
  the database directly. Deferred until it proves annoying in practice.
- **No currency column.** Every amount is assumed to be in one currency.
- **No authentication.** Every expense is written as `user_id = 'default'`, so
  the deployed server is single-tenant until phase 4.

## Layout

```
main.py           the server: three tools, one resource
schema.sql        one-time table + index creation
categories.json   the category taxonomy, single source of truth
.env.example      documents DATABASE_URL
```

## Built with

[FastMCP 3](https://gofastmcp.com) · [asyncpg](https://github.com/MagicStack/asyncpg) · [Neon Postgres](https://neon.tech)
