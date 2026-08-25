# expense-tracker-mcp

A remote [MCP](https://modelcontextprotocol.io) server for tracking personal
expenses, backed by Postgres, designed to be driven by **two different
clients**: Claude as a connector, and a custom LangGraph agent.

Log an expense by saying "spent 450 on groceries today", then ask "what did I
spend on food this month?" — and get the same answer from either client,
because the state lives in a database rather than in a chat session.

```
Claude (connector) ──┐
                     │
Streamlit UI ──┐     ├─► expense-tracker-mcp ─► Neon Postgres
               ├─────┘        (FastMCP)
Terminal REPL ─┘
   (one LangGraph agent, two front-ends)
```

## Status

| Phase | | |
|---|---|---|
| 1 | Server foundation — typed tools, Postgres, category validation | **done** |
| 2 | LangGraph client — terminal, agent loop, checkpointed memory | **done** |
| 3 | Streamlit frontend on top of the working agent | **done** |
| 4 | Queries scoped to the authenticated user | **done** |

The server is deployed and driven by both clients. Every claim above was
verified by checking what actually landed in Postgres, not by reading what the
model said it did.

## Tools

| Tool | Purpose |
|---|---|
| `whoami` | Which user the server sees, and whether expenses are scoped. |
| `list_categories` | The valid taxonomy, so the model can look it up instead of guessing. |
| `add_expense` | Record one expense. Validates the category before writing. |
| `list_expenses` | Individual rows, newest first. Optional date range and category filters. |
| `delete_expense` | Remove one expense by id. Scoped to the owner. |
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

## The terminal client

[`client.py`](client.py) is the second consumer — a LangGraph agent you talk to
in plain language:

```
you> log 250 on petrol today
  → list_categories()
  ← 20 categories
  → add_expense(date='2026-08-21', amount=250, category='transport', subcategory='fuel')
  ← saved #15 250.00 transport/fuel on 2026-08-21

Logged: ₹250.00 for fuel (transport) on 2026-08-21.
```

Nothing in the taxonomy says "petrol", so the agent looks the categories up
before writing rather than guessing.

```bash
uv sync --group client

uv run --group client python client.py            # against a local server
uv run --group client python client.py --remote   # against the deployed one
```

The client's dependencies live in a **separate group**. The deployment
installs the project on every build and has no use for LangGraph, so keeping
them out of the main list keeps the server's build lean.

Three things it does that a naive chat loop does not:

**A real agent loop.** *"Log 90 on coffee yesterday and then tell me my food
total"* needs two sequential tool calls in one turn. `create_agent` keeps
calling tools until the model stops asking for them; a single-round loop
answers half the question.

**One event loop, one MCP session, for the whole process.** An MCP session is
bound to the event loop it was created on. Calling `asyncio.run()` per message
— which is the obvious way to bolt async onto a REPL, and what the earlier
version of this project did — creates and destroys a loop each time, so the
second message dies with `Event loop is closed`. Here everything runs inside a
single `asyncio.run()` with the session held open.

**Conversation memory that survives restarts.** A LangGraph checkpointer keyed
by `thread_id`, stored in the same Neon database. Quit the client, start it
again, and ask what you said earlier — it knows, because the history is in
Postgres rather than in a list in memory.

The system prompt also injects today's local date, because `add_expense`
deliberately refuses to infer it: the server's clock is UTC and would log the
wrong day either side of midnight. The client knows the user's date; the
server does not.

## The web UI

[`app.py`](app.py) is a Streamlit chat interface over the same agent — same
prompt, same tools, same conversation memory, because both front-ends build
their runtime from [`agent.py`](agent.py) rather than each assembling one.

```bash
uv run --group ui streamlit run app.py
```

Tool calls are shown under each answer, so you can watch the agent look up a
category, get a rejection, and correct itself.

**Streamlit re-executes the whole script on every interaction**, which is
hostile to exactly what this app holds: an MCP session and a psycopg
connection, both bound to the event loop that created them. Rebuilt per rerun
they would be opened and closed on every keystroke. So the async half lives in
**one background thread owning one event loop**, built once behind
`@st.cache_resource`; the script hands coroutines to that loop and waits for
results. Reruns redraw the page and cannot disturb the connections.

The conversation id lives in the **URL**, not in `st.session_state` — session
state is wiped by a browser refresh, which would drop you back into the
default conversation at precisely the moment persistence is supposed to prove
itself. The sidebar lists every stored conversation, read back from the
checkpoint tables, so conversations started in the terminal client appear
there too.

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
uv run python main.py            # stdio, for a client that spawns it
uv run python main.py http       # http://127.0.0.1:8000/mcp
```

stdio is the default because that is what an MCP client spawning this file as
a subprocess expects — JSON-RPC over stdin/stdout.

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

**Every query is scoped to the authenticated user, and there is one place it
can go wrong.** Prefect Horizon terminates auth at its edge and forwards the
identity in `horizon-user-id`; the server reads it in `current_user_id()`.
Reads go through a single helper that always emits `user_id = $1`, so no tool
builds its own `WHERE` clause and none can forget the filter. `delete_expense`
scopes in the `DELETE` itself rather than checking ownership first — one
statement cannot disagree with itself the way a check-then-delete can — and
returns the same "not in your records" answer whether the id is missing or
someone else's, so it never confirms that a row exists.

`user_id` was on the table from day one, defaulted and unused, precisely so
this step would be a change of value rather than a migration. It is
deliberately *not* a tool parameter: if the model could choose it, any client
could read anyone's expenses just by asking.

**Header-based identity is only sound if headers cannot be forged**, so that
was tested rather than assumed: sending `horizon-user-id: 00000000-dead-beef-…`
from a client, the gateway overwrote it and the server still saw the real
subject. Had it not, this would be an assertion rather than authentication and
unusable as a security boundary.

**Logging goes to stderr.** Over the stdio transport, stdout *is* the JSON-RPC
channel, and a stray `print()` corrupts the protocol stream.

**One connection string, two drivers that disagree about it.** `DATABASE_URL`
feeds asyncpg on the server and psycopg in the client's checkpointer. asyncpg
rejects libpq query parameters outright; psycopg is built on libpq and wants
them. So the server strips `sslmode` and passes `ssl="require"` in code, while
the client adds `sslmode=require` back. Both directions are commented, because
the natural assumption — that one DSN works everywhere — is wrong.

**The checkpointer disables prepared statements.** Neon's pooled endpoint is
PgBouncer in transaction mode, which hands a different backend to each
transaction, so a statement prepared on one connection is missing on the next.
`prepare_threshold=None` avoids it. Left on, this fails *intermittently* after
appearing to work, which is a much worse failure than one that shows up
immediately.

**The client sets a selector event loop on Windows.** psycopg's async mode
refuses to run on `ProactorEventLoop`, the Windows default; asyncio
subprocesses on Windows run *only* on `ProactorEventLoop`. A stdio MCP
connection spawns the server as a subprocess, so a stdio transport and an
async Postgres checkpointer cannot share one loop. The client talks HTTP
instead — which is what the deployed setup needs anyway. Neither restriction
exists on Linux or macOS.

## Not implemented yet

Honest limitations rather than oversights:

- **No edit tool.** You can add and delete, but not amend in place; correcting
  an amount means deleting and re-logging.
- **No currency column.** Every amount is assumed to be in one currency; the
  client is told they are rupees.
- **The web UI is single-user.** The server scopes by authenticated user, but
  `app.py` holds one API key, so every browser visitor acts as its owner. Real
  per-user identity applies to people connecting with their own accounts — a
  Claude connector, or their own key. Making the UI multi-user needs its own
  login, which is a different piece of work.
- **No long-term memory.** The agent remembers a conversation, not facts across
  conversations. Those are genuinely different features and only the first is
  built.

## Layout

```
main.py           the server: four tools, one resource
agent.py          the agent: MCP session, checkpointer, model, prompt
client.py         terminal front-end
app.py            Streamlit front-end
schema.sql        one-time table + index creation
categories.json   the category taxonomy, single source of truth
.env.example      documents every variable all three need
```

## Built with

**Server:** [FastMCP 3](https://gofastmcp.com) · [asyncpg](https://github.com/MagicStack/asyncpg) · [Neon Postgres](https://neon.tech) · [Prefect Horizon](https://horizon.prefect.io)

**Client:** [LangGraph](https://langchain-ai.github.io/langgraph/) · [langchain-mcp-adapters](https://github.com/langchain-ai/langchain-mcp-adapters) · [Gemini](https://ai.google.dev) · [Streamlit](https://streamlit.io)
