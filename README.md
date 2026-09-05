# expense-tracker-mcp

**[Try it →](https://expense-tracker-mcp.streamlit.app)**  ·  sign in with
Google; your expenses are private to you

A remote [MCP](https://modelcontextprotocol.io) server for tracking personal
expenses, backed by Postgres, driven by **three different clients**: Claude as
a connector, a terminal agent, and a web app.

Log an expense by saying "spent 450 on groceries today", then ask "what did I
spend on food this month?" — and get the same answer from any of them, because
the state lives in a database rather than in a chat session.

```
Claude (connector) ──┐
                     │
Streamlit web app ─┐ ├─► expense-tracker-mcp ─► Neon Postgres
                   ├─┘        (FastMCP)
Terminal REPL ─────┘
   (one LangGraph agent, two front-ends)
```

Every query is scoped to the authenticated user, so several people share the
deployment without sharing a ledger.

## Status

| Phase | | |
|---|---|---|
| 1 | Server foundation — typed tools, Postgres, category validation | **done** |
| 2 | LangGraph client — terminal, agent loop, checkpointed memory | **done** |
| 3 | Streamlit frontend on top of the working agent | **done** |
| 4 | Queries scoped to the authenticated user | **done** |
| 5 | Google sign-in, so the web app is multi-user too | **done** |

All five are deployed and in use. Every claim was verified by checking what
actually landed in Postgres, not by reading what the model said it did — the
`id` sequence is a useful receipt here, since rejected input never advances it.

## Tools

| Tool | Purpose |
|---|---|
| `whoami` | Which user the server sees, and whether expenses are scoped. |
| `list_categories` | The valid taxonomy, so the model can look it up instead of guessing. |
| `add_expense` | Record one expense. Validates the category before writing. |
| `list_expenses` | Individual rows, newest first. Optional date range and category filters. |
| `edit_expense` | Change any field of one expense. Only what you pass is touched. |
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

**Live at [expense-tracker-mcp.streamlit.app](https://expense-tracker-mcp.streamlit.app)** —
sign in with Google and you get your own private ledger.

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
itself. The sidebar lists your stored conversations, read back from the
checkpoint tables, so conversations started in the terminal client appear
there too.

### How the web app knows who you are

This is the interesting part, because a web app cannot authenticate to the MCP
server the way Claude does.

Each Claude user makes their **own** connection, so the gateway sees a distinct
caller and can identify each one. The web app is one program with one API key
serving many people — the gateway sees a single caller and never learns the
humans exist. Keeping track of them is the app's job.

So the app signs users in with Google, then tells the server whose request each
one is:

```
POST /mcp
Authorization: Bearer <the app's API key>    may this caller connect?     yes
x-app-secret:  <shared secret>               is this really my app?       yes
x-app-user:    alice@gmail.com               whose expense is this?    Alice's
```

The key opens the door; the label on the request decides whose row it becomes.

**The secret is what makes the label trustworthy.** `x-app-user` is only text —
without proof of who wrote it, anyone holding an API key could name any user
and read their expenses. The server accepts an asserted identity *only* when
the shared secret matches, verified with `secrets.compare_digest` so a wrong
value cannot be found one character at a time by timing. If the secret is
unset, the path is disabled rather than open, and the app shows an error banner
when the server disagrees with the browser session — it fails loudly instead of
quietly writing to the wrong ledger.

Identity is keyed on **email**, because two doors reach the same person: Claude
gives a gateway UUID, Google gives a Google subject. Keyed on ids, one human
would own two unrelated ledgers. The tradeoff is that emails are mutable, so
changing yours orphans your history — fine here, wrong for a bank, where you
would keep an immutable id and a separate email column.

## Running it locally

**Prerequisites:** Python 3.10+, [uv](https://docs.astral.sh/uv/), and a
[Neon](https://neon.tech) account (the free tier is enough).

```bash
git clone https://github.com/mhopareprathmesh5-creator/expense-tracker-mcp
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

**To run the web app**, add Google sign-in credentials as well:

```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
uv run --group ui streamlit run app.py
```

Fill in a Google OAuth client id and secret (Google Cloud → *Google Auth
Platform* → *Clients* → Web application), with
`http://localhost:8501/oauth2callback` as an authorized redirect URI — Google
refuses to send users anywhere not registered in advance, which is what
`redirect_uri_mismatch` means. `APP_SHARED_SECRET` in `.env` must match the
value set on the server, or the server ignores the app's claim about who is
signed in and everyone falls back to one ledger.

## Deploying

**The server** runs on [Prefect Horizon](https://horizon.prefect.io) (formerly
FastMCP Cloud). Point it at this repo with entrypoint `main.py:mcp` and set
`DATABASE_URL` and `APP_SHARED_SECRET` in its environment variables. Deployed
servers get a `*.fastmcp.app` URL, which can be added directly to Claude as a
connector.

Horizon reads environment variables at **container start**, so adding one is
not enough — the app must be redeployed, or it keeps running with the old
value. An unset `APP_SHARED_SECRET` looks identical to a wrong one from the
outside, which is why `whoami` reports whether the server has a secret, whether
the headers arrived, and whether they matched.

**The web app** runs on [Streamlit Community Cloud](https://share.streamlit.io),
deployed from the same repo with `app.py` as the entry point. Its secrets hold
the `[auth]` blocks plus `DATABASE_URL`, `GOOGLE_API_KEY`, `HORIZON_API_KEY`
and `APP_SHARED_SECRET`, and `redirect_uri` must point at the deployed URL and
be registered with Google.

Two deployment notes worth knowing:

- Streamlit installs with `uv sync` against `uv.lock`, and **`uv sync` installs
  only default groups** — hence `default-groups = ["ui"]` in `pyproject.toml`.
  A `requirements.txt` is silently ignored, because `uv.lock` takes precedence.
- There is deliberately **no `.python-version` file**. Horizon builds with
  `UV_PROJECT_ENVIRONMENT=/usr/local`, a system Python prefix rather than a
  virtualenv; a version pin makes uv reject it, download a managed CPython, and
  fail trying to recreate a non-venv directory. The
  `requires-python = ">=3.10"` floor is sufficient.

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
identity in headers; the server resolves it in `current_user_id()`.
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

`edit_expense` is the one place that has to read before it writes, because a
partial edit cannot be validated otherwise — changing only a subcategory means
checking it against the category already stored. Both statements carry the
`user_id` filter, so the write is scoped on its own rather than trusting the
read that preceded it. It also drops any field already equal to what is
stored, so the result names only what genuinely changed; otherwise editing a
subcategory would report the category as changed too, and the model would
repeat that back to the user.

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

**The checkpointer uses a pool that checks connections before lending them.**
Neon's free tier suspends the compute after inactivity, dropping every open
connection — and the web app caches a runtime for as long as it is up, so a
single held connection eventually goes stale and every later request fails with
`the connection is closed`. It presents as one account being broken, because it
hits whoever has been idle longest. `AsyncConnectionPool` with
`check=check_connection` tests a connection before handing it out and replaces
a dead one. The `check` is the load-bearing part: a pool without it just holds
several stale connections instead of one.

**The client sets a selector event loop on Windows.** psycopg's async mode
refuses to run on `ProactorEventLoop`, the Windows default; asyncio
subprocesses on Windows run *only* on `ProactorEventLoop`. A stdio MCP
connection spawns the server as a subprocess, so a stdio transport and an
async Postgres checkpointer cannot share one loop. The client talks HTTP
instead — which is what the deployed setup needs anyway. Neither restriction
exists on Linux or macOS.

## Not implemented yet

Honest limitations rather than oversights:

- **No currency column.** Every amount is assumed to be in one currency; the
  client is told they are rupees.
- **No long-term memory.** The agent remembers a conversation, not facts across
  conversations. Those are genuinely different features and only the first is
  built.
- **The web app trusts itself.** The server believes an asserted user because
  the caller holds a shared secret. That is the standard backend-for-frontend
  arrangement, and it means the app is a trusted component: anyone who obtained
  both the API key and the secret could impersonate any user. A stricter design
  would have each browser user authenticate to the server directly.
- **Free-tier realities.** The database suspends when idle, so the first
  request after a quiet spell is slow, and one shared API key funds every
  user's model calls.

## Layout

```
main.py                        the server: six tools, one resource
agent.py                       the agent: MCP session, checkpointer, prompt
client.py                      terminal front-end
app.py                         Streamlit front-end, with Google sign-in
schema.sql                     one-time table + index creation
categories.json                the category taxonomy, single source of truth
.env.example                   documents every variable all three need
.streamlit/secrets.toml.example  the shape of the Google sign-in config
```

## Built with

**Server:** [FastMCP 3](https://gofastmcp.com) · [asyncpg](https://github.com/MagicStack/asyncpg) · [Neon Postgres](https://neon.tech) · [Prefect Horizon](https://horizon.prefect.io)

**Client:** [LangGraph](https://langchain-ai.github.io/langgraph/) · [langchain-mcp-adapters](https://github.com/langchain-ai/langchain-mcp-adapters) · [Gemini](https://ai.google.dev) · [Streamlit](https://streamlit.io)
