"""Terminal client for the expense tracker MCP server.

A LangGraph ReAct agent that talks to the MCP server over stdio and lets you
log and query expenses in plain language.

Two structural decisions, both reactions to how the tutorial version failed:

* **One event loop, one MCP session, for the whole process.** The tutorial
  called `asyncio.run()` once per message. An MCP session is bound to the loop
  it was created on, so the second message died with `Event loop is closed`.
  Here everything lives inside a single `asyncio.run(main())` and the session
  is held open for the life of the REPL.
* **A real agent loop, not one request per message.** "Log 200 on petrol and
  tell me my food total" needs two sequential tool calls. `create_agent` keeps
  calling tools until the model stops asking for them; the tutorial's
  single-round loop would answer half the question.

Run it:  uv run --group client python client.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import uuid
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import psycopg
from dotenv import load_dotenv
from langchain.agents import create_agent
from langchain_core.messages import AIMessage, ToolMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg.rows import dict_row

# Windows ships two incompatible event loops and this project needs both
# halves of the incompatibility:
#
#   * psycopg's async mode refuses to run on ProactorEventLoop, which is the
#     Windows default, and demands the selector loop.
#   * asyncio subprocesses on Windows only work on ProactorEventLoop.
#
# So a stdio MCP connection (which spawns the server as a subprocess) and an
# async Postgres checkpointer cannot share one event loop here. The resolution
# is to stop spawning the server and talk HTTP to it instead -- see
# LOCAL_HTTP below. On Linux and macOS neither restriction exists.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

HERE = Path(__file__).parent
load_dotenv(HERE / ".env")

# Gemini's function-calling schema is a subset of JSON Schema, so the adapter
# logs a warning for every key it drops -- four lines before every model call.
# The dropped keys are `additionalProperties` and `exclusiveMinimum`, and
# neither loss matters: the server validates amounts itself and rejects
# unknown arguments. Worth being explicit that this is a *client-side* schema
# hint being discarded, not validation being skipped.
logging.getLogger("langchain_google_genai").setLevel(logging.ERROR)

MODEL = "gemini-3.1-flash-lite-preview"
SERVER_NAME = "expenses"
CURRENCY = "₹"

# Two places the same server runs. Local is the default because the agent loop
# was built against it: debugging an agent and an auth handshake at the same
# time is twice the work. `--remote` points at the deployed copy instead.
#
# Note what does *not* change between them -- the tools, the agent, the
# prompt, the checkpointer. Swapping a local server for a deployed one is a
# change of URL and one header. That is the whole argument for MCP.
#
# Stdio would avoid needing a second terminal for the local server, but see
# the event loop note above: on Windows it cannot coexist with the async
# Postgres checkpointer.
LOCAL_URL = "http://127.0.0.1:8000/mcp"
REMOTE_URL = "https://given-green-vole.fastmcp.app/mcp"


def connection(remote: bool) -> dict[str, Any]:
    """Build the MCP connection config for either target."""
    if not remote:
        return {"url": LOCAL_URL, "transport": "streamable_http"}

    key = os.environ.get("HORIZON_API_KEY")
    if not key:
        raise RuntimeError(
            "HORIZON_API_KEY is not set, and the deployed server requires it.\n"
            "Get a key from horizon.prefect.io and add it to .env."
        )
    return {
        "url": REMOTE_URL,
        "transport": "streamable_http",
        "headers": {"Authorization": f"Bearer {key}"},
    }


def checkpointer_dsn() -> str:
    """Adapt DATABASE_URL for psycopg -- the opposite of what asyncpg needs.

    The same connection string feeds two different drivers, and they disagree:

    * **asyncpg** (the server) rejects libpq query parameters outright, so
      `main.py` strips `sslmode` and passes `ssl="require"` in code.
    * **psycopg** (this checkpointer) is built on libpq and wants exactly
      those parameters, so `sslmode=require` gets added back here.

    Strip it in one place, restore it in the other. Sharing one env var across
    two drivers is convenient, but they do not share a DSN dialect.
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set -- see .env.example")

    parts = urlparse(dsn.strip())
    params = dict(parse_qsl(parts.query))
    params.setdefault("sslmode", "require")
    return urlunparse(parts._replace(query=urlencode(params)))


async def open_checkpointer() -> tuple[AsyncPostgresSaver, psycopg.AsyncConnection]:
    """Connect to Neon and return a checkpointer sharing that connection.

    Two connection options are load-bearing, and both are easy to omit:

    * `row_factory=dict_row` -- the saver reads checkpoint rows as mappings
      and fails without it.
    * `prepare_threshold=None` -- disables prepared statements. Neon's pooled
      endpoint is PgBouncer in transaction mode, which hands a different
      backend to each transaction, so a statement prepared on one connection
      is missing on the next. Left on, this fails intermittently under reuse
      rather than immediately, which is the worst way for it to fail.

    `from_conn_string()` would be shorter, but it does not let either option
    through.
    """
    conn = await psycopg.AsyncConnection.connect(
        checkpointer_dsn(),
        autocommit=True,
        prepare_threshold=None,
        row_factory=dict_row,
    )
    saver = AsyncPostgresSaver(conn)
    # Idempotent: creates the checkpoint tables on first run, no-ops after.
    # They sit alongside `expenses`, which is untouched.
    await saver.setup()
    return saver, conn


def system_prompt() -> str:
    """Build the system prompt, with today's date baked in.

    The date matters more than it looks. `add_expense` deliberately refuses to
    infer "today" -- the server's clock is UTC and would log the wrong day
    either side of midnight for anyone east of Greenwich. So the client, which
    does know the user's local date, has to supply it. Without this line
    "yesterday" is unanswerable.
    """
    return (
        "You are an expense tracking assistant with access to a real database.\n\n"
        f"Today's date is {date.today().isoformat()}. Use it to resolve relative "
        "dates like 'today', 'yesterday' or 'last Friday' into YYYY-MM-DD before "
        "calling any tool.\n\n"
        "Only pass a date range when the user asks for one. 'What have I "
        "spent?' means everything on record, so call summarize with no "
        "filters -- do not invent a start date.\n\n"
        "Categories are a fixed taxonomy. If you are unsure which category "
        "something belongs to, call list_categories rather than guessing. If a "
        "tool rejects a category it returns the valid values -- correct yourself "
        "and retry instead of asking the user.\n\n"
        "Amounts come back as decimal strings to preserve exact cents. Never do "
        f"arithmetic on them yourself; call summarize and report what it returns. "
        f"All amounts are in Indian rupees -- write them as {CURRENCY}250.00, "
        "never as dollars.\n\n"
        "Be concise. Confirm what you logged, including the date and category."
    )


# --------------------------------------------------------------------------
# Result formatting
# --------------------------------------------------------------------------


def unwrap(result: Any) -> dict:
    """Pull the payload out of an MCP tool result.

    A converted tool returns MCP content blocks rather than the dict the tool
    returned: `[{"type": "text", "text": "<json>"}]`. When the agent invokes a
    tool it gets a ToolMessage instead, whose `artifact["structured_content"]`
    already holds the parsed dict. Both paths are handled here.
    """
    artifact = getattr(result, "artifact", None)
    if isinstance(artifact, dict) and "structured_content" in artifact:
        return artifact["structured_content"]
    if hasattr(result, "content"):
        return unwrap(result.content)

    if isinstance(result, str):
        try:
            return json.loads(result)
        except json.JSONDecodeError:
            return {"raw": result}
    if isinstance(result, dict):
        if result.get("type") == "text" and "text" in result:
            return unwrap(result["text"])
        return result
    if isinstance(result, (list, tuple)) and result:
        return unwrap(result[0])
    return {}


def text_of(message: Any) -> str:
    """Flatten message content, which may be a string or a list of blocks."""
    content = getattr(message, "content", message)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = [
            block.get("text", "")
            for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        ]
        return "".join(parts)
    return str(content)


def brief(payload: dict) -> str:
    """One-line summary of a tool result, for the trace."""
    if not payload.get("ok", True):
        return f"rejected: {payload.get('error', '?')}"
    if "expense" in payload:
        e = payload["expense"]
        return f"saved #{e['id']} {e['amount']} {e['category']}/{e['subcategory'] or '-'} on {e['date']}"
    if "expenses" in payload:
        return f"{payload.get('count', 0)} rows"
    if "breakdown" in payload:
        return f"total {payload.get('total')} across {payload.get('count')} expenses"
    if "categories" in payload:
        return f"{payload.get('count', 0)} categories"
    return "ok"


# --------------------------------------------------------------------------
# REPL
# --------------------------------------------------------------------------


async def run_turn(agent: Any, config: dict, user_text: str) -> None:
    """Stream one turn, showing each tool call as it happens.

    The trace is the point of a terminal client: you can see the agent make
    two calls in a row, which is exactly the behaviour a single-round loop
    cannot produce.
    """
    reply = ""
    async for chunk in agent.astream(
        {"messages": [("user", user_text)]}, config=config, stream_mode="updates"
    ):
        for update in chunk.values():
            if not isinstance(update, dict):
                continue
            for message in update.get("messages", []):
                if isinstance(message, AIMessage):
                    for call in message.tool_calls or []:
                        args = ", ".join(f"{k}={v!r}" for k, v in call["args"].items())
                        print(f"  → {call['name']}({args})")
                    if text := text_of(message):
                        reply = text
                elif isinstance(message, ToolMessage):
                    print(f"  ← {brief(unwrap(message))}")

    print(f"\n{reply.strip()}\n" if reply.strip() else "\n(no reply)\n")


async def repl(agent: Any, tools: list) -> None:
    # A fixed default thread means restarting the client resumes the same
    # conversation, because the history lives in Postgres rather than here.
    thread_id = "default"
    print(f"model: {MODEL}   tools: {', '.join(t.name for t in tools)}")
    print(f"memory: Neon Postgres, thread '{thread_id}'")
    print("commands: /new (fresh conversation)  /quit\n")

    while True:
        try:
            # input() blocks; run it off the event loop so nothing else stalls.
            user_text = (await asyncio.to_thread(input, "you> ")).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return

        if not user_text:
            continue
        if user_text in ("/quit", "/exit"):
            return
        if user_text == "/new":
            thread_id = f"thread-{uuid.uuid4().hex[:8]}"
            print(f"started a new conversation ({thread_id})\n")
            continue

        try:
            await run_turn(agent, {"configurable": {"thread_id": thread_id}}, user_text)
        except Exception as exc:  # keep the REPL alive through model errors
            print(f"\n  !! {type(exc).__name__}: {exc}\n")


async def main(remote: bool) -> None:
    config = connection(remote)
    client = MultiServerMCPClient({SERVER_NAME: config})
    checkpointer, conn = await open_checkpointer()
    print(f"server: {config['url']}" + ("  (deployed)" if remote else "  (local)"))

    try:
        # One session, held open for the whole REPL.
        async with client.session(SERVER_NAME) as session:
            try:
                tools = await load_mcp_tools(session)
            except Exception as exc:
                hint = (
                    "check HORIZON_API_KEY in .env -- a 401 means the key is "
                    "wrong or the header format is not what Horizon expects"
                    if remote
                    else "start it in another terminal first:\n"
                    "  uv run python main.py http"
                )
                print(
                    f"could not reach the MCP server at {config['url']}\n"
                    f"  {type(exc).__name__}: {exc}\n\n{hint}",
                    file=sys.stderr,
                )
                return

            # The ReAct loop: call the model, run any tools it asks for, feed
            # the results back, repeat until it answers without a tool call.
            # That loop is what makes "log this and then tell me my total"
            # work in one turn.
            agent = create_agent(
                ChatGoogleGenerativeAI(model=MODEL, temperature=0),
                tools,
                system_prompt=system_prompt(),
                checkpointer=checkpointer,
            )

            await repl(agent, tools)
    finally:
        await conn.close()

    print("session closed cleanly")


if __name__ == "__main__":
    try:
        asyncio.run(main(remote="--remote" in sys.argv))
    except KeyboardInterrupt:
        sys.exit(130)
