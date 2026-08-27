"""The agent itself, shared by the terminal client and the Streamlit UI.

Neither front-end builds an agent of its own. They both construct an
`AgentRuntime`, which owns the three things that are awkward to set up and
easy to get subtly wrong:

* the MCP session, and the tools loaded from it
* the Postgres checkpointer that holds conversation history
* the model, the system prompt, and the agent loop wired together

`run_turn` yields structured events rather than printing anything, so the
terminal renders them as lines and the browser renders them as chat bubbles
without either duplicating the other's logic.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from contextlib import AsyncExitStack
from datetime import date
from pathlib import Path
from typing import Any, AsyncIterator
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import psycopg
from dotenv import load_dotenv
from langchain.agents import create_agent
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg.rows import dict_row

HERE = Path(__file__).parent
load_dotenv(HERE / ".env")

# Gemini's function-calling schema is a subset of JSON Schema, so the adapter
# logs a warning for every key it drops -- several lines before every model
# call. The dropped keys are `additionalProperties` and `exclusiveMinimum`,
# and neither loss matters: the server validates amounts itself and rejects
# unknown arguments. This is a client-side schema hint being discarded, not
# validation being skipped.
logging.getLogger("langchain_google_genai").setLevel(logging.ERROR)

MODEL = "gemini-3.1-flash-lite-preview"
SERVER_NAME = "expenses"
CURRENCY = "₹"

LOCAL_URL = "http://127.0.0.1:8000/mcp"
REMOTE_URL = "https://given-green-vole.fastmcp.app/mcp"


def use_selector_event_loop() -> None:
    """Required before any async Postgres work on Windows.

    psycopg's async mode refuses to run on `ProactorEventLoop`, the Windows
    default. The catch is that asyncio subprocesses on Windows run *only* on
    ProactorEventLoop -- so a stdio MCP connection, which spawns the server as
    a subprocess, cannot coexist with an async Postgres checkpointer. That is
    why both front-ends talk HTTP to the server instead of spawning it.

    Neither restriction exists on Linux or macOS.
    """
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def connection(remote: bool, as_user: str | None = None) -> dict[str, Any]:
    """MCP connection config for either the local or the deployed server.

    `as_user` is for the web UI, which serves many people through one API key.
    It names whose request this is, and pairs the claim with a shared secret
    the server checks -- without which any caller could name any user. The
    terminal client leaves it None and is identified by its own credential.
    """
    headers: dict[str, str] = {}

    if as_user:
        secret = os.environ.get("APP_SHARED_SECRET")
        if not secret:
            raise RuntimeError(
                "APP_SHARED_SECRET is not set, so the server would ignore the "
                "asserted user and put everyone in one ledger. Refusing to "
                "run rather than silently mixing people's expenses."
            )
        headers["x-app-user"] = as_user.strip().lower()
        headers["x-app-secret"] = secret

    if not remote:
        return {
            "url": LOCAL_URL,
            "transport": "streamable_http",
            **({"headers": headers} if headers else {}),
        }

    key = os.environ.get("HORIZON_API_KEY")
    if not key:
        raise RuntimeError(
            "HORIZON_API_KEY is not set, and the deployed server requires it.\n"
            "Get a key from horizon.prefect.io and add it to .env."
        )
    return {
        "url": REMOTE_URL,
        "transport": "streamable_http",
        "headers": {"Authorization": f"Bearer {key}", **headers},
    }


def checkpointer_dsn() -> str:
    """Adapt DATABASE_URL for psycopg -- the opposite of what asyncpg needs.

    One connection string feeds two drivers that disagree about it:

    * **asyncpg** (the server) rejects libpq query parameters outright, so
      `main.py` strips `sslmode` and passes `ssl="require"` in code.
    * **psycopg** (this checkpointer) is built on libpq and wants exactly
      those parameters, so `sslmode=require` gets added back here.

    Strip it in one place, restore it in the other.
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set -- see .env.example")

    parts = urlparse(dsn.strip())
    params = dict(parse_qsl(parts.query))
    params.setdefault("sslmode", "require")
    return urlunparse(parts._replace(query=urlencode(params)))


def system_prompt() -> str:
    """Build the system prompt, with today's date baked in.

    The date matters more than it looks. `add_expense` deliberately refuses to
    infer "today" -- the server's clock is UTC and would log the wrong day
    either side of midnight for anyone east of Greenwich. The client knows the
    user's local date; the server does not. Without this line "yesterday" is
    unanswerable.
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
# Reading tool results
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
        return (
            f"saved #{e['id']} {e['amount']} "
            f"{e['category']}/{e['subcategory'] or '-'} on {e['date']}"
        )
    if "deleted" in payload:
        e = payload["deleted"]
        return f"deleted #{e['id']} {e['amount']} {e['category']} on {e['date']}"
    if "expenses" in payload:
        return f"{payload.get('count', 0)} rows"
    if "breakdown" in payload:
        return f"total {payload.get('total')} across {payload.get('count')} expenses"
    if "categories" in payload:
        return f"{payload.get('count', 0)} categories"
    return "ok"


# --------------------------------------------------------------------------
# The runtime
# --------------------------------------------------------------------------


class AgentRuntime:
    """An agent, its MCP session and its checkpointer, held open together.

    Deliberately not an `async with` block. Streamlit reruns its script on
    every interaction, so the UI needs to build this once and keep it alive
    across many reruns -- which means explicit `start()` and `aclose()` rather
    than a scope that ends when a function returns. `AsyncExitStack` keeps the
    nested context managers open without nesting `with` statements.
    """

    def __init__(self, remote: bool = True, as_user: str | None = None) -> None:
        self.remote = remote
        self.as_user = as_user
        self.url = REMOTE_URL if remote else LOCAL_URL
        self.agent: Any = None
        self.tools: list = []
        self.user_id = "default"
        self.identified_by = "unknown"
        self._stack = AsyncExitStack()
        self._conn: psycopg.AsyncConnection | None = None

    def thread_key(self, name: str) -> str:
        """Namespace a conversation by its owner.

        Expenses are scoped by the server, but conversation history is stored
        by this client, so it has to scope itself. Without this the sidebar
        would list every conversation in the database regardless of who
        started it -- the expenses would be private and the chat about them
        would not.
        """
        return f"{self.user_id}:{name}"

    async def start(self) -> AgentRuntime:
        self._conn = await psycopg.AsyncConnection.connect(
            checkpointer_dsn(),
            autocommit=True,
            # Neon's pooled endpoint is PgBouncer in transaction mode, handing
            # a different backend to each transaction, so a statement prepared
            # on one connection is missing on the next. Left on, this fails
            # intermittently after appearing to work -- the worst failure mode.
            prepare_threshold=None,
            # The saver reads checkpoint rows as mappings and fails without it.
            row_factory=dict_row,
        )
        checkpointer = AsyncPostgresSaver(self._conn)
        # Idempotent: creates the checkpoint tables on first run, no-ops after.
        # They sit alongside `expenses`, which is untouched.
        await checkpointer.setup()

        client = MultiServerMCPClient(
            {SERVER_NAME: connection(self.remote, self.as_user)}
        )
        session = await self._stack.enter_async_context(client.session(SERVER_NAME))
        self.tools = await load_mcp_tools(session)

        # Ask the server who it thinks we are, rather than assuming. Against
        # the deployed server this returns the authenticated identity; locally
        # it returns "default", and conversations are namespaced accordingly.
        whoami = next((t for t in self.tools if t.name == "whoami"), None)
        if whoami is not None:
            identity = unwrap(await whoami.ainvoke({}))
            self.user_id = identity.get("user_id", "default")
            self.identified_by = identity.get("identified_by", "unknown")

        # The agent loop: call the model, run any tools it asks for, feed the
        # results back, repeat until it answers without a tool call. That loop
        # is what makes "log this and then tell me my total" work in one turn.
        self.agent = create_agent(
            ChatGoogleGenerativeAI(model=MODEL, temperature=0),
            self.tools,
            system_prompt=system_prompt(),
            checkpointer=checkpointer,
        )
        return self

    async def aclose(self) -> None:
        await self._stack.aclose()
        if self._conn is not None:
            await self._conn.close()

    async def run_turn(self, text: str, thread_id: str) -> AsyncIterator[dict]:
        """Stream one turn as structured events.

        Yields `{"type": "tool_call" | "tool_result" | "text", ...}`. Emitting
        events rather than printing is what lets the terminal and the browser
        render the same turn differently without duplicating any logic.
        """
        config = {"configurable": {"thread_id": self.thread_key(thread_id)}}
        async for chunk in self.agent.astream(
            {"messages": [("user", text)]}, config=config, stream_mode="updates"
        ):
            for update in chunk.values():
                if not isinstance(update, dict):
                    continue
                for message in update.get("messages", []):
                    if isinstance(message, AIMessage):
                        for call in message.tool_calls or []:
                            yield {
                                "type": "tool_call",
                                "name": call["name"],
                                "args": call["args"],
                            }
                        if body := text_of(message):
                            yield {"type": "text", "text": body}
                    elif isinstance(message, ToolMessage):
                        payload = unwrap(message)
                        yield {
                            "type": "tool_result",
                            "name": message.name,
                            "summary": brief(payload),
                            "payload": payload,
                        }

    async def list_threads(self) -> list[str]:
        """Every conversation the checkpointer holds, newest first.

        Read straight from the checkpoint tables rather than kept in the UI,
        so the list is whatever is actually stored -- including conversations
        started from the terminal client.
        """
        if self._conn is None:
            return []
        prefix = f"{self.user_id}:"
        cursor = await self._conn.execute(
            "SELECT thread_id, max(checkpoint_id::text) AS newest "
            "FROM checkpoints WHERE thread_id LIKE %s "
            "GROUP BY thread_id ORDER BY newest DESC",
            (prefix + "%",),
        )
        return [
            row["thread_id"][len(prefix) :] for row in await cursor.fetchall()
        ]

    async def history(self, thread_id: str) -> list[dict]:
        """Replay a stored conversation from the checkpointer.

        This is the payoff of keeping memory in Postgres rather than in the
        process: a browser refresh, or a restart, can rebuild the visible
        conversation instead of starting blank.
        """
        config = {"configurable": {"thread_id": self.thread_key(thread_id)}}
        snapshot = await self.agent.aget_state(config)
        if not snapshot or not snapshot.values:
            return []

        turns: list[dict] = []
        for message in snapshot.values.get("messages", []):
            if isinstance(message, HumanMessage):
                turns.append({"role": "user", "text": text_of(message)})
            elif isinstance(message, AIMessage):
                if body := text_of(message):
                    turns.append({"role": "assistant", "text": body})
        return turns
