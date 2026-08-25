"""Terminal front-end for the expense tracker agent.

Everything interesting lives in `agent.py`; this file is the terminal's view
of it -- read a line, run a turn, print the events as they arrive.

    uv run --group client python client.py            # local server
    uv run --group client python client.py --remote   # deployed server

The local target needs the server running in another terminal:

    uv run python main.py http

One structural detail worth keeping: everything runs inside a single
`asyncio.run()`, with the MCP session held open for the life of the REPL. An
MCP session is bound to the event loop that created it, so calling
`asyncio.run()` per message -- the obvious way to bolt async onto a REPL --
destroys the loop the session lives on and the second message fails with
`Event loop is closed`.
"""

from __future__ import annotations

import asyncio
import sys
import uuid

from agent import MODEL, AgentRuntime, use_selector_event_loop

use_selector_event_loop()


async def run_turn(runtime: AgentRuntime, text: str, thread_id: str) -> None:
    """Print one turn's events as they happen.

    The trace is the point of a terminal client: you can watch the agent make
    two calls in a row, which is exactly what a single-round loop cannot do.
    """
    reply = ""
    async for event in runtime.run_turn(text, thread_id):
        if event["type"] == "tool_call":
            args = ", ".join(f"{k}={v!r}" for k, v in event["args"].items())
            print(f"  → {event['name']}({args})")
        elif event["type"] == "tool_result":
            print(f"  ← {event['summary']}")
        else:
            reply = event["text"]

    print(f"\n{reply.strip()}\n" if reply.strip() else "\n(no reply)\n")


async def repl(runtime: AgentRuntime) -> None:
    # A fixed default thread means restarting the client resumes the same
    # conversation, because the history lives in Postgres rather than here.
    thread_id = "default"
    print(f"model: {MODEL}   tools: {', '.join(t.name for t in runtime.tools)}")
    print(f"user:  {runtime.email or runtime.user_id}")
    print(f"memory: Neon Postgres, thread '{thread_id}'")
    print("commands: /new (fresh conversation)  /quit\n")

    while True:
        try:
            # input() blocks; run it off the event loop so nothing else stalls.
            text = (await asyncio.to_thread(input, "you> ")).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return

        if not text:
            continue
        if text in ("/quit", "/exit"):
            return
        if text == "/new":
            thread_id = f"thread-{uuid.uuid4().hex[:8]}"
            print(f"started a new conversation ({thread_id})\n")
            continue

        try:
            await run_turn(runtime, text, thread_id)
        except Exception as exc:  # keep the REPL alive through model errors
            print(f"\n  !! {type(exc).__name__}: {exc}\n")


async def main(remote: bool) -> None:
    runtime = AgentRuntime(remote=remote)
    print(f"server: {runtime.url}" + ("  (deployed)" if remote else "  (local)"))

    try:
        await runtime.start()
    except Exception as exc:
        hint = (
            "check HORIZON_API_KEY in .env -- a 401 means the key is wrong or "
            "the header format is not what Horizon expects"
            if remote
            else "start the server in another terminal first:\n"
            "  uv run python main.py http"
        )
        print(f"could not start: {type(exc).__name__}: {exc}\n\n{hint}", file=sys.stderr)
        return

    try:
        await repl(runtime)
    finally:
        await runtime.aclose()

    print("session closed cleanly")


if __name__ == "__main__":
    try:
        asyncio.run(main(remote="--remote" in sys.argv))
    except KeyboardInterrupt:
        sys.exit(130)
