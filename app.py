"""Streamlit chat UI for the expense tracker agent.

    uv run --group ui streamlit run app.py

The agent, its MCP session and its checkpointer all come from `agent.py`
unchanged -- this file is only a second view onto the same runtime the
terminal client uses.

## The problem this file exists to solve

Streamlit re-executes the entire script on every interaction: every message,
every button, every widget change. That is fine for drawing widgets and fatal
for connections, because an MCP session and a psycopg connection are both
bound to the event loop that created them. Rebuilt per rerun they would be
opened and closed constantly; kept in a plain module global they would be
touched from whichever loop Streamlit happens to be running.

So the async half of the app lives in **one background thread owning one
event loop**, created once and cached by Streamlit across reruns. The script
never awaits anything itself -- it hands coroutines to that loop and waits for
the result. Reruns redraw the page; they cannot disturb the session.

This is the specific failure the earlier version of this project hit: calling
`asyncio.run()` per message inside Streamlit, which creates and destroys a
loop each time and kills the session bound to it.
"""

from __future__ import annotations

import asyncio
import os
import queue
import threading
import uuid
from typing import Any, Iterator

import streamlit as st

from agent import CURRENCY, MODEL, AgentRuntime, use_selector_event_loop

# Locally these come from `.env`; on Streamlit Cloud there is no .env and they
# are supplied through the platform's secrets. `agent.py` reads os.environ so
# that the terminal client and the UI share one way of finding credentials, so
# bridge the platform's secrets into the environment here rather than teaching
# agent.py about Streamlit.
for _key in ("DATABASE_URL", "GOOGLE_API_KEY", "HORIZON_API_KEY", "APP_SHARED_SECRET"):
    if not os.environ.get(_key) and _key in st.secrets:
        os.environ[_key] = str(st.secrets[_key])

# Talk to the deployed server, so there is no second terminal to remember.
# Set to False to use a local `uv run python main.py http` instead.
REMOTE = True

st.set_page_config(page_title="Expense Tracker", page_icon="💸")


# --------------------------------------------------------------------------
# The background event loop
# --------------------------------------------------------------------------


class LoopThread:
    """A dedicated thread running one asyncio loop for the app's lifetime."""

    def __init__(self) -> None:
        # The policy has to be set before the loop is created: psycopg's async
        # mode refuses to run on Windows' default ProactorEventLoop.
        use_selector_event_loop()
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def run(self, coro: Any) -> Any:
        """Run a coroutine on the background loop and wait for its result."""
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result()


@st.cache_resource
def get_loop() -> LoopThread:
    """One event loop for the whole app, shared by every signed-in user."""
    return LoopThread()


@st.cache_resource(show_spinner="Connecting to the expense server...")
def get_runtime(email: str) -> AgentRuntime:
    """Build one agent per signed-in user, and reuse it across reruns.

    `@st.cache_resource` is what makes this work: it caches the *object*, not
    a copy, and survives reruns. Without it every keystroke would open a new
    MCP session and a new database connection.

    Keyed by email, so each user gets a session whose requests carry their own
    identity. That costs one MCP session and one Postgres connection per
    active user, which is fine at this scale and would need pooling at a
    larger one.
    """
    return get_loop().run(AgentRuntime(remote=REMOTE, as_user=email).start())


def stream_turn(
    loop_thread: LoopThread, runtime: AgentRuntime, text: str, thread_id: str
) -> Iterator[dict]:
    """Bridge the agent's async event stream into a normal sync iterator.

    The agent yields events on the background loop; Streamlit renders on the
    main thread. A queue passes them across, so tool calls appear as they
    happen instead of all at once when the turn finishes.
    """
    events: queue.Queue = queue.Queue()
    done = object()

    async def pump() -> None:
        try:
            async for event in runtime.run_turn(text, thread_id):
                events.put(event)
        except Exception as exc:
            events.put({"type": "error", "text": f"{type(exc).__name__}: {exc}"})
        finally:
            events.put(done)

    future = asyncio.run_coroutine_threadsafe(pump(), loop_thread.loop)

    while True:
        event = events.get()
        if event is done:
            break
        yield event

    future.result()  # surface anything the pump swallowed


# --------------------------------------------------------------------------
# UI
# --------------------------------------------------------------------------


def render_trace(container: Any, calls: list[dict]) -> None:
    """Show the tool calls behind an answer, collapsed by default."""
    if not calls:
        return
    label = f"{len(calls)} tool call{'s' if len(calls) > 1 else ''}"
    with container.expander(label, expanded=False):
        for call in calls:
            if call["type"] == "tool_call":
                args = ", ".join(f"{k}={v!r}" for k, v in call["args"].items())
                st.markdown(f"**→ `{call['name']}`**  \n`{args or 'no arguments'}`")
            else:
                st.markdown(f"← {call['summary']}")


# --------------------------------------------------------------------------
# Sign-in gate
# --------------------------------------------------------------------------
#
# Everything below this point requires a signed-in user, because without one
# there is no identity to scope expenses by -- and defaulting to "show me
# something" would mean showing one person's ledger to whoever opened the URL.
if not st.user.is_logged_in:
    st.title("💸 Expense Tracker")
    st.markdown(
        "Track expenses by talking to them. Sign in to keep a private ledger — "
        "your expenses are visible only to you."
    )
    # "google" selects the [auth.google] section of secrets.toml. Calling
    # st.login() with no argument looks for a *default* provider configured
    # directly under [auth], which is not how the secrets file is laid out.
    st.button(
        "Sign in with Google",
        on_click=st.login,
        args=("google",),
        type="primary",
    )
    st.stop()

user_email = (st.user.email or "").strip().lower()
if not user_email:
    st.error("Signed in, but Google did not share an email address.")
    st.stop()

loop_thread = get_loop()
runtime = get_runtime(user_email)

# The thread id lives in the URL, not in st.session_state, because
# session_state is wiped by a browser refresh -- which would send you back to
# the default conversation at exactly the moment persistence is meant to prove
# itself. In the query string it survives refreshes and is shareable.
thread_id = st.query_params.get("thread", "default")

# Reload whenever the thread changes, which covers first load, a refresh, and
# switching conversations, without a separate branch for each.
if st.session_state.get("loaded_thread") != thread_id:
    # Rehydrate from Postgres rather than starting blank. This is the visible
    # payoff of keeping conversation memory in a database.
    st.session_state.messages = loop_thread.run(runtime.history(thread_id))
    st.session_state.loaded_thread = thread_id

with st.sidebar:
    st.subheader("Expense Tracker")
    st.caption("A LangGraph agent talking to a remote MCP server.")

    st.markdown(
        f"**Signed in as**  \n`{runtime.user_id}`\n\n"
        f"**Server**  \n`{runtime.url}`\n\n"
        f"**Model**  \n`{MODEL}`\n\n"
        f"**Tools**  \n" + "  \n".join(f"`{t.name}`" for t in runtime.tools)
    )

    # The server is the authority on who you are, not the browser session. If
    # these disagree, the asserted identity was not accepted and expenses
    # would land in the wrong ledger -- worth failing loudly rather than
    # quietly writing to somebody else's rows.
    if runtime.user_id != user_email:
        st.error(
            f"Signed in as {user_email}, but the server sees "
            f"`{runtime.user_id}` (via {runtime.identified_by}). "
            "Check APP_SHARED_SECRET matches on both sides.",
            icon="🚨",
        )

    st.button("Sign out", on_click=st.logout, use_container_width=True)

    # Every stored conversation, read back from the checkpoint tables --
    # including ones started in the terminal client, since both front-ends
    # write to the same database.
    threads = loop_thread.run(runtime.list_threads())
    if thread_id not in threads:
        threads.insert(0, thread_id)

    choice = st.selectbox("Conversation", threads, index=threads.index(thread_id))
    if choice != thread_id:
        st.query_params["thread"] = choice
        st.rerun()

    if st.button("New conversation", use_container_width=True):
        st.query_params["thread"] = f"thread-{uuid.uuid4().hex[:8]}"
        st.rerun()

    st.caption(
        f"{len(threads)} conversation{'s' if len(threads) != 1 else ''} in Postgres. "
        "Refreshing keeps you in this one."
    )

st.title("💸 Expense Tracker")
st.caption(
    f"Log expenses in plain language. Amounts are in {CURRENCY}. "
    "Try: *log 250 on petrol today*, or *what did I spend on food this month?*"
)

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if message.get("calls"):
            render_trace(st, message["calls"])
        st.markdown(message["text"])

if prompt := st.chat_input("Log an expense, or ask about your spending"):
    st.session_state.messages.append({"role": "user", "text": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        status = st.empty()
        calls: list[dict] = []
        reply = ""

        for event in stream_turn(loop_thread, runtime, prompt, thread_id):
            if event["type"] == "tool_call":
                calls.append(event)
                status.caption(f"calling `{event['name']}`...")
            elif event["type"] == "tool_result":
                calls.append(event)
                status.caption(f"← {event['summary']}")
            elif event["type"] == "error":
                reply = f"Something went wrong: {event['text']}"
            else:
                reply = event["text"]

        status.empty()
        render_trace(st, calls)
        st.markdown(reply or "_(no reply)_")

    st.session_state.messages.append(
        {"role": "assistant", "text": reply, "calls": calls}
    )
