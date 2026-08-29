"""Remote MCP server for expense tracking, backed by Neon Postgres.

Deployed on Prefect Horizon with entrypoint `main.py:mcp`, and consumed by
both Claude (as a connector) and a custom LangGraph client.

Design notes worth knowing before editing this file:

* The connection pool is created lazily, on the first tool call that needs
  it -- never at import time. Connecting at import turns a transient database
  problem into a *deploy* failure; connecting lazily turns it into one failed
  tool call that the caller can retry.
* Every tool returns a dict, on both success and failure, and every dict has
  an "ok" key. A tool that returns a list on success and a dict on error
  forces every caller to type-check the result before using it.
* Money is `Decimal` in Python and `NUMERIC` in Postgres, and crosses the
  wire as a *string*. JSON numbers are IEEE-754 doubles, so serialising a
  Decimal as a float would reintroduce, at the very last step, exactly the
  drift the NUMERIC column exists to prevent.
* Categories are validated against `categories.json` before any write. An
  unvalidated category means the model can invent "foood" and silently
  fragment every future summary.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import sys
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from datetime import date as Date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

import asyncpg
from dotenv import load_dotenv
from fastmcp import FastMCP
from fastmcp.server.dependencies import get_http_headers
from pydantic import Field

# stderr, never stdout: over the stdio transport, stdout *is* the JSON-RPC
# channel, and a stray print() corrupts the protocol stream.
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
log = logging.getLogger("expense-tracker")

load_dotenv()

HERE = Path(__file__).parent

# Reading a local JSON file at import is fine -- it cannot fail transiently
# and it cannot hang. Opening a network connection at import is what we avoid.
CATEGORIES: dict[str, list[str]] = json.loads(
    (HERE / "categories.json").read_text(encoding="utf-8")
)

# Used only when no authenticated identity reaches the server -- i.e. running
# locally with no gateway in front. Deployed, every request carries a real one.
#
# `user_id` is deliberately NOT a tool parameter: if the model could choose it,
# any client could read anyone's expenses just by asking.
DEFAULT_USER_ID = "default"

# Prefect Horizon terminates authentication at its edge and forwards the
# authenticated identity in headers. This one carries a stable UUID; there is
# also `horizon-user-email`, but emails change and user ids do not.
#
# Trusting a header is only sound if clients cannot forge it. Verified by
# sending `horizon-user-id: 00000000-dead-beef-...` from a client: the gateway
# overwrote it and the server still saw the real subject. Were that not true,
# this would be an assertion rather than authentication, and unusable as a
# security boundary.
IDENTITY_HEADER = "horizon-user-id"
EMAIL_HEADER = "horizon-user-email"

# Identity is keyed on **email**, not on the Horizon UUID, because two
# different doors lead to the same person: connecting through Claude gives a
# Horizon UUID, while signing into the web UI with Google gives a Google
# subject. Keyed on ids, one human would own two unrelated ledgers. Email is
# the one identifier both doors produce.
#
# The cost, worth being able to state: emails are mutable, so changing yours
# orphans your history. Fine here; not fine at a bank, where you would keep an
# immutable id and a separate email column.

# --- the trusted-app path ------------------------------------------------
#
# The web UI is one program serving many people, so it authenticates once
# (its own API key) and then has to say *which* of its users each request is
# for. `APP_USER_HEADER` carries that claim.
#
# A claim is only worth as much as the proof behind it. Without
# `APP_SECRET_HEADER` matching the shared secret, anyone holding an API key
# could name any user and read their expenses. With it, the server can tell
# "my web app said this" from "some caller said this".
#
# If the secret is unset, the whole path is disabled rather than open.
APP_USER_HEADER = "x-app-user"
APP_SECRET_HEADER = "x-app-secret"
APP_SHARED_SECRET = os.environ.get("APP_SHARED_SECRET", "")

MAX_LIMIT = 500

@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[None]:
    """Close the pool on shutdown.

    Deliberately asymmetric: nothing is opened here. Opening the pool at
    startup would make an unreachable database a failed *deploy* rather than
    a failed tool call. Closing on the way out is free and returns
    connections to Neon's pooler promptly instead of waiting for them to
    time out.
    """
    yield
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        log.info("connection pool closed")


mcp = FastMCP(
    name="expense-tracker",
    lifespan=lifespan,
    instructions=(
        "Tracks personal expenses in a Postgres database.\n\n"
        "Categories are a fixed taxonomy. Call `list_categories` to see valid "
        "category/subcategory pairs before logging an expense; do not invent "
        "new ones. If a tool rejects a category it returns the valid values "
        "in the error, so you can correct and retry without asking the user.\n\n"
        "Amounts are returned as decimal strings, not numbers, to preserve "
        "exact cents. `add_expense` needs the date the money was spent in "
        "YYYY-MM-DD form -- pass the user's local date rather than assuming "
        "the server's.\n\n"
        "`delete_expense` is permanent. Find the id with `list_expenses` and "
        "confirm which row the user means before deleting; never guess an id "
        "from a description."
    ),
)


# --------------------------------------------------------------------------
# Database
# --------------------------------------------------------------------------

_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()


def _clean_dsn(dsn: str) -> str:
    """Drop libpq-only query parameters that asyncpg rejects.

    Neon hands you `...?sslmode=require&channel_binding=require`. asyncpg does
    not accept those as DSN options and raises
    `invalid dsn: invalid connection option "sslmode"`. TLS is requested via
    the explicit `ssl=` argument to create_pool() instead.

    `.env` already has them stripped; this exists so that pasting the raw
    console string into a Horizon environment variable still works.
    """
    parts = urlparse(dsn.strip())
    if not parts.query:
        return dsn.strip()
    kept = [(k, v) for k, v in parse_qsl(parts.query) if k not in ("sslmode", "channel_binding")]
    return urlunparse(parts._replace(query=urlencode(kept)))


async def get_pool() -> asyncpg.Pool:
    """Return the shared pool, creating it on first use.

    Double-checked under a lock so that concurrent first calls open one pool
    rather than several.
    """
    global _pool
    if _pool is not None:
        return _pool

    async with _pool_lock:
        if _pool is None:
            dsn = os.environ.get("DATABASE_URL")
            if not dsn:
                raise RuntimeError(
                    "DATABASE_URL is not set. Locally, copy .env.example to .env "
                    "and fill it in; on Horizon, set it as an environment variable."
                )
            _pool = await asyncpg.create_pool(
                _clean_dsn(dsn),
                ssl="require",
                min_size=1,
                # Horizon runs short-lived containers against Neon's pooler;
                # a small ceiling per container keeps the pooler's own limit
                # from being consumed by one instance.
                max_size=5,
                command_timeout=30,
            )
            log.info("connection pool created")
    return _pool


# --------------------------------------------------------------------------
# Validation and serialisation helpers
# --------------------------------------------------------------------------


def _validate_category(category: str, subcategory: str) -> tuple[str, str, dict | None]:
    """Normalise and check a category/subcategory pair.

    Returns `(category, subcategory, error)`. When `error` is not None it is a
    ready-to-return payload that names the valid values, so the model can
    correct itself in one round trip instead of guessing again.
    """
    cat = category.strip().lower()
    sub = subcategory.strip().lower()

    if cat not in CATEGORIES:
        return cat, sub, {
            "ok": False,
            "error": f"Unknown category {category!r}.",
            "valid_categories": sorted(CATEGORIES),
        }

    if sub and sub not in CATEGORIES[cat]:
        return cat, sub, {
            "ok": False,
            "error": f"Unknown subcategory {subcategory!r} for category {cat!r}.",
            "valid_subcategories": CATEGORIES[cat],
        }

    return cat, sub, None


def _money(value: Decimal) -> str:
    """Serialise money as a fixed 2-decimal string.

    A string, not a float: JSON numbers are doubles and 0.1 has no exact
    double representation, so a client summing floats drifts by cents.
    """
    return str(value.quantize(Decimal("0.01")))


def _row_to_expense(row: asyncpg.Record) -> dict[str, Any]:
    return {
        "id": row["id"],
        "date": row["date"].isoformat(),
        "amount": _money(row["amount"]),
        "category": row["category"],
        "subcategory": row["subcategory"],
        "note": row["note"],
    }


def _date_filters(
    start_date: Date | None, end_date: Date | None, category: str | None
) -> tuple[list[str], list[Any]]:
    """Build the shared WHERE clause for the read tools.

    Parameters are numbered ($1, $2, ...) and passed separately -- never
    interpolated into the SQL string.
    """
    # Every read starts here, and every read is scoped. There is no code path
    # that queries expenses without a user_id predicate, which is the property
    # that makes multi-tenancy hold: forgetting the filter is not possible in
    # one tool because no tool builds its own WHERE clause.
    clauses = ["user_id = $1"]
    args: list[Any] = [current_user_id()]

    if start_date is not None:
        args.append(start_date)
        clauses.append(f"date >= ${len(args)}")
    if end_date is not None:
        args.append(end_date)
        clauses.append(f"date <= ${len(args)}")
    if category:
        args.append(category.strip().lower())
        clauses.append(f"category = ${len(args)}")

    return clauses, args


# --------------------------------------------------------------------------
# Tools
# --------------------------------------------------------------------------


def _header(name: str) -> str | None:
    """Case-insensitive lookup of one request header, if there is a request."""
    wanted = name.lower()
    for key, value in get_http_headers().items():
        if key.lower() == wanted and value.strip():
            return value.strip()
    return None


def identity() -> tuple[str, str]:
    """Whose expenses this request may touch, and how we know.

    Three cases, in order of precedence:

    1. **A trusted app acting for one of its users.** Accepted only when the
       shared secret matches, so an arbitrary caller cannot name a user.
    2. **A person authenticated by the gateway**, connecting with their own
       account -- a Claude connector, or their own API key.
    3. **Nobody** -- no gateway in front, which means running locally. Single
       user by definition, and never a security decision, because a local
       server has no other tenants to leak to.
    """
    asserted = _header(APP_USER_HEADER)
    if asserted and APP_SHARED_SECRET:
        offered = _header(APP_SECRET_HEADER) or ""
        # compare_digest, not ==, so a wrong secret cannot be discovered one
        # character at a time by timing the response.
        if secrets.compare_digest(offered, APP_SHARED_SECRET):
            return asserted.strip().lower(), "trusted app"

    if email := _header(EMAIL_HEADER):
        return email.strip().lower(), "gateway"

    return DEFAULT_USER_ID, "unauthenticated"


def current_user_id() -> str:
    """The identity every read and write is scoped to."""
    return identity()[0]


@mcp.tool
def whoami() -> dict[str, Any]:
    """Report which user the server sees, and whether expenses are scoped.

    Worth having permanently rather than as a one-off diagnostic: "why can't I
    see my expenses?" is answered by this tool in one call, and the answer is
    almost always that the request arrived unauthenticated and landed in the
    shared local bucket.
    """
    user_id, source = identity()
    scoped = user_id != DEFAULT_USER_ID

    # Enough to diagnose a rejected assertion without revealing anything: does
    # the server have a secret at all, did the app headers arrive, and did the
    # secret match? A "no" to the second means the gateway dropped them; a
    # "no" to the first or third is a configuration mismatch.
    offered = _header(APP_SECRET_HEADER)
    assertion = {
        "server_has_secret": bool(APP_SHARED_SECRET),
        "app_user_header_arrived": _header(APP_USER_HEADER) is not None,
        "app_secret_header_arrived": offered is not None,
        "secret_matched": bool(
            offered and APP_SHARED_SECRET
            and secrets.compare_digest(offered, APP_SHARED_SECRET)
        ),
    }

    return {
        "ok": True,
        "user_id": user_id,
        "identified_by": source,
        "scoped": scoped,
        "assertion": assertion,
        "note": (
            "Expenses are scoped to this user; nobody else can read them."
            if scoped
            else "No authenticated identity in this request, so expenses go to "
            "the shared local bucket. Expected when running the server "
            "locally; unexpected on the deployed server."
        ),
    }


@mcp.tool
def list_categories() -> dict[str, Any]:
    """List every valid category and its subcategories.

    Call this before logging an expense if you are unsure which category a
    purchase belongs to. Categories are a fixed taxonomy; anything outside it
    is rejected.
    """
    return {
        "ok": True,
        "categories": CATEGORIES,
        "count": len(CATEGORIES),
    }


@mcp.tool
async def add_expense(
    date: Annotated[Date, Field(description="Date the money was spent, YYYY-MM-DD.")],
    amount: Annotated[
        Decimal, Field(gt=0, description="Amount spent. Must be greater than zero.")
    ],
    category: Annotated[
        str, Field(description="Top-level category, e.g. 'food'. Must be a known category.")
    ],
    subcategory: Annotated[
        str, Field(description="Optional subcategory, e.g. 'groceries'.")
    ] = "",
    note: Annotated[str, Field(description="Optional free-text note.")] = "",
) -> dict[str, Any]:
    """Record a single expense.

    Pass the user's local date -- the server does not infer 'today', because
    its clock is UTC and would log the wrong day either side of midnight.
    """
    cat, sub, error = _validate_category(category, subcategory)
    if error:
        return error

    try:
        amt = Decimal(amount).quantize(Decimal("0.01"))
    except (InvalidOperation, TypeError):
        return {"ok": False, "error": f"Could not read {amount!r} as an amount."}

    if amt <= 0:
        return {"ok": False, "error": "Amount must be greater than zero."}

    user_id = current_user_id()

    pool = await get_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO expenses (user_id, date, amount, category, subcategory, note)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, date, amount, category, subcategory, note
        """,
        user_id,
        date,
        amt,
        cat,
        sub,
        note.strip(),
    )

    log.info("added expense id=%s user=%s %s %s", row["id"], user_id, cat, amt)
    return {"ok": True, "expense": _row_to_expense(row)}


@mcp.tool
async def list_expenses(
    start_date: Annotated[
        Date | None, Field(description="Earliest date to include, inclusive, YYYY-MM-DD.")
    ] = None,
    end_date: Annotated[
        Date | None, Field(description="Latest date to include, inclusive, YYYY-MM-DD.")
    ] = None,
    category: Annotated[
        str | None, Field(description="Only return expenses in this category.")
    ] = None,
    limit: Annotated[
        int, Field(ge=1, le=MAX_LIMIT, description="Maximum rows to return.")
    ] = 50,
) -> dict[str, Any]:
    """List individual expenses, newest first.

    All filters are optional; with none set this returns the most recent
    expenses. Use `summarize` instead when you want totals rather than rows.
    """
    if category:
        cat, _, error = _validate_category(category, "")
        if error:
            return error
        category = cat

    if start_date and end_date and start_date > end_date:
        return {"ok": False, "error": "start_date is after end_date."}

    clauses, args = _date_filters(start_date, end_date, category)
    args.append(min(limit, MAX_LIMIT))

    rows = await (await get_pool()).fetch(
        f"""
        SELECT id, date, amount, category, subcategory, note
        FROM expenses
        WHERE {' AND '.join(clauses)}
        ORDER BY date DESC, id DESC
        LIMIT ${len(args)}
        """,
        *args,
    )

    return {
        "ok": True,
        "count": len(rows),
        "expenses": [_row_to_expense(r) for r in rows],
    }


@mcp.tool(
    # Tells a client this tool destroys data, so it can ask the user before
    # running it. Idempotent because deleting the same id twice leaves the
    # same state -- the second call simply reports nothing to delete.
    annotations={"destructiveHint": True, "idempotentHint": True},
)
async def delete_expense(
    expense_id: Annotated[
        int, Field(gt=0, description="id of the expense to delete, from list_expenses.")
    ],
) -> dict[str, Any]:
    """Delete one expense, by id.

    Use `list_expenses` first to find the id, and confirm with the user which
    one they mean before deleting -- ids are not guessable from a description
    and deleting the wrong row cannot be undone.
    """
    user_id = current_user_id()

    # `AND user_id = $2` is the whole security of this tool. Without it, any
    # user could delete any row by guessing an id -- ids are sequential, so
    # guessing is trivial. The scoping is in the WHERE clause rather than in a
    # separate ownership check because a check-then-delete is two statements
    # that can disagree; this is one.
    row = await (await get_pool()).fetchrow(
        """
        DELETE FROM expenses
        WHERE id = $1 AND user_id = $2
        RETURNING id, date, amount, category, subcategory, note
        """,
        expense_id,
        user_id,
    )

    if row is None:
        # Deliberately the same answer whether the id never existed or belongs
        # to somebody else. Saying "that expense is not yours" would confirm
        # the id exists, which is a small leak but a free one to avoid.
        return {
            "ok": False,
            "error": f"No expense #{expense_id} in your records.",
        }

    log.info("deleted expense id=%s user=%s", row["id"], user_id)
    return {"ok": True, "deleted": _row_to_expense(row)}


@mcp.tool
async def summarize(
    start_date: Annotated[
        Date | None, Field(description="Earliest date to include, inclusive, YYYY-MM-DD.")
    ] = None,
    end_date: Annotated[
        Date | None, Field(description="Latest date to include, inclusive, YYYY-MM-DD.")
    ] = None,
    category: Annotated[
        str | None,
        Field(
            description=(
                "Restrict to one category. When set, the breakdown is by "
                "subcategory instead of by category."
            )
        ),
    ] = None,
) -> dict[str, Any]:
    """Total spending over a date range, broken down by category.

    Summing happens in Postgres over NUMERIC values, so the total is exact.
    Prefer this over listing every row and adding them up.
    """
    if category:
        cat, _, error = _validate_category(category, "")
        if error:
            return error
        category = cat

    if start_date and end_date and start_date > end_date:
        return {"ok": False, "error": "start_date is after end_date."}

    clauses, args = _date_filters(start_date, end_date, category)

    # Drilling into one category is more useful broken down by subcategory;
    # otherwise the single row would just repeat the grand total.
    group_col = "subcategory" if category else "category"

    rows = await (await get_pool()).fetch(
        f"""
        SELECT {group_col} AS bucket, SUM(amount) AS total, COUNT(*) AS n
        FROM expenses
        WHERE {' AND '.join(clauses)}
        GROUP BY {group_col}
        ORDER BY total DESC
        """,
        *args,
    )

    total = sum((r["total"] for r in rows), Decimal("0"))

    return {
        "ok": True,
        "grouped_by": group_col,
        "total": _money(total),
        "count": sum(r["n"] for r in rows),
        "breakdown": [
            {
                "bucket": r["bucket"] or "(none)",
                "total": _money(r["total"]),
                "count": r["n"],
            }
            for r in rows
        ],
    }


# --------------------------------------------------------------------------
# Resources
# --------------------------------------------------------------------------


@mcp.resource("expenses://categories", mime_type="application/json")
def categories() -> dict[str, list[str]]:
    """The full category taxonomy: every category and its subcategories.

    Deliberately duplicates the `list_categories` tool. Resources are the
    correct MCP primitive for read-only reference data, but a client only
    reads one when a *user* attaches it -- models are handed tools, not
    resources. Testing against Claude showed exactly that: it reported the
    taxonomy as unavailable and proposed writing a junk row to discover the
    valid values from the rejection. The tool is what the model can actually
    reach; this stays for clients that browse resources directly.
    """
    return CATEGORIES


if __name__ == "__main__":
    # Local development only. On Horizon the entrypoint is `main.py:mcp` and
    # the platform owns the transport, so this block never runs there.
    #
    # Defaults to stdio because that is what an MCP client spawning this file
    # as a subprocess expects: JSON-RPC over stdin/stdout. Pass `http` to
    # serve over HTTP instead, for the Inspector or curl.
    #
    # Note 0.0.0.0 is a bind-all wildcard, not an address you can browse to --
    # open http://localhost:8000/mcp. A browser GET on that path returns
    # 406 Not Acceptable, which is correct: MCP needs POST with
    # `Accept: application/json, text/event-stream`.
    if len(sys.argv) > 1 and sys.argv[1] == "http":
        mcp.run(transport="http", host="127.0.0.1", port=8000)
    else:
        # No banner over stdio: a client spawns this as a subprocess and the
        # banner is pure noise in the client's terminal.
        mcp.run(transport="stdio", show_banner=False)
