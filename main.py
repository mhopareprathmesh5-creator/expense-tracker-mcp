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

# Phase 4 replaces this with the subject of the OAuth token. It is deliberately
# NOT a tool parameter: if the model could choose the user_id, any client could
# read any other user's expenses just by asking for them.
DEFAULT_USER_ID = "default"

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
        "the server's."
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
    clauses = ["user_id = $1"]
    args: list[Any] = [DEFAULT_USER_ID]

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

    pool = await get_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO expenses (user_id, date, amount, category, subcategory, note)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, date, amount, category, subcategory, note
        """,
        DEFAULT_USER_ID,
        date,
        amt,
        cat,
        sub,
        note.strip(),
    )

    log.info("added expense id=%s %s %s", row["id"], cat, amt)
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
    # Note 0.0.0.0 is a bind-all wildcard, not an address you can browse to --
    # open http://localhost:8000/mcp. A browser GET on that path returns
    # 406 Not Acceptable, which is correct: MCP needs POST with
    # `Accept: application/json, text/event-stream`. Use the Inspector.
    mcp.run(transport="http", host="127.0.0.1", port=8000)
