-- Schema for expense-tracker-mcp (Neon Postgres)
--
-- Run this ONCE against your Neon database before starting the server.
-- Every statement is idempotent, so re-running it is safe and does nothing
-- the second time.
--
-- Why this lives in a file instead of running at server startup:
-- the old version of this project called init_db() at import time, which
-- meant a database problem crashed the *deploy* rather than a single tool
-- call. Schema setup is a deliberate, one-time operation - keep it separate
-- from application boot.

CREATE TABLE IF NOT EXISTS expenses (
    -- BIGSERIAL, not SERIAL: a 4-byte int tops out at ~2.1 billion. The
    -- cost of 8 bytes is nothing and it removes a whole class of future bug.
    id           BIGSERIAL     PRIMARY KEY,

    -- Present from day one even though nothing populates it yet.
    -- When OAuth is switched on in phase 4, every query gets scoped by this
    -- column. Adding a NOT NULL column to a populated table later is a
    -- migration; adding it now with a default costs nothing.
    user_id      TEXT          NOT NULL DEFAULT 'default',

    -- DATE, not TEXT. The old version stored dates as strings, which made
    -- range filters string comparisons - they happen to work for ISO
    -- YYYY-MM-DD and silently break for anything else. A real DATE column
    -- rejects garbage like '2026-13-45' at write time.
    date         DATE          NOT NULL,

    -- NUMERIC, never FLOAT/REAL, for money. Binary floating point cannot
    -- represent 0.1 exactly, so summing many float amounts accumulates
    -- error and your totals drift by cents. NUMERIC is exact decimal.
    -- (12,2) = up to 10 digits before the decimal point, 2 after.
    amount       NUMERIC(12,2) NOT NULL CHECK (amount > 0),

    -- Validated in Python against categories.json before insert.
    category     TEXT          NOT NULL,
    subcategory  TEXT          NOT NULL DEFAULT '',
    note         TEXT          NOT NULL DEFAULT '',

    -- Audit trail: when the row was written, as opposed to the date the
    -- money was actually spent. TIMESTAMPTZ stores UTC and converts on
    -- read, so this stays correct regardless of server timezone.
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Supports the query both list_expenses and summarize run:
-- filter by user, filter by date range, order by date descending.
-- A composite index in (user_id, date) order lets Postgres seek straight to
-- one user's slice and walk it in date order without a sort step.
CREATE INDEX IF NOT EXISTS expenses_user_date_idx
    ON expenses (user_id, date DESC);

-- Supports summarize's GROUP BY category within a user's rows.
CREATE INDEX IF NOT EXISTS expenses_user_category_idx
    ON expenses (user_id, category);
