import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS brands (
    brand_id        TEXT PRIMARY KEY,
    brand_name      TEXT NOT NULL,
    instance_label  TEXT,
    agency          TEXT,
    parent_company  TEXT,
    is_active       INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS profiles (
    distinct_id TEXT PRIMARY KEY,
    email       TEXT,
    brand_id    TEXT,
    properties  TEXT,
    last_seen   TEXT,
    updated_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_profiles_email ON profiles(email);
CREATE INDEX IF NOT EXISTS idx_profiles_brand ON profiles(brand_id);

CREATE TABLE IF NOT EXISTS events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    dedup_key    TEXT UNIQUE,
    event_name   TEXT NOT NULL,
    distinct_id  TEXT,
    brand_id     TEXT NOT NULL,
    ist_date     TEXT NOT NULL,
    ist_ts       TEXT NOT NULL,
    current_url  TEXT,
    modules      TEXT,
    sub_page     TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_brand_date ON events(brand_id, ist_date);
CREATE INDEX IF NOT EXISTS idx_events_distinct  ON events(distinct_id);
CREATE INDEX IF NOT EXISTS idx_events_modules   ON events(modules);

CREATE TABLE IF NOT EXISTS runs (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at              TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at             TEXT,
    kind                    TEXT NOT NULL,
    requested_from          TEXT,
    requested_to            TEXT,
    actual_from             TEXT,
    actual_to               TEXT,
    fallback_hops           INTEGER DEFAULT 0,
    events_seen             INTEGER DEFAULT 0,
    events_kept             INTEGER DEFAULT 0,
    dropped_no_brand_id     INTEGER DEFAULT 0,
    dropped_not_on_roster   INTEGER DEFAULT 0,
    dropped_no_profile      INTEGER DEFAULT 0,
    dropped_hawky_excluded  INTEGER DEFAULT 0,
    dropped_no_time         INTEGER DEFAULT 0,
    dropped_out_of_window   INTEGER DEFAULT 0,
    profiles_fetched        INTEGER DEFAULT 0,
    error                   TEXT,
    status                  TEXT DEFAULT 'running'
);

CREATE TABLE IF NOT EXISTS snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    label       TEXT,
    run_id      INTEGER REFERENCES runs(id),
    payload     TEXT
);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hawky_exclusions (
    email      TEXT PRIMARY KEY,
    kind       TEXT NOT NULL,
    note       TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hawky_rules (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    field      TEXT NOT NULL DEFAULT 'email',
    operator   TEXT NOT NULL,  -- contains, not_contains, starts_with, ends_with, equals
    value      TEXT NOT NULL,
    kind       TEXT NOT NULL,  -- allow, deny
    note       TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Creative Production Tracker --------------------------------------------
-- One row per creative (Mongo `inventories` doc). UNLIKE `events`, this table
-- is NOT subject to the Hawky exclusion filter (data/filters.py): HAWKY-made
-- counts are a first-class metric here. cleanup_hawky_events() only touches
-- `events`, so this stays safe — keep it that way.
CREATE TABLE IF NOT EXISTS creatives (
    inventory_id    TEXT PRIMARY KEY,   -- Mongo _id hex
    hash            TEXT,
    brand_id        TEXT NOT NULL,      -- Mongo brandId hex; same key space as brands.brand_id
    source          TEXT NOT NULL,      -- PRODUCTION_TABLE | COPILOT | CREATIVE_AGENT | OTHER
    status          TEXT,               -- pending | approved | live | rejected | …
    created_at_utc  TEXT NOT NULL,      -- ISO UTC
    ist_ts          TEXT NOT NULL,      -- ISO IST (with +05:30 offset)
    ist_date        TEXT NOT NULL,      -- YYYY-MM-DD IST (filtering key, matches events.ist_date)
    creator_email   TEXT,               -- normalized: lowercase, +alias stripped; NULL = no real email
    creator_type    TEXT NOT NULL,      -- BRAND | HAWKY | AGENT | UNATTRIBUTED
    resolution      TEXT,               -- direct_email | user_join | agent_rule | unresolved (debuggability)
    table_id        TEXT,
    table_name      TEXT,
    agent_id        TEXT,
    agent_name      TEXT,
    run_session     TEXT,               -- chatId — identifies the agent run / chat session
    model_used      TEXT,
    creative_url    TEXT
);
CREATE INDEX IF NOT EXISTS idx_creatives_brand_date ON creatives(brand_id, ist_date);
CREATE INDEX IF NOT EXISTS idx_creatives_date_src   ON creatives(ist_date, source);
CREATE INDEX IF NOT EXISTS idx_creatives_email      ON creatives(creator_email);
CREATE INDEX IF NOT EXISTS idx_creatives_agent      ON creatives(agent_id);

-- Brand-name dimension for ALL brands seen in inventories (roster + off-roster).
CREATE TABLE IF NOT EXISTS creative_brands (
    brand_id     TEXT PRIMARY KEY,
    brand_name   TEXT,
    is_on_roster INTEGER NOT NULL DEFAULT 0,
    updated_at   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Sync run log (mirrors `runs`); drives "data as of" + /config status.
CREATE TABLE IF NOT EXISTS creative_sync_runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at   TEXT,
    kind          TEXT NOT NULL,         -- auto | manual
    docs_seen     INTEGER DEFAULT 0,
    docs_upserted INTEGER DEFAULT 0,
    unattributed  INTEGER DEFAULT 0,
    status        TEXT DEFAULT 'running',
    error         TEXT
);
"""


def _ensure_column(conn, table, col, type_):
    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {type_}")


def init_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    _ensure_column(conn, "brands", "parent_company", "TEXT")
    conn.commit()
    return conn


@contextmanager
def db(db_path: Path):
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# Settings -----------------------------------------------------------------

def get_setting(conn, key, default=None):
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(conn, key, value):
    conn.execute(
        "INSERT INTO settings(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )


# Brands -------------------------------------------------------------------

def list_brands(conn, active_only=True):
    q = "SELECT * FROM brands"
    if active_only:
        q += " WHERE is_active=1"
    # Sort agencies first (alphabetical), then parent-company groups
    # (alphabetical), then pure direct clients at the bottom.
    q += (
        " ORDER BY "
        " CASE WHEN agency IS NOT NULL AND agency <> '' THEN 0 "
        "      WHEN parent_company IS NOT NULL AND parent_company <> '' THEN 1 "
        "      ELSE 2 END, "
        " COALESCE(agency,''), COALESCE(parent_company,''), "
        " brand_name, COALESCE(instance_label,'')"
    )
    return [dict(r) for r in conn.execute(q).fetchall()]


def brand_ids(conn, active_only=True):
    q = "SELECT brand_id FROM brands"
    if active_only:
        q += " WHERE is_active=1"
    return [r["brand_id"] for r in conn.execute(q).fetchall()]


def upsert_brand(conn, brand_id, brand_name, instance_label=None, agency=None,
                 parent_company=None, is_active=1):
    conn.execute(
        """INSERT INTO brands(brand_id, brand_name, instance_label, agency, parent_company, is_active)
           VALUES(?,?,?,?,?,?)
           ON CONFLICT(brand_id) DO UPDATE SET
               brand_name     = excluded.brand_name,
               instance_label = excluded.instance_label,
               agency         = excluded.agency,
               parent_company = excluded.parent_company,
               is_active      = excluded.is_active""",
        (brand_id, brand_name, instance_label, agency, parent_company, is_active),
    )


def delete_brand(conn, brand_id):
    conn.execute("DELETE FROM brands WHERE brand_id=?", (brand_id,))


# Hawky exclusions ---------------------------------------------------------

def get_exclusions(conn):
    rows = conn.execute("SELECT email, kind FROM hawky_exclusions").fetchall()
    allow = {r["email"] for r in rows if r["kind"] == "allow"}
    deny = {r["email"] for r in rows if r["kind"] == "deny"}
    return allow, deny


def upsert_exclusion(conn, email, kind, note=None):
    conn.execute(
        "INSERT OR REPLACE INTO hawky_exclusions(email,kind,note) VALUES(?,?,?)",
        (email.strip().lower(), kind, note),
    )


def delete_exclusion(conn, email):
    conn.execute("DELETE FROM hawky_exclusions WHERE email=?", (email.strip().lower(),))


# Hawky pattern rules ------------------------------------------------------
# Generalises per-email overrides. e.g. "email contains hawky → deny"
# matches all addresses with "hawky" in them, regardless of domain.

HAWKY_RULE_OPERATORS = ("contains", "not_contains", "starts_with", "ends_with", "equals")
HAWKY_RULE_KINDS = ("allow", "deny")


def list_rules(conn):
    rows = conn.execute(
        "SELECT id, field, operator, value, kind, note, created_at "
        "FROM hawky_rules ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def upsert_rule(conn, *, field="email", operator, value, kind, note=None, rule_id=None):
    operator = (operator or "").strip().lower()
    kind = (kind or "").strip().lower()
    value = (value or "").strip()
    if operator not in HAWKY_RULE_OPERATORS:
        raise ValueError(f"Invalid operator '{operator}'")
    if kind not in HAWKY_RULE_KINDS:
        raise ValueError(f"Invalid kind '{kind}'")
    if not value:
        raise ValueError("Rule value cannot be empty")
    if rule_id:
        conn.execute(
            "UPDATE hawky_rules SET field=?, operator=?, value=?, kind=?, note=? WHERE id=?",
            (field, operator, value, kind, note, int(rule_id)),
        )
        return int(rule_id)
    cur = conn.execute(
        "INSERT INTO hawky_rules(field, operator, value, kind, note) VALUES(?,?,?,?,?)",
        (field, operator, value, kind, note),
    )
    return cur.lastrowid


def delete_rule(conn, rule_id):
    conn.execute("DELETE FROM hawky_rules WHERE id=?", (int(rule_id),))


# Runs ---------------------------------------------------------------------

def start_run(conn, kind, requested_from, requested_to):
    cur = conn.execute(
        "INSERT INTO runs(kind, requested_from, requested_to) VALUES(?,?,?)",
        (kind, str(requested_from), str(requested_to)),
    )
    return cur.lastrowid


def finish_run(conn, run_id, **fields):
    if not fields:
        conn.execute(
            "UPDATE runs SET finished_at=CURRENT_TIMESTAMP, status='ok' WHERE id=?",
            (run_id,),
        )
        return
    assigns = ", ".join(f"{k}=?" for k in fields)
    values = list(fields.values()) + [run_id]
    conn.execute(
        f"UPDATE runs SET finished_at=CURRENT_TIMESTAMP, status='ok', {assigns} WHERE id=?",
        values,
    )


def fail_run(conn, run_id, error):
    conn.execute(
        "UPDATE runs SET finished_at=CURRENT_TIMESTAMP, status='error', error=? WHERE id=?",
        (str(error)[:1000], run_id),
    )


def latest_run(conn):
    row = conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 1").fetchone()
    return dict(row) if row else None


def recent_runs(conn, n=10):
    rows = conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT ?", (n,)).fetchall()
    return [dict(r) for r in rows]


# Events -------------------------------------------------------------------

def insert_event(conn, *, dedup_key, event_name, distinct_id, brand_id,
                 ist_date, ist_ts, current_url, modules, sub_page):
    conn.execute(
        """INSERT OR IGNORE INTO events
           (dedup_key, event_name, distinct_id, brand_id, ist_date, ist_ts, current_url, modules, sub_page)
           VALUES(?,?,?,?,?,?,?,?,?)""",
        (
            dedup_key, event_name, distinct_id, brand_id, ist_date, ist_ts,
            current_url, ",".join(modules) if modules else None, sub_page,
        ),
    )


def prune_events_before(conn, ist_date):
    conn.execute("DELETE FROM events WHERE ist_date < ?", (str(ist_date),))


def module_counts_by_brand(conn, ist_from, ist_to):
    """
    Returns {brand_id: {'Creative Intel': n, 'Competitive Intel': n,
    'Production+Playbooks': n, 'Co-Pilot': n, 'Agents': n, 'total': n}}.
    Co-Pilot events DOUBLE-count per resolved decision: they show up in
    Co-Pilot AND the URL-derived module. 'total' is the raw event count and
    does NOT double-count.
    """
    rows = conn.execute(
        """SELECT brand_id,
                  SUM(CASE WHEN modules LIKE '%Creative Intel%'         THEN 1 ELSE 0 END) AS creative,
                  SUM(CASE WHEN modules LIKE '%Competitive Intel%'      THEN 1 ELSE 0 END) AS competitor,
                  SUM(CASE WHEN modules LIKE '%Production+Playbooks%'   THEN 1 ELSE 0 END) AS production,
                  SUM(CASE WHEN modules LIKE '%Co-Pilot%'               THEN 1 ELSE 0 END) AS copilot,
                  SUM(CASE WHEN modules LIKE '%Agents%'                 THEN 1 ELSE 0 END) AS agents,
                  COUNT(*) AS total
           FROM events
           WHERE ist_date BETWEEN ? AND ?
           GROUP BY brand_id""",
        (str(ist_from), str(ist_to)),
    ).fetchall()
    out = {}
    for r in rows:
        out[r["brand_id"]] = {
            "Creative Intel": r["creative"] or 0,
            "Competitive Intel": r["competitor"] or 0,
            "Production+Playbooks": r["production"] or 0,
            "Co-Pilot": r["copilot"] or 0,
            "Agents": r["agents"] or 0,
            "total": r["total"] or 0,
        }
    return out


def module_users_by_brand(conn, ist_from, ist_to, brand_ids=None):
    """
    Per-brand, per-module DISTINCT-USER count. Sibling of
    module_counts_by_brand which counts events instead.

    Returns {brand_id: {'Creative Intel': n_users, ..., 'Agents': n_users}}.
    Optional brand_ids list scopes the query (matches Slice 8 pattern).
    """
    if brand_ids is not None:
        brand_ids = list(brand_ids)
        if not brand_ids:
            return {}
        placeholders = ",".join(["?"] * len(brand_ids))
        where_brand = f" AND brand_id IN ({placeholders})"
        extra_args = list(brand_ids)
    else:
        where_brand = ""
        extra_args = []

    rows = conn.execute(
        f"""SELECT brand_id,
                  COUNT(DISTINCT CASE WHEN modules LIKE '%Creative Intel%'        THEN distinct_id END) AS creative,
                  COUNT(DISTINCT CASE WHEN modules LIKE '%Competitive Intel%'     THEN distinct_id END) AS competitor,
                  COUNT(DISTINCT CASE WHEN modules LIKE '%Production+Playbooks%'  THEN distinct_id END) AS production,
                  COUNT(DISTINCT CASE WHEN modules LIKE '%Co-Pilot%'              THEN distinct_id END) AS copilot,
                  COUNT(DISTINCT CASE WHEN modules LIKE '%Agents%'                THEN distinct_id END) AS agents
           FROM events
           WHERE ist_date BETWEEN ? AND ?{where_brand}
           GROUP BY brand_id""",
        [str(ist_from), str(ist_to)] + extra_args,
    ).fetchall()
    out = {}
    for r in rows:
        out[r["brand_id"]] = {
            "Creative Intel":       r["creative"] or 0,
            "Competitive Intel":    r["competitor"] or 0,
            "Production+Playbooks": r["production"] or 0,
            "Co-Pilot":             r["copilot"] or 0,
            "Agents":               r["agents"] or 0,
        }
    return out


def brand_module_meta(conn, brand_id, ist_from, ist_to):
    """
    For ONE brand, per-module: events, users, last_seen. Feeds the
    Overview module-use cards (each card mirrors the existing Co-Pilot
    sub-line pattern).
    """
    r = conn.execute(
        """SELECT
              SUM(CASE WHEN modules LIKE '%Creative Intel%'        THEN 1 ELSE 0 END) AS creative_n,
              COUNT(DISTINCT CASE WHEN modules LIKE '%Creative Intel%'        THEN distinct_id END) AS creative_u,
              MAX(CASE WHEN modules LIKE '%Creative Intel%'        THEN ist_ts END) AS creative_t,
              SUM(CASE WHEN modules LIKE '%Competitive Intel%'     THEN 1 ELSE 0 END) AS competitor_n,
              COUNT(DISTINCT CASE WHEN modules LIKE '%Competitive Intel%'     THEN distinct_id END) AS competitor_u,
              MAX(CASE WHEN modules LIKE '%Competitive Intel%'     THEN ist_ts END) AS competitor_t,
              SUM(CASE WHEN modules LIKE '%Production+Playbooks%'  THEN 1 ELSE 0 END) AS production_n,
              COUNT(DISTINCT CASE WHEN modules LIKE '%Production+Playbooks%'  THEN distinct_id END) AS production_u,
              MAX(CASE WHEN modules LIKE '%Production+Playbooks%'  THEN ist_ts END) AS production_t,
              SUM(CASE WHEN modules LIKE '%Co-Pilot%'              THEN 1 ELSE 0 END) AS copilot_n,
              COUNT(DISTINCT CASE WHEN modules LIKE '%Co-Pilot%'              THEN distinct_id END) AS copilot_u,
              MAX(CASE WHEN modules LIKE '%Co-Pilot%'              THEN ist_ts END) AS copilot_t,
              SUM(CASE WHEN modules LIKE '%Agents%'                THEN 1 ELSE 0 END) AS agents_n,
              COUNT(DISTINCT CASE WHEN modules LIKE '%Agents%'                THEN distinct_id END) AS agents_u,
              MAX(CASE WHEN modules LIKE '%Agents%'                THEN ist_ts END) AS agents_t
           FROM events
           WHERE brand_id = ? AND ist_date BETWEEN ? AND ?""",
        (brand_id, str(ist_from), str(ist_to)),
    ).fetchone()
    return {
        "Creative Intel":       {"events": r["creative_n"]   or 0, "users": r["creative_u"]   or 0, "last_seen": r["creative_t"]},
        "Competitive Intel":    {"events": r["competitor_n"] or 0, "users": r["competitor_u"] or 0, "last_seen": r["competitor_t"]},
        "Production+Playbooks": {"events": r["production_n"] or 0, "users": r["production_u"] or 0, "last_seen": r["production_t"]},
        "Co-Pilot":             {"events": r["copilot_n"]    or 0, "users": r["copilot_u"]    or 0, "last_seen": r["copilot_t"]},
        "Agents":               {"events": r["agents_n"]     or 0, "users": r["agents_u"]     or 0, "last_seen": r["agents_t"]},
    }


def brand_user_day_module(conn, brand_id, ist_from, ist_to):
    """
    For ONE brand: per (distinct_id, ist_date) → per-module event counts
    + total. Powers the per-user activity strip on the Users tab; tooltips
    pull the module breakdown for any given day.

    Returns {distinct_id: {ist_date: {module: count, 'total': n}}}.
    """
    rows = conn.execute(
        """SELECT distinct_id, ist_date,
                  SUM(CASE WHEN modules LIKE '%Creative Intel%'        THEN 1 ELSE 0 END) AS creative,
                  SUM(CASE WHEN modules LIKE '%Competitive Intel%'     THEN 1 ELSE 0 END) AS competitor,
                  SUM(CASE WHEN modules LIKE '%Production+Playbooks%'  THEN 1 ELSE 0 END) AS production,
                  SUM(CASE WHEN modules LIKE '%Co-Pilot%'              THEN 1 ELSE 0 END) AS copilot,
                  SUM(CASE WHEN modules LIKE '%Agents%'                THEN 1 ELSE 0 END) AS agents,
                  COUNT(*) AS total
           FROM events
           WHERE brand_id = ? AND ist_date BETWEEN ? AND ?
           GROUP BY distinct_id, ist_date""",
        (brand_id, str(ist_from), str(ist_to)),
    ).fetchall()
    out = {}
    for r in rows:
        out.setdefault(r["distinct_id"], {})[r["ist_date"]] = {
            "Creative Intel":       r["creative"]   or 0,
            "Competitive Intel":    r["competitor"] or 0,
            "Production+Playbooks": r["production"] or 0,
            "Co-Pilot":             r["copilot"]    or 0,
            "Agents":               r["agents"]     or 0,
            "total":                r["total"]      or 0,
        }
    return out


def events_by_day_total(conn, ist_from, ist_to):
    rows = conn.execute(
        """SELECT ist_date, COUNT(*) AS n
           FROM events
           WHERE ist_date BETWEEN ? AND ?
           GROUP BY ist_date""",
        (str(ist_from), str(ist_to)),
    ).fetchall()
    return {r["ist_date"]: r["n"] for r in rows}


def metric_series_by_day(conn, ist_from, ist_to,
                          copilot_message_event="copilot_message_sent",
                          brand_ids=None):
    """
    Per-day metric snapshot for the engagement chart's metric dropdown.
    Returns {ist_date: {events, users, brands, copilot_events, copilot_users,
    copilot_brands, copilot_messages, copilot_message_users,
    copilot_message_brands}}.

    'copilot_messages' is the count of `copilot_message_event` events (default
    'copilot_message_sent'), which is the canonical Co-Pilot adoption signal.

    If `brand_ids` is provided (a list/iterable), the result is scoped to
    those brands. Passing an empty list returns an empty dict.
    """
    if brand_ids is not None:
        brand_ids = list(brand_ids)
        if not brand_ids:
            return {}
        placeholders = ",".join(["?"] * len(brand_ids))
        where_brand = f" AND brand_id IN ({placeholders})"
        extra_args = list(brand_ids)
    else:
        where_brand = ""
        extra_args = []

    rows = conn.execute(
        f"""SELECT ist_date,
                  COUNT(*)                                                AS events,
                  COUNT(DISTINCT distinct_id)                             AS users,
                  COUNT(DISTINCT brand_id)                                AS brands,
                  SUM(CASE WHEN modules LIKE '%Co-Pilot%' THEN 1 ELSE 0 END) AS copilot_events,
                  COUNT(DISTINCT CASE WHEN modules LIKE '%Co-Pilot%' THEN distinct_id END) AS copilot_users,
                  COUNT(DISTINCT CASE WHEN modules LIKE '%Co-Pilot%' THEN brand_id    END) AS copilot_brands,
                  SUM(CASE WHEN event_name = ? THEN 1 ELSE 0 END)         AS copilot_messages,
                  COUNT(DISTINCT CASE WHEN event_name = ? THEN distinct_id END) AS copilot_message_users,
                  COUNT(DISTINCT CASE WHEN event_name = ? THEN brand_id    END) AS copilot_message_brands
           FROM events
           WHERE ist_date BETWEEN ? AND ?{where_brand}
           GROUP BY ist_date""",
        [copilot_message_event, copilot_message_event, copilot_message_event,
         str(ist_from), str(ist_to)] + extra_args,
    ).fetchall()
    return {
        r["ist_date"]: {
            "events":                 r["events"] or 0,
            "users":                  r["users"] or 0,
            "brands":                 r["brands"] or 0,
            "copilot_events":         r["copilot_events"] or 0,
            "copilot_users":          r["copilot_users"] or 0,
            "copilot_brands":         r["copilot_brands"] or 0,
            "copilot_messages":       r["copilot_messages"] or 0,
            "copilot_message_users":  r["copilot_message_users"] or 0,
            "copilot_message_brands": r["copilot_message_brands"] or 0,
        }
        for r in rows
    }


def copilot_brand_summary(conn, ist_today_date, day_window=21,
                          copilot_message_event="copilot_message_sent"):
    """
    Per-brand Co-Pilot rollup for the /copilot adoption page.

    Returns {brand_id: {window_events, window_users, week_events, week_users,
    window_messages, week_messages, last_seen, last_message, daily,
    daily_messages}}.

    *_events count all Co-Pilot-attributed events (event whose modules string
    contains 'Co-Pilot'). *_messages count only the canonical message-sent
    event, which is the high-signal adoption metric.

    week_* = rolling 7 IST days (inclusive of today). That's still the metric
    used for Heavy/Moderate/Trialing/Never bucketing per resolved decision.
    """
    from datetime import timedelta as _td  # local to avoid leaking name
    ist_from = ist_today_date - _td(days=day_window - 1)
    week_from = ist_today_date - _td(days=6)

    daily = conn.execute(
        """SELECT brand_id, ist_date,
                  COUNT(*) AS n,
                  SUM(CASE WHEN event_name = ? THEN 1 ELSE 0 END) AS msgs
           FROM events
           WHERE modules LIKE '%Co-Pilot%' AND ist_date BETWEEN ? AND ?
           GROUP BY brand_id, ist_date""",
        (copilot_message_event, str(ist_from), str(ist_today_date)),
    ).fetchall()

    summary = conn.execute(
        """SELECT brand_id,
                  COUNT(*)                                                          AS window_events,
                  COUNT(DISTINCT distinct_id)                                       AS window_users,
                  MAX(ist_ts)                                                       AS last_seen,
                  SUM(CASE WHEN ist_date BETWEEN ? AND ? THEN 1 ELSE 0 END)         AS week_events,
                  COUNT(DISTINCT CASE WHEN ist_date BETWEEN ? AND ? THEN distinct_id END) AS week_users,
                  SUM(CASE WHEN event_name = ? THEN 1 ELSE 0 END)                   AS window_messages,
                  COUNT(DISTINCT CASE WHEN event_name = ? THEN distinct_id END)     AS window_message_users,
                  SUM(CASE WHEN event_name = ? AND ist_date BETWEEN ? AND ? THEN 1 ELSE 0 END) AS week_messages,
                  COUNT(DISTINCT CASE WHEN event_name = ? AND ist_date BETWEEN ? AND ? THEN distinct_id END) AS week_message_users,
                  MAX(CASE WHEN event_name = ? THEN ist_ts ELSE NULL END)           AS last_message
           FROM events
           WHERE modules LIKE '%Co-Pilot%' AND ist_date BETWEEN ? AND ?
           GROUP BY brand_id""",
        (
            str(week_from), str(ist_today_date),
            str(week_from), str(ist_today_date),
            copilot_message_event,
            copilot_message_event,
            copilot_message_event, str(week_from), str(ist_today_date),
            copilot_message_event, str(week_from), str(ist_today_date),
            copilot_message_event,
            str(ist_from), str(ist_today_date),
        ),
    ).fetchall()

    daily_by = {}
    daily_msgs = {}
    for r in daily:
        daily_by.setdefault(r["brand_id"], {})[r["ist_date"]] = r["n"]
        daily_msgs.setdefault(r["brand_id"], {})[r["ist_date"]] = r["msgs"] or 0

    out = {}
    for r in summary:
        out[r["brand_id"]] = {
            "window_events":         r["window_events"] or 0,
            "window_users":          r["window_users"] or 0,
            "week_events":           r["week_events"] or 0,
            "week_users":            r["week_users"] or 0,
            "window_messages":       r["window_messages"] or 0,
            "window_message_users":  r["window_message_users"] or 0,
            "week_messages":         r["week_messages"] or 0,
            "week_message_users":    r["week_message_users"] or 0,
            "last_seen":             r["last_seen"],
            "last_message":          r["last_message"],
            "daily":                 daily_by.get(r["brand_id"], {}),
            "daily_messages":        daily_msgs.get(r["brand_id"], {}),
        }
    return out


def brand_copilot_messages(conn, brand_id, ist_from, ist_to,
                           copilot_message_event="copilot_message_sent"):
    r = conn.execute(
        """SELECT COUNT(*) AS n,
                  COUNT(DISTINCT distinct_id) AS users,
                  MAX(ist_ts) AS last_sent
           FROM events
           WHERE brand_id = ? AND event_name = ? AND ist_date BETWEEN ? AND ?""",
        (brand_id, copilot_message_event, str(ist_from), str(ist_to)),
    ).fetchone()
    return {
        "events":    r["n"] or 0,
        "users":     r["users"] or 0,
        "last_sent": r["last_sent"],
    }


_ZERO_MODULE_MIX = {
    "Creative Intel": 0, "Competitive Intel": 0,
    "Production+Playbooks": 0, "Co-Pilot": 0, "Agents": 0,
}


def module_mix_total(conn, ist_from, ist_to, brand_ids=None):
    """
    Module-mix totals across the window. If `brand_ids` is provided, the
    result is scoped to those brands. Empty list → all-zero dict.
    """
    if brand_ids is not None:
        brand_ids = list(brand_ids)
        if not brand_ids:
            return dict(_ZERO_MODULE_MIX)
        placeholders = ",".join(["?"] * len(brand_ids))
        where_brand = f" AND brand_id IN ({placeholders})"
        extra_args = list(brand_ids)
    else:
        where_brand = ""
        extra_args = []

    r = conn.execute(
        f"""SELECT
              SUM(CASE WHEN modules LIKE '%Creative Intel%'        THEN 1 ELSE 0 END) AS creative,
              SUM(CASE WHEN modules LIKE '%Competitive Intel%'     THEN 1 ELSE 0 END) AS competitor,
              SUM(CASE WHEN modules LIKE '%Production+Playbooks%'  THEN 1 ELSE 0 END) AS production,
              SUM(CASE WHEN modules LIKE '%Co-Pilot%'              THEN 1 ELSE 0 END) AS copilot,
              SUM(CASE WHEN modules LIKE '%Agents%'                THEN 1 ELSE 0 END) AS agents
           FROM events
           WHERE ist_date BETWEEN ? AND ?{where_brand}""",
        [str(ist_from), str(ist_to)] + extra_args,
    ).fetchone()
    return {
        "Creative Intel": r["creative"] or 0,
        "Competitive Intel": r["competitor"] or 0,
        "Production+Playbooks": r["production"] or 0,
        "Co-Pilot": r["copilot"] or 0,
        "Agents": r["agents"] or 0,
    }


def active_brands_since(conn, since_iso):
    """Distinct brand_ids with at least one event whose ist_ts >= since_iso."""
    rows = conn.execute(
        "SELECT DISTINCT brand_id FROM events WHERE ist_ts >= ?",
        (since_iso,),
    ).fetchall()
    return {r["brand_id"] for r in rows}


# Brand drill-down queries -------------------------------------------------

def brand_user_summary(conn, brand_id, ist_from, ist_to):
    """
    Per-distinct_id rollup for one brand over the IST window.
    'active_days' is our session-count proxy (Mixpanel default events don't
    carry $session_id reliably). Module names come from the comma-joined
    modules column on each event; we explode and dedup.
    """
    rows = conn.execute(
        """SELECT e.distinct_id,
                  p.email,
                  COUNT(*)                       AS events,
                  COUNT(DISTINCT e.ist_date)     AS active_days,
                  COUNT(DISTINCT e.current_url)  AS pages_seen,
                  MAX(e.ist_ts)                  AS last_seen,
                  MIN(e.ist_ts)                  AS first_seen,
                  GROUP_CONCAT(DISTINCT e.modules) AS module_blob
           FROM events e
           LEFT JOIN profiles p ON p.distinct_id = e.distinct_id
           WHERE e.brand_id = ? AND e.ist_date BETWEEN ? AND ?
           GROUP BY e.distinct_id
           ORDER BY events DESC""",
        (brand_id, str(ist_from), str(ist_to)),
    ).fetchall()
    out = []
    for r in rows:
        blob = r["module_blob"] or ""
        modules_touched = sorted({m for m in blob.split(",") if m})
        out.append({
            "distinct_id": r["distinct_id"],
            "email": r["email"],
            "events": r["events"],
            "active_days": r["active_days"],
            "pages_seen": r["pages_seen"],
            "last_seen": r["last_seen"],
            "first_seen": r["first_seen"],
            "modules": modules_touched,
        })
    return out


def brand_section_rollup(conn, brand_id, ist_from, ist_to, modules_order):
    """
    Returns {module: {'total': n, 'last_seen': iso, 'sub_pages': [(sub_page, {events, last_seen})]}}.
    Co-Pilot double-count: events whose modules string mentions both X and Co-Pilot
    contribute to both module buckets (per resolved decision).
    """
    rows = conn.execute(
        """SELECT modules, sub_page,
                  COUNT(*) AS n, MAX(ist_ts) AS last_seen
           FROM events
           WHERE brand_id = ? AND ist_date BETWEEN ? AND ?
             AND modules IS NOT NULL AND modules <> ''
           GROUP BY modules, sub_page""",
        (brand_id, str(ist_from), str(ist_to)),
    ).fetchall()
    out = {m: {"total": 0, "last_seen": None, "sub_pages": {}} for m in modules_order}
    for r in rows:
        mods = [m for m in (r["modules"] or "").split(",") if m]
        sub = r["sub_page"] or "(landing / no sub-page)"
        n = r["n"] or 0
        ls = r["last_seen"]
        for m in mods:
            if m not in out:
                continue
            bucket = out[m]
            bucket["total"] += n
            if ls and (bucket["last_seen"] is None or ls > bucket["last_seen"]):
                bucket["last_seen"] = ls
            sp = bucket["sub_pages"].setdefault(sub, {"events": 0, "last_seen": None})
            sp["events"] += n
            if ls and (sp["last_seen"] is None or ls > sp["last_seen"]):
                sp["last_seen"] = ls
    for m, bucket in out.items():
        bucket["sub_pages"] = sorted(
            bucket["sub_pages"].items(), key=lambda kv: -kv[1]["events"],
        )
    return out


def brand_top_pages(conn, brand_id, ist_from, ist_to, sort="events", limit=100):
    order = {
        "events": "n DESC",
        "users":  "users DESC, n DESC",
        "recent": "last_seen DESC",
    }.get(sort, "n DESC")
    rows = conn.execute(
        f"""SELECT current_url,
                   COUNT(*)                     AS n,
                   COUNT(DISTINCT distinct_id)  AS users,
                   MAX(ist_ts)                  AS last_seen
            FROM events
            WHERE brand_id = ? AND ist_date BETWEEN ? AND ?
              AND current_url IS NOT NULL AND current_url <> ''
            GROUP BY current_url
            ORDER BY {order}
            LIMIT ?""",
        (brand_id, str(ist_from), str(ist_to), int(limit)),
    ).fetchall()
    return [dict(r) for r in rows]


def brand_events_by_day_module(conn, brand_id, ist_from, ist_to):
    """
    Per-day breakdown for one brand. Returns {ist_date: {module: count, 'total': raw_count}}.
    Co-Pilot column double-counts (matches the matrix rule).
    """
    rows = conn.execute(
        """SELECT ist_date,
                  SUM(CASE WHEN modules LIKE '%Creative Intel%'        THEN 1 ELSE 0 END) AS creative,
                  SUM(CASE WHEN modules LIKE '%Competitive Intel%'     THEN 1 ELSE 0 END) AS competitor,
                  SUM(CASE WHEN modules LIKE '%Production+Playbooks%'  THEN 1 ELSE 0 END) AS production,
                  SUM(CASE WHEN modules LIKE '%Co-Pilot%'              THEN 1 ELSE 0 END) AS copilot,
                  SUM(CASE WHEN modules LIKE '%Agents%'                THEN 1 ELSE 0 END) AS agents,
                  COUNT(*) AS total
           FROM events
           WHERE brand_id = ? AND ist_date BETWEEN ? AND ?
           GROUP BY ist_date""",
        (brand_id, str(ist_from), str(ist_to)),
    ).fetchall()
    return {
        r["ist_date"]: {
            "Creative Intel": r["creative"] or 0,
            "Competitive Intel": r["competitor"] or 0,
            "Production+Playbooks": r["production"] or 0,
            "Co-Pilot": r["copilot"] or 0,
            "Agents": r["agents"] or 0,
            "total": r["total"] or 0,
        }
        for r in rows
    }


def brand_metric_series_by_day(conn, brand_id, ist_from, ist_to,
                               copilot_message_event="copilot_message_sent"):
    """
    Per-day metric snapshot for one brand — powers the metric dropdown on the
    brand Overview's events/day chart. Returns {ist_date: {users,
    copilot_users, copilot_messages}}.
    Per-module + total counts are NOT here (already in brand_events_by_day_module).
    """
    rows = conn.execute(
        """SELECT ist_date,
                  COUNT(DISTINCT distinct_id)                                     AS users,
                  SUM(CASE WHEN event_name = ? THEN 1 ELSE 0 END)                 AS copilot_messages,
                  COUNT(DISTINCT CASE WHEN event_name = ? THEN distinct_id END)   AS copilot_users
           FROM events
           WHERE brand_id = ? AND ist_date BETWEEN ? AND ?
           GROUP BY ist_date""",
        (copilot_message_event, copilot_message_event,
         brand_id, str(ist_from), str(ist_to)),
    ).fetchall()
    return {
        r["ist_date"]: {
            "users":            r["users"] or 0,
            "copilot_messages": r["copilot_messages"] or 0,
            "copilot_users":    r["copilot_users"] or 0,
        }
        for r in rows
    }


def brand_recent_events(conn, brand_id, limit=10):
    rows = conn.execute(
        """SELECT e.event_name, e.current_url, e.modules, e.sub_page,
                  e.distinct_id, e.ist_ts, p.email
           FROM events e
           LEFT JOIN profiles p ON p.distinct_id = e.distinct_id
           WHERE e.brand_id = ?
           ORDER BY e.ist_ts DESC
           LIMIT ?""",
        (brand_id, int(limit)),
    ).fetchall()
    return [dict(r) for r in rows]


def events_by_brand_day(conn, ist_from, ist_to):
    rows = conn.execute(
        """SELECT brand_id, ist_date,
                  COUNT(*) AS n,
                  COUNT(DISTINCT distinct_id) AS users
           FROM events
           WHERE ist_date BETWEEN ? AND ?
           GROUP BY brand_id, ist_date""",
        (str(ist_from), str(ist_to)),
    ).fetchall()
    out = {}
    for r in rows:
        out.setdefault(r["brand_id"], {})[r["ist_date"]] = {
            "events": r["n"],
            "users": r["users"],
        }
    return out


# Profiles -----------------------------------------------------------------

def upsert_profile(conn, distinct_id, email, brand_id, properties, last_seen):
    conn.execute(
        """INSERT INTO profiles(distinct_id, email, brand_id, properties, last_seen, updated_at)
           VALUES(?,?,?,?,?, CURRENT_TIMESTAMP)
           ON CONFLICT(distinct_id) DO UPDATE SET
               email      = excluded.email,
               brand_id   = excluded.brand_id,
               properties = excluded.properties,
               last_seen  = excluded.last_seen,
               updated_at = CURRENT_TIMESTAMP""",
        (
            distinct_id, email, brand_id,
            json.dumps(properties) if properties is not None else None,
            last_seen,
        ),
    )


# Snapshots ----------------------------------------------------------------

def insert_snapshot(conn, label, run_id, payload):
    cur = conn.execute(
        "INSERT INTO snapshots(label, run_id, payload) VALUES(?,?,?)",
        (label, run_id, json.dumps(payload)),
    )
    return cur.lastrowid


def write_current_snapshot(conn, ist_from, ist_to, label, cap):
    """
    Fast-path snapshot: capture the current events-table state without going
    back to Mixpanel. Builds the same {by_brand_day} payload shape that
    run_refresh(snapshot=True) writes, so /compare works the same against it.
    """
    rows = conn.execute(
        """SELECT brand_id, ist_date,
                  COUNT(*) AS n,
                  COUNT(DISTINCT distinct_id) AS users
           FROM events
           WHERE ist_date BETWEEN ? AND ?
           GROUP BY brand_id, ist_date""",
        (str(ist_from), str(ist_to)),
    ).fetchall()
    by_brand_day = {}
    for r in rows:
        by_brand_day.setdefault(r["brand_id"], {})[r["ist_date"]] = {
            "events": r["n"], "users": r["users"],
        }
    payload = {
        "ist_from": str(ist_from),
        "ist_to":   str(ist_to),
        "actual_from": str(ist_from),
        "actual_to":   str(ist_to),
        "kind": "current-state",
        "by_brand_day": by_brand_day,
    }
    snap_id = insert_snapshot(conn, label=label, run_id=None, payload=payload)
    prune_snapshots(conn, cap)
    return snap_id


def prune_snapshots(conn, cap):
    conn.execute(
        "DELETE FROM snapshots WHERE id NOT IN "
        "(SELECT id FROM snapshots ORDER BY id DESC LIMIT ?)",
        (int(cap),),
    )


def list_snapshots(conn, limit=30, include_payload=False):
    cols = "id, created_at, label, run_id"
    if include_payload:
        cols += ", payload"
    rows = conn.execute(
        f"SELECT {cols} FROM snapshots ORDER BY id DESC LIMIT ?",
        (int(limit),),
    ).fetchall()
    return [dict(r) for r in rows]


def get_snapshot(conn, snapshot_id):
    row = conn.execute(
        "SELECT id, created_at, label, run_id, payload FROM snapshots WHERE id=?",
        (int(snapshot_id),),
    ).fetchone()
    if not row:
        return None
    d = dict(row)
    try:
        d["payload"] = json.loads(d["payload"]) if d["payload"] else {}
    except (TypeError, ValueError):
        d["payload"] = {}
    return d


def _brand_totals_from_payload(payload):
    """Pull {brand_id: total_events} out of a snapshot payload."""
    out = {}
    for bid, days in (payload.get("by_brand_day") or {}).items():
        out[bid] = sum(v.get("events", 0) for v in days.values())
    return out


def _brand_totals_for_window(conn, ist_from, ist_to):
    rows = conn.execute(
        """SELECT brand_id, COUNT(*) AS n
           FROM events
           WHERE ist_date BETWEEN ? AND ?
           GROUP BY brand_id""",
        (str(ist_from), str(ist_to)),
    ).fetchall()
    return {r["brand_id"]: r["n"] or 0 for r in rows}


def _brand_users_for_window(conn, ist_from, ist_to):
    rows = conn.execute(
        """SELECT brand_id, COUNT(DISTINCT distinct_id) AS u
           FROM events
           WHERE ist_date BETWEEN ? AND ?
           GROUP BY brand_id""",
        (str(ist_from), str(ist_to)),
    ).fetchall()
    return {r["brand_id"]: r["u"] or 0 for r in rows}


def compare_brand_totals(before, after, brand_meta):
    """
    Build a sorted (descending Δ magnitude) list of per-brand delta rows.
    `before` and `after` are {brand_id: int}. `brand_meta` is
    {brand_id: {brand_name, agency, parent_company}}.
    """
    keys = set(before) | set(after)
    rows = []
    for bid in keys:
        b = before.get(bid, 0)
        a = after.get(bid, 0)
        d = a - b
        pct = (d / b * 100.0) if b > 0 else (100.0 if a > 0 else 0.0)
        meta = brand_meta.get(bid, {"brand_name": bid, "agency": None, "parent_company": None})
        rows.append({
            "brand_id":       bid,
            "brand_name":     meta.get("brand_name") or bid,
            "agency":         meta.get("agency"),
            "parent_company": meta.get("parent_company"),
            "before":         b,
            "after":          a,
            "delta":          d,
            "pct":            pct,
        })
    rows.sort(key=lambda r: (-abs(r["delta"]), -r["after"], r["brand_name"].lower()))
    return rows


# ==========================================================================
# Creative Production Tracker
# ==========================================================================

CREATIVE_SOURCES = ("PRODUCTION_TABLE", "COPILOT", "CREATIVE_AGENT", "OTHER")
CREATOR_TYPES = ("BRAND", "HAWKY", "AGENT", "UNATTRIBUTED")

# Map UI/URL filter tokens → stored enum values.
SOURCE_FILTER_MAP = {
    "production_table": "PRODUCTION_TABLE",
    "copilot": "COPILOT",
    "creative_agent": "CREATIVE_AGENT",
    "other": "OTHER",
}
CREATOR_FILTER_MAP = {
    "brand": "BRAND",
    "hawky": "HAWKY",
    "agent": "AGENT",
    "unattributed": "UNATTRIBUTED",
}


# --- Sync writers ---------------------------------------------------------

def replace_creatives(conn, rows):
    """
    Full resync: wipe and bulk-reinsert the creatives table in one transaction.
    `rows` is a list of dicts with keys matching the column names. ~4.6k rows
    insert in well under a second; this self-heals status changes, attribution
    backfills, and deletions without watermarking.
    """
    conn.execute("DELETE FROM creatives")
    if not rows:
        return 0
    cols = (
        "inventory_id", "hash", "brand_id", "source", "status",
        "created_at_utc", "ist_ts", "ist_date", "creator_email",
        "creator_type", "resolution", "table_id", "table_name",
        "agent_id", "agent_name", "run_session", "model_used", "creative_url",
    )
    placeholders = ",".join(["?"] * len(cols))
    conn.executemany(
        f"INSERT OR REPLACE INTO creatives ({','.join(cols)}) VALUES ({placeholders})",
        [tuple(r.get(c) for c in cols) for r in rows],
    )
    return len(rows)


def upsert_creative_brand(conn, brand_id, brand_name, is_on_roster):
    conn.execute(
        """INSERT INTO creative_brands(brand_id, brand_name, is_on_roster, updated_at)
           VALUES(?,?,?, CURRENT_TIMESTAMP)
           ON CONFLICT(brand_id) DO UPDATE SET
               brand_name   = excluded.brand_name,
               is_on_roster = excluded.is_on_roster,
               updated_at   = CURRENT_TIMESTAMP""",
        (brand_id, brand_name, 1 if is_on_roster else 0),
    )


# --- Sync run bookkeeping (mirrors runs CRUD) -----------------------------

def start_creative_sync(conn, kind):
    cur = conn.execute(
        "INSERT INTO creative_sync_runs(kind) VALUES(?)", (kind,),
    )
    return cur.lastrowid


def finish_creative_sync(conn, run_id, **fields):
    if not fields:
        conn.execute(
            "UPDATE creative_sync_runs SET finished_at=CURRENT_TIMESTAMP, status='ok' WHERE id=?",
            (run_id,),
        )
        return
    assigns = ", ".join(f"{k}=?" for k in fields)
    values = list(fields.values()) + [run_id]
    conn.execute(
        f"UPDATE creative_sync_runs SET finished_at=CURRENT_TIMESTAMP, status='ok', {assigns} WHERE id=?",
        values,
    )


def fail_creative_sync(conn, run_id, error):
    conn.execute(
        "UPDATE creative_sync_runs SET finished_at=CURRENT_TIMESTAMP, status='error', error=? WHERE id=?",
        (str(error)[:1000], run_id),
    )


def latest_creative_sync(conn):
    row = conn.execute(
        "SELECT * FROM creative_sync_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def latest_ok_creative_sync(conn):
    row = conn.execute(
        "SELECT * FROM creative_sync_runs WHERE status='ok' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def recent_creative_syncs(conn, n=10):
    rows = conn.execute(
        "SELECT * FROM creative_sync_runs ORDER BY id DESC LIMIT ?", (n,)
    ).fetchall()
    return [dict(r) for r in rows]


# --- Query helpers --------------------------------------------------------

def _creatives_where(ist_from, ist_to, source=None, creator_type=None, brand_id=None):
    """Build the shared WHERE fragment + params for creative queries."""
    clauses = ["ist_date BETWEEN ? AND ?"]
    params = [str(ist_from), str(ist_to)]
    if source:
        clauses.append("source = ?")
        params.append(source)
    if creator_type:
        clauses.append("creator_type = ?")
        params.append(creator_type)
    if brand_id:
        clauses.append("brand_id = ?")
        params.append(brand_id)
    return " AND ".join(clauses), params


# The CASE-sum column block reused by summary + per-brand + per-user queries.
_CREATIVE_SUMS = """
    COUNT(*) AS total,
    SUM(CASE WHEN source='PRODUCTION_TABLE' THEN 1 ELSE 0 END) AS src_production_table,
    SUM(CASE WHEN source='COPILOT'          THEN 1 ELSE 0 END) AS src_copilot,
    SUM(CASE WHEN source='CREATIVE_AGENT'   THEN 1 ELSE 0 END) AS src_creative_agent,
    SUM(CASE WHEN source='OTHER'            THEN 1 ELSE 0 END) AS src_other,
    SUM(CASE WHEN creator_type='BRAND'        THEN 1 ELSE 0 END) AS ct_brand,
    SUM(CASE WHEN creator_type='HAWKY'        THEN 1 ELSE 0 END) AS ct_hawky,
    SUM(CASE WHEN creator_type='AGENT'        THEN 1 ELSE 0 END) AS ct_agent,
    SUM(CASE WHEN creator_type='UNATTRIBUTED' THEN 1 ELSE 0 END) AS ct_unattributed
"""


def creatives_summary(conn, ist_from, ist_to, source=None, creator_type=None, brand_id=None):
    """
    Global (or brand-scoped) rollup for the KPI cards. Returns a flat dict with
    total, per-source, per-creator-type, plus distinct brand-user and
    hawky-user counts. Called twice by the route (current + previous window)
    for the period-over-period delta.
    """
    where, params = _creatives_where(ist_from, ist_to, source, creator_type, brand_id)
    r = conn.execute(
        f"""SELECT {_CREATIVE_SUMS},
               COUNT(DISTINCT CASE WHEN creator_type='BRAND' THEN creator_email END) AS brand_users,
               COUNT(DISTINCT CASE WHEN creator_type='HAWKY' THEN creator_email END) AS hawky_users
           FROM creatives WHERE {where}""",
        params,
    ).fetchone()
    return _row_to_summary(r)


def _row_to_summary(r):
    d = {k: (r[k] or 0) for k in (
        "total", "src_production_table", "src_copilot", "src_creative_agent",
        "src_other", "ct_brand", "ct_hawky", "ct_agent", "ct_unattributed",
    )}
    d["brand_users"] = (r["brand_users"] or 0) if "brand_users" in r.keys() else 0
    d["hawky_users"] = (r["hawky_users"] or 0) if "hawky_users" in r.keys() else 0
    return d


def creatives_by_brand(conn, ist_from, ist_to, source=None, creator_type=None):
    """{brand_id: {summary dict + last_activity}} for the overview table."""
    where, params = _creatives_where(ist_from, ist_to, source, creator_type)
    rows = conn.execute(
        f"""SELECT brand_id, {_CREATIVE_SUMS}, MAX(ist_ts) AS last_activity
            FROM creatives WHERE {where}
            GROUP BY brand_id""",
        params,
    ).fetchall()
    out = {}
    for r in rows:
        d = {k: (r[k] or 0) for k in (
            "total", "src_production_table", "src_copilot", "src_creative_agent",
            "src_other", "ct_brand", "ct_hawky", "ct_agent", "ct_unattributed",
        )}
        d["last_activity"] = r["last_activity"]
        out[r["brand_id"]] = d
    return out


def list_creative_brands(conn):
    """
    Every brand that has produced ≥1 creative, with display name + roster flag.
    Roster name (brands table) wins over the Mongo-derived name.
    """
    rows = conn.execute(
        """SELECT cb.brand_id,
                  COALESCE(b.brand_name, cb.brand_name, cb.brand_id) AS brand_name,
                  cb.is_on_roster,
                  b.agency, b.parent_company, b.instance_label
           FROM creative_brands cb
           LEFT JOIN brands b ON b.brand_id = cb.brand_id"""
    ).fetchall()
    return [dict(r) for r in rows]


def creative_brand_meta(conn, brand_id):
    """Display metadata for one brand (roster name wins, else Mongo name)."""
    row = conn.execute(
        """SELECT cb.brand_id,
                  COALESCE(b.brand_name, cb.brand_name, cb.brand_id) AS brand_name,
                  COALESCE(cb.is_on_roster, 0) AS is_on_roster,
                  b.agency, b.parent_company, b.instance_label
           FROM creative_brands cb
           LEFT JOIN brands b ON b.brand_id = cb.brand_id
           WHERE cb.brand_id = ?""",
        (brand_id,),
    ).fetchone()
    if row:
        return dict(row)
    # Brand may be on the roster but have no creatives yet.
    row = conn.execute(
        "SELECT brand_id, brand_name, agency, parent_company, instance_label FROM brands WHERE brand_id=?",
        (brand_id,),
    ).fetchone()
    if row:
        d = dict(row)
        d["is_on_roster"] = 1
        return d
    return None


def creatives_brand_users(conn, brand_id, ist_from, ist_to, source=None, creator_type=None):
    """
    Per-creator rollup for one brand. NULL creator_email collapses into a single
    synthetic '(unattributed)' row. Agent creatives (creator_type='AGENT') are
    excluded here — they're shown in the separate agents breakdown.
    """
    where, params = _creatives_where(ist_from, ist_to, source, creator_type, brand_id)
    rows = conn.execute(
        f"""SELECT COALESCE(creator_email, '') AS email,
                   MAX(creator_type) AS creator_type,
                   {_CREATIVE_SUMS},
                   MAX(ist_ts) AS last_active
            FROM creatives
            WHERE {where} AND creator_type != 'AGENT'
            GROUP BY COALESCE(creator_email, '')
            ORDER BY total DESC""",
        params,
    ).fetchall()
    out = []
    for r in rows:
        d = {k: (r[k] or 0) for k in (
            "total", "src_production_table", "src_copilot", "src_creative_agent",
            "src_other", "ct_brand", "ct_hawky", "ct_agent", "ct_unattributed",
        )}
        d["email"] = r["email"] or None
        d["creator_type"] = r["creator_type"]
        d["last_active"] = r["last_active"]
        out.append(d)
    return out


def creatives_brand_agents(conn, brand_id, ist_from, ist_to):
    """
    Per-agent breakdown for one brand (the AGENT bucket, expanded). Returns
    agent_name, creatives, runs (distinct run_session), creatives/run,
    last_run, and distinct table count.
    """
    rows = conn.execute(
        """SELECT agent_id,
                  MAX(agent_name) AS agent_name,
                  COUNT(*) AS creatives,
                  COUNT(DISTINCT run_session) AS runs,
                  COUNT(DISTINCT table_id) AS tables,
                  MAX(ist_ts) AS last_run
           FROM creatives
           WHERE brand_id = ? AND ist_date BETWEEN ? AND ? AND source = 'CREATIVE_AGENT'
           GROUP BY agent_id
           ORDER BY creatives DESC""",
        (brand_id, str(ist_from), str(ist_to)),
    ).fetchall()
    out = []
    for r in rows:
        runs = r["runs"] or 0
        creatives = r["creatives"] or 0
        out.append({
            "agent_id": r["agent_id"],
            "agent_name": r["agent_name"] or r["agent_id"] or "(unknown agent)",
            "creatives": creatives,
            "runs": runs,
            "tables": r["tables"] or 0,
            "per_run": round(creatives / runs, 1) if runs else creatives,
            "last_run": r["last_run"],
        })
    return out


def creatives_user_tables(conn, brand_id, ist_from, ist_to):
    """
    {creator_email_or_None: [{table_id, table_name, n, last}]} — production
    tables created by each user for this brand, powering "View N tables".
    """
    rows = conn.execute(
        """SELECT creator_email, table_id,
                  MAX(table_name) AS table_name,
                  COUNT(*) AS n,
                  MIN(ist_ts) AS first,
                  MAX(ist_ts) AS last,
                  COUNT(DISTINCT ist_date) AS days
           FROM creatives
           WHERE brand_id = ? AND ist_date BETWEEN ? AND ?
             AND source = 'PRODUCTION_TABLE' AND table_id IS NOT NULL
           GROUP BY creator_email, table_id
           ORDER BY n DESC""",
        (brand_id, str(ist_from), str(ist_to)),
    ).fetchall()
    out = {}
    for r in rows:
        out.setdefault(r["creator_email"], []).append({
            "table_id": r["table_id"],
            "table_name": r["table_name"],
            "n": r["n"],
            "first": r["first"],
            "last": r["last"],
            "days": r["days"],
        })
    return out
