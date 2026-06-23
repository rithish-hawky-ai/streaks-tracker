"""
Creative Production Tracker — MongoDB → SQLite sync.

Pulls the Hawky production `inventories` collection (one doc per creative) into
the local `creatives` table, classifying each into a 4-way creator bucket and
mapping the Mongo `source` enum to our surface enum.

This module is the ONLY place pymongo is imported, so a Mongo outage can never
break the Mixpanel pipeline (data/refresh.py stays pymongo-free). The sync is a
full delete+reinsert resync — at ~4.6k docs it runs in well under a second and
self-heals status changes, attribution backfills, and deletions without any
watermarking.

Classification (locked with PM):
  1. source == 'agent_run'           -> AGENT  (automated, no human in the loop)
  2. else real creator email found   -> HAWKY if email contains "hawky" else BRAND
  3. else                            -> UNATTRIBUTED
"""

import logging
from datetime import datetime

from . import store
from .ist import utc_dt_to_ist_datetime

log = logging.getLogger(__name__)

# Mongo `source` enum -> our surface enum.
SOURCE_MAP = {
    "production_table": "PRODUCTION_TABLE",
    "chat": "COPILOT",
    "agent_run": "CREATIVE_AGENT",
    "manual_upload": "OTHER",
}


# --- value cleaning -------------------------------------------------------
# This collection was migrated; missing values often arrive as the literal
# string "None" (not a real null). Treat "None"/""/null uniformly as absent.

def _clean_str(v):
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() == "none":
        return None
    return s


def is_real_email(v):
    return isinstance(v, str) and "@" in v


def normalize_email(v):
    """Lowercase, strip whitespace, drop +alias. Returns None if not an email."""
    if not is_real_email(v):
        return None
    e = v.strip().lower()
    local, _, domain = e.partition("@")
    local = local.split("+", 1)[0]
    if not local or not domain:
        return None
    return f"{local}@{domain}"


def classify_creator(email):
    """Real email -> HAWKY (contains 'hawky') else BRAND. None -> UNATTRIBUTED."""
    if not email:
        return "UNATTRIBUTED"
    return "HAWKY" if "hawky" in email else "BRAND"


def resolve_email(doc, users_by_id):
    """
    Email waterfall for non-agent creatives:
      createdByEmail (real @) -> users[createdBy].email (real @) -> None.
    Returns (normalized_email_or_None, resolution_label).
    """
    raw = doc.get("createdByEmail")
    if is_real_email(raw):
        return normalize_email(raw), "direct_email"
    cb = doc.get("createdBy")
    cb_key = _clean_str(cb)
    if cb_key:
        em = users_by_id.get(cb_key)
        if is_real_email(em):
            return normalize_email(em), "user_join"
    return None, "unresolved"


def _parse_created_at(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _build_row(doc, users_by_id, agents_by_id):
    """Map one inventories doc to a creatives row dict, or None to skip."""
    dt = _parse_created_at(doc.get("createdAt"))
    if dt is None:
        return None
    ist_dt = utc_dt_to_ist_datetime(dt)

    raw_source = _clean_str(doc.get("source"))
    source = SOURCE_MAP.get(raw_source, "OTHER")

    # 4-way creator classification.
    if source == "CREATIVE_AGENT":
        creator_email, creator_type, resolution = None, "AGENT", "agent_rule"
    else:
        creator_email, resolution = resolve_email(doc, users_by_id)
        creator_type = classify_creator(creator_email)

    agent_id = _clean_str(doc.get("agentId"))
    agent_name = _clean_str(agents_by_id.get(agent_id)) if agent_id else None

    url = doc.get("url")
    creative_url = None
    if isinstance(url, dict):
        creative_url = _clean_str(url.get("creativeUrl"))
    else:
        creative_url = _clean_str(url)

    return {
        "inventory_id": str(doc["_id"]),
        "hash": _clean_str(doc.get("hash")),
        "brand_id": str(doc.get("brandId")) if doc.get("brandId") is not None else None,
        "source": source,
        "status": _clean_str(doc.get("status")),
        "created_at_utc": dt.isoformat(),
        "ist_ts": ist_dt.isoformat(),
        "ist_date": str(ist_dt.date()),
        "creator_email": creator_email,
        "creator_type": creator_type,
        "resolution": resolution,
        "table_id": _clean_str(doc.get("tableId")),
        "table_name": _clean_str(doc.get("tableName")),
        "agent_id": agent_id,
        "agent_name": agent_name,
        "run_session": _clean_str(doc.get("chatId")),
        "model_used": _clean_str(doc.get("modelUsed")),
        "creative_url": creative_url,
    }


def fetch_mongo_data(mongo_uri, db_name):
    """
    Read inventories + the small dimension maps. MongoClient is created and
    closed per call (daemon-thread safe; no stale global connection).
    Returns (docs, users_by_id, agents_by_id, brand_names).
    """
    from pymongo import MongoClient  # local import keeps pymongo out of refresh.py

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    try:
        db = client[db_name]
        inv_proj = {
            "_id": 1, "hash": 1, "brandId": 1, "createdAt": 1, "source": 1,
            "status": 1, "modelUsed": 1, "url": 1, "tableId": 1, "tableName": 1,
            "createdBy": 1, "createdByEmail": 1, "agentId": 1, "chatId": 1,
        }
        docs = list(db.inventories.find({}, inv_proj))
        users_by_id = {
            str(u["_id"]): u.get("email")
            for u in db.users.find({}, {"email": 1})
        }
        agents_by_id = {
            str(a["_id"]): a.get("name")
            for a in db.agents.find({}, {"name": 1})
        }
        brand_names = {}
        for b in db.brands.find({}, {"coreIdentity.brandName": 1}):
            ci = b.get("coreIdentity")
            nm = ci.get("brandName") if isinstance(ci, dict) else None
            brand_names[str(b["_id"])] = _clean_str(nm)
        return docs, users_by_id, agents_by_id, brand_names
    finally:
        client.close()


def run_creatives_sync(db_path, mongo_uri, db_name="test", *, kind="auto"):
    """
    Full end-to-end sync. Records a creative_sync_runs row. Raises on failure
    (caller isolates), but always marks the run as errored first.
    """
    if not mongo_uri:
        log.info("MONGO_URI unset — skipping creatives sync.")
        return {"skipped": True}

    with store.db(db_path) as conn:
        run_id = store.start_creative_sync(conn, kind)
        roster = set(store.brand_ids(conn, active_only=False))

    try:
        docs, users_by_id, agents_by_id, brand_names = fetch_mongo_data(mongo_uri, db_name)

        rows = []
        unattributed = 0
        brand_seen = {}
        for doc in docs:
            try:
                row = _build_row(doc, users_by_id, agents_by_id)
            except Exception:
                log.warning("Skipping malformed inventory doc %s", doc.get("_id"), exc_info=True)
                continue
            if row is None or not row["brand_id"]:
                continue
            rows.append(row)
            if row["creator_type"] == "UNATTRIBUTED":
                unattributed += 1
            bid = row["brand_id"]
            if bid not in brand_seen:
                brand_seen[bid] = brand_names.get(bid)

        with store.db(db_path) as conn:
            n = store.replace_creatives(conn, rows)
            for bid, mongo_name in brand_seen.items():
                store.upsert_creative_brand(conn, bid, mongo_name, is_on_roster=bid in roster)
            store.finish_creative_sync(
                conn, run_id,
                docs_seen=len(docs), docs_upserted=n, unattributed=unattributed,
            )
        log.info(
            "Creatives sync ok: %d docs seen, %d stored, %d unattributed.",
            len(docs), n, unattributed,
        )
        return {"docs_seen": len(docs), "docs_upserted": n, "unattributed": unattributed}
    except Exception as e:
        with store.db(db_path) as conn:
            store.fail_creative_sync(conn, run_id, e)
        log.exception("Creatives sync failed")
        raise
