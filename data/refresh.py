import logging
import threading
from datetime import timedelta

from . import store
from .filters import is_hawky_email
from .ist import ist_today, ist_to_utc_export_window, utc_ts_to_ist_datetime
from .modules import attribute_event


def cleanup_hawky_events(db_path, hawky_domains, hawky_substrings):
    """
    Retroactive cleanup: delete events for any distinct_id whose profile email
    now matches the Hawky filter (post-rule change). Returns
    (events_deleted, distinct_ids_dropped, sample_emails).
    """
    hawky_domains = set(hawky_domains or set())
    hawky_substrings = tuple(hawky_substrings or ())
    with store.db(db_path) as conn:
        allow, deny = store.get_exclusions(conn)
        rules = store.list_rules(conn)
        profiles = conn.execute(
            "SELECT distinct_id, email FROM profiles WHERE email IS NOT NULL"
        ).fetchall()
        bad_ids = []
        sample = []
        for p in profiles:
            email = (p["email"] or "").strip().lower()
            if email and is_hawky_email(email, hawky_domains, allow, deny, hawky_substrings, rules):
                bad_ids.append(p["distinct_id"])
                if len(sample) < 10:
                    sample.append(email)
        deleted = 0
        if bad_ids:
            # batch DELETE in chunks (SQLite IN-list limit safety)
            for i in range(0, len(bad_ids), 500):
                chunk = bad_ids[i:i + 500]
                qs = ",".join(["?"] * len(chunk))
                cur = conn.execute(
                    f"DELETE FROM events WHERE distinct_id IN ({qs})", chunk
                )
                deleted += cur.rowcount or 0
        return deleted, len(bad_ids), sample

log = logging.getLogger(__name__)


def _dedup_key(props, event_name, distinct_id, ist_ts, current_url):
    iid = props.get("$insert_id")
    if iid:
        return f"id:{iid}"
    safe_url = (current_url or "")[:120]
    return f"k:{event_name}|{distinct_id}|{ist_ts}|{safe_url}"


def run_refresh(db_path, mp_client, *, kind="auto", ist_to=None,
                day_window=21, hawky_domains=None, hawky_substrings=None,
                snapshot=False, snapshot_label=None, snapshot_cap=30):
    """
    Single end-to-end refresh:
      1. Fetch People profiles (for email+domain join).
      2. Stream Export events across the IST window.
      3. Bucket by IST date, double-attribute Co-Pilot, drop Hawky/off-roster.
      4. Persist counters to runs table. Optionally write a snapshot.
    """
    ist_to = ist_to or ist_today()
    ist_from = ist_to - timedelta(days=day_window - 1)
    utc_from, utc_to = ist_to_utc_export_window(ist_from, ist_to)
    hawky_domains = set(hawky_domains or set())
    hawky_substrings = tuple(hawky_substrings or ())

    # --- Phase A: profiles ------------------------------------------------
    with store.db(db_path) as conn:
        run_id = store.start_run(conn, kind, ist_from, ist_to)
        roster = set(store.brand_ids(conn, active_only=True))
        allow, deny = store.get_exclusions(conn)
        rules = store.list_rules(conn)

    profiles_by_distinct = {}
    try:
        for prof in mp_client.engage_profiles():
            distinct = prof.get("$distinct_id") or prof.get("distinct_id")
            if not distinct:
                continue
            props = prof.get("$properties") or prof.get("properties") or {}
            raw_email = props.get("$email") or props.get("email") or ""
            email = raw_email.strip().lower() or None
            brand_id_p = props.get("brand_id") or props.get("$brand_id")
            last_seen = props.get("$last_seen")
            profiles_by_distinct[distinct] = {"email": email, "brand_id": brand_id_p}
            with store.db(db_path) as conn:
                store.upsert_profile(conn, distinct, email, brand_id_p, props, last_seen)
    except Exception as e:
        with store.db(db_path) as conn:
            store.fail_run(conn, run_id, e)
        log.exception("Engage fetch failed")
        raise

    # --- Phase B: events --------------------------------------------------
    counters = dict(
        events_seen=0, events_kept=0,
        dropped_no_brand_id=0, dropped_not_on_roster=0,
        dropped_no_profile=0, dropped_hawky_excluded=0,
        dropped_no_time=0, dropped_out_of_window=0,
    )
    try:
        events_iter, actual_from, actual_to, hops = mp_client.export_events(utc_from, utc_to)
        with store.db(db_path) as conn:
            for ev in events_iter:
                counters["events_seen"] += 1
                name = ev.get("event")
                props = ev.get("properties") or {}
                ts = props.get("time")
                if ts is None:
                    counters["dropped_no_time"] += 1
                    continue
                ist_dt = utc_ts_to_ist_datetime(ts)
                ist_d = ist_dt.date()
                if ist_d < ist_from or ist_d > ist_to:
                    counters["dropped_out_of_window"] += 1
                    continue
                brand_id = props.get("brand_id")
                if not brand_id:
                    counters["dropped_no_brand_id"] += 1
                    continue
                if brand_id not in roster:
                    counters["dropped_not_on_roster"] += 1
                    continue
                distinct_id = props.get("distinct_id") or props.get("$distinct_id")
                prof = profiles_by_distinct.get(distinct_id) if distinct_id else None
                if prof is None:
                    counters["dropped_no_profile"] += 1
                    continue
                email = prof.get("email")
                if email and is_hawky_email(email, hawky_domains, allow, deny, hawky_substrings, rules):
                    counters["dropped_hawky_excluded"] += 1
                    continue
                current_url = props.get("$current_url") or props.get("current_url")
                modules, sub_page = attribute_event(name, current_url)
                ist_ts = ist_dt.isoformat()
                store.insert_event(
                    conn,
                    dedup_key=_dedup_key(props, name, distinct_id, ist_ts, current_url),
                    event_name=name,
                    distinct_id=distinct_id,
                    brand_id=brand_id,
                    ist_date=str(ist_d),
                    ist_ts=ist_ts,
                    current_url=current_url,
                    modules=modules,
                    sub_page=sub_page,
                )
                counters["events_kept"] += 1
            store.finish_run(
                conn, run_id,
                actual_from=str(actual_from), actual_to=str(actual_to),
                fallback_hops=hops,
                profiles_fetched=len(profiles_by_distinct),
                **counters,
            )

            if snapshot:
                agg = store.events_by_brand_day(conn, ist_from, ist_to)
                store.insert_snapshot(
                    conn,
                    label=snapshot_label or f"manual @ {actual_to}",
                    run_id=run_id,
                    payload={
                        "ist_from": str(ist_from),
                        "ist_to": str(ist_to),
                        "actual_from": str(actual_from),
                        "actual_to": str(actual_to),
                        "counters": counters,
                        "by_brand_day": agg,
                    },
                )
                store.prune_snapshots(conn, snapshot_cap)
    except Exception as e:
        with store.db(db_path) as conn:
            store.fail_run(conn, run_id, e)
        log.exception("Export fetch failed")
        raise

    # Belt-and-braces: re-apply Hawky filter against the profiles table.
    # Catches events whose profile email was null at fetch time but is now
    # populated, plus any change to hawky_substrings / hawky_domains made
    # since the previous refresh.
    try:
        deleted, dropped_ids, _ = cleanup_hawky_events(
            db_path, hawky_domains, hawky_substrings,
        )
        if deleted:
            log.info(
                "Post-refresh Hawky cleanup: removed %d events from %d profile(s).",
                deleted, dropped_ids,
            )
            with store.db(db_path) as conn:
                # Adjust the run record so events_kept reflects truth.
                conn.execute(
                    "UPDATE runs SET events_kept = events_kept - ?, "
                    "dropped_hawky_excluded = dropped_hawky_excluded + ? "
                    "WHERE id = ?",
                    (deleted, deleted, run_id),
                )
    except Exception:
        log.exception("Post-refresh Hawky cleanup failed (non-fatal)")

    return run_id, counters


class RefreshDaemon:
    """Background thread that fires run_refresh() every N minutes (configurable)."""

    def __init__(self, db_path, mp_client, get_interval_min, get_day_window,
                 get_hawky_domains, get_hawky_substrings=lambda: (),
                 extra_jobs=()):
        self.db_path = db_path
        self.mp_client = mp_client
        self.get_interval_min = get_interval_min
        self.get_day_window = get_day_window
        self.get_hawky_domains = get_hawky_domains
        self.get_hawky_substrings = get_hawky_substrings
        # Extra side jobs (e.g. the Mongo creatives sync) run after the main
        # Mixpanel refresh, each isolated so one failing never affects the
        # others or the core refresh.
        self.extra_jobs = list(extra_jobs)
        self._timer = None
        self._lock = threading.Lock()
        self._running = False
        # Last interval that read cleanly (seconds); used as the fallback when a
        # settings read fails so a transient DB hiccup can't stall the daemon.
        self._last_interval = 900  # 15 min, matches DEFAULT_REFRESH_MIN

    def _tick(self):
        try:
            try:
                run_refresh(
                    self.db_path, self.mp_client,
                    kind="auto",
                    day_window=int(self.get_day_window()),
                    hawky_domains=set(self.get_hawky_domains()),
                    hawky_substrings=tuple(self.get_hawky_substrings()),
                    snapshot=False,
                )
            except Exception as e:
                log.warning("Auto refresh failed: %s", e)
            for job in self.extra_jobs:
                try:
                    job()
                except Exception as e:
                    log.warning("Extra refresh job failed: %s", e)
        finally:
            # Always reschedule, even if the body raised — a transient error
            # (e.g. a momentary DB hiccup) must never silently kill the daemon.
            self._schedule_next()

    def _schedule_next(self):
        if not self._running:
            return
        try:
            interval = max(60, int(self.get_interval_min()) * 60)
            self._last_interval = interval
        except Exception as e:
            # Reading the interval hits the settings DB; on a transient failure
            # fall back to the last good interval so refreshes keep firing.
            interval = self._last_interval
            log.warning("Could not read refresh interval (%s); using %ss", e, interval)
        t = threading.Timer(interval, self._tick)
        t.daemon = True
        t.start()
        self._timer = t

    def start(self, initial_delay=60):
        with self._lock:
            if self._running:
                return
            self._running = True
        t = threading.Timer(initial_delay, self._tick)
        t.daemon = True
        t.start()
        self._timer = t

    def stop(self):
        with self._lock:
            self._running = False
            if self._timer:
                self._timer.cancel()
