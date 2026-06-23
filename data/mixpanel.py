import json
import time
from datetime import date, timedelta

import requests
from requests.auth import HTTPBasicAuth


class MixpanelClient:
    def __init__(self, project_id, username, secret, export_url, engage_url):
        self.project_id = project_id
        self.auth = HTTPBasicAuth(username, secret)
        self.export_url = export_url
        self.engage_url = engage_url

    # ----- Export ----------------------------------------------------------

    def export_events(self, from_date: date, to_date: date, max_fallback_hops: int = 2):
        """
        Returns (events_iter, actual_from, actual_to, fallback_hops).
        Mixpanel returns 400 if to_date is ahead of its clock (today's events
        not yet materialized). Walk to_date back day-by-day up to
        max_fallback_hops, then surface the actual window.
        """
        hops = 0
        cur_to = to_date
        last_err = None
        while hops <= max_fallback_hops:
            params = {
                "from_date": str(from_date),
                "to_date": str(cur_to),
                "project_id": self.project_id,
            }
            resp = requests.get(
                self.export_url, params=params, auth=self.auth,
                stream=True, timeout=180,
            )
            if resp.status_code == 200:
                return self._iter_ndjson(resp), from_date, cur_to, hops
            if resp.status_code == 400:
                last_err = resp.text[:300]
                hops += 1
                cur_to = cur_to - timedelta(days=1)
                if cur_to < from_date:
                    break
                continue
            raise RuntimeError(f"Export API {resp.status_code}: {resp.text[:300]}")
        raise RuntimeError(
            f"Export API rejected window after {hops} fallback hop(s). "
            f"Last error: {last_err}"
        )

    @staticmethod
    def _iter_ndjson(resp):
        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            try:
                yield json.loads(raw)
            except json.JSONDecodeError:
                continue

    # ----- Engage ----------------------------------------------------------

    def engage_profiles(self, page_size=1000, max_pages=500):
        """
        Yields raw People profile dicts. Mixpanel Engage pagination:
        first call returns session_id+page=0+results; subsequent calls send
        session_id and page+=1. Loop until results empty or session expires.
        """
        session_id = None
        page = 0
        for _ in range(max_pages):
            params = {"project_id": self.project_id}
            if session_id is not None:
                params["session_id"] = session_id
                params["page"] = page
            resp = requests.post(
                self.engage_url, params=params, auth=self.auth, timeout=90,
            )
            if resp.status_code != 200:
                raise RuntimeError(f"Engage API {resp.status_code}: {resp.text[:300]}")
            data = resp.json()
            results = data.get("results") or []
            for prof in results:
                yield prof
            session_id = data.get("session_id")
            returned = len(results)
            page_size_resp = int(data.get("page_size") or page_size)
            if not session_id or returned == 0 or returned < page_size_resp:
                break
            page = int(data.get("page", page)) + 1
            time.sleep(0.15)  # gentle on rate limit
