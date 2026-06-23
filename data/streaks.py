from datetime import date, timedelta

from .ist import is_weekend, ist_today


def calc_streak(active_dates, today: date | None = None, weekend_days=None) -> int:
    """
    Consecutive IST weekdays with activity, walked backward from `today`.
    Weekends are skipped entirely (never count, never break a streak).
    Today counts iff it is in `active_dates`; otherwise the streak ends at the
    most recent prior weekday with activity.
    """
    today = today or ist_today()
    active = {d if isinstance(d, date) else date.fromisoformat(d) for d in active_dates}

    cur = today
    # If today is a weekday with no activity yet, walk back to yesterday so the
    # streak isn't artificially clipped.
    if not is_weekend(cur, weekend_days) and cur not in active:
        cur = cur - timedelta(days=1)

    streak = 0
    safety = 0
    while safety < 400:
        safety += 1
        if is_weekend(cur, weekend_days):
            cur = cur - timedelta(days=1)
            continue
        if cur in active:
            streak += 1
            cur = cur - timedelta(days=1)
        else:
            break
    return streak
