from datetime import date, datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))


def utc_ts_to_ist_datetime(ts: float) -> datetime:
    return datetime.fromtimestamp(float(ts), tz=IST)


def utc_ts_to_ist_date(ts: float) -> date:
    return utc_ts_to_ist_datetime(ts).date()


def utc_dt_to_ist_datetime(dt: datetime) -> datetime:
    """
    Convert a datetime to IST. Naive datetimes are assumed UTC (pymongo returns
    naive-UTC datetimes by default), so we attach UTC before converting.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST)


def ist_now() -> datetime:
    return datetime.now(IST)


def ist_today() -> date:
    return ist_now().date()


def is_weekend(d: date, weekend_days=None) -> bool:
    days = weekend_days if weekend_days is not None else {5, 6}
    return d.weekday() in days


def last_n_dates_ist(n: int, end: date | None = None):
    end = end or ist_today()
    return [end - timedelta(days=i) for i in reversed(range(n))]


def ist_to_utc_export_window(ist_from: date, ist_to: date):
    """
    The Export API is UTC-date-granular. To capture IST events for [ist_from, ist_to],
    fetch UTC [ist_from - 1d, ist_to] and re-bucket each event by its IST timestamp in code.
    """
    return ist_from - timedelta(days=1), ist_to
