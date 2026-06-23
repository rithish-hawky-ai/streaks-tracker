import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BASE_DIR = Path(__file__).resolve().parent
# DB path is env-overridable so the Fly.io volume mount (`/data/streaks.db`)
# is honored without code changes. Local dev keeps the project-root default.
DB_PATH = Path(os.environ.get("STREAKS_DB_PATH") or (BASE_DIR / "streaks.db"))
SEED_PATH = BASE_DIR / "seed" / "brands.csv"

# Mixpanel service-account credentials. NEVER commit values here.
# Local dev: put them in a `.env` file (gitignored).
# Production: `fly secrets set MIXPANEL_PROJECT_ID=… …`.
MIXPANEL_PROJECT_ID = os.environ.get("MIXPANEL_PROJECT_ID")
MIXPANEL_USERNAME = os.environ.get("MIXPANEL_USERNAME")
MIXPANEL_SECRET = os.environ.get("MIXPANEL_SECRET")

assert all([MIXPANEL_PROJECT_ID, MIXPANEL_USERNAME, MIXPANEL_SECRET]), (
    "Missing Mixpanel credentials. Set MIXPANEL_PROJECT_ID, MIXPANEL_USERNAME, "
    "and MIXPANEL_SECRET via .env (local) or `fly secrets set` (prod). "
    "See .env.example for the keys."
)

EXPORT_URL = "https://data.mixpanel.com/api/2.0/export"
ENGAGE_URL = "https://mixpanel.com/api/2.0/engage"

# Creative Production Tracker — MongoDB source (the Hawky production DB).
# OPTIONAL: when MONGO_URI is unset the app still boots and the Mixpanel
# pipeline runs normally; the creatives sync simply no-ops. NEVER commit the
# URI here — set it via .env (local) or `fly secrets set MONGO_URI=…` (prod).
MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DB = os.environ.get("MONGO_DB", "test")
# Product base URL for "View N tables" deep links. Editable on /config.
DEFAULT_PRODUCT_BASE_URL = os.environ.get(
    "PRODUCT_BASE_URL", "https://staging.hawky.ai",
)


DEFAULT_REFRESH_MIN = 15
DEFAULT_DAY_WINDOW = 21
DEFAULT_WEEKEND_DAYS = "5,6"  # Python weekday(): 5=Sat, 6=Sun
DEFAULT_TIMEZONE = "Asia/Kolkata"
DEFAULT_HAWKY_DOMAINS = "hawky.ai,hawky.in,hawkyai.com"
DEFAULT_HAWKY_SUBSTRINGS = "hawky"  # catches hawky.ai@gmail.com & similar non-domain cases
DEFAULT_COPILOT_HEAVY = 30
DEFAULT_COPILOT_MODERATE = 15
DEFAULT_COPILOT_MESSAGE_EVENT = "copilot_message_sent"  # canonical adoption signal
DEFAULT_SNAPSHOT_CAP = 30
DEFAULT_INITIAL_REFRESH_DELAY = 60  # seconds before the first auto refresh fires
