FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Kolkata

# Install deps first for better layer cache.
COPY requirements.txt .
RUN pip install -r requirements.txt

# App code.
COPY . .

EXPOSE 8080

# CRITICAL: -w 1 (single worker). RefreshDaemon (data/refresh.py) uses
# threading.Timer at module import time; multiple workers would each spawn
# their own daemon and race on the SQLite file.
# --timeout 120 gives slow Mixpanel calls headroom (the request handler
# itself returns fast; this is belt-and-braces).
CMD ["gunicorn", "-w", "1", "-b", "0.0.0.0:8080", "--timeout", "120", \
     "--access-logfile", "-", "--error-logfile", "-", "app:app"]
