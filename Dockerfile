# Paper-mode arbbot for container hosts (Render, Fly, ...). Serves a read-only dashboard behind a login
# (DASHBOARD_PASSWORD) plus /healthz; never the local dashboard with its pause/flatten controls.
FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY run.py config.toml ./
COPY arbbot ./arbbot
COPY web ./web
COPY vercel-dashboard/views ./vercel-dashboard/views
RUN mkdir -p data logs

# Runs as root on purpose: a persistent disk mounted at /app/data is root-owned on most hosts, and a non-root user
# would then fail to write the database. The process only makes outbound connections and answers /healthz.
# --hosted is paper-only; real orders are refused in this mode.
# --max-markets 25: Render's free instance is capped at 0.15 vCPU and was pinned at 100% scanning all 55 markets, which
# slowed order-book fetches until signals expired. To undo: delete "--max-markets", "25" (config.toml's 55 then applies).
CMD ["python", "run.py", "--hosted", "--min-samples", "20", "--max-markets", "25"]
