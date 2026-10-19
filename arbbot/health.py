"""Port handling for container hosts (Render, Fly, ...). The hosted app itself lives in hosted_dashboard.py."""
import os

DEFAULT_PORT = 10000  # Render's default; the platform normally sets PORT itself


def hosted_port() -> int:
    raw = os.environ.get("PORT", "")
    try:
        port = int(raw) if raw else DEFAULT_PORT
    except ValueError:
        raise SystemExit(f"PORT must be a number, got {raw!r}")
    if not 1 <= port <= 65535:
        raise SystemExit(f"PORT out of range: {port}")
    return port
