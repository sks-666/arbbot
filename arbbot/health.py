"""Health-check endpoint for container hosts (Render, Fly, ...), used instead of the local dashboard.

The local dashboard has control endpoints (pause, flatten), so it is never bound to a public interface. Hosted mode
serves only this: a fixed reply with no market, position or account data. It exists so the platform can see the
process is alive and (on free plans) so an uptime pinger has something to request.
"""
import os

from aiohttp import web

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


def make_health_app() -> web.Application:
    async def ok(_request: web.Request) -> web.Response:
        return web.json_response({"ok": True}, headers={"Cache-Control": "no-store"})

    app = web.Application()
    app.router.add_get("/", ok)
    app.router.add_get("/healthz", ok)
    return app
