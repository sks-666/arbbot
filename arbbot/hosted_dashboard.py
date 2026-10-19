"""Read-only dashboard for container hosts (Render, ...): the same page and sanitised data as the Vercel copy.

Served straight from the running bot, so it needs no data store. There are NO control endpoints (no pause, no flatten).
It fails CLOSED like the Vercel version: without DASHBOARD_PASSWORD (at least 12 characters) every route except the
health check answers 503; with it, HTTP Basic auth is required, compared in constant time, failures are delayed, and a
global cap on failed attempts per minute makes guessing hopeless. What is sent is `public_snapshot()`: the same
reduced view the bot publishes to Upstash (no keys, no raw error text, no control state).
"""
import asyncio
import base64
import binascii
import hashlib
import hmac
import os
import time
from pathlib import Path

from aiohttp import web

from .publisher import public_snapshot

MIN_PASSWORD = 12
MAX_FAILS_PER_MIN = 30
CSP = ("default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; "
       "img-src data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
SECURITY_HEADERS = {
    "Cache-Control": "no-store",
    "X-Robots-Tag": "noindex, nofollow",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
}


def _same(a: str, b: str) -> bool:
    """Constant-time string compare (equal-length digests, so length is not leaked either)."""
    return hmac.compare_digest(hashlib.sha256(a.encode()).digest(), hashlib.sha256(b.encode()).digest())


def credentials_ok(header: str, user: str, password: str) -> bool:
    if not header.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(header[6:], validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return False
    given_user, sep, given_password = decoded.partition(":")  # the password itself may contain ':'
    if not sep:
        return False
    return _same(user, given_user) & _same(password, given_password)  # '&', not 'and': always compare both


def settings_from_env() -> tuple[str, str]:
    return os.environ.get("DASHBOARD_PASSWORD", ""), os.environ.get("DASHBOARD_USER", "") or "viewer"


def make_hosted_app(engine, html_path: Path, password: str = "", user: str = "viewer",
                    fail_delay_s: float = 0.4) -> web.Application:
    password, user = password or "", user or "viewer"
    configured = len(password) >= MIN_PASSWORD
    fails: list[float] = []  # monotonic times of recent failed logins (global, not per client)

    @web.middleware
    async def guard(request: web.Request, handler):
        if request.path == "/healthz":  # liveness only: a fixed reply, no data
            return await handler(request)
        if not configured:
            return web.Response(status=503, headers=SECURITY_HEADERS, text=(
                f"Dashboard is not configured: set DASHBOARD_PASSWORD (at least {MIN_PASSWORD} characters)."))
        now = time.monotonic()
        fails[:] = [t for t in fails if now - t < 60]
        if len(fails) >= MAX_FAILS_PER_MIN:
            return web.Response(status=429, headers={**SECURITY_HEADERS, "Retry-After": "60"},
                                text="Too many failed attempts. Try again in a minute.")
        if not credentials_ok(request.headers.get("Authorization", ""), user, password):
            fails.append(now)
            await asyncio.sleep(fail_delay_s)  # slow online guessing
            return web.Response(status=401, text="Authentication required",
                                headers={**SECURITY_HEADERS, "WWW-Authenticate": 'Basic realm="arbbot", charset="UTF-8"'})
        response = await handler(request)
        for k, v in SECURITY_HEADERS.items():
            response.headers.setdefault(k, v)
        return response

    async def healthz(_request: web.Request) -> web.Response:
        return web.json_response({"ok": True}, headers={"Cache-Control": "no-store"})

    async def index(_request: web.Request) -> web.Response:
        try:
            html = Path(html_path).read_text(encoding="utf-8")
        except OSError:
            return web.Response(status=500, text="Dashboard page is missing.")
        return web.Response(text=html, content_type="text/html", headers={"Content-Security-Policy": CSP})

    async def snapshot(_request: web.Request) -> web.Response:
        return web.json_response({"snapshot": public_snapshot(engine.snapshot()), "server_time": time.time()})

    app = web.Application(middlewares=[guard])
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/", index)
    app.router.add_get("/api/snapshot", snapshot)
    return app
