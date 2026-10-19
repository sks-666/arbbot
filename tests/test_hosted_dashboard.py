"""The read-only dashboard served from the hosted bot: fails closed, needs the login, exposes nothing but the page and data."""
import base64
import json
import tempfile
import unittest
from pathlib import Path

import aiohttp
from aiohttp import web

from arbbot.hosted_dashboard import MAX_FAILS_PER_MIN, credentials_ok, make_hosted_app
from tests.test_model import engine, warm

PASSWORD = "correct-horse-battery-9"
PAGE = "<html><body>READ-ONLY PAGE</body></html>"


def basic(user: str, password: str) -> dict:
    return {"Authorization": "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()}


class CredentialTests(unittest.TestCase):
    def test_header_parsing(self):
        good = basic("viewer", PASSWORD)["Authorization"]
        self.assertTrue(credentials_ok(good, "viewer", PASSWORD))
        self.assertFalse(credentials_ok(good, "viewer", PASSWORD + "x"))
        self.assertFalse(credentials_ok(good, "someone", PASSWORD))
        self.assertTrue(credentials_ok(basic("viewer", "a:b:" + PASSWORD)["Authorization"], "viewer", "a:b:" + PASSWORD))
        for junk in ("", "Bearer abc", "Basic", "Basic !!!notbase64", "Basic " + base64.b64encode(b"nocolon").decode()):
            self.assertFalse(credentials_ok(junk, "viewer", PASSWORD), junk)


class HostedDashboardTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.eng, self.m, self.pf, self.cfg = engine()
        await warm(self.eng, self.m)
        self.dir = tempfile.mkdtemp()
        self.page = Path(self.dir) / "dashboard.html"
        self.page.write_text(PAGE, encoding="utf-8")
        self.runners: list[web.AppRunner] = []
        self.session = aiohttp.ClientSession()

    async def asyncTearDown(self):
        await self.session.close()
        for r in self.runners:
            await r.cleanup()

    async def serve(self, password=PASSWORD, user="viewer") -> str:
        app = make_hosted_app(self.eng, self.page, password, user, fail_delay_s=0)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        self.runners.append(runner)
        return f"http://127.0.0.1:{runner.addresses[0][1]}"

    async def test_fails_closed_without_a_proper_password(self):
        for pw in ("", "short"):
            base = await self.serve(password=pw)
            for path in ("/", "/api/snapshot"):
                async with self.session.get(base + path, headers=basic("viewer", pw)) as r:
                    self.assertEqual(r.status, 503, f"{path} must not be served without a configured password")
                    self.assertIn("not configured", await r.text())
            async with self.session.get(base + "/healthz") as r:  # liveness stays open and carries no data
                self.assertEqual((r.status, await r.json()), (200, {"ok": True}))

    async def test_login_is_required_and_wrong_credentials_get_nothing(self):
        base = await self.serve()
        for headers in ({}, basic("viewer", "wrong-password-123"), basic("admin", PASSWORD)):
            for path in ("/", "/api/snapshot"):
                async with self.session.get(base + path, headers=headers) as r:
                    self.assertEqual(r.status, 401)
                    self.assertIn("Basic", r.headers["WWW-Authenticate"])
                    self.assertNotIn("READ-ONLY PAGE", await r.text())

    async def test_page_and_snapshot_with_the_right_login(self):
        base = await self.serve()
        auth = basic("viewer", PASSWORD)
        async with self.session.get(base + "/", headers=auth) as r:
            self.assertEqual(r.status, 200)
            self.assertEqual(await r.text(), PAGE)
            self.assertIn("default-src 'none'", r.headers["Content-Security-Policy"])
            self.assertEqual(r.headers["Cache-Control"], "no-store")
            self.assertIn("noindex", r.headers["X-Robots-Tag"])
        async with self.session.get(base + "/api/snapshot", headers=auth) as r:
            body = await r.json()
            self.assertEqual(r.status, 200)
            snap = body["snapshot"]
            self.assertIn("server_time", body)
            for needed in ("markets", "positions", "trades", "events", "stats", "model", "published_at", "schema"):
                self.assertIn(needed, snap)
            self.assertEqual(set(snap["halt"]), {"halted", "reason", "trading"})  # same reduced view as the Upstash copy
            self.assertNotIn("last_error", snap["feed"])
            self.assertNotIn(PASSWORD, json.dumps(body))

    async def test_a_custom_user_is_honoured(self):
        base = await self.serve(user="owner")
        async with self.session.get(base + "/api/snapshot", headers=basic("owner", PASSWORD)) as r:
            self.assertEqual(r.status, 200)
        async with self.session.get(base + "/api/snapshot", headers=basic("viewer", PASSWORD)) as r:
            self.assertEqual(r.status, 401)

    async def test_no_control_endpoints_exist(self):
        base = await self.serve()
        auth = basic("viewer", PASSWORD)
        for method, path in (("post", "/api/halt"), ("post", "/api/resume"), ("post", "/api/flatten"),
                             ("get", "/ws"), ("post", "/api/snapshot"), ("post", "/")):
            async with getattr(self.session, method)(base + path, headers=auth) as r:
                self.assertIn(r.status, (404, 405), f"{method} {path}")

    async def test_repeated_failures_are_capped_then_recover_the_gate_only_by_waiting(self):
        base = await self.serve()
        for _ in range(MAX_FAILS_PER_MIN):
            async with self.session.get(base + "/api/snapshot", headers=basic("viewer", "nope-nope-nope-1")) as r:
                self.assertEqual(r.status, 401)
        async with self.session.get(base + "/api/snapshot", headers=basic("viewer", "nope-nope-nope-1")) as r:
            self.assertEqual(r.status, 429)
        async with self.session.get(base + "/api/snapshot", headers=basic("viewer", PASSWORD)) as r:
            self.assertEqual(r.status, 429)  # even the right password waits: guessing must stay impractical
        async with self.session.get(base + "/healthz") as r:
            self.assertEqual(r.status, 200)  # liveness is unaffected

    async def test_missing_page_is_a_clean_error(self):
        base = await self.serve()
        self.page.unlink()
        async with self.session.get(base + "/", headers=basic("viewer", PASSWORD)) as r:
            self.assertEqual(r.status, 500)
            self.assertNotIn(self.dir, await r.text())  # no filesystem paths in error text

    async def test_the_real_hosted_page_exists_and_polls_the_matching_endpoint(self):
        real = Path(__file__).resolve().parent.parent / "vercel-dashboard" / "views" / "dashboard.html"
        html = real.read_text(encoding="utf-8")
        self.assertIn("/api/snapshot", html)
        for forbidden in ("/api/halt", "/api/flatten", "WebSocket(", "__TOKEN__"):
            self.assertNotIn(forbidden, html)


if __name__ == "__main__":
    unittest.main()
