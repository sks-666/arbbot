"""Hosted mode: the health endpoint leaks nothing, the port is validated, and real orders are refused."""
import asyncio
import os
import unittest
from unittest import mock

import aiohttp
from aiohttp import web

from arbbot.health import DEFAULT_PORT, hosted_port, make_health_app
from arbbot.main import amain, parse_args


class HealthServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runner = web.AppRunner(make_health_app())
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.base = f"http://127.0.0.1:{self.runner.addresses[0][1]}"

    async def asyncTearDown(self):
        await self.runner.cleanup()

    async def test_health_replies_with_only_ok(self):
        async with aiohttp.ClientSession() as s:
            for path in ("/", "/healthz"):
                async with s.get(self.base + path) as r:
                    self.assertEqual(r.status, 200)
                    self.assertEqual(await r.json(), {"ok": True})
                    self.assertEqual(r.headers["Cache-Control"], "no-store")
            async with s.head(self.base + "/healthz") as r:  # uptime pingers often use HEAD
                self.assertEqual(r.status, 200)

    async def test_no_dashboard_or_control_routes(self):
        async with aiohttp.ClientSession() as s:
            for method, path in (("get", "/api/snapshot"), ("get", "/ws"), ("post", "/api/flatten"),
                                 ("post", "/api/halt"), ("post", "/api/resume")):
                async with getattr(s, method)(self.base + path) as r:
                    self.assertIn(r.status, (404, 405), f"{method} {path} must not exist in hosted mode")


class PortTests(unittest.TestCase):
    def test_default_and_env(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PORT", None)
            self.assertEqual(hosted_port(), DEFAULT_PORT)
        with mock.patch.dict(os.environ, {"PORT": "8080"}):
            self.assertEqual(hosted_port(), 8080)

    def test_bad_values_stop_startup(self):
        for bad in ("abc", "0", "70000", "-1"):
            with mock.patch.dict(os.environ, {"PORT": bad}):
                with self.assertRaises(SystemExit):
                    hosted_port()


class HostedFlagTests(unittest.TestCase):
    def test_flag_parses(self):
        self.assertTrue(parse_args(["--hosted"]).hosted)
        self.assertFalse(parse_args([]).hosted)

    def test_hosted_refuses_live(self):
        with mock.patch("arbbot.main.load_dotenv"), self.assertRaises(SystemExit) as cm:  # never read the real .env
            asyncio.run(amain(parse_args(["--hosted", "--live"])))
        self.assertIn("paper-only", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
