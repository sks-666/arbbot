"""The bot-side snapshot publisher, against a mock Upstash Redis REST endpoint."""
import asyncio
import json
import math
import unittest
from pathlib import Path

import aiohttp
from aiohttp import web

from arbbot.config import Config, PublisherCfg, load
from arbbot.publisher import SCHEMA, SnapshotPublisher, _round, public_snapshot
from tests.test_model import engine, warm

TOKEN = "AXtoken-READ-WRITE-1234567890"


class MockUpstash:
    def __init__(self):
        self.commands: list[list] = []
        self.auth: list[str] = []
        self.script: list[tuple[int, dict]] = []

    async def start(self):
        app = web.Application()
        app.router.add_post("/", self.handle)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.url = f"http://127.0.0.1:{self.runner.addresses[0][1]}"

    async def handle(self, request):
        self.auth.append(request.headers.get("Authorization", ""))
        if self.script:
            status, body = self.script.pop(0)
            return web.json_response(body, status=status)
        self.commands.append(await request.json())
        return web.json_response({"result": "OK"})

    async def stop(self):
        await self.runner.cleanup()


class SnapshotContentTests(unittest.IsolatedAsyncioTestCase):
    async def test_public_snapshot_drops_controls_and_error_strings(self):
        eng, m, pf, cfg = engine()
        await warm(eng, m)
        snap = eng.snapshot()
        snap["feed"]["last_error"] = "ClientConnectorDNSError: Cannot connect to host fapi.binance.com:443"
        pub = public_snapshot(snap, now=1234.5)
        self.assertNotIn("last_error", pub["feed"])
        self.assertEqual(set(pub["halt"]), {"halted", "reason", "trading"})  # no manual / flattening control state
        self.assertEqual((pub["schema"], pub["published_at"]), (SCHEMA, 1234.5))
        for needed in ("markets", "positions", "trades", "events", "stats", "model", "mode"):
            self.assertIn(needed, pub)

    async def test_floats_are_rounded_and_nan_becomes_null(self):
        self.assertEqual(_round({"a": 1.23456789, "b": [2.00004999, float("nan"), math.inf], "c": "x", "d": 7}),
                         {"a": 1.2346, "b": [2.0, None, None], "c": "x", "d": 7})

    async def test_payload_for_55_markets_stays_small(self):
        eng, m, pf, cfg = engine()
        await warm(eng, m)
        snap = eng.snapshot()
        snap["markets"] = [dict(snap["markets"][0], sym=f"COIN{i}USDT") for i in range(55)]
        size = len(json.dumps(public_snapshot(snap), separators=(",", ":")))
        self.assertLess(size, 40_000)

    async def test_events_are_capped(self):
        eng, m, pf, cfg = engine()
        snap = eng.snapshot()
        snap["events"] = [{"ts": i, "lvl": "info", "msg": str(i)} for i in range(200)]
        self.assertEqual(len(public_snapshot(snap)["events"]), 40)


class PublisherTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.up = MockUpstash()
        await self.up.start()
        self.session = aiohttp.ClientSession()
        self.eng, self.m, self.pf, self.cfg = engine()
        await warm(self.eng, self.m)
        self.pcfg = PublisherCfg(interval_s=0.05, key="arbbot:test", ttl_s=77)

    async def asyncTearDown(self):
        await self.session.close()
        await self.up.stop()

    def pub(self, **kw):
        return SnapshotPublisher(self.pcfg, self.eng, kw.get("url", self.up.url), kw.get("token", TOKEN), self.session)

    async def test_push_sends_a_set_with_ttl_and_bearer_auth(self):
        p = self.pub()
        self.assertTrue(await p.push_once())
        cmd = self.up.commands[0]
        self.assertEqual((cmd[0], cmd[1], cmd[3], cmd[4]), ("SET", "arbbot:test", "EX", "77"))
        data = json.loads(cmd[2])
        self.assertEqual(data["schema"], SCHEMA)
        self.assertIn("markets", data)
        self.assertEqual(self.up.auth[0], f"Bearer {TOKEN}")
        self.assertNotIn(TOKEN, cmd[2])  # the token never travels inside the data

    async def test_run_publishes_repeatedly(self):
        p = self.pub()
        task = asyncio.create_task(p.run())
        await asyncio.sleep(0.5)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertGreaterEqual(len(self.up.commands), 3)

    async def test_bad_token_fails_quietly_and_never_leaks_it(self):
        self.up.script = [(401, {"error": f"WRONGPASS {TOKEN}"})]
        p = self.pub()
        self.assertFalse(await p.push_once())
        self.assertEqual(p.failed, 1)
        self.assertNotIn(TOKEN, p.last_error)
        self.assertIn("HTTP 401", p.last_error)

    async def test_unreachable_store_never_raises_and_backs_off(self):
        p = self.pub(url="http://127.0.0.1:1")
        self.assertFalse(await p.push_once())
        self.assertAlmostEqual(p.next_delay(), 0.1)  # 0.05 * 2
        for _ in range(8):
            await p.push_once()
        self.assertLessEqual(p.next_delay(), 60.0)
        p.url = self.up.url  # the store comes back
        self.assertTrue(await p.push_once())
        self.assertAlmostEqual(p.next_delay(), 0.05)  # recovered: normal cadence again

    async def test_without_credentials_it_is_disabled_and_idles(self):
        p = SnapshotPublisher(self.pcfg, self.eng, "", "", self.session)
        self.assertFalse(p.enabled)
        task = asyncio.create_task(p.run())
        await asyncio.sleep(0.2)
        self.assertFalse(task.done())  # a finished task would look like a crash to main()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(self.up.commands, [])

    async def test_fingerprints_compare_settings_without_revealing_them(self):
        a = self.pub().fingerprints()
        self.assertEqual(a, self.pub(url=self.up.url + "/ ", token=" " + TOKEN + "\n").fingerprints())  # trimmed like the real values
        self.assertNotEqual(a[1], self.pub(token=TOKEN[:-1] + "X").fingerprints()[1])  # one changed character shows up
        self.assertNotEqual(a[0], self.pub(url="https://other.upstash.io").fingerprints()[0])
        for shown in a:
            self.assertEqual(len(shown), 8)
            self.assertNotIn(shown, TOKEN)
        self.assertEqual(self.pub(url="", token="").fingerprints(), ("-", "-"))

    async def test_config_switch_disables_it(self):
        off = SnapshotPublisher(PublisherCfg(enabled=False), self.eng, self.up.url, TOKEN, self.session)
        self.assertFalse(off.enabled)

    async def test_shipped_config_loads_the_publisher_section(self):
        cfg = load(Path(__file__).resolve().parent.parent / "config.toml")
        self.assertEqual((cfg.publisher.key, cfg.publisher.ttl_s), ("arbbot:snapshot", 120))
        self.assertIsInstance(Config().publisher, PublisherCfg)


if __name__ == "__main__":
    unittest.main()
