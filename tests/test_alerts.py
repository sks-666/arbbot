"""Telegram alert tests against a local mock of the Bot API, plus engine-level alert wiring."""
import asyncio
import tempfile
import unittest
from pathlib import Path

import aiohttp
from aiohttp import web

from arbbot.alerts import Notifier, find_chat_ids
from arbbot.config import AlertsCfg
from arbbot.engine import Engine
from arbbot.executor import PaperExecutor
from arbbot.feeds import Board
from arbbot.models import Market, Signal
from arbbot.portfolio import Portfolio, Store
from arbbot.risk import Risk
from arbbot.scanner import Scanner
from tests.test_core import set_quotes, small_cfg

TOKEN = "123456:SECRET-TOKEN-VALUE"


class MockTelegram:
    def __init__(self):
        self.messages: list[dict] = []
        self.script: list[tuple[int, dict]] = []  # queued (status, body) overrides
        self.updates: list[dict] = []

    async def start(self):
        app = web.Application()
        app.router.add_post(f"/bot{TOKEN}/sendMessage", self.send)
        app.router.add_get(f"/bot{TOKEN}/getUpdates", self.get_updates)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.base = f"http://127.0.0.1:{self.runner.addresses[0][1]}"

    async def send(self, request):
        if self.script:
            status, body = self.script.pop(0)
            return web.json_response(body, status=status)
        self.messages.append(await request.json())
        return web.json_response({"ok": True, "result": {}})

    async def get_updates(self, request):
        return web.json_response({"ok": True, "result": self.updates})

    async def stop(self):
        await self.runner.cleanup()

    @property
    def texts(self):
        return [m["text"] for m in self.messages]


class NotifierTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tg = MockTelegram()
        await self.tg.start()
        self.session = aiohttp.ClientSession()
        self.cfg = AlertsCfg(api_base=self.tg.base, min_interval_s=0.0, signal_cooldown_s=300.0)
        self.n = Notifier(self.cfg, TOKEN, "42", "PAPER mainnet", self.session)
        self.task = asyncio.create_task(self.n.run())

    async def asyncTearDown(self):
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        await self.session.close()
        await self.tg.stop()

    def sig(self, sym="ETHUSDT"):
        return Signal(sym, 0.0, status="signal", basis_mid_bps=62.0, basis_in_bps=60.0, z=7.4,
                      net_edge_bps=16.8, fillable_usd=28_600, funding_apr_pct=15.0)

    async def test_delivers_to_the_configured_chat(self):
        self.n.send("hello")
        await self.n.flush()
        self.assertEqual(self.tg.messages[0]["chat_id"], "42")
        self.assertEqual(self.tg.texts, ["hello"])

    async def test_signal_message_says_long_spot_short_perp(self):
        self.n.signal(self.sig(), now=1000.0, will_trade=True)
        await self.n.flush()
        text = self.tg.texts[0]
        for needle in ("SIGNAL", "ETHUSDT", "LONG spot / SHORT perp", "+62.0 bps", "z 7.4", "+16.8", "entering now"):
            self.assertIn(needle, text)

    async def test_signal_dedupe_per_symbol_with_cooldown(self):
        self.n.signal(self.sig("ETHUSDT"), 1000.0, True)
        self.n.signal(self.sig("ETHUSDT"), 1100.0, True)  # inside cooldown: dropped
        self.n.signal(self.sig("BTCUSDT"), 1100.0, True)  # other market: sent
        self.n.signal(self.sig("ETHUSDT"), 1400.0, True)  # cooldown over: sent
        await self.n.flush()
        self.assertEqual(len(self.tg.messages), 3)

    async def test_unsent_signal_explains_why(self):
        self.n.signal(self.sig(), 1000.0, False, "paused manually")
        await self.n.flush()
        self.assertIn("NOT trading (paused manually)", self.tg.texts[0])

    async def test_halt_and_fault_messages(self):
        self.n.halted("daily loss limit $25.00 reached")
        self.n.halted("FAULT: 3 consecutive execution errors", fault=True)
        self.n.resumed()
        await self.n.flush()
        self.assertIn("ENTRIES HALTED", self.tg.texts[0])
        self.assertIn("FAULT", self.tg.texts[1])
        self.assertIn("resumed", self.tg.texts[2])

    async def test_rate_limit_is_retried(self):
        self.tg.script = [(429, {"ok": False, "parameters": {"retry_after": 0}})]
        self.n.send("will be retried")
        await self.n.flush(3.0)
        self.assertEqual(self.tg.texts, ["will be retried"])
        self.assertEqual(self.n.sent, 1)

    async def test_bad_credentials_fail_quietly_and_never_leak_the_token(self):
        self.tg.script = [(401, {"ok": False, "description": f"Unauthorized {TOKEN}"})]
        self.n.send("x")
        await self.n.flush(3.0)
        self.assertEqual((self.n.sent, self.n.failed), (0, 1))
        self.assertNotIn(TOKEN, self.n.last_error)

    async def test_unreachable_telegram_never_raises(self):
        n = Notifier(AlertsCfg(api_base="http://127.0.0.1:1", min_interval_s=0.0), TOKEN, "42", "", self.session)
        task = asyncio.create_task(n.run())
        n.send("nobody is listening")
        await n.flush(8.0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(n.failed, 1)
        self.assertNotIn(TOKEN, n.last_error)

    async def test_queue_overflow_keeps_priority_messages(self):
        n = Notifier(AlertsCfg(api_base=self.tg.base, queue_size=3), TOKEN, "42", "", self.session)  # no run(): stays queued
        n.send("fault!", priority=True)
        for i in range(5):
            n.send(f"noise {i}")
        queued = [q[0] for q in n._q]
        self.assertEqual(len(queued), 3)
        self.assertIn("fault!", queued)
        self.assertEqual(queued[-1], "noise 4")

    async def test_disabled_without_credentials(self):
        n = Notifier(self.cfg, "", "", "", self.session)
        n.send("nope")
        self.assertFalse(n.enabled)
        self.assertEqual(len(n._q), 0)
        ok, detail = await n.send_now("nope")
        self.assertFalse(ok)
        self.assertIn("TELEGRAM_BOT_TOKEN", detail)

    async def test_send_now_and_find_chat_ids(self):
        ok, detail = await self.n.send_now("direct")
        self.assertTrue(ok)
        self.assertEqual(self.tg.texts, ["direct"])
        self.tg.updates = [{"message": {"chat": {"id": 777, "type": "private", "first_name": "Sam"}}},
                           {"message": {"chat": {"id": 777, "type": "private", "first_name": "Sam"}}}]
        chats = await find_chat_ids(self.session, self.cfg, TOKEN)
        self.assertEqual(chats, [{"id": 777, "type": "private", "name": "Sam"}])


class EngineAlertTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tg = MockTelegram()
        await self.tg.start()
        self.session = aiohttp.ClientSession()
        tmp = tempfile.mkdtemp()
        self.cfg = small_cfg(tmp)
        self.cfg.alerts = AlertsCfg(api_base=self.tg.base, min_interval_s=0.0, feed_debounce_s=10.0)
        self.board = Board("sim")
        self.m = Market("ETHUSDT", "ETHUSDT", "ETH")
        self.board.markets["ETHUSDT"] = self.m
        self.pf = Portfolio(Store(Path(":memory:")), 1000.0)
        self.risk = Risk(self.cfg.risk, self.pf)
        self.notifier = Notifier(self.cfg.alerts, TOKEN, "42", "SIM", self.session)
        self.task = asyncio.create_task(self.notifier.run())
        self.eng = Engine(self.cfg, self.board, Scanner(self.cfg.strategy, self.cfg.fees, self.cfg.feed.max_quote_age_s),
                          self.pf, self.risk, PaperExecutor(self.cfg.fees), alerts=self.notifier)

    async def asyncTearDown(self):
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        await self.session.close()
        await self.tg.stop()

    async def _warm(self):
        t = 1000.0
        for i in range(30):
            set_quotes(self.m, 100.0, 2.0 + 0.3 * (-1) ** i, t)
            await self.eng.tick(now=t)
            t += 1
        return t

    async def test_signal_entry_and_exit_alerts(self):
        t = await self._warm()
        set_quotes(self.m, 100.0, 90.0, t)
        await self.eng.tick(now=t)
        for _ in range(12):
            t += 1
            set_quotes(self.m, 100.0, 2.0, t)
            await self.eng.tick(now=t)
        await self.notifier.flush()
        kinds = [x.split("\n")[0] for x in self.tg.texts]
        self.assertTrue(kinds[0].startswith("🟢 SIGNAL"), kinds)
        self.assertTrue(kinds[1].startswith("📥 ENTERED"), kinds)
        self.assertTrue(kinds[2].startswith("✅ EXIT"), kinds)
        self.assertEqual(len(kinds), 3)

    async def test_halt_and_resume_alerts_follow_state_changes(self):
        t = await self._warm()
        self.eng.request_halt()
        await self.eng.tick(now=t)
        await self.eng.tick(now=t + 1)  # no change: no second alert
        self.eng.request_resume()
        await self.eng.tick(now=t + 2)
        await self.notifier.flush()
        self.assertEqual(len(self.tg.messages), 2)
        self.assertIn("HALTED", self.tg.texts[0])
        self.assertIn("paused manually", self.tg.texts[0])
        self.assertIn("resumed", self.tg.texts[1])

    async def test_signal_while_halted_says_not_trading(self):
        t = await self._warm()
        self.eng.request_halt()
        set_quotes(self.m, 100.0, 90.0, t)
        await self.eng.tick(now=t)
        await self.notifier.flush()
        signal_msgs = [x for x in self.tg.texts if "SIGNAL" in x]
        self.assertEqual(len(signal_msgs), 1)
        self.assertIn("NOT trading", signal_msgs[0])
        self.assertEqual(len(self.pf.open), 0)

    async def test_fault_alert(self):
        t = await self._warm()
        self.risk.trip_fault("UNHEDGED: SHORT PERP OPEN on ETHUSDT")
        await self.eng.tick(now=t)
        await self.notifier.flush()
        self.assertTrue(any(x.startswith("🚨 FAULT") and "UNHEDGED" in x for x in self.tg.texts))

    async def test_kill_file_alert(self):
        t = await self._warm()
        Path(self.cfg.risk.kill_file).touch()
        await self.eng.tick(now=t)
        await self.notifier.flush()
        self.assertTrue(any("kill file" in x for x in self.tg.texts))

    async def test_feed_outage_alert_is_debounced_and_recovery_reported(self):
        self.board.kind = self.board.source = "ws"  # a real feed, not the simulator
        t = 5000.0
        self.board.ok, self.board.last_update = True, t
        set_quotes(self.m, 100.0, 2.0, t)
        await self.eng.tick(now=t)
        self.board.ok = False  # feed drops
        await self.eng.tick(now=t + 1)
        await self.eng.tick(now=t + 6)  # 5 s in: still inside the debounce window
        await self.notifier.flush()
        self.assertEqual(len(self.tg.messages), 0)
        await self.eng.tick(now=t + 12)  # outage has persisted past 10 s
        self.board.ok, self.board.last_update = True, t + 13
        await self.eng.tick(now=t + 13)
        self.board.last_update = t + 25  # feed keeps delivering
        await self.eng.tick(now=t + 25)
        await self.notifier.flush()
        self.assertEqual(len(self.tg.messages), 2)
        self.assertIn("DOWN", self.tg.texts[0])
        self.assertIn("recovered", self.tg.texts[1])

    async def test_rest_fallback_alert(self):
        self.board.kind, self.board.source = "ws", "rest"
        t = 8000.0
        self.board.ok, self.board.last_update = True, t
        for dt in (0, 5, 11):
            self.board.last_update = t + dt
            await self.eng.tick(now=t + dt)
        await self.notifier.flush()
        self.assertEqual(len(self.tg.messages), 1)
        self.assertIn("REST fallback", self.tg.texts[0])


if __name__ == "__main__":
    unittest.main()
