"""Telegram command interface: answers from a real engine, plus the listener against a mock Bot API."""
import asyncio
import tempfile
import unittest
from pathlib import Path

import aiohttp
from aiohttp import web

from arbbot.alerts import Notifier
from arbbot.commands import MENU, CommandHandler, TelegramListener
from arbbot.config import AlertsCfg
from arbbot.engine import Engine
from arbbot.executor import PaperExecutor
from arbbot.feeds import Board
from arbbot.models import Market
from arbbot.portfolio import Portfolio, Store
from arbbot.risk import Risk
from arbbot.scanner import Scanner
from tests.test_core import set_quotes, small_cfg

TOKEN = "555:TEST-TOKEN"
MARKETS = [("BTCUSDT", "BTCUSDT", "BTC", 1.0, 100.0), ("ETHUSDT", "ETHUSDT", "ETH", 1.0, 100.0),
           ("SOLUSDT", "SOLUSDT", "SOL", 1.0, 100.0), ("DOGEUSDT", "DOGEUSDT", "DOGE", 1.0, 100.0),
           ("1000PEPEUSDT", "PEPEUSDT", "PEPE", 1000.0, 0.1), ("AUSDT", "AUSDT", "A", 1.0, 100.0)]


def build(trading: bool = True) -> Engine:
    cfg = small_cfg(tempfile.mkdtemp())
    board = Board("sim")
    for sym, spot_sym, base, mult, _ in MARKETS:
        board.markets[sym] = Market(sym, spot_sym, base, mult)
    pf = Portfolio(Store(Path(":memory:")), 1000.0)
    return Engine(cfg, board, Scanner(cfg.strategy, cfg.fees, cfg.feed.max_quote_age_s), pf, Risk(cfg.risk, pf),
                  PaperExecutor(cfg.fees), trading=trading)


async def step(eng: Engine, t: float, basis: dict | None = None, i: int = 0) -> None:
    basis = basis or {}
    for sym, _, _, _, spot in MARKETS:
        set_quotes(eng.board.markets[sym], spot, basis.get(sym, 2.0 + 0.3 * (-1) ** i), t)
    await eng.tick(now=t)


async def warm(eng: Engine, n: int = 30) -> float:
    t = 1000.0
    for i in range(n):
        await step(eng, t, i=i)
        t += 1
    return t


class AnswerTests(unittest.IsolatedAsyncioTestCase):
    async def test_signal_question_lists_long_spot_short_perp_and_the_reverse_side(self):
        eng = build(trading=False)
        t = await warm(eng)
        await step(eng, t, {"ETHUSDT": 90.0, "DOGEUSDT": -40.0})
        text = CommandHandler(eng, "PAPER").answer("any trades long or short in the market?")
        for needle in ("1 trade signal", "ETHUSDT", "LONG spot / SHORT perp", "Monitor-only", "DOGEUSDT",
                       "information only", "Scanned 6 markets"):
            self.assertIn(needle, text)

    async def test_no_signal_explains_what_is_closest_and_why(self):
        eng = build()
        t = await warm(eng)
        await step(eng, t, {"SOLUSDT": 20.0})  # big z, but 20 bps cannot pay for a 34 bps round trip
        text = CommandHandler(eng).answer("/signals")
        self.assertIn("No actionable trades", text)
        self.assertIn("SOLUSDT", text)
        self.assertIn("needs:", text)
        self.assertNotIn("🟢", text)

    async def test_warmup_is_reported_honestly(self):
        eng = build()
        await step(eng, 1000.0)
        text = CommandHandler(eng).answer("/signals")
        self.assertIn("6 warming up", text)

    async def test_signal_while_halted_says_it_will_not_trade(self):
        eng = build()
        t = await warm(eng)
        eng.request_halt()
        await step(eng, t, {"ETHUSDT": 90.0})
        self.assertIn("will NOT trade", CommandHandler(eng).answer("/signals"))

    async def test_market_detail_by_ticker_symbol_or_scaled_name(self):
        eng = build()
        await warm(eng)
        h = CommandHandler(eng, "PAPER")
        for q in ("how is BTC?", "/market btc", "/market BTCUSDT", "tell me about pepe", "/market 1000pepe"):
            text = h.answer(q)
            self.assertIn("Basis", text, q)
        self.assertIn("1000PEPEUSDT", h.answer("/market pepe"))
        self.assertIn("don't track", h.answer("/market NOTACOIN"))
        self.assertIn("Which market", h.answer("/market"))

    async def test_market_detail_says_which_side_the_trade_would_be(self):
        eng = build(trading=False)
        t = await warm(eng)
        await step(eng, t, {"ETHUSDT": 90.0, "DOGEUSDT": -40.0})
        h = CommandHandler(eng)
        self.assertIn("LONG spot / SHORT perp", h.answer("/market ETH"))
        self.assertIn("🟢 SIGNAL", h.answer("/market ETH"))
        self.assertIn("reverse side", h.answer("/market DOGE"))

    async def test_filler_words_never_match_a_one_letter_ticker(self):
        eng = build()
        await warm(eng)
        text = CommandHandler(eng).answer("is there a trade right now?")
        self.assertIn("Scanned", text)  # routed to the signals answer, not to AUSDT

    async def test_natural_language_routing(self):
        eng = build()
        await warm(eng)
        h = CommandHandler(eng)
        self.assertIn("No open positions", h.answer("what positions do you hold?"))
        self.assertIn("P&L", h.answer("what's my profit?"))
        self.assertIn("Feed:", h.answer("are you running ok?"))
        self.assertIn("Controls", h.answer("help"))
        self.assertIn("Controls", h.answer("/start"))
        self.assertIn("didn't catch", h.answer("qwerty zzz"))
        self.assertIn("Unknown command", h.answer("/dance"))
        self.assertEqual(h.answer("   "), "")
        self.assertIn("Basis", h.answer("/market@arbbot_bot btc"))  # group-style @botname suffix on other commands

    async def test_top_is_sorted_by_edge_and_capped(self):
        eng = build()
        t = await warm(eng)
        await step(eng, t, {"ETHUSDT": 90.0})
        text = CommandHandler(eng).answer("/top 3")
        lines = text.splitlines()
        self.assertEqual(len(lines), 4)
        self.assertTrue(lines[1].startswith("ETHUSDT"))
        self.assertEqual(len(CommandHandler(eng).answer("/top 999").splitlines()), 7)  # 6 markets + header

    async def test_status_positions_and_pnl_after_a_real_trade(self):
        eng = build()
        t = await warm(eng)
        h = CommandHandler(eng, "PAPER")
        await step(eng, t, {"ETHUSDT": 90.0})
        self.assertEqual(len(eng.pf.open), 1)
        pos_text = h.answer("/positions")
        for needle in ("ETHUSDT", "LONG spot / SHORT perp", "unrealised"):
            self.assertIn(needle, pos_text)
        self.assertIn("open positions: 1/5", h.answer("/status"))
        for k in range(1, 13):
            await step(eng, t + k, i=k)  # back to the normal range, quotes alternating around the baseline
        self.assertEqual(len(eng.pf.open), 0)
        pnl = h.answer("/pnl")
        self.assertIn("ETHUSDT", pnl)
        self.assertIn("1W / 0L", pnl)
        self.assertIn("converged", pnl)

    async def test_pause_and_resume(self):
        eng = build()
        await warm(eng)
        h = CommandHandler(eng)
        self.assertIn("paused", h.answer("/pause"))
        self.assertTrue(eng.risk.manual_halt)
        self.assertIn("already paused", h.answer("/pause"))
        self.assertIn("resumed", h.answer("/resume"))
        self.assertFalse(eng.risk.manual_halt)
        self.assertIn("weren't paused", h.answer("/resume"))

    async def test_resume_cannot_override_fault_kill_switch_or_loss_limit(self):
        eng = build()
        await warm(eng)
        h = CommandHandler(eng)
        eng.risk.trip_fault("UNHEDGED: SHORT PERP OPEN on ETHUSDT")
        self.assertIn("Can't resume", h.answer("/resume"))
        self.assertTrue(eng.risk.halted)
        eng2 = build()
        await warm(eng2)
        h2 = CommandHandler(eng2)
        h2.answer("/pause")
        eng2.risk.kill_active = True
        self.assertIn("stay halted", h2.answer("/resume"))
        self.assertTrue(eng2.risk.halted)

    async def test_plain_words_never_change_state(self):
        eng = build()
        await warm(eng)
        h = CommandHandler(eng)
        for phrase in ("please pause everything", "stop trading now", "halt", "resume trading"):
            h.answer(phrase)
            self.assertFalse(eng.risk.manual_halt, phrase)

    async def test_pause_in_monitor_only_mode(self):
        eng = build(trading=False)
        await warm(eng)
        self.assertIn("monitor-only", CommandHandler(eng).answer("/pause"))
        self.assertFalse(eng.risk.manual_halt)


class MockBot:
    def __init__(self):
        self.updates: list[dict] = []
        self.sent: list[dict] = []
        self.menu: list[dict] = []
        self.offsets: list[str] = []
        self.get_status: list[int] = []  # queued overrides for getUpdates (e.g. 409)

    async def start(self):
        app = web.Application()
        app.router.add_get(f"/bot{TOKEN}/getUpdates", self.get_updates)
        app.router.add_post(f"/bot{TOKEN}/sendMessage", self.send_message)
        app.router.add_post(f"/bot{TOKEN}/setMyCommands", self.set_commands)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.base = f"http://127.0.0.1:{self.runner.addresses[0][1]}"

    async def get_updates(self, request):
        if self.get_status:
            return web.json_response({"ok": False, "description": "Conflict"}, status=self.get_status.pop(0))
        offset = int(request.query.get("offset", "0"))
        self.offsets.append(request.query.get("offset", "0"))
        if offset == -1:
            res = self.updates[-1:]
        else:
            res = [u for u in self.updates if u["update_id"] >= offset]
        return web.json_response({"ok": True, "result": res})

    async def send_message(self, request):
        self.sent.append(await request.json())
        return web.json_response({"ok": True, "result": {}})

    async def set_commands(self, request):
        self.menu = (await request.json())["commands"]
        return web.json_response({"ok": True, "result": True})

    async def stop(self):
        await self.runner.cleanup()

    def push(self, update_id: int, chat_id, text: str):
        self.updates.append({"update_id": update_id, "message": {"chat": {"id": chat_id, "type": "private"}, "text": text}})

    @property
    def texts(self):
        return [m["text"] for m in self.sent]


class ListenerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bot = MockBot()
        await self.bot.start()
        self.session = aiohttp.ClientSession()
        self.cfg = AlertsCfg(api_base=self.bot.base, min_interval_s=0.0)
        self.eng = build()
        await warm(self.eng)
        self.notifier = Notifier(self.cfg, TOKEN, "42", "TEST", self.session)
        self.handler = CommandHandler(self.eng, "TEST")
        self.tasks: list[asyncio.Task] = [asyncio.create_task(self.notifier.run())]

    async def asyncTearDown(self):
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.session.close()
        await self.bot.stop()

    def start_listener(self, **kw) -> TelegramListener:
        listener = TelegramListener(self.cfg, self.notifier, self.handler, poll_timeout_s=0.05, conflict_backoff_s=0.05, **kw)
        self.tasks.append(asyncio.create_task(listener.run()))
        return listener

    async def wait_for(self, cond, timeout=3.0):
        end = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < end:
            if cond():
                return True
            await asyncio.sleep(0.02)
        return False

    async def test_menu_is_registered(self):
        self.start_listener()
        self.assertTrue(await self.wait_for(lambda: bool(self.bot.menu)))
        self.assertEqual([c["command"] for c in self.bot.menu], [c for c, _ in MENU])

    async def test_authorised_chat_gets_an_answer(self):
        self.start_listener()
        await asyncio.sleep(0.2)
        self.bot.push(10, 42, "any trades long or short in the market?")
        self.assertTrue(await self.wait_for(lambda: any("Scanned" in t for t in self.bot.texts)))
        self.assertEqual(self.bot.sent[-1]["chat_id"], "42")

    async def test_other_chats_are_ignored_completely(self):
        listener = self.start_listener()
        await asyncio.sleep(0.2)
        self.bot.push(11, 999, "/pause")
        self.bot.push(12, 999, "/status")
        self.assertTrue(await self.wait_for(lambda: listener.ignored == 2))
        self.assertFalse(self.eng.risk.manual_halt)
        self.assertEqual(self.bot.sent, [])

    async def test_commands_sent_while_offline_are_not_replayed(self):
        self.bot.push(1, 42, "/pause")  # sent hours ago, while the bot was down
        self.bot.push(2, 42, "/pause")
        self.start_listener()
        await asyncio.sleep(0.4)
        self.assertFalse(self.eng.risk.manual_halt)
        self.assertEqual(self.bot.sent, [])
        self.bot.push(3, 42, "/status")  # a fresh command is answered
        self.assertTrue(await self.wait_for(lambda: any("arbbot" in t for t in self.bot.texts)))
        self.assertFalse(self.eng.risk.manual_halt)

    async def test_pause_over_telegram_takes_effect(self):
        self.start_listener()
        await asyncio.sleep(0.2)
        self.bot.push(20, 42, "/pause")
        self.assertTrue(await self.wait_for(lambda: self.eng.risk.manual_halt))
        self.assertTrue(await self.wait_for(lambda: any("paused" in t for t in self.bot.texts)))

    async def test_each_update_is_handled_once(self):
        listener = self.start_listener()
        await asyncio.sleep(0.2)
        self.bot.push(30, 42, "/status")
        await asyncio.sleep(0.6)  # many polls happen; the offset must advance past update 30
        self.assertEqual(listener.handled, 1)
        self.assertEqual(sum(1 for t in self.bot.texts if "arbbot" in t), 1)

    async def test_conflict_from_another_poller_is_survived(self):
        self.bot.get_status = [409, 409]
        self.start_listener()
        await asyncio.sleep(0.4)
        self.bot.push(40, 42, "/status")
        self.assertTrue(await self.wait_for(lambda: any("arbbot" in t for t in self.bot.texts)))

    async def test_a_crashing_answer_does_not_kill_the_listener(self):
        self.start_listener()
        await asyncio.sleep(0.2)
        original = self.handler.answer
        self.handler.answer = lambda text: (_ for _ in ()).throw(ValueError("boom"))
        self.bot.push(50, 42, "/status")
        self.assertTrue(await self.wait_for(lambda: any("hit an error" in t for t in self.bot.texts)))
        self.handler.answer = original
        self.bot.push(51, 42, "/status")
        self.assertTrue(await self.wait_for(lambda: any("Feed:" in t for t in self.bot.texts)))

    async def test_long_replies_are_cut_to_fit_telegram(self):
        self.start_listener()
        await asyncio.sleep(0.2)
        self.handler.answer = lambda text: "x" * 6000
        self.bot.push(60, 42, "hello")
        self.assertTrue(await self.wait_for(lambda: bool(self.bot.sent)))
        self.assertLessEqual(len(self.bot.sent[-1]["text"]), 4096)

    async def test_disabled_listener_idles_instead_of_ending(self):
        quiet = Notifier(self.cfg, "", "", "", self.session)  # no credentials
        listener = TelegramListener(self.cfg, quiet, self.handler, session=self.session)
        task = asyncio.create_task(listener.run())
        self.tasks.append(task)
        await asyncio.sleep(0.2)
        self.assertFalse(task.done())  # a finished task would look like a crash to main()
        self.assertEqual(self.bot.offsets, [])


if __name__ == "__main__":
    unittest.main()
