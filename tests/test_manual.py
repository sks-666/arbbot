"""Custom (manual) paper trades from Telegram: two-step confirmation, every refusal, exits, persistence."""
import unittest

from arbbot.commands import HELP, MENU, PENDING_TTL_S, CommandHandler
from arbbot.models import Position
from tests.test_commands import build, step, warm
from tests.test_core import set_quotes


class ManualTradeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.eng = build()
        self.t = await warm(self.eng)
        self.h = CommandHandler(self.eng, "T")
        self.clock = [0.0]
        self.h._now = lambda: self.clock[0]
        self.eng.clock = lambda: self.t  # the quotes are stamped with simulated time, so manual trades must be too

    async def advance(self, seconds: float = 1.0, basis: dict | None = None):
        self.t += seconds
        await step(self.eng, self.t, basis)

    async def open_it(self, sym="BTC", usd=200):
        await self.h.reply(f"/open {sym} {usd}")
        return await self.h.reply("/confirm")

    # ---- two-step flow -------------------------------------------------------------------------
    async def test_open_shows_a_preview_and_only_confirm_opens_it(self):
        preview = await self.h.reply("/open BTC 200")
        self.assertIn("not opened yet", preview)
        self.assertIn("/confirm", preview)
        self.assertIn("loses money", preview)  # the honest part: it starts about the round-trip cost under water
        self.assertEqual(self.eng.pf.open, {})  # nothing happens before /confirm
        done = await self.h.reply("/confirm")
        self.assertIn("Opened a paper trade", done)
        pos = self.eng.pf.open["BTCUSDT"]
        self.assertTrue(pos.manual)
        self.assertEqual(pos.notional, 200.0)
        self.assertEqual(pos.predicted_net_bps, 0.0)  # no forecast: the adaptive risk buffer must ignore it
        self.assertFalse(pos.live)
        self.assertIsNone(self.h.pending)

    async def test_confirm_with_nothing_pending_or_after_expiry_or_cancel(self):
        self.assertIn("Nothing to confirm", await self.h.reply("/confirm"))
        await self.h.reply("/open ETH 100")
        self.clock[0] += PENDING_TTL_S + 1
        self.assertIn("expired", await self.h.reply("/confirm"))
        self.assertEqual(self.eng.pf.open, {})
        await self.h.reply("/open ETH 100")
        self.assertIn("Cancelled", await self.h.reply("/cancel"))
        self.assertIn("Nothing to confirm", await self.h.reply("/confirm"))
        self.assertIn("Nothing was pending", await self.h.reply("/cancel"))
        self.assertEqual(self.eng.pf.open, {})

    async def test_size_parsing_and_default(self):
        await self.h.reply("/open ETH")
        self.assertEqual(self.h.pending[1], self.eng.cfg.risk.trade_notional_usd)
        await self.h.reply("/open eth $150")
        self.assertEqual(self.h.pending[:2], ("ETHUSDT", 150.0))
        await self.h.reply("/open SOLUSDT 90")
        self.assertEqual(self.h.pending[:2], ("SOLUSDT", 90.0))

    async def test_bad_input_opens_nothing_and_leaves_nothing_pending(self):
        for text, expect in (("/open", "Which market"), ("/open NOPE 100", "don't know"),
                             ("/open BTC abc", "not a number"), ("/open BTC 5", "at least"),
                             ("/open BTC nan", "at least"), ("/open BTC inf", "at least")):
            reply = await self.h.reply(text)
            self.assertIn(expect, reply, text)
            self.assertIsNone(self.h.pending, text)
        self.assertEqual(self.eng.pf.open, {})

    async def test_plain_words_never_open_anything(self):
        for text in ("open trade we got signal", "open btc 200", "buy ETH now", "enter sol"):
            reply = await self.h.reply(text)
            self.assertEqual(self.eng.pf.open, {}, text)
            self.assertNotIn("Opened", reply)
        self.assertIn("/open BTC 200", await self.h.reply("open btc 200"))

    # ---- limits (the same ones automatic entries obey) ------------------------------------------
    async def test_refusals(self):
        e = self.eng
        self.assertIn("Opened", await self.open_it("BTC", 100))
        self.assertIn("already open", await self.h.reply("/open BTC 100"))
        e.cfg.risk.max_open_positions = 1
        self.assertIn("limit of 1", await self.h.reply("/open ETH 100"))
        e.cfg.risk.max_open_positions = 5
        e.cfg.risk.max_total_notional_usd = 250.0
        self.assertIn("exposure cap", await self.h.reply("/open ETH 200"))
        e.cfg.risk.max_total_notional_usd = 1000.0
        e.request_halt()
        self.assertIn("halted", await self.h.reply("/open ETH 100"))
        e.request_resume()
        e.cfg.risk.min_touch_depth_usd = 1e9
        self.assertIn("too thin", await self.h.reply("/open ETH 100"))
        e.cfg.risk.min_touch_depth_usd = 0.0
        self.assertEqual(set(e.pf.open), {"BTCUSDT"})  # none of the refusals opened anything

    async def test_size_cannot_exceed_half_the_book(self):
        self.t += 1
        for m in self.eng.board.markets.values():
            thin = 400.0 if m.symbol == "ETHUSDT" else 50_000.0
            set_quotes(m, 0.1 if m.mult != 1.0 else 100.0, 2.0, self.t, depth_usd=thin)
        await self.eng.tick(now=self.t)
        self.eng.cfg.risk.max_total_notional_usd = 1e9
        reply = await self.h.reply("/open ETH 300")
        self.assertIn("too big for this book", reply)
        self.assertIn("$200", reply)  # half of the $400 at the best price
        self.assertIn("not opened yet", await self.h.reply("/open ETH 150"))

    async def test_no_fresh_quotes_refuses(self):
        await self.eng.tick(now=self.t + 120)  # quotes are now far older than max_quote_age_s
        self.assertIn("no fresh quotes", await self.h.reply("/open BTC 100"))

    async def test_never_works_when_live(self):
        self.assertIn("Opened", await self.open_it("BTC", 100))
        self.eng.executor.live = True  # as if the bot were running with --live
        self.assertIn("paper-only", await self.h.reply("/open ETH 100"))
        self.assertIn("paper-only", await self.h.reply("/close BTC"))
        self.assertIn("BTCUSDT", self.eng.pf.open)  # untouched

    async def test_a_confirmed_request_is_rechecked_at_the_moment_of_opening(self):
        await self.h.reply("/open ETH 100")
        self.eng.request_halt()  # paused between preview and confirm
        reply = await self.h.reply("/confirm")
        self.assertIn("Can't open", reply)
        self.assertIn("halted", reply)
        self.assertEqual(self.eng.pf.open, {})

    # ---- how the position lives and dies --------------------------------------------------------
    async def test_manual_positions_ignore_strategy_exits_but_keep_protective_ones(self):
        await self.open_it("BTC", 200)
        pos, m = self.eng.pf.open["BTCUSDT"], self.eng.board.markets["BTCUSDT"]
        await self.advance(1000)  # past min_hold and max_hold, basis flat: an automatic trade would exit
        sig = self.eng.sigs["BTCUSDT"]
        self.assertIsNone(self.eng._exit_reason(pos, sig, m, self.t))
        pos.manual = False  # the same situation for an automatic position closes
        self.assertIn(self.eng._exit_reason(pos, sig, m, self.t), ("converged", "timeout"))
        pos.manual = True
        await self.advance(1, basis={"BTCUSDT": 90.0})  # a big move against it still triggers a protective exit
        reason = self.eng._exit_reason(pos, self.eng.sigs["BTCUSDT"], m, self.t)
        self.assertIn(reason, ("stop-loss", "basis-stop"))
        self.eng.flatten_requested = True  # and so do the global ones
        self.assertEqual(self.eng._exit_reason(pos, self.eng.sigs["BTCUSDT"], m, self.t), "manual-flatten")

    async def test_close_records_the_trade_as_manual_close(self):
        await self.open_it("BTC", 200)
        await self.advance(10)
        reply = await self.h.reply("/close BTC")
        self.assertIn("Closed BTCUSDT", reply)
        self.assertEqual(self.eng.pf.open, {})
        trade = self.eng.pf.closed[-1]
        self.assertEqual((trade.symbol, trade.reason, trade.live), ("BTCUSDT", "manual-close", False))
        self.assertLess(trade.pnl_usd, 0)  # a flat basis cannot beat the round-trip cost
        self.assertEqual(self.eng.pf.store.recent_trades(1)[0].reason, "manual-close")

    async def test_close_edge_cases(self):
        self.assertIn("Which one", await self.h.reply("/close"))
        self.assertIn("nothing is open", await self.h.reply("/close"))
        self.assertIn("is not open", await self.h.reply("/close ETH"))
        self.assertIn("don't know", await self.h.reply("/close NOPE"))
        await self.open_it("SOL", 100)
        self.assertIn("SOL", await self.h.reply("/close"))  # lists what is open
        await self.eng.tick(now=self.t + 10)  # stale (>3 s) but short of the 30 s stale-feed exit: can't close on them
        self.assertIn("no fresh quotes", await self.h.reply("/close SOL"))
        self.assertIn("SOLUSDT", self.eng.pf.open)

    async def test_manual_trades_do_not_feed_the_adaptive_risk_buffer(self):
        before = self.eng.scanner.risk_buffer_bps
        for _ in range(12):  # enough closed trades that an adaptive buffer would normally engage
            await self.open_it("BTC", 100)
            await self.advance(6)
            await self.h.reply("/close BTC")
            await self.advance(125)  # past the symbol cooldown
        self.assertEqual(len(self.eng.pf.closed), 12)
        self.eng._update_buffer()
        self.assertEqual(self.eng.scanner.risk_buffer_bps, before)

    async def test_flag_is_saved_shown_and_loaded_back(self):
        await self.open_it("BTC", 200)
        snap = self.eng.snapshot()
        self.assertTrue(snap["positions"][0]["manual"])
        saved = self.eng.pf.store.load_positions()[0]
        self.assertTrue(saved.manual)
        legacy = {k: v for k, v in saved.__dict__.items() if k != "manual"}  # a row written by an older version
        self.assertFalse(Position(**legacy).manual)

    async def test_position_detail_for_an_automatic_and_a_manual_trade(self):
        await step(self.eng, self.t, {"ETHUSDT": 90.0})  # a real signal: the bot opens ETH by itself
        self.assertIn("ETHUSDT", self.eng.pf.open)
        await self.open_it("BTC", 150)
        await self.advance(30, {"ETHUSDT": 60.0})
        eth = await self.h.reply("/positions ETH")
        for needle in ("ETHUSDT", "AUTO", "Size $200", "Opened 30s ago", "fees paid", "Model at entry",
                       "Now: unwind basis", "P&L if closed now", "converged when z", "timeout in", "P&L stop at -59 bps",
                       "basis stop at +25 bps"):
            self.assertIn(needle, eth)
        btc = await self.h.reply("/positions BTC")
        for needle in ("BTCUSDT", "MANUAL", "Size $150", "held until you send /close BTC", "P&L stop at -59 bps"):
            self.assertIn(needle, btc)
        for absent in ("Model at entry", "timeout in", "converged when"):
            self.assertNotIn(absent, btc)  # a manual trade has no model forecast and no automatic exits
        listing = await self.h.reply("/positions")
        self.assertEqual(listing.count("manual"), 1)  # only the hand-picked one is tagged
        self.assertIn("Details of one: /positions BTC", listing)
        self.assertEqual(await self.h.reply("show me the eth position"), eth)
        self.assertEqual(await self.h.reply("/position eth"), eth)

    async def test_position_detail_edge_cases(self):
        await self.open_it("BTC", 100)
        self.assertIn("is not open", await self.h.reply("/positions SOL"))
        self.assertIn("don't know", await self.h.reply("/positions NOPE"))
        await self.eng.tick(now=self.t + 10)  # stale (>3 s) but short of the 30 s stale-feed exit
        stale = await self.h.reply("/positions BTC")
        self.assertIn("No fresh quote", stale)
        self.assertNotIn("Now:", stale)
        self.assertIn("Size $100", stale)  # what was bought is still shown

    async def test_the_direction_is_spelled_out_everywhere_the_command_is_shown(self):
        # "is it long or short?": the answer (both, as a hedged pair) must be visible without opening anything
        for text in (HELP, await self.h.reply("/open"), await self.h.reply("/open BTC 200"),
                     await self.h.reply("open btc 200")):
            self.assertIn("LONG", text)
            self.assertIn("SHORT", text)
            self.assertIn("hedged pair", text)
        self.assertIn("BUY $200 spot (LONG) + SELL $200 perp (SHORT)", await self.h.reply("/open BTC 200"))
        self.assertIn("NOT betting on the price", await self.h.reply("/open BTC 200"))
        self.assertIn("Not available: a pure long, a pure short", HELP)
        menu = dict(MENU)["open"]
        self.assertIn("BUY spot", menu)
        self.assertIn("SELL perp", menu)

    async def test_menu_and_help_mention_the_new_commands(self):
        names = [c for c, _ in MENU]
        for cmd in ("open", "confirm", "cancel", "close"):
            self.assertIn(cmd, names)
            self.assertIn(f"/{cmd}", HELP)
        self.assertIn("never real orders", HELP)


if __name__ == "__main__":
    unittest.main()
