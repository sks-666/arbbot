"""WebSocket feed tests. A local aiohttp server impersonates Binance's combined-stream endpoints."""
import asyncio
import time
import unittest

import aiohttp
from aiohttp import web

from arbbot.config import Config
from arbbot.feeds import Board, HybridFeed, WsFeed
from arbbot.models import Market, Quote


def make_board() -> Board:
    board = Board("ws")
    board.markets["BTCUSDT"] = Market("BTCUSDT", "BTCUSDT", "BTC")
    board.markets["1000SHIBUSDT"] = Market("1000SHIBUSDT", "SHIBUSDT", "SHIB", mult=1000.0)
    return board


def make_cfg() -> Config:
    cfg = Config()
    cfg.feed.interval_s = 0.05
    cfg.feed.ws_stale_s = 1.0
    cfg.feed.ws_max_silent_s = 30.0
    return cfg


def mark_msg(now_ms: float) -> dict:
    return {"stream": "!markPrice@arr@1s", "data": [
        {"e": "markPriceUpdate", "E": now_ms, "s": "BTCUSDT", "p": "65010.5", "i": "65000.0", "r": "0.0001", "T": 1_900_000_000_000},
        {"e": "markPriceUpdate", "E": now_ms, "s": "1000SHIBUSDT", "p": "20.10", "i": "20.00", "r": "-0.0002", "T": 1_900_000_000_000},
        {"e": "markPriceUpdate", "E": now_ms, "s": "NOTTRACKEDUSDT", "p": "1", "i": "1", "r": "0", "T": 1},
    ]}


FUT_BOOK = {"stream": "btcusdt@bookTicker", "data": {"e": "bookTicker", "u": 1, "s": "BTCUSDT", "b": "65020.0", "B": "2.5", "a": "65020.1", "A": "3.0"}}
FUT_SHIB = {"stream": "1000shibusdt@bookTicker", "data": {"e": "bookTicker", "s": "1000SHIBUSDT", "b": "20.12", "B": "900", "a": "20.13", "A": "800"}}
SPOT_BOOK = {"stream": "btcusdt@bookTicker", "data": {"u": 5, "s": "BTCUSDT", "b": "65000.0", "B": "1.5", "a": "65000.5", "A": "1.0"}}
SPOT_SHIB = {"stream": "shibusdt@bookTicker", "data": {"u": 6, "s": "SHIBUSDT", "b": "0.0200", "B": "9e6", "a": "0.020001", "A": "8e6"}}


class HandleTests(unittest.TestCase):
    def setUp(self):
        self.board = make_board()
        self.feed = WsFeed(None, self.board, make_cfg(), "ws://x", "ws://y")

    def test_parses_mark_index_funding_and_books(self):
        now = 1000.0
        self.feed.handle("fut", mark_msg(now * 1000), now)
        self.feed.handle("fut", FUT_BOOK, now)
        self.feed.handle("spot", SPOT_BOOK, now)
        m = self.board.markets["BTCUSDT"]
        self.assertEqual((m.mark, m.index, m.funding_rate), (65010.5, 65000.0, 0.0001))
        self.assertEqual((m.perp.bid, m.perp.ask, m.perp.bid_qty), (65020.0, 65020.1, 2.5))
        self.assertEqual((m.spot.bid, m.spot.ask, m.spot.ask_qty), (65000.0, 65000.5, 1.0))
        self.assertEqual(m.next_funding_ms, 1_900_000_000_000)

    def test_spot_symbol_maps_to_multiplied_perp(self):
        self.feed.handle("spot", SPOT_SHIB, 5.0)
        m = self.board.markets["1000SHIBUSDT"]
        self.assertAlmostEqual(m.spot.bid * m.mult, 20.0)

    def test_ignores_unknown_symbols_and_zero_quotes(self):
        self.feed.handle("spot", {"data": {"s": "FOOUSDT", "b": "1", "B": "1", "a": "2", "A": "1"}}, 5.0)
        self.feed.handle("fut", {"data": {"s": "BTCUSDT", "b": "0", "B": "1", "a": "0", "A": "1"}}, 5.0)
        self.assertIsNone(self.board.markets["BTCUSDT"].perp)

    def test_lag_measured_from_event_time(self):
        now = 1000.0
        self.feed.handle("fut", mark_msg(now * 1000 - 120), now)
        self.assertAlmostEqual(self.board.latency_ms, 120.0, places=3)

    def test_urls_use_lowercase_streams(self):
        (fut_url,) = self.feed.urls("fut")
        (spot_url,) = self.feed.urls("spot")
        self.assertIn("!markPrice@arr@1s/btcusdt@bookTicker/1000shibusdt@bookTicker", fut_url)
        self.assertTrue(spot_url.endswith("streams=btcusdt@bookTicker/shibusdt@bookTicker"))

    def test_streams_are_chunked_so_urls_stay_short_at_scale(self):
        board = Board("ws")
        for i in range(100):
            sym = f"COIN{i}USDT"
            board.markets[sym] = Market(sym, sym, f"COIN{i}")
        cfg = make_cfg()
        cfg.feed.ws_streams_per_conn = 40
        feed = WsFeed(None, board, cfg, "wss://f", "wss://s")
        self.assertEqual([len(c) for c in feed.plan["fut"]], [40, 40, 21])  # 100 books + markPrice
        self.assertEqual([len(c) for c in feed.plan["spot"]], [40, 40, 20])
        self.assertTrue(all(len(u) < 1200 for u in feed.urls("fut") + feed.urls("spot")))  # vs 2000+ unchunked
        self.assertIn("!markPrice@arr@1s", feed.urls("fut")[0])
        every = [s for c in feed.plan["fut"] for s in c]
        self.assertEqual(len(every), len(set(every)))  # each stream subscribed exactly once

    def test_venue_needs_every_connection_up(self):
        board = Board("ws")
        for i in range(60):
            sym = f"COIN{i}USDT"
            board.markets[sym] = Market(sym, sym, f"COIN{i}")
        feed = WsFeed(None, board, make_cfg(), "wss://f", "wss://s")
        feed.last_msg = {"fut": 100.0, "spot": 100.0}
        feed.connected = {k: True for k in feed.conn_keys}
        self.assertTrue(feed.healthy(100.5))
        feed.connected[("spot", 1)] = False  # one of the spot connections drops
        self.assertTrue(feed.alive("fut", 100.5))
        self.assertFalse(feed.alive("spot", 100.5))
        self.assertFalse(feed.healthy(100.5))


class TouchTests(unittest.TestCase):
    def setUp(self):
        self.board = make_board()
        self.feed = WsFeed(None, self.board, make_cfg(), "ws://x", "ws://y")
        self.m = self.board.markets["BTCUSDT"]

    def _alive(self, now):
        self.feed.connected = {k: True for k in self.feed.conn_keys}
        self.feed.last_msg = {"fut": now, "spot": now}

    def test_quiet_book_stays_fresh_while_connection_is_alive(self):
        self.feed.handle("fut", FUT_BOOK, 100.0)
        self.feed.handle("spot", SPOT_BOOK, 100.0)
        self._alive(110.0)  # streams alive, but BTC's book has not changed for 10 s
        self.feed.touch(110.0)
        self.assertEqual((self.m.perp.ts, self.m.spot.ts), (110.0, 110.0))

    def test_silent_symbol_is_not_kept_alive_forever(self):
        self.feed.handle("fut", FUT_BOOK, 100.0)
        self._alive(200.0)  # 100 s without a BTC update > ws_max_silent_s
        self.feed.touch(200.0)
        self.assertEqual(self.m.perp.ts, 100.0)

    def test_dead_connection_is_not_touched(self):
        self.feed.handle("fut", FUT_BOOK, 100.0)
        self.feed.connected = {k: False for k in self.feed.conn_keys}
        self.feed.touch(101.0)
        self.assertEqual(self.m.perp.ts, 100.0)


class SocketRoundTripTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.seen = []
        app = web.Application()

        async def serve(request, first, repeat):
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            self.seen.append(request.query_string)
            for msg in first:
                await ws.send_json(msg)
            while not ws.closed:
                await asyncio.sleep(0.05)
                try:
                    await ws.send_json(repeat() if callable(repeat) else repeat)
                except ConnectionResetError:
                    break
            return ws

        app.router.add_get("/fut/stream", lambda r: serve(r, [FUT_BOOK, FUT_SHIB], lambda: mark_msg(time.time() * 1000)))
        app.router.add_get("/spot/stream", lambda r: serve(r, [SPOT_BOOK, SPOT_SHIB], SPOT_BOOK))
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.port = self.runner.addresses[0][1]
        self.session = aiohttp.ClientSession()

    async def asyncTearDown(self):
        await self.session.close()
        await self.runner.cleanup()

    async def test_connects_streams_and_goes_healthy(self):
        board = make_board()
        base = f"ws://127.0.0.1:{self.port}"
        feed = WsFeed(self.session, board, make_cfg(), base + "/fut", base + "/spot")
        task = asyncio.create_task(feed.run())
        try:
            btc = board.markets["BTCUSDT"]
            for _ in range(80):  # wait for real data, not just for the sockets to be up
                if feed.healthy() and board.ok and board.polls and btc.mark and btc.perp and btc.spot:
                    break
                await asyncio.sleep(0.05)
            self.assertTrue(feed.healthy())
            self.assertEqual(board.source, "ws")
            self.assertEqual(btc.perp.bid, 65020.0)
            self.assertEqual(btc.spot.ask, 65000.5)
            self.assertEqual(btc.mark, 65010.5)
            self.assertAlmostEqual(btc.perp.ts, time.time(), delta=1.5)  # touched, so it reads as fresh
            self.assertTrue(any("!markPrice@arr@1s" in q and "btcusdt@bookTicker" in q for q in self.seen))
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


    async def test_chunked_connections_all_connect_and_health_needs_all(self):
        board = make_board()
        cfg = make_cfg()
        cfg.feed.ws_streams_per_conn = 2  # forces 2 futures connections (3 streams) + 1 spot connection
        base = f"ws://127.0.0.1:{self.port}"
        feed = WsFeed(self.session, board, cfg, base + "/fut", base + "/spot")
        self.assertEqual((len(feed.plan["fut"]), len(feed.plan["spot"])), (2, 1))
        task = asyncio.create_task(feed.run())
        try:
            for _ in range(60):
                if feed.healthy():
                    break
                await asyncio.sleep(0.05)
            self.assertTrue(feed.healthy())
            self.assertEqual(len(self.seen), 3)  # one request per connection
            self.assertTrue(all(feed.connected.values()))
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


class StubWs:
    def __init__(self):
        self.up = False

    def healthy(self, now=None):
        return self.up

    async def run(self):
        await asyncio.sleep(3600)


class StubRest:
    def __init__(self, board):
        self.board, self.calls = board, 0

    async def step(self):
        self.calls += 1
        self.board.ok = True
        return 0.0


class HybridFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_rest_takes_over_when_ws_is_down_and_hands_back(self):
        board, cfg = make_board(), make_cfg()
        ws, rest = StubWs(), StubRest(board)
        task = asyncio.create_task(HybridFeed(ws, rest, board, cfg).run())
        try:
            await asyncio.sleep(0.8)
            self.assertGreaterEqual(rest.calls, 2)
            self.assertEqual(board.source, "rest")

            ws.up = True  # streams recover: REST goes quiet
            await asyncio.sleep(0.15)
            calls = rest.calls
            await asyncio.sleep(0.3)
            self.assertEqual(rest.calls, calls)

            ws.up = False  # streams drop again: REST resumes
            await asyncio.sleep(0.3)
            self.assertGreater(rest.calls, calls)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_unreachable_stream_endpoint_falls_back_without_crashing(self):
        board, cfg = make_board(), make_cfg()
        async with aiohttp.ClientSession() as session:
            ws = WsFeed(session, board, cfg, "ws://127.0.0.1:1", "ws://127.0.0.1:1")  # nothing listens here
            rest = StubRest(board)
            task = asyncio.create_task(HybridFeed(ws, rest, board, cfg).run())
            try:
                await asyncio.sleep(0.5)
                self.assertFalse(ws.healthy())
                self.assertGreaterEqual(rest.calls, 3)
                self.assertEqual(board.source, "rest")
                self.assertIn("stream", board.last_error)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
