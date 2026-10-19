import asyncio
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from arbbot.config import Config, FeesCfg, load
from arbbot.engine import Engine
from arbbot.executor import PaperExecutor
from arbbot.feeds import Board, SimFeed, parse_filters, split_base
from arbbot.live import align_qty
from arbbot.models import ClosedTrade, Market, Position, Quote, Signal
from arbbot.portfolio import Portfolio, Store, unrealized_usd
from arbbot.risk import Risk
from arbbot.scanner import Scanner, round_trip_cost_bps

ROOT = Path(__file__).resolve().parent.parent


def set_quotes(m: Market, spot_mid: float, basis_bps: float, ts: float, spread_bps: float = 1.0, depth_usd: float = 50_000):
    h = spread_bps / 2e4
    perp_mid = spot_mid * m.mult * (1 + basis_bps / 1e4)
    m.spot = Quote(spot_mid * (1 - h), depth_usd / spot_mid, spot_mid * (1 + h), depth_usd / spot_mid, ts)
    m.perp = Quote(perp_mid * (1 - h), depth_usd / perp_mid, perp_mid * (1 + h), depth_usd / perp_mid, ts)


def small_cfg(tmp: str) -> Config:
    cfg = Config()
    cfg.strategy.min_samples = 20
    cfg.strategy.lookback = 100
    cfg.risk.kill_file = str(Path(tmp) / "KILL")
    cfg.risk.min_touch_depth_usd = 0.0  # the thin-touch guard has its own tests; synthetic books here are small
    return cfg


class ConfigTests(unittest.TestCase):
    def test_shipped_config_loads(self):
        cfg = load(ROOT / "config.toml")
        self.assertEqual(cfg.execution.mode, "paper")
        self.assertEqual(round_trip_cost_bps(cfg.fees), 34.0)

    def test_unknown_key_rejected(self):
        with tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False) as f:
            f.write("[risk]\ndaily_loss_limt_usd = 5\n")  # typo on purpose
        with self.assertRaises(ValueError):
            load(f.name)


class FeedHelperTests(unittest.TestCase):
    def test_split_base(self):
        self.assertEqual(split_base("1000SHIB"), ("SHIB", 1000.0))
        self.assertEqual(split_base("1000000MOG"), ("MOG", 1e6))
        self.assertEqual(split_base("BTC"), ("BTC", 1.0))
        self.assertEqual(split_base("1INCH"), ("1INCH", 1.0))

    def test_parse_filters(self):
        f = parse_filters([
            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
            {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.01", "minQty": "0.01"},
            {"filterType": "MIN_NOTIONAL", "notional": "100"},
        ])
        self.assertEqual((f.step, f.min_qty, f.min_notional), (0.01, 0.01, 100.0))

    def test_sim_feed_populates_55_markets(self):
        cfg = Config()
        board = Board("sim")
        feed = SimFeed(board, cfg, seed=1)
        feed.load_universe()
        feed.step()
        self.assertGreaterEqual(len(board.markets), 50)
        self.assertTrue(all(m.spot and m.perp for m in board.markets.values()))


class ScannerTests(unittest.TestCase):
    def setUp(self):
        self.cfg = Config()
        self.cfg.strategy.min_samples = 20
        self.cfg.strategy.lookback = 100
        self.sc = Scanner(self.cfg.strategy, self.cfg.fees, self.cfg.feed.max_quote_age_s)

    def warm(self, m, n=30, t0=1000.0):
        for i in range(n):
            set_quotes(m, 100.0, 2.0 + 0.3 * (-1) ** i, t0 + i)
            self.sc.evaluate(m, t0 + i)
        return t0 + n

    def test_multiplier_is_applied(self):
        m = Market("1000SHIBUSDT", "SHIBUSDT", "SHIB", mult=1000.0)
        set_quotes(m, 0.00002, 5.0, 10.0, spread_bps=0.0)
        sig = self.sc.evaluate(m, 10.0)
        self.assertAlmostEqual(sig.basis_mid_bps, 5.0, places=6)

    def test_large_dislocation_signals(self):
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        t = self.warm(m)
        set_quotes(m, 100.0, 90.0, t)
        sig = self.sc.evaluate(m, t)
        self.assertEqual(sig.status, "signal")
        self.assertGreater(sig.net_edge_bps, 40)
        self.assertGreaterEqual(sig.z, 3.0)

    def test_dislocation_smaller_than_costs_does_not_signal(self):
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        t = self.warm(m)
        set_quotes(m, 100.0, 25.0, t)  # huge z, but 25 bps cannot pay for a 34 bps round trip
        sig = self.sc.evaluate(m, t)
        self.assertGreater(sig.z, 3.0)
        self.assertLess(sig.net_edge_bps, 0)
        self.assertNotEqual(sig.status, "signal")

    def test_deeply_negative_basis_never_signals_even_after_a_big_z_jump(self):
        """Real paper trades (ERAUSDT, SANDUSDT): basis hopped from about -218 to -138 bps. z was 7+ and the
        'edge' looked like +24 bps, but it was reversion toward a broken mean and both trades lost ~60 bps."""
        m = Market("ERAUSDT", "ERAUSDT", "ERA")
        t = 1000.0
        for i in range(30):
            set_quotes(m, 100.0, -218.0 + 0.4 * (-1) ** i, t + i)
            self.sc.evaluate(m, t + i)
        t += 30
        set_quotes(m, 100.0, -138.0, t)
        sig = self.sc.evaluate(m, t)
        self.assertGreater(sig.z, 7)
        self.assertGreater(sig.net_edge_bps, self.cfg.strategy.min_net_edge_bps)  # the old rule would have fired
        self.assertNotEqual(sig.status, "signal")
        self.assertEqual(sig.status, "watch")

    def test_dislocated_mark_index_is_rejected(self):
        """Real data: ERA/GTC/SAND showed ~-200 bps basis with mark ~200 bps off index (broken or halted spot)."""
        m = Market("SANDUSDT", "SANDUSDT", "SAND")
        t = self.warm(m)
        set_quotes(m, 100.0, 90.0, t)
        m.mark, m.index = 99.0, 101.9  # ~ -285 bps
        sig = self.sc.evaluate(m, t)
        self.assertEqual(sig.status, "stale")
        self.assertIn("mark/index", sig.note)
        m.mark, m.index = 100.1, 100.0  # healthy index again
        set_quotes(m, 100.0, 90.0, t + 1)
        self.assertEqual(self.sc.evaluate(m, t + 1).status, "signal")

    def test_warmup_blocks_signals(self):
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        set_quotes(m, 100.0, 90.0, 5.0)
        self.assertEqual(self.sc.evaluate(m, 5.0).status, "warmup")

    def test_stale_quotes_blocked(self):
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        set_quotes(m, 100.0, 2.0, 100.0)
        sig = self.sc.evaluate(m, 100.0 + self.cfg.feed.max_quote_age_s + 1)
        self.assertEqual(sig.status, "stale")

    def test_crossed_book_rejected(self):
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        set_quotes(m, 100.0, 2.0, 100.0)
        m.perp = Quote(101.0, 10, 100.0, 10, 100.0)
        self.assertEqual(self.sc.evaluate(m, 100.0).status, "stale")

    def test_same_snapshot_counted_once(self):
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        set_quotes(m, 100.0, 2.0, 50.0)
        self.sc.evaluate(m, 50.0)
        self.sc.evaluate(m, 50.4)
        self.assertEqual(self.sc.evaluate(m, 50.8).samples, 1)


class PaperExecutorTests(unittest.IsolatedAsyncioTestCase):
    def _market(self):
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        m.spot = Quote(100.0, 1e3, 100.0, 1e3, 0.0)
        m.perp = Quote(101.0, 1e3, 101.0, 1e3, 0.0)
        return m

    async def test_round_trip_pnl_without_costs(self):
        ex = PaperExecutor(FeesCfg(0.0, 0.0, 0.0))
        m = self._market()
        sig = Signal("ETHUSDT", 0.0, basis_in_bps=100.0, z=5.0)
        pos = await ex.open(m, sig, 1010.0, 0.0)
        self.assertAlmostEqual(pos.perp_qty, 10.0)
        m.perp = Quote(100.2, 1e3, 100.2, 1e3, 1.0)  # basis collapsed
        trade = await ex.close(pos, m, Signal("ETHUSDT", 1.0, basis_out_bps=20.0), 1.0, "converged")
        self.assertAlmostEqual(trade.pnl_usd, 8.0, places=6)  # 10 contracts * (101 - 100.2)

    async def test_fees_are_charged_on_all_four_fills(self):
        ex = PaperExecutor(FeesCfg(futures_taker_bps=5.0, spot_taker_bps=10.0, slippage_bps=0.0))
        m = self._market()
        pos = await ex.open(m, Signal("ETHUSDT", 0.0), 1010.0, 0.0)
        self.assertAlmostEqual(pos.fees_paid, 1000 * 0.001 + 1010 * 0.0005, places=6)  # 1.505
        m.perp = Quote(100.2, 1e3, 100.2, 1e3, 1.0)
        trade = await ex.close(pos, m, Signal("ETHUSDT", 1.0), 1.0, "converged")
        self.assertAlmostEqual(trade.fees_usd, 1.505 + 1.0 + 0.501, places=6)
        self.assertAlmostEqual(trade.pnl_usd, 8.0 - 3.006, places=6)

    async def test_unrealized_matches_realized_when_closed_now(self):
        fees = FeesCfg(5.0, 10.0, 1.0)
        ex = PaperExecutor(fees)
        m = self._market()
        pos = await ex.open(m, Signal("ETHUSDT", 0.0), 500.0, 0.0)
        m.perp = Quote(100.5, 1e3, 100.6, 1e3, 1.0)
        u = unrealized_usd(pos, m, fees)
        trade = await ex.close(pos, m, Signal("ETHUSDT", 1.0), 1.0, "x")
        self.assertAlmostEqual(u, trade.pnl_usd, places=9)


class RiskPortfolioTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.cfg = small_cfg(self.tmp)
        self.pf = Portfolio(Store(Path(":memory:")), 1_000_000.0)
        self.risk = Risk(self.cfg.risk, self.pf)

    def sig(self, depth, sym="ETHUSDT"):
        return Signal(sym, 0.0, status="signal", fillable_usd=depth)

    def fake_pos(self, sym):
        return Position(sym, sym, 1.0, 0.0, 200.0, 1.0, 1.0, 100.0, 100.0, 50.0, 4.0)

    def test_size_is_capped_by_depth(self):
        self.assertEqual(self.risk.size_entry(self.sig(1_000_000), 0)[0], 200.0)
        self.assertEqual(self.risk.size_entry(self.sig(100), 0)[0], 50.0)  # 50% of top-of-book
        notional, why = self.risk.size_entry(self.sig(40), 0)
        self.assertEqual(notional, 0.0)
        self.assertIn("minimum", why)

    def test_thin_touch_is_refused_even_when_the_size_would_fit(self):
        """AVAUSDT: ~$50-110 at the touch produced a +69 bps 'signal' that lost 111 bps in 3 seconds."""
        self.cfg.risk.min_touch_depth_usd = 300.0
        notional, why = self.risk.size_entry(self.sig(100), 0)
        self.assertEqual(notional, 0.0)
        self.assertIn("thin touch", why)
        self.assertEqual(self.risk.size_entry(self.sig(400), 0)[0], 200.0)

    def test_exposure_caps(self):
        for i in range(5):
            self.pf.add(self.fake_pos(f"S{i}USDT"))
        self.assertEqual(self.risk.size_entry(self.sig(1e6), 0), (0.0, "max open positions"))

    def test_cooldown_after_exit(self):
        self.risk.note_exit("ETHUSDT", 100.0)
        self.assertEqual(self.risk.size_entry(self.sig(1e6), 101.0)[0], 0.0)
        self.assertGreater(self.risk.size_entry(self.sig(1e6), 100.0 + self.cfg.risk.symbol_cooldown_s + 1)[0], 0.0)

    def test_daily_loss_halts_and_flattens(self):
        pos = self.fake_pos("ETHUSDT")
        self.pf.add(pos)
        self.pf.close(pos, ClosedTrade("ETHUSDT", 0, 1_000_000.0, 200, -30.0, -1500, 50, 0, 3.0, 0, "stop-loss"))
        self.risk.refresh(1_000_001.0)
        self.assertTrue(self.risk.halted)
        self.assertTrue(self.risk.flatten_required)
        self.assertEqual(self.risk.size_entry(self.sig(1e6), 1_000_001.0)[0], 0.0)

    def test_kill_file(self):
        self.risk.refresh(0.0)
        self.assertFalse(self.risk.halted)
        Path(self.cfg.risk.kill_file).touch()
        self.risk.refresh(0.0)
        self.assertTrue(self.risk.kill_active and self.risk.flatten_required)
        Path(self.cfg.risk.kill_file).unlink()
        self.risk.refresh(0.0)
        self.assertFalse(self.risk.halted)

    def test_error_breaker_latches(self):
        self.assertFalse(self.risk.record_error())
        self.assertFalse(self.risk.record_error())
        self.assertTrue(self.risk.record_error())
        self.assertTrue(self.risk.halted)
        self.risk.manual_halt = False
        self.assertTrue(self.risk.halted)  # a fault is not clearable from the dashboard

    def test_state_survives_restart(self):
        path = Path(self.tmp) / "state.db"
        now = 2_000_000.0
        pf1 = Portfolio(Store(path), now)
        keep, done = self.fake_pos("AAAUSDT"), self.fake_pos("BBBUSDT")
        pf1.add(keep)
        pf1.add(done)
        pf1.close(done, ClosedTrade("BBBUSDT", now - 10, now, 200, -12.5, -625, 50, 0, 3.0, 0, "stop-loss"))
        pf2 = Portfolio(Store(path), now + 5)
        self.assertEqual(list(pf2.open), ["AAAUSDT"])
        self.assertAlmostEqual(pf2.realized_usd, -12.5)
        self.assertAlmostEqual(pf2.day_pnl(now + 5), -12.5)  # restart cannot reset the daily-loss limit
        self.assertEqual((pf2.wins, pf2.losses), (0, 1))


class LiveQtyTests(unittest.TestCase):
    def test_aligns_to_coarser_step(self):
        q, s = align_qty(1.2345, 1.0, 0.001, 0.01)
        self.assertEqual((q, s), (Decimal("1.23"), Decimal("1.23")))

    def test_multiplied_contract(self):
        q, s = align_qty(0.4567, 1000.0, 0.001, 1.0)
        self.assertEqual((q, s), (Decimal("0.456"), Decimal("456.000")))

    def test_impossible_size_returns_zero(self):
        q, s = align_qty(0.0004, 1.0, 0.001, 0.001)
        self.assertEqual((q, s), (Decimal(0), Decimal(0)))


class EngineCycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_dislocation_is_entered_then_exited_at_a_profit(self):
        tmp = tempfile.mkdtemp()
        cfg = small_cfg(tmp)
        board = Board("sim")
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        board.markets["ETHUSDT"] = m
        pf = Portfolio(Store(Path(":memory:")), 1000.0)
        risk = Risk(cfg.risk, pf)
        eng = Engine(cfg, board, Scanner(cfg.strategy, cfg.fees, cfg.feed.max_quote_age_s), pf, risk,
                     PaperExecutor(cfg.fees))
        t = 1000.0
        for i in range(30):  # quiet market: warm-up, nothing to trade
            set_quotes(m, 100.0, 2.0 + 0.3 * (-1) ** i, t)
            await eng.tick(now=t)
            t += 1
        self.assertEqual(len(pf.open), 0)

        set_quotes(m, 100.0, 90.0, t)  # perp suddenly 90 bps rich
        await eng.tick(now=t)
        self.assertIn("ETHUSDT", pf.open)
        entered_at = t

        for _ in range(12):  # basis snaps back
            t += 1
            set_quotes(m, 100.0, 2.0, t)
            await eng.tick(now=t)
        self.assertEqual(len(pf.open), 0)
        trade = pf.closed[-1]
        self.assertEqual(trade.reason, "converged")
        self.assertGreater(trade.pnl_usd, 0)
        self.assertGreaterEqual(trade.closed_at - entered_at, cfg.strategy.min_hold_s)
        self.assertEqual(pf.wins, 1)

    async def test_halted_engine_opens_nothing(self):
        tmp = tempfile.mkdtemp()
        cfg = small_cfg(tmp)
        board = Board("sim")
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        board.markets["ETHUSDT"] = m
        pf = Portfolio(Store(Path(":memory:")), 1000.0)
        risk = Risk(cfg.risk, pf)
        eng = Engine(cfg, board, Scanner(cfg.strategy, cfg.fees, cfg.feed.max_quote_age_s), pf, risk,
                     PaperExecutor(cfg.fees))
        eng.request_halt()
        t = 1000.0
        for i in range(30):
            set_quotes(m, 100.0, 2.0 + 0.3 * (-1) ** i, t)
            await eng.tick(now=t)
            t += 1
        set_quotes(m, 100.0, 90.0, t)
        await eng.tick(now=t)
        self.assertEqual(len(pf.open), 0)
        self.assertEqual(eng.snapshot()["stats"]["signals"], 1)  # still detected, just not traded


class ForcedExitTests(unittest.IsolatedAsyncioTestCase):
    async def _open_position(self):
        tmp = tempfile.mkdtemp()
        cfg = small_cfg(tmp)
        board = Board("sim")
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        board.markets["ETHUSDT"] = m
        pf = Portfolio(Store(Path(":memory:")), 1000.0)
        risk = Risk(cfg.risk, pf)
        eng = Engine(cfg, board, Scanner(cfg.strategy, cfg.fees, cfg.feed.max_quote_age_s), pf, risk,
                     PaperExecutor(cfg.fees))
        t = 1000.0
        for i in range(30):
            set_quotes(m, 100.0, 2.0 + 0.3 * (-1) ** i, t)
            await eng.tick(now=t)
            t += 1
        set_quotes(m, 100.0, 90.0, t)
        await eng.tick(now=t)
        self.assertIn("ETHUSDT", pf.open)
        return cfg, eng, m, pf, t

    async def test_kill_file_flattens_open_positions(self):
        cfg, eng, m, pf, t = await self._open_position()
        Path(cfg.risk.kill_file).touch()
        t += 1
        set_quotes(m, 100.0, 90.0, t)  # still dislocated: only the kill switch can explain an exit
        await eng.tick(now=t)
        self.assertEqual(len(pf.open), 0)
        self.assertEqual(pf.closed[-1].reason, "risk-flatten")
        t += 1
        await eng.tick(now=t)
        self.assertEqual(len(pf.open), 0)  # and nothing new is opened while the file exists

    async def test_stop_loss_when_basis_widens(self):
        cfg, eng, m, pf, t = await self._open_position()
        t += 1
        set_quotes(m, 100.0, 90.0 + cfg.strategy.stop_loss_bps + 5, t)
        await eng.tick(now=t)
        self.assertEqual(pf.closed[-1].reason, "stop-loss")
        self.assertLess(pf.closed[-1].pnl_usd, 0)

    async def test_stale_feed_forces_exit_after_grace(self):
        cfg, eng, m, pf, t = await self._open_position()
        last_fresh = t
        # No new quotes: the position is held through the grace window, then closed.
        await eng.tick(now=last_fresh + cfg.feed.max_quote_age_s + 1)
        self.assertEqual(len(pf.open), 1)
        await eng.tick(now=last_fresh + cfg.strategy.stale_force_exit_s + 1)
        self.assertEqual(pf.closed[-1].reason, "stale-feed")

    async def test_dashboard_flatten_request(self):
        cfg, eng, m, pf, t = await self._open_position()
        eng.request_flatten()
        t += 1
        set_quotes(m, 100.0, 90.0, t)
        await eng.tick(now=t)
        self.assertEqual(pf.closed[-1].reason, "manual-flatten")
        self.assertTrue(eng.risk.manual_halt)
        self.assertFalse(eng.flatten_requested)


if __name__ == "__main__":
    unittest.main()
