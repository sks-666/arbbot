"""The revised core entry formula: robust/frozen baseline, discrete funding, exit cost, risk buffer,
depth-based sizing, partial-fill handling, fee tiers and funding rules."""
import asyncio
import tempfile
import unittest
from decimal import ROUND_DOWN, Decimal
from pathlib import Path

from arbbot.binance_client import BinanceError
from arbbot.config import Config, FeesCfg
from arbbot.engine import Engine
from arbbot.executor import PaperExecutor, UnhedgedError
from arbbot.feeds import Board, RestFeed
from arbbot.live import LiveExecutor, hedge_excess
from arbbot.models import ClosedTrade, Market, Position, Quote, Signal, SymbolFilter
from arbbot.portfolio import Portfolio, Store
from arbbot.risk import Risk
from arbbot.scanner import Scanner, expected_funding_bps, percentile
from arbbot.sizing import Books, RestDepth, plan_size, walk
from tests.test_core import set_quotes, small_cfg


def engine(depth=None, trading=True):
    cfg = small_cfg(tempfile.mkdtemp())
    board = Board("sim")
    m = Market("ETHUSDT", "ETHUSDT", "ETH")
    board.markets["ETHUSDT"] = m
    pf = Portfolio(Store(Path(":memory:")), 1000.0)
    eng = Engine(cfg, board, Scanner(cfg.strategy, cfg.fees, cfg.feed.max_quote_age_s), pf, Risk(cfg.risk, pf),
                 PaperExecutor(cfg.fees), trading=trading, depth=depth)
    return eng, m, pf, cfg


async def warm(eng, m, n=30, spread=1.0):
    t = 1000.0
    for i in range(n):
        set_quotes(m, 100.0, 2.0 + 0.3 * (-1) ** i, t, spread_bps=spread)
        await eng.tick(now=t)
        t += 1
    return t


class BaselineTests(unittest.TestCase):
    def test_robust_baseline_is_not_inflated_by_a_spike_but_mean_std_is(self):
        normal = [2.0 + 0.3 * (-1) ** i for i in range(250)]
        spiked = normal + [60.0] * 50  # 50 s of the dislocation itself
        robust = Scanner(Config().strategy, Config().fees, 3.0)
        cfg = Config()
        cfg.strategy.baseline = "meanstd"
        classic = Scanner(cfg.strategy, cfg.fees, 3.0)
        mu_r, sd_r = robust.baseline(spiked)
        mu_c, sd_c = classic.baseline(spiked)
        self.assertAlmostEqual(mu_r, 2.0, delta=0.4)
        self.assertLess(sd_r, 1.5)
        self.assertGreater(mu_c, 10)   # the dislocation dragged the mean up...
        self.assertGreater(sd_c, 15)   # ...and blew up the std, so z collapses
        self.assertGreater((60 - mu_r) / sd_r, 30)

    def test_std_floor_applies(self):
        mu, sd = Scanner(Config().strategy, Config().fees, 3.0).baseline([5.0] * 100)
        self.assertEqual((mu, sd), (5.0, Config().strategy.std_floor_bps))


class FundingTests(unittest.TestCase):
    def market(self, next_in_s, rate=0.0001, interval=8.0, cap=None, floor=None, now=10_000.0):
        return Market("X", "X", "X", next_funding_ms=int((now + next_in_s) * 1000), funding_rate=rate,
                      funding_interval_h=interval, funding_cap=cap, funding_floor=floor)

    def test_no_funding_event_inside_the_hold_means_zero_carry(self):
        self.assertEqual(expected_funding_bps(self.market(3600), 10_000.0, 300), 0.0)

    def test_event_inside_the_hold_pays_the_whole_rate_not_a_prorated_slice(self):
        self.assertAlmostEqual(expected_funding_bps(self.market(120), 10_000.0, 300), 1.0)  # 0.01% = 1 bp

    def test_negative_funding_is_a_cost_for_the_short(self):
        self.assertAlmostEqual(expected_funding_bps(self.market(60, rate=-0.0003), 10_000.0, 300), -3.0)

    def test_short_funding_interval_can_cross_several_events(self):
        m = self.market(100, rate=0.0001, interval=1.0)  # hourly funding, 2.5 h hold
        self.assertAlmostEqual(expected_funding_bps(m, 10_000.0, 9000), 3.0)

    def test_stale_timestamp_rolls_forward(self):
        m = self.market(-3600 * 7.9, rate=0.0001)  # "next" funding time passed 7.9 h ago -> the next one is in 6 min
        self.assertEqual(expected_funding_bps(m, 10_000.0, 300), 0.0)
        self.assertAlmostEqual(expected_funding_bps(m, 10_000.0, 600), 1.0)

    def test_exchange_cap_and_floor_clip_the_rate(self):
        self.assertAlmostEqual(expected_funding_bps(self.market(60, rate=0.01, cap=0.0075), 10_000.0, 300), 75.0)
        self.assertAlmostEqual(expected_funding_bps(self.market(60, rate=-0.01, floor=-0.0075), 10_000.0, 300), -75.0)

    def test_scanner_only_counts_funding_that_is_actually_crossed(self):
        cfg = Config()
        cfg.strategy.min_samples = 20
        sc = Scanner(cfg.strategy, cfg.fees, 3.0)
        m = Market("ETHUSDT", "ETHUSDT", "ETH", funding_rate=0.0004)
        t = 1000.0
        for i in range(30):
            set_quotes(m, 100.0, 2.0 + 0.3 * (-1) ** i, t + i)
            sc.evaluate(m, t + i)
        t += 30
        set_quotes(m, 100.0, 60.0, t)
        m.next_funding_ms = int((t + 3600) * 1000)
        far = sc.evaluate(m, t)
        m.next_funding_ms = int((t + 60) * 1000)
        set_quotes(m, 100.0, 60.0, t + 1)
        near = sc.evaluate(m, t + 1)
        self.assertEqual(far.carry_bps, 0.0)
        self.assertAlmostEqual(near.carry_bps, 4.0)
        self.assertAlmostEqual(near.net_edge_bps - far.net_edge_bps, 4.0, delta=0.5)


class EdgeComponentTests(unittest.TestCase):
    def setUp(self):
        self.cfg = Config()
        self.cfg.strategy.min_samples = 20

    def warmed(self, spreads, risk_buffer=None):
        if risk_buffer is not None:
            self.cfg.strategy.risk_buffer_bps = risk_buffer
        sc = Scanner(self.cfg.strategy, self.cfg.fees, 3.0)
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        t = 1000.0
        for i, sp in enumerate(spreads):
            set_quotes(m, 100.0, 2.0 + 0.3 * (-1) ** i, t + i, spread_bps=sp)
            sc.evaluate(m, t + i)
        return sc, m, t + len(spreads)

    def test_exit_cost_uses_a_conservative_percentile_not_todays_tight_spread(self):
        sc, m, t = self.warmed([1.0, 8.0] * 15)
        set_quotes(m, 100.0, 60.0, t, spread_bps=1.0)  # today the spread happens to be tight
        sig = sc.evaluate(m, t)
        today = sig.basis_out_bps - sig.basis_mid_bps
        self.assertAlmostEqual(today, 1.0, delta=0.2)
        self.assertGreater(sig.exit_cost_bps, 7.0)  # ~p75 of the recent 1 / 8 bp spreads
        self.assertAlmostEqual(sig.gross_bps, sig.basis_in_bps - (sig.target_mid_bps + sig.exit_cost_bps), places=6)

    def test_net_edge_subtracts_the_risk_buffer(self):
        sc0, m0, t0 = self.warmed([1.0] * 30, risk_buffer=0.0)
        set_quotes(m0, 100.0, 60.0, t0)
        net0 = sc0.evaluate(m0, t0).net_edge_bps
        sc1, m1, t1 = self.warmed([1.0] * 30, risk_buffer=10.0)
        set_quotes(m1, 100.0, 60.0, t1)
        sig1 = sc1.evaluate(m1, t1)
        self.assertAlmostEqual(net0 - sig1.net_edge_bps, 10.0, places=6)
        self.assertEqual(sig1.risk_buffer_bps, 10.0)

    def test_a_big_buffer_removes_marginal_signals(self):
        sc, m, t = self.warmed([1.0] * 30, risk_buffer=2.0)
        set_quotes(m, 100.0, 45.0, t)  # ~+7 bps net: fine with a 2 bp buffer
        self.assertEqual(sc.evaluate(m, t).status, "signal")
        sc.risk_buffer_bps = 20.0
        set_quotes(m, 100.0, 45.0, t + 1)
        self.assertNotEqual(sc.evaluate(m, t + 1).status, "signal")

    def test_percentile_nearest_rank(self):
        self.assertEqual(percentile([1, 2, 3, 4], 0.75), 3)
        self.assertEqual(percentile([5], 0.95), 5)
        self.assertEqual(percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0.95), 10)


class FrozenBaselineEngineTests(unittest.IsolatedAsyncioTestCase):
    async def test_position_is_not_declared_converged_while_the_basis_is_still_abnormal(self):
        eng, m, pf, cfg = engine()
        t = await warm(eng, m)
        set_quotes(m, 100.0, 90.0, t)
        await eng.tick(now=t)
        pos = pf.open["ETHUSDT"]
        self.assertAlmostEqual(pos.mu_entry, 2.0, delta=0.5)
        self.assertGreater(pos.sigma_entry, 0)
        self.assertAlmostEqual(pos.entry_mid_bps, 90.0, delta=1.0)
        for k in range(1, 25):  # basis falls part-way and then SITS at 40 bps for 24 s (normal is 2)
            set_quotes(m, 100.0, 40.0, t + k)
            await eng.tick(now=t + k)
        self.assertIn("ETHUSDT", pf.open, "a rolling baseline would have been dragged up and called this converged")
        for k in range(25, 40):  # now it really returns to normal
            set_quotes(m, 100.0, 2.0 + 0.3 * (-1) ** k, t + k)
            await eng.tick(now=t + k)
        self.assertNotIn("ETHUSDT", pf.open)
        self.assertEqual(pf.closed[-1].reason, "converged")
        self.assertGreater(pf.closed[-1].pnl_usd, 0)

    async def test_a_momentarily_wide_spread_does_not_trigger_a_basis_stop(self):
        eng, m, pf, cfg = engine()
        t = await warm(eng, m)
        set_quotes(m, 100.0, 90.0, t)
        await eng.tick(now=t)
        for k in range(1, 6):  # spread blows out to 8 bps, mid basis unchanged at 90
            set_quotes(m, 100.0, 90.0, t + k, spread_bps=8.0)
            await eng.tick(now=t + k)
        self.assertIn("ETHUSDT", pf.open)

    async def test_pnl_stop_fires_when_closing_now_would_lose_too_much(self):
        eng, m, pf, cfg = engine()
        t = await warm(eng, m)
        set_quotes(m, 100.0, 90.0, t)
        await eng.tick(now=t)
        set_quotes(m, 100.0, 90.0 + cfg.strategy.stop_loss_bps + 15, t + 1)  # basis widens against the short
        await eng.tick(now=t + 1)
        self.assertEqual(pf.closed[-1].reason, "stop-loss")
        self.assertLess(pf.closed[-1].pnl_usd, 0)

    async def test_legacy_position_without_a_frozen_baseline_still_exits_sensibly(self):
        eng, m, pf, cfg = engine()
        t = await warm(eng, m)
        set_quotes(m, 100.0, 90.0, t)
        await eng.tick(now=t)
        pos = pf.open["ETHUSDT"]
        pos.sigma_entry = 0.0  # as if saved by an older version
        for k in range(1, 15):
            set_quotes(m, 100.0, 2.0 + 0.3 * (-1) ** k, t + k)
            await eng.tick(now=t + k)
        self.assertNotIn("ETHUSDT", pf.open)


class AdaptiveBufferTests(unittest.TestCase):
    def trade(self, predicted, realised):
        return ClosedTrade("X", 0, 1, 200, realised / 50, realised, 50, 0, 0.5, 0, "converged", predicted_net_bps=predicted)

    def buffer(self, trades, **cfg_changes):
        eng, m, pf, cfg = engine()
        for k, v in cfg_changes.items():
            setattr(cfg.strategy, k, v)
        for t in trades:
            pf.closed.append(t)
        eng._update_buffer()
        return eng.scanner.risk_buffer_bps

    def test_buffer_is_the_floor_until_enough_trades_exist(self):
        self.assertEqual(self.buffer([self.trade(20, -40)] * 9), 2.0)

    def test_buffer_grows_to_the_p95_model_error_and_is_capped(self):
        self.assertEqual(self.buffer([self.trade(20, -40)] * 12), 30.0)  # shortfall 60, capped at 30
        self.assertEqual(self.buffer([self.trade(20, 15)] * 9 + [self.trade(20, 3)]), 17.0)  # the one 17 bp shortfall is the p95

    def test_a_model_that_beats_its_predictions_keeps_the_floor(self):
        self.assertEqual(self.buffer([self.trade(10, 25)] * 12), 2.0)

    def test_trades_without_a_prediction_are_ignored(self):
        self.assertEqual(self.buffer([self.trade(0, -90)] * 20), 2.0)

    def test_can_be_switched_off(self):
        self.assertEqual(self.buffer([self.trade(20, -40)] * 12, adaptive_buffer=False), 2.0)


class DepthSizingTests(unittest.TestCase):
    def setUp(self):
        self.m = Market("ETHUSDT", "ETHUSDT", "ETH")
        self.cfg = Config()
        self.sig = Signal("ETHUSDT", 0.0, basis_mid_bps=90.0, target_mid_bps=2.25, exit_cost_bps=1.5, carry_bps=0.0,
                          cost_bps=34.0, risk_buffer_bps=2.0)

    def books(self, perp_bids, spot_asks=None):
        return Books(spot_bids=[(99.9, 1000)], spot_asks=spot_asks or [(100.0, 1000)],
                     perp_bids=perp_bids, perp_asks=[(100.5, 1000)])

    def plan(self, books, notional=200.0):
        return plan_size(self.m, self.sig, notional, books, self.cfg.strategy, self.cfg.risk.min_trade_usd,
                         self.cfg.risk.size_steps)

    def test_walk_gives_the_volume_weighted_price(self):
        self.assertAlmostEqual(walk([(100.0, 0.5), (99.0, 100.0)], 2.0), (0.5 * 100 + 1.5 * 99) / 2.0)
        self.assertEqual(walk([(100.0, 1.0)], 1.0), 100.0)
        self.assertIsNone(walk([(100.0, 1.0)], 1.5))
        self.assertIsNone(walk([], 1.0))
        self.assertIsNone(walk([(100.0, 1.0)], 0))

    def test_deep_book_gets_the_full_size(self):
        p = self.plan(self.books([(101.0, 1000)]))
        self.assertAlmostEqual(p.notional, 200.0)
        self.assertAlmostEqual(p.entry_basis_bps, 100.0, places=3)
        self.assertGreater(p.net_edge_bps, 50)

    def test_thin_top_of_book_shrinks_the_position_until_the_edge_survives(self):
        thin = self.books([(101.0, 0.5), (98.0, 1000)])  # only 0.5 contracts at the good price
        p = self.plan(thin)
        self.assertIsNotNone(p)
        self.assertLess(p.notional, 200.0)
        self.assertAlmostEqual(p.notional, 40.0)         # 0.2 x 200: fits inside the 0.5 contracts
        self.assertGreater(p.entry_basis_bps, 90)

    def test_no_size_works_when_even_the_smallest_cannot_fill(self):
        self.assertIsNone(self.plan(self.books([(101.0, 0.1)])))

    def test_edge_that_only_exists_at_the_top_of_a_thin_book_is_rejected(self):
        self.sig.basis_mid_bps = 40.0   # a modest dislocation: top-of-book edge is thin
        self.assertIsNone(self.plan(self.books([(100.4, 0.1), (99.0, 1000)])))

    def test_multiplier_is_respected(self):
        m = Market("1000SHIBUSDT", "SHIBUSDT", "SHIB", mult=1000.0)
        books = Books(spot_bids=[(0.01999, 1e9)], spot_asks=[(0.02, 1e9)], perp_bids=[(20.2, 1e6)], perp_asks=[(20.21, 1e6)])
        p = plan_size(m, self.sig, 200.0, books, self.cfg.strategy, 25.0, [1.0])
        self.assertAlmostEqual(p.entry_basis_bps, 100.0, places=3)  # 20.2 vs 0.02*1000


class PaperVwapTests(unittest.IsolatedAsyncioTestCase):
    async def test_paper_fills_at_the_book_vwap_when_depth_is_given(self):
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        m.spot = Quote(99.9, 100, 100.0, 100, 0.0)
        m.perp = Quote(100.0, 0.5, 100.1, 100, 0.0)
        books = Books(spot_bids=[(99.9, 100)], spot_asks=[(100.0, 100)], perp_bids=[(100.0, 0.5), (99.0, 100)],
                      perp_asks=[(100.1, 100)])
        ex = PaperExecutor(FeesCfg(0.0, 0.0, 0.0))
        sig = Signal("ETHUSDT", 0.0, basis_in_bps=0.0)
        flat = await ex.open(m, sig, 200.0, 0.0)
        deep = await ex.open(m, sig, 200.0, 0.0, books=books)
        self.assertAlmostEqual(flat.perp_entry, 100.0)
        self.assertAlmostEqual(deep.perp_entry, 99.25)  # 0.5 @ 100 + 1.5 @ 99 over 2 contracts
        self.assertLess(deep.entry_basis_bps, 0)        # and the recorded entry basis reflects the real fill

    async def test_entry_stores_the_frozen_baseline_and_the_prediction(self):
        m = Market("ETHUSDT", "ETHUSDT", "ETH", funding_interval_h=4.0)
        m.spot, m.perp = Quote(99.9, 100, 100.0, 100, 0.0), Quote(100.9, 100, 101.0, 100, 0.0)
        sig = Signal("ETHUSDT", 0.0, mean_bps=2.0, std_bps=0.5, basis_mid_bps=90.0, net_edge_bps=17.0)
        pos = await PaperExecutor(FeesCfg()).open(m, sig, 200.0, 0.0)
        self.assertEqual((pos.mu_entry, pos.sigma_entry, pos.entry_mid_bps, pos.predicted_net_bps, pos.funding_interval_h),
                         (2.0, 0.5, 90.0, 17.0, 4.0))


class FakeDepth:
    def __init__(self, books):
        self.books_value, self.calls = books, 0

    async def books(self, m):
        self.calls += 1
        return self.books_value


class EngineDepthTests(unittest.IsolatedAsyncioTestCase):
    async def test_depth_plan_sets_the_size_and_the_prediction(self):
        thin = Books(spot_bids=[(99.9, 1e6)], spot_asks=[(100.0, 1e6)], perp_bids=[(100.9, 0.5), (96.0, 1e6)],
                     perp_asks=[(100.5, 1e6)])
        eng, m, pf, cfg = engine(depth=FakeDepth(thin))
        t = await warm(eng, m)
        set_quotes(m, 100.0, 90.0, t)
        await eng.tick(now=t)
        pos = pf.open["ETHUSDT"]
        self.assertLess(pos.notional, 200.0)  # shrunk to what the book can absorb
        self.assertGreater(pos.predicted_net_bps, cfg.strategy.min_net_edge_bps)

    async def test_signal_that_dies_at_executable_size_is_skipped_and_not_hammered(self):
        hopeless = Books(spot_bids=[(99.9, 1e6)], spot_asks=[(100.0, 1e6)], perp_bids=[(100.9, 0.01)], perp_asks=[(100.5, 1e6)])
        depth = FakeDepth(hopeless)
        eng, m, pf, cfg = engine(depth=depth)
        t = await warm(eng, m)
        for k in range(4):
            set_quotes(m, 100.0, 90.0, t + k)
            await eng.tick(now=t + k)
        self.assertEqual(len(pf.open), 0)
        self.assertEqual(depth.calls, 1)  # blocked for 5 s after the first failed check
        self.assertTrue(any("SKIP ETHUSDT" in e["msg"] for e in eng.events))

    async def test_depth_outage_falls_back_to_top_of_book(self):
        eng, m, pf, cfg = engine(depth=FakeDepth(None))
        t = await warm(eng, m)
        set_quotes(m, 100.0, 90.0, t)
        await eng.tick(now=t)
        self.assertIn("ETHUSDT", pf.open)


class PersistenceTests(unittest.TestCase):
    def test_positions_saved_by_an_older_version_still_load(self):
        store = Store(Path(":memory:"))
        old = ('{"symbol": "ETHUSDT", "spot_symbol": "ETHUSDT", "mult": 1.0, "opened_at": 5.0, "notional": 200.0, '
               '"spot_qty": 2.0, "perp_qty": 2.0, "spot_entry": 100.0, "perp_entry": 101.0, "entry_basis_bps": 90.0, '
               '"entry_z": 5.0, "fees_paid": 1.0, "funding_acc": 0.0, "next_funding_ms": 0, "last_fresh": 5.0, "live": false}')
        store.db.execute("INSERT INTO positions(symbol, json) VALUES (?, ?)", ("ETHUSDT", old))
        pos = Portfolio(store, 10.0).open["ETHUSDT"]
        self.assertEqual((pos.mu_entry, pos.sigma_entry, pos.funding_interval_h), (0.0, 0.0, 8.0))

    def test_new_fields_round_trip(self):
        store = Store(Path(":memory:"))
        pf = Portfolio(store, 10.0)
        pf.add(Position("ETHUSDT", "ETHUSDT", 1.0, 5.0, 200.0, 2.0, 2.0, 100.0, 101.0, 90.0, 5.0, mu_entry=2.0,
                        sigma_entry=0.5, entry_mid_bps=90.0, predicted_net_bps=17.5, funding_interval_h=4.0))
        back = Portfolio(store, 10.0).open["ETHUSDT"]
        self.assertEqual((back.mu_entry, back.sigma_entry, back.entry_mid_bps, back.predicted_net_bps), (2.0, 0.5, 90.0, 17.5))


class HedgeExcessTests(unittest.TestCase):
    def test_matched_legs(self):
        self.assertIsNone(hedge_excess(Decimal("2"), Decimal("1.998"), 1.0, 0.001, 0.001))

    def test_excess_spot_is_reported_in_spot_units(self):
        self.assertEqual(hedge_excess(Decimal("1.2"), Decimal("1.998"), 1.0, 0.001, 0.001), ("spot", Decimal("0.798")))

    def test_excess_short_is_reported_in_contracts(self):
        side, q = hedge_excess(Decimal("2.0"), Decimal("1.1988"), 1.0, 0.001, 0.001)
        self.assertEqual((side, q), ("perp", Decimal("0.801")))

    def test_multiplier_scales_the_comparison(self):
        self.assertIsNone(hedge_excess(Decimal("0.5"), Decimal("500"), 1000.0, 0.001, 1.0))
        self.assertEqual(hedge_excess(Decimal("0.5"), Decimal("300"), 1000.0, 0.001, 1.0), ("perp", Decimal("0.200")))

    def test_dust_below_a_lot_step_is_ignored(self):
        self.assertIsNone(hedge_excess(Decimal("10"), Decimal("10.0005"), 1.0, 0.001, 0.01))


def _floor3(x: Decimal) -> Decimal:
    return (x / Decimal("0.001")).to_integral_value(rounding=ROUND_DOWN) * Decimal("0.001")


class FakeExchange:
    """Programmable Binance: per-order fill fractions for each venue; fractions repeat their last value."""

    def __init__(self, fut=(1.0,), spot=(1.0,), price=100.0, fail=()):
        self.fut, self.spot, self.price, self.fail = list(fut), list(spot), price, set(fail)
        self.log: list[tuple] = []
        self.n = 0

    @staticmethod
    def _next(seq):
        return seq.pop(0) if len(seq) > 1 else seq[0]

    async def post(self, venue, path, params=None, signed=True):
        params = params or {}
        if path in ("/fapi/v1/marginType", "/fapi/v1/leverage"):
            return {}
        side = params["side"]
        if (venue, side) in self.fail:
            raise BinanceError(400, -2010, "insufficient balance", path)
        qty = Decimal(params["quantity"])
        self.n += 1
        if venue == "fapi":
            frac = self._next(self.fut)
            ex = _floor3(qty * Decimal(str(frac)))
            self.log.append(("fut", side, ex, params.get("reduceOnly")))
            return {"orderId": self.n, "status": "FILLED" if frac >= 1 else "EXPIRED", "executedQty": str(ex),
                    "avgPrice": str(self.price)}
        frac = self._next(self.spot)
        ex = _floor3(qty * Decimal(str(frac)))
        self.log.append(("spot", side, ex))
        commission = ex * Decimal("0.001") if side == "BUY" else ex * Decimal(str(self.price)) * Decimal("0.001")
        asset = "ETH" if side == "BUY" else "USDT"
        return {"orderId": self.n, "status": "FILLED" if frac >= 1 else "EXPIRED", "executedQty": str(ex),
                "cummulativeQuoteQty": str(ex * Decimal(str(self.price))),
                "fills": [{"price": str(self.price), "qty": str(ex), "commission": str(commission), "commissionAsset": asset}]}

    async def get(self, venue, path, params=None, signed=False):
        raise AssertionError(f"unexpected GET {path}")

    async def request(self, *a, **k):
        raise AssertionError("unexpected request")


def live_market():
    m = Market("ETHUSDT", "ETHUSDT", "ETH")
    m.fut_filter = SymbolFilter(0.001, 0.001, 5.0)
    m.spot_filter = SymbolFilter(0.001, 0.001, 5.0)
    m.spot = Quote(99.9, 100, 100.0, 100, 0.0)
    m.perp = Quote(100.0, 100, 100.1, 100, 0.0)
    return m


def held_position(**kw):
    return Position("ETHUSDT", "ETHUSDT", 1.0, 0.0, 200.0, 2.0, 2.0, 100.0, 101.0, 90.0, 5.0, live=True, **kw)


class LivePartialFillTests(unittest.IsolatedAsyncioTestCase):
    def sig(self):
        return Signal("ETHUSDT", 0.0, basis_in_bps=90.0, mean_bps=2.0, std_bps=0.5, basis_mid_bps=90.0, net_edge_bps=17.0)

    def ex(self, **kw):
        return LiveExecutor(FakeExchange(**kw), Config())

    async def test_clean_fills_give_a_matched_position(self):
        ex = self.ex()
        pos = await ex.open(live_market(), self.sig(), 200.0, 5.0)
        self.assertAlmostEqual(pos.perp_qty, 2.0)
        self.assertAlmostEqual(pos.spot_qty, 1.998)  # 0.1% spot fee taken in the base asset
        self.assertEqual((pos.mu_entry, pos.predicted_net_bps), (2.0, 17.0))
        self.assertTrue(pos.live)

    async def test_partial_futures_fill_sells_the_surplus_spot(self):
        ex = self.ex(fut=(0.6,))
        pos = await ex.open(live_market(), self.sig(), 200.0, 5.0)
        self.assertAlmostEqual(pos.perp_qty, 1.2)
        self.assertAlmostEqual(pos.spot_qty, 1.2, places=3)  # matched again
        self.assertIn(("spot", "SELL", Decimal("0.798")), ex.c.log)
        self.assertGreater(pos.fees_paid, 0)

    async def test_partial_spot_fill_buys_back_the_surplus_short(self):
        ex = self.ex(spot=(0.6,))
        pos = await ex.open(live_market(), self.sig(), 200.0, 5.0)
        self.assertIn(("fut", "BUY", Decimal("0.801"), "true"), ex.c.log)  # reduce-only
        self.assertAlmostEqual(pos.perp_qty, 1.199, places=3)
        self.assertAlmostEqual(pos.spot_qty, 1.1988, places=3)

    async def test_failed_rebalance_is_an_unhedged_fault(self):
        ex = self.ex(fut=(0.6,), fail={("spot", "SELL")})
        with self.assertRaises(UnhedgedError):
            await ex.open(live_market(), self.sig(), 200.0, 5.0)

    async def test_one_leg_failing_outright_unwinds_the_other(self):
        ex = self.ex(fail={("spot", "BUY")})
        with self.assertRaises(RuntimeError) as cm:
            await ex.open(live_market(), self.sig(), 200.0, 5.0)
        self.assertNotIsInstance(cm.exception, UnhedgedError)
        self.assertIn(("fut", "BUY", Decimal("2.000"), "true"), ex.c.log)

    async def test_close_tops_up_a_partial_fill(self):
        ex = self.ex(fut=(0.5, 1.0))
        pos = held_position(fees_paid=1.0, predicted_net_bps=17.0)
        trade = await ex.close(pos, live_market(), Signal("ETHUSDT", 1.0, basis_out_bps=3.0), 9.0, "converged")
        fut_buys = [e for e in ex.c.log if e[0] == "fut"]
        self.assertEqual([e[2] for e in fut_buys], [Decimal("1.000"), Decimal("1.000")])  # 50% then the remaining half
        self.assertEqual(trade.predicted_net_bps, 17.0)
        self.assertTrue(trade.live)
        self.assertAlmostEqual(trade.pnl_usd, 2.0 * (101.0 - 100.0) - trade.fees_usd, delta=0.3)

    async def test_close_with_a_stuck_remainder_is_an_unhedged_fault(self):
        ex = self.ex(fut=(0.5,))
        with self.assertRaises(UnhedgedError) as cm:
            await ex.close(held_position(), live_market(), Signal("ETHUSDT", 1.0), 9.0, "converged")
        self.assertIn("STILL SHORT", str(cm.exception))

    async def test_dust_remainder_is_not_a_fault(self):
        ex = self.ex(fut=(0.9999,))  # leaves 0.001 contracts ($0.10), far below the $5 minimum notional
        trade = await ex.close(held_position(), live_market(), Signal("ETHUSDT", 1.0), 9.0, "converged")
        self.assertTrue(trade.live)


class FakeAccountClient:
    def __init__(self, depth_fails=False):
        self.depth_fails = depth_fails
        self.timeouts = []

    async def get(self, venue, path, params=None, signed=False, timeout=None):
        self.timeouts.append(timeout)
        if path == "/fapi/v1/commissionRate":
            return {"symbol": "BTCUSDT", "makerCommissionRate": "0.000200", "takerCommissionRate": "0.000400"}
        if path == "/api/v3/account":
            return {"commissionRates": {"maker": "0.00075000", "taker": "0.00075000"}, "balances": []}
        if path == "/fapi/v1/depth":
            if self.depth_fails:
                raise BinanceError(429, None, "slow down", path)
            return {"bids": [["100.0", "2.0"], ["99.5", "5.0"]], "asks": [["100.1", "1.0"]]}
        if path == "/api/v3/depth":
            return {"bids": [["99.9", "3.0"]], "asks": [["100.0", "4.0"], ["100.2", "9.0"]]}
        raise AssertionError(path)


class FeeAndDepthFetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_fee_tier_is_read_from_the_account(self):
        fut_bps, spot_bps = await LiveExecutor(FakeAccountClient(), Config()).fetch_fees()
        self.assertAlmostEqual(fut_bps, 4.0)
        self.assertAlmostEqual(spot_bps, 7.5)

    async def test_rest_depth_parses_both_books(self):
        books = await RestDepth(FakeAccountClient()).books(Market("ETHUSDT", "ETHUSDT", "ETH"))
        self.assertEqual(books.perp_bids[0], (100.0, 2.0))
        self.assertEqual(books.spot_asks[1], (100.2, 9.0))

    async def test_rest_depth_failure_returns_none_instead_of_raising(self):
        self.assertIsNone(await RestDepth(FakeAccountClient(depth_fails=True)).books(Market("ETHUSDT", "ETHUSDT", "ETH")))

    async def test_depth_requests_use_a_short_timeout_and_failures_are_counted(self):
        client = FakeAccountClient()
        depth = RestDepth(client)
        await depth.books(Market("ETHUSDT", "ETHUSDT", "ETH"))
        self.assertEqual(client.timeouts, [1.5, 1.5])  # a slow book is a stale book
        bad = RestDepth(FakeAccountClient(depth_fails=True))
        for _ in range(3):
            await bad.books(Market("ETHUSDT", "ETHUSDT", "ETH"))
        self.assertEqual(bad.failures, 3)  # never silently swallowed after the first warning


class StaleSignalRegressionTests(unittest.IsolatedAsyncioTestCase):
    """Real trade ZECUSDT 21:00: signal at +32 bps, 4 s wait for the book, filled at -11 bps, lost 33.6 bps."""

    async def test_signal_that_vanishes_while_waiting_for_the_book_is_not_traded(self):
        class MovingDepth:
            ts = 0.0

            async def books(self, mk):
                set_quotes(mk, 100.0, -10.0, self.ts)  # the dislocation closes while we wait on the network
                return Books(spot_bids=[(99.9, 1e6)], spot_asks=[(100.0, 1e6)], perp_bids=[(100.89, 1e6)], perp_asks=[(100.91, 1e6)])

        depth = MovingDepth()
        eng, m, pf, cfg = engine(depth=depth)
        t = await warm(eng, m)
        set_quotes(m, 100.0, 90.0, t)
        depth.ts = t
        await eng.tick(now=t)
        self.assertEqual(len(pf.open), 0)
        self.assertTrue(any("signal gone" in e["msg"] for e in eng.events))

    async def test_a_signal_that_survives_the_wait_is_traded_on_the_fresh_quotes(self):
        class SlowDepth:
            async def books(self, mk):
                await asyncio.sleep(0.06)
                return Books(spot_bids=[(99.9, 1e6)], spot_asks=[(100.0, 1e6)], perp_bids=[(100.89, 1e6)], perp_asks=[(100.91, 1e6)])

        eng, m, pf, cfg = engine(depth=SlowDepth())
        t = await warm(eng, m)
        set_quotes(m, 100.0, 90.0, t)
        await eng.tick(now=t)
        pos = pf.open["ETHUSDT"]
        self.assertGreaterEqual(pos.opened_at, t + 0.05)  # stamped when it really filled, so min_hold counts real seconds
        self.assertLess(pos.opened_at, t + 1.0)

    async def test_recorded_entry_basis_is_the_basis_actually_obtained(self):
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        m.spot, m.perp = Quote(99.9, 100, 100.0, 100, 0.0), Quote(99.9, 100, 100.0, 100, 0.0)  # perp bid 10 bps BELOW spot ask
        sig = Signal("ETHUSDT", 0.0, basis_in_bps=32.3)  # what the stale signal claimed
        pos = await PaperExecutor(FeesCfg(0.0, 0.0, 0.0)).open(m, sig, 200.0, 0.0)
        self.assertAlmostEqual(pos.entry_basis_bps, -10.0, delta=0.2)


class FakeUniverseClient:
    def __init__(self, funding_ok=True):
        self.funding_ok = funding_ok

    async def get(self, venue, path, params=None, signed=False):
        def sym(s, base):
            return {"symbol": s, "contractType": "PERPETUAL", "status": "TRADING", "quoteAsset": "USDT",
                    "baseAsset": base, "filters": []}
        if path == "/fapi/v1/exchangeInfo":
            return {"symbols": [sym("BTCUSDT", "BTC"), sym("FOOUSDT", "FOO"), sym("1000SHIBUSDT", "1000SHIB")]}
        if path == "/fapi/v1/ticker/24hr":
            return [{"symbol": s, "quoteVolume": "5e9"} for s in ("BTCUSDT", "FOOUSDT", "1000SHIBUSDT")]
        if path == "/api/v3/ticker/bookTicker":
            return [{"symbol": s, "bidPrice": "1", "askPrice": "1.1"} for s in ("BTCUSDT", "FOOUSDT", "SHIBUSDT")]
        if path == "/fapi/v1/fundingInfo":
            if not self.funding_ok:
                raise BinanceError(500, None, "boom", path)
            return [{"symbol": "FOOUSDT", "adjustedFundingRateCap": "0.02500000", "adjustedFundingRateFloor": "-0.02500000",
                     "fundingIntervalHours": 4}]
        raise AssertionError(path)


class FundingRuleTests(unittest.IsolatedAsyncioTestCase):
    async def test_funding_interval_and_caps_come_from_the_exchange(self):
        board = Board("rest")
        await RestFeed(FakeUniverseClient(), board, Config()).load_universe()
        btc, foo, shib = board.markets["BTCUSDT"], board.markets["FOOUSDT"], board.markets["1000SHIBUSDT"]
        self.assertEqual((btc.funding_interval_h, btc.funding_cap), (8.0, None))
        self.assertEqual((foo.funding_interval_h, foo.funding_cap, foo.funding_floor), (4.0, 0.025, -0.025))
        self.assertEqual((shib.mult, shib.spot_symbol), (1000.0, "SHIBUSDT"))  # contract multiplier handled

    async def test_missing_funding_info_falls_back_to_defaults(self):
        board = Board("rest")
        await RestFeed(FakeUniverseClient(funding_ok=False), board, Config()).load_universe()
        self.assertEqual(board.markets["FOOUSDT"].funding_interval_h, 8.0)


if __name__ == "__main__":
    unittest.main()
