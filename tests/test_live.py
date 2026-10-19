"""Startup reconciliation of the bot's saved positions against what the exchange reports (mocked client)."""
import unittest

from arbbot.binance_client import BinanceError
from arbbot.config import Config
from arbbot.live import LiveExecutor
from arbbot.models import Market, Position


class FakeClient:
    def __init__(self, risk, balances, v3_missing=False):
        self.risk, self.balances, self.v3_missing = risk, balances, v3_missing
        self.paths = []

    async def get(self, venue, path, params=None, signed=False):
        self.paths.append(path)
        if path == "/fapi/v3/positionRisk":
            if self.v3_missing:
                raise BinanceError(404, None, "not found", path)
            return self.risk
        if path == "/fapi/v2/positionRisk":
            return self.risk
        if path == "/api/v3/account":
            return {"balances": self.balances}
        raise AssertionError(path)


def pos(sym="ETHUSDT", perp=2.0, spot=1.998):
    return Position(sym, sym, 1.0, 0.0, 200.0, spot, perp, 100.0, 100.0, 50.0, 4.0, live=True)


MARKETS = {s: Market(s, s, s[:-4]) for s in ("ETHUSDT", "BTCUSDT")}


def exec_with(risk, balances, **kw):
    return LiveExecutor(FakeClient(risk, balances, **kw), Config())


class ReconcileTests(unittest.IsolatedAsyncioTestCase):
    async def test_matching_state_has_no_problems(self):
        ex = exec_with([{"symbol": "ETHUSDT", "positionAmt": "-2.0"}, {"symbol": "BTCUSDT", "positionAmt": "0.0"}],
                       [{"asset": "ETH", "free": "1.998", "locked": "0"}, {"asset": "USDT", "free": "50", "locked": "0"}])
        self.assertEqual(await ex.reconcile({"ETHUSDT": pos()}, MARKETS), [])

    async def test_empty_book_and_empty_exchange_is_clean(self):
        ex = exec_with([{"symbol": "ETHUSDT", "positionAmt": "0.000"}], [])
        self.assertEqual(await ex.reconcile({}, MARKETS), [])

    async def test_missing_short_is_reported(self):
        ex = exec_with([], [{"asset": "ETH", "free": "2.0", "locked": "0"}])
        problems = await ex.reconcile({"ETHUSDT": pos()}, MARKETS)
        self.assertEqual(len(problems), 1)
        self.assertIn("positionAmt 0", problems[0])

    async def test_wrong_size_and_wrong_side_are_reported(self):
        ex = exec_with([{"symbol": "ETHUSDT", "positionAmt": "-1.0"}], [{"asset": "ETH", "free": "2", "locked": "0"}])
        self.assertEqual(len(await ex.reconcile({"ETHUSDT": pos()}, MARKETS)), 1)
        ex = exec_with([{"symbol": "ETHUSDT", "positionAmt": "2.0"}], [{"asset": "ETH", "free": "2", "locked": "0"}])
        self.assertEqual(len(await ex.reconcile({"ETHUSDT": pos()}, MARKETS)), 1)  # long, not short

    async def test_missing_spot_inventory_is_reported(self):
        ex = exec_with([{"symbol": "ETHUSDT", "positionAmt": "-2.0"}], [{"asset": "ETH", "free": "0.5", "locked": "0"}])
        problems = await ex.reconcile({"ETHUSDT": pos()}, MARKETS)
        self.assertEqual(len(problems), 1)
        self.assertIn("wallet holds 0.5", problems[0])

    async def test_locked_balance_counts_as_held(self):
        ex = exec_with([{"symbol": "ETHUSDT", "positionAmt": "-2.0"}], [{"asset": "ETH", "free": "1.0", "locked": "1.0"}])
        self.assertEqual(await ex.reconcile({"ETHUSDT": pos()}, MARKETS), [])

    async def test_orphan_position_in_a_tracked_market_is_reported(self):
        ex = exec_with([{"symbol": "BTCUSDT", "positionAmt": "-0.01"}, {"symbol": "DOGEUSDT", "positionAmt": "-50"}], [])
        problems = await ex.reconcile({}, MARKETS)
        self.assertEqual(len(problems), 1)  # DOGEUSDT is not a tracked market, so it is left alone
        self.assertIn("BTCUSDT", problems[0])

    async def test_falls_back_to_v2_when_v3_is_absent(self):
        ex = exec_with([{"symbol": "ETHUSDT", "positionAmt": "-2.0"}], [{"asset": "ETH", "free": "2", "locked": "0"}], v3_missing=True)
        self.assertEqual(await ex.reconcile({"ETHUSDT": pos()}, MARKETS), [])
        self.assertEqual(ex.c.paths[:2], ["/fapi/v3/positionRisk", "/fapi/v2/positionRisk"])


if __name__ == "__main__":
    unittest.main()
