"""Recorder, replay engine, parameter sweeps and slippage analysis."""
import asyncio
import contextlib
import gzip
import io
import json
import tempfile
import unittest
from pathlib import Path

from arbbot.backtest import (ReplayDepth, apply_override, format_info, format_metrics, format_slippage, info, main, metrics,
                             replay, resolve_paths, slippage_report)
from arbbot.config import Config, RecorderCfg
from arbbot.engine import Engine
from arbbot.executor import PaperExecutor
from arbbot.feeds import Board
from arbbot.models import Market, Quote
from arbbot.portfolio import Portfolio, Store
from arbbot.recorder import DepthSampler, Recorder, books_from_record, books_to_record, read_records
from arbbot.risk import Risk
from arbbot.scanner import Scanner
from arbbot.sizing import Books
from tests.test_core import set_quotes

T0 = 1_700_000_000.0  # 2023-11-14 22:13:20 UTC; the next UTC midnight is T0 + 6400 s


def deep_books(perp_bid_qty=1e6):
    return Books(spot_bids=[(99.9, 1e6)], spot_asks=[(100.0, 1e6)], perp_bids=[(100.89, perp_bid_qty)],
                 perp_asks=[(100.91, 1e6)])


def make_recording(folder, depth=None, depth_at=-0.5, spike=90.0, n_quiet=40, n_revert=15):
    rec = Recorder(RecorderCfg(dir=str(folder), max_mb=100), 3.0, fees=Config().fees)
    m = Market("ETHUSDT", "ETHUSDT", "ETH")
    markets = {"ETHUSDT": m}
    t = T0
    for i in range(n_quiet):
        set_quotes(m, 100.0, 2.0 + 0.3 * (-1) ** i, t)
        rec.tick(t, markets)
        t += 1
    if depth is not None:
        rec.depth("ETHUSDT", depth, t + depth_at, "sample")
    set_quotes(m, 100.0, spike, t)
    rec.tick(t, markets)
    t += 1
    for i in range(n_revert):
        set_quotes(m, 100.0, 2.0 + 0.3 * (-1) ** i, t)
        rec.tick(t, markets)
        t += 1
    rec.close()
    return sorted(Path(folder).glob("quotes-*.jsonl.gz"))


def replay_cfg():
    cfg = Config()
    cfg.strategy.min_samples = 20
    cfg.strategy.lookback = 100
    return cfg


class RecorderTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())

    def recorder(self, **kw):
        return Recorder(RecorderCfg(dir=str(self.dir), **kw), 3.0, fees=Config().fees)

    def test_writes_meta_then_ticks_with_only_fresh_markets(self):
        rec = self.recorder()
        fresh, stale = Market("BTCUSDT", "BTCUSDT", "BTC"), Market("OLDUSDT", "OLDUSDT", "OLD", mult=1000.0)
        set_quotes(fresh, 100.0, 5.0, T0)
        set_quotes(stale, 100.0, 5.0, T0 - 60)
        fresh.mark, fresh.index, fresh.funding_rate, fresh.next_funding_ms = 100.1, 100.0, 0.0001, 123
        rec.tick(T0, {"BTCUSDT": fresh, "OLDUSDT": stale})
        rec.close()
        meta, tick = list(read_records(sorted(self.dir.glob("*.gz"))))
        self.assertEqual((meta["type"], meta["v"]), ("meta", 1))
        self.assertEqual(set(meta["markets"]), {"BTCUSDT", "OLDUSDT"})  # meta describes every known market
        self.assertEqual(meta["markets"]["OLDUSDT"]["mult"], 1000.0)
        rows = tick["q"]
        self.assertEqual([r[0] for r in rows], ["BTCUSDT"])  # but the stale market's quotes are not recorded
        row = rows[0]
        self.assertEqual(len(row), 13)
        self.assertEqual((row[9], row[10], row[11], row[12]), (100.1, 100.0, 0.0001, 123))
        self.assertLess(row[1], row[3])  # perp bid < ask

    def test_meta_carries_multiplier_funding_rules_and_fees_and_restarts_append(self):
        rec = self.recorder()
        m = Market("1000SHIBUSDT", "SHIBUSDT", "SHIB", mult=1000.0, funding_interval_h=4.0, funding_cap=0.02, funding_floor=-0.02)
        set_quotes(m, 0.02, 3.0, T0)
        rec.tick(T0, {"1000SHIBUSDT": m})
        rec.close()
        rec2 = self.recorder()  # a restart the same day appends another session with a fresh meta line
        rec2.tick(T0 + 1, {"1000SHIBUSDT": m})
        rec2.close()
        metas = [r for r in read_records(sorted(self.dir.glob("*.gz"))) if r.get("type") == "meta"]
        self.assertEqual(len(metas), 2)
        d = metas[1]["markets"]["1000SHIBUSDT"]
        self.assertEqual((d["spot"], d["base"], d["mult"], d["fi"], d["cap"], d["floor"]), ("SHIBUSDT", "SHIB", 1000.0, 4.0, 0.02, -0.02))
        self.assertEqual(metas[1]["fees"]["futures_taker_bps"], 5.0)

    def test_one_file_per_utc_day_each_starting_with_meta(self):
        rec = self.recorder()
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        for t in (T0, T0 + 1, T0 + 6400 + 10):  # the last one is just after UTC midnight
            set_quotes(m, 100.0, 3.0, t)
            rec.tick(t, {"ETHUSDT": m})
        rec.close()
        files = sorted(self.dir.glob("quotes-*.jsonl.gz"))
        self.assertEqual([f.name for f in files], ["quotes-2023-11-14.jsonl.gz", "quotes-2023-11-15.jsonl.gz"])
        for f in files:
            self.assertEqual(next(iter(read_records([f])))["type"], "meta")

    def test_truncated_file_yields_what_was_readable_without_raising(self):
        files = make_recording(self.dir)
        raw = files[0].read_bytes()
        files[0].write_bytes(raw[: len(raw) // 2])  # crash mid-write
        got = list(read_records(files))
        self.assertGreater(len(got), 1)
        self.assertLess(len(got), 57)

    def test_depth_and_events_round_trip(self):
        rec = self.recorder()
        books = deep_books()
        rec.depth("ETHUSDT", books, T0, "signal")
        rec.event("entry", T0 + 1, sym="ETHUSDT", notional=200.0, predicted_net_bps=17.0)
        rec.event("exit", T0 + 9, sym="ETHUSDT", pnl_bps=12.0, reason="converged")
        rec.close()
        recs = list(read_records(sorted(self.dir.glob("*.gz"))))
        d = next(r for r in recs if "d" in r)
        self.assertEqual((d["d"], d["why"]), ("ETHUSDT", "signal"))
        self.assertEqual(books_from_record(d), books)
        self.assertEqual(books_from_record(books_to_record(books)), books)
        self.assertEqual([r["e"] for r in recs if "e" in r], ["entry", "exit"])

    def test_disk_cap_deletes_oldest_but_never_todays_file(self):
        for day in ("2020-01-01", "2020-01-02"):
            with gzip.open(self.dir / f"quotes-{day}.jsonl.gz", "wt") as f:
                f.write('{"t":1}\n' * 100)
        rec = self.recorder(max_mb=0)
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        set_quotes(m, 100.0, 3.0, T0)
        rec.tick(T0, {"ETHUSDT": m})
        rec.close()
        left = [f.name for f in sorted(self.dir.glob("*.gz"))]
        self.assertEqual(left, ["quotes-2023-11-14.jsonl.gz"])

    def test_io_failure_disables_recording_and_never_raises(self):
        blocker = self.dir / "blocker"
        blocker.write_text("a file where a directory is needed")
        rec = Recorder(RecorderCfg(dir=str(blocker / "sub")), 3.0)
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        set_quotes(m, 100.0, 3.0, T0)
        rec.tick(T0, {"ETHUSDT": m})  # must not raise
        rec.depth("ETHUSDT", deep_books(), T0)
        rec.event("entry", T0)
        self.assertFalse(rec.enabled)

    def test_disabled_recorder_writes_nothing(self):
        rec = self.recorder(enabled=False)
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        set_quotes(m, 100.0, 3.0, T0)
        rec.tick(T0, {"ETHUSDT": m})
        self.assertEqual(list(self.dir.glob("*")), [])


class FakeDepth:
    def __init__(self, none_for=()):
        self.calls, self.none_for = [], set(none_for)

    async def books(self, m):
        self.calls.append(m.symbol)
        return None if m.symbol in self.none_for else deep_books()


class DepthSamplerTests(unittest.IsolatedAsyncioTestCase):
    async def test_samples_markets_round_robin_and_skips_failures(self):
        board = Board("ws")
        for s in ("AAAUSDT", "BBBUSDT", "CCCUSDT"):
            board.markets[s] = Market(s, s, s[:3])
        folder = Path(tempfile.mkdtemp())
        rec = Recorder(RecorderCfg(dir=str(folder)), 3.0)
        depth = FakeDepth(none_for={"BBBUSDT"})
        sampler = DepthSampler(rec, depth, board, every_s=0.2)
        task = asyncio.create_task(sampler.run())
        await asyncio.sleep(1.9)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        rec.close()
        self.assertEqual(depth.calls[:6], ["AAAUSDT", "BBBUSDT", "CCCUSDT", "AAAUSDT", "BBBUSDT", "CCCUSDT"])
        recorded = [r["d"] for r in read_records(sorted(folder.glob("*.gz"))) if "d" in r]
        self.assertNotIn("BBBUSDT", recorded)  # nothing to record when the fetch returned None
        self.assertGreaterEqual(sampler.samples, 3)
        self.assertEqual(set(recorded), {"AAAUSDT", "CCCUSDT"})


class EngineRecordingTests(unittest.IsolatedAsyncioTestCase):
    async def test_engine_records_ticks_signal_depth_and_trades(self):
        folder = Path(tempfile.mkdtemp())
        cfg = replay_cfg()
        cfg.risk.kill_file = str(folder / "NOKILL")
        board = Board("sim")
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        board.markets["ETHUSDT"] = m
        pf = Portfolio(Store(Path(":memory:")), T0)

        class Depth:
            async def books(self, mk):
                return deep_books()

        rec = Recorder(RecorderCfg(dir=str(folder)), 3.0)
        eng = Engine(cfg, board, Scanner(cfg.strategy, cfg.fees, 3.0), pf, Risk(cfg.risk, pf), PaperExecutor(cfg.fees),
                     depth=Depth(), recorder=rec)
        t = T0
        for i in range(30):
            set_quotes(m, 100.0, 2.0 + 0.3 * (-1) ** i, t)
            await eng.tick(now=t)
            t += 1
        set_quotes(m, 100.0, 90.0, t)
        await eng.tick(now=t)
        for i in range(12):
            t += 1
            set_quotes(m, 100.0, 2.0 + 0.3 * (-1) ** i, t)
            await eng.tick(now=t)
        rec.close()
        recs = list(read_records(sorted(folder.glob("*.gz"))))
        self.assertEqual(sum(1 for r in recs if "q" in r), 43)
        self.assertEqual([r["why"] for r in recs if "d" in r], ["signal"])
        events = [r for r in recs if "e" in r]
        self.assertEqual([e["e"] for e in events], ["entry", "exit"])
        self.assertEqual(events[1]["reason"], "converged")
        self.assertGreater(events[0]["predicted_net_bps"], 0)


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())

    def test_replay_reproduces_the_live_trade(self):
        files = make_recording(self.dir, depth=deep_books())
        m = metrics(replay(files, replay_cfg()))
        self.assertEqual((m["trades"], m["wins"]), (1, 1))
        self.assertEqual(m["reasons"], {"converged": 1})
        self.assertGreater(m["pnl_usd"], 0)
        self.assertEqual(m["open_at_end"], 0)
        self.assertIn("model_error_p95_bps", m)
        self.assertIn("1W / 0L", format_metrics(m))

    def test_replay_without_any_dislocation_trades_nothing(self):
        files = make_recording(self.dir, spike=2.5)
        self.assertEqual(metrics(replay(files, replay_cfg()))["trades"], 0)

    def test_parameter_change_changes_the_outcome(self):
        files = make_recording(self.dir, depth=deep_books())
        strict = replay_cfg()
        strict.strategy.z_entry = 1000.0
        self.assertEqual(metrics(replay(files, strict))["trades"], 0)
        costly = replay_cfg()
        costly.fees.futures_taker_bps = 60.0  # fees so high the edge disappears
        self.assertEqual(metrics(replay(files, costly))["trades"], 0)

    def test_recorded_depth_gates_the_entry_and_can_be_switched_off(self):
        thin = deep_books(perp_bid_qty=0.001)  # nothing to sell into at the touch
        files = make_recording(self.dir, depth=thin)
        with_depth = metrics(replay(files, replay_cfg()))
        self.assertEqual(with_depth["trades"], 0)
        self.assertGreaterEqual(with_depth["skipped_thin"], 1)
        no_depth = metrics(replay(files, replay_cfg(), use_depth=False))
        self.assertEqual(no_depth["trades"], 1)  # top-of-book only: fills the unrealistic trade

    def test_stale_depth_snapshot_is_not_used(self):
        files = make_recording(self.dir, depth=deep_books(perp_bid_qty=0.001), depth_at=-500.0)
        self.assertEqual(metrics(replay(files, replay_cfg()))["trades"], 1)  # too old: falls back to top-of-book

    def test_no_ticks_is_an_error(self):
        path = self.dir / "quotes-2023-11-14.jsonl.gz"
        with gzip.open(path, "wt") as f:
            f.write(json.dumps({"v": 1, "type": "meta", "t": T0, "markets": {}, "fees": {}}) + "\n")
        with self.assertRaises(ValueError):
            replay([path], replay_cfg())

    def test_replay_depth_age_rules(self):
        rd = ReplayDepth(max_age_s=10)
        rd.add("ETHUSDT", 100.0, deep_books())
        m = Market("ETHUSDT", "ETHUSDT", "ETH")
        rd.now = 105.0
        self.assertIsNotNone(asyncio.run(rd.books(m)))
        rd.now = 120.0
        self.assertIsNone(asyncio.run(rd.books(m)))
        rd.now = 99.0  # a snapshot from the future must never be used
        self.assertIsNone(asyncio.run(rd.books(m)))

    def test_a_market_missing_from_later_ticks_goes_stale_in_replay(self):
        """Quotes the recorder skipped as stale must not trade in replay either."""
        rec = Recorder(RecorderCfg(dir=str(self.dir)), 3.0)
        a, b = Market("AAAUSDT", "AAAUSDT", "AAA"), Market("BBBUSDT", "BBBUSDT", "BBB")
        t = T0
        for i in range(40):
            set_quotes(a, 100.0, 2.0 + 0.3 * (-1) ** i, t)
            set_quotes(b, 100.0, 2.0 + 0.3 * (-1) ** i, t)
            rec.tick(t, {"AAAUSDT": a, "BBBUSDT": b})
            t += 1
        set_quotes(a, 100.0, 90.0, t)  # only A is fresh now; B's last quote ages out
        rec.tick(t, {"AAAUSDT": a, "BBBUSDT": b})
        rec.close()
        res = replay(sorted(self.dir.glob("*.gz")), replay_cfg(), use_depth=False)
        self.assertNotIn("BBBUSDT", {tr.symbol for tr in res.trades})
        self.assertEqual(res.open_at_end, 1)  # A entered on the spike and the recording ended before it converged


class OverrideTests(unittest.TestCase):
    def test_types_are_respected(self):
        cfg = Config()
        apply_override(cfg, "strategy.z_entry=4.5")
        apply_override(cfg, "risk.max_open_positions=3")
        apply_override(cfg, "risk.use_depth_sizing=false")
        apply_override(cfg, "risk.size_steps=1/0.5")
        apply_override(cfg, "strategy.baseline=meanstd")
        self.assertEqual((cfg.strategy.z_entry, cfg.risk.max_open_positions, cfg.risk.use_depth_sizing), (4.5, 3, False))
        self.assertEqual((cfg.risk.size_steps, cfg.strategy.baseline), ([1.0, 0.5], "meanstd"))

    def test_bad_overrides_are_rejected(self):
        for bad in ("strategy.nope=1", "nosection.x=1", "strategy.z_entry", "strategy.baseline=weird", "=1"):
            with self.assertRaises(ValueError, msg=bad):
                apply_override(Config(), bad)


def snapshot_record(sym, t, perp_bids, spot_asks=None, perp_asks=None, spot_bids=None):
    return {"t": t, "d": sym, "why": "sample", "pb": [list(x) for x in perp_bids],
            "pa": [list(x) for x in (perp_asks or [(100.1, 1e6)])], "sb": [list(x) for x in (spot_bids or [(99.9, 1e6)])],
            "sa": [list(x) for x in (spot_asks or [(100.0, 1e6)])]}


def write_records(folder, records):
    path = Path(folder) / "quotes-2023-11-14.jsonl.gz"
    with gzip.open(path, "wt") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    return [path]


class SlippageTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())

    def test_known_book_gives_the_hand_computed_slippage(self):
        meta = {"v": 1, "type": "meta", "t": T0, "markets": {"ETHUSDT": {"spot": "ETHUSDT", "base": "ETH", "mult": 1.0}}}
        # $200 -> 2 contracts: 0.5 @ 100 + 1.5 @ 99 => VWAP 99.25 => 75 bps on the perp-sell leg; the other three legs are at the touch.
        snaps = [snapshot_record("ETHUSDT", T0 + i, [(100.0, 0.5), (99.0, 100.0)]) for i in range(4)]
        rep = slippage_report(write_records(self.dir, [meta] + snaps), sizes=(200.0, 100000.0), ref_size=200.0)
        row = next(r for r in rep["sizes"] if r["usd"] == 200.0)
        self.assertEqual(row["snapshots"], 4)
        self.assertAlmostEqual(row["max"], 75.0, places=6)
        self.assertAlmostEqual(row["p95"], 75.0, places=6)
        self.assertEqual(row["p50"], 0.0)
        self.assertEqual(row["unfillable_pct"], 0.0)
        huge = next(r for r in rep["sizes"] if r["usd"] == 100000.0)
        self.assertEqual(huge["unfillable_pct"], 100.0)  # the book can't absorb $100k

    def test_multiplier_scales_the_spot_side(self):
        meta = {"v": 1, "type": "meta", "t": T0, "markets": {"1000SHIBUSDT": {"spot": "SHIBUSDT", "base": "SHIB", "mult": 1000.0}}}
        snap = snapshot_record("1000SHIBUSDT", T0, [(20.0, 1e6)], spot_asks=[(0.02, 1e5), (0.0201, 1e9)], perp_asks=[(20.1, 1e6)],
                               spot_bids=[(0.0199, 1e9)])
        # $200 -> 10 contracts -> 10,000 SHIB; the first 1e5 SHIB level absorbs it entirely: zero impact.
        rep = slippage_report(write_records(self.dir, [meta, snap]), sizes=(200.0,), ref_size=200.0)
        self.assertEqual(rep["sizes"][0]["max"], 0.0)

    def test_hardest_markets_come_first_and_unfillable_dominates(self):
        meta = {"v": 1, "type": "meta", "t": T0, "markets": {s: {"spot": s, "base": s[:3], "mult": 1.0} for s in ("AAAUSDT", "BBBUSDT")}}
        records = [meta,
                   snapshot_record("AAAUSDT", T0, [(100.0, 1e6)]),
                   snapshot_record("BBBUSDT", T0, [(100.0, 0.01)])]  # cannot fill $200
        rep = slippage_report(write_records(self.dir, records), sizes=(200.0,), ref_size=200.0)
        self.assertEqual(rep["worst_markets"][0]["symbol"], "BBBUSDT")
        self.assertEqual(rep["worst_markets"][0]["unfillable_pct"], 100.0)

    def test_report_flags_when_measured_impact_exceeds_the_configured_slippage(self):
        meta = {"v": 1, "type": "meta", "t": T0, "markets": {"ETHUSDT": {"spot": "ETHUSDT", "base": "ETH", "mult": 1.0}}}
        thin = dict(perp_bids=[(100.0, 0.001), (99.5, 1e6)], spot_asks=[(100.0, 0.001), (100.5, 1e6)],
                    perp_asks=[(100.1, 0.001), (100.6, 1e6)], spot_bids=[(99.9, 0.001), (99.4, 1e6)])
        snaps = [snapshot_record("ETHUSDT", T0 + i, **thin) for i in range(5)]
        rep = slippage_report(write_records(self.dir, [meta] + snaps), sizes=(200.0,), ref_size=200.0)
        self.assertGreater(rep["recommended_slippage_bps"], 40)
        text = format_slippage(rep, configured_bps=1.0)
        self.assertIn("exceeds the configured figure", text)
        self.assertIn("Hardest markets", text)

    def test_info_counts_everything(self):
        files = make_recording(self.dir, depth=deep_books())
        i = info(files)
        self.assertEqual((i["ticks"], i["depth_snapshots"], i["markets"]), (56, 1, 1))
        self.assertAlmostEqual(i["span_s"], 55.0, delta=1.0)
        self.assertIn("market ticks: 56", format_info(i))


class CliTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.files = make_recording(self.dir, depth=deep_books())

    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main(list(argv))
        return code, out.getvalue(), err.getvalue()

    def test_info_and_slippage_commands(self):
        code, out, _ = self.run_cli("info", str(self.dir))  # a directory expands to its recordings
        self.assertEqual(code, 0)
        self.assertIn("market ticks: 56", out)
        code, out, _ = self.run_cli("slippage", str(self.files[0]), "--sizes", "200,1000")
        self.assertEqual(code, 0)
        self.assertIn("Book-impact slippage", out)

    def test_replay_sweep_prints_one_row_per_value(self):
        code, out, _ = self.run_cli("replay", str(self.files[0]), "--set", "strategy.min_samples=20", "--set", "strategy.lookback=100",
                                    "--sweep", "strategy.z_entry=3,1000")
        self.assertEqual(code, 0)
        rows = [line for line in out.splitlines() if line.strip().startswith("strategy.z_entry=")]
        self.assertEqual(len(rows), 2)
        self.assertEqual((rows[0].split()[1], rows[1].split()[1]), ("1", "0"))  # 1 trade at z>=3, none at z>=1000

    def test_replay_json_output_is_machine_readable(self):
        code, out, _ = self.run_cli("replay", str(self.files[0]), "--set", "strategy.min_samples=20", "--set", "strategy.lookback=100",
                                    "--json")
        self.assertEqual(code, 0)
        data = json.loads(out)
        self.assertEqual((data[0]["trades"], data[0]["wins"]), (1, 1))

    def test_errors_are_reported_not_raised(self):
        code, _, err = self.run_cli("info", str(self.dir / "missing.jsonl.gz"))
        self.assertEqual(code, 2)
        self.assertIn("no such recording", err)
        code, _, err = self.run_cli("replay", str(self.files[0]), "--set", "strategy.bogus=1")
        self.assertEqual(code, 2)
        self.assertIn("bad override", err)

    def test_empty_directory_explains_how_to_get_recordings(self):
        empty = Path(tempfile.mkdtemp())
        with self.assertRaises(FileNotFoundError) as cm:
            resolve_paths([str(empty)])
        self.assertIn("start the bot", str(cm.exception))


class QuoteReplayFidelityTests(unittest.TestCase):
    def test_recorded_quotes_have_sane_structure(self):
        files = make_recording(Path(tempfile.mkdtemp()))
        ticks = [r for r in read_records(files) if "q" in r]
        row = ticks[0]["q"][0]
        q = Quote(row[1], row[2], row[3], row[4], ticks[0]["t"])
        self.assertGreater(q.ask, q.bid)
        self.assertEqual(ticks[0]["t"], T0)


if __name__ == "__main__":
    unittest.main()
