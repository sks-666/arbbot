"""Backtest tool: replay recorded market data through the real strategy, and measure real book slippage.

    python backtest.py info     [FILES...]
    python backtest.py slippage [FILES...] [--sizes 100,200,500,1000,5000] [--ref-size 200]
    python backtest.py replay   [FILES...] [--set strategy.z_entry=4] [--sweep strategy.z_entry=2.5,3,4] [--json]

FILES default to every recording in data/recordings. Replay drives the SAME Engine, Scanner, Risk and
PaperExecutor the live bot uses, on a simulated clock, so a parameter change here is exactly what it
would have done on those days. Order-book depth for sizing/fills comes from the most recent recorded
snapshot of that market (default: no older than 120 s); without one it falls back to top-of-book.

Limits worth knowing: the recorder only sees what the bot saw (top of book every second plus sampled depth),
paper exits fill at top-of-book plus the configured slippage, and latency slippage (price moving between
decision and fill) cannot be measured from recordings, only from live fills.
"""
import argparse
import asyncio
import copy
import json
import logging
import os
import sys
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from statistics import median

from .config import Config, load
from .engine import Engine
from .executor import PaperExecutor
from .feeds import Board
from .models import ClosedTrade, Market, Quote
from .portfolio import Portfolio, Store, unrealized_usd
from .recorder import books_from_record, read_records
from .risk import Risk
from .scanner import Scanner, percentile
from .sizing import Books, walk

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_GLOB = "quotes-*.jsonl.gz"


def resolve_paths(args: list[str] | None) -> list[Path]:
    """Files as given; directories expand to their recordings; nothing given means data/recordings."""
    items = [Path(a) for a in args] if args else [ROOT / "data" / "recordings"]
    out: list[Path] = []
    for p in items:
        out.extend(sorted(p.glob(DEFAULT_GLOB)) if p.is_dir() else [p])
    missing = [str(p) for p in out if not p.exists()]
    if missing:
        raise FileNotFoundError(f"no such recording: {', '.join(missing)}")
    if not out:
        raise FileNotFoundError("no recordings found (start the bot on a real feed; it records by default)")
    return out


# ---- info ----------------------------------------------------------------------------------------
def info(paths) -> dict:
    ticks = depth = entries = exits = 0
    first = last = None
    why: Counter = Counter()
    markets: set[str] = set()
    for rec in read_records(paths):
        if rec.get("type") == "meta":
            markets.update(rec["markets"])
            continue
        t = rec.get("t")
        if t is not None:
            first = t if first is None else min(first, t)
            last = t if last is None else max(last, t)
        if "q" in rec:
            ticks += 1
        elif "d" in rec:
            depth += 1
            why[rec.get("why", "sample")] += 1
        elif rec.get("e") == "entry":
            entries += 1
        elif rec.get("e") == "exit":
            exits += 1
    size = sum(Path(p).stat().st_size for p in paths)
    span = (last - first) if first is not None else 0.0
    return {
        "files": [Path(p).name for p in paths], "bytes": size, "first": first, "last": last, "span_s": span,
        "ticks": ticks, "depth_snapshots": depth, "depth_by_reason": dict(why), "bot_entries": entries,
        "bot_exits": exits, "markets": len(markets), "mb_per_day": (size / 1e6) / (span / 86400) if span > 0 else 0.0,
    }


# ---- overrides -----------------------------------------------------------------------------------
def apply_override(cfg: Config, spec: str) -> None:
    key, sep, raw = spec.partition("=")
    section, _, name = key.partition(".")
    target = getattr(cfg, section, None)
    if not sep or target is None or not name or not hasattr(target, name):
        raise ValueError(f"bad override {spec!r}: use section.key=value, e.g. strategy.z_entry=4")
    cur = getattr(target, name)
    if isinstance(cur, bool):
        val = raw.strip().lower() in ("1", "true", "yes", "on")
    elif isinstance(cur, int):
        val = int(raw)
    elif isinstance(cur, float):
        val = float(raw)
    elif isinstance(cur, list):
        val = [float(x) for x in raw.split("/")]  # lists use '/' because ',' separates sweep values
    else:
        val = raw
    setattr(target, name, val)
    cfg.validate()


# ---- replay --------------------------------------------------------------------------------------
class ReplayDepth:
    """Serves the most recent recorded order-book snapshot per market (None when none is recent enough)."""

    def __init__(self, max_age_s: float = 120.0):
        self.max_age = max_age_s
        self.now = 0.0
        self.latest: dict[str, tuple[float, Books]] = {}

    def add(self, symbol: str, t: float, books: Books) -> None:
        self.latest[symbol] = (t, books)

    async def books(self, m: Market) -> Books | None:
        snap = self.latest.get(m.symbol)
        return snap[1] if snap is not None and 0 <= self.now - snap[0] <= self.max_age else None


@dataclass
class ReplayResult:
    label: str
    trades: list[ClosedTrade]
    ticks: int
    span_s: float
    skipped_thin: int
    open_at_end: int
    unrealized_usd: float
    cfg_summary: dict = field(default_factory=dict)


async def _replay(paths, cfg: Config, label: str, use_depth: bool, depth_age_s: float, progress: bool) -> ReplayResult:
    cfg = copy.deepcopy(cfg)
    cfg.risk.kill_file = os.path.join(tempfile.gettempdir(), "arbbot-replay-no-such-kill-file")
    logging.getLogger("engine").setLevel(logging.WARNING)
    board = Board("sim")  # "sim" keeps feed-health alerts out of the way; quotes come from the recording
    depth = ReplayDepth(depth_age_s) if use_depth else None
    engine: Engine | None = None
    ticks, first_t, last_t = 0, None, None
    for rec in read_records(paths):
        if rec.get("type") == "meta":
            for sym, d in rec["markets"].items():
                m = board.markets.get(sym) or Market(sym, d["spot"], d["base"], d["mult"])
                m.funding_interval_h = d.get("fi") or 8.0
                m.funding_cap, m.funding_floor = d.get("cap"), d.get("floor")
                board.markets[sym] = m
            continue
        t = rec.get("t")
        if "d" in rec:
            if depth is not None:
                depth.add(rec["d"], t, books_from_record(rec))
            continue
        if "q" not in rec:
            continue
        if engine is None:
            pf = Portfolio(Store(Path(":memory:")), t)
            engine = Engine(cfg, board, Scanner(cfg.strategy, cfg.fees, cfg.feed.max_quote_age_s), pf, Risk(cfg.risk, pf),
                            PaperExecutor(cfg.fees), trading=True, depth=depth)
            first_t = t
        for row in rec["q"]:
            m = board.markets.get(row[0])
            if m is None:
                continue
            m.perp = Quote(row[1], row[2], row[3], row[4], t)
            m.spot = Quote(row[5], row[6], row[7], row[8], t)
            m.mark, m.index, m.funding_rate, m.next_funding_ms = row[9], row[10], row[11], int(row[12])
        if depth is not None:
            depth.now = t
        await engine.tick(now=t)
        ticks += 1
        last_t = t
        if progress and ticks % 50_000 == 0:
            print(f"  replaying {label}: {ticks:,} ticks...", file=sys.stderr)
    if engine is None:
        raise ValueError("the recordings contain no market ticks")
    trades = engine.pf.store.recent_trades(10_000_000)
    unreal = sum(unrealized_usd(p, board.markets[s], cfg.fees) for s, p in engine.pf.open.items() if s in board.markets)
    return ReplayResult(
        label=label, trades=trades, ticks=ticks, span_s=(last_t - first_t) if first_t is not None else 0.0,
        skipped_thin=sum(1 for e in engine.events if "SKIP" in e["msg"]), open_at_end=len(engine.pf.open),
        unrealized_usd=unreal,
    )


def replay(paths, cfg: Config, label: str = "replay", use_depth: bool = True, depth_age_s: float = 120.0,
           progress: bool = False) -> ReplayResult:
    return asyncio.run(_replay(paths, cfg, label, use_depth, depth_age_s, progress))


def metrics(res: ReplayResult) -> dict:
    trades = sorted(res.trades, key=lambda t: t.closed_at)
    n = len(trades)
    pnl = sum(t.pnl_usd for t in trades)
    wins = sum(1 for t in trades if t.pnl_usd > 0)
    peak = cum = max_dd = 0.0
    for t in trades:
        cum += t.pnl_usd
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)
    errors = [t.predicted_net_bps - t.pnl_bps for t in trades if t.predicted_net_bps]
    by_symbol: defaultdict = defaultdict(float)
    for t in trades:
        by_symbol[t.symbol] += t.pnl_usd
    days = res.span_s / 86400 if res.span_s > 0 else 0.0
    return {
        "label": res.label, "trades": n, "wins": wins, "win_rate": wins / n if n else 0.0, "pnl_usd": pnl,
        "avg_pnl_bps": sum(t.pnl_bps for t in trades) / n if n else 0.0,
        "median_pnl_bps": median([t.pnl_bps for t in trades]) if n else 0.0,
        "fees_usd": sum(t.fees_usd for t in trades), "avg_hold_s": sum(t.closed_at - t.opened_at for t in trades) / n if n else 0.0,
        "pnl_per_day_usd": pnl / days if days else 0.0, "max_drawdown_usd": max_dd, "days": days, "ticks": res.ticks,
        "reasons": dict(Counter(t.reason for t in trades)),
        "model_error_mean_bps": sum(errors) / len(errors) if errors else 0.0,
        "model_error_p95_bps": percentile(errors, 0.95) if errors else 0.0,
        "skipped_thin": res.skipped_thin, "open_at_end": res.open_at_end, "unrealized_usd": res.unrealized_usd,
        "top_symbols": sorted(by_symbol.items(), key=lambda kv: -abs(kv[1]))[:5],
    }


def format_metrics(m: dict) -> str:
    lines = [
        f"== {m['label']} ==",
        f"  span {m['days']:.2f} days, {m['ticks']:,} ticks | trades {m['trades']} ({m['wins']}W / {m['trades'] - m['wins']}L, "
        f"{m['win_rate'] * 100:.0f}% win)",
        f"  P&L {m['pnl_usd']:+.2f} USD ({m['pnl_per_day_usd']:+.2f}/day) | avg {m['avg_pnl_bps']:+.1f} bps, "
        f"median {m['median_pnl_bps']:+.1f} bps | fees {m['fees_usd']:.2f} | max drawdown {m['max_drawdown_usd']:.2f}",
        f"  avg hold {m['avg_hold_s']:.0f}s | exits {m['reasons'] or '-'}",
        f"  model error (predicted - realised): mean {m['model_error_mean_bps']:+.1f} bps, p95 {m['model_error_p95_bps']:+.1f} bps"
        f" | skipped at size {m['skipped_thin']} | open at end {m['open_at_end']} ({m['unrealized_usd']:+.2f} unrealised)",
    ]
    if m["top_symbols"]:
        lines.append("  by market: " + ", ".join(f"{s} {v:+.2f}" for s, v in m["top_symbols"]))
    return "\n".join(lines)


# ---- slippage ------------------------------------------------------------------------------------
def slippage_report(paths, sizes=(100.0, 200.0, 500.0, 1000.0, 5000.0), ref_size: float = 200.0, top: int = 10) -> dict:
    """Book-impact slippage per fill (bps vs the touch price) as a function of size, from recorded depth snapshots.

    Four fills are measured per snapshot and size: perp sold into the bids and spot bought from the asks (entry),
    perp bought from the asks and spot sold into the bids (exit).
    """
    sizes = sorted(set(sizes) | {ref_size})
    mult: dict[str, float] = {}
    per_size = {s: {"n": 0, "unfillable": 0, "slips": []} for s in sizes}
    per_market: defaultdict = defaultdict(list)
    market_n: Counter = Counter()
    market_unfillable: Counter = Counter()
    snapshots = 0
    for rec in read_records(paths):
        if rec.get("type") == "meta":
            mult.update({sym: d["mult"] for sym, d in rec["markets"].items()})
            continue
        if "d" not in rec:
            continue
        books = books_from_record(rec)
        if not (books.perp_bids and books.perp_asks and books.spot_bids and books.spot_asks):
            continue
        sym, k = rec["d"], mult.get(rec["d"], 1.0)
        snapshots += 1
        market_n[sym] += 1
        pb, sa, pa, sb = books.perp_bids[0][0], books.spot_asks[0][0], books.perp_asks[0][0], books.spot_bids[0][0]
        for usd in sizes:
            q = usd / pb
            legs = (walk(books.perp_bids, q), walk(books.spot_asks, q * k), walk(books.perp_asks, q), walk(books.spot_bids, q * k))
            stat = per_size[usd]
            stat["n"] += 1
            if any(x is None for x in legs):
                stat["unfillable"] += 1
                if usd == ref_size:
                    market_unfillable[sym] += 1
                continue
            slips = [(pb - legs[0]) / pb * 1e4, (legs[1] - sa) / sa * 1e4, (legs[2] - pa) / pa * 1e4, (sb - legs[3]) / sb * 1e4]
            stat["slips"].extend(slips)
            if usd == ref_size:
                per_market[sym].extend(slips)

    def dist(xs: list[float]) -> dict:
        return {"p50": percentile(xs, 0.5), "p75": percentile(xs, 0.75), "p95": percentile(xs, 0.95), "max": max(xs)} if xs \
            else {"p50": 0.0, "p75": 0.0, "p95": 0.0, "max": 0.0}

    rows = []
    for usd in sizes:
        st = per_size[usd]
        rows.append({"usd": usd, "snapshots": st["n"], "unfillable_pct": 100 * st["unfillable"] / st["n"] if st["n"] else 0.0,
                     **dist(st["slips"])})
    markets = []
    for sym, n in market_n.items():
        d = dist(per_market[sym])
        markets.append({"symbol": sym, "snapshots": n, "unfillable_pct": 100 * market_unfillable[sym] / n, **d})
    markets.sort(key=lambda r: (-r["unfillable_pct"], -r["p95"]))
    ref_row = next((r for r in rows if r["usd"] == ref_size), None)
    return {"snapshots": snapshots, "markets_seen": len(market_n), "ref_size": ref_size, "sizes": rows,
            "worst_markets": markets[:top], "recommended_slippage_bps": ref_row["p75"] if ref_row else 0.0}


def format_slippage(rep: dict, configured_bps: float) -> str:
    lines = [f"Book-impact slippage per fill (bps vs touch), from {rep['snapshots']:,} depth snapshots across "
             f"{rep['markets_seen']} markets", "", "  size (USD)  snapshots  unfillable    p50    p75    p95    max"]
    for r in rep["sizes"]:
        lines.append(f"  {r['usd']:>9,.0f}  {r['snapshots']:>9,}  {r['unfillable_pct']:>9.1f}%  {r['p50']:>5.2f}  {r['p75']:>5.2f}  "
                     f"{r['p95']:>5.2f}  {r['max']:>5.1f}")
    lines += ["", f"  Hardest markets at ${rep['ref_size']:,.0f}:", "    market          snapshots  unfillable    p50    p95"]
    for r in rep["worst_markets"]:
        lines.append(f"    {r['symbol']:<14} {r['snapshots']:>9,}  {r['unfillable_pct']:>9.1f}%  {r['p50']:>5.2f}  {r['p95']:>5.2f}")
    rec = rep["recommended_slippage_bps"]
    lines += ["", f"  p75 book impact at ${rep['ref_size']:,.0f} is {rec:.2f} bps per fill; config.toml assumes {configured_bps:.2f}."]
    lines.append("  Depth-aware sizing already prices this impact explicitly. The configured slippage should therefore cover "
                 "only what recordings cannot show: the price moving between decision and fill (latency).")
    if rec > configured_bps:
        lines.append(f"  NOTE: measured book impact exceeds the configured figure; consider raising [fees] slippage_bps to ~{rec:.1f}.")
    return "\n".join(lines)


def format_info(i: dict) -> str:
    from datetime import datetime, timezone
    fmt = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if t else "-"  # noqa: E731
    return "\n".join([
        f"files: {', '.join(i['files'])}  ({i['bytes'] / 1e6:.1f} MB)",
        f"span: {fmt(i['first'])} -> {fmt(i['last'])}  ({i['span_s'] / 3600:.2f} h)  ~{i['mb_per_day']:.0f} MB/day",
        f"market ticks: {i['ticks']:,} | markets: {i['markets']} | depth snapshots: {i['depth_snapshots']:,} {i['depth_by_reason']}",
        f"bot trades recorded: {i['bot_entries']} entries, {i['bot_exits']} exits",
    ])


# ---- CLI -----------------------------------------------------------------------------------------
def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="backtest", description="Replay recordings and measure real slippage")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("info", "slippage", "replay"):
        sp = sub.add_parser(name)
        sp.add_argument("files", nargs="*", help="recording files or directories (default: data/recordings)")
        sp.add_argument("--config", default=str(ROOT / "config.toml"))
    sub.choices["slippage"].add_argument("--sizes", default="100,200,500,1000,5000")
    sub.choices["slippage"].add_argument("--ref-size", type=float, default=200.0)
    r = sub.choices["replay"]
    r.add_argument("--set", action="append", default=[], metavar="section.key=value", help="override a config value")
    r.add_argument("--sweep", metavar="section.key=v1,v2,...", help="run once per value")
    r.add_argument("--no-depth", action="store_true", help="ignore recorded depth (top-of-book fills and sizing)")
    r.add_argument("--depth-age", type=float, default=120.0, help="max age in seconds of a depth snapshot used for sizing")
    r.add_argument("--json", action="store_true")
    args = p.parse_args(argv)

    try:
        paths = resolve_paths(args.files)
        cfg = load(args.config)
        if args.cmd == "info":
            print(format_info(info(paths)))
        elif args.cmd == "slippage":
            sizes = tuple(float(x) for x in args.sizes.split(","))
            print(format_slippage(slippage_report(paths, sizes, args.ref_size), cfg.fees.slippage_bps))
        else:
            for spec in args.set:
                apply_override(cfg, spec)
            runs: list[tuple[str, Config]] = []
            if args.sweep:
                key, _, vals = args.sweep.partition("=")
                for v in vals.split(","):
                    c = copy.deepcopy(cfg)
                    apply_override(c, f"{key}={v}")
                    runs.append((f"{key}={v}", c))
            else:
                runs.append(("baseline" + (f" ({', '.join(args.set)})" if args.set else ""), cfg))
            results = [metrics(replay(paths, c, label, not args.no_depth, args.depth_age, progress=sys.stderr.isatty()))
                       for label, c in runs]
            if args.json:
                print(json.dumps(results, indent=2, default=list))
            else:
                for m in results:
                    print(format_metrics(m))
                if len(results) > 1:
                    print("\n  " + "setting".ljust(34) + "trades  win%     P&L USD   avg bps   max DD")
                    for m in results:
                        print(f"  {m['label']:<34}{m['trades']:>6}  {m['win_rate'] * 100:>3.0f}%  {m['pnl_usd']:>10.2f}  "
                              f"{m['avg_pnl_bps']:>8.1f}  {m['max_drawdown_usd']:>7.2f}")
        return 0
    except (FileNotFoundError, ValueError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
