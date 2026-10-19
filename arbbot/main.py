"""Entry point: python -m arbbot [--feed rest|sim] [--live [--testnet]] [--no-trade]"""
import argparse
import asyncio
import logging
import logging.handlers
import os
import signal
import sys
import time
from pathlib import Path

import aiohttp
from aiohttp import web

from . import __version__
from .alerts import Notifier, find_chat_ids
from .binance_client import BinanceClient
from .commands import CommandHandler, TelegramListener
from .config import load
from .engine import Engine
from .executor import PaperExecutor
from .feeds import WS_DEFAULTS, Board, HybridFeed, RestFeed, SimFeed, WsFeed
from .health import hosted_port, make_health_app
from .live import LiveExecutor
from .portfolio import Portfolio, Store
from .publisher import SnapshotPublisher
from .recorder import DepthSampler, Recorder
from .risk import Risk
from .scanner import Scanner, round_trip_cost_bps
from .server import Dashboard
from .sizing import RestDepth

ROOT = Path(__file__).resolve().parent.parent
LIVE_ACK = "I_UNDERSTAND_THIS_TRADES_REAL_MONEY"


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="arbbot", description="Binance USD-M perp/spot basis scanner and trader")
    p.add_argument("--config", default=str(ROOT / "config.toml"))
    p.add_argument("--feed", choices=["ws", "rest", "sim"], help="override feed.mode (ws = streams with REST fallback)")
    p.add_argument("--interval", type=float, help="override feed.interval_s (seconds)")
    p.add_argument("--port", type=int, help="override server.port")
    p.add_argument("--max-markets", type=int, help="override universe.max_markets")
    p.add_argument("--min-samples", type=int, help="override strategy.min_samples (shorter warm-up)")
    p.add_argument("--no-trade", action="store_true", help="monitor only: never open positions")
    p.add_argument("--live", action="store_true", help="send REAL orders (needs API keys; mainnet also needs ARBBOT_LIVE_ACK)")
    p.add_argument("--testnet", action="store_true", help="use Binance testnet hosts (with --live)")
    p.add_argument("--hosted", action="store_true",
                   help="container-host mode (paper only): serve just a health check on 0.0.0.0:$PORT instead of the "
                        "local dashboard; the recorder stays off unless ARBBOT_RECORD=1")
    p.add_argument("--seed", type=int, help="random seed for the simulator")
    p.add_argument("--test-alert", action="store_true", help="send one Telegram test message and exit")
    p.add_argument("--find-chat-id", action="store_true", help="list Telegram chats that messaged your bot, then exit")
    return p.parse_args(argv)


async def telegram_utility(args: argparse.Namespace, cfg, token: str, chat_id: str) -> None:
    async with aiohttp.ClientSession() as session:
        if args.find_chat_id:
            if not token:
                sys.exit("Set TELEGRAM_BOT_TOKEN in .env first (create a bot with @BotFather).")
            try:
                chats = await find_chat_ids(session, cfg.alerts, token)
            except Exception as e:
                sys.exit(f"Telegram error: {type(e).__name__}: {str(e).replace(token, '***')}")
            if not chats:
                sys.exit("No chats found. Open your bot in Telegram, press Start (or send any message), then run this again.")
            for c in chats:
                print(f"chat id {c['id']}  ({c['type']}, {c['name']})")
            print("Put that id in .env as TELEGRAM_CHAT_ID=<id>, then run --test-alert.")
            return
        notifier = Notifier(cfg.alerts, token, chat_id, "TEST", session)
        ok, detail = await notifier.send_now("🤖 arbbot test alert: Telegram alerts are wired up.")
        if not ok:
            sys.exit(f"Test alert FAILED: {detail}")
        print("Test alert sent. Check Telegram.")


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def setup_logging() -> None:
    (ROOT / "logs").mkdir(exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s", "%H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    file_h = logging.handlers.RotatingFileHandler(ROOT / "logs" / "bot.log", maxBytes=5_000_000, backupCount=3)
    file_h.setFormatter(fmt)
    root.addHandler(console)
    root.addHandler(file_h)
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)


async def amain(args: argparse.Namespace) -> None:
    load_dotenv(ROOT / ".env")
    cfg = load(args.config)
    if args.feed:
        cfg.feed.mode = args.feed
    if args.interval:
        cfg.feed.interval_s = args.interval
    if args.port:
        cfg.server.port = args.port
    if args.max_markets:
        cfg.universe.max_markets = args.max_markets
    if args.min_samples:
        cfg.strategy.min_samples = args.min_samples
        cfg.strategy.lookback = max(cfg.strategy.lookback, args.min_samples)
    if args.testnet:
        cfg.execution.env = "testnet"
    if args.hosted:
        if args.live:
            sys.exit("--hosted is paper-only: real orders are never sent from an unattended host. Run --live yourself.")
        cfg.recorder.enabled = os.environ.get("ARBBOT_RECORD") == "1"  # ephemeral disks fill up; opt in with a disk

    # Two-key rule for real orders: config says live AND --live is passed.
    if cfg.execution.mode == "live" and not args.live:
        sys.exit("config.toml sets execution.mode='live' but --live was not passed. Refusing to start.")
    live = args.live
    if live:
        cfg.execution.mode = "live"
        if cfg.feed.mode == "sim":
            sys.exit("--live requires a real feed (--feed ws or rest); the simulator cannot drive real orders.")
        if cfg.execution.env == "mainnet" and os.environ.get("ARBBOT_LIVE_ACK") != LIVE_ACK:
            sys.exit(f"Mainnet live trading needs ARBBOT_LIVE_ACK={LIVE_ACK} in the environment. Try --testnet first.")
        if not (os.environ.get("BINANCE_API_KEY") and os.environ.get("BINANCE_API_SECRET")):
            sys.exit("--live needs BINANCE_API_KEY and BINANCE_API_SECRET (see .env.example).")
    else:
        cfg.execution.mode = "paper"
    cfg.risk.kill_file = str(ROOT / cfg.risk.kill_file) if not os.path.isabs(cfg.risk.kill_file) else cfg.risk.kill_file
    cfg.validate()

    setup_logging()
    log = logging.getLogger("main")
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if args.test_alert or args.find_chat_id:
        await telegram_utility(args, cfg, token, chat_id)
        return
    mode = "live" if live else "paper"
    db_path = ROOT / "data" / f"{mode}-{cfg.execution.env}-{'sim' if cfg.feed.mode == 'sim' else 'real'}.db"
    now = time.time()
    store = Store(db_path)
    portfolio = Portfolio(store, now)
    risk = Risk(cfg.risk, portfolio)
    scanner = Scanner(cfg.strategy, cfg.fees, cfg.feed.max_quote_age_s)
    board = Board(cfg.feed.mode)

    async with aiohttp.ClientSession() as session:
        client = BinanceClient(
            session, cfg.execution.env,
            os.environ.get("BINANCE_API_KEY", "") if live else "",
            os.environ.get("BINANCE_API_SECRET", "") if live else "",
            cfg.feed.request_timeout_s,
        )
        if live:
            executor = LiveExecutor(client, cfg)
            await executor.preflight()
            try:  # the edge calculation must use what this account really pays, not the VIP-0 defaults
                fut_bps, spot_bps = await executor.fetch_fees()
                cfg.fees.futures_taker_bps, cfg.fees.spot_taker_bps = fut_bps, spot_bps
                log.info("real fee tier: futures taker %.2f bps, spot taker %.2f bps (config.toml values replaced)",
                         fut_bps, spot_bps)
            except Exception as e:
                log.warning("could not read real fee rates (%s: %s); using the config.toml fees", type(e).__name__, e)
        else:
            executor = PaperExecutor(cfg.fees)

        if cfg.feed.mode in ("rest", "ws"):
            rest = RestFeed(client, board, cfg, with_filters=live)
            try:
                await rest.load_universe(must_include=frozenset(portfolio.open))
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                sys.exit(f"Could not load the market universe from Binance ({type(e).__name__}: {e}). "
                         "Check your connection, or try --feed sim for offline data.")
            feed = rest
            if cfg.feed.mode == "ws":
                fut_default, spot_default = WS_DEFAULTS[cfg.execution.env]
                ws = WsFeed(session, board, cfg, cfg.feed.ws_futures_url or fut_default,
                            cfg.feed.ws_spot_url or spot_default)
                feed = HybridFeed(ws, rest, board, cfg)
        else:
            feed = SimFeed(board, cfg, seed=args.seed)
            feed.load_universe()
            feed.step()

        label = "SIM" if cfg.feed.mode == "sim" else f"{mode.upper()} {cfg.execution.env}"
        alerts = Notifier(cfg.alerts, token, chat_id, label, session)
        if live:  # never trade on top of a book that disagrees with the exchange
            try:
                problems = await executor.reconcile(portfolio.open, board.markets)
            except Exception as e:
                problems = [f"could not reconcile with the exchange: {type(e).__name__}: {e}"]
            for problem in problems:
                log.error("RECONCILE: %s", problem)
            if problems:
                risk.trip_fault(("startup reconciliation failed: " + "; ".join(problems))[:400])
            else:
                log.info("reconciled: saved positions match the exchange")
        depth = RestDepth(client) if (cfg.feed.mode in ("rest", "ws") and cfg.risk.use_depth_sizing) else None
        recorder = None
        if cfg.recorder.enabled and cfg.feed.mode != "sim":  # synthetic data is never worth recording
            recorder = Recorder(cfg.recorder, cfg.feed.max_quote_age_s, root=ROOT, fees=cfg.fees)
        engine = Engine(cfg, board, scanner, portfolio, risk, executor, trading=not args.no_trade, alerts=alerts,
                        depth=depth, recorder=recorder)
        sampler = DepthSampler(recorder, depth, board, cfg.recorder.depth_every_s) if (recorder and depth) else None
        listener = TelegramListener(cfg.alerts, alerts, CommandHandler(engine, label),
                                    poll_timeout_s=cfg.alerts.poll_timeout_s)
        dash = Dashboard(engine, cfg, ROOT / "web")
        if args.hosted:  # public interface: health check only, never the dashboard with its control endpoints
            runner = web.AppRunner(make_health_app())
            host, port = "0.0.0.0", hosted_port()
        else:
            runner = web.AppRunner(dash.make_app())
            host, port = cfg.server.host, cfg.server.port
        await runner.setup()
        await web.TCPSite(runner, host, port).start()

        banner_mode = "LIVE (REAL ORDERS)" if live else "PAPER (no orders sent)"
        log.info("arbbot %s | %s | feed=%s env=%s", __version__, banner_mode, cfg.feed.mode, cfg.execution.env)
        log.info("scanning %d markets | round-trip cost %.1f bps | z>=%.1f net>=%.1f bps",
                 len(board.markets), round_trip_cost_bps(cfg.fees), cfg.strategy.z_entry, cfg.strategy.min_net_edge_bps)
        if args.hosted:
            log.info("hosted mode: health check on %s:%d (no dashboard) | state: %s | pause entries with the Telegram /pause command",
                     host, port, db_path.name)
        else:
            log.info("dashboard: http://%s:%d   | state: %s | kill switch: touch %s",
                     host, port, db_path.name, cfg.risk.kill_file)
        if portfolio.open:
            log.warning("resuming with %d open position(s): %s", len(portfolio.open), ", ".join(portfolio.open))
        if cfg.feed.mode == "sim":
            log.warning("SIMULATED FEED: prices and dislocations are synthetic")
        if alerts.enabled:
            log.info("Telegram alerts: ON (signals=%s trades=%s halts=%s) | commands: %s",
                     cfg.alerts.notify_signals, cfg.alerts.notify_trades, cfg.alerts.notify_halts,
                     "ON (your chat only; try /help)" if cfg.alerts.commands else "off")
        else:
            log.info("Telegram alerts: off (set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env; test with --test-alert)")
        if recorder is not None:
            log.info("recorder: ON -> %s (backtest with: python backtest.py info|slippage|replay)", recorder.dir)
        alerts.started(len(board.markets), len(portfolio.open))

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        publisher = SnapshotPublisher(cfg.publisher, engine, os.environ.get("DASHBOARD_PUSH_URL", ""),
                                      os.environ.get("DASHBOARD_PUSH_TOKEN", ""), session)
        if publisher.enabled:
            url_fp, token_fp = publisher.fingerprints()
            log.info("hosted dashboard: publishing a read-only snapshot every %.0fs (settings fingerprint: url %s, token %s)",
                     cfg.publisher.interval_s, url_fp, token_fp)
        coros = [feed.run(), engine.run(), dash.broadcast_loop(), alerts.run(), listener.run(), publisher.run()]
        if sampler is not None:
            coros.append(sampler.run())
        tasks = [asyncio.create_task(c) for c in coros]
        waiter = asyncio.create_task(stop.wait())
        done, _ = await asyncio.wait([waiter, *tasks], return_when=asyncio.FIRST_COMPLETED)
        crashed = [t.exception() for t in done if t is not waiter and not t.cancelled() and t.exception()]
        for exc in crashed:
            log.error("task crashed: %r", exc)
        if crashed:
            alerts.send(f"🚨 arbbot CRASHED [{label}]: {type(crashed[0]).__name__}", priority=True)
        alerts.stopped(list(portfolio.open))
        await alerts.flush(3)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if recorder is not None:
            recorder.close()
        await runner.cleanup()
        if portfolio.open:
            log.warning("shutting down with OPEN positions (saved; they resume on next start): %s",
                        ", ".join(portfolio.open))
            if live:
                log.warning("LIVE positions remain on the exchange. Close them manually if you will not restart the bot.")
        if crashed:
            sys.exit(1)  # non-zero so launchd (KeepAlive.SuccessfulExit=false) restarts the bot


def main(argv=None) -> None:
    args = parse_args(argv)
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        pass
