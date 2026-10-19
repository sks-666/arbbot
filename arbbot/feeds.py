"""Market data feeds.

RestFeed polls Binance public REST once per interval for USD-M perp book tickers, mark/index/funding
and the matching spot book tickers. SimFeed generates an offline universe with mean-reverting basis
and injected dislocations, so the scanner, paper trader and dashboard can be exercised without network.
"""
import asyncio
import json
import logging
import math
import random
import time

import aiohttp

from .binance_client import BinanceClient
from .config import Config
from .models import Market, Quote, SymbolFilter

log = logging.getLogger("feed")


def split_base(base: str) -> tuple[str, float]:
    """1000SHIB -> (SHIB, 1000.0). Perps with a price multiplier map onto the unscaled spot symbol."""
    for prefix, mult in (("1000000", 1e6), ("1000", 1e3)):
        if base.startswith(prefix) and len(base) > len(prefix):
            return base[len(prefix):], mult
    return base, 1.0


def parse_filters(filters: list[dict]) -> SymbolFilter:
    step = min_qty = min_notional = 0.0
    for f in filters:
        t = f.get("filterType")
        if t in ("LOT_SIZE", "MARKET_LOT_SIZE"):
            step = max(step, float(f.get("stepSize") or 0))
            min_qty = max(min_qty, float(f.get("minQty") or 0))
        elif t in ("MIN_NOTIONAL", "NOTIONAL"):
            min_notional = max(min_notional, float(f.get("notional") or f.get("minNotional") or 0))
    return SymbolFilter(step=step, min_qty=min_qty, min_notional=min_notional or 5.0)


class Board:
    """Shared, latest-value view of every market plus feed health."""

    def __init__(self, kind: str):
        self.kind = kind      # configured feed: ws | rest | sim
        self.source = kind    # what is feeding data right now (ws may fall back to rest)
        self.markets: dict[str, Market] = {}
        self.ok = False
        self.polls = 0
        self.errors = 0
        self.last_error = ""
        self.latency_ms = 0.0
        self.last_update = 0.0
        self.started = time.time()


class RestFeed:
    def __init__(self, client: BinanceClient, board: Board, cfg: Config, with_filters: bool = False):
        self.c = client
        self.board = board
        self.cfg = cfg
        self.with_filters = with_filters
        self._spot_symbols: list[str] = []
        self._backoff = 0.0

    async def load_universe(self, must_include: set[str] | frozenset = frozenset()) -> None:
        u = self.cfg.universe
        info, tick24, spot_book = await asyncio.gather(
            self.c.get("fapi", "/fapi/v1/exchangeInfo"),
            self.c.get("fapi", "/fapi/v1/ticker/24hr"),
            self.c.get("spot", "/api/v3/ticker/bookTicker"),
        )
        # Only spot pairs that actually quote: halted/delisted symbols stay listed with zero prices.
        spot_symbols = {
            t["symbol"] for t in spot_book if float(t["bidPrice"]) > 0 and float(t["askPrice"]) > 0
        }
        volume = {t["symbol"]: float(t["quoteVolume"]) for t in tick24}
        excluded = set(u.exclude_bases)
        cands = []
        for s in info["symbols"]:
            if s.get("contractType") != "PERPETUAL" or s.get("status") != "TRADING" or s.get("quoteAsset") != "USDT":
                continue
            spot_base, mult = split_base(s["baseAsset"])
            spot_sym = spot_base + "USDT"
            if spot_base in excluded or spot_sym not in spot_symbols:
                continue
            cands.append((volume.get(s["symbol"], 0.0), s, spot_base, spot_sym, mult))
        cands.sort(key=lambda c: c[0], reverse=True)
        chosen = [c for c in cands if c[0] >= u.min_quote_volume_usd][: u.max_markets]
        have = {c[1]["symbol"] for c in chosen}
        chosen += [c for c in cands if c[1]["symbol"] in must_include and c[1]["symbol"] not in have]

        for vol, s, spot_base, spot_sym, mult in chosen:
            self.board.markets[s["symbol"]] = Market(
                symbol=s["symbol"], spot_symbol=spot_sym, base_asset=spot_base, mult=mult,
                quote_volume_usd=vol, fut_filter=parse_filters(s.get("filters", [])),
            )
        self._spot_symbols = [m.spot_symbol for m in self.board.markets.values()]

        # Exchange-specific funding rules: most perps settle every 8h, some every 4h or 1h, with their own caps.
        try:
            for fi in await self.c.get("fapi", "/fapi/v1/fundingInfo"):
                m = self.board.markets.get(fi.get("symbol"))
                if m is None:
                    continue
                m.funding_interval_h = float(fi.get("fundingIntervalHours") or 8)
                if fi.get("adjustedFundingRateCap") not in (None, ""):
                    m.funding_cap = float(fi["adjustedFundingRateCap"])
                if fi.get("adjustedFundingRateFloor") not in (None, ""):
                    m.funding_floor = float(fi["adjustedFundingRateFloor"])
        except Exception as e:  # defaults (8h, uncapped) are right for most symbols
            log.warning("could not load funding rules (%s: %s); assuming 8h intervals", type(e).__name__, e)

        if self.with_filters and self._spot_symbols:
            ei = await self.c.get(
                "spot", "/api/v3/exchangeInfo", {"symbols": json.dumps(self._spot_symbols, separators=(",", ":"))}
            )
            by_spot = {m.spot_symbol: m for m in self.board.markets.values()}
            for s in ei["symbols"]:
                if s["symbol"] in by_spot:
                    by_spot[s["symbol"]].spot_filter = parse_filters(s.get("filters", []))
        log.info("universe: %d USDT perpetuals with spot counterparts", len(self.board.markets))

    async def poll_once(self) -> None:
        t0 = time.time()
        prem, fbook, sbook = await asyncio.gather(
            self.c.get("fapi", "/fapi/v1/premiumIndex"),
            self.c.get("fapi", "/fapi/v1/ticker/bookTicker"),
            self.c.get(
                "spot", "/api/v3/ticker/bookTicker",
                {"symbols": json.dumps(self._spot_symbols, separators=(",", ":"))},
            ),
        )
        now = time.time()
        prem_by = {p["symbol"]: p for p in prem}
        fbook_by = {b["symbol"]: b for b in fbook}
        sbook_by = {b["symbol"]: b for b in sbook}
        for m in self.board.markets.values():
            p = prem_by.get(m.symbol)
            if p:
                m.mark = float(p["markPrice"])
                m.index = float(p["indexPrice"])
                m.funding_rate = float(p.get("lastFundingRate") or 0.0)
                m.next_funding_ms = int(p.get("nextFundingTime") or 0)
            fb = fbook_by.get(m.symbol)
            if fb and float(fb["bidPrice"]) > 0 and float(fb["askPrice"]) > 0:
                m.perp = Quote(float(fb["bidPrice"]), float(fb["bidQty"]), float(fb["askPrice"]), float(fb["askQty"]), now)
            sb = sbook_by.get(m.spot_symbol)
            if sb and float(sb["bidPrice"]) > 0 and float(sb["askPrice"]) > 0:
                m.spot = Quote(float(sb["bidPrice"]), float(sb["bidQty"]), float(sb["askPrice"]), float(sb["askQty"]), now)
        b = self.board
        b.polls += 1
        b.latency_ms = (now - t0) * 1000
        b.last_update = now
        b.ok = True

    async def step(self) -> float:
        """Poll once with error bookkeeping. Returns extra backoff seconds to wait after a failure."""
        b = self.board
        try:
            await self.poll_once()
            if self._backoff:
                log.info("REST feed recovered")
            self._backoff = 0.0
        except asyncio.CancelledError:
            raise
        except Exception as e:  # network blips must never kill the loop
            b.ok = False
            b.errors += 1
            msg = f"{type(e).__name__}: {e}"
            if msg != b.last_error:
                log.warning("feed error: %s", msg)
            b.last_error = msg
            self._backoff = min(30.0, max(1.0, self._backoff * 2))
        return self._backoff

    async def run(self) -> None:
        while True:
            started = time.monotonic()
            backoff = await self.step()
            delay = max(0.0, self.cfg.feed.interval_s - (time.monotonic() - started)) + backoff
            await asyncio.sleep(delay)


WS_DEFAULTS = {
    "mainnet": ("wss://fstream.binance.com", "wss://stream.binance.com:9443"),
    "testnet": ("wss://fstream.binancefuture.com", "wss://stream.testnet.binance.vision"),
}


class WsFeed:
    """Binance WebSocket streams: futures `!markPrice@arr@1s` + per-symbol `@bookTicker`, spot `@bookTicker`.

    A book stream only emits when the book changes, so a quiet market would look stale. While a venue's
    connection is demonstrably alive, a once-per-interval "touch" re-confirms quotes that have changed
    within `ws_max_silent_s`. `healthy` is what HybridFeed uses to decide on REST fallback.
    """

    def __init__(self, session: aiohttp.ClientSession, board: Board, cfg: Config, fut_base: str, spot_base: str):
        self.session = session
        self.board = board
        self.cfg = cfg
        self.bases = {"fut": fut_base.rstrip("/"), "spot": spot_base.rstrip("/")}
        self.by_perp = {m.symbol: m for m in board.markets.values()}
        self.by_spot = {m.spot_symbol: m for m in board.markets.values()}
        # Streams are split over several connections so every URL stays short however many markets we scan.
        n = max(1, cfg.feed.ws_streams_per_conn)
        fut = ["!markPrice@arr@1s"] + [f"{s.lower()}@bookTicker" for s in self.by_perp]
        spot = [f"{s.lower()}@bookTicker" for s in self.by_spot]
        self.plan = {
            "fut": [fut[i:i + n] for i in range(0, len(fut), n)],
            "spot": [spot[i:i + n] for i in range(0, len(spot), n)],
        }
        self.conn_keys = [(v, i) for v, chunks in self.plan.items() for i in range(len(chunks))]
        self.connected = {k: False for k in self.conn_keys}
        self.last_msg = {"fut": 0.0, "spot": 0.0}
        self._last_real: dict[tuple[str, str], float] = {}

    def urls(self, venue: str) -> list[str]:
        return [f"{self.bases[venue]}/stream?streams={'/'.join(c)}" for c in self.plan[venue]]

    def alive(self, venue: str, now: float) -> bool:
        """Every connection of the venue is up and the venue has produced data recently."""
        all_up = all(up for (v, _), up in self.connected.items() if v == venue)
        return all_up and now - self.last_msg[venue] <= self.cfg.feed.ws_stale_s

    def healthy(self, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        return self.alive("fut", now) and self.alive("spot", now)

    def handle(self, venue: str, payload: dict | list, now: float) -> None:
        """Apply one decoded stream message. Unknown symbols/shapes are ignored."""
        self.last_msg[venue] = now
        data = payload.get("data", payload) if isinstance(payload, dict) else payload
        if venue == "fut" and isinstance(data, list):  # !markPrice@arr
            for e in data:
                m = self.by_perp.get(e.get("s"))
                if m is None:
                    continue
                m.mark = float(e["p"])
                m.index = float(e["i"])
                m.funding_rate = float(e.get("r") or 0.0)
                m.next_funding_ms = int(e.get("T") or 0)
            if data and data[0].get("E"):
                self.board.latency_ms = max(0.0, now * 1000 - float(data[0]["E"]))
            return
        if not isinstance(data, dict) or "b" not in data:
            return
        sym = data.get("s")
        m = self.by_perp.get(sym) if venue == "fut" else self.by_spot.get(sym)
        if m is None:
            return
        bid, ask = float(data["b"]), float(data["a"])
        if bid <= 0 or ask <= 0:
            return
        q = Quote(bid, float(data["B"]), ask, float(data["A"]), now)
        if venue == "fut":
            m.perp = q
        else:
            m.spot = q
        self._last_real[(venue, sym)] = now

    def touch(self, now: float) -> None:
        """Re-confirm quotes of live venues whose symbol has updated recently (book unchanged != stale)."""
        silent = self.cfg.feed.ws_max_silent_s
        for m in self.board.markets.values():
            for venue, sym, q in (("fut", m.symbol, m.perp), ("spot", m.spot_symbol, m.spot)):
                if q is not None and self.alive(venue, now) and now - self._last_real.get((venue, sym), 0.0) <= silent:
                    q.ts = now

    async def _run_conn(self, venue: str, idx: int) -> None:
        backoff = 1.0
        key = (venue, idx)
        url = self.urls(venue)[idx]
        while True:
            try:
                # receive_timeout is generous: a chunk of quieter symbols can be silent for a while,
                # and heartbeat pings already detect a dead socket.
                async with self.session.ws_connect(url, heartbeat=20, receive_timeout=60) as ws:
                    self.connected[key] = True
                    self.last_msg[venue] = time.time()
                    log.info("%s stream %d/%d connected", venue, idx + 1, len(self.plan[venue]))
                    backoff = 1.0
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self.handle(venue, json.loads(msg.data), time.time())
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("%s stream %d error: %s: %s", venue, idx + 1, type(e).__name__, e)
                self.board.last_error = f"{venue} stream: {type(e).__name__}: {e}"
            finally:
                self.connected[key] = False
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 2)

    async def run(self) -> None:
        tasks = [asyncio.create_task(self._run_conn(v, i)) for v, i in self.conn_keys]
        try:
            while True:
                await asyncio.sleep(self.cfg.feed.interval_s)
                now = time.time()
                if self.healthy(now):
                    self.touch(now)
                    b = self.board
                    b.polls += 1
                    b.last_update = now
                    b.ok = True
                    b.source = "ws"
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


class HybridFeed:
    """WebSocket primary; REST polling takes over whenever the streams are unhealthy, and hands back on recovery."""

    def __init__(self, ws: WsFeed, rest: RestFeed, board: Board, cfg: Config):
        self.ws, self.rest, self.board, self.cfg = ws, rest, board, cfg

    async def run(self) -> None:
        ws_task = asyncio.create_task(self.ws.run())
        was_ws = False
        try:
            while True:
                started = time.monotonic()
                if self.ws.healthy():
                    if not was_ws:
                        log.info("WebSocket feed healthy; REST fallback idle")
                    was_ws = True
                    delay = self.cfg.feed.interval_s
                else:
                    if was_ws or self.board.source != "rest":
                        log.warning("WebSocket feed unhealthy; polling REST as fallback")
                    was_ws = False
                    self.board.source = "rest"
                    delay = max(0.0, self.cfg.feed.interval_s - (time.monotonic() - started))
                    delay += await self.rest.step()
                await asyncio.sleep(delay)
        finally:
            ws_task.cancel()
            await asyncio.gather(ws_task, return_exceptions=True)


_SIM_BASES = (
    "BTC ETH BNB SOL XRP DOGE ADA AVAX LINK DOT TRX LTC BCH ATOM NEAR APT ARB OP SUI INJ FIL ETC HBAR UNI AAVE MKR "
    "RUNE SEI TIA STX IMX GRT ALGO EGLD XLM VET SAND MANA AXS CRV LDO SNX DYDX PENDLE JUP WLD ENA ONDO TON "
    "1000SHIB 1000PEPE 1000BONK 1000FLOKI ORDI WIF FET GALA"
).split()


class SimFeed:
    """Synthetic universe. Basis is an Ornstein-Uhlenbeck process plus occasional decaying shocks."""

    def __init__(self, board: Board, cfg: Config, seed: int | None = None, shock_prob: float = 0.003):
        self.board = board
        self.cfg = cfg
        self.rng = random.Random(seed)
        self.shock_prob = shock_prob
        self._st: dict[str, dict] = {}

    def load_universe(self) -> None:
        rng = self.rng
        for base in _SIM_BASES[: self.cfg.universe.max_markets]:
            spot_base, mult = split_base(base)
            sym = base + "USDT"
            price = math.exp(rng.uniform(math.log(0.05), math.log(60000)))
            self.board.markets[sym] = Market(
                symbol=sym, spot_symbol=spot_base + "USDT", base_asset=spot_base, mult=mult,
                quote_volume_usd=rng.uniform(5e7, 5e9),
                fut_filter=SymbolFilter(step=0.001, min_qty=0.001, min_notional=5.0),
                spot_filter=SymbolFilter(step=0.001, min_qty=0.001, min_notional=5.0),
            )
            self._st[sym] = {
                "spot": price, "vol": rng.uniform(0.0002, 0.0012), "basis": rng.gauss(2, 2), "mu": rng.gauss(2, 2),
                "spot_spread": rng.uniform(1.0, 3.5), "perp_spread": rng.uniform(0.6, 2.5),
                "depth_usd": rng.uniform(4_000, 60_000), "funding": rng.gauss(0.0001, 0.00015),
            }

    def step(self) -> None:
        rng, now = self.rng, time.time()
        next_funding = (int(now // 28800) + 1) * 28800 * 1000
        for m in self.board.markets.values():
            st = self._st[m.symbol]
            st["spot"] *= math.exp(rng.gauss(0, st["vol"]))
            st["basis"] += 0.15 * (st["mu"] - st["basis"]) + rng.gauss(0, 0.8)
            if rng.random() < self.shock_prob:
                st["basis"] += rng.uniform(25, 75) * (1 if rng.random() < 0.8 else -1)
            spot_mid = st["spot"]
            perp_mid = spot_mid * m.mult * (1 + st["basis"] / 1e4)
            sh, ph = st["spot_spread"] / 2e4, st["perp_spread"] / 2e4
            depth = st["depth_usd"] * rng.uniform(0.5, 1.5)
            m.spot = Quote(spot_mid * (1 - sh), depth / spot_mid, spot_mid * (1 + sh), depth / spot_mid, now)
            m.perp = Quote(perp_mid * (1 - ph), depth / perp_mid, perp_mid * (1 + ph), depth / perp_mid, now)
            m.index = spot_mid * m.mult * (1 + rng.gauss(0, 0.00003))
            m.mark = m.index * (1 + st["basis"] / 2e4)
            m.funding_rate = st["funding"]
            m.next_funding_ms = next_funding
        b = self.board
        b.polls += 1
        b.latency_ms = 0.0
        b.last_update = now
        b.ok = True

    async def run(self) -> None:
        while True:
            started = time.monotonic()
            self.step()
            await asyncio.sleep(max(0.0, self.cfg.feed.interval_s - (time.monotonic() - started)))
