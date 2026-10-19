"""Publishes a compact, READ-ONLY snapshot of the bot to Upstash Redis (REST) so a hosted dashboard can show it.

Outbound only: the hosted page can look but never touch (no endpoint on the bot is reachable from outside).
What leaves the machine is the dashboard snapshot minus control state and raw error strings; credentials are never
included, and the token is redacted from any log line. Failures can never affect trading.
"""
import asyncio
import hashlib
import json
import logging
import math
import time

import aiohttp

from .config import PublisherCfg

log = logging.getLogger("publisher")
SCHEMA = 1
KEEP = ("ts", "mode", "env", "feed", "halt", "stats", "model", "markets", "positions", "trades", "events")


def _round(obj, nd: int = 4):
    """Round floats (smaller payload) and turn NaN/inf into null so the JSON is always valid."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else round(obj, nd)
    if isinstance(obj, dict):
        return {k: _round(v, nd) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_round(v, nd) for v in obj]
    return obj


def public_snapshot(snap: dict, now: float | None = None) -> dict:
    """The part of the dashboard snapshot that is allowed to leave the machine."""
    out = {k: snap[k] for k in KEEP if k in snap}
    out["feed"] = {k: v for k, v in snap.get("feed", {}).items() if k != "last_error"}  # error text can name hosts
    halt = snap.get("halt", {})
    out["halt"] = {"halted": halt.get("halted", False), "reason": halt.get("reason", ""), "trading": halt.get("trading", True)}
    out["events"] = list(snap.get("events", []))[:40]
    out["published_at"] = time.time() if now is None else now
    out["schema"] = SCHEMA
    return _round(out)


class SnapshotPublisher:
    def __init__(self, cfg: PublisherCfg, engine, url: str = "", token: str = "",
                 session: aiohttp.ClientSession | None = None):
        self.cfg, self.engine = cfg, engine
        self.url = url.strip().rstrip("/")
        self.token = token.strip()
        self.session = session
        self.pushed = 0
        self.failed = 0
        self.last_error = ""
        self._consecutive = 0
        self._last_warned = 0.0

    @property
    def enabled(self) -> bool:
        return bool(self.cfg.enabled and self.url and self.token and self.session is not None)

    def fingerprints(self) -> tuple[str, str]:
        """Short one-way hashes of the URL and token, safe to log: compare two machines' settings without a secret."""
        def fp(value: str) -> str:
            return hashlib.sha256(value.encode()).hexdigest()[:8] if value else "-"
        return fp(self.url), fp(self.token)

    def _redact(self, text: str) -> str:
        return text.replace(self.token, "***") if self.token else text

    def next_delay(self) -> float:
        """Normal interval; backs off (up to 60 s) while the store is unreachable."""
        if self._consecutive == 0:
            return self.cfg.interval_s
        return min(60.0, self.cfg.interval_s * 2 ** min(self._consecutive, 6))

    async def push_once(self) -> bool:
        try:
            payload = json.dumps(public_snapshot(self.engine.snapshot()), separators=(",", ":"))
            body = ["SET", self.cfg.key, payload, "EX", str(int(self.cfg.ttl_s))]
            async with self.session.post(self.url, json=body, headers={"Authorization": f"Bearer {self.token}"},
                                         timeout=aiohttp.ClientTimeout(total=8)) as r:
                data = await r.json(content_type=None)
                if r.status == 200 and isinstance(data, dict) and data.get("result") == "OK":
                    self.pushed += 1
                    self._consecutive = 0
                    return True
                err = data.get("error") if isinstance(data, dict) else None
                raise RuntimeError(f"HTTP {r.status}: {err or 'unexpected response'}")
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - publishing must never affect trading
            self.failed += 1
            self._consecutive += 1
            self.last_error = self._redact(f"{type(e).__name__}: {e}")
            now = time.monotonic()
            if now - self._last_warned > 60:  # at most one warning a minute, never silent for good
                self._last_warned = now
                log.warning("dashboard publish failed (%s); %d failure(s) so far", self.last_error, self.failed)
            return False

    async def run(self) -> None:
        if not self.enabled:
            while True:  # keep the task alive: a finished task would be mistaken for a crash
                await asyncio.sleep(3600)
        while True:
            await self.push_once()
            await asyncio.sleep(self.next_delay())
