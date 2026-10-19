"""Minimal async Binance REST client (public + HMAC-signed) for USD-M futures and spot."""
import hashlib
import hmac
import json
import time
import urllib.parse

import aiohttp

HOSTS = {
    "mainnet": {"fapi": "https://fapi.binance.com", "spot": "https://api.binance.com"},
    "testnet": {"fapi": "https://testnet.binancefuture.com", "spot": "https://testnet.binance.vision"},
}


class BinanceError(Exception):
    def __init__(self, status: int, code: int | None, msg: str, path: str):
        super().__init__(f"{path} -> HTTP {status} code={code} {msg}")
        self.status, self.code, self.msg, self.path = status, code, msg, path


class BinanceClient:
    def __init__(
        self, session: aiohttp.ClientSession, env: str = "mainnet", api_key: str = "", api_secret: str = "",
        timeout_s: float = 4.0,
    ):
        self.session = session
        self.hosts = HOSTS[env]
        self.env = env
        self.api_key = api_key
        self.api_secret = api_secret
        self.timeout = aiohttp.ClientTimeout(total=timeout_s)
        self.time_offset_ms = 0
        self.used_weight = {"fapi": 0, "spot": 0}
        self.backoff_until = 0.0

    async def request(self, venue: str, method: str, path: str, params: dict | None = None, signed: bool = False,
                      timeout: float | None = None):
        if time.time() < self.backoff_until:
            raise BinanceError(429, None, "local rate-limit backoff active", path)
        params = {k: v for k, v in (params or {}).items() if v is not None}
        headers = {}
        if signed:
            if not (self.api_key and self.api_secret):
                raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET are required for signed requests")
            params["timestamp"] = int(time.time() * 1000) + self.time_offset_ms
            params["recvWindow"] = 5000
            query = urllib.parse.urlencode(params)
            sig = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
            query = f"{query}&signature={sig}"
        else:
            query = urllib.parse.urlencode(params)
        if self.api_key:
            headers["X-MBX-APIKEY"] = self.api_key
        url = f"{self.hosts[venue]}{path}" + (f"?{query}" if query else "")
        limit = aiohttp.ClientTimeout(total=timeout) if timeout else self.timeout
        async with self.session.request(method, url, headers=headers, timeout=limit) as resp:
            body = await resp.text()
            weight = resp.headers.get("X-MBX-USED-WEIGHT-1M") or resp.headers.get("X-MBX-USED-WEIGHT")
            if weight and weight.isdigit():
                self.used_weight[venue] = int(weight)
            if resp.status in (418, 429):
                retry_after = resp.headers.get("Retry-After", "10")
                self.backoff_until = time.time() + (int(retry_after) if retry_after.isdigit() else 10)
            try:
                data = json.loads(body)
            except ValueError:
                data = None
            if resp.status != 200:
                code = data.get("code") if isinstance(data, dict) else None
                msg = data.get("msg") if isinstance(data, dict) else body[:200]
                raise BinanceError(resp.status, code, str(msg), path)
            return data

    async def get(self, venue: str, path: str, params: dict | None = None, signed: bool = False,
                  timeout: float | None = None):
        return await self.request(venue, "GET", path, params, signed, timeout)

    async def post(self, venue: str, path: str, params: dict | None = None, signed: bool = True):
        return await self.request(venue, "POST", path, params, signed)

    async def sync_time(self) -> int:
        """Align signed-request timestamps with Binance server time; returns the offset in ms."""
        t0 = int(time.time() * 1000)
        data = await self.get("fapi", "/fapi/v1/time")
        t1 = int(time.time() * 1000)
        self.time_offset_ms = int(data["serverTime"]) - (t0 + t1) // 2
        return self.time_offset_ms
