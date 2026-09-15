from __future__ import annotations

import random
import time
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Callable
from urllib.parse import urlsplit

import requests


@dataclass(slots=True)
class HttpResult:
    response: requests.Response
    elapsed_ms: int


class ResilientHttpClient:
    """Cliente público con timeout y tres reintentos progresivos."""

    # Shared by listing and attachment readers; do not bypass portal limits.
    _host_lock = threading.Lock()
    _host_last: dict[str, float] = {}
    _host_cooldown: dict[str, float] = {}
    host_intervals = {'www.ungm.org': 4.0}

    def __init__(
        self,
        *,
        timeout: float = 25.0,
        retries: int = 3,
        minimum_interval: float = 0.15,
        sleeper: Callable[[float], None] = time.sleep,
        session: requests.Session | None = None,
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.minimum_interval = minimum_interval
        self.sleeper = sleeper
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (compatible; RS-RIR-OpportunityMonitor/1.0; "
                    "+https://rs-sp-rir.streamlit.app)"
                ),
                "Accept-Language": "es-PA,es;q=0.9,en;q=0.7",
            }
        )
        self._last_request_at = 0.0

    @staticmethod
    def retry_after(value: str | None) -> float:
        try:
            return max(0.0, float(value or '0'))
        except ValueError:
            try:
                date = parsedate_to_datetime(value)
                if date.tzinfo is None:
                    date = date.replace(tzinfo=timezone.utc)
                return max(0.0, (date - datetime.now(timezone.utc)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                return 0.0

    def _pace(self, host: str) -> None:
        if host not in self.host_intervals:
            wait = self.minimum_interval - (time.monotonic() - self._last_request_at)
            if wait > 0:
                self.sleeper(wait)
            return
        with self._host_lock:
            now = time.monotonic()
            due = max(self._host_last.get(host, 0) + self.host_intervals[host],
                      self._host_cooldown.get(host, 0))
            wait = max(0.0, due - now)
            if wait > 60:
                raise requests.HTTPError(f'{host}: espera indicada por el portal; consulta aplazada ({wait:.0f}s)')
            if wait:
                self.sleeper(wait)
            self._host_last[host] = time.monotonic()

    def request(self, method: str, url: str, **kwargs) -> HttpResult:
        last_error: Exception | None = None
        timeout = kwargs.pop('timeout', self.timeout)
        host = urlsplit(url).hostname or ''
        for attempt in range(self.retries + 1):
            self._pace(host)
            started = time.monotonic()
            response = None
            try:
                response = self.session.request(
                    method,
                    url,
                    timeout=timeout,
                    **kwargs,
                )
                self._last_request_at = time.monotonic()
                if response.status_code in {429, 500, 502, 503, 504}:
                    response.raise_for_status()
                response.raise_for_status()
                return HttpResult(response=response, elapsed_ms=int((time.monotonic() - started) * 1000))
            except (requests.RequestException, TimeoutError) as exc:
                self._last_request_at = time.monotonic()
                last_error = exc
                code = response.status_code if response is not None else None
                delay = self.retry_after(response.headers.get('Retry-After')) if response is not None else 0
                if response is not None:
                    response.close()
                if code == 429:
                    delay = max(delay, 15.0 * (2 ** attempt))
                    with self._host_lock:
                        self._host_cooldown[host] = time.monotonic() + delay
                # Permanent 4xx errors cannot be repaired by hammering the URL.
                if attempt >= self.retries or (code is not None and code not in {408,429,500,502,503,504}):
                    raise
                if delay > 60:
                    raise
                delay = max(delay, (2**attempt) + random.uniform(0.0, 0.25))
                logging.getLogger('otras_fuentes').warning('%s HTTP %s: reintento %s en %.1fs', host, code or 'timeout', attempt + 1, delay)
                self.sleeper(delay)
        raise RuntimeError(str(last_error or "Fallo HTTP desconocido"))

    def get(self, url: str, **kwargs) -> HttpResult:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> HttpResult:
        return self.request("POST", url, **kwargs)
