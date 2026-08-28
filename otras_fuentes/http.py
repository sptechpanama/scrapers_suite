from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable

import requests


@dataclass(slots=True)
class HttpResult:
    response: requests.Response
    elapsed_ms: int


class ResilientHttpClient:
    """Cliente público con timeout y tres reintentos progresivos."""

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

    def request(self, method: str, url: str, **kwargs) -> HttpResult:
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            wait_for_rate = self.minimum_interval - (time.monotonic() - self._last_request_at)
            if wait_for_rate > 0:
                self.sleeper(wait_for_rate)
            started = time.monotonic()
            try:
                response = self.session.request(
                    method,
                    url,
                    timeout=kwargs.pop("timeout", self.timeout),
                    **kwargs,
                )
                self._last_request_at = time.monotonic()
                if response.status_code in {429, 500, 502, 503, 504}:
                    response.raise_for_status()
                response.raise_for_status()
                return HttpResult(response=response, elapsed_ms=int((time.monotonic() - started) * 1000))
            except (requests.RequestException, TimeoutError) as exc:
                last_error = exc
                if attempt >= self.retries:
                    raise
                self.sleeper((2**attempt) + random.uniform(0.0, 0.25))
        raise RuntimeError(str(last_error or "Fallo HTTP desconocido"))

    def get(self, url: str, **kwargs) -> HttpResult:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> HttpResult:
        return self.request("POST", url, **kwargs)
