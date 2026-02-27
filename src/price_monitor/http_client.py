from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional, Tuple

import requests


@dataclass(frozen=True)
class HttpResult:
    status_code: int
    text: str
    headers: dict[str, str]


@dataclass(frozen=True)
class RetryPolicy:
    """Retry policy for transient network / anti-bot responses."""

    max_tries: int = 4
    base_sleep: float = 0.6
    max_sleep: float = 6.0
    retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504)


class HttpClient:
    """Simple HTTP client with retries + polite throttling.

    Notes:
    - This is intentionally lightweight (portfolio-friendly).
    - For heavy scraping you would add proxy rotation, fingerprinting, etc.
    """

    def __init__(
        self,
        *,
        timeout: float = 20.0,
        user_agent: str = "Mozilla/5.0",
        min_interval: float = 0.25,
    ) -> None:
        self.timeout = float(timeout)
        self.min_interval = float(min_interval)
        self._last_request_ts: float = 0.0

        self.sess = requests.Session()
        self.sess.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

    def _throttle(self) -> None:
        if self.min_interval <= 0:
            return
        now = time.monotonic()
        delta = now - self._last_request_ts
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)

    @staticmethod
    def _retry_after_seconds(headers: dict[str, str]) -> Optional[float]:
        ra = headers.get("Retry-After") or headers.get("retry-after")
        if not ra:
            return None
        ra = ra.strip()
        # Most common: integer seconds.
        try:
            return float(int(ra))
        except ValueError:
            return None

    def get(self, url: str, *, retry: Optional[RetryPolicy] = None) -> Tuple[Optional[HttpResult], Optional[str]]:
        rp = retry or RetryPolicy()
        last_err: Optional[str] = None

        for attempt in range(1, rp.max_tries + 1):
            self._throttle()
            try:
                r = self.sess.get(url, timeout=self.timeout)
                self._last_request_ts = time.monotonic()

                headers = {k: v for k, v in r.headers.items()}
                res = HttpResult(status_code=int(r.status_code), text=r.text, headers=headers)

                if res.status_code in rp.retry_statuses and attempt < rp.max_tries:
                    last_err = f"HTTP {res.status_code}"
                    # Respect Retry-After when present.
                    ra = self._retry_after_seconds(headers)
                    if ra is not None:
                        time.sleep(min(rp.max_sleep, max(0.0, ra)))
                        continue

                    sleep_s = min(rp.max_sleep, rp.base_sleep * (2 ** (attempt - 1)))
                    time.sleep(sleep_s)
                    continue

                return res, None

            except requests.RequestException as e:
                last_err = f"{type(e).__name__}: {e}"

            sleep_s = min(rp.max_sleep, rp.base_sleep * (2 ** (attempt - 1)))
            time.sleep(sleep_s)

        return None, last_err
