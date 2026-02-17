from __future__ import annotations

import random
import time
from dataclasses import dataclass
from types import TracebackType
from typing import Callable, Iterable, Optional, Set, Tuple
from urllib.parse import urlsplit

import httpx

from shared.job_connectors.robots import RobotsCache, RobotsDecision


class JobConnectorHttpError(RuntimeError):
    pass


class HostNotAllowedError(JobConnectorHttpError):
    def __init__(self, host: str) -> None:
        super().__init__(f"Host not allowlisted: {host}")
        self.host = host


class RobotsDisallowedError(JobConnectorHttpError):
    def __init__(self, decision: RobotsDecision, url: str) -> None:
        super().__init__(f"robots.txt disallows fetching {url} ({decision.robots_url})")
        self.decision = decision
        self.url = url


class FetchError(JobConnectorHttpError):
    def __init__(self, url: str, status_code: Optional[int], message: str) -> None:
        super().__init__(message)
        self.url = url
        self.status_code = status_code


def _host_of(url: str) -> str:
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    return host


class PerHostRateLimiter:
    def __init__(
        self,
        *,
        min_interval_s: float,
        now: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._min_interval_s = max(0.0, float(min_interval_s))
        self._now = now
        self._sleep = sleep
        self._next_allowed: dict[str, float] = {}

    def wait(self, host: str) -> None:
        if self._min_interval_s <= 0:
            return
        ts = self._next_allowed.get(host, 0.0)
        now = self._now()
        if now < ts:
            self._sleep(ts - now)
        self._next_allowed[host] = self._now() + self._min_interval_s


@dataclass(frozen=True)
class FetchResult:
    url: str
    status_code: int
    text: str
    content_type: Optional[str]


class PoliteHttpClient:
    def __init__(
        self,
        *,
        user_agent: str,
        timeout_connect_s: float = 10.0,
        timeout_read_s: float = 20.0,
        rate_limit_per_host_s: float = 1.0,
        max_retries: int = 3,
        backoff_base_s: float = 0.5,
        backoff_max_s: float = 30.0,
        robots_strict: bool = True,
        require_allowlist: bool = True,
        allowlist_hosts: Optional[Iterable[str]] = None,
        transport: Optional[httpx.BaseTransport] = None,
        now: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        rng: Optional[random.Random] = None,
    ) -> None:
        self._ua = user_agent
        self._timeout = httpx.Timeout(
            connect=timeout_connect_s,
            read=timeout_read_s,
            write=timeout_read_s,
            pool=timeout_connect_s,
        )
        self._max_retries = max(0, int(max_retries))
        self._backoff_base_s = float(backoff_base_s)
        self._backoff_max_s = float(backoff_max_s)
        self._now = now
        self._sleep = sleep
        self._rng = rng or random.Random()

        self._allowlist_hosts: Set[str] = {h.strip().lower() for h in (allowlist_hosts or []) if h and h.strip()}
        self._require_allowlist = bool(require_allowlist)

        self._rate_limiter = PerHostRateLimiter(
            min_interval_s=float(rate_limit_per_host_s),
            now=now,
            sleep=sleep,
        )

        self._client = httpx.Client(
            timeout=self._timeout,
            follow_redirects=False,  # enforce allowlist + robots per redirect hop
            transport=transport,
            headers={
                "User-Agent": self._ua,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )

        self._robots = RobotsCache(
            fetch_robots_txt=self._fetch_robots_txt,
            user_agent=self._ua,
            strict=robots_strict,
            ttl_s=60 * 60,
            now=now,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "PoliteHttpClient":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def _enforce_allowlist(self, url: str) -> None:
        host = _host_of(url)
        if not host:
            raise FetchError(url, None, f"Invalid URL (no host): {url}")
        if self._require_allowlist and host not in self._allowlist_hosts:
            raise HostNotAllowedError(host)

    def _backoff(self, attempt: int) -> float:
        base = self._backoff_base_s * (2 ** max(0, attempt - 1))
        jitter = self._rng.random() * 0.25
        return min(self._backoff_max_s, base + jitter)

    def _is_retryable_status(self, status_code: int) -> bool:
        return status_code == 429 or 500 <= status_code <= 599

    def _parse_retry_after_s(self, value: Optional[str]) -> Optional[float]:
        if not value:
            return None
        value = value.strip()
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            return None

    def _request_with_retries(self, url: str) -> httpx.Response:
        self._enforce_allowlist(url)
        host = _host_of(url)

        last_exc: Optional[Exception] = None
        for attempt in range(1, self._max_retries + 2):
            try:
                self._rate_limiter.wait(host)
                resp = self._client.get(url)
            except (httpx.TimeoutException, httpx.TransportError) as e:
                last_exc = e
                if attempt >= self._max_retries + 1:
                    raise FetchError(url, None, f"HTTP transport error for {url}: {e}") from e
                self._sleep(self._backoff(attempt))
                continue

            if self._is_retryable_status(resp.status_code) and attempt < self._max_retries + 1:
                retry_after = self._parse_retry_after_s(resp.headers.get("Retry-After"))
                self._sleep(retry_after if retry_after is not None else self._backoff(attempt))
                continue

            return resp

        raise FetchError(url, None, f"HTTP failed for {url}: {last_exc}")

    def _follow_redirects(self, url: str, *, check_robots: bool, max_redirects: int = 5) -> httpx.Response:
        current = url
        for _ in range(max_redirects + 1):
            if check_robots:
                decision = self._robots.check(current)
                if not decision.allowed:
                    raise RobotsDisallowedError(decision, current)

            resp = self._request_with_retries(current)
            if resp.status_code not in {301, 302, 303, 307, 308}:
                return resp

            location = resp.headers.get("Location")
            if not location:
                return resp

            # Resolve relative redirects against the current URL.
            next_url = str(resp.url.join(location))
            self._enforce_allowlist(next_url)
            current = next_url

        raise FetchError(url, None, f"Too many redirects for {url}")

    def _fetch_robots_txt(self, robots_url: str) -> Tuple[int, str]:
        # Robots fetch must not recurse into robots checks.
        self._enforce_allowlist(robots_url)
        resp = self._follow_redirects(robots_url, check_robots=False)
        return int(resp.status_code), resp.text

    def get_text(self, url: str, *, check_robots: bool = True) -> FetchResult:
        self._enforce_allowlist(url)
        resp = self._follow_redirects(url, check_robots=check_robots)
        if resp.status_code >= 400:
            raise FetchError(url, int(resp.status_code), f"HTTP {resp.status_code} for {url}")

        content_type = resp.headers.get("Content-Type")
        return FetchResult(
            url=str(resp.url),
            status_code=int(resp.status_code),
            text=resp.text,
            content_type=content_type,
        )
