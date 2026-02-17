from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Tuple
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

FetchRobotsFn = Callable[[str], Tuple[int, str]]


@dataclass(frozen=True)
class RobotsDecision:
    allowed: bool
    robots_url: str
    reason: str


class RobotsCache:
    def __init__(
        self,
        *,
        fetch_robots_txt: FetchRobotsFn,
        user_agent: str,
        strict: bool = True,
        ttl_s: int = 60 * 60,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._fetch = fetch_robots_txt
        self._ua = user_agent
        self._strict = strict
        self._ttl_s = ttl_s
        self._now = now
        self._cache: Dict[str, Tuple[float, RobotFileParser]] = {}

    def _robots_url_for(self, url: str) -> Optional[str]:
        parts = urlsplit(url)
        if not parts.scheme or not parts.netloc:
            return None
        return f"{parts.scheme}://{parts.netloc}/robots.txt"

    def _get(self, robots_url: str) -> RobotFileParser:
        cached = self._cache.get(robots_url)
        if cached is not None:
            fetched_at, rp = cached
            if (self._now() - fetched_at) < self._ttl_s:
                return rp

        rp = RobotFileParser()
        rp.set_url(robots_url)
        try:
            status_code, body = self._fetch(robots_url)
        except Exception:
            # Avoid relying on internal allow_all/disallow_all flags (not in type stubs).
            rp.parse(["User-agent: *", "Disallow: /" if self._strict else "Disallow:"])
            self._cache[robots_url] = (self._now(), rp)
            return rp

        if status_code == 404:
            rp.parse(["User-agent: *", "Disallow:"])
        elif status_code >= 400:
            rp.parse(["User-agent: *", "Disallow: /" if self._strict else "Disallow:"])
        else:
            lines = body.splitlines()
            rp.parse(lines)

        self._cache[robots_url] = (self._now(), rp)
        return rp

    def check(self, url: str) -> RobotsDecision:
        robots_url = self._robots_url_for(url)
        if robots_url is None:
            # If we can't parse host/scheme, err on the side of safety when strict.
            return RobotsDecision(
                allowed=not self._strict,
                robots_url="",
                reason="invalid_url",
            )

        rp = self._get(robots_url)
        allowed = rp.can_fetch(self._ua, url)
        return RobotsDecision(
            allowed=bool(allowed),
            robots_url=robots_url,
            reason="allow" if allowed else "disallow",
        )
