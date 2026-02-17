from __future__ import annotations

import gzip
import json
import os
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Optional

from shared.job_connectors.models import RawJobPayload


class RawPayloadStore:
    def __init__(
        self,
        *,
        base_dir: str,
        enabled: bool,
        retention_days: int = 7,
    ) -> None:
        self._base_dir = base_dir
        self._enabled = bool(enabled)
        self._retention_days = max(0, int(retention_days))

    def _cleanup(self) -> None:
        if self._retention_days <= 0:
            return
        if not os.path.isdir(self._base_dir):
            return

        cutoff = datetime.now(timezone.utc) - timedelta(days=self._retention_days)
        cutoff_ts = cutoff.timestamp()

        for root, _dirs, files in os.walk(self._base_dir):
            for name in files:
                path = os.path.join(root, name)
                try:
                    st = os.stat(path)
                except OSError:
                    continue
                if st.st_mtime < cutoff_ts:
                    try:
                        os.remove(path)
                    except OSError:
                        pass

    def save(self, payload: RawJobPayload) -> Optional[str]:
        if not self._enabled:
            return None

        self._cleanup()

        day = payload.fetched_at.astimezone(timezone.utc).strftime("%Y%m%d")
        url_hash = sha256(payload.url.encode("utf-8", errors="replace")).hexdigest()[:16]
        ts = int(payload.fetched_at.timestamp())
        rel_dir = os.path.join(payload.source, day)
        out_dir = os.path.join(self._base_dir, rel_dir)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{payload.source_job_id}-{url_hash}-{ts}.json.gz")

        data = asdict(payload)
        data["fetched_at"] = payload.fetched_at.isoformat()

        with gzip.open(out_path, "wt", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=True, sort_keys=True)

        return out_path
