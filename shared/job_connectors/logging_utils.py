from __future__ import annotations

import json
import logging
from typing import Any, Dict


def log_event(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    """Log a single-line JSON event for easy parsing in log aggregators."""

    payload: Dict[str, Any] = {"event": event, **fields}
    try:
        msg = json.dumps(payload, ensure_ascii=True, sort_keys=True, default=str)
    except TypeError:
        # As a last resort, stringify non-serializable values.
        safe_payload = {
            k: (v if isinstance(v, (str, int, float, bool)) or v is None else str(v)) for k, v in payload.items()
        }
        msg = json.dumps(safe_payload, ensure_ascii=True, sort_keys=True)
    logger.log(level, msg)
