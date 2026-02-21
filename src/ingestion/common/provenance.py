"""Bronze artifact provenance: compute checksums and build metadata JSON.

Every raw artifact written to MinIO has a paired .json metadata file
stored at the same run_dt partition under /meta/. This module produces
that metadata so that all downstream consumers can trace provenance.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class ArtifactMeta:
    source_name: str
    dataset: str
    run_id: str
    artifact_id: str
    fetched_at_utc: str
    url: str
    canonical_url: str
    http_status: int
    content_type: str
    response_ms: int
    checksum_sha256: str
    byte_size: int
    # "export_download" | "xhr_json" | "dom_html" | "dbt_artifact"
    extraction_method: str
    entity_keys: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str, indent=2)


def compute_checksum(data: bytes) -> str:
    """Return the SHA-256 hex digest of *data*."""
    return hashlib.sha256(data).hexdigest()


def build_meta(
    *,
    source_name: str,
    dataset: str,
    run_id: str,
    artifact_id: str,
    url: str,
    canonical_url: str,
    http_status: int,
    content_type: str,
    response_ms: int,
    raw_bytes: bytes,
    extraction_method: str,
    entity_keys: Optional[dict] = None,
) -> ArtifactMeta:
    """Construct a fully-populated ArtifactMeta from raw artifact data."""
    return ArtifactMeta(
        source_name=source_name,
        dataset=dataset,
        run_id=run_id,
        artifact_id=artifact_id,
        fetched_at_utc=datetime.now(timezone.utc).isoformat(),
        url=url,
        canonical_url=canonical_url,
        http_status=http_status,
        content_type=content_type,
        response_ms=response_ms,
        checksum_sha256=compute_checksum(raw_bytes),
        byte_size=len(raw_bytes),
        extraction_method=extraction_method,
        entity_keys=entity_keys or {},
    )
