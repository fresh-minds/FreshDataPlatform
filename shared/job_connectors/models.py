from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, Literal, Optional

EmploymentType = Literal["perm", "contract", "freelance", "unknown"]
RemotePolicy = Literal["remote", "hybrid", "onsite", "unknown"]


@dataclass(frozen=True)
class JobRef:
    """Lightweight job reference discovered by a connector."""

    source: str
    source_job_id: str
    url: str
    title: Optional[str] = None
    published_at: Optional[datetime] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RawJobPayload:
    """Raw fetched payload for a job detail page or feed item."""

    source: str
    source_job_id: str
    url: str
    fetched_at: datetime
    body: str
    status_code: Optional[int] = None
    content_type: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class NormalizedJob:
    """Canonical normalized job record used by the ingestion pipeline."""

    source: str
    source_job_id: str
    url: str
    title: str
    company: Optional[str]
    location_text: Optional[str]
    description: Optional[str]
    published_at: Optional[datetime]
    employment_type: EmploymentType
    remote_policy: RemotePolicy
    checksum: str

    def as_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        if self.published_at is not None:
            data["published_at"] = self.published_at.isoformat()
        return data
