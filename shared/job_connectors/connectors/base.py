from __future__ import annotations

import abc
from typing import List, Optional

from shared.job_connectors.http import PoliteHttpClient
from shared.job_connectors.models import JobRef, NormalizedJob, RawJobPayload


class BaseConnector(abc.ABC):
    """Connector interface: list -> fetch -> normalize."""

    @property
    @abc.abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def list_jobs(self, http: PoliteHttpClient, *, run_id: str) -> List[JobRef]:
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_job_details(
        self,
        http: PoliteHttpClient,
        *,
        run_id: str,
        job_ref: JobRef,
    ) -> Optional[RawJobPayload]:
        raise NotImplementedError

    @abc.abstractmethod
    def normalize(self, raw: RawJobPayload, *, run_id: str) -> Optional[NormalizedJob]:
        raise NotImplementedError
