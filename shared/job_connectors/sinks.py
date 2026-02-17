from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Dict, Tuple, cast

from shared.job_connectors.models import EmploymentType, NormalizedJob, RemotePolicy
from shared.job_connectors.normalize import parse_datetime

UpsertAction = str  # "inserted" | "updated" | "skipped"


class JobSink:
    def upsert(self, job: NormalizedJob) -> UpsertAction:
        raise NotImplementedError

    def close(self) -> None:
        return None


@dataclass
class _StoredJob:
    job: NormalizedJob


class InMemorySink(JobSink):
    def __init__(self) -> None:
        self.items: Dict[Tuple[str, str], NormalizedJob] = {}
        self._by_url: Dict[str, Tuple[str, str]] = {}

    def upsert(self, job: NormalizedJob) -> UpsertAction:
        key = (job.source, job.source_job_id)
        existing = self.items.get(key)
        if existing is not None:
            if existing.checksum == job.checksum:
                return "skipped"
            self.items[key] = job
            self._by_url[job.url] = key
            return "updated"

        other_key = self._by_url.get(job.url)
        if other_key is not None and other_key != key:
            # Duplicate URL discovered under a different key.
            return "skipped"

        self.items[key] = job
        self._by_url[job.url] = key
        return "inserted"


class JsonlSink(JobSink):
    """Upsert sink that stores NormalizedJob records in a JSONL file."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._dirty = False
        self._items: Dict[Tuple[str, str], NormalizedJob] = {}
        self._by_url: Dict[str, Tuple[str, str]] = {}
        self._load_existing()

    def _load_existing(self) -> None:
        if not os.path.exists(self._path):
            return

        with open(self._path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                try:
                    source = str(obj.get("source"))
                    source_job_id = str(obj.get("source_job_id"))
                    url = str(obj.get("url"))
                    title = str(obj.get("title"))
                    checksum = str(obj.get("checksum"))
                except Exception:
                    continue

                # Optional fields
                company = obj.get("company")
                location_text = obj.get("location_text")
                description = obj.get("description")
                published_at = obj.get("published_at")
                employment_type_raw = obj.get("employment_type") or "unknown"
                remote_policy_raw = obj.get("remote_policy") or "unknown"
                employment_type_str = str(employment_type_raw)
                remote_policy_str = str(remote_policy_raw)
                if employment_type_str not in {"perm", "contract", "freelance", "unknown"}:
                    employment_type_str = "unknown"
                if remote_policy_str not in {"remote", "hybrid", "onsite", "unknown"}:
                    remote_policy_str = "unknown"
                employment_type = cast(EmploymentType, employment_type_str)
                remote_policy = cast(RemotePolicy, remote_policy_str)

                # Keep published_at as string in storage; loader doesn't re-parse to datetime
                # because the sink only needs checksum-based upserts.
                job = NormalizedJob(
                    source=source,
                    source_job_id=source_job_id,
                    url=url,
                    title=title,
                    company=company if isinstance(company, str) or company is None else str(company),
                    location_text=location_text
                    if isinstance(location_text, str) or location_text is None
                    else str(location_text),
                    description=description
                    if isinstance(description, str) or description is None
                    else str(description),
                    published_at=parse_datetime(published_at) if isinstance(published_at, str) else None,
                    employment_type=employment_type,
                    remote_policy=remote_policy,
                    checksum=checksum,
                )
                key = (job.source, job.source_job_id)
                self._items[key] = job
                self._by_url[job.url] = key

    def upsert(self, job: NormalizedJob) -> UpsertAction:
        key = (job.source, job.source_job_id)

        existing = self._items.get(key)
        if existing is not None:
            if existing.checksum == job.checksum:
                return "skipped"
            self._items[key] = job
            self._by_url[job.url] = key
            self._dirty = True
            return "updated"

        other_key = self._by_url.get(job.url)
        if other_key is not None and other_key != key:
            return "skipped"

        self._items[key] = job
        self._by_url[job.url] = key
        self._dirty = True
        return "inserted"

    def close(self) -> None:
        if not self._dirty:
            return

        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        tmp_path = f"{self._path}.tmp"

        with open(tmp_path, "w", encoding="utf-8") as f:
            for job in self._items.values():
                f.write(json.dumps(job.as_dict(), ensure_ascii=True, sort_keys=True))
                f.write("\n")

        os.replace(tmp_path, self._path)
        self._dirty = False


class StdoutSink(JobSink):
    def __init__(self) -> None:
        self.count = 0

    def upsert(self, job: NormalizedJob) -> UpsertAction:
        print(json.dumps(job.as_dict(), ensure_ascii=True, sort_keys=True))
        self.count += 1
        return "inserted"
