"""Web scraping / ingestion connectors for job listings.

This package provides:
- A connector interface (list refs -> fetch raw -> normalize)
- Shared HTTP utilities (rate limiting, retries, robots.txt compliance)
- Normalization utilities to emit canonical NormalizedJob records
"""

from shared.job_connectors.models import JobRef, NormalizedJob, RawJobPayload

__all__ = ["JobRef", "NormalizedJob", "RawJobPayload"]
