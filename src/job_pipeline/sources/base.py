"""Minimal source boundary for milestone 1."""

from collections.abc import Iterator
from datetime import datetime
from typing import Protocol

from job_pipeline.contracts import CanonicalJob


class SourceAdapter(Protocol):
    def iter_jobs(self, *, collected_at: datetime) -> Iterator[CanonicalJob]: ...
