"""Pydantic checks for the DE canonical contract."""

import re
from datetime import datetime, timedelta
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ValidatedJob(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    source_name: Literal["fake_jobs", "vietjobs", "arbeitnow"]
    source_job_id: str | None
    source_url: str | None
    raw_title: str
    raw_company: str | None
    raw_location: str | None
    raw_employment_type: str | None
    raw_description: str
    raw_requirements: str | None = None
    raw_experience: str | None = None
    raw_technical_skills: str | None = None
    raw_soft_skills: str | None = None
    raw_qualifications: str | None = None
    raw_languages: str | None = None
    raw_benefits: str | None = None
    raw_salary: str | None = None
    raw_working_hours: str | None = None
    raw_category: str | None = None
    raw_country: str | None = None
    posted_at: datetime | None
    collected_at: datetime
    source_record_hash: str
    schema_version: Literal[2]

    @field_validator("raw_title", "raw_description")
    @classmethod
    def required_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("required text is empty")
        return value

    @field_validator("source_record_hash")
    @classmethod
    def valid_hash(cls, value: str) -> str:
        if not SHA256.fullmatch(value):
            raise ValueError("expected lowercase SHA-256")
        return value

    @field_validator("source_url")
    @classmethod
    def valid_url(cls, value: str | None) -> str | None:
        if value is not None:
            from urllib.parse import urlparse

            parsed = urlparse(value)
            if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                raise ValueError("source_url must be an absolute HTTP URL")
        return value

    @model_validator(mode="after")
    def valid_dates(self) -> "ValidatedJob":
        if self.collected_at.tzinfo is None:
            raise ValueError("collected_at needs a timezone")
        if self.posted_at is not None:
            if self.posted_at.tzinfo is None:
                raise ValueError("posted_at needs a timezone")
            if self.posted_at > self.collected_at + timedelta(days=1):
                raise ValueError("posted_at is in the future")
        return self
