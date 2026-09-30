"""Small canonical record shape shared by source adapters."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class CanonicalJob:
    source_name: str
    source_job_id: str | None
    source_url: str | None
    raw_title: str
    raw_company: str | None
    raw_location: str | None
    raw_employment_type: str | None
    raw_description: str
    posted_at: datetime | None
    collected_at: datetime
    source_record_hash: str
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
    schema_version: int = 2
