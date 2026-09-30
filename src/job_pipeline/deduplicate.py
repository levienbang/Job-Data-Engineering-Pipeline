"""Source-level identities for Gold."""

from collections.abc import Mapping


def identity(job: Mapping[str, object]) -> tuple[str, str, str]:
    source = str(job["source_name"])
    source_id = job.get("source_job_id")
    if source_id:
        return source, "id", str(source_id)
    return source, "hash", str(job["source_record_hash"])
