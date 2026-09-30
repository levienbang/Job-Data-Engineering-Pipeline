"""Both acquisition paths satisfy the same minimal source contract."""

from datetime import UTC, datetime
from pathlib import Path

import httpx

from job_pipeline.contracts import CanonicalJob
from job_pipeline.sources.base import SourceAdapter
from job_pipeline.sources.fake_jobs import LIST_URL, FakeJobsCrawler
from job_pipeline.sources.vietjobs import VietJobsDatasetSource

FIXTURES = Path(__file__).parent / "fixtures"
COLLECTED_AT = datetime(2026, 9, 30, tzinfo=UTC)


def _collect(source: SourceAdapter) -> list[CanonicalJob]:
    return list(source.iter_jobs(collected_at=COLLECTED_AT))


def test_vietjobs_and_fake_jobs_share_the_canonical_contract(tmp_path: Path) -> None:
    vietjobs = _collect(VietJobsDatasetSource(FIXTURES / "vietjobs_small.csv"))
    list_html = (FIXTURES / "fake_jobs" / "list.html").read_text(encoding="utf-8")
    detail = (FIXTURES / "fake_jobs" / "senior-python-developer-0.html").read_text(
        encoding="utf-8"
    )

    def respond(request: httpx.Request) -> httpx.Response:
        if str(request.url) == LIST_URL:
            return httpx.Response(200, text=list_html)
        if str(request.url).endswith("senior-python-developer-0.html"):
            return httpx.Response(200, text=detail)
        return httpx.Response(404)

    # One complete fake job is enough to exercise the common interface.
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with FakeJobsCrawler(
            client=client,
            raw_html_dir=tmp_path,
            parsed_records_dir=tmp_path / "parsed",
            min_request_interval_seconds=0,
        ) as fake_source:
            fake_job = next(iter(fake_source.iter_jobs(collected_at=COLLECTED_AT)))

    for job in [*vietjobs, fake_job]:
        assert isinstance(job, CanonicalJob)
        assert job.collected_at == COLLECTED_AT
        assert len(job.source_record_hash) == 64
    assert vietjobs[0].source_url is None
    assert vietjobs[0].raw_company is None
    assert fake_job.source_url is not None
    assert fake_job.raw_company == "Example Labs"
