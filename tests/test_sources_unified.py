"""Both acquisition paths satisfy the same minimal source contract."""

from datetime import UTC, datetime
from pathlib import Path

import httpx

from job_pipeline.contracts import CanonicalJob
from job_pipeline.sources.base import RawSource
from job_pipeline.sources.fake_jobs import LIST_URL, FakeJobsCrawler, map_fake_jobs_row
from job_pipeline.sources.vietjobs import VietJobsDatasetSource, map_vietjobs_row

FIXTURES = Path(__file__).parent / "fixtures"
COLLECTED_AT = datetime(2026, 9, 30, tzinfo=UTC)


def _collect(source: RawSource) -> list[CanonicalJob]:
    assert isinstance(source, RawSource)
    mapper = {"vietjobs": map_vietjobs_row, "fake_jobs": map_fake_jobs_row}[
        source.source_name
    ]
    return [mapper(raw, collected_at=COLLECTED_AT) for raw in source.iter_raw()]


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
            min_request_interval_seconds=0,
        ) as fake_source:
            assert isinstance(fake_source, RawSource)
            fake_job = map_fake_jobs_row(
                next(fake_source.iter_raw()), collected_at=COLLECTED_AT
            )

    for job in [*vietjobs, fake_job]:
        assert isinstance(job, CanonicalJob)
        assert job.collected_at == COLLECTED_AT
        assert len(job.source_record_hash) == 64
    assert vietjobs[0].source_url is None
    assert vietjobs[0].raw_company is None
    assert fake_job.source_url is not None
    assert fake_job.raw_company == "Example Labs"
