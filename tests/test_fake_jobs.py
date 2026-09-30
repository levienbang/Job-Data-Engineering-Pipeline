"""Offline Fake Jobs parsing and acquisition behavior."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from job_pipeline.sources.fake_jobs import (
    LIST_URL,
    FakeJobsCrawler,
    RawHtmlPage,
    map_fake_job,
    parse_detail_page,
    parse_list_page,
)

FIXTURES = Path(__file__).parent / "fixtures" / "fake_jobs"


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_list_parsing_skips_incomplete_cards_and_normalizes_whitespace() -> None:
    cards = parse_list_page(fixture("list.html"))
    assert len(cards) == 2
    assert cards[0].title == "Senior Python Developer"
    assert cards[0].company == "Example Labs"
    assert cards[0].location == "Hanoi, Vietnam"
    assert cards[0].detail_url.endswith("/senior-python-developer-0.html")
    assert cards[0].posted_at == datetime(2021, 4, 8, tzinfo=UTC)
    assert cards[1].company is None
    assert cards[1].location is None
    assert cards[1].posted_at is None


def test_detail_parsing_and_canonical_mapping() -> None:
    first, second = parse_list_page(fixture("list.html"))
    record = parse_detail_page(fixture("senior-python-developer-0.html"), card=first)
    assert (
        record.description == "Build Python services.\n\nWork with APIs and databases."
    )
    assert record.company == "Example Labs"
    assert record.location == "Hanoi, Vietnam"
    job = map_fake_job(record, collected_at=datetime(2026, 9, 30, tzinfo=UTC))
    assert job.source_name == "fake_jobs"
    assert job.source_job_id is None
    assert job.source_url == first.detail_url
    assert job.raw_employment_type is None
    assert job.raw_description == record.description
    assert job.posted_at == datetime(2021, 4, 8, tzinfo=UTC)

    minimal = parse_detail_page(fixture("data-engineer-1.html"), card=second)
    assert minimal.company is None
    assert minimal.location is None
    assert minimal.posted_at is None


def test_detail_requires_description() -> None:
    card = parse_list_page(fixture("list.html"))[0]
    with pytest.raises(ValueError, match="lacks title or description"):
        parse_detail_page(
            '<div class="box"><h1 class="title">Title</h1></div>', card=card
        )


def test_disallowed_domain_and_scheme_rejected_before_request(tmp_path: Path) -> None:
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, text="ok")

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with FakeJobsCrawler(client=client, raw_html_dir=tmp_path) as crawler:
            for url in (
                "https://realpython.github.io.evil.test/fake-jobs/",
                "http://realpython.github.io/fake-jobs/",
                "https://www.realpython.com/",
            ):
                with pytest.raises(ValueError, match="allowlist"):
                    crawler.fetch_page(url)
    assert calls == 0


def test_timeout_and_retry_limit(tmp_path: Path) -> None:
    calls = 0

    def timeout(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("timed out", request=request)

    with httpx.Client(transport=httpx.MockTransport(timeout)) as client:
        crawler = FakeJobsCrawler(
            client=client,
            raw_html_dir=tmp_path,
            max_attempts=3,
            min_request_interval_seconds=0,
            backoff_seconds=0,
        )
        with pytest.raises(httpx.ReadTimeout):
            crawler.fetch_page(LIST_URL)
    assert calls == 3


def test_successful_retry_and_5xx_limit(tmp_path: Path) -> None:
    statuses = iter([503, 200])
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(next(statuses), text="<html>ok</html>")

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        crawler = FakeJobsCrawler(
            client=client,
            raw_html_dir=tmp_path,
            max_attempts=3,
            min_request_interval_seconds=0,
            backoff_seconds=0,
        )
        assert crawler.fetch_page(LIST_URL).http_status == 200
    assert calls == 2

    failed_calls = 0

    def unavailable(request: httpx.Request) -> httpx.Response:
        nonlocal failed_calls
        failed_calls += 1
        return httpx.Response(500)

    with httpx.Client(transport=httpx.MockTransport(unavailable)) as client:
        crawler = FakeJobsCrawler(
            client=client,
            raw_html_dir=tmp_path,
            max_attempts=2,
            min_request_interval_seconds=0,
            backoff_seconds=0,
        )
        with pytest.raises(httpx.HTTPStatusError):
            crawler.fetch_page(LIST_URL)
    assert failed_calls == 2


def test_permanent_4xx_not_retried(tmp_path: Path) -> None:
    calls = 0

    def missing(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(404)

    with httpx.Client(transport=httpx.MockTransport(missing)) as client:
        crawler = FakeJobsCrawler(client=client, raw_html_dir=tmp_path)
        with pytest.raises(httpx.HTTPStatusError):
            crawler.fetch_page(LIST_URL)
    assert calls == 1


def test_rate_limit_and_backoff_are_bounded(tmp_path: Path) -> None:
    now = 0.0
    sleeps: list[float] = []
    request_times: list[float] = []
    calls = 0

    def clock() -> float:
        return now

    def sleep(seconds: float) -> None:
        nonlocal now
        sleeps.append(seconds)
        now += seconds

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        request_times.append(now)
        return httpx.Response(503 if calls < 3 else 200, text="ok")

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        crawler = FakeJobsCrawler(
            client=client,
            raw_html_dir=tmp_path,
            clock=clock,
            sleep=sleep,
            backoff_seconds=0.25,
        )
        crawler.fetch_page(LIST_URL)
    assert request_times == [0.0, 1.0, 2.0]
    assert sleeps == [0.25, 0.75, 0.5, 0.5]


def test_raw_html_hash_and_snapshot_are_stable(tmp_path: Path) -> None:
    html = "<html>unchanged</html>"
    page = RawHtmlPage(
        requested_url=LIST_URL,
        fetched_at=datetime(2026, 9, 30, 12, tzinfo=UTC),
        http_status=200,
        content_hash=hashlib.sha256(html.encode()).hexdigest(),
        html=html,
    )
    path = page.persist(tmp_path)
    metadata_path = path.with_suffix(".json")
    before = metadata_path.read_text()
    later = RawHtmlPage(
        requested_url=LIST_URL,
        fetched_at=datetime(2026, 9, 30, 13, tzinfo=UTC),
        http_status=200,
        content_hash=page.content_hash,
        html=html,
    )
    assert later.persist(tmp_path) == path
    assert path.read_text() == html
    assert metadata_path.read_text() == before
    assert json.loads(before)["content_hash"] == page.content_hash


def test_crawler_rerun_yields_same_hashes_and_no_duplicate_links(
    tmp_path: Path,
) -> None:
    duplicate = (
        '<div class="card"><div class="card-content">'
        '<h2 class="title">Senior Python Developer</h2></div>'
        '<footer><a href="/fake-jobs/jobs/senior-python-developer-0.html">'
        "Apply</a></footer></div>"
    )
    list_html = fixture("list.html").replace(
        "</div></body>", duplicate + "</div></body>"
    )
    pages = {
        LIST_URL: list_html,
        f"{LIST_URL}jobs/senior-python-developer-0.html": fixture(
            "senior-python-developer-0.html"
        ),
        f"{LIST_URL}jobs/data-engineer-1.html": fixture("data-engineer-1.html"),
    }
    requested: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        assert request.headers["user-agent"].startswith("job-data-pipeline/")
        assert request.extensions["timeout"]["connect"] == 10.0
        return httpx.Response(200, text=pages[str(request.url)])

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        crawler = FakeJobsCrawler(
            client=client,
            raw_html_dir=tmp_path,
            parsed_records_dir=tmp_path / "parsed",
            min_request_interval_seconds=0,
        )
        first = list(crawler.iter_jobs(collected_at=datetime(2026, 9, 30, tzinfo=UTC)))
        second = list(crawler.iter_jobs(collected_at=datetime(2026, 10, 1, tzinfo=UTC)))
    assert len(first) == len(second) == 2
    assert [job.source_record_hash for job in first] == [
        job.source_record_hash for job in second
    ]
    assert first[0].collected_at != second[0].collected_at
    assert len(requested) == 6
    assert len(list(tmp_path.rglob("*.html"))) == 3
    parsed = list((tmp_path / "parsed").glob("*.json"))
    assert len(parsed) == 2
    assert {json.loads(path.read_text())["source_record_hash"] for path in parsed} == {
        job.source_record_hash for job in first
    }
