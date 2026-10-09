"""Arbeitnow fixture coverage: no live HTTP or paid services."""

import copy
import hashlib
import json
import logging
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path

import httpx
import polars as pl
import pytest

from job_pipeline import __main__, workflow
from job_pipeline.config import arbeitnow_max_pages
from job_pipeline.pipeline import SchemaDriftError, arbeitnow_watermark, run_pipeline
from job_pipeline.sources.arbeitnow import (
    API_URL,
    USER_AGENT,
    ArbeitnowSource,
    description_text,
    is_english,
    map_arbeitnow_row,
)
from job_pipeline.sources.base import RawSource
from job_pipeline.sources.technology import is_technology_title

FIXTURES = Path(__file__).parent / "fixtures" / "arbeitnow"
NOW = datetime(2026, 10, 9, 12, tzinfo=UTC)


def pages():
    return [json.loads((FIXTURES / f"page-{i}.json").read_text()) for i in (1, 2)]


def mock_source(bodies=None, **kwargs):
    bodies = pages() if bodies is None else bodies
    requests, sleeps = [], []

    def respond(request):
        requests.append(request)
        return httpx.Response(200, json=bodies[int(request.url.params["page"]) - 1])

    client = httpx.Client(transport=httpx.MockTransport(respond))
    return (
        ArbeitnowSource(
            client, sleep=sleeps.append, **{"max_pages": 5, "watermark": None, **kwargs}
        ),
        requests,
        sleeps,
    )


@pytest.mark.parametrize("limit,count,reason", [(1, 8, "max_pages"), (5, 15, "end")])
def test_pagination_raw_and_request_policy(limit, count, reason):
    source, requests, sleeps = mock_source(max_pages=limit)
    assert isinstance(source, RawSource)
    records = list(source.iter_raw())
    assert records == [r for page in pages() for r in page["data"]][:count]
    assert source.pages_fetched == len(requests) == (1 if limit == 1 else 2)
    assert source.stop_reason == reason
    assert sleeps == ([] if limit == 1 else [1.0])
    assert all(r.headers["User-Agent"] == USER_AGENT for r in requests)
    assert all(r.extensions["timeout"]["read"] == 30 for r in requests)
    assert not source.client.is_closed  # Caller owns injected clients.


@pytest.mark.parametrize(
    "first,delay", [(503, 1.0), (429, 3.0), ("timeout", 1.0), ("network", 1.0)]
)
def test_retries(first, delay):
    calls, sleeps = [], []

    def respond(request):
        calls.append(request)
        if len(calls) == 1:
            if first == "timeout":
                raise httpx.ReadTimeout("timeout", request=request)
            if first == "network":
                raise httpx.ConnectError("network", request=request)
            return httpx.Response(
                first, headers={"Retry-After": "3"} if first == 429 else {}
            )
        return httpx.Response(200, json=pages()[1])

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        source = ArbeitnowSource(
            client, max_pages=1, watermark=None, sleep=sleeps.append
        )
        assert len(list(source.iter_raw())) == 7
    assert len(calls) == 2
    assert sleeps == [delay]


def test_retry_after_http_date():
    calls, sleeps = [], []

    def respond(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(
                429,
                headers={
                    "Retry-After": format_datetime(
                        datetime.now(UTC) + timedelta(seconds=30), usegmt=True
                    )
                },
            )
        return httpx.Response(200, json=pages()[1])

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        list(
            ArbeitnowSource(
                client, max_pages=1, watermark=None, sleep=sleeps.append
            ).iter_raw()
        )
    assert 25 <= sleeps[0] <= 30


@pytest.mark.parametrize(
    "status,attempts", [(404, 1), (403, 1), (503, 3), (429, 3), (302, 1)]
)
def test_http_failure_bounded_and_no_redirect(status, attempts):
    calls, sleeps = [], []

    def respond(request):
        calls.append(request)
        return httpx.Response(status, headers={"Location": "https://evil.test/"})

    with httpx.Client(
        transport=httpx.MockTransport(respond), follow_redirects=True
    ) as client:
        with pytest.raises(httpx.HTTPStatusError):
            list(
                ArbeitnowSource(
                    client, max_pages=5, watermark=None, sleep=sleeps.append
                ).iter_raw()
            )
    assert len(calls) == attempts
    assert len(sleeps) == attempts - 1
    assert all(delay >= 1 for delay in sleeps)


def test_exhausted_network_failure():
    calls = []

    def respond(request):
        calls.append(request)
        raise httpx.ConnectError("offline", request=request)

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(httpx.ConnectError):
            list(
                ArbeitnowSource(
                    client, max_pages=1, watermark=None, sleep=lambda _: None
                ).iter_raw()
            )
    assert len(calls) == 3


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.test/jobs",
        "http://www.arbeitnow.com/jobs",
        "https://www.arbeitnow.com:444/jobs",
        "https://user@www.arbeitnow.com/jobs",
    ],
)
def test_next_url_allowlist_even_at_page_limit(url):
    bodies = pages()
    bodies[0]["links"]["next"] = url
    source, requests, _ = mock_source(bodies, max_pages=1)
    with pytest.raises(ValueError, match="allowlisted"):
        list(source.iter_raw())
    assert len(requests) == 1


@pytest.mark.parametrize(
    "body",
    [
        {},
        [],
        {"data": {}},
        {"data": []},
        {"data": [], "links": {}},
        {"data": [1], "links": {"next": None}},
        {"data": [], "links": {"next": 3}},
    ],
)
def test_response_schema(body):
    source, _, _ = mock_source([body])
    with pytest.raises(SchemaDriftError):
        list(source.iter_raw())


def test_invalid_json():
    with httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, text="oops"))
    ) as client:
        with pytest.raises(SchemaDriftError, match="not JSON"):
            list(ArbeitnowSource(client, max_pages=1, watermark=None).iter_raw())


@pytest.mark.parametrize("exception", [None, "new", "equal", "missing", "invalid"])
def test_watermark_requires_every_record_older(exception):
    bodies = pages()
    cutoff = 1_791_600_000
    for row in bodies[0]["data"]:
        row["created_at"] = cutoff - 1
    # A pinned old first row cannot hide a newer row later on the page.
    if exception == "new":
        bodies[0]["data"][-1]["created_at"] = cutoff + 1
    elif exception == "equal":
        bodies[0]["data"][-1]["created_at"] = cutoff
    elif exception == "missing":
        bodies[0]["data"][-1].pop("created_at")
    elif exception == "invalid":
        bodies[0]["data"][-1]["created_at"] = "invalid"
    source, requests, _ = mock_source(
        bodies, watermark=datetime.fromtimestamp(cutoff, UTC)
    )
    records = list(source.iter_raw())
    assert len(records) == (8 if exception is None else 15)
    assert len(requests) == (1 if exception is None else 2)
    assert source.stop_reason == "watermark"


def test_gold_watermark(tmp_path):
    assert arbeitnow_watermark(tmp_path) is None
    path = tmp_path / "gold/source=arbeitnow/jobs.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame({"posted_at": [NOW, NOW - timedelta(days=1), None]}).write_parquet(
        path
    )
    assert arbeitnow_watermark(tmp_path) == NOW - timedelta(minutes=60)
    pl.DataFrame(schema={"posted_at": pl.Datetime("us", "UTC")}).write_parquet(path)
    assert arbeitnow_watermark(tmp_path) is None


def test_mapper_fields_hash_and_no_inventions():
    raw = copy.deepcopy(pages()[1]["data"][0])
    raw.update(
        description="&amp;lt;p&amp;gt;Hello &amp;lt;b&amp;gt;our team"
        "&amp;lt;/b&amp;gt;&amp;lt;/p&amp;gt;&amp;lt;ul&amp;gt;"
        "&amp;lt;li&amp;gt;Build systems&amp;lt;/li&amp;gt;&amp;lt;/ul&amp;gt;",
        remote=True,
        tags=["IT", "Data"],
        job_types=["Full Time", "Permanent"],
        new_field="preserved",
    )
    before = copy.deepcopy(raw)
    job = map_arbeitnow_row(raw, collected_at=NOW)
    assert raw == before
    assert job.source_name == "arbeitnow"
    assert job.source_job_id == raw["slug"]
    assert job.source_url == raw["url"]
    assert job.raw_title == raw["title"]
    assert job.raw_company == raw["company_name"]
    assert job.raw_location == f"{raw['location']}, Remote"
    assert job.raw_employment_type == "Full Time, Permanent"
    assert job.raw_category == "IT, Data"
    assert job.raw_description == "Hello our team\nBuild systems"
    assert job.posted_at == datetime.fromtimestamp(raw["created_at"], UTC)
    expected_hash = hashlib.sha256(
        json.dumps(
            {"source_name": "arbeitnow", "record": raw},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    assert job.source_record_hash == expected_hash
    assert (
        map_arbeitnow_row(
            dict(reversed(list(raw.items()))), collected_at=NOW + timedelta(days=1)
        ).source_record_hash
        == expected_hash
    )
    for key, value in asdict(job).items():
        if key.startswith("raw_") and key not in {
            "raw_title",
            "raw_company",
            "raw_location",
            "raw_employment_type",
            "raw_category",
            "raw_description",
        }:
            assert value is None
    assert (
        map_arbeitnow_row(
            {**raw, "title": "Changed"}, collected_at=NOW
        ).source_record_hash
        != expected_hash
    )


@pytest.mark.parametrize(
    "remote,location,expected",
    [
        (True, "", "Remote"),
        (True, None, "Remote"),
        (True, "Berlin", "Berlin, Remote"),
        (True, "Remote", "Remote"),
        (True, "Berlin (remote)", "Berlin (remote)"),
        (False, "Berlin", "Berlin"),
        (False, "", None),
    ],
)
def test_optional_fields(remote, location, expected):
    raw = {
        **pages()[0]["data"][0],
        "remote": remote,
        "location": location,
        "company_name": "",
        "tags": [],
        "job_types": [],
        "created_at": None,
    }
    job = map_arbeitnow_row(raw, collected_at=NOW)
    assert job.raw_location == expected
    assert (
        job.raw_company
        is job.raw_category
        is job.raw_employment_type
        is job.posted_at
        is None
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("title", " "),
        ("description", "<p> </p>"),
        ("created_at", "bad"),
        ("created_at", 10**30),
    ],
)
def test_mapper_invalid_values(field, value):
    raw = {**pages()[0]["data"][0], field: value}
    with pytest.raises(ValueError):
        map_arbeitnow_row(raw, collected_at=NOW)


def test_language_all_fixture_rows_and_threshold():
    expected = [
        False,
        False,
        True,
        True,
        False,
        False,
        True,
        True,
        True,
        True,
        False,
        True,
        False,
        True,
        True,
    ]
    rows = [row for page in pages() for row in page["data"]]
    assert [
        is_english(description_text(row["description"])) for row in rows
    ] == expected
    assert not is_english("123 _ !")
    assert is_english("the " + "example " * 9)
    assert not is_english("the " + "example " * 10)
    assert (
        description_text("<h2>Title</h2><p>we <b>build</b> systems<br>for you</p>")
        == "Title\nwe build systems\nfor you"
    )


@pytest.mark.parametrize(
    "title",
    [
        "Data Scientist",
        "ML Research Engineer",
        "AI Research Engineer",
        "Multimodal ML Engineer",
        "Frontend Engineer",
        "Backend Developer",
        "Full-stack Engineer",
        "Software Engineer",
        "DevOps Engineer",
        "SRE",
        "Platform Engineer",
        "Security Engineer",
        "QA Engineer",
        "Computer Vision Engineer - Tracker",
        "AI Solutions Engineer",
        "Data Analyst (m/f/d)",
        "Forward Deployed Engineer",
        "Solutions Architect",
        "Engineering Manager (all genders)",
    ],
)
def test_it_keeps(title):
    assert is_technology_title(title)


@pytest.mark.parametrize(
    "title",
    [
        "Werkstudent Marketing",
        "Projektmanager",
        "Sales Engineer",
        "Software Sales Engineer",
        "Account Executive - Data Engineer",
        "Frontend Marketing",
        "Data Entry",
        "Manufacturing Methods Engineer",
        "Optical Engineer - Camera",
        "Electrical Design Engineer & Machine Safety Specialist (M/F)",
        "Screening and Escalations Senior Payment Analyst",
        "German-Speaking Architectural Draftsperson / Project Architect",
    ],
)
def test_it_rejects(title):
    assert not is_technology_title(title)


def test_end_to_end_rerun_and_raw_preservation(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    source, _, _ = mock_source()
    first = run_pipeline(
        "arbeitnow", source.iter_raw(), root=tmp_path, collected_at=NOW
    )
    assert (
        first.input_count,
        first.valid_count,
        first.invalid_count,
        first.filtered_count,
    ) == (15, 8, 0, 7)
    bronze = pl.read_parquet(first.bronze_path)
    assert [json.loads(r) for r in bronze["raw_record_json"]] == [
        r for p in pages() for r in p["data"]
    ]
    assert "reason=language count=6" in caplog.text
    assert "reason=not_it count=1" in caplog.text
    gold = pl.read_parquet(first.gold_path)
    assert set(gold["raw_title"]) == {
        *[pages()[0]["data"][i]["title"] for i in (3, 6, 7)],
        *[pages()[1]["data"][i]["title"] for i in (0, 1, 3, 5, 6)],
    }
    original = first.bronze_path.read_bytes()
    source, _, _ = mock_source(watermark=arbeitnow_watermark(tmp_path))
    second = run_pipeline(
        "arbeitnow",
        source.iter_raw(),
        root=tmp_path,
        collected_at=NOW + timedelta(hours=1),
    )
    assert second.duplicate_count == 8
    assert pl.read_parquet(second.gold_path).equals(gold)
    assert first.bronze_path.read_bytes() == original


def test_schema_extra_fields_and_quarantine_order(tmp_path):
    row = pages()[1]["data"][0]
    extra = {**row, "new_field": {"retained": True}}
    missing = {k: v for k, v in row.items() if k != "slug"}
    bad_mapping = {**row, "title": ""}
    bad_validation = {**row, "url": "invalid"}
    result = run_pipeline(
        "arbeitnow",
        [extra, extra, missing, bad_mapping, bad_validation],
        root=tmp_path,
        collected_at=NOW,
    )
    assert (result.valid_count, result.invalid_count, result.duplicate_count) == (
        2,
        3,
        1,
    )
    assert pl.read_parquet(result.quarantine_path)["error_code"].to_list() == [
        "SCHEMA_DRIFT",
        "MAPPING_ERROR",
        "VALIDATION_ERROR",
    ]
    assert (
        json.loads(pl.read_parquet(result.bronze_path)["raw_record_json"][0]) == extra
    )


@pytest.mark.parametrize("value", ["0", "-1", "1.2", "abc"])
def test_config_invalid(monkeypatch, value):
    monkeypatch.setenv("ARBEITNOW_MAX_PAGES", value)
    with pytest.raises(ValueError, match="ARBEITNOW_MAX_PAGES"):
        arbeitnow_max_pages()


def test_config_defaults(monkeypatch):
    monkeypatch.delenv("ARBEITNOW_MAX_PAGES", raising=False)
    assert arbeitnow_max_pages() == 5
    monkeypatch.setenv("ARBEITNOW_MAX_PAGES", "1")
    assert arbeitnow_max_pages() == 1


def test_cli_rerun_uses_watermark_and_never_delivers(monkeypatch, tmp_path, capsys):
    seen = []

    def factory(**kwargs):
        seen.append(kwargs)
        return mock_source(**kwargs)[0]

    monkeypatch.setattr(__main__, "ArbeitnowSource", factory)
    monkeypatch.setenv("ARBEITNOW_MAX_PAGES", "1")

    def unexpected(*args, **kwargs):
        pytest.fail("run must not access export, delivery, or database")

    for name in ("export_batches", "deliver_gold", "make_session_factory"):
        monkeypatch.setattr(__main__, name, unexpected)
    for _ in range(2):
        assert (
            __main__.main(["run", "--source", "arbeitnow", "--root", str(tmp_path)])
            == 0
        )
        assert json.loads(capsys.readouterr().out)["count"] == 3
    assert seen[0] == {"max_pages": 1, "watermark": None}
    assert seen[1] == {"max_pages": 1, "watermark": arbeitnow_watermark(tmp_path)}


def test_cli_http_failure(monkeypatch, tmp_path, capsys):
    class FailingSource:
        def __init__(self, **kwargs):
            pass

        def iter_raw(self):
            raise httpx.ConnectError("offline")

    monkeypatch.setattr(__main__, "ArbeitnowSource", FailingSource)
    assert __main__.main(["run", "--source", "arbeitnow", "--root", str(tmp_path)]) == 1
    assert "offline" in capsys.readouterr().err


def test_workflow_acquisition_snapshot_and_watermark(monkeypatch, tmp_path):
    seen, assets = [], []

    def factory(**kwargs):
        seen.append(kwargs)
        return mock_source(**kwargs)[0]

    monkeypatch.setattr(workflow, "ArbeitnowSource", factory)
    monkeypatch.setattr(workflow, "_sessions", lambda: None)
    monkeypatch.setattr(workflow, "start_run", lambda *a, **kw: None)
    monkeypatch.setattr(
        workflow, "record_source_asset", lambda *a, **kw: assets.append(kw)
    )
    monkeypatch.setenv("PIPELINE_SOURCE", "arbeitnow")
    monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("ARBEITNOW_MAX_PAGES", "5")
    context = {"run_id": "fixture", "logical_date": NOW}
    state = workflow.acquire_task(**context)
    snapshot = Path(state["path"])
    assert "acquired/source=arbeitnow" in str(snapshot)
    assert len(json.loads(snapshot.read_text())) == 15
    original = snapshot.stat().st_mtime_ns

    class TI:
        def xcom_pull(self, *, task_ids):
            return state

    bronze = workflow.bronze_task(**context, ti=TI())
    assert bronze["input_count"] == 15
    assert assets[0]["source_url_or_path"] == API_URL
    run_pipeline(
        "arbeitnow", json.loads(snapshot.read_text()), root=tmp_path, collected_at=NOW
    )
    assert workflow.acquire_task(**context) == state
    assert snapshot.stat().st_mtime_ns == original
    assert seen == [
        {"max_pages": 5, "watermark": None},
        {"max_pages": 5, "watermark": arbeitnow_watermark(tmp_path)},
    ]


@pytest.mark.parametrize("records", [[], [pages()[0]["data"][0]]])
def test_empty_or_all_filtered_can_rerun(tmp_path, records):
    for _ in range(2):
        result = run_pipeline("arbeitnow", records, collected_at=NOW, root=tmp_path)
        assert result.valid_count == result.invalid_count == result.duplicate_count == 0
        assert result.filtered_count == len(records)
        assert pl.read_parquet(result.gold_path).height == 0
        assert arbeitnow_watermark(tmp_path) is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_pages": 0},
        {"max_pages": True},
        {"max_pages": 1.5},
        {"max_attempts": 0},
        {"backoff_seconds": -1},
        {"backoff_seconds": float("nan")},
        {"watermark": datetime(2026, 10, 9)},
    ],
)
def test_source_invalid_config(kwargs):
    with pytest.raises(ValueError):
        ArbeitnowSource(**{"max_pages": 5, "watermark": None, **kwargs})


def test_pagination_follows_next_verbatim():
    next_url = "https://www.arbeitnow.com/api/job-board-api?page=7&cursor=fixture"
    bodies = pages()
    bodies[0]["links"]["next"] = next_url
    requests = []

    def respond(request):
        requests.append(str(request.url))
        return httpx.Response(200, json=bodies[len(requests) - 1])

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        assert (
            len(
                list(
                    ArbeitnowSource(
                        client, max_pages=5, watermark=None, sleep=lambda _: None
                    ).iter_raw()
                )
            )
            == 15
        )
    assert requests == [API_URL, next_url]
