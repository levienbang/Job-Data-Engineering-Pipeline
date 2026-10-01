"""Acquire the Real Python Fake Jobs practice pages without rewriting their data."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from datetime import time as datetime_time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, Tag

from job_pipeline.contracts import CanonicalJob

LIST_URL = "https://realpython.github.io/fake-jobs/"
ALLOWED_HOST = "realpython.github.io"
USER_AGENT = "job-data-pipeline/0.1 (educational Fake Jobs crawler)"
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


def _text(tag: Tag | None) -> str | None:
    if tag is None:
        return None
    value = " ".join(" ".join(tag.stripped_strings).split())
    return value or None


def _posted_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.combine(date.fromisoformat(value), datetime_time(), UTC)
    except ValueError:
        return None


def _allowed_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != ALLOWED_HOST or parsed.port:
        raise ValueError(f"Fake Jobs URL is outside the allowlist: {url}")


@dataclass(frozen=True, slots=True)
class RawHtmlPage:
    requested_url: str
    fetched_at: datetime
    http_status: int
    content_hash: str
    html: str

    def persist(self, root: Path) -> Path:
        """Write one immutable HTML snapshot and its request metadata."""
        page_key = hashlib.sha256(
            f"{self.requested_url}\0{self.content_hash}".encode()
        ).hexdigest()
        directory = root / self.fetched_at.date().isoformat()
        directory.mkdir(parents=True, exist_ok=True)
        html_path = directory / f"{page_key}.html"
        metadata_path = directory / f"{page_key}.json"
        if not html_path.exists():
            html_path.write_text(self.html, encoding="utf-8")
        if not metadata_path.exists():
            metadata_path.write_text(
                json.dumps(
                    {
                        "requested_url": self.requested_url,
                        "fetched_at": self.fetched_at.isoformat(),
                        "http_status": self.http_status,
                        "content_hash": self.content_hash,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
        return html_path


@dataclass(frozen=True, slots=True)
class FakeJobCard:
    title: str
    company: str | None
    location: str | None
    detail_url: str
    posted_at: datetime | None


@dataclass(frozen=True, slots=True)
class FakeJobRecord:
    title: str
    company: str | None
    location: str | None
    description: str
    source_url: str
    posted_at: datetime | None


def parse_list_page(html: str, *, page_url: str = LIST_URL) -> list[FakeJobCard]:
    """Read cards and their same-site Apply links; skip incomplete cards."""
    cards: list[FakeJobCard] = []
    soup = BeautifulSoup(html, "html.parser")
    for card in soup.select("div.card"):
        content = card.select_one("div.card-content")
        if content is None:
            continue
        title = _text(content.select_one("h2.title"))
        apply_link = next(
            (
                link
                for link in card.select("a[href]")
                if _text(link) and _text(link).casefold() == "apply"
            ),
            None,
        )
        if not title or apply_link is None:
            continue
        href = apply_link.get("href")
        if not isinstance(href, str):
            continue
        detail_url = urljoin(page_url, href)
        _allowed_url(detail_url)
        date_tag = content.select_one("time[datetime]")
        date_value = date_tag.get("datetime") if date_tag else None
        cards.append(
            FakeJobCard(
                title=title,
                company=_text(content.select_one("h3.subtitle")),
                location=_text(content.select_one("p.location")),
                detail_url=detail_url,
                posted_at=_posted_at(
                    date_value if isinstance(date_value, str) else None
                ),
            )
        )
    return cards


def parse_detail_page(html: str, *, card: FakeJobCard) -> FakeJobRecord:
    """Prefer detail-page values, retaining list-card fields when detail omits them."""
    soup = BeautifulSoup(html, "html.parser")
    box = soup.select_one("div.box")
    if box is None:
        raise ValueError(f"Fake Jobs detail has no job box: {card.detail_url}")
    title = _text(box.select_one("h1.title")) or card.title
    company = _text(box.select_one("h2.subtitle")) or card.company
    paragraphs = [_text(tag) for tag in box.select("p")]
    description_parts: list[str] = []
    location = card.location
    posted_at = card.posted_at
    for paragraph in paragraphs:
        if not paragraph:
            continue
        if paragraph.startswith("Location:"):
            location = paragraph.removeprefix("Location:").strip() or location
        elif paragraph.startswith("Posted:"):
            posted_at = (
                _posted_at(paragraph.removeprefix("Posted:").strip()) or posted_at
            )
        else:
            description_parts.append(paragraph)
    description = "\n\n".join(description_parts)
    if not title or not description:
        raise ValueError(
            f"Fake Jobs detail lacks title or description: {card.detail_url}"
        )
    return FakeJobRecord(
        title=title,
        company=company,
        location=location,
        description=description,
        source_url=card.detail_url,
        posted_at=posted_at,
    )


def map_fake_jobs_row(
    raw: Mapping[str, object], *, collected_at: datetime
) -> CanonicalJob:
    """Hash source content independently of crawl time for idempotent reruns."""
    posted_at = raw.get("posted_at")
    if isinstance(posted_at, str):
        posted_at = datetime.fromisoformat(posted_at)
    content = json.dumps(
        {
            "source_url": raw["source_url"],
            "title": raw["title"],
            "company": raw.get("company"),
            "location": raw.get("location"),
            "description": raw["description"],
            "posted_at": posted_at.isoformat() if posted_at else None,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return CanonicalJob(
        source_name="fake_jobs",
        source_job_id=None,
        source_url=raw["source_url"],
        raw_title=raw["title"],
        raw_company=raw.get("company"),
        raw_location=raw.get("location"),
        raw_employment_type=None,
        raw_description=raw["description"],
        posted_at=posted_at,
        collected_at=collected_at,
        source_record_hash=hashlib.sha256(content.encode("utf-8")).hexdigest(),
    )


class FakeJobsCrawler:
    """Bounded, rate-limited crawler for the one allowlisted practice site."""

    source_name = "fake_jobs"

    def __init__(
        self,
        *,
        list_url: str = LIST_URL,
        raw_html_dir: Path = Path("data/raw_html/fake_jobs"),
        client: httpx.Client | None = None,
        timeout_seconds: float = 10.0,
        min_request_interval_seconds: float = 1.0,
        max_attempts: int = 3,
        backoff_seconds: float = 0.5,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        _allowed_url(list_url)
        if timeout_seconds <= 0 or min_request_interval_seconds < 0:
            raise ValueError(
                "Timeout must be positive and request interval nonnegative"
            )
        if max_attempts < 1 or backoff_seconds < 0:
            raise ValueError("Retry configuration is invalid")
        self.list_url = list_url
        self.raw_html_dir = raw_html_dir
        self.timeout_seconds = timeout_seconds
        self.min_request_interval_seconds = min_request_interval_seconds
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self._clock = clock
        self._sleep = sleep
        self._last_request_at: float | None = None
        self._client = client or httpx.Client(follow_redirects=False)
        self._owns_client = client is None

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> FakeJobsCrawler:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _wait_for_slot(self) -> None:
        now = self._clock()
        if self._last_request_at is not None:
            remaining = self.min_request_interval_seconds - (
                now - self._last_request_at
            )
            if remaining > 0:
                self._sleep(remaining)
        self._last_request_at = self._clock()

    def fetch_page(self, url: str) -> RawHtmlPage:
        _allowed_url(url)
        for attempt in range(self.max_attempts):
            self._wait_for_slot()
            try:
                response = self._client.get(
                    url,
                    headers={"User-Agent": USER_AGENT},
                    timeout=self.timeout_seconds,
                    follow_redirects=False,
                )
            except httpx.TransportError:
                if attempt == self.max_attempts - 1:
                    raise
            else:
                html = response.text
                page = RawHtmlPage(
                    requested_url=url,
                    fetched_at=datetime.now(UTC),
                    http_status=response.status_code,
                    content_hash=hashlib.sha256(html.encode("utf-8")).hexdigest(),
                    html=html,
                )
                page.persist(self.raw_html_dir)
                if response.status_code not in RETRYABLE_STATUS:
                    response.raise_for_status()
                    return page
                if attempt == self.max_attempts - 1:
                    response.raise_for_status()
            self._sleep(self.backoff_seconds * 2**attempt)
        raise AssertionError("retry loop did not terminate")

    def iter_records(self) -> Iterator[FakeJobRecord]:
        """Yield parsed source records while preserving raw HTML snapshots."""
        list_page = self.fetch_page(self.list_url)
        seen_urls: set[str] = set()
        for card in parse_list_page(list_page.html, page_url=self.list_url):
            if card.detail_url in seen_urls:
                continue
            seen_urls.add(card.detail_url)
            detail_page = self.fetch_page(card.detail_url)
            yield parse_detail_page(detail_page.html, card=card)

    def iter_raw(self) -> Iterator[dict[str, object]]:
        """Yield source fields for Bronze, retaining HTML acquisition audits."""
        for record in self.iter_records():
            raw = asdict(record)
            raw["posted_at"] = (
                record.posted_at.isoformat() if record.posted_at else None
            )
            yield raw
