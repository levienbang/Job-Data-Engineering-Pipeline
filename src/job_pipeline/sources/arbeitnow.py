"""Public Arbeitnow acquisition and pure mapping to the canonical contract."""

import hashlib
import html
import json
import logging
import re
import time
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from math import isfinite
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from job_pipeline.contracts import CanonicalJob

API_URL = "https://www.arbeitnow.com/api/job-board-api?page=1"
USER_AGENT = "job-data-pipeline/0.1 (educational Arbeitnow API client)"
REQUIRED_COLUMNS = frozenset(
    {
        "slug",
        "company_name",
        "title",
        "description",
        "remote",
        "url",
        "tags",
        "job_types",
        "location",
        "created_at",
    }
)
ENGLISH_STOPWORDS = {
    "and",
    "we",
    "the",
    "for",
    "with",
    "you",
    "your",
    "our",
    "team",
    "will",
    "are",
    "is",
    "of",
    "to",
    "in",
}
# German is the source's main non-English language; mixed descriptions often
# open in English and continue in German.
GERMAN_STOPWORDS = {
    "und",
    "wir",
    "die",
    "der",
    "für",
    "mit",
    "sie",
    "ihre",
    "eine",
    "bei",
    "oder",
    "deine",
    "du",
    "auf",
    "ist",
    "das",
    "zu",
    "den",
    "dich",
    "von",
}
FRENCH_STOPWORDS = {
    "et",
    "nous",
    "les",
    "des",
    "pour",
    "avec",
    "vous",
    "votre",
    "une",
    "est",
    "dans",
    "sur",
    "le",
    "la",
    "du",
    "au",
}
LOGGER = logging.getLogger(__name__)
# Display only: stored timestamps and comparisons stay in UTC.
VIETNAM = ZoneInfo("Asia/Ho_Chi_Minh")


def description_text(value: str) -> str:
    """Decode up to three layers; keep block boundaries and inline word spacing."""
    for _ in range(3):
        decoded = html.unescape(value)
        if decoded == value:
            break
        value = decoded
    soup = BeautifulSoup(value, "html.parser")
    for node in soup(["script", "style"]):
        node.decompose()
    for node in soup.find_all(
        ["p", "li", "br", "div", "h1", "h2", "h3", "h4", "h5", "h6"]
    ):
        node.insert_before("\n")
        node.insert_after("\n")
    return "\n".join(
        line
        for part in soup.get_text().splitlines()
        if (line := " ".join(part.split()))
    )


def is_english(text: str) -> bool:
    """Strict: English share >= 0.10 and at least twice German and French."""
    words = re.findall(r"[^\W\d_]+", text.casefold())
    if not words:
        return False

    def share(stopwords: set[str]) -> float:
        return sum(w in stopwords for w in words) / len(words)

    english = share(ENGLISH_STOPWORDS)
    other = max(share(GERMAN_STOPWORDS), share(FRENCH_STOPWORDS))
    return english >= 0.10 and english >= 2 * other


def _posted_at(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("created_at must be Unix seconds or null")
    try:
        return datetime.fromtimestamp(value, UTC)
    except (ValueError, OverflowError, OSError) as exc:
        raise ValueError("invalid created_at timestamp") from exc


def map_arbeitnow_row(
    raw: Mapping[str, object], *, collected_at: datetime
) -> CanonicalJob:
    title = raw.get("title")
    if not isinstance(title, str) or not title.strip():
        raise ValueError("Arbeitnow row has no title")
    description = raw.get("description")
    if not isinstance(description, str) or not (
        description := description_text(description)
    ):
        raise ValueError("Arbeitnow row has no description")
    location = raw.get("location") or None
    if raw.get("remote") is True and "remote" not in str(location or "").casefold():
        location = f"{location}, Remote" if location else "Remote"
    content = json.dumps(
        {"source_name": "arbeitnow", "record": dict(raw)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return CanonicalJob(
        source_name="arbeitnow",
        source_job_id=raw.get("slug"),
        source_url=raw.get("url"),
        raw_title=title,
        raw_company=raw.get("company_name") or None,
        raw_location=location,
        raw_employment_type=", ".join(raw.get("job_types") or []) or None,
        raw_category=", ".join(raw.get("tags") or []) or None,
        raw_description=description,
        posted_at=_posted_at(raw.get("created_at")),
        collected_at=collected_at,
        source_record_hash=hashlib.sha256(content.encode("utf-8")).hexdigest(),
    )


def _allowed_url(url: str) -> None:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.arbeitnow.com"
        or parsed.port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(f"Arbeitnow URL is not allowlisted: {url}")


def _retry_after(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        seconds = float(value)
    except ValueError:
        try:
            seconds = (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds()
        except (ValueError, TypeError, OverflowError):
            return 0.0
    return max(0.0, seconds) if isfinite(seconds) else 0.0


class ArbeitnowSource:
    source_name = "arbeitnow"

    def __init__(
        self,
        client: httpx.Client | None = None,
        *,
        max_pages: int,
        watermark: datetime | None,
        sleep: Callable[[float], None] = time.sleep,
        max_attempts: int = 3,
        backoff_seconds: float = 0.5,
    ) -> None:
        if (
            isinstance(max_pages, bool)
            or not isinstance(max_pages, int)
            or max_pages < 1
        ):
            raise ValueError("max_pages must be an integer >= 1")
        if (
            isinstance(max_attempts, bool)
            or not isinstance(max_attempts, int)
            or max_attempts < 1
            or not isfinite(backoff_seconds)
            or backoff_seconds < 0
        ):
            raise ValueError("invalid retry configuration")
        if watermark is not None and watermark.tzinfo is None:
            raise ValueError("watermark needs a timezone")
        self.client = client
        self.max_pages = max_pages
        self.watermark = watermark
        self.sleep = sleep
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.pages_fetched = 0
        self.stop_reason: str | None = None
        self._requested = False

    def _fetch(self, client: httpx.Client, url: str) -> httpx.Response:
        _allowed_url(url)
        delay = 1.0
        for attempt in range(self.max_attempts):
            if self._requested:
                self.sleep(delay)
            self._requested = True
            try:
                response = client.get(
                    url,
                    headers={"User-Agent": USER_AGENT},
                    timeout=30,
                    follow_redirects=False,
                )
            except httpx.TransportError:
                if attempt + 1 == self.max_attempts:
                    raise
                delay = max(1.0, self.backoff_seconds * 2**attempt)
                continue
            if response.status_code != 429 and response.status_code < 500:
                response.raise_for_status()
                return response
            if attempt + 1 == self.max_attempts:
                response.raise_for_status()
            delay = max(
                1.0,
                self.backoff_seconds * 2**attempt,
                _retry_after(response.headers.get("Retry-After")),
            )
        raise AssertionError("unreachable retry loop")

    def iter_raw(self) -> Iterator[dict]:
        # Local import keeps the pipeline mapper registry free of an import cycle.
        from job_pipeline.pipeline import SchemaDriftError

        self.pages_fetched = 0
        self.stop_reason = None
        client = self.client if self.client is not None else httpx.Client()
        try:
            url = API_URL
            for _ in range(self.max_pages):
                response = self._fetch(client, url)
                try:
                    body = response.json()
                except ValueError as exc:
                    raise SchemaDriftError("Arbeitnow response is not JSON") from exc
                if (
                    not isinstance(body, dict)
                    or not isinstance(body.get("data"), list)
                    or not isinstance(body.get("links"), dict)
                    or "next" not in body["links"]
                ):
                    raise SchemaDriftError(
                        "Arbeitnow response requires data list and links.next"
                    )
                records, next_url = body["data"], body["links"]["next"]
                if not all(isinstance(row, dict) for row in records):
                    raise SchemaDriftError("Arbeitnow data entries must be objects")
                if next_url is not None:
                    if not isinstance(next_url, str):
                        raise SchemaDriftError(
                            "Arbeitnow links.next must be a URL or null"
                        )
                    _allowed_url(next_url)
                self.pages_fetched += 1
                yield from records
                # Invalid/missing dates must reach quarantine, never stop pagination.
                all_old = self.watermark is not None
                for row in records:
                    try:
                        posted = _posted_at(row.get("created_at"))
                        if (
                            posted is None
                            or self.watermark is None
                            or posted >= self.watermark
                        ):
                            all_old = False
                    except ValueError:
                        all_old = False
                if all_old:
                    self.stop_reason = "watermark"
                elif next_url is None:
                    self.stop_reason = "end"
                elif self.pages_fetched == self.max_pages:
                    self.stop_reason = "max_pages"
                if self.stop_reason:
                    break
                url = next_url
            LOGGER.info(
                "Arbeitnow acquisition pages=%s stop_reason=%s watermark=%s "
                "(Vietnam time %s)",
                self.pages_fetched,
                self.stop_reason,
                self.watermark,
                self.watermark.astimezone(VIETNAM) if self.watermark else None,
            )
        finally:
            if self.client is None:
                client.close()
