"""Deterministic VietJobs technology scope selection before later pipeline stages."""

import re
from collections.abc import Mapping

TECHNOLOGY_CATEGORY = "công_nghệ_thông_tin_kỹ_thuật_số"

# Titles are a narrow fallback for relevant jobs assigned to another category.
# Generic words such as "data", "cloud", "software" and "QA" are insufficient.
TITLE_PATTERNS = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        (
            r"\b(?:software (?:engineer|developer|architect|tester)|kỹ sư phần mềm|"
            r"phát triển phần mềm|lập trình viên|lập trình "
            r"(?:web|mobile|ứng dụng|nhúng|unity|java|python|c#|\.net))\b"
        ),
        (
            r"\b(?:back[ -]?end|front[ -]?end|full[ -]?stack|"
            r"mobile (?:developer|engineer)|"
            r"(?:android|ios|flutter|react native) (?:developer|engineer))\b"
        ),
        (
            r"\b(?:data (?:engineer|scientist|architect)|kỹ sư dữ liệu|"
            r"khoa học dữ liệu|machine learning|ml engineer|"
            r"ai (?:engineer|developer|researcher)|trí tuệ nhân tạo)\b"
        ),
        (
            r"\b(?:devops|site reliability engineer|sre|"
            r"cloud (?:engineer|architect|developer)|"
            r"kỹ sư (?:cloud|điện toán đám mây))\b"
        ),
        (
            r"\b(?:software qa|qa (?:tester|automation|software)|"
            r"(?:manual|automation|software) tester|kiểm thử phần mềm|"
            r"cyber[ -]?security|an ninh mạng|bảo mật thông tin)\b"
        ),
        r"\b(?:database (?:engineer|administrator|developer)|dba|"
        r"quản trị cơ sở dữ liệu)\b",
    )
)

FALLBACK_EXCLUSIONS = re.compile(
    r"\b(?:marketing|mkt|tuyển sinh|giảng viên|presales|tiền bán hàng)\b",
    re.IGNORECASE,
)


def is_technology_job(record: Mapping[str, str | None]) -> bool:
    """Select published IT-category rows or narrowly named technology roles."""
    category = (record.get("category") or "").strip().casefold()
    if category == TECHNOLOGY_CATEGORY:
        return True
    title = (record.get("job_title") or "").strip()
    if FALLBACK_EXCLUSIONS.search(title):
        return False
    return any(pattern.search(title) for pattern in TITLE_PATTERNS)


# English job boards have no IT category column. Selection is precision-first:
# a job must pass a strict title check AND name concrete technologies in its
# description. Missing some IT roles is acceptable; letting non-IT through is not.
ENGLISH_STANDALONE_ROLE = re.compile(
    r"\b(?:developers?|programmers?|devops|sre|site reliability|"
    r"data scientists?|machine learning|software engineer(?:s|ing)?)\b",
    re.IGNORECASE,
)
ENGLISH_ROLE = re.compile(
    r"\b(?:engineers?|engineering|architects?|scientists?|analysts?|"
    r"administrators?|leads?|testers?)\b",
    re.IGNORECASE,
)
ENGLISH_IT_QUALIFIER = re.compile(
    r"(?:\b(?:software|data|ml|ai|cloud|back[ -]?end|front[ -]?end|"
    r"full[ -]?stack|mobile|ios|android|web|platform|infrastructure|security|"
    r"cyber\w*|qa|test|database|network|embedded|computer vision|nlp|llm|"
    r"api|python|java|golang|react|typescript|kubernetes|analytics|bi|"
    r"tech|technical|it|information security)\b|\.net\b|c\+\+)",
    re.IGNORECASE,
)
ENGLISH_EXCLUSIONS = re.compile(
    r"\b(?:sales|pre[ -]?sales|marketing|account (?:executive|manager)|"
    r"manufacturing|industriali[sz]ation|electrical|mechanical|optical|civil|"
    r"chemical|hardware|architectural|draftsperson|payment|people|recruit\w*|"
    r"talent|fire|propellant|composite|procurement|value engineer|"
    r"quality control|maintenance|project engineer|sustainability|"
    r"customer success|support specialist|product manager|product owner|"
    r"consultant|designer)\b",
    re.IGNORECASE,
)
# Description must name concrete technologies; generic words such as "data"
# or "cloud" alone are not evidence. "Go" and "C" are omitted as too ambiguous.
TECH_TERMS = re.compile(
    r"(?:\b(?:python|java|javascript|typescript|golang|rust|scala|kotlin|swift|"
    r"php|ruby|rails|sql|postgres(?:ql)?|mysql|mongodb|redis|kafka|spark|"
    r"airflow|dbt|snowflake|bigquery|databricks|pytorch|tensorflow|"
    r"scikit-learn|pandas|llms?|machine learning|deep learning|"
    r"computer vision|nlp|aws|azure|gcp|google cloud|docker|kubernetes|k8s|"
    r"terraform|ansible|jenkins|github|gitlab|git|ci/cd|linux|"
    r"microservices|rest(?:ful)?|graphql|apis?|react|angular|vue(?:\.js)?|"
    r"node(?:\.js)?|django|flask|fastapi|spring|flutter|html|css|"
    r"elasticsearch|grafana|prometheus|selenium|cypress|playwright|"
    r"siem|devops|sre)\b|\.net\b|c\+\+|c#)",
    re.IGNORECASE,
)


def is_technology_title(title: str) -> bool:
    """Strict English title check; VietJobs keeps its own category rule."""
    if ENGLISH_EXCLUSIONS.search(title) or FALLBACK_EXCLUSIONS.search(title):
        return False
    if ENGLISH_STANDALONE_ROLE.search(title):
        return True
    return bool(ENGLISH_ROLE.search(title) and ENGLISH_IT_QUALIFIER.search(title))


def tech_terms(text: str) -> set[str]:
    return {match.casefold() for match in TECH_TERMS.findall(text)}


def mentions_technologies(text: str, minimum: int = 2) -> bool:
    """At least `minimum` distinct technologies named in the description."""
    return len(tech_terms(text)) >= minimum
