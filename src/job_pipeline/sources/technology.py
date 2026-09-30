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
