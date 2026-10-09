"""Validated environment configuration for export and delivery."""

import os
from dataclasses import dataclass
from math import isfinite
from pathlib import Path


@dataclass(frozen=True)
class DeliveryConfig:
    profile: str = "ai_platform_v1"
    missing_company: str = "block"
    max_bytes: int = 1_000_000
    max_records: int = 20
    timeout_seconds: float = 300
    max_attempts: int = 3
    enabled: bool = True
    export_root: Path = Path("data/export")

    def __post_init__(self) -> None:
        if self.profile not in {"ai_platform_v1", "extended"}:
            raise ValueError(
                "DELIVERY_PAYLOAD_PROFILE must be ai_platform_v1 or extended"
            )
        if self.missing_company not in {"block", "send_null"}:
            raise ValueError("DELIVERY_MISSING_COMPANY must be block or send_null")
        if self.max_bytes < 3:
            raise ValueError("DELIVERY_MAX_BYTES must be at least 3")
        if self.max_records < 1:
            raise ValueError("DELIVERY_MAX_RECORDS must be positive")
        if not isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("DELIVERY_TIMEOUT_SECONDS must be positive")
        if self.max_attempts < 1:
            raise ValueError("DELIVERY_MAX_ATTEMPTS must be positive")

    @classmethod
    def from_env(cls) -> "DeliveryConfig":
        enabled = os.getenv("DELIVERY_ENABLED", "true").lower()
        if enabled not in {"true", "false"}:
            raise ValueError("DELIVERY_ENABLED must be true or false")
        try:
            return cls(
                profile=os.getenv("DELIVERY_PAYLOAD_PROFILE", "ai_platform_v1"),
                missing_company=os.getenv("DELIVERY_MISSING_COMPANY", "block"),
                max_bytes=int(os.getenv("DELIVERY_MAX_BYTES", "1000000")),
                max_records=int(os.getenv("DELIVERY_MAX_RECORDS", "20")),
                timeout_seconds=float(os.getenv("DELIVERY_TIMEOUT_SECONDS", "300")),
                max_attempts=int(os.getenv("DELIVERY_MAX_ATTEMPTS", "3")),
                enabled=enabled == "true",
                export_root=Path(os.getenv("PIPELINE_EXPORT_ROOT", "data/export")),
            )
        except ValueError as exc:
            raise ValueError(f"invalid delivery configuration: {exc}") from exc


def arbeitnow_max_pages() -> int:
    """Shared CLI/Airflow bound for public API acquisition."""
    try:
        value = int(os.getenv("ARBEITNOW_MAX_PAGES", "5"))
        if value < 1:
            raise ValueError
    except ValueError as exc:
        raise ValueError("ARBEITNOW_MAX_PAGES must be an integer >= 1") from exc
    return value
