"""Raw acquisition boundary shared by the supported sources."""

from collections.abc import Iterator
from typing import Protocol, runtime_checkable


@runtime_checkable
class RawSource(Protocol):
    source_name: str

    def iter_raw(self) -> Iterator[dict[str, object]]: ...
