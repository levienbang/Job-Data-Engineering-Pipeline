"""Local Parquet persistence and immutable Bronze checksums."""

import hashlib
import os
from pathlib import Path

import polars as pl


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_parquet(frame: pl.DataFrame, path: Path, *, immutable: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if immutable and path.exists():
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        frame.write_parquet(temporary)
        if immutable:
            try:
                os.link(temporary, path)
            except FileExistsError:
                pass
        else:
            temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def read_parquet(path: Path) -> pl.DataFrame:
    try:
        return pl.read_parquet(path)
    except Exception as exc:
        raise ValueError(f"corrupt Parquet input: {path}") from exc
