"""DuckDB checks over local Gold Parquet."""

from pathlib import Path
from statistics import median

import duckdb

from job_pipeline.pipeline import CANONICAL_SCHEMA

NEW_FIELDS = tuple(
    field
    for field in CANONICAL_SCHEMA
    if field.startswith("raw_")
    and field not in {"raw_title", "raw_description", "raw_company", "raw_location"}
)


def quality_report(gold_path: Path) -> dict[str, object]:
    """Return counts, required-field nulls, duplicate identities and distributions."""
    with duckdb.connect(":memory:") as connection:
        connection.from_parquet(str(gold_path)).create_view("jobs")
        columns = {row[0] for row in connection.execute("DESCRIBE jobs").fetchall()}
        if (
            set(CANONICAL_SCHEMA) - columns
            or connection.execute(
                "SELECT count(*) FROM jobs WHERE schema_version != 2"
            ).fetchone()[0]
        ):
            raise ValueError(
                f"Gold schema v1 detected at {gold_path}; rerun the pipeline"
            )
        count, missing_title, missing_description = connection.execute(
            """SELECT count(*),
                      count(*) FILTER (WHERE raw_title IS NULL OR trim(raw_title) = ''),
                      count(*) FILTER (WHERE raw_description IS NULL
                                       OR trim(raw_description) = '')
               FROM jobs"""
        ).fetchone()
        duplicates = connection.execute(
            """SELECT coalesce(sum(n - 1), 0)
               FROM (SELECT count(*) AS n FROM jobs
                     GROUP BY source_name,
                     coalesce('id:' || source_job_id,
                              'hash:' || source_record_hash))"""
        ).fetchone()[0]
        sources = dict(
            connection.execute(
                "SELECT source_name, count(*) FROM jobs "
                "GROUP BY source_name ORDER BY source_name"
            ).fetchall()
        )
        locations = dict(
            connection.execute(
                "SELECT raw_location, count(*) FROM jobs GROUP BY raw_location "
                "ORDER BY count(*) DESC, raw_location LIMIT 10"
            ).fetchall()
        )
        nulls = dict(
            zip(
                (
                    "source_job_id",
                    "source_url",
                    "raw_company",
                    "raw_location",
                    "posted_at",
                ),
                connection.execute(
                    """SELECT count(*) FILTER (WHERE source_job_id IS NULL),
                              count(*) FILTER (WHERE source_url IS NULL),
                              count(*) FILTER (WHERE raw_company IS NULL),
                              count(*) FILTER (WHERE raw_location IS NULL),
                              count(*) FILTER (WHERE posted_at IS NULL)
                       FROM jobs"""
                ).fetchone(),
                strict=True,
            )
        )
        nulls.update(
            dict(
                zip(
                    NEW_FIELDS,
                    connection.execute(
                        "SELECT "
                        + ", ".join(
                            f"count(*) FILTER (WHERE {field} IS NULL)"
                            for field in NEW_FIELDS
                        )
                        + " FROM jobs"
                    ).fetchone(),
                    strict=True,
                )
            )
        )
        versions = dict(
            connection.execute(
                "SELECT schema_version, count(*) FROM jobs GROUP BY schema_version"
            ).fetchall()
        )
        near_duplicates = connection.execute(
            "SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(*) AS n FROM jobs "
            "GROUP BY raw_title, raw_location, raw_employment_type, raw_description)"
        ).fetchone()[0]
        byte_lengths = [
            len(value.encode("utf-8"))
            for (value,) in connection.execute(
                "SELECT raw_description FROM jobs WHERE raw_description IS NOT NULL"
            ).fetchall()
        ]
        byte_lengths.sort()
        employment_types = dict(
            connection.execute(
                "SELECT raw_employment_type, count(*) FROM jobs "
                "GROUP BY raw_employment_type ORDER BY count(*) DESC"
            ).fetchall()
        )
        return {
            "count": count,
            "missing_title": missing_title,
            "missing_description": missing_description,
            "duplicates": duplicates,
            "sources": sources,
            "nulls": nulls,
            "top_locations": locations,
            "employment_types": employment_types,
            "schema_versions": versions,
            "missing_company": nulls["raw_company"],
            "near_duplicates": near_duplicates,
            "description_bytes": {
                "min": byte_lengths[0] if byte_lengths else None,
                "p50": median(byte_lengths) if byte_lengths else None,
                "max": byte_lengths[-1] if byte_lengths else None,
            },
        }
