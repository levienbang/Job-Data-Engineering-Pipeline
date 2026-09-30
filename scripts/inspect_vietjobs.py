"""Print a local VietJobs CSV profile without downloading or writing data."""

import argparse
import csv
from pathlib import Path

from job_pipeline.sources.technology import is_technology_job


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", type=Path, help="Path to a local VietJobs CSV")
    args = parser.parse_args()

    with args.csv_path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        columns = reader.fieldnames
        if not columns:
            raise ValueError("CSV has no header")
        nulls = dict.fromkeys(columns, 0)
        total = 0
        technology = 0
        for row in reader:
            total += 1
            technology += is_technology_job(row)
            for column in columns:
                nulls[column] += not (row[column] or "").strip()

    print(f"Rows: {total}")
    print(f"Columns ({len(columns)}): {', '.join(columns)}")
    for column in columns:
        percentage = 100 * nulls[column] / total if total else 0
        print(f"{column}: {nulls[column]} null ({percentage:.2f}%)")
    percentage = 100 * technology / total if total else 0
    print(f"Technology jobs: {technology} ({percentage:.2f}%)")


if __name__ == "__main__":
    main()
