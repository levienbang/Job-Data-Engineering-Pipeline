import subprocess
import sys
from pathlib import Path


def test_local_inspection_reports_fixture_counts() -> None:
    root = Path(__file__).parent.parent
    csv_path = root / "tests" / "fixtures" / "vietjobs_small.csv"
    result = subprocess.run(
        [sys.executable, str(root / "scripts" / "inspect_vietjobs.py"), str(csv_path)],
        check=True,
        capture_output=True,
        text=True,
    )

    output = result.stdout
    assert "Rows: 2" in output
    assert "Columns (18):" in output
    assert "Technology jobs: 0 (0.00%)" in output
