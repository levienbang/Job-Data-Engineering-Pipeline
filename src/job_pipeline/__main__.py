"""Manual pipeline, export, and delivery commands."""

import argparse
import json
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx

from job_pipeline.checkpoints import finish_delivery_run, finish_run, start_run
from job_pipeline.config import DeliveryConfig, arbeitnow_max_pages
from job_pipeline.delivery import deliver_gold, export_batches
from job_pipeline.metadata import make_session_factory, new_run_id
from job_pipeline.pipeline import (
    arbeitnow_watermark,
    run_fake_jobs,
    run_pipeline,
    run_vietjobs,
)
from job_pipeline.quality import quality_report
from job_pipeline.sources.arbeitnow import ArbeitnowSource
from job_pipeline.sources.fake_jobs import FakeJobsCrawler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m job_pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "export", "deliver"):
        command = commands.add_parser(name)
        command.add_argument(
            "--source", choices=("vietjobs", "fake_jobs", "arbeitnow"), required=True
        )
        command.add_argument("--root", type=Path, default=Path("data"))
        if name == "run":
            command.add_argument("--csv", type=Path)
        if name == "export":
            command.add_argument("--retry-rejected", action="store_true")
            command.add_argument("--retry-blocked", action="store_true")
        if name == "deliver":
            command.add_argument("--endpoint", required=True)
            command.add_argument("--manifest", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "run":
            now = datetime.now(UTC)
            if args.source == "vietjobs":
                if args.csv is None:
                    raise ValueError("--csv is required for VietJobs")
                result = run_vietjobs(args.csv, collected_at=now, root=args.root)
            elif args.source == "arbeitnow":
                logging.basicConfig(level=logging.INFO)
                source = ArbeitnowSource(
                    max_pages=arbeitnow_max_pages(),
                    watermark=arbeitnow_watermark(args.root),
                )
                result = run_pipeline(
                    "arbeitnow", source.iter_raw(), collected_at=now, root=args.root
                )
            else:
                with FakeJobsCrawler() as crawler:
                    result = run_fake_jobs(
                        crawler.iter_raw(), collected_at=now, root=args.root
                    )
            print(json.dumps(quality_report(result.gold_path), ensure_ascii=False))
            return 0
        database_url = os.getenv("PIPELINE_DATABASE_URL")
        if not database_url:
            raise ValueError("PIPELINE_DATABASE_URL is required")
        sessions = make_session_factory(database_url)
        config = DeliveryConfig.from_env()
        if args.command == "export":
            run_id = new_run_id()
            start_run(sessions, run_id=run_id, source_name=args.source)
            gold = args.root / "gold" / f"source={args.source}" / "jobs.parquet"
            try:
                manifest = export_batches(
                    gold,
                    run_id=run_id,
                    sessions=sessions,
                    out_dir=config.export_root,
                    config=config,
                    retry_rejected=args.retry_rejected,
                    retry_blocked=args.retry_blocked,
                )
            except Exception as exc:
                finish_run(sessions, run_id, status="failed", error_message=str(exc))
                raise
            # The export run is complete; a later deliver updates the same run.
            finish_run(
                sessions,
                run_id,
                status="succeeded",
                blocked_count=manifest.data["blocked_count"],
            )
            print(
                json.dumps(
                    {
                        "manifest": str(manifest.path),
                        "batches": len(manifest.batches),
                        "accepted": 0,
                        "rejected": 0,
                        "blocked": manifest.data["blocked_count"],
                        "failed": 0,
                    }
                )
            )
            return 0
        manifest = args.manifest
        if manifest is None:
            options = list(
                config.export_root.glob(f"source={args.source}/run_id=*/manifest.json")
            )
            if not options:
                raise ValueError(f"no manifest found for {args.source}")
            manifest = max(options, key=lambda path: path.stat().st_mtime_ns)
        data = json.loads(manifest.read_text())
        if data["source_name"] != args.source:
            raise ValueError("manifest source differs from --source")
        start_run(sessions, run_id=data["run_id"], source_name=args.source)
        with httpx.Client() as client:
            report = deliver_gold(
                sessions=sessions,
                endpoint=args.endpoint,
                client=client,
                manifest=manifest,
                config=config,
            )
        finish_delivery_run(
            sessions,
            data["run_id"],
            status="failed" if report.failed_batches else "succeeded",
            blocked_count=report.blocked_records,
            error_message="delivery batches failed" if report.failed_batches else None,
        )
        print(
            json.dumps(
                {
                    "manifest": str(manifest),
                    "batches": report.sent_batches,
                    "accepted": report.delivered_records,
                    "rejected": report.rejected_records,
                    "blocked": report.blocked_records,
                    "failed": report.failed_batches,
                }
            )
        )
        return 1 if report.failed_batches else 0
    except (OSError, ValueError, RuntimeError, httpx.HTTPError) as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
