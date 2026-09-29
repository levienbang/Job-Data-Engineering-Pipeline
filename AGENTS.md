# AGENTS.md

## Project

This repository implements the Job Data Engineering Pipeline. Read
`docs/PROJECT_SPEC.md` before architectural changes and implement only the
milestone requested by the user.

## Working rules

- Inspect relevant code before editing and preserve existing user changes.
- Keep source adapters, transforms, validation, storage and delivery separate.
- Prefer simple, testable functions over premature abstractions.
- Do not modify the project spec unless explicitly requested.
- Add tests for every behavior change and report checks that could not run.
- Never commit datasets, raw dumps, `.env`, credentials or private data.
- Do not add optional infrastructure unless the current milestone requires it.

## Serena

Use Serena for semantic code navigation.

- Activate this repository when starting a task if needed.
- Check only Serena memories relevant to the task.
- Prefer Serena for symbol lookup, references and implementation discovery
  before broad repository scans.
- Verify implementation details against current source code.
- Store only durable architectural decisions and project conventions in memory.
- Source code and `docs/PROJECT_SPEC.md` override stale Serena memories.

## Architecture

```text
Sources → Bronze → clean/validate → Silver → deduplicate → Gold
        → DeliveryMapper → POST /jobs/import
```

- Bronze snapshots are immutable.
- Gold keeps the DE canonical data and lineage metadata.
- `DeliveryMapper` converts Gold fields to the AI Platform import contract.
- DE PostgreSQL stores run metadata, checkpoints and delivery status.
- Do not write directly to the AI Platform PostgreSQL or Qdrant.
- Do not import ORM models from the AI Platform repository.

## Data and source safety

- Never invent missing values; use `null`.
- Keep transformations deterministic and idempotent.
- Exclude collection timestamps from content hashes.
- Quarantine invalid records with explicit reasons.
- Split delivery files by serialized byte size.
- Tests use fixtures, not live websites or paid APIs.
- Supported sources are Fake Jobs and the published VietJobs dataset.
- Respect allowlists, timeouts, rate limits and bounded retries.
- Never bypass login, CAPTCHA, robots controls or anti-bot protections.

## Stack

Core: Python 3.12, HTTPX, Beautiful Soup, Polars, Parquet, DuckDB, Pydantic,
PostgreSQL, SQLAlchemy, Alembic, Pytest, Ruff and Docker Compose.

Add Airflow only after standalone pipeline modules work. AWS, Kafka and
Snowflake are optional and must not be added unless explicitly requested.

## Before completion

- Run focused tests, then the relevant full suite.
- Run lint/format checks and verify migrations when applicable.
- Test reruns, duplicates and expected failures.
- Distinguish fake/deterministic tests from live integrations.
- Report modified files, commands, actual results and remaining limitations.
