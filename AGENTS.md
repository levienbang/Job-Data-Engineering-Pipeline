# AGENTS.md

## Project

This repository implements the Job Data Engineering Pipeline. Read
`docs/PROJECT_SPEC.md` before architectural changes and implement only the
milestone requested by the user.

## Current milestone

Read `docs/CURRENT_MILESTONE.md`. The user authorized that file; it is
overwritten for each new milestone. Where it conflicts with
`docs/PROJECT_SPEC.md`, the current milestone file wins for that milestone
only. Do not modify the AI Platform repository (`../LLM`). If the file says the
milestone is complete, do not re-implement it; wait for a new request.

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
- Memories live in `.serena/memories/`; start from `core` and follow its `mem:`
  references. Follow `memory_maintenance` for style and the update threshold.

## Keeping docs and memory current

Every task that changes code is incomplete until these are checked. Update only
what the change actually affects; do not rewrite unrelated sections.

| Change | Update |
|---|---|
| Source map, module responsibility, data layout, milestone state | Serena `core` |
| Data invariant, hashing, dedup, quarantine, delivery/retry semantics, payload contract | Serena `conventions` + README |
| Dependency, version pin, new tool | Serena `tech_stack` + `pyproject.toml` |
| New/changed CLI command, script or flag | Serena `suggested_commands` + README |
| New/changed check or completion gate | Serena `task_completion` |
| New environment variable | `.env.example` + README variable table |
| Milestone finished | `docs/CURRENT_MILESTONE.md` set to the "no active milestone" note listing what finished; Serena `core` state line |

- Use Serena `write_memory`/`edit_memory`. If Serena is not active for this
  repository, edit the files in `.serena/memories/` directly.
- Write memories as dense, durable bullets; never put logs, line numbers, temporary
  debugging notes or secrets in them. Delete or fix a memory that the change made
  wrong instead of appending a contradiction.
- If nothing needed updating, say so in the final report.

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
- Report which docs and Serena memories were updated (see "Keeping docs and
  memory current"), or state that none needed changes.
