# junex

A minimal `uv` project scaffold for batch jobs that call external APIs, aggregate data, compute metrics, and write results to a database.

## Requirements

- Python 3.11+
- `uv`

## Install

```bash
uv sync --dev
```

## Stack

- `alembic` for schema migrations
- `httpx` for outbound API calls
- `pydantic` for internal data models
- `pydantic-settings` for config via env and `.env`
- `psycopg` for PostgreSQL driver support
- `sqlalchemy` for database access
- `pandas` for aggregation and statistics
- `structlog` for structured logs

## Run

```bash
uv run junex
```

If you want a local PostgreSQL with Docker:

```bash
docker compose up -d db
uv run alembic upgrade head
uv run junex
```

The current scaffold does this:

- call external APIs
- normalize response data
- merge records
- compute summary stats
- write summary data into a database
- print JSON output
- sync J-Quants `/v2/equities/master` into PostgreSQL when `JQUANTS_API_KEY` is configured

## Test

```bash
uv run pytest
```

## Structure

```text
app/
  cli.py         # batch entrypoint
  clients.py     # outbound API clients
  config.py      # settings from env/.env
  db.py          # database access
  jobs.py        # batch job orchestration
  logging.py     # structured logging
  aggregator.py  # merge and aggregation logic
  stats.py       # statistical summaries
  models.py      # internal data models
```

## Configuration

Environment variables supported by default:

- `DATABASE_URL`
- `API_BASE_URL`
- `API_TIMEOUT_SECONDS`
- `DATASET_A_PATH`
- `DATASET_B_PATH`
- `JQUANTS_API_BASE_URL`
- `JQUANTS_API_KEY`
- `JQUANTS_TIMEOUT_SECONDS`
- `LOG_LEVEL`

Copy `.env.example` to `.env` for local development and put your real credentials only in `.env`.
The project ignores `.env`, so your secrets will not be committed as long as you do not paste them into tracked files.

## Migrations

```bash
uv run alembic upgrade head
```
