# junex

A `uv`-managed data pipeline that uses the official J-Quants Python client, keeps datasets as pandas DataFrames, and persists them into a local DuckDB database for OLAP.

## Requirements

- Python 3.11+
- `uv`

## Install

```bash
uv sync --dev
```

## Stack

- `duckdb` for local analytical storage
- `httpx` for outbound API calls
- `jquants-api-client` for the official J-Quants v2 client
- `pandas` for DataFrame transforms and statistics
- `pydantic` for internal data models
- `pydantic-settings` for config via env and `.env`
- `structlog` for structured logs

## Run

```bash
uv run junex
```

The current scaffold does this:

- call external APIs
- normalize response data
- merge records
- compute summary stats
- write summary data into DuckDB
- print JSON output
- sync J-Quants datasets into DuckDB when `JQUANTS_API_KEY` is configured

## Test

```bash
uv run pytest
```

## Structure

```text
app/
  cli.py         # batch entrypoint
  clients.py     # external HTTP client + official J-Quants wrapper
  config.py      # settings from env/.env
  db.py          # DuckDB persistence helpers
  jobs.py        # batch job orchestration
  logging.py     # structured logging
  aggregator.py  # merge and aggregation logic
  stats.py       # statistical summaries
  models.py      # internal data models
```

## Configuration

Environment variables supported by default:

- `DUCKDB_PATH`
- `API_BASE_URL`
- `API_TIMEOUT_SECONDS`
- `DATASET_A_PATH`
- `DATASET_B_PATH`
- `JQUANTS_API_KEY`
- `LOG_LEVEL`

Example:

```bash
export JQUANTS_API_KEY=your_api_key
export DUCKDB_PATH=var/junex.duckdb
uv run junex sync-margin-interest --code 7203 --from-date 2024-01-01 --to-date 2024-03-31
```

Then inspect the stored data with DuckDB:

```bash
duckdb "$DUCKDB_PATH" 'select * from market_data.margin_interest limit 5;'
```
