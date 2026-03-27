# junex

A `uv`-managed stock analysis pipeline built on J-Quants, DuckDB, and LLM-based post-close evaluation workflows.

## Requirements

- Python 3.13+
- `uv`

## Install

```bash
uv sync --dev
```

If you want to run the automatic LLM evaluation flow, also configure one or more provider API keys in `.env`:

- `OPENAI_API_KEY`
- `GEMINI_API_KEY`
- `XAI_API_KEY`

## Stack

- `duckdb` for local analytical storage
- `httpx` for outbound API calls
- `jquants-api-client` for the official J-Quants v2 client
- `openai` for OpenAI/ChatGPT SDK access
- `google-genai` for Gemini SDK access
- `xai-sdk` for Grok/xAI SDK access
- `pandas` for DataFrame transforms and statistics
- `pydantic` for internal data models
- `pydantic-settings` for config via env and `.env`
- `structlog` for structured logs

## Run

```bash
uv run junex
```

The project currently supports:

- syncing J-Quants datasets into DuckDB
- building factor tables and labels
- exporting per-stock factor snapshots
- evaluating structured LLM predictions against next-day labels
- running fully automatic provider-SDK based LLM evaluation

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
  llm.py         # provider SDK wrappers and LLM schema
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
- `OPENAI_API_KEY`
- `GEMINI_API_KEY`
- `XAI_API_KEY`
- `LOG_LEVEL`

Example:

```bash
export JQUANTS_API_KEY=your_api_key
export OPENAI_API_KEY=your_openai_key
export DUCKDB_PATH=var/junex.duckdb
uv run junex sync-margin-interest --code 7203 --from-date 2024-01-01 --to-date 2024-03-31
```

Then inspect the stored data with DuckDB:

```bash
duckdb "$DUCKDB_PATH" 'select * from market_data.margin_interest limit 5;'
```

## Analytics Layers

Current analytics tables:

- `analytics.price_action_features`
- `analytics.market_industry_linkage_features`
- `analytics.flow_structure_features`
- `analytics.fundamental_event_features`
- `analytics.next_day_labels`
- `analytics.llm_predictions`
- `analytics.llm_prediction_eval`

Build commands:

```bash
uv run junex build-price-action-features
uv run junex build-market-industry-linkage-features
uv run junex build-flow-structure-features
uv run junex build-fundamental-event-features
uv run junex build-next-day-labels
```

## Daily Workflow

Run the end-of-day workflow for one trading date:

```bash
uv run junex dailywork --date 2026-03-19
```

This syncs source data and rebuilds:

- price action factors
- market/industry linkage factors
- flow structure factors
- fundamental event factors
- next-day labels

## LLM Evaluation

### Semi-automatic workflow

Prepare a bundle for manual use with ChatGPT / Gemini / Grok:

```bash
uv run junex prepare-llm-eval --code 45920 --date 2026-03-18 --output-dir var/llm_eval
```

This creates a directory like:

- `bundle.json`
- `prompts/openai_stock_eval_v1.md`
- `prompts/gemini_stock_eval_v1.md`
- `prompts/grok_stock_eval_v1.md`
- `predictions/`

Each provider uses its own prompt template under `prompts/`.

After you save a model response JSON into `predictions/`, import and evaluate it:

```bash
uv run junex finalize-llm-eval \
  --bundle-dir var/llm_eval/45920_2026-03-18 \
  --provider gemini \
  --model gemini-3.1-pro-preview \
  --prompt-version v1
```

### Fully automatic workflow

Run one automatic SDK-backed prediction and evaluation:

```bash
uv run junex run-llm-eval-auto \
  --code 45920 \
  --provider gemini \
  --model gemini-3.1-pro-preview \
  --date 2026-03-18 \
  --prompt-version v1 \
  --output-dir var/llm_eval
```

### Historical batch evaluation

Run automatic evaluation across a trading-date range:

```bash
uv run junex run-llm-eval-auto-range \
  --code 45920 \
  --provider chatgpt \
  --model GPT-5.4 \
  --from-date 2026-01-01 \
  --to-date 2026-03-19 \
  --prompt-version v1 \
  --output-dir var/llm_eval
```

Notes:

- The range command uses available trading dates from `analytics.price_action_features`
- By default it skips dates that already exist in `analytics.llm_predictions` for the same `code + provider + model + prompt_version`
- Use `--overwrite-existing` if you want to rerun and replace stored predictions

### OpenAI Batch workflow

For larger historical tests with OpenAI/ChatGPT, use the dedicated Batch flow:

1. Prepare a Batch request file across a trading-date range:

```bash
uv run junex prepare-openai-batch-range \
  --code 45920 \
  --model GPT-5.4 \
  --from-date 2026-01-01 \
  --to-date 2026-03-19 \
  --prompt-version v1 \
  --output-dir var/openai_batch
```

2. Submit the prepared Batch job:

```bash
uv run junex submit-openai-batch \
  --batch-dir var/openai_batch/45920_2026-01-01_2026-03-19_GPT-5.4_v1
```

3. Finalize the Batch job:

```bash
uv run junex finalize-openai-batch \
  --batch-dir var/openai_batch/45920_2026-01-01_2026-03-19_GPT-5.4_v1
```

If the Batch job is not finished yet, `finalize-openai-batch` returns `status: pending` and does not import or evaluate results.

### Evaluation summary

Summarize accuracy and average realized returns:

```bash
uv run junex summarize-llm-prediction-eval
uv run junex summarize-llm-prediction-eval --code 45920
uv run junex summarize-llm-prediction-eval --provider gemini
```
