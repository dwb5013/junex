from __future__ import annotations

import asyncio

import duckdb

from app.config import Settings
from app.jobs import run_llm_eval_auto_range


async def _fake_run_llm_eval_auto(
    _settings,
    *,
    code,
    provider,
    model,
    target_date=None,
    prompt_version="v1",
    window_trading_days=20,
    output_dir=None,
    reasoning_effort=None,
):
    return {
        "bundle_dir": "dummy",
        "prediction_path": "dummy.json",
        "upserted": 1,
        "eval_row_count": 1,
        "evaluation": {
            "trade_date": target_date,
            "code": code,
            "provider": provider,
            "model": model,
            "prompt_version": prompt_version,
        },
    }


async def _fake_summarize_llm_prediction_eval(_settings, **kwargs):
    return {
        "summary": [
            {
                "provider": kwargs.get("provider"),
                "model": kwargs.get("model"),
                "prompt_version": kwargs.get("prompt_version"),
                "sample_count": 1,
            }
        ],
        "latest_samples": [],
        "filters": kwargs,
    }


def test_run_llm_eval_auto_range_processes_trade_dates_and_skips_existing(tmp_path, monkeypatch) -> None:
    database_path = tmp_path / "junex.duckdb"
    settings = Settings(duckdb_path=str(database_path))

    connection = duckdb.connect(str(database_path))
    connection.execute(
        """
        create schema analytics;
        create table analytics.price_action_features as
        select date '2026-03-17' as trade_date, '45920' as code
        union all
        select date '2026-03-18' as trade_date, '45920' as code
        union all
        select date '2026-03-19' as trade_date, '45920' as code;

        create table analytics.llm_predictions as
        select
            '2026-03-18'::varchar as trade_date,
            '45920'::varchar as code,
            'chatgpt'::varchar as provider,
            'gpt-5'::varchar as model,
            'v1'::varchar as prompt_version,
            'bullish'::varchar as pred_direction,
            60.0::double as pred_confidence,
            'mean_reversion'::varchar as pred_pattern_judgement,
            'summary'::varchar as pred_summary,
            '[]'::varchar as pred_drivers_json,
            '[]'::varchar as pred_risks_json,
            ''::varchar as pred_latest_day_analysis,
            ''::varchar as pred_market_relative_analysis,
            ''::varchar as pred_industry_relative_analysis,
            '{}'::varchar as raw_json,
            current_timestamp as imported_at;
        """
    )
    connection.close()

    processed: list[str] = []

    async def recording_fake_run(*args, **kwargs):
        processed.append(kwargs["target_date"])
        return await _fake_run_llm_eval_auto(*args, **kwargs)

    monkeypatch.setattr("app.jobs.run_llm_eval_auto", recording_fake_run)
    monkeypatch.setattr("app.jobs.summarize_llm_prediction_eval", _fake_summarize_llm_prediction_eval)

    result = asyncio.run(
        run_llm_eval_auto_range(
            settings,
            code="45920",
            provider="chatgpt",
            model="gpt-5",
            start_date="2026-03-17",
            end_date="2026-03-19",
            prompt_version="v1",
        )
    )

    assert processed == ["2026-03-17", "2026-03-19"]
    assert result["total_trade_dates"] == 3
    assert result["processed_count"] == 2
    assert result["skipped_count"] == 1
    assert result["skipped_dates"] == ["2026-03-18"]
    assert result["failed_count"] == 0
