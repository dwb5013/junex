from __future__ import annotations

import asyncio
import json

import duckdb

from app.config import Settings
from app.jobs import build_llm_prediction_eval_table, import_llm_prediction, summarize_llm_prediction_eval


def test_import_llm_prediction_and_build_eval(tmp_path) -> None:
    database_path = tmp_path / "junex.duckdb"
    settings = Settings(duckdb_path=str(database_path))

    prediction_path = tmp_path / "prediction.json"
    prediction_path.write_text(
        json.dumps(
            {
                "latest_date": "2026-03-18",
                "direction": "bullish",
                "confidence": 65,
                "summary": "Strong rebound day.",
                "drivers": ["industry strength", "volume support"],
                "risks": ["trend still weak"],
                "pattern_judgement": "mean_reversion",
                "latest_day_analysis": "Rebounded after a gap down.",
                "market_relative_analysis": "Outperformed TOPIX on the day.",
                "industry_relative_analysis": "Strong versus industry.",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    connection = duckdb.connect(str(database_path))
    connection.execute(
        """
        create schema analytics;
        create table analytics.next_day_labels as
        select
            date '2026-03-18' as trade_date,
            '45920' as code,
            date '2026-03-19' as next_trade_date,
            -0.06298828125::double as label_next_ret_1d,
            0::bigint as label_next_up_1d,
            -0.033933::double as label_next_excess_ret_1d,
            -0.035834::double as label_next_industry_excess_ret_1d,
            'bearish' as label_next_direction_1d;
        """
    )
    connection.close()

    upserted = asyncio.run(
        import_llm_prediction(
            settings,
            input_path=str(prediction_path),
            code="45920",
            provider="chatgpt",
            model="gpt-test",
            prompt_version="v1",
        )
    )
    assert upserted == 1

    row_count = asyncio.run(build_llm_prediction_eval_table(settings))
    assert row_count == 1

    connection = duckdb.connect(str(database_path), read_only=True)
    imported = connection.execute(
        """
        select trade_date, code, provider, model, prompt_version, pred_direction, pred_confidence
        from analytics.llm_predictions
        """
    ).fetchone()
    evaluated = connection.execute(
        """
        select
            trade_date,
            code,
            pred_direction,
            actual_next_direction_1d,
            is_direction_correct,
            is_binary_direction_correct
        from analytics.llm_prediction_eval
        """
    ).fetchone()
    connection.close()

    assert imported == ("2026-03-18", "45920", "chatgpt", "gpt-test", "v1", "bullish", 65.0)
    assert evaluated == ("2026-03-18", "45920", "bullish", "bearish", 0, 0)


def test_import_llm_prediction_migrates_old_schema(tmp_path) -> None:
    database_path = tmp_path / "junex.duckdb"
    settings = Settings(duckdb_path=str(database_path))

    prediction_path = tmp_path / "prediction_long.json"
    prediction_path.write_text(
        json.dumps(
            {
                "latest_date": "2026-03-18",
                "direction": "bullish",
                "confidence": 61,
                "summary": "summary",
                "drivers": ["driver"],
                "risks": ["risk"],
                "pattern_judgement": "mean_reversion",
                "latest_day_analysis": "long text",
                "market_relative_analysis": "market text",
                "industry_relative_analysis": "industry text",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    connection = duckdb.connect(str(database_path))
    connection.execute(
        """
        create schema analytics;
        create table analytics.llm_predictions as
        select
            '2026-03-18'::varchar as trade_date,
            '45920'::varchar as code,
            'chatgpt'::varchar as provider,
            'gpt-old'::varchar as model,
            'v1'::varchar as prompt_version,
            'bullish'::varchar as pred_direction,
            61::bigint as pred_confidence,
            'mean_reversion'::varchar as pred_pattern_judgement,
            'summary'::varchar as pred_summary,
            '[]'::varchar as pred_drivers_json,
            '[]'::varchar as pred_risks_json,
            null::integer as pred_latest_day_analysis,
            null::integer as pred_market_relative_analysis,
            null::integer as pred_industry_relative_analysis,
            '{}'::varchar as raw_json,
            current_timestamp as imported_at;
        """
    )
    connection.close()

    upserted = asyncio.run(
        import_llm_prediction(
            settings,
            input_path=str(prediction_path),
            code="45920",
            provider="gemini",
            model="gemini-test",
            prompt_version="v1",
        )
    )
    assert upserted == 1

    connection = duckdb.connect(str(database_path), read_only=True)
    describe_rows = connection.execute("describe analytics.llm_predictions").fetchall()
    row = connection.execute(
        """
        select
            pred_latest_day_analysis,
            pred_market_relative_analysis,
            pred_industry_relative_analysis
        from analytics.llm_predictions
        where provider = 'gemini'
        """
    ).fetchone()
    connection.close()

    types = {column: dtype for column, dtype, *_ in describe_rows}
    assert types["pred_latest_day_analysis"] == "VARCHAR"
    assert types["pred_market_relative_analysis"] == "VARCHAR"
    assert types["pred_industry_relative_analysis"] == "VARCHAR"
    assert row == ("long text", "market text", "industry text")


def test_summarize_llm_prediction_eval_returns_group_metrics(tmp_path) -> None:
    database_path = tmp_path / "junex.duckdb"
    settings = Settings(duckdb_path=str(database_path))

    connection = duckdb.connect(str(database_path))
    connection.execute(
        """
        create schema analytics;
        create table analytics.llm_prediction_eval as
        select * from (
            values
                (date '2026-03-18', '45920', 'chatgpt', 'gpt-a', 'v1', 'bullish', 61.0, 'bearish', 0, 0, -0.062988::double, -0.033933::double, -0.035834::double),
                (date '2026-03-17', '45920', 'chatgpt', 'gpt-a', 'v1', 'bullish', 55.0, 'bullish', 1, 1, 0.031738::double, 0.006831::double, 0.027548::double),
                (date '2026-03-18', '45920', 'gemini', 'gemini-a', 'v1', 'bullish', 65.0, 'bearish', 0, 0, -0.062988::double, -0.033933::double, -0.035834::double)
        ) as t(
            trade_date,
            code,
            provider,
            model,
            prompt_version,
            pred_direction,
            pred_confidence,
            actual_next_direction_1d,
            is_direction_correct,
            is_binary_direction_correct,
            actual_next_ret_1d,
            actual_next_excess_ret_1d,
            actual_next_industry_excess_ret_1d
        );
        """
    )
    connection.close()

    result = asyncio.run(summarize_llm_prediction_eval(settings, provider="chatgpt"))

    assert result["filters"]["provider"] == "chatgpt"
    assert len(result["summary"]) == 1
    summary = result["summary"][0]
    assert summary["provider"] == "chatgpt"
    assert summary["model"] == "gpt-a"
    assert summary["sample_count"] == 2
    assert round(summary["direction_accuracy"], 6) == 0.5
    assert round(summary["binary_direction_accuracy"], 6) == 0.5
    assert round(summary["avg_confidence"], 6) == 58.0
    assert len(result["latest_samples"]) == 2
