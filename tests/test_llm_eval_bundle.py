from __future__ import annotations

import asyncio
import json
from pathlib import Path

import duckdb

from app.config import Settings
from app.jobs import finalize_llm_eval_bundle, prepare_llm_eval_bundle


def test_prepare_and_finalize_llm_eval_bundle(tmp_path) -> None:
    database_path = tmp_path / "junex.duckdb"
    settings = Settings(duckdb_path=str(database_path))

    connection = duckdb.connect(str(database_path))
    connection.execute(
        """
        create schema analytics;
        create schema market_data;

        create table market_data.market_calendar as
        select date '2026-03-18' as "Date", '1' as "HolDiv"
        union all
        select date '2026-03-19' as "Date", '1' as "HolDiv";

        create table analytics.price_action_features as
        select * from (
            values
                (date '2026-03-18', '45920', 0.031738::double),
                (date '2026-03-19', '45920', -0.062988::double)
        ) as t(trade_date, code, ret_1d);

        create table analytics.market_industry_linkage_features as
        select * from (
            values
                (date '2026-03-18', '45920', 0.0::double),
                (date '2026-03-19', '45920', 0.0::double)
        ) as t(trade_date, code, industry_strength_pct_1d);

        create table analytics.flow_structure_features as
        select * from (
            values
                (date '2026-03-18', '45920', 0.0::double),
                (date '2026-03-19', '45920', 505337300.0::double)
        ) as t(trade_date, code, net_cash_va);

        create table analytics.fundamental_event_features as
        select * from (
            values
                (date '2026-03-18', '45920', 1),
                (date '2026-03-19', '45920', 0)
        ) as t(trade_date, code, earnings_event_window_3d);

        create table analytics.next_day_labels as
        select
            date '2026-03-18' as trade_date,
            '45920' as code,
            date '2026-03-19' as next_trade_date,
            -0.062988::double as label_next_ret_1d,
            0::bigint as label_next_up_1d,
            -0.033933::double as label_next_excess_ret_1d,
            -0.035834::double as label_next_industry_excess_ret_1d,
            'bearish' as label_next_direction_1d;
        """
    )
    connection.close()

    prepared = asyncio.run(
        prepare_llm_eval_bundle(
            settings,
            code="45920",
            target_date="2026-03-18",
            window_trading_days=20,
            output_dir=str(tmp_path / "bundles"),
        )
    )

    bundle_dir = Path(prepared["bundle_dir"])
    assert bundle_dir.exists()
    assert (bundle_dir / "bundle.json").exists()
    assert (bundle_dir / "prompts" / "openai_stock_eval_v1.md").exists()
    assert (bundle_dir / "prompts" / "gemini_stock_eval_v1.md").exists()
    assert (bundle_dir / "prompts" / "grok_stock_eval_v1.md").exists()
    assert (bundle_dir / "predictions").exists()

    bundle = json.loads((bundle_dir / "bundle.json").read_text(encoding="utf-8"))
    assert bundle["code"] == "45920"
    assert bundle["latest_date"] == "2026-03-18"
    assert len(bundle["factor_tables"]["price_action_features"]) == 1

    prediction_path = bundle_dir / "predictions" / "chatgpt.json"
    prediction_path.write_text(
        json.dumps(
            {
                "latest_date": "2026-03-18",
                "direction": "bullish",
                "confidence": 61,
                "summary": "test summary",
                "drivers": ["driver"],
                "risks": ["risk"],
                "pattern_judgement": "mean_reversion",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    finalized = asyncio.run(
        finalize_llm_eval_bundle(
            settings,
            bundle_dir=str(bundle_dir),
            provider="chatgpt",
            model="gpt-test",
            prompt_version="v1",
        )
    )

    assert finalized["upserted"] == 1
    assert finalized["evaluation"]["actual_next_direction_1d"] == "bearish"
    assert finalized["evaluation"]["is_direction_correct"] == 0
