from __future__ import annotations

import asyncio

import duckdb

from app.config import Settings
from app.jobs import run_llm_eval_auto


class FakeProvider:
    def predict_from_bundle(
        self,
        *,
        bundle: dict,
        prompt: str,
        model: str,
        reasoning_effort: str | None = None,
    ) -> dict:
        assert bundle["code"] == "45920"
        assert model == "fake-model"
        assert "Stock Eval v1" in prompt
        assert "direction" in prompt
        assert reasoning_effort == "low"
        return {
            "latest_date": bundle["latest_date"],
            "direction": "bullish",
            "confidence": 55,
            "summary": "fake summary",
            "drivers": ["driver"],
            "risks": ["risk"],
            "pattern_judgement": "mean_reversion",
        }


def test_run_llm_eval_auto_uses_provider_and_builds_eval(tmp_path, monkeypatch) -> None:
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
        select date '2026-03-18' as trade_date, '45920' as code, 0.031738::double as ret_1d
        union all
        select date '2026-03-19' as trade_date, '45920' as code, -0.062988::double as ret_1d;

        create table analytics.market_industry_linkage_features as
        select date '2026-03-18' as trade_date, '45920' as code, 0.0::double as industry_strength_pct_1d
        union all
        select date '2026-03-19' as trade_date, '45920' as code, 0.0::double as industry_strength_pct_1d;

        create table analytics.flow_structure_features as
        select date '2026-03-18' as trade_date, '45920' as code, 0.0::double as net_cash_va
        union all
        select date '2026-03-19' as trade_date, '45920' as code, 505337300.0::double as net_cash_va;

        create table analytics.fundamental_event_features as
        select date '2026-03-18' as trade_date, '45920' as code, 1 as earnings_event_window_3d
        union all
        select date '2026-03-19' as trade_date, '45920' as code, 0 as earnings_event_window_3d;

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

    monkeypatch.setattr("app.jobs.get_llm_provider", lambda provider_name, settings: FakeProvider())

    result = asyncio.run(
        run_llm_eval_auto(
            settings,
            code="45920",
            provider="chatgpt",
            model="fake-model",
            target_date="2026-03-18",
            prompt_version="v1",
            output_dir=str(tmp_path / "bundles"),
            reasoning_effort="low",
        )
    )

    assert result["upserted"] == 1
    assert result["evaluation"]["actual_next_direction_1d"] == "bearish"
    assert result["evaluation"]["is_direction_correct"] == 0
