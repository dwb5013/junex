from __future__ import annotations

import asyncio
import json
from pathlib import Path

import duckdb

from app.config import Settings
from app.jobs import finalize_openai_batch, prepare_openai_batch_range


class FakeBatchRunner:
    def __init__(self) -> None:
        self.lines: list[dict[str, object]] = []

    def build_request_line(
        self,
        *,
        custom_id: str,
        bundle: dict,
        prompt: str,
        model: str,
        reasoning_effort: str | None = None,
    ) -> dict:
        self.lines.append(
            {
                "custom_id": custom_id,
                "latest_date": bundle["latest_date"],
                "prompt": prompt,
                "model": model,
                "reasoning_effort": reasoning_effort,
            }
        )
        return {
            "custom_id": custom_id,
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {"model": model},
        }


class PendingBatchRunner(FakeBatchRunner):
    def retrieve_batch(self, *, batch_id: str) -> dict:
        return {
            "batch_id": batch_id,
            "status": "in_progress",
            "input_file_id": "file-input",
            "output_file_id": None,
            "error_file_id": None,
            "completion_window": "24h",
        }


class CompletedBatchRunner(FakeBatchRunner):
    def retrieve_batch(self, *, batch_id: str) -> dict:
        return {
            "batch_id": batch_id,
            "status": "completed",
            "input_file_id": "file-input",
            "output_file_id": "file-output",
            "error_file_id": None,
            "completion_window": "24h",
        }

    def download_file_text(self, *, file_id: str) -> str:
        assert file_id == "file-output"
        return json.dumps(
            {
                "custom_id": "45920__2026-03-18__chatgpt__gpt-5.4__v1",
                "response": {
                    "body": {
                        "choices": [
                            {
                                "message": {
                                    "content": json.dumps(
                                        {
                                            "latest_date": "2026-03-18",
                                            "direction": "bullish",
                                            "confidence": 55,
                                            "summary": "batch summary",
                                            "drivers": ["driver"],
                                            "risks": ["risk"],
                                            "pattern_judgement": "mean_reversion",
                                        },
                                        ensure_ascii=False,
                                    )
                                }
                            }
                        ]
                    }
                },
            },
            ensure_ascii=False,
        )


def _seed_minimal_llm_db(database_path: Path) -> None:
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


def test_prepare_openai_batch_range_writes_jsonl_and_manifest(tmp_path, monkeypatch) -> None:
    database_path = tmp_path / "junex.duckdb"
    settings = Settings(duckdb_path=str(database_path))
    _seed_minimal_llm_db(database_path)

    fake_runner = FakeBatchRunner()
    monkeypatch.setattr("app.jobs.get_openai_batch_runner", lambda settings: fake_runner)

    result = asyncio.run(
            prepare_openai_batch_range(
                settings,
                code="45920",
                model="gpt-5.4",
                start_date="2026-03-18",
                end_date="2026-03-19",
                output_dir=str(tmp_path / "batch"),
                reasoning_effort="low",
            )
        )

    assert result["request_count"] == 2
    manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
    assert manifest["request_count"] == 2
    assert len(manifest["items"]) == 2
    jsonl_lines = Path(result["jsonl_path"]).read_text(encoding="utf-8").strip().splitlines()
    assert len(jsonl_lines) == 2
    assert "OpenAI Stock Eval v1" in fake_runner.lines[0]["prompt"]
    assert fake_runner.lines[0]["reasoning_effort"] == "low"


def test_finalize_openai_batch_returns_pending_when_not_ready(tmp_path, monkeypatch) -> None:
    database_path = tmp_path / "junex.duckdb"
    settings = Settings(duckdb_path=str(database_path))
    _seed_minimal_llm_db(database_path)

    fake_runner = PendingBatchRunner()
    monkeypatch.setattr("app.jobs.get_openai_batch_runner", lambda settings: fake_runner)

    prepared = asyncio.run(
        prepare_openai_batch_range(
            settings,
            code="45920",
            model="gpt-5.4",
            start_date="2026-03-18",
            end_date="2026-03-18",
            output_dir=str(tmp_path / "batch"),
        )
    )
    batch_dir = Path(prepared["batch_dir"])
    (batch_dir / "submission.json").write_text(
        json.dumps({"batch_id": "batch-123", "status": "in_progress"}, ensure_ascii=False),
        encoding="utf-8",
    )

    result = asyncio.run(finalize_openai_batch(settings, batch_dir=str(batch_dir)))

    assert result["status"] == "pending"
    assert result["evaluated"] is False
    assert result["imported_count"] == 0


def test_finalize_openai_batch_imports_predictions_when_completed(tmp_path, monkeypatch) -> None:
    database_path = tmp_path / "junex.duckdb"
    settings = Settings(duckdb_path=str(database_path))
    _seed_minimal_llm_db(database_path)

    fake_runner = CompletedBatchRunner()
    monkeypatch.setattr("app.jobs.get_openai_batch_runner", lambda settings: fake_runner)

    prepared = asyncio.run(
        prepare_openai_batch_range(
            settings,
            code="45920",
            model="gpt-5.4",
            start_date="2026-03-18",
            end_date="2026-03-18",
            output_dir=str(tmp_path / "batch"),
        )
    )
    batch_dir = Path(prepared["batch_dir"])
    (batch_dir / "submission.json").write_text(
        json.dumps({"batch_id": "batch-456", "status": "submitted"}, ensure_ascii=False),
        encoding="utf-8",
    )

    result = asyncio.run(finalize_openai_batch(settings, batch_dir=str(batch_dir)))

    assert result["status"] == "completed"
    assert result["imported_count"] == 1
    assert result["evaluated"] is True
