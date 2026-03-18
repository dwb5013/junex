from __future__ import annotations

import pandas as pd

from app.db import DuckDBRepository


def test_duckdb_repository_upsert_table(tmp_path) -> None:
    repository = DuckDBRepository(str(tmp_path / "junex.duckdb"))

    first_batch = pd.DataFrame(
        [
            {"Date": "2024-03-15", "Code": "72030", "LongVol": 100, "source_api": "/v2/markets/margin-interest"},
            {"Date": "2024-03-22", "Code": "72030", "LongVol": 110, "source_api": "/v2/markets/margin-interest"},
        ]
    )
    second_batch = pd.DataFrame(
        [
            {"Date": "2024-03-22", "Code": "72030", "LongVol": 999, "source_api": "/v2/markets/margin-interest"},
        ]
    )

    assert repository.upsert_table("market_data.margin_interest", first_batch, key_columns=["Date", "Code"]) == 2
    assert repository.upsert_table("market_data.margin_interest", second_batch, key_columns=["Date", "Code"]) == 1

    result = repository.query(
        'select "Date", "Code", "LongVol" from market_data.margin_interest order by "Date", "Code"'
    )

    assert result.to_dict(orient="records") == [
        {"Date": "2024-03-15", "Code": "72030", "LongVol": 100},
        {"Date": "2024-03-22", "Code": "72030", "LongVol": 999},
    ]


def test_duckdb_repository_replace_table(tmp_path) -> None:
    repository = DuckDBRepository(str(tmp_path / "junex.duckdb"))

    summary = pd.DataFrame(
        [
            {"category": "orders", "value": 15.0},
            {"category": "revenue", "value": 20.0},
        ]
    )

    assert repository.replace_table("analytics.summary", summary) == 2

    result = repository.query("select category, value from analytics.summary order by category")
    assert result.to_dict(orient="records") == [
        {"category": "orders", "value": 15.0},
        {"category": "revenue", "value": 20.0},
    ]
