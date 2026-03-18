from __future__ import annotations

import asyncio

import pandas as pd

from app.config import Settings
from app.jobs import sync_margin_interest


class FakeJQuantsClient:
    def fetch_margin_interest(
        self,
        *,
        code: str | None = None,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> pd.DataFrame:
        assert code == "7203"
        assert from_date == "2024-03-01"
        assert to_date == "2024-03-31"
        return pd.DataFrame(
            [
                {
                    "Date": "2024-03-15",
                    "Code": "72030",
                    "LongVol": 100,
                    "source_api": "/v2/markets/margin-interest",
                    "fetched_at": pd.Timestamp("2024-03-18T00:00:00Z"),
                }
            ]
        )


def test_sync_margin_interest_writes_to_duckdb(tmp_path, monkeypatch) -> None:
    settings = Settings(
        jquants_api_key="test-api-key",
        duckdb_path=str(tmp_path / "junex.duckdb"),
    )

    monkeypatch.setattr("app.jobs._build_jquants_client", lambda _settings: FakeJQuantsClient())

    upserted = asyncio.run(
        sync_margin_interest(
            settings,
            code="7203",
            from_date="2024-03-01",
            to_date="2024-03-31",
        )
    )

    assert upserted == 1

    import duckdb

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    result = connection.execute(
        'select "Date", "Code", "LongVol" from market_data.margin_interest'
    ).fetchall()
    connection.close()

    assert result == [("2024-03-15", "72030", 100)]
