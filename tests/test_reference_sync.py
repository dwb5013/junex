from __future__ import annotations

import asyncio

import pandas as pd

from app.config import Settings
from app.jobs import sync_reference_market_segments, sync_reference_sector17, sync_reference_sector33


class FakeReferenceClient:
    def fetch_market_segments(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "Mkt": "0113",
                    "MktNm": "Growth",
                    "MktNmEn": "Growth",
                    "source_api": "official_client:get_market_segments",
                    "fetched_at": pd.Timestamp("2026-03-19T00:00:00Z"),
                }
            ]
        )

    def fetch_sector17(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "S17": "5",
                    "S17Nm": "医薬品",
                    "S17NmEn": "PHARMACEUTICAL",
                    "source_api": "official_client:get_17_sectors",
                    "fetched_at": pd.Timestamp("2026-03-19T00:00:00Z"),
                }
            ]
        )

    def fetch_sector33(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "S33": "3250",
                    "S33Nm": "医薬品",
                    "S33NmEn": "Pharmaceutical",
                    "S17": "5",
                    "source_api": "official_client:get_33_sectors",
                    "fetched_at": pd.Timestamp("2026-03-19T00:00:00Z"),
                }
            ]
        )


def test_reference_sync_jobs_write_reference_tables(tmp_path, monkeypatch) -> None:
    settings = Settings(
        jquants_api_key="test-api-key",
        duckdb_path=str(tmp_path / "junex.duckdb"),
    )
    monkeypatch.setattr("app.jobs._build_jquants_client", lambda _settings: FakeReferenceClient())

    asyncio.run(sync_reference_market_segments(settings))
    asyncio.run(sync_reference_sector17(settings))
    asyncio.run(sync_reference_sector33(settings))

    import duckdb

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    market_rows = connection.execute('select "Mkt", "MktNm" from reference.market_segments').fetchall()
    sector17_rows = connection.execute('select "S17", "S17Nm" from reference.sector17').fetchall()
    sector33_rows = connection.execute('select "S33", "S33Nm", "S17" from reference.sector33').fetchall()
    connection.close()

    assert market_rows == [("0113", "Growth")]
    assert sector17_rows == [("5", "医薬品")]
    assert sector33_rows == [("3250", "医薬品", "5")]
