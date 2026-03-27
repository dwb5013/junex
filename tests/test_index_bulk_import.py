from __future__ import annotations

import asyncio
import gzip
from pathlib import Path

import pandas as pd

from app.config import Settings
from app.jobs import _select_bulk_keys, sync_index_daily_bars


class FakeBulkJQuantsClient:
    def __init__(self) -> None:
        self.downloaded_keys: list[str] = []

    def fetch_bulk_file_list(self, *, endpoint: str) -> pd.DataFrame:
        assert endpoint == "/indices/bars/daily"
        return pd.DataFrame(
            [
                {"Key": "indices/bars/daily/historical/2024/indices_bars_daily_202402.csv.gz"},
                {"Key": "indices/bars/daily/historical/2024/indices_bars_daily_202403.csv.gz"},
                {"Key": "indices/bars/daily/historical/2024/indices_bars_daily_202404.csv.gz"},
            ]
        )

    def download_bulk_file(self, *, key: str, output_path: str) -> Path:
        self.downloaded_keys.append(key)
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = "Date,Code,O,H,L,C\n2024-03-15,0047,1,2,0.5,1.5\n2024-03-18,0047,1.1,2.1,0.6,1.6\n"
        with gzip.open(path, mode="wt", encoding="utf-8") as handle:
            handle.write(rows)
        return path


def test_select_bulk_keys_filters_months() -> None:
    file_list = pd.DataFrame(
        [
            {"Key": "indices/bars/daily/historical/2024/indices_bars_daily_202402.csv.gz"},
            {"Key": "indices/bars/daily/historical/2024/indices_bars_daily_202403.csv.gz"},
            {"Key": "indices/bars/daily/historical/2024/indices_bars_daily_202404.csv.gz"},
        ]
    )

    assert _select_bulk_keys(file_list=file_list, start_date="2024-03-01", end_date="2024-03-31") == [
        "indices/bars/daily/historical/2024/indices_bars_daily_202403.csv.gz"
    ]


def test_sync_index_daily_bars_uses_bulk_files(tmp_path, monkeypatch) -> None:
    settings = Settings(
        jquants_api_key="test-api-key",
        duckdb_path=str(tmp_path / "junex.duckdb"),
        jquants_bulk_download_dir=str(tmp_path / "bulk"),
    )

    fake_client = FakeBulkJQuantsClient()
    monkeypatch.setattr("app.jobs._build_jquants_client", lambda _settings: fake_client)

    upserted = asyncio.run(sync_index_daily_bars(settings, date="2024-03-18"))

    assert upserted == 2
    assert fake_client.downloaded_keys == ["indices/bars/daily/historical/2024/indices_bars_daily_202403.csv.gz"]

    import duckdb

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    result = connection.execute(
        'select "Date", "Code", "C" from market_data.index_daily_bar order by "Date", "Code"'
    ).fetchall()
    connection.close()

    assert result == [("2024-03-15", "0047", 1.5), ("2024-03-18", "0047", 1.6)]
