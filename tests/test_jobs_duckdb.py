from __future__ import annotations

import asyncio

import pandas as pd

from app.config import Settings
from app.jobs import (
    export_stock_factor_snapshot,
    run_daily_workflow,
    sync_fins_dividend,
    sync_margin_alert,
    sync_margin_interest,
    sync_market_breakdown,
    sync_market_calendar,
    sync_short_sale_report,
    sync_short_ratio,
)


class FakeJQuantsClient:
    def fetch_market_breakdown(
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
                    "LongBuyVa": 1000.0,
                    "source_api": "/v2/markets/breakdown",
                    "fetched_at": pd.Timestamp("2024-03-18T00:00:00Z"),
                }
            ]
        )

    def fetch_short_ratio(
        self,
        *,
        s33: str | None = None,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> pd.DataFrame:
        assert s33 == "3250"
        assert from_date == "2024-03-01"
        assert to_date == "2024-03-31"
        return pd.DataFrame(
            [
                {
                    "Date": "2024-03-15",
                    "S33": "3250",
                    "SellExShortVa": 100.0,
                    "ShrtWithResVa": 50.0,
                    "ShrtNoResVa": 25.0,
                    "source_api": "/v2/markets/short-ratio",
                    "fetched_at": pd.Timestamp("2024-03-18T00:00:00Z"),
                }
            ]
        )

    def fetch_margin_alert(
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
                    "PubDate": "2024-03-15",
                    "Code": "72030",
                    "AppDate": "2024-03-14",
                    "SLRatio": 1.2,
                    "LongOutChg": "-",
                    "ShrtOutChg": "10",
                    "source_api": "/v2/markets/margin-alert",
                    "fetched_at": pd.Timestamp("2024-03-18T00:00:00Z"),
                }
            ]
        )

    def fetch_short_sale_report(
        self,
        *,
        code: str | None = None,
        disclosed_date: str | None = None,
        disclosed_date_from: str | None = None,
        disclosed_date_to: str | None = None,
        calculated_date: str | None = None,
    ) -> pd.DataFrame:
        assert code == "7203"
        assert calculated_date == "2024-03-14"
        return pd.DataFrame(
            [
                {
                    "DiscDate": "2024-03-15",
                    "CalcDate": "2024-03-14",
                    "Code": "72030",
                    "SSName": "Fund A",
                    "DICName": "",
                    "FundName": "",
                    "ShrtPosToSO": 0.6,
                    "PrevRptRatio": 0.5,
                    "source_api": "/v2/markets/short-sale-report",
                    "fetched_at": pd.Timestamp("2024-03-18T00:00:00Z"),
                }
            ]
        )

    def fetch_market_calendar(
        self,
        *,
        holiday_division: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> pd.DataFrame:
        assert holiday_division == "3"
        assert from_date == "2024-03-19"
        assert to_date == "2024-03-31"
        return pd.DataFrame(
            [
                {
                    "Date": "2024-03-20",
                    "HolDiv": "3",
                    "source_api": "/v2/markets/calendar",
                    "fetched_at": pd.Timestamp("2024-03-19T00:00:00Z"),
                }
            ]
        )

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

    def fetch_fins_dividend(
        self,
        *,
        code: str | None = None,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "PubDate": "2024-03-19",
                    "PubTime": "15:30",
                    "Code": "72030",
                    "RefNo": "2024031900001",
                    "StatCode": "1",
                    "BoardDate": "2024-03-19",
                    "IFCode": "2",
                    "FRCode": "2",
                    "IFTerm": "2024-03",
                    "DivRate": "-",
                    "RecDate": "2024-03-31",
                    "ExDate": "2024-03-28",
                    "ActRecDate": "2024-03-31",
                    "PayDate": "-",
                    "CARefNo": "2024031900001",
                    "DistAmt": "",
                    "RetEarn": "",
                    "DeemDiv": "",
                    "DeemCapGains": "",
                    "NetAssetDecRatio": "",
                    "CommSpecCode": "0",
                    "CommDivRate": "",
                    "SpecDivRate": "",
                    "source_api": "/v2/fins/dividend",
                    "fetched_at": pd.Timestamp("2024-03-19T00:00:00Z"),
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


def test_sync_market_breakdown_writes_to_duckdb(tmp_path, monkeypatch) -> None:
    settings = Settings(
        jquants_api_key="test-api-key",
        duckdb_path=str(tmp_path / "junex.duckdb"),
    )

    monkeypatch.setattr("app.jobs._build_jquants_client", lambda _settings: FakeJQuantsClient())

    upserted = asyncio.run(
        sync_market_breakdown(
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
        'select "Date", "Code", "LongBuyVa" from market_data.market_breakdown'
    ).fetchall()
    connection.close()

    assert result == [("2024-03-15", "72030", 1000.0)]


def test_sync_market_breakdown_migrates_old_schema(tmp_path, monkeypatch) -> None:
    settings = Settings(
        jquants_api_key="test-api-key",
        duckdb_path=str(tmp_path / "junex.duckdb"),
    )

    import duckdb

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    connection.execute(
        """
        create schema if not exists market_data;
        create table market_data.market_breakdown as
        select
            '2024-03-14'::date as "Date",
            '72030'::varchar as "Code",
            1.0::double as "LongSellVa",
            2.0::double as "ShrtNoMrgnVa",
            3.0::double as "MrgnSellNewVa",
            4.0::double as "MrgnSellCloseVa",
            5.0::double as "LongBuyVa",
            6.0::double as "MrgnBuyNewVa",
            7.0::double as "MrgnBuyCloseVa"
        """
    )
    connection.close()

    class BreakdownMigrationClient(FakeJQuantsClient):
        def fetch_market_breakdown(
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
                        "LongSellVa": 10.0,
                        "ShrtNoMrgnVa": 20.0,
                        "MrgnSellNewVa": 30.0,
                        "MrgnSellCloseVa": 40.0,
                        "LongBuyVa": 1000.0,
                        "MrgnBuyNewVa": 60.0,
                        "MrgnBuyCloseVa": 70.0,
                        "LongSellVo": 11.0,
                        "ShrtNoMrgnVo": 21.0,
                        "MrgnSellNewVo": 31.0,
                        "MrgnSellCloseVo": 41.0,
                        "LongBuyVo": 51.0,
                        "MrgnBuyNewVo": 61.0,
                        "MrgnBuyCloseVo": 71.0,
                        "source_api": "/v2/markets/breakdown",
                        "fetched_at": pd.Timestamp("2024-03-18T00:00:00Z"),
                    }
                ]
            )

    monkeypatch.setattr("app.jobs._build_jquants_client", lambda _settings: BreakdownMigrationClient())

    upserted = asyncio.run(
        sync_market_breakdown(
            settings,
            code="7203",
            from_date="2024-03-01",
            to_date="2024-03-31",
        )
    )

    assert upserted == 1

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    describe_rows = connection.execute("describe market_data.market_breakdown").fetchall()
    result = connection.execute(
        'select "Date", "Code", "LongBuyVa", "LongBuyVo", "source_api" from market_data.market_breakdown order by "Date"'
    ).fetchall()
    connection.close()

    columns = [row[0] for row in describe_rows]
    assert "LongBuyVo" in columns
    assert "LongSellVo" in columns
    assert "source_api" in columns
    assert result[0][1:] == ("72030", 5.0, None, None)
    assert result[1][1:] == ("72030", 1000.0, 51.0, "/v2/markets/breakdown")


def test_sync_short_ratio_writes_to_duckdb(tmp_path, monkeypatch) -> None:
    settings = Settings(
        jquants_api_key="test-api-key",
        duckdb_path=str(tmp_path / "junex.duckdb"),
    )

    monkeypatch.setattr("app.jobs._build_jquants_client", lambda _settings: FakeJQuantsClient())

    upserted = asyncio.run(
        sync_short_ratio(
            settings,
            s33="3250",
            from_date="2024-03-01",
            to_date="2024-03-31",
        )
    )

    assert upserted == 1

    import duckdb

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    result = connection.execute(
        'select "Date", "S33", "SellExShortVa" from market_data.short_ratio'
    ).fetchall()
    connection.close()

    assert result == [("2024-03-15", "3250", 100.0)]


def test_sync_short_ratio_migrates_old_schema(tmp_path, monkeypatch) -> None:
    settings = Settings(
        jquants_api_key="test-api-key",
        duckdb_path=str(tmp_path / "junex.duckdb"),
    )

    import duckdb

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    connection.execute(
        """
        create schema if not exists market_data;
        create table market_data.short_ratio as
        select
            '2024-03-14'::date as "Date",
            '3250'::varchar as "S33",
            1.0::double as "SellExShortVa",
            2.0::double as "ShrtWithResVa",
            3.0::double as "ShrtNoResVa"
        """
    )
    connection.close()

    class ShortRatioMigrationClient(FakeJQuantsClient):
        def fetch_short_ratio(
            self,
            *,
            s33: str | None = None,
            date: str | None = None,
            from_date: str | None = None,
            to_date: str | None = None,
        ) -> pd.DataFrame:
            assert s33 == "3250"
            assert from_date == "2024-03-01"
            assert to_date == "2024-03-31"
            return pd.DataFrame(
                [
                    {
                        "Date": "2024-03-15",
                        "S33": "3250",
                        "SellExShortVa": 100.0,
                        "ShrtWithResVa": 50.0,
                        "ShrtNoResVa": 25.0,
                        "source_api": "/v2/markets/short-ratio",
                        "fetched_at": pd.Timestamp("2024-03-18T00:00:00Z"),
                    }
                ]
            )

    monkeypatch.setattr("app.jobs._build_jquants_client", lambda _settings: ShortRatioMigrationClient())

    upserted = asyncio.run(
        sync_short_ratio(
            settings,
            s33="3250",
            from_date="2024-03-01",
            to_date="2024-03-31",
        )
    )

    assert upserted == 1

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    describe_rows = connection.execute("describe market_data.short_ratio").fetchall()
    result = connection.execute(
        'select "S33", "SellExShortVa", "source_api" from market_data.short_ratio order by "Date"'
    ).fetchall()
    connection.close()

    columns = [row[0] for row in describe_rows]
    assert "source_api" in columns
    assert "fetched_at" in columns
    assert result == [
        ("3250", 1.0, None),
        ("3250", 100.0, "/v2/markets/short-ratio"),
    ]


def test_sync_margin_alert_writes_to_duckdb(tmp_path, monkeypatch) -> None:
    settings = Settings(
        jquants_api_key="test-api-key",
        duckdb_path=str(tmp_path / "junex.duckdb"),
    )

    monkeypatch.setattr("app.jobs._build_jquants_client", lambda _settings: FakeJQuantsClient())

    upserted = asyncio.run(
        sync_margin_alert(
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
        'select "PubDate", "Code", "SLRatio", "LongOutChg" from market_data.margin_alert'
    ).fetchall()
    connection.close()

    assert result == [("2024-03-15", "72030", 1.2, "-")]


def test_sync_margin_alert_migrates_old_schema_and_code_type(tmp_path, monkeypatch) -> None:
    settings = Settings(
        jquants_api_key="test-api-key",
        duckdb_path=str(tmp_path / "junex.duckdb"),
    )

    import duckdb

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    connection.execute(
        """
        create schema if not exists market_data;
        create table market_data.margin_alert as
        select
            '2024-03-14'::varchar as "PubDate",
            72030::integer as "Code",
            '2024-03-13'::varchar as "AppDate",
            1.0::double as "SLRatio",
            '-'::varchar as "LongOutChg",
            '1'::varchar as "ShrtOutChg"
        """
    )
    connection.close()

    class MarginAlertMigrationClient(FakeJQuantsClient):
        def fetch_margin_alert(
            self,
            *,
            code: str | None = None,
            date: str | None = None,
            from_date: str | None = None,
            to_date: str | None = None,
        ) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "PubDate": "2024-03-15",
                        "Code": "153A0",
                        "AppDate": "2024-03-14",
                        "SLRatio": 1.2,
                        "LongOutChg": "-",
                        "ShrtOutChg": "10",
                        "source_api": "/v2/markets/margin-alert",
                        "fetched_at": pd.Timestamp("2024-03-18T00:00:00Z"),
                    }
                ]
            )

    monkeypatch.setattr("app.jobs._build_jquants_client", lambda _settings: MarginAlertMigrationClient())

    upserted = asyncio.run(sync_margin_alert(settings, date="2024-03-15"))

    assert upserted == 1

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    describe_rows = connection.execute("describe market_data.margin_alert").fetchall()
    result = connection.execute(
        'select "Code", "SLRatio", "source_api" from market_data.margin_alert order by "PubDate", "Code"'
    ).fetchall()
    connection.close()

    types = {row[0]: row[1] for row in describe_rows}
    assert types["Code"] == "VARCHAR"
    assert "source_api" in types
    assert result == [
        ("72030", 1.0, None),
        ("153A0", 1.2, "/v2/markets/margin-alert"),
    ]


def test_sync_short_sale_report_writes_to_duckdb(tmp_path, monkeypatch) -> None:
    settings = Settings(
        jquants_api_key="test-api-key",
        duckdb_path=str(tmp_path / "junex.duckdb"),
    )

    monkeypatch.setattr("app.jobs._build_jquants_client", lambda _settings: FakeJQuantsClient())

    upserted = asyncio.run(
        sync_short_sale_report(
            settings,
            code="7203",
            calculated_date="2024-03-14",
        )
    )

    assert upserted == 1

    import duckdb

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    result = connection.execute(
        'select "DiscDate", "CalcDate", "Code", "ShrtPosToSO" from market_data.short_sale_report'
    ).fetchall()
    connection.close()

    assert result == [("2024-03-15", "2024-03-14", "72030", 0.6)]


def test_sync_market_calendar_supports_optional_filters(tmp_path, monkeypatch) -> None:
    settings = Settings(
        jquants_api_key="test-api-key",
        duckdb_path=str(tmp_path / "junex.duckdb"),
    )

    monkeypatch.setattr("app.jobs._build_jquants_client", lambda _settings: FakeJQuantsClient())

    upserted = asyncio.run(
        sync_market_calendar(
            settings,
            holiday_division="3",
            from_date="2024-03-19",
            to_date="2024-03-31",
        )
    )

    assert upserted == 1

    import duckdb

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    result = connection.execute(
        'select "Date", "HolDiv" from market_data.market_calendar'
    ).fetchall()
    connection.close()

    assert result == [("2024-03-20", "3")]


def test_sync_fins_dividend_migrates_mixed_type_columns_to_varchar(tmp_path, monkeypatch) -> None:
    settings = Settings(
        jquants_api_key="test-api-key",
        duckdb_path=str(tmp_path / "junex.duckdb"),
    )

    import duckdb

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    connection.execute(
        """
        create schema if not exists market_data;
        create table market_data.fin_dividend as
        select
            '2024-03-18'::varchar as PubDate,
            '15:30'::varchar as PubTime,
            '72030'::varchar as Code,
            '2024031800001'::varchar as RefNo,
            10.5::double as DivRate
        """
    )
    connection.close()

    monkeypatch.setattr("app.jobs._build_jquants_client", lambda _settings: FakeJQuantsClient())

    upserted = asyncio.run(sync_fins_dividend(settings, date="2024-03-19"))

    assert upserted == 1

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    describe_rows = connection.execute("describe market_data.fin_dividend").fetchall()
    result = connection.execute(
        'select "RefNo", "DivRate" from market_data.fin_dividend order by "RefNo"'
    ).fetchall()
    connection.close()

    types = {row[0]: row[1] for row in describe_rows}
    assert types["DivRate"] == "VARCHAR"
    assert result == [("2024031800001", "10.5"), ("2024031900001", "-")]


def test_run_daily_workflow_runs_expected_steps(monkeypatch) -> None:
    settings = Settings(jquants_api_key="test-api-key", duckdb_path=":memory:")
    calls: list[tuple[str, str | None]] = []

    async def fake_sync_equity_master(_settings, *, code=None, date=None):
        calls.append(("equity_master", date))
        return 1

    async def fake_sync_equity_daily_bars(_settings, *, code=None, date=None, from_date=None, to_date=None):
        calls.append(("equity_daily_bar", date))
        return 2

    async def fake_sync_topix_daily_bars(_settings, *, date=None, from_date=None, to_date=None):
        calls.append(("topix_daily_bar", date))
        return 3

    async def fake_sync_index_daily_bars(_settings, *, code=None, date=None, from_date=None, to_date=None):
        calls.append(("index_daily_bar", date))
        return 4

    async def fake_sync_fins_summary(_settings, *, code=None, date=None, from_date=None, to_date=None):
        calls.append(("fin_summary", date))
        return 5

    async def fake_sync_fins_dividend(_settings, *, code=None, date=None, from_date=None, to_date=None):
        calls.append(("fin_dividend", date))
        return 6

    async def fake_sync_margin_interest(_settings, *, code=None, date=None, from_date=None, to_date=None):
        calls.append(("margin_interest", date))
        return 7

    async def fake_sync_market_breakdown(_settings, *, code=None, date=None, from_date=None, to_date=None):
        calls.append(("market_breakdown", date))
        return 12

    async def fake_sync_short_ratio(_settings, *, s33=None, date=None, from_date=None, to_date=None):
        calls.append(("short_ratio", date))
        return 13

    async def fake_sync_margin_alert(_settings, *, code=None, date=None, from_date=None, to_date=None):
        calls.append(("margin_alert", date))
        return 14

    async def fake_sync_short_sale_report(
        _settings,
        *,
        code=None,
        disclosed_date=None,
        disclosed_date_from=None,
        disclosed_date_to=None,
        calculated_date=None,
    ):
        calls.append(("short_sale_report", calculated_date))
        return 15

    async def fake_sync_earnings_calendar(_settings):
        calls.append(("earnings_calendar", None))
        return 8

    async def fake_sync_market_calendar(_settings, *, holiday_division=None, from_date=None, to_date=None):
        calls.append(("market_calendar", from_date))
        assert from_date == "2026-03-20"
        assert to_date == "2026-03-20"
        return 9

    async def fake_build_price_action_feature_table(_settings):
        calls.append(("price_action_features", None))
        return 10

    async def fake_build_market_industry_linkage_feature_table(_settings):
        calls.append(("market_industry_linkage_features", None))
        return 11

    async def fake_build_flow_structure_feature_table(_settings):
        calls.append(("flow_structure_features", None))
        return 16

    async def fake_build_fundamental_event_feature_table(_settings):
        calls.append(("fundamental_event_features", None))
        return 17

    async def fake_build_next_day_label_table(_settings):
        calls.append(("next_day_labels", None))
        return 18

    monkeypatch.setattr("app.jobs.sync_equity_master", fake_sync_equity_master)
    monkeypatch.setattr("app.jobs.sync_equity_daily_bars", fake_sync_equity_daily_bars)
    monkeypatch.setattr("app.jobs.sync_topix_daily_bars", fake_sync_topix_daily_bars)
    monkeypatch.setattr("app.jobs.sync_index_daily_bars", fake_sync_index_daily_bars)
    monkeypatch.setattr("app.jobs.sync_fins_summary", fake_sync_fins_summary)
    monkeypatch.setattr("app.jobs.sync_fins_dividend", fake_sync_fins_dividend)
    monkeypatch.setattr("app.jobs.sync_margin_interest", fake_sync_margin_interest)
    monkeypatch.setattr("app.jobs.sync_market_breakdown", fake_sync_market_breakdown)
    monkeypatch.setattr("app.jobs.sync_short_ratio", fake_sync_short_ratio)
    monkeypatch.setattr("app.jobs.sync_margin_alert", fake_sync_margin_alert)
    monkeypatch.setattr("app.jobs.sync_short_sale_report", fake_sync_short_sale_report)
    monkeypatch.setattr("app.jobs.sync_earnings_calendar", fake_sync_earnings_calendar)
    monkeypatch.setattr("app.jobs.sync_market_calendar", fake_sync_market_calendar)
    monkeypatch.setattr("app.jobs.build_price_action_feature_table", fake_build_price_action_feature_table)
    monkeypatch.setattr(
        "app.jobs.build_market_industry_linkage_feature_table",
        fake_build_market_industry_linkage_feature_table,
    )
    monkeypatch.setattr(
        "app.jobs.build_flow_structure_feature_table",
        fake_build_flow_structure_feature_table,
    )
    monkeypatch.setattr(
        "app.jobs.build_fundamental_event_feature_table",
        fake_build_fundamental_event_feature_table,
    )
    monkeypatch.setattr(
        "app.jobs.build_next_day_label_table",
        fake_build_next_day_label_table,
    )

    result = asyncio.run(run_daily_workflow(settings, target_date="2026-03-20"))

    assert result == {
        "equity_master": 1,
        "equity_daily_bar": 2,
        "topix_daily_bar": 3,
        "index_daily_bar": 4,
        "fin_summary": 5,
        "fin_dividend": 6,
        "margin_interest": 7,
        "market_breakdown": 12,
        "short_ratio": 13,
        "margin_alert": 14,
        "short_sale_report": 15,
        "earnings_calendar": 8,
        "market_calendar": 9,
        "price_action_features": 10,
        "market_industry_linkage_features": 11,
        "flow_structure_features": 16,
        "fundamental_event_features": 17,
        "next_day_labels": 18,
    }
    assert calls == [
        ("equity_master", "2026-03-20"),
        ("equity_daily_bar", "2026-03-20"),
        ("topix_daily_bar", "2026-03-20"),
        ("index_daily_bar", "2026-03-20"),
        ("fin_summary", "2026-03-20"),
        ("fin_dividend", "2026-03-20"),
        ("margin_interest", "2026-03-20"),
        ("market_breakdown", "2026-03-20"),
        ("short_ratio", "2026-03-20"),
        ("margin_alert", "2026-03-20"),
        ("short_sale_report", "2026-03-20"),
        ("earnings_calendar", None),
        ("market_calendar", "2026-03-20"),
        ("price_action_features", None),
        ("market_industry_linkage_features", None),
        ("flow_structure_features", None),
        ("fundamental_event_features", None),
        ("next_day_labels", None),
    ]


def test_export_stock_factor_snapshot_collects_all_factor_tables(tmp_path) -> None:
    settings = Settings(duckdb_path=str(tmp_path / "junex.duckdb"))

    import duckdb

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    connection.execute(
        """
        create schema analytics;
        create schema market_data;
        create table analytics.price_action_features as
        select date '2026-03-19' as trade_date, '45920' as code, 0.1::double as ret_1d;
        create table analytics.market_industry_linkage_features as
        select date '2026-03-19' as trade_date, '45920' as code, 0.8::double as industry_strength_pct_1d;
        create table analytics.flow_structure_features as
        select date '2026-03-19' as trade_date, '45920' as code, 100.0::double as net_cash_va;
        create table analytics.fundamental_event_features as
        select date '2026-03-19' as trade_date, '45920' as code, 1 as is_earnings_day;
        create table market_data.market_calendar as
        select date '2026-03-19' as "Date", '1' as "HolDiv";
        """
    )
    connection.close()

    payload = asyncio.run(export_stock_factor_snapshot(settings, code="45920", target_date="2026-03-19"))

    assert payload["code"] == "45920"
    assert payload["requested_date"] == "2026-03-19"
    assert payload["status"] == "ok"
    assert payload["trade_date"] == "2026-03-19"
    factor_tables = payload["factor_tables"]
    assert factor_tables["price_action_features"]["ret_1d"] == 0.1
    assert factor_tables["market_industry_linkage_features"]["industry_strength_pct_1d"] == 0.8
    assert factor_tables["flow_structure_features"]["net_cash_va"] == 100.0
    assert factor_tables["fundamental_event_features"]["is_earnings_day"] == 1


def test_export_stock_factor_snapshot_returns_not_trading_day_status(tmp_path) -> None:
    settings = Settings(duckdb_path=str(tmp_path / "junex.duckdb"))

    import duckdb

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    connection.execute(
        """
        create schema analytics;
        create schema market_data;
        create table analytics.price_action_features as
        select date '2026-03-19' as trade_date, '45920' as code, 0.1::double as ret_1d;
        create table analytics.market_industry_linkage_features as
        select date '2026-03-19' as trade_date, '45920' as code, 0.8::double as industry_strength_pct_1d;
        create table analytics.flow_structure_features as
        select date '2026-03-19' as trade_date, '45920' as code, 100.0::double as net_cash_va;
        create table analytics.fundamental_event_features as
        select date '2026-03-19' as trade_date, '45920' as code, 1 as is_earnings_day;
        create table market_data.market_calendar as
        select date '2026-03-21' as "Date", '0' as "HolDiv";
        """
    )
    connection.close()

    payload = asyncio.run(export_stock_factor_snapshot(settings, code="45920", target_date="2026-03-21"))

    assert payload["requested_date"] == "2026-03-21"
    assert payload["trade_date"] == "2026-03-21"
    assert payload["status"] == "not_trading_day"
    assert payload["factor_tables"] == {}


def test_export_stock_factor_snapshot_returns_no_data_status(tmp_path) -> None:
    settings = Settings(duckdb_path=str(tmp_path / "junex.duckdb"))

    import duckdb

    connection = duckdb.connect(str(tmp_path / "junex.duckdb"))
    connection.execute(
        """
        create schema analytics;
        create schema market_data;
        create table analytics.price_action_features as
        select date '2026-03-19' as trade_date, '45920' as code, 0.1::double as ret_1d;
        create table analytics.market_industry_linkage_features as
        select date '2026-03-19' as trade_date, '45920' as code, 0.8::double as industry_strength_pct_1d;
        create table analytics.flow_structure_features as
        select date '2026-03-19' as trade_date, '45920' as code, 100.0::double as net_cash_va;
        create table analytics.fundamental_event_features as
        select date '2026-03-19' as trade_date, '45920' as code, 1 as is_earnings_day;
        create table market_data.market_calendar as
        select date '2026-03-18' as "Date", '1' as "HolDiv"
        union all
        select date '2026-03-20' as "Date", '1' as "HolDiv";
        """
    )
    connection.close()

    payload = asyncio.run(export_stock_factor_snapshot(settings, code="45920", target_date="2026-03-20"))

    assert payload["requested_date"] == "2026-03-20"
    assert payload["trade_date"] == "2026-03-20"
    assert payload["status"] == "no_data"
    assert payload["factor_tables"] == {
        "price_action_features": None,
        "market_industry_linkage_features": None,
        "flow_structure_features": None,
        "fundamental_event_features": None,
    }
