from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.clients import JQuantsClient, RateLimiter


class FakeOfficialClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, str]]] = []

    def get_mkt_margin_interest(
        self,
        *,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        self.calls.append(
            (
                "get_mkt_margin_interest",
                {
                    "code": code,
                    "from_yyyymmdd": from_yyyymmdd,
                    "to_yyyymmdd": to_yyyymmdd,
                    "date_yyyymmdd": date_yyyymmdd,
                },
            )
        )
        return pd.DataFrame(
            [
                {"Date": "2024-03-15", "Code": "72030", "LongVol": 100},
                {"Date": "2024-03-08", "Code": "72030", "LongVol": 90},
            ]
        )

    def get_mkt_breakdown(
        self,
        *,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        self.calls.append(
            (
                "get_mkt_breakdown",
                {
                    "code": code,
                    "from_yyyymmdd": from_yyyymmdd,
                    "to_yyyymmdd": to_yyyymmdd,
                    "date_yyyymmdd": date_yyyymmdd,
                },
            )
        )
        return pd.DataFrame([{"Date": "2024-03-15", "Code": "72030", "LongBuyVa": 1000.0}])

    def get_mkt_short_ratio(
        self,
        *,
        sector_33_code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        self.calls.append(
            (
                "get_mkt_short_ratio",
                {
                    "sector_33_code": sector_33_code,
                    "from_yyyymmdd": from_yyyymmdd,
                    "to_yyyymmdd": to_yyyymmdd,
                    "date_yyyymmdd": date_yyyymmdd,
                },
            )
        )
        return pd.DataFrame([{"Date": "2024-03-15", "S33": sector_33_code or "3250", "SellExShortVa": 100.0}])

    def get_mkt_margin_alert(
        self,
        *,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        self.calls.append(
            (
                "get_mkt_margin_alert",
                {
                    "code": code,
                    "from_yyyymmdd": from_yyyymmdd,
                    "to_yyyymmdd": to_yyyymmdd,
                    "date_yyyymmdd": date_yyyymmdd,
                },
            )
        )
        return pd.DataFrame([{"PubDate": "2024-03-15", "Code": "72030", "SLRatio": 1.2}])

    def get_mkt_short_sale_report(
        self,
        *,
        code: str = "",
        disclosed_date: str = "",
        disclosed_date_from: str = "",
        disclosed_date_to: str = "",
        calculated_date: str = "",
    ) -> pd.DataFrame:
        self.calls.append(
            (
                "get_mkt_short_sale_report",
                {
                    "code": code,
                    "disclosed_date": disclosed_date,
                    "disclosed_date_from": disclosed_date_from,
                    "disclosed_date_to": disclosed_date_to,
                    "calculated_date": calculated_date,
                },
            )
        )
        return pd.DataFrame([{"DiscDate": "2024-03-15", "CalcDate": "2024-03-14", "Code": "72030", "ShrtPosToSO": 0.6}])

    def get_mkt_calendar(
        self,
        *,
        holiday_division: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        self.calls.append(
            (
                "get_mkt_calendar",
                {
                    "holiday_division": holiday_division,
                    "from_yyyymmdd": from_yyyymmdd,
                    "to_yyyymmdd": to_yyyymmdd,
                },
            )
        )
        return pd.DataFrame([{"Date": "2024-03-20", "HolDiv": holiday_division or "3"}])

    def get_idx_bars_daily_topix(self, *, from_yyyymmdd: str = "", to_yyyymmdd: str = "") -> pd.DataFrame:
        self.calls.append(
            (
                "get_idx_bars_daily_topix",
                {
                    "from_yyyymmdd": from_yyyymmdd,
                    "to_yyyymmdd": to_yyyymmdd,
                },
            )
        )
        return pd.DataFrame([{"Date": "2024-03-15", "C": 2750.0}])

    def get_idx_bars_daily(
        self,
        *,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        self.calls.append(
            (
                "get_idx_bars_daily",
                {
                    "code": code,
                    "from_yyyymmdd": from_yyyymmdd,
                    "to_yyyymmdd": to_yyyymmdd,
                    "date_yyyymmdd": date_yyyymmdd,
                },
            )
        )
        if date_yyyymmdd == "2024-03-16":
            return pd.DataFrame()
        target_date = date_yyyymmdd or to_yyyymmdd or from_yyyymmdd or "2024-03-15"
        return pd.DataFrame([{"Date": target_date, "Code": code or "0047", "C": 1200.0}])

    def get_bulk_list(self, endpoint: str) -> pd.DataFrame:
        self.calls.append(("get_bulk_list", {"endpoint": endpoint}))
        return pd.DataFrame([{"Key": "indices/bars/daily/historical/2024/indices_bars_daily_202403.csv.gz"}])

    def download_bulk(self, key: str, output_path: str) -> None:
        self.calls.append(("download_bulk", {"key": key, "output_path": output_path}))
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_bytes(b"test")

    def get_market_segments(self) -> pd.DataFrame:
        self.calls.append(("get_market_segments", {}))
        return pd.DataFrame([{"Mkt": "0113", "MktNm": "Growth", "MktNmEn": "Growth"}])

    def get_17_sectors(self) -> pd.DataFrame:
        self.calls.append(("get_17_sectors", {}))
        return pd.DataFrame([{"S17": "5", "S17Nm": "医薬品", "S17NmEn": "PHARMACEUTICAL"}])

    def get_33_sectors(self) -> pd.DataFrame:
        self.calls.append(("get_33_sectors", {}))
        return pd.DataFrame(
            [{"S33": "3250", "S33Nm": "医薬品", "S33NmEn": "Pharmaceutical", "S17": "5"}]
        )


def test_margin_interest_wrapper_uses_official_client_and_adds_metadata() -> None:
    official_client = FakeOfficialClient()
    client = JQuantsClient(api_key="test-api-key", official_client=official_client)

    dataframe = client.fetch_margin_interest(code="7203", from_date="2024-03-01", to_date="2024-03-31")

    assert official_client.calls == [
        (
            "get_mkt_margin_interest",
            {
                "code": "7203",
                "from_yyyymmdd": "2024-03-01",
                "to_yyyymmdd": "2024-03-31",
                "date_yyyymmdd": "",
            },
        )
    ]
    assert dataframe["Date"].tolist() == ["2024-03-08", "2024-03-15"]
    assert set(["source_api", "fetched_at"]).issubset(dataframe.columns)
    assert dataframe["source_api"].iloc[0] == "/v2/markets/margin-interest"


def test_market_breakdown_wrapper_uses_official_client_and_adds_metadata() -> None:
    official_client = FakeOfficialClient()
    client = JQuantsClient(api_key="test-api-key", official_client=official_client)

    dataframe = client.fetch_market_breakdown(code="7203", from_date="2024-03-01", to_date="2024-03-31")

    assert official_client.calls == [
        (
            "get_mkt_breakdown",
            {
                "code": "7203",
                "from_yyyymmdd": "2024-03-01",
                "to_yyyymmdd": "2024-03-31",
                "date_yyyymmdd": "",
            },
        )
    ]
    assert dataframe["source_api"].iloc[0] == "/v2/markets/breakdown"


def test_short_ratio_wrapper_uses_official_client_and_adds_metadata() -> None:
    official_client = FakeOfficialClient()
    client = JQuantsClient(api_key="test-api-key", official_client=official_client)

    dataframe = client.fetch_short_ratio(s33="3250", from_date="2024-03-01", to_date="2024-03-31")

    assert official_client.calls == [
        (
            "get_mkt_short_ratio",
            {
                "sector_33_code": "3250",
                "from_yyyymmdd": "2024-03-01",
                "to_yyyymmdd": "2024-03-31",
                "date_yyyymmdd": "",
            },
        )
    ]
    assert dataframe["source_api"].iloc[0] == "/v2/markets/short-ratio"


def test_margin_alert_wrapper_uses_official_client_and_adds_metadata() -> None:
    official_client = FakeOfficialClient()
    client = JQuantsClient(api_key="test-api-key", official_client=official_client)

    dataframe = client.fetch_margin_alert(code="7203", from_date="2024-03-01", to_date="2024-03-31")

    assert official_client.calls == [
        (
            "get_mkt_margin_alert",
            {
                "code": "7203",
                "from_yyyymmdd": "2024-03-01",
                "to_yyyymmdd": "2024-03-31",
                "date_yyyymmdd": "",
            },
        )
    ]
    assert dataframe["source_api"].iloc[0] == "/v2/markets/margin-alert"


def test_short_sale_report_wrapper_uses_official_client_and_adds_metadata() -> None:
    official_client = FakeOfficialClient()
    client = JQuantsClient(api_key="test-api-key", official_client=official_client)

    dataframe = client.fetch_short_sale_report(code="7203", calculated_date="2024-03-14")

    assert official_client.calls == [
        (
            "get_mkt_short_sale_report",
            {
                "code": "7203",
                "disclosed_date": "",
                "disclosed_date_from": "",
                "disclosed_date_to": "",
                "calculated_date": "2024-03-14",
            },
        )
    ]
    assert dataframe["source_api"].iloc[0] == "/v2/markets/short-sale-report"


def test_topix_wrapper_maps_single_date_to_from_and_to() -> None:
    official_client = FakeOfficialClient()
    client = JQuantsClient(api_key="test-api-key", official_client=official_client)

    dataframe = client.fetch_topix_daily_bars(date="2024-03-15")

    assert official_client.calls == [
        (
            "get_idx_bars_daily_topix",
            {
                "from_yyyymmdd": "2024-03-15",
                "to_yyyymmdd": "2024-03-15",
            },
        )
    ]
    assert dataframe["source_api"].iloc[0] == "/v2/indices/bars/daily/topix"


def test_market_calendar_wrapper_supports_official_optional_params() -> None:
    official_client = FakeOfficialClient()
    client = JQuantsClient(api_key="test-api-key", official_client=official_client)

    dataframe = client.fetch_market_calendar(
        holiday_division="3",
        from_date="2024-03-01",
        to_date="2024-03-31",
    )

    assert official_client.calls == [
        (
            "get_mkt_calendar",
            {
                "holiday_division": "3",
                "from_yyyymmdd": "2024-03-01",
                "to_yyyymmdd": "2024-03-31",
            },
        )
    ]
    assert dataframe["Date"].tolist() == ["2024-03-20"]
    assert dataframe["HolDiv"].tolist() == ["3"]
    assert dataframe["source_api"].iloc[0] == "/v2/markets/calendar"


def test_market_calendar_wrapper_allows_empty_filters() -> None:
    official_client = FakeOfficialClient()
    client = JQuantsClient(api_key="test-api-key", official_client=official_client)

    client.fetch_market_calendar()

    assert official_client.calls == [
        (
            "get_mkt_calendar",
            {
                "holiday_division": "",
                "from_yyyymmdd": "",
                "to_yyyymmdd": "",
            },
        )
    ]


def test_index_daily_bar_wrapper_supports_code_scoped_range() -> None:
    official_client = FakeOfficialClient()
    client = JQuantsClient(api_key="test-api-key", official_client=official_client)

    dataframe = client.fetch_index_daily_bars(code="0047", from_date="2024-03-01", to_date="2024-03-31")

    assert official_client.calls == [
        (
            "get_idx_bars_daily",
            {
                "code": "0047",
                "from_yyyymmdd": "2024-03-01",
                "to_yyyymmdd": "2024-03-31",
                "date_yyyymmdd": "",
            },
        )
    ]
    assert dataframe["source_api"].iloc[0] == "/v2/indices/bars/daily"


def test_index_daily_bar_wrapper_passes_range_to_official_client() -> None:
    official_client = FakeOfficialClient()
    client = JQuantsClient(api_key="test-api-key", official_client=official_client)

    dataframe = client.fetch_index_daily_bars(from_date="2024-03-15", to_date="2024-03-17")

    assert official_client.calls == [
        (
            "get_idx_bars_daily",
            {
                "code": "",
                "from_yyyymmdd": "2024-03-15",
                "to_yyyymmdd": "2024-03-17",
                "date_yyyymmdd": "",
            },
        ),
    ]
    assert dataframe["Date"].tolist() == ["2024-03-17"]
    assert set(["source_api", "fetched_at"]).issubset(dataframe.columns)


def test_margin_interest_wrapper_requires_code_or_date() -> None:
    client = JQuantsClient(api_key="test-api-key", official_client=FakeOfficialClient())

    try:
        client.fetch_margin_interest()
    except ValueError as exc:
        assert str(exc) == "Either code or date is required for /v2/markets/margin-interest"
    else:
        raise AssertionError("Expected ValueError when neither code nor date is provided")


def test_market_breakdown_wrapper_requires_code_or_date() -> None:
    client = JQuantsClient(api_key="test-api-key", official_client=FakeOfficialClient())

    try:
        client.fetch_market_breakdown()
    except ValueError as exc:
        assert str(exc) == "Either code or date is required for /v2/markets/breakdown"
    else:
        raise AssertionError("Expected ValueError when neither code nor date is provided")


def test_short_ratio_wrapper_requires_s33_or_date() -> None:
    client = JQuantsClient(api_key="test-api-key", official_client=FakeOfficialClient())

    try:
        client.fetch_short_ratio()
    except ValueError as exc:
        assert str(exc) == "Either s33 or date is required for /v2/markets/short-ratio"
    else:
        raise AssertionError("Expected ValueError when neither s33 nor date is provided")


def test_margin_alert_wrapper_requires_code_or_date() -> None:
    client = JQuantsClient(api_key="test-api-key", official_client=FakeOfficialClient())

    try:
        client.fetch_margin_alert()
    except ValueError as exc:
        assert str(exc) == "Either code or date is required for /v2/markets/margin-alert"
    else:
        raise AssertionError("Expected ValueError when neither code nor date is provided")


def test_short_sale_report_wrapper_requires_some_filter() -> None:
    client = JQuantsClient(api_key="test-api-key", official_client=FakeOfficialClient())

    try:
        client.fetch_short_sale_report()
    except ValueError as exc:
        assert str(exc) == (
            "At least one of code, disclosed_date, disclosed_date_from, disclosed_date_to, or calculated_date is required for /v2/markets/short-sale-report"
        )
    else:
        raise AssertionError("Expected ValueError when no filters are provided")


def test_index_daily_bar_wrapper_requires_code_or_date_when_no_range() -> None:
    client = JQuantsClient(api_key="test-api-key", official_client=FakeOfficialClient())

    try:
        client.fetch_index_daily_bars()
    except ValueError as exc:
        assert str(exc) == "At least one of code, date, from_date, or to_date is required for /v2/indices/bars/daily"
    else:
        raise AssertionError("Expected ValueError when neither code nor date is provided")


def test_bulk_helpers_delegate_to_official_client(tmp_path) -> None:
    official_client = FakeOfficialClient()
    client = JQuantsClient(api_key="test-api-key", official_client=official_client)

    listing = client.fetch_bulk_file_list(endpoint="/indices/bars/daily")
    destination = client.download_bulk_file(
        key="indices/bars/daily/historical/2024/indices_bars_daily_202403.csv.gz",
        output_path=str(tmp_path / "indices_bars_daily_202403.csv.gz"),
    )

    assert listing["Key"].tolist() == ["indices/bars/daily/historical/2024/indices_bars_daily_202403.csv.gz"]
    assert destination.exists()
    assert official_client.calls[0] == ("get_bulk_list", {"endpoint": "/indices/bars/daily"})
    assert official_client.calls[1][0] == "download_bulk"


def test_reference_helpers_delegate_to_official_client() -> None:
    official_client = FakeOfficialClient()
    client = JQuantsClient(api_key="test-api-key", official_client=official_client)

    market_segments = client.fetch_market_segments()
    sector17 = client.fetch_sector17()
    sector33 = client.fetch_sector33()

    assert market_segments["Mkt"].tolist() == ["0113"]
    assert sector17["S17"].tolist() == ["5"]
    assert sector33["S33"].tolist() == ["3250"]
    assert official_client.calls == [
        ("get_market_segments", {}),
        ("get_17_sectors", {}),
        ("get_33_sectors", {}),
    ]


def test_rate_limiter_enforces_minimum_interval(monkeypatch) -> None:
    timeline = {"now": 0.0, "slept": []}

    def fake_monotonic() -> float:
        return timeline["now"]

    def fake_sleep(duration: float) -> None:
        timeline["slept"].append(duration)
        timeline["now"] += duration

    monkeypatch.setattr("app.clients.monotonic", fake_monotonic)
    monkeypatch.setattr("app.clients.sleep", fake_sleep)

    limiter = RateLimiter(rate_limit_per_minute=500)
    limiter.acquire()
    limiter.acquire()

    assert len(timeline["slept"]) == 1
    assert timeline["slept"][0] == 60.0 / 500
