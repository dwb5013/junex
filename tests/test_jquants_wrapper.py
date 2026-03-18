from __future__ import annotations

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


def test_margin_interest_wrapper_requires_code_or_date() -> None:
    client = JQuantsClient(api_key="test-api-key", official_client=FakeOfficialClient())

    try:
        client.fetch_margin_interest()
    except ValueError as exc:
        assert str(exc) == "Either code or date is required for /v2/markets/margin-interest"
    else:
        raise AssertionError("Expected ValueError when neither code nor date is provided")


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
