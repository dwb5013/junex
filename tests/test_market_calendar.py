from __future__ import annotations

import asyncio

import httpx

from app.clients import JQuantsClient
from app.models import MarketCalendarRecord


def test_market_calendar_record_alias_mapping() -> None:
    record = MarketCalendarRecord.model_validate(
        {
            "Date": "2026-03-20",
            "HolDiv": "3",
        }
    )

    assert record.trade_date.isoformat() == "2026-03-20"
    assert record.holiday_division == "3"


def test_jquants_client_fetch_market_calendar() -> None:
    async def run() -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v2/markets/calendar"
            assert request.url.params["from"] == "2026-03-01"
            assert request.url.params["to"] == "2026-03-31"
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"Date": "2026-03-01", "HolDiv": "0"},
                        {"Date": "2026-03-02", "HolDiv": "1"},
                    ]
                },
            )

        transport = httpx.MockTransport(handler)
        client = JQuantsClient(api_key="test-api-key", base_url="https://api.jquants.com", timeout=5.0)
        original_async_client = httpx.AsyncClient

        def patched_async_client(*args, **kwargs):  # type: ignore[no-untyped-def]
            kwargs["transport"] = transport
            return original_async_client(*args, **kwargs)

        httpx.AsyncClient = patched_async_client  # type: ignore[assignment]
        try:
            records = await client.fetch_market_calendar(from_date="2026-03-01", to_date="2026-03-31")
        finally:
            httpx.AsyncClient = original_async_client  # type: ignore[assignment]

        assert len(records) == 2
        assert records[0].holiday_division == "0"
        assert records[1].holiday_division == "1"

    asyncio.run(run())
