from __future__ import annotations

import asyncio

import httpx

from app.clients import JQuantsClient
from app.models import IndexDailyBarRecord


def test_index_daily_bar_record_alias_mapping() -> None:
    record = IndexDailyBarRecord.model_validate(
        {
            "Date": "2026-03-18",
            "Code": "0028",
            "O": 2718.52,
            "H": 2730.11,
            "L": 2709.84,
            "C": 2726.49,
        }
    )

    assert record.trade_date.isoformat() == "2026-03-18"
    assert record.code == "0028"
    assert record.close_price == 2726.49


def test_jquants_client_fetch_index_daily_bars_handles_pagination() -> None:
    async def run() -> None:
        responses = [
            {
                "data": [
                    {
                        "Date": "2026-03-18",
                        "Code": "0028",
                        "O": 2718.52,
                        "H": 2730.11,
                        "L": 2709.84,
                        "C": 2726.49,
                    }
                ],
                "pagination_key": "next-page",
            },
            {
                "data": [
                    {
                        "Date": "2026-03-19",
                        "Code": "0028",
                        "O": 2726.49,
                        "H": 2741.0,
                        "L": 2720.25,
                        "C": 2738.88,
                    }
                ]
            },
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v2/indices/bars/daily"
            assert request.headers["x-api-key"] == "test-api-key"
            payload = responses.pop(0)
            return httpx.Response(200, json=payload)

        transport = httpx.MockTransport(handler)
        client = JQuantsClient(api_key="test-api-key", base_url="https://api.jquants.com", timeout=5.0)
        original_async_client = httpx.AsyncClient

        def patched_async_client(*args, **kwargs):  # type: ignore[no-untyped-def]
            kwargs["transport"] = transport
            return original_async_client(*args, **kwargs)

        httpx.AsyncClient = patched_async_client  # type: ignore[assignment]
        try:
            records = await client.fetch_index_daily_bars(code="0028", from_date="2026-03-18", to_date="2026-03-19")
        finally:
            httpx.AsyncClient = original_async_client  # type: ignore[assignment]

        assert len(records) == 2
        assert records[0].trade_date.isoformat() == "2026-03-18"
        assert records[1].trade_date.isoformat() == "2026-03-19"

    asyncio.run(run())
