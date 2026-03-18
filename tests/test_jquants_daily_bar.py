from __future__ import annotations

import asyncio

import httpx

from app.clients import JQuantsClient
from app.models import EquityDailyBarRecord


def test_equity_daily_bar_record_alias_mapping() -> None:
    record = EquityDailyBarRecord.model_validate(
        {
            "Date": "2026-03-18",
            "Code": "45920",
            "O": 101.0,
            "H": 105.0,
            "L": 99.5,
            "C": 104.2,
            "UL": "0",
            "LL": "0",
            "Vo": 100000.0,
            "Va": 10300000.0,
            "AdjFactor": 1.0,
            "AdjO": 101.0,
            "AdjH": 105.0,
            "AdjL": 99.5,
            "AdjC": 104.2,
            "AdjVo": 100000.0,
        }
    )

    assert record.code == "45920"
    assert record.close_price == 104.2


def test_jquants_client_fetch_equity_daily_bars_handles_pagination() -> None:
    async def run() -> None:
        responses = [
            {
                "data": [
                    {
                        "Date": "2026-03-18",
                        "Code": "45920",
                        "O": 101.0,
                        "H": 105.0,
                        "L": 99.5,
                        "C": 104.2,
                        "UL": "0",
                        "LL": "0",
                        "Vo": 100000.0,
                        "Va": 10300000.0,
                        "AdjFactor": 1.0,
                        "AdjO": 101.0,
                        "AdjH": 105.0,
                        "AdjL": 99.5,
                        "AdjC": 104.2,
                        "AdjVo": 100000.0,
                    }
                ],
                "pagination_key": "next-page",
            },
            {
                "data": [
                    {
                        "Date": "2026-03-19",
                        "Code": "45920",
                        "O": 104.5,
                        "H": 106.0,
                        "L": 103.0,
                        "C": 105.0,
                        "UL": "0",
                        "LL": "0",
                        "Vo": 110000.0,
                        "Va": 11500000.0,
                        "AdjFactor": 1.0,
                        "AdjO": 104.5,
                        "AdjH": 106.0,
                        "AdjL": 103.0,
                        "AdjC": 105.0,
                        "AdjVo": 110000.0,
                    }
                ]
            },
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v2/equities/bars/daily"
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
            records = await client.fetch_equity_daily_bars(code="4592", from_date="2026-03-18", to_date="2026-03-19")
        finally:
            httpx.AsyncClient = original_async_client  # type: ignore[assignment]

        assert len(records) == 2
        assert records[0].trade_date.isoformat() == "2026-03-18"
        assert records[1].trade_date.isoformat() == "2026-03-19"

    asyncio.run(run())
