from __future__ import annotations

import asyncio

import httpx

from app.clients import JQuantsClient
from app.models import EarningsCalendarRecord


def test_earnings_calendar_record_alias_mapping() -> None:
    record = EarningsCalendarRecord.model_validate(
        {
            "Date": "2026-02-16",
            "Code": "43760",
            "CoName": "くふうカンパニーホールディングス",
            "FY": "9月30日",
            "SectorNm": "情報・通信業",
            "FQ": "第１四半期",
            "Section": "グロース",
        }
    )

    assert record.code == "43760"
    assert record.fiscal_quarter == "第１四半期"


def test_jquants_client_fetch_earnings_calendar() -> None:
    async def run() -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v2/equities/earnings-calendar"
            assert request.url.params["date"] == "2026-02-16"
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "Date": "2026-02-16",
                            "Code": "43760",
                            "CoName": "くふうカンパニーホールディングス",
                            "FY": "9月30日",
                            "SectorNm": "情報・通信業",
                            "FQ": "第１四半期",
                            "Section": "グロース",
                        }
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
            records = await client.fetch_earnings_calendar(date="2026-02-16")
        finally:
            httpx.AsyncClient = original_async_client  # type: ignore[assignment]

        assert len(records) == 1
        assert records[0].company_name == "くふうカンパニーホールディングス"

    asyncio.run(run())
