from __future__ import annotations

import asyncio
from decimal import Decimal

import httpx

from app.clients import JQuantsClient
from app.models import FinsSummaryRecord


def test_fins_summary_record_alias_mapping() -> None:
    record = FinsSummaryRecord.model_validate(
        {
            "DiscDate": "2025-03-17",
            "DiscTime": "15:30:00",
            "Code": "45920",
            "DiscNo": "20250317595034",
            "DocType": "FYFinancialStatements_Consolidated_JP",
            "CurPerType": "FY",
            "CurPerSt": "2024-02-01",
            "CurPerEn": "2025-01-31",
            "Sales": "",
            "OP": "-3516000000",
            "EPS": "-41.86",
            "MatChgSub": "",
            "ChgByASRev": "true",
        }
    )

    assert record.disclosure_number == "20250317595034"
    assert record.sales is None
    assert record.operating_profit == Decimal("-3516000000")
    assert record.change_by_accounting_standard_revision is True


def test_jquants_client_fetch_fins_summary() -> None:
    async def run() -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v2/fins/summary"
            assert request.url.params["code"] == "4592"
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "DiscDate": "2025-03-17",
                            "DiscTime": "15:30:00",
                            "Code": "45920",
                            "DiscNo": "20250317595034",
                            "DocType": "FYFinancialStatements_Consolidated_JP",
                            "CurPerType": "FY",
                            "CurPerSt": "2024-02-01",
                            "CurPerEn": "2025-01-31",
                            "OP": "-3516000000",
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
            records = await client.fetch_fins_summary(code="4592")
        finally:
            httpx.AsyncClient = original_async_client  # type: ignore[assignment]

        assert len(records) == 1
        assert records[0].code == "45920"

    asyncio.run(run())
