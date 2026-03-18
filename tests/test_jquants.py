from __future__ import annotations

import asyncio

import httpx

from app.clients import JQuantsClient
from app.models import EquityMasterRecord


def test_equity_master_record_alias_mapping() -> None:
    record = EquityMasterRecord.model_validate(
        {
            "Date": "2026-03-18",
            "Code": "86970",
            "CoName": "日本取引所グループ",
            "CoNameEn": "Japan Exchange Group, Inc.",
            "S17": "16",
            "S17Nm": "金融（除く銀行）",
            "S33": "7200",
            "S33Nm": "その他金融業",
            "ScaleCat": "TOPIX Large70",
            "Mkt": "0111",
            "MktNm": "プライム",
            "Mrgn": "1",
            "MrgnNm": "信用",
        }
    )

    assert record.code == "86970"
    assert record.market_name == "プライム"


def test_jquants_client_fetch_equities_master() -> None:
    async def run() -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v2/equities/master"
            assert request.headers["x-api-key"] == "test-api-key"
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "Date": "2026-03-18",
                            "Code": "86970",
                            "CoName": "日本取引所グループ",
                            "CoNameEn": "Japan Exchange Group, Inc.",
                            "S17": "16",
                            "S17Nm": "金融（除く銀行）",
                            "S33": "7200",
                            "S33Nm": "その他金融業",
                            "ScaleCat": "TOPIX Large70",
                            "Mkt": "0111",
                            "MktNm": "プライム",
                            "Mrgn": "1",
                            "MrgnNm": "信用",
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
            records = await client.fetch_equities_master(date="2026-03-18")
        finally:
            httpx.AsyncClient = original_async_client  # type: ignore[assignment]

        assert len(records) == 1
        assert records[0].sector33_code == "7200"

    asyncio.run(run())
