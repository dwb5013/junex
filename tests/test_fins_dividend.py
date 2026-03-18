from __future__ import annotations

import asyncio

import httpx

from app.clients import JQuantsClient
from app.models import FinsDividendRecord


def test_fins_dividend_record_alias_mapping() -> None:
    record = FinsDividendRecord.model_validate(
        {
            "PubDate": "2014-02-24",
            "PubTime": "09:21",
            "Code": "15550",
            "RefNo": "201402241B00002",
            "StatCode": "1",
            "BoardDate": "2014-02-24",
            "IFCode": "2",
            "FRCode": "2",
            "IFTerm": "2014-03",
            "DivRate": 12.5,
            "RecDate": "2014-03-10",
            "ExDate": "2014-03-06",
            "ActRecDate": "2014-03-10",
            "PayDate": "",
            "CARefNo": "201402241B00002",
            "DistAmt": "",
            "RetEarn": "",
            "DeemDiv": "",
            "DeemCapGains": "",
            "NetAssetDecRatio": "",
            "CommSpecCode": "0",
            "CommDivRate": "",
            "SpecDivRate": "",
        }
    )

    assert record.reference_number == "201402241B00002"
    assert record.dividend_rate == "12.5"
    assert record.payment_start_date is None
    assert record.commemorative_special_code == "0"


def test_jquants_client_fetch_fins_dividend() -> None:
    async def run() -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v2/fins/dividend"
            assert request.url.params["code"] == "2780"
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "PubDate": "2014-02-24",
                            "PubTime": "09:21",
                            "Code": "27800",
                            "RefNo": "201402241B00002",
                            "StatCode": "1",
                            "BoardDate": "2014-02-24",
                            "IFCode": "2",
                            "FRCode": "2",
                            "IFTerm": "2014-03",
                            "DivRate": "-",
                            "RecDate": "2014-03-10",
                            "ExDate": "2014-03-06",
                            "ActRecDate": "2014-03-10",
                            "PayDate": "-",
                            "CARefNo": "201402241B00002",
                            "DistAmt": "",
                            "RetEarn": "",
                            "DeemDiv": "",
                            "DeemCapGains": "",
                            "NetAssetDecRatio": "",
                            "CommSpecCode": "0",
                            "CommDivRate": "",
                            "SpecDivRate": "",
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
            records = await client.fetch_fins_dividend(code="2780")
        finally:
            httpx.AsyncClient = original_async_client  # type: ignore[assignment]

        assert len(records) == 1
        assert records[0].code == "27800"
        assert records[0].payment_start_date == "-"

    asyncio.run(run())
