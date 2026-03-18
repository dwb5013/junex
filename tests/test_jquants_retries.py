from __future__ import annotations

import asyncio

import httpx

from app.clients import JQuantsClient


def test_jquants_client_retries_timeout_then_succeeds(monkeypatch) -> None:
    async def run() -> None:
        calls = {"count": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["count"] += 1
            if calls["count"] == 1:
                raise httpx.ConnectTimeout("timed out")
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "Date": "2026-03-18",
                            "Code": "0028",
                            "O": 2718.52,
                            "H": 2730.11,
                            "L": 2709.84,
                            "C": 2726.49,
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

        async def no_sleep(_: float) -> None:
            return None

        monkeypatch.setattr("app.clients.asyncio.sleep", no_sleep)
        httpx.AsyncClient = patched_async_client  # type: ignore[assignment]
        try:
            records = await client.fetch_index_daily_bars(code="0028")
        finally:
            httpx.AsyncClient = original_async_client  # type: ignore[assignment]

        assert calls["count"] == 2
        assert len(records) == 1
        assert records[0].code == "0028"

    asyncio.run(run())
