from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import httpx

from app.models import (
    EarningsCalendarRecord,
    EquityDailyBarRecord,
    EquityMasterRecord,
    FinsDividendRecord,
    FinsSummaryRecord,
    IndexDailyBarRecord,
    MarketCalendarRecord,
    TopixDailyBarRecord,
)


class ExternalAPIClient:
    """Thin wrapper around outbound HTTP calls."""

    def __init__(self, base_url: str, timeout: float = 10.0, max_retries: int = 4) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries

    async def fetch_json(self, path: str) -> list[dict[str, Any]]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await _get_with_retries(
                client,
                url,
                max_retries=self.max_retries,
            )
            payload = response.json()
        if not isinstance(payload, Sequence):
            raise ValueError("Expected a list payload from external API")
        return [item for item in payload if isinstance(item, dict)]


class JQuantsClient:
    """HTTP client for the J-Quants REST API."""

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.jquants.com",
        timeout: float = 10.0,
        max_retries: int = 4,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries

    async def fetch_equities_master(
        self,
        *,
        code: str | None = None,
        date: str | None = None,
    ) -> list[EquityMasterRecord]:
        params = {name: value for name, value in {"code": code, "date": date}.items() if value}
        headers = {"x-api-key": self.api_key}

        async with httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout, headers=headers) as client:
            response = await self._get(client, "/v2/equities/master", params=params)
            payload = response.json()

        items = payload.get("data")
        if not isinstance(items, Sequence):
            raise ValueError("Expected a top-level 'data' array from J-Quants /v2/equities/master")

        return [EquityMasterRecord.model_validate(item) for item in items if isinstance(item, dict)]

    async def fetch_equity_daily_bars(
        self,
        *,
        code: str | None = None,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> list[EquityDailyBarRecord]:
        if not code and not date:
            raise ValueError("Either code or date is required for /v2/equities/bars/daily")

        headers = {"x-api-key": self.api_key}
        pagination_key: str | None = None
        records: list[EquityDailyBarRecord] = []

        async with httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout, headers=headers) as client:
            while True:
                params = {
                    name: value
                    for name, value in {
                        "code": code,
                        "date": date,
                        "from": from_date,
                        "to": to_date,
                        "pagination_key": pagination_key,
                    }.items()
                    if value
                }
                response = await self._get(client, "/v2/equities/bars/daily", params=params)
                payload = response.json()
                items = payload.get("data")
                if not isinstance(items, Sequence):
                    raise ValueError("Expected a top-level 'data' array from J-Quants /v2/equities/bars/daily")

                records.extend(
                    EquityDailyBarRecord.model_validate(item) for item in items if isinstance(item, dict)
                )
                pagination_key = payload.get("pagination_key")
                if not pagination_key:
                    break

        return records

    async def fetch_market_calendar(
        self,
        *,
        from_date: str,
        to_date: str,
    ) -> list[MarketCalendarRecord]:
        headers = {"x-api-key": self.api_key}

        async with httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout, headers=headers) as client:
            response = await self._get(client, "/v2/markets/calendar", params={"from": from_date, "to": to_date})
            payload = response.json()

        items = payload.get("data")
        if not isinstance(items, Sequence):
            raise ValueError("Expected a top-level 'data' array from J-Quants /v2/markets/calendar")

        return [MarketCalendarRecord.model_validate(item) for item in items if isinstance(item, dict)]

    async def fetch_topix_daily_bars(
        self,
        *,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> list[TopixDailyBarRecord]:
        headers = {"x-api-key": self.api_key}
        pagination_key: str | None = None
        records: list[TopixDailyBarRecord] = []

        async with httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout, headers=headers) as client:
            while True:
                params = {
                    name: value
                    for name, value in {
                        "date": date,
                        "from": from_date,
                        "to": to_date,
                        "pagination_key": pagination_key,
                    }.items()
                    if value
                }
                response = await self._get(client, "/v2/indices/bars/daily/topix", params=params)
                payload = response.json()
                items = payload.get("data")
                if not isinstance(items, Sequence):
                    raise ValueError("Expected a top-level 'data' array from J-Quants /v2/indices/bars/daily/topix")

                records.extend(TopixDailyBarRecord.model_validate(item) for item in items if isinstance(item, dict))
                pagination_key = payload.get("pagination_key")
                if not pagination_key:
                    break

        return records

    async def fetch_index_daily_bars(
        self,
        *,
        code: str | None = None,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> list[IndexDailyBarRecord]:
        if not code and not date:
            raise ValueError("Either code or date is required for /v2/indices/bars/daily")

        headers = {"x-api-key": self.api_key}
        pagination_key: str | None = None
        records: list[IndexDailyBarRecord] = []

        async with httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout, headers=headers) as client:
            while True:
                params = {
                    name: value
                    for name, value in {
                        "code": code,
                        "date": date,
                        "from": from_date,
                        "to": to_date,
                        "pagination_key": pagination_key,
                    }.items()
                    if value
                }
                response = await self._get(client, "/v2/indices/bars/daily", params=params)
                payload = response.json()
                items = payload.get("data")
                if not isinstance(items, Sequence):
                    raise ValueError("Expected a top-level 'data' array from J-Quants /v2/indices/bars/daily")

                records.extend(IndexDailyBarRecord.model_validate(item) for item in items if isinstance(item, dict))
                pagination_key = payload.get("pagination_key")
                if not pagination_key:
                    break

        return records

    async def fetch_earnings_calendar(
        self,
        *,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> list[EarningsCalendarRecord]:
        headers = {"x-api-key": self.api_key}
        params = {
            name: value
            for name, value in {"date": date, "from": from_date, "to": to_date}.items()
            if value
        }

        async with httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout, headers=headers) as client:
            response = await self._get(client, "/v2/equities/earnings-calendar", params=params)
            payload = response.json()

        items = payload.get("data")
        if not isinstance(items, Sequence):
            raise ValueError("Expected a top-level 'data' array from J-Quants /v2/equities/earnings-calendar")

        return [EarningsCalendarRecord.model_validate(item) for item in items if isinstance(item, dict)]

    async def fetch_fins_summary(
        self,
        *,
        code: str | None = None,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> list[FinsSummaryRecord]:
        headers = {"x-api-key": self.api_key}
        pagination_key: str | None = None
        records: list[FinsSummaryRecord] = []

        async with httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout, headers=headers) as client:
            while True:
                params = {
                    name: value
                    for name, value in {
                        "code": code,
                        "date": date,
                        "from": from_date,
                        "to": to_date,
                        "pagination_key": pagination_key,
                    }.items()
                    if value
                }
                response = await self._get(client, "/v2/fins/summary", params=params)
                payload = response.json()
                items = payload.get("data")
                if not isinstance(items, Sequence):
                    raise ValueError("Expected a top-level 'data' array from J-Quants /v2/fins/summary")

                records.extend(FinsSummaryRecord.model_validate(item) for item in items if isinstance(item, dict))
                pagination_key = payload.get("pagination_key")
                if not pagination_key:
                    break

        return records

    async def fetch_fins_dividend(
        self,
        *,
        code: str | None = None,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> list[FinsDividendRecord]:
        if not code and not date:
            raise ValueError("Either code or date is required for /v2/fins/dividend")

        headers = {"x-api-key": self.api_key}
        pagination_key: str | None = None
        records: list[FinsDividendRecord] = []

        async with httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout, headers=headers) as client:
            while True:
                params = {
                    name: value
                    for name, value in {
                        "code": code,
                        "date": date,
                        "from": from_date,
                        "to": to_date,
                        "pagination_key": pagination_key,
                    }.items()
                    if value
                }
                response = await self._get(client, "/v2/fins/dividend", params=params)
                payload = response.json()
                items = payload.get("data")
                if not isinstance(items, Sequence):
                    raise ValueError("Expected a top-level 'data' array from J-Quants /v2/fins/dividend")

                records.extend(FinsDividendRecord.model_validate(item) for item in items if isinstance(item, dict))
                pagination_key = payload.get("pagination_key")
                if not pagination_key:
                    break

        return records

    async def _get(
        self,
        client: httpx.AsyncClient,
        path: str,
        *,
        params: dict[str, str] | None = None,
    ) -> httpx.Response:
        return await _get_with_retries(
            client,
            path,
            params=params,
            max_retries=self.max_retries,
        )


async def _get_with_retries(
    client: httpx.AsyncClient,
    path: str,
    *,
    params: dict[str, str] | None = None,
    max_retries: int = 4,
) -> httpx.Response:
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            response = await client.get(path, params=params)
            if response.status_code in {429, 500, 502, 503, 504}:
                response.raise_for_status()
            response.raise_for_status()
            return response
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
            last_error = exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code not in {429, 500, 502, 503, 504}:
                raise
            last_error = exc

        if attempt == max_retries:
            break

        await asyncio.sleep(0.5 * (2 ** (attempt - 1)))

    assert last_error is not None
    raise last_error
