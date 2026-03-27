from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
from threading import Lock
from time import monotonic, sleep
from typing import Any

import httpx
import pandas as pd
from jquantsapi import ClientV2


class ExternalAPIClient:
    """Thin wrapper around outbound HTTP calls."""

    def __init__(self, base_url: str, timeout: float = 10.0, max_retries: int = 4) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries

    async def fetch_json(self, path: str) -> list[dict[str, Any]]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await _get_with_retries(client, url, max_retries=self.max_retries)
            payload = response.json()
        if not isinstance(payload, list):
            raise ValueError("Expected a list payload from external API")
        return [item for item in payload if isinstance(item, dict)]


class RateLimiter:
    """Thread-safe minimum-interval limiter for J-Quants API requests."""

    def __init__(self, rate_limit_per_minute: int) -> None:
        self._minimum_interval_seconds = 60.0 / rate_limit_per_minute
        self._next_allowed_at = 0.0
        self._lock = Lock()

    def acquire(self) -> None:
        with self._lock:
            now = monotonic()
            if now < self._next_allowed_at:
                sleep(self._next_allowed_at - now)
                now = monotonic()
            self._next_allowed_at = max(now, self._next_allowed_at) + self._minimum_interval_seconds


class RateLimitedClientV2(ClientV2):
    """Official J-Quants client with a process-local request rate limiter."""

    def __init__(self, api_key: str, *, rate_limit_per_minute: int) -> None:
        super().__init__(api_key=api_key)
        self._rate_limiter = RateLimiter(rate_limit_per_minute)

    def _get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ):  # type: ignore[override]
        self._rate_limiter.acquire()
        return super()._get(url, params=params)


class JQuantsClient:
    """Wrapper around the official jquants-api-client ClientV2."""

    def __init__(
        self,
        api_key: str,
        *,
        official_client: ClientV2 | None = None,
        rate_limit_per_minute: int = 500,
    ) -> None:
        self._client = official_client or RateLimitedClientV2(
            api_key=api_key,
            rate_limit_per_minute=rate_limit_per_minute,
        )

    def fetch_equities_master(
        self,
        *,
        code: str | None = None,
        date: str | None = None,
    ) -> pd.DataFrame:
        return self._with_metadata(
            self._client.get_eq_master(code=code or "", date=date or ""),
            source_api="/v2/equities/master",
        )

    def fetch_market_segments(self) -> pd.DataFrame:
        return self._with_metadata(
            self._client.get_market_segments(),
            source_api="official_client:get_market_segments",
            sort_columns=["Mkt"],
        )

    def fetch_sector17(self) -> pd.DataFrame:
        return self._with_metadata(
            self._client.get_17_sectors(),
            source_api="official_client:get_17_sectors",
            sort_columns=["S17"],
        )

    def fetch_sector33(self) -> pd.DataFrame:
        return self._with_metadata(
            self._client.get_33_sectors(),
            source_api="official_client:get_33_sectors",
            sort_columns=["S33"],
        )

    def fetch_equity_daily_bars(
        self,
        *,
        code: str | None = None,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> pd.DataFrame:
        if not code and not date:
            raise ValueError("Either code or date is required for /v2/equities/bars/daily")
        return self._with_metadata(
            self._client.get_eq_bars_daily(
                code=code or "",
                from_yyyymmdd=from_date or "",
                to_yyyymmdd=to_date or "",
                date_yyyymmdd=date or "",
            ),
            source_api="/v2/equities/bars/daily",
            sort_columns=["Date", "Code"],
        )

    def fetch_market_calendar(
        self,
        *,
        holiday_division: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> pd.DataFrame:
        return self._with_metadata(
            self._client.get_mkt_calendar(
                holiday_division=holiday_division or "",
                from_yyyymmdd=from_date or "",
                to_yyyymmdd=to_date or "",
            ),
            source_api="/v2/markets/calendar",
            sort_columns=["Date"],
        )

    def fetch_margin_interest(
        self,
        *,
        code: str | None = None,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> pd.DataFrame:
        if not code and not date:
            raise ValueError("Either code or date is required for /v2/markets/margin-interest")
        return self._with_metadata(
            self._client.get_mkt_margin_interest(
                code=code or "",
                from_yyyymmdd=from_date or "",
                to_yyyymmdd=to_date or "",
                date_yyyymmdd=date or "",
            ),
            source_api="/v2/markets/margin-interest",
            sort_columns=["Date", "Code"],
        )

    def fetch_market_breakdown(
        self,
        *,
        code: str | None = None,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> pd.DataFrame:
        if not code and not date:
            raise ValueError("Either code or date is required for /v2/markets/breakdown")
        return self._with_metadata(
            self._client.get_mkt_breakdown(
                code=code or "",
                from_yyyymmdd=from_date or "",
                to_yyyymmdd=to_date or "",
                date_yyyymmdd=date or "",
            ),
            source_api="/v2/markets/breakdown",
            sort_columns=["Date", "Code"],
        )

    def fetch_short_ratio(
        self,
        *,
        s33: str | None = None,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> pd.DataFrame:
        if not s33 and not date:
            raise ValueError("Either s33 or date is required for /v2/markets/short-ratio")
        return self._with_metadata(
            self._client.get_mkt_short_ratio(
                sector_33_code=s33 or "",
                from_yyyymmdd=from_date or "",
                to_yyyymmdd=to_date or "",
                date_yyyymmdd=date or "",
            ),
            source_api="/v2/markets/short-ratio",
            sort_columns=["Date", "S33"],
        )

    def fetch_margin_alert(
        self,
        *,
        code: str | None = None,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> pd.DataFrame:
        if not code and not date:
            raise ValueError("Either code or date is required for /v2/markets/margin-alert")
        return self._with_metadata(
            self._client.get_mkt_margin_alert(
                code=code or "",
                from_yyyymmdd=from_date or "",
                to_yyyymmdd=to_date or "",
                date_yyyymmdd=date or "",
            ),
            source_api="/v2/markets/margin-alert",
            sort_columns=["PubDate", "Code", "AppDate"],
        )

    def fetch_short_sale_report(
        self,
        *,
        code: str | None = None,
        disclosed_date: str | None = None,
        disclosed_date_from: str | None = None,
        disclosed_date_to: str | None = None,
        calculated_date: str | None = None,
    ) -> pd.DataFrame:
        if not any([code, disclosed_date, disclosed_date_from, disclosed_date_to, calculated_date]):
            raise ValueError(
                "At least one of code, disclosed_date, disclosed_date_from, disclosed_date_to, or calculated_date is required for /v2/markets/short-sale-report"
            )
        return self._with_metadata(
            self._client.get_mkt_short_sale_report(
                code=code or "",
                disclosed_date=disclosed_date or "",
                disclosed_date_from=disclosed_date_from or "",
                disclosed_date_to=disclosed_date_to or "",
                calculated_date=calculated_date or "",
            ),
            source_api="/v2/markets/short-sale-report",
            sort_columns=["DiscDate", "CalcDate", "Code"],
        )

    def fetch_topix_daily_bars(
        self,
        *,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> pd.DataFrame:
        final_from_date = from_date or date or ""
        final_to_date = to_date or date or ""
        return self._with_metadata(
            self._client.get_idx_bars_daily_topix(
                from_yyyymmdd=final_from_date,
                to_yyyymmdd=final_to_date,
            ),
            source_api="/v2/indices/bars/daily/topix",
            sort_columns=["Date"],
        )

    def fetch_index_daily_bars(
        self,
        *,
        code: str | None = None,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> pd.DataFrame:
        if not any([code, date, from_date, to_date]):
            raise ValueError("At least one of code, date, from_date, or to_date is required for /v2/indices/bars/daily")
        dataframe = self._client.get_idx_bars_daily(
            code=code or "",
            from_yyyymmdd=from_date or "",
            to_yyyymmdd=to_date or "",
            date_yyyymmdd=date or "",
        )

        return self._with_metadata(
            dataframe,
            source_api="/v2/indices/bars/daily",
            sort_columns=["Date", "Code"],
        )

    def fetch_earnings_calendar(self) -> pd.DataFrame:
        return self._with_metadata(
            self._client.get_eq_earnings_cal(),
            source_api="/v2/equities/earnings-calendar",
            sort_columns=["Date", "Code"],
        )

    def fetch_fins_summary(
        self,
        *,
        code: str | None = None,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> pd.DataFrame:
        if from_date or to_date:
            if code:
                dataframe = self._fetch_code_scoped_date_range(
                    fetcher=lambda target_date: self._client.get_fin_summary(
                        code=code,
                        date_yyyymmdd=target_date,
                    ),
                    start_date=from_date or to_date or "",
                    end_date=to_date or from_date or "",
                )
            else:
                dataframe = self._client.get_fin_summary_range(
                    start_dt=from_date or to_date or "",
                    end_dt=to_date or from_date or "",
                )
        else:
            dataframe = self._client.get_fin_summary(code=code or "", date_yyyymmdd=date or "")

        return self._with_metadata(
            dataframe,
            source_api="/v2/fins/summary",
            sort_columns=["DiscDate", "DiscTime", "Code"],
        )

    def fetch_fins_dividend(
        self,
        *,
        code: str | None = None,
        date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> pd.DataFrame:
        if not code and not date:
            raise ValueError("Either code or date is required for /v2/fins/dividend")
        return self._with_metadata(
            self._client.get_fin_dividend(
                code=code or "",
                from_yyyymmdd=from_date or "",
                to_yyyymmdd=to_date or "",
                date_yyyymmdd=date or "",
            ),
            source_api="/v2/fins/dividend",
            sort_columns=["PubDate", "Code", "RefNo"],
        )

    def fetch_bulk_file_list(self, *, endpoint: str) -> pd.DataFrame:
        return self._with_metadata(
            self._client.get_bulk_list(endpoint),
            source_api="/v2/bulk/list",
            sort_columns=["Key"],
        )

    def download_bulk_file(self, *, key: str, output_path: str) -> Path:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        self._client.download_bulk(key, str(output))
        return output

    def _fetch_code_scoped_date_range(
        self,
        *,
        fetcher: Callable[[str], pd.DataFrame],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        date_range = pd.date_range(start_date, end_date, freq="D")
        frames = [fetcher(target_date.strftime("%Y-%m-%d")) for target_date in date_range]
        non_empty_frames = [frame for frame in frames if not frame.empty]
        if not non_empty_frames:
            return pd.DataFrame()
        return pd.concat(non_empty_frames, ignore_index=True)

    def _with_metadata(
        self,
        dataframe: pd.DataFrame,
        *,
        source_api: str,
        sort_columns: list[str] | None = None,
    ) -> pd.DataFrame:
        if dataframe.empty:
            return dataframe.copy()

        result = dataframe.copy()
        result["source_api"] = source_api
        result["fetched_at"] = pd.Timestamp.now(tz="UTC")
        if sort_columns:
            available_sort_columns = [column for column in sort_columns if column in result.columns]
            if available_sort_columns:
                result = result.sort_values(available_sort_columns).reset_index(drop=True)
        return result


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
