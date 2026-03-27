from __future__ import annotations

import math

import pandas as pd

from app.db import DuckDBRepository
from app.features import build_market_industry_linkage_features


def test_build_market_industry_linkage_features_creates_expected_metrics(tmp_path) -> None:
    repository = DuckDBRepository(str(tmp_path / "junex.duckdb"))

    rows: list[dict[str, object]] = []
    topix_rows: list[dict[str, object]] = []
    industry_index_rows: list[dict[str, object]] = []
    topix_returns = [0.01 * value for value in range(1, 61)]
    stock_a_returns = [0.02 * value for value in range(1, 61)]
    stock_b_returns = [-(0.01 * value) for value in range(1, 61)]

    for idx in range(60):
        trade_date = pd.Timestamp("2024-01-01") + pd.Timedelta(days=idx)
        topix_rows.append({"Date": trade_date.date(), "C": 2000.0 + idx})
        industry_index_rows.append({"Date": trade_date.date(), "Code": "0047", "C": 500.0 + idx})
        rows.extend(
            [
                {
                    "trade_date": trade_date.date(),
                    "code": "45920",
                    "s33": "3250",
                    "s33_name": "Pharmaceutical",
                    "industry_index_code": "0047",
                    "close": 100.0 + idx,
                    "ret_1d": stock_a_returns[idx],
                    "ret_5d": stock_a_returns[idx] * 2,
                    "ret_20d": stock_a_returns[idx] * 3,
                    "topix_ret_1d": topix_returns[idx],
                    "topix_ret_5d": topix_returns[idx] * 2,
                    "industry_ret_1d": 0.5 * (stock_a_returns[idx] + stock_b_returns[idx]),
                    "industry_ret_5d": stock_a_returns[idx],
                    "volume_shock_20d_inclusive": 1.2,
                },
                {
                    "trade_date": trade_date.date(),
                    "code": "99990",
                    "s33": "3250",
                    "s33_name": "Pharmaceutical",
                    "industry_index_code": "0047",
                    "close": 80.0 - idx,
                    "ret_1d": stock_b_returns[idx],
                    "ret_5d": stock_b_returns[idx] * 2,
                    "ret_20d": stock_b_returns[idx] * 3,
                    "topix_ret_1d": topix_returns[idx],
                    "topix_ret_5d": topix_returns[idx] * 2,
                    "industry_ret_1d": 0.5 * (stock_a_returns[idx] + stock_b_returns[idx]),
                    "industry_ret_5d": stock_a_returns[idx],
                    "volume_shock_20d_inclusive": 0.8,
                },
            ]
        )

    repository.replace_table("analytics.price_action_features", pd.DataFrame(rows))
    repository.replace_table("market_data.topix_daily_bar", pd.DataFrame(topix_rows))
    repository.replace_table("market_data.index_daily_bar", pd.DataFrame(industry_index_rows))

    row_count = build_market_industry_linkage_features(repository)

    assert row_count == 120

    result = repository.query(
        """
        select
            trade_date,
            code,
            topix_close_vs_ma20,
            industry_close_vs_ma20,
            market_up_ratio,
            market_new_high_20d_ratio,
            market_volume_up_ratio,
            industry_up_ratio,
            industry_new_high_20d_ratio,
            industry_volume_up_ratio,
            industry_strength_pct_1d,
            industry_strength_pct_5d,
            industry_strength_pct_20d,
            industry_excess_ret_1d,
            beta_20,
            beta_60
        from analytics.market_industry_linkage_features
        where trade_date = '2024-02-29' and code = '45920'
        """
    ).iloc[0]

    assert math.isclose(result["topix_close_vs_ma20"], (2059.0 / 2049.5) - 1.0, rel_tol=1e-9)
    assert math.isclose(result["industry_close_vs_ma20"], (559.0 / 549.5) - 1.0, rel_tol=1e-9)
    assert math.isclose(result["market_up_ratio"], 0.5, rel_tol=1e-9)
    assert math.isclose(result["market_new_high_20d_ratio"], 0.5, rel_tol=1e-9)
    assert math.isclose(result["market_volume_up_ratio"], 0.5, rel_tol=1e-9)
    assert math.isclose(result["industry_up_ratio"], 0.5, rel_tol=1e-9)
    assert math.isclose(result["industry_new_high_20d_ratio"], 0.5, rel_tol=1e-9)
    assert math.isclose(result["industry_volume_up_ratio"], 0.5, rel_tol=1e-9)
    assert math.isclose(result["industry_strength_pct_1d"], 1.0, rel_tol=1e-9)
    assert math.isclose(result["industry_strength_pct_5d"], 1.0, rel_tol=1e-9)
    assert math.isclose(result["industry_strength_pct_20d"], 1.0, rel_tol=1e-9)
    assert math.isclose(result["industry_excess_ret_1d"], 0.9, rel_tol=1e-9)
    assert math.isclose(result["beta_20"], 2.0, rel_tol=1e-9)
    assert math.isclose(result["beta_60"], 2.0, rel_tol=1e-9)
