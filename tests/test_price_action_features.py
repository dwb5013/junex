from __future__ import annotations

import math

import pandas as pd

from app.db import DuckDBRepository
from app.features import build_price_action_features


def test_build_price_action_features_creates_expected_metrics(tmp_path) -> None:
    repository = DuckDBRepository(str(tmp_path / "junex.duckdb"))

    equity_daily_bar = pd.DataFrame(
        [
            {"Date": "2024-01-01", "Code": "45920", "AdjO": 99.0, "AdjH": 101.0, "AdjL": 98.0, "AdjC": 100.0, "AdjVo": 100.0, "Va": 1000.0, "UL": "0", "LL": "0"},
            {"Date": "2024-01-02", "Code": "45920", "AdjO": 101.0, "AdjH": 103.0, "AdjL": 100.0, "AdjC": 102.0, "AdjVo": 120.0, "Va": 1200.0, "UL": "0", "LL": "0"},
            {"Date": "2024-01-03", "Code": "45920", "AdjO": 102.0, "AdjH": 103.0, "AdjL": 100.0, "AdjC": 101.0, "AdjVo": 110.0, "Va": 1100.0, "UL": "0", "LL": "0"},
            {"Date": "2024-01-04", "Code": "45920", "AdjO": 102.0, "AdjH": 104.0, "AdjL": 101.0, "AdjC": 103.0, "AdjVo": 130.0, "Va": 1400.0, "UL": "0", "LL": "0"},
            {"Date": "2024-01-05", "Code": "45920", "AdjO": 104.0, "AdjH": 106.0, "AdjL": 103.0, "AdjC": 105.0, "AdjVo": 150.0, "Va": 1600.0, "UL": "0", "LL": "0"},
            {"Date": "2024-01-08", "Code": "45920", "AdjO": 105.0, "AdjH": 106.0, "AdjL": 103.0, "AdjC": 104.0, "AdjVo": 180.0, "Va": 2000.0, "UL": "1", "LL": "0"},
        ]
    )
    topix_daily_bar = pd.DataFrame(
        [
            {"Date": "2024-01-01", "O": 2000.0, "H": 2005.0, "L": 1995.0, "C": 2000.0},
            {"Date": "2024-01-02", "O": 2005.0, "H": 2015.0, "L": 2000.0, "C": 2010.0},
            {"Date": "2024-01-03", "O": 2010.0, "H": 2025.0, "L": 2008.0, "C": 2020.0},
            {"Date": "2024-01-04", "O": 2020.0, "H": 2022.0, "L": 2010.0, "C": 2015.0},
            {"Date": "2024-01-05", "O": 2015.0, "H": 2030.0, "L": 2012.0, "C": 2025.0},
            {"Date": "2024-01-08", "O": 2025.0, "H": 2035.0, "L": 2020.0, "C": 2030.0},
        ]
    )
    equity_master = pd.DataFrame(
        [
            {"Date": "2024-01-01", "Code": "45920", "S33": "3250", "S33Nm": "Pharmaceutical"},
            {"Date": "2024-01-02", "Code": "45920", "S33": "3250", "S33Nm": "Pharmaceutical"},
            {"Date": "2024-01-03", "Code": "45920", "S33": "3250", "S33Nm": "Pharmaceutical"},
            {"Date": "2024-01-04", "Code": "45920", "S33": "3250", "S33Nm": "Pharmaceutical"},
            {"Date": "2024-01-05", "Code": "45920", "S33": "3250", "S33Nm": "Pharmaceutical"},
            {"Date": "2024-01-08", "Code": "45920", "S33": "3250", "S33Nm": "Pharmaceutical"},
        ]
    )
    industry_index_daily_bar = pd.DataFrame(
        [
            {"Date": "2024-01-01", "Code": "0047", "O": 500.0, "H": 501.0, "L": 499.0, "C": 500.0},
            {"Date": "2024-01-02", "Code": "0047", "O": 505.0, "H": 506.0, "L": 504.0, "C": 505.0},
            {"Date": "2024-01-03", "Code": "0047", "O": 510.0, "H": 511.0, "L": 509.0, "C": 510.0},
            {"Date": "2024-01-04", "Code": "0047", "O": 515.0, "H": 516.0, "L": 514.0, "C": 515.0},
            {"Date": "2024-01-05", "Code": "0047", "O": 520.0, "H": 521.0, "L": 519.0, "C": 520.0},
            {"Date": "2024-01-08", "Code": "0047", "O": 530.0, "H": 531.0, "L": 529.0, "C": 530.0},
        ]
    )

    repository.upsert_table("market_data.equity_daily_bar", equity_daily_bar, key_columns=["Date", "Code"])
    repository.upsert_table("market_data.topix_daily_bar", topix_daily_bar, key_columns=["Date"])
    repository.upsert_table("market_data.equity_master", equity_master, key_columns=["Date", "Code"])
    repository.upsert_table("market_data.index_daily_bar", industry_index_daily_bar, key_columns=["Date", "Code"])

    row_count = build_price_action_features(repository)

    assert row_count == 6

    result = repository.query(
        """
        select
            trade_date,
            code,
            s33,
            industry_index_code,
            ret_1d,
            ret_3d,
            topix_ret_1d,
            industry_ret_1d,
            industry_ret_3d,
            excess_ret_1d,
            industry_excess_ret_1d,
            body_ratio,
            upper_shadow_ratio,
            lower_shadow_ratio,
            close_position,
            gap_pct,
            volume_shock_20d_inclusive,
            value_shock_20d_inclusive,
            true_range,
            mean_true_range_5,
            mean_true_range_20,
            logret_vol_5,
            logret_vol_20,
            hit_limit_up,
            hit_limit_down
        from analytics.price_action_features
        where code = '45920' and trade_date = '2024-01-08'
        """
    ).iloc[0]

    assert result["s33"] == "3250"
    assert result["industry_index_code"] == "0047"
    assert math.isclose(result["ret_1d"], (104.0 / 105.0) - 1.0, rel_tol=1e-9)
    assert math.isclose(result["ret_3d"], (104.0 / 101.0) - 1.0, rel_tol=1e-9)
    assert math.isclose(result["topix_ret_1d"], (2030.0 / 2025.0) - 1.0, rel_tol=1e-9)
    assert math.isclose(result["industry_ret_1d"], (530.0 / 520.0) - 1.0, rel_tol=1e-9)
    assert math.isclose(result["industry_ret_3d"], (530.0 / 510.0) - 1.0, rel_tol=1e-9)
    assert math.isclose(
        result["excess_ret_1d"],
        ((104.0 / 105.0) - 1.0) - ((2030.0 / 2025.0) - 1.0),
        rel_tol=1e-9,
    )
    assert math.isclose(
        result["industry_excess_ret_1d"],
        ((104.0 / 105.0) - 1.0) - ((530.0 / 520.0) - 1.0),
        rel_tol=1e-9,
    )
    assert math.isclose(result["body_ratio"], 1.0 / 3.0, rel_tol=1e-9)
    assert math.isclose(result["upper_shadow_ratio"], 1.0 / 3.0, rel_tol=1e-9)
    assert math.isclose(result["lower_shadow_ratio"], 1.0 / 3.0, rel_tol=1e-9)
    assert math.isclose(result["close_position"], 1.0 / 3.0, rel_tol=1e-9)
    assert math.isclose(result["gap_pct"], 0.0, rel_tol=1e-9)
    assert math.isclose(
        result["volume_shock_20d_inclusive"],
        180.0 / ((100.0 + 120.0 + 110.0 + 130.0 + 150.0 + 180.0) / 6.0),
        rel_tol=1e-9,
    )
    assert math.isclose(
        result["value_shock_20d_inclusive"],
        2000.0 / ((1000.0 + 1200.0 + 1100.0 + 1400.0 + 1600.0 + 2000.0) / 6.0),
        rel_tol=1e-9,
    )
    assert math.isclose(result["true_range"], 3.0, rel_tol=1e-9)
    assert math.isclose(result["mean_true_range_5"], 3.0, rel_tol=1e-9)
    assert math.isclose(result["mean_true_range_20"], 3.0, rel_tol=1e-9)
    assert result["logret_vol_5"] > 0
    assert result["logret_vol_20"] > 0
    assert result["hit_limit_up"] == 1
    assert result["hit_limit_down"] == 0


def test_build_price_action_features_uses_latest_code_level_equity_classification(tmp_path) -> None:
    repository = DuckDBRepository(str(tmp_path / "junex.duckdb"))

    equity_daily_bar = pd.DataFrame(
        [
            {"Date": "2024-01-01", "Code": "45920", "AdjO": 99.0, "AdjH": 101.0, "AdjL": 98.0, "AdjC": 100.0, "AdjVo": 100.0, "Va": 1000.0, "UL": "0", "LL": "0"},
            {"Date": "2024-01-02", "Code": "45920", "AdjO": 101.0, "AdjH": 103.0, "AdjL": 100.0, "AdjC": 102.0, "AdjVo": 120.0, "Va": 1200.0, "UL": "0", "LL": "0"},
            {"Date": "2024-01-03", "Code": "45920", "AdjO": 102.0, "AdjH": 103.0, "AdjL": 100.0, "AdjC": 101.0, "AdjVo": 110.0, "Va": 1100.0, "UL": "0", "LL": "0"},
        ]
    )
    topix_daily_bar = pd.DataFrame(
        [
            {"Date": "2024-01-01", "O": 2000.0, "H": 2005.0, "L": 1995.0, "C": 2000.0},
            {"Date": "2024-01-02", "O": 2005.0, "H": 2015.0, "L": 2000.0, "C": 2010.0},
            {"Date": "2024-01-03", "O": 2010.0, "H": 2025.0, "L": 2008.0, "C": 2020.0},
        ]
    )
    equity_master = pd.DataFrame(
        [
            {"Date": "2024-01-03", "Code": "45920", "S33": "3250", "S33Nm": "Pharmaceutical"},
        ]
    )
    industry_index_daily_bar = pd.DataFrame(
        [
            {"Date": "2024-01-01", "Code": "0047", "O": 500.0, "H": 501.0, "L": 499.0, "C": 500.0},
            {"Date": "2024-01-02", "Code": "0047", "O": 505.0, "H": 506.0, "L": 504.0, "C": 505.0},
            {"Date": "2024-01-03", "Code": "0047", "O": 510.0, "H": 511.0, "L": 509.0, "C": 510.0},
        ]
    )

    repository.upsert_table("market_data.equity_daily_bar", equity_daily_bar, key_columns=["Date", "Code"])
    repository.upsert_table("market_data.topix_daily_bar", topix_daily_bar, key_columns=["Date"])
    repository.upsert_table("market_data.equity_master", equity_master, key_columns=["Date", "Code"])
    repository.upsert_table("market_data.index_daily_bar", industry_index_daily_bar, key_columns=["Date", "Code"])

    build_price_action_features(repository)

    result = repository.query(
        """
        select trade_date, s33, s33_name, industry_index_code
        from analytics.price_action_features
        where code = '45920'
        order by trade_date
        """
    )

    assert result.to_dict(orient="records") == [
        {"trade_date": pd.Timestamp("2024-01-01"), "s33": "3250", "s33_name": "Pharmaceutical", "industry_index_code": "0047"},
        {"trade_date": pd.Timestamp("2024-01-02"), "s33": "3250", "s33_name": "Pharmaceutical", "industry_index_code": "0047"},
        {"trade_date": pd.Timestamp("2024-01-03"), "s33": "3250", "s33_name": "Pharmaceutical", "industry_index_code": "0047"},
    ]
