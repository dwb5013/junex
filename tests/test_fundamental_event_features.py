from __future__ import annotations

import math

import pandas as pd

from app.db import DuckDBRepository
from app.features import build_fundamental_event_features


def test_build_fundamental_event_features_creates_expected_metrics(tmp_path) -> None:
    repository = DuckDBRepository(str(tmp_path / "junex.duckdb"))

    repository.replace_table(
        "analytics.price_action_features",
        pd.DataFrame(
            [
                {"trade_date": pd.Timestamp("2024-03-15"), "code": "45920"},
                {"trade_date": pd.Timestamp("2024-03-18"), "code": "45920"},
            ]
        ),
    )
    repository.replace_table(
        "market_data.fin_summary",
        pd.DataFrame(
            [
                {
                    "DiscDate": pd.Timestamp("2024-03-10"),
                    "Code": "45920",
                    "DiscNo": "A1",
                    "DocType": "FY",
                    "FOP": "100",
                    "FNP": "50",
                    "FEPS": "10",
                    "MatChgSub": "",
                    "ChgByASRev": "false",
                    "ChgNoASRev": "false",
                    "ChgAcEst": "false",
                },
                {
                    "DiscDate": pd.Timestamp("2024-03-15"),
                    "Code": "45920",
                    "DiscNo": "A2",
                    "DocType": "FY",
                    "FOP": "120",
                    "FNP": "60",
                    "FEPS": "12",
                    "MatChgSub": "Revision",
                    "ChgByASRev": "true",
                    "ChgNoASRev": "false",
                    "ChgAcEst": "false",
                },
            ]
        ),
    )
    repository.replace_table(
        "market_data.fin_dividend",
        pd.DataFrame(
            [
                {"PubDate": pd.Timestamp("2024-03-01"), "Code": "45920", "RefNo": "D1", "DivRate": "10"},
                {"PubDate": pd.Timestamp("2024-03-15"), "Code": "45920", "RefNo": "D2", "DivRate": "12"},
            ]
        ),
    )
    repository.replace_table(
        "market_data.earnings_calendar",
        pd.DataFrame(
            [
                {"Date": pd.Timestamp("2024-03-20"), "Code": "45920"},
            ]
        ),
    )

    row_count = build_fundamental_event_features(repository)

    assert row_count == 2

    result = repository.query(
        """
        select *
        from analytics.fundamental_event_features
        where code = '45920'
        order by trade_date
        """
    )

    day0 = result.iloc[0]
    assert day0["is_earnings_day"] == 1
    assert day0["earnings_event_window_1d"] == 1
    assert day0["earnings_event_window_3d"] == 1
    assert day0["days_to_next_earnings"] == 5
    assert math.isclose(day0["forecast_op_revision_pct"], 0.2, rel_tol=1e-9)
    assert math.isclose(day0["forecast_net_revision_pct"], 0.2, rel_tol=1e-9)
    assert math.isclose(day0["forecast_eps_revision_pct"], 0.2, rel_tol=1e-9)
    assert day0["forecast_revision_direction"] == "upward"
    assert day0["is_dividend_announcement_day"] == 1
    assert math.isclose(day0["dividend_revision_pct"], 0.2, rel_tol=1e-9)
    assert day0["dividend_revision_direction"] == "increase"
    assert day0["guidance_positive_flag"] == 1
    assert day0["fundamental_event_conflict_flag"] == 0

    day1 = result.iloc[1]
    assert day1["is_earnings_day"] == 0
    assert day1["days_since_earnings"] == 3
    assert day1["trading_days_since_earnings"] == 1
    assert day1["earnings_event_window_3d"] == 1
    assert day1["is_dividend_announcement_day"] == 0
