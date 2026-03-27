from __future__ import annotations

import math

import pandas as pd

from app.db import DuckDBRepository
from app.features import build_flow_structure_features


def test_build_flow_structure_features_creates_expected_metrics(tmp_path) -> None:
    repository = DuckDBRepository(str(tmp_path / "junex.duckdb"))

    repository.replace_table(
        "analytics.price_action_features",
        pd.DataFrame(
            [
                {
                    "trade_date": pd.Timestamp("2024-03-15"),
                    "code": "45920",
                    "s33": "3250",
                    "s33_name": "Pharmaceutical",
                    "industry_index_code": "0047",
                }
            ]
        ),
    )
    repository.replace_table(
        "market_data.market_breakdown",
        pd.DataFrame(
            [
                {
                    "Date": "2024-03-15",
                    "Code": "45920",
                    "LongSellVa": 100.0,
                    "ShrtNoMrgnVa": 50.0,
                    "MrgnSellNewVa": 30.0,
                    "MrgnSellCloseVa": 20.0,
                    "LongBuyVa": 200.0,
                    "MrgnBuyNewVa": 60.0,
                    "MrgnBuyCloseVa": 40.0,
                }
            ]
        ),
    )
    repository.replace_table(
        "market_data.short_ratio",
        pd.DataFrame(
            [
                {
                    "Date": "2024-03-15",
                    "S33": "3250",
                    "SellExShortVa": 300.0,
                    "ShrtWithResVa": 150.0,
                    "ShrtNoResVa": 50.0,
                }
            ]
        ),
    )
    repository.replace_table(
        "market_data.margin_alert",
        pd.DataFrame(
            [
                {
                    "PubDate": pd.Timestamp("2024-03-14"),
                    "Code": "45920",
                    "AppDate": pd.Timestamp("2024-03-13"),
                    "SLRatio": 1.25,
                    "LongOutChg": "-10",
                    "ShrtOutChg": "5",
                }
            ]
        ),
    )
    repository.replace_table(
        "market_data.short_sale_report",
        pd.DataFrame(
            [
                {
                    "DiscDate": pd.Timestamp("2024-03-15"),
                    "CalcDate": pd.Timestamp("2024-03-14"),
                    "Code": "45920",
                    "ShrtPosToSO": 0.6,
                    "PrevRptRatio": 0.5,
                }
            ]
        ),
    )

    row_count = build_flow_structure_features(repository)

    assert row_count == 1

    result = repository.query("select * from analytics.flow_structure_features").iloc[0]

    assert math.isclose(result["total_buy_va"], 300.0, rel_tol=1e-9)
    assert math.isclose(result["total_sell_va"], 200.0, rel_tol=1e-9)
    assert math.isclose(result["total_trade_va"], 500.0, rel_tol=1e-9)
    assert math.isclose(result["long_buy_share_of_buy_va"], 200.0 / 300.0, rel_tol=1e-9)
    assert math.isclose(result["margin_buy_new_share_of_buy_va"], 60.0 / 300.0, rel_tol=1e-9)
    assert math.isclose(result["margin_buy_close_share_of_buy_va"], 40.0 / 300.0, rel_tol=1e-9)
    assert math.isclose(result["long_sell_share_of_sell_va"], 100.0 / 200.0, rel_tol=1e-9)
    assert math.isclose(result["short_nomargin_share_of_sell_va"], 50.0 / 200.0, rel_tol=1e-9)
    assert math.isclose(result["margin_sell_new_share_of_sell_va"], 30.0 / 200.0, rel_tol=1e-9)
    assert math.isclose(result["margin_sell_close_share_of_sell_va"], 20.0 / 200.0, rel_tol=1e-9)
    assert math.isclose(result["net_cash_va"], 100.0, rel_tol=1e-9)
    assert math.isclose(result["net_margin_new_va"], 30.0, rel_tol=1e-9)
    assert math.isclose(result["net_margin_close_va"], 20.0, rel_tol=1e-9)
    assert math.isclose(result["sector_short_ratio"], 200.0 / 500.0, rel_tol=1e-9)
    assert math.isclose(result["margin_alert_sl_ratio"], 1.25, rel_tol=1e-9)
    assert result["margin_alert_long_out_chg"] == "-10"
    assert result["margin_alert_short_out_chg"] == "5"
    assert math.isclose(result["short_sale_pos_to_so"], 0.6, rel_tol=1e-9)
    assert math.isclose(result["short_sale_prev_ratio"], 0.5, rel_tol=1e-9)
    assert math.isclose(result["short_sale_ratio_change"], 0.1, rel_tol=1e-9)
