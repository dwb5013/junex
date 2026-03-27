from __future__ import annotations

import pandas as pd

import duckdb

from app.db import DuckDBRepository
from app.features import build_next_day_labels


def test_build_next_day_labels_creates_expected_targets(tmp_path) -> None:
    database_path = tmp_path / "junex.duckdb"
    repository = DuckDBRepository(database_path)

    connection = duckdb.connect(str(database_path))
    connection.execute(
        """
        create schema analytics;
        create table analytics.price_action_features as
        select *
        from (
            values
                (date '2026-03-17', '45920', 100.0, 0.0100, 0.0050),
                (date '2026-03-18', '45920', 102.0, 0.0200, 0.0100),
                (date '2026-03-19', '45920', 99.0, -0.0300, -0.0200),
                (date '2026-03-23', '45920', 101.0, 0.0150, 0.0080)
        ) as t(trade_date, code, close, topix_ret_1d, industry_ret_1d);
        """
    )
    connection.close()

    row_count = build_next_day_labels(repository)

    assert row_count == 4

    result = repository.query(
        """
        select
            trade_date,
            next_trade_date,
            label_next_ret_1d,
            label_next_up_1d,
            label_next_excess_ret_1d,
            label_next_industry_excess_ret_1d,
            label_next_ret_3d,
            label_next_up_3d,
            label_next_direction_1d
        from analytics.next_day_labels
        where code = '45920'
        order by trade_date
        """
    )

    first = result.iloc[0]
    assert str(first["trade_date"])[:10] == "2026-03-17"
    assert str(first["next_trade_date"])[:10] == "2026-03-18"
    assert round(first["label_next_ret_1d"], 6) == 0.02
    assert first["label_next_up_1d"] == 1
    assert round(first["label_next_excess_ret_1d"], 6) == 0.0
    assert round(first["label_next_industry_excess_ret_1d"], 6) == 0.01
    assert round(first["label_next_ret_3d"], 6) == 0.01
    assert first["label_next_up_3d"] == 1
    assert first["label_next_direction_1d"] == "bullish"

    second = result.iloc[1]
    assert round(second["label_next_ret_1d"], 6) == round((99.0 / 102.0) - 1.0, 6)
    assert second["label_next_up_1d"] == 0
    assert second["label_next_direction_1d"] == "bearish"

    last = result.iloc[3]
    assert pd.isna(last["next_trade_date"])
    assert pd.isna(last["label_next_ret_1d"])
    assert pd.isna(last["label_next_up_1d"])
    assert pd.isna(last["label_next_direction_1d"])
