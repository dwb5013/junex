from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from app.models import MetricRecord


def summarize_by_category(records: Iterable[MetricRecord]) -> dict[str, float]:
    frame = pd.DataFrame(
        [{"category": record.category, "value": record.value} for record in records]
    )
    if frame.empty:
        return {}

    grouped = frame.groupby("category", as_index=False)["value"].sum()
    return {
        str(row["category"]): float(row["value"])
        for row in grouped.to_dict(orient="records")
    }
