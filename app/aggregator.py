from __future__ import annotations

from collections.abc import Iterable

from app.models import MetricRecord


def merge_records(*groups: Iterable[MetricRecord]) -> list[MetricRecord]:
    merged: list[MetricRecord] = []
    for group in groups:
        merged.extend(group)
    return merged
