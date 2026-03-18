from app.aggregator import merge_records
from app.models import MetricRecord
from app.stats import summarize_by_category


def test_merge_records() -> None:
    left = [MetricRecord(source="a", category="orders", value=10)]
    right = [MetricRecord(source="b", category="orders", value=5)]

    merged = merge_records(left, right)

    assert merged == [
        MetricRecord(source="a", category="orders", value=10),
        MetricRecord(source="b", category="orders", value=5),
    ]


def test_summarize_by_category() -> None:
    records = [
        MetricRecord(source="a", category="orders", value=10),
        MetricRecord(source="b", category="orders", value=5),
        MetricRecord(source="b", category="revenue", value=20),
    ]

    summary = summarize_by_category(records)

    assert summary == {"orders": 15.0, "revenue": 20.0}


def test_summarize_by_category_empty() -> None:
    assert summarize_by_category([]) == {}
