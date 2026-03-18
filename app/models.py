from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class MetricRecord(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    source: str
    category: str
    value: float = Field(default=0.0)
