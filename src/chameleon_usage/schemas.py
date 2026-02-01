"""Pipeline schemas for stage boundary validation."""

from dataclasses import dataclass
from datetime import datetime

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import LazyFrame


@dataclass
class PipelineSpec:
    """Pipeline configuration: grouping and time window.

    Both fields are immutable - set once at pipeline start.
    """

    group_cols: tuple[str, ...]  # immutable
    time_range: tuple[datetime, datetime]  # immutable, (start, end)

    @property
    def interval_required(self) -> set[str]:
        return {"entity_id", "start", "end", *self.group_cols}

    @property
    def delta_required(self) -> set[str]:
        return {"timestamp", "change", *self.group_cols}

    @property
    def count_required(self) -> set[str]:
        return {"timestamp", "count", *self.group_cols}

    def validate_stage(self, df: pl.LazyFrame, stage: str) -> pl.LazyFrame:
        required = getattr(self, f"{stage}_required")
        actual = set(df.collect_schema().names())
        missing = required - actual
        if missing:
            raise ValueError(f"Stage {stage} missing columns: {missing}")
        return df


class IntervalSchema(pa.DataFrameModel):
    """Output of adapters, input to intervals_to_counts."""

    entity_id: str
    start: pl.Datetime
    end: pl.Datetime = pa.Field(nullable=True)
    quantity_type: str


class CountSchema(pa.DataFrameModel):
    """Output of intervals_to_counts, input to resample."""

    timestamp: pl.Datetime
    quantity_type: str
    count: float = pa.Field(coerce=True)


class UsageSchema(pa.DataFrameModel):
    """Final output with site context."""

    timestamp: pl.Datetime
    quantity_type: str
    count: float = pa.Field(coerce=True)
    site: str
    collector_type: str


# Type aliases
IntervalFrame = LazyFrame[IntervalSchema]
CountFrame = LazyFrame[CountSchema]
UsageFrame = LazyFrame[UsageSchema]
