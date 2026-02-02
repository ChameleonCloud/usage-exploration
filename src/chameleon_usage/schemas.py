"""Pipeline schemas for stage boundary validation."""

from dataclasses import dataclass
from datetime import datetime

import pandera.polars as pa
import polars as pl
from pandera.api.polars.model_config import BaseConfig
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


class IntervalModel(pa.DataFrameModel):
    """Output of adapters, input to intervals_to_counts."""

    entity_id: str
    start: pl.Datetime
    end: pl.Datetime = pa.Field(nullable=True)
    metric: str
    resource: str
    value: float

    class Config(BaseConfig):
        strict = True  # make sure all specified columns are in the validated dataframe
        ordered = True  #: validate columns order
        # Columns that are "values" not dimensions
        value_cols = ("timestamp", "count")

    @classmethod
    def group_cols(cls) -> tuple[str, ...]:
        """Helper to allow chacking group_by columns"""
        all_cols = set(cls.to_schema().columns.keys())
        return tuple(all_cols - set(cls.Config.value_cols))


class TimelineModel(pa.DataFrameModel):
    """Output of intervals_to_counts, input to resample."""

    timestamp: pl.Datetime
    metric: str
    resource: str
    value: float = pa.Field(coerce=True)

    class Config(BaseConfig):
        strict = True  # make sure all specified columns are in the validated dataframe
        ordered = True  #: validate columns order
        # Columns that are "values" not dimensions
        value_cols = ("timestamp", "count")

    @classmethod
    def group_cols(cls) -> tuple[str, ...]:
        """Helper to allow chacking group_by columns"""
        all_cols = set(cls.to_schema().columns.keys())
        return tuple(all_cols - set(cls.Config.value_cols))


class UsageModel(TimelineModel):
    """Final output with site context."""

    site: str
    collector_type: str
