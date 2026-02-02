"""Pipeline schemas for stage boundary validation."""

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

import pandera.polars as pa
import polars as pl
from pandera.api.polars.model_config import BaseConfig


@dataclass
class PipelineSpec:
    """Pipeline configuration: grouping and time window.

    Both fields are immutable - set once at pipeline start.
    """

    group_cols: tuple[str, ...]  # immutable
    time_range: tuple[datetime, datetime]  # immutable, (start, end)


class IntervalModel(pa.DataFrameModel):
    """IntervalAdapters MUST produce this format."""

    entity_id: str
    start: pl.Datetime
    end: pl.Datetime = pa.Field(nullable=True)
    metric: str
    resource: str
    value: float

    # Columns that are "values" not dimensions
    _value_cols: ClassVar[tuple[str, ...]] = ("start", "end", "value")

    class Config(BaseConfig):
        strict = True  # make sure all specified columns are in the validated dataframe
        ordered = True  #: validate columns order

    @classmethod
    def group_cols(cls) -> tuple[str, ...]:
        """Helper to allow checking group_by columns."""
        all_cols = set(cls.to_schema().columns.keys())
        return tuple(sorted(all_cols - set(cls._value_cols)))

    @classmethod
    def validate(cls, check_obj, *args, **kwargs):
        """Reorder columns to match schema, then validate."""
        check_obj = check_obj.select(*cls.to_schema().columns.keys())
        return super().validate(check_obj, *args, **kwargs)


class TimelineModel(pa.DataFrameModel):
    """Output of intervals_to_counts, input to resample."""

    timestamp: pl.Datetime
    value: float = pa.Field(coerce=True)
    metric: str
    resource: str

    _value_cols: ClassVar[tuple[str, ...]] = ("timestamp", "value")

    class Config(BaseConfig):
        strict = True
        ordered = True

    @classmethod
    def group_cols(cls) -> tuple[str, ...]:
        all_cols = set(cls.to_schema().columns.keys())
        return tuple(sorted(all_cols - set(cls._value_cols)))

    @classmethod
    def validate(cls, check_obj, *args, **kwargs):
        """Reorder columns to match schema, then validate."""
        check_obj = check_obj.select(*cls.to_schema().columns.keys())
        return super().validate(check_obj, *args, **kwargs)


class UsageModel(TimelineModel):
    """Final output with site context."""

    site: str
    collector_type: str
