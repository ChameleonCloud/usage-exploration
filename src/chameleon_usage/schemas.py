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

    def validate_against(self, df: "pl.LazyFrame") -> None:
        """Raise if group_cols not present in dataframe."""
        data_cols = set(df.collect_schema().names())
        missing = set(self.group_cols) - data_cols
        if missing:
            raise ValueError(f"group_cols not in data: {missing}")


class _OrderedModel(pa.DataFrameModel):
    """Base model that coerces column order to match schema.

    See https://github.com/unionai-oss/pandera/issues/1317
    """

    _value_cols: ClassVar[tuple[str, ...]] = ()

    class Config(BaseConfig):
        strict = False
        ordered = True

    @classmethod
    def group_cols(cls) -> tuple[str, ...]:
        """Non-value columns in sorted order."""
        all_cols = set(cls.to_schema().columns.keys())
        return tuple(sorted(all_cols - set(cls._value_cols)))

    @classmethod
    def validate(cls, check_obj, *args, **kwargs):
        """Reorder schema columns to front, preserve extra columns, then validate."""
        schema_cols = list(cls.to_schema().columns.keys())
        all_cols = check_obj.collect_schema().names()
        extra_cols = [c for c in all_cols if c not in schema_cols]
        check_obj = check_obj.select(*schema_cols, *extra_cols)
        return super().validate(check_obj, *args, **kwargs)


class IntervalModel(_OrderedModel):
    """IntervalAdapters MUST produce this format."""

    entity_id: str
    start: pl.Datetime
    end: pl.Datetime = pa.Field(nullable=True, coerce=True)
    metric: str
    resource: str
    value: float

    _value_cols: ClassVar[tuple[str, ...]] = ("start", "end", "value")


class TimelineModel(_OrderedModel):
    """Output of intervals_to_counts, input to resample."""

    timestamp: pl.Datetime
    value: float = pa.Field(coerce=True)
    metric: str
    resource: str

    _value_cols: ClassVar[tuple[str, ...]] = ("timestamp", "value")


class UsageModel(TimelineModel):
    """Final output with site context."""

    site: str
    collector_type: str


class WideOutput(pa.DataFrameModel):
    """Wide output for stacked usage plots."""

    time: pl.Datetime
    site: str
    resource: str
    total: float
    reservable: float
    committed: float
    occupied_ondemand: float
    occupied_reserved: float
    active_ondemand: float
    active_reserved: float

    class Config(BaseConfig):
        strict = True
        ordered = True
