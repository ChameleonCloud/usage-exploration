"""Pipeline schemas for stage boundary validation."""

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import LazyFrame


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
