"""Domain-aware pipeline wrappers with validation."""

import polars as pl

from chameleon_usage.math import transforms
from chameleon_usage.schemas import IntervalSchema, CountSchema, UsageSchema
from chameleon_usage.constants import QuantityTypes as QT


def intervals_to_counts(df: pl.LazyFrame) -> pl.LazyFrame:
    """Validated wrapper: intervals → counts."""
    IntervalSchema.validate(df)
    result = transforms.intervals_to_counts(df, "start", "end", ["quantity_type"])
    return CountSchema.validate(result)


def resample(df: pl.LazyFrame, interval: str = "1d") -> pl.LazyFrame:
    """Validated wrapper: resample counts to regular intervals."""
    CountSchema.validate(df)
    result = transforms.resample(df, "timestamp", "count", interval, ["quantity_type"])
    return CountSchema.validate(result)


def compute_derived_metrics(df: pl.LazyFrame) -> pl.LazyFrame:
    """Compute available and idle from base metrics.

    available = reservable - committed
    idle = committed - occupied
    """
    CountSchema.validate(df)

    index_cols = ["timestamp"]

    # Long to wide
    pivoted = df.collect().pivot(
        on="quantity_type", index=index_cols, values="count"
    )

    cols = pivoted.columns

    # Compute derived
    if QT.RESERVABLE in cols and QT.COMMITTED in cols:
        pivoted = pivoted.with_columns(
            (pl.col(QT.RESERVABLE) - pl.col(QT.COMMITTED)).alias(QT.AVAILABLE),
        )

    if QT.COMMITTED in cols and QT.OCCUPIED in cols:
        pivoted = pivoted.with_columns(
            (pl.col(QT.COMMITTED) - pl.col(QT.OCCUPIED)).alias(QT.IDLE),
        )

    # Wide to long
    result = (
        pivoted.unpivot(
            index=index_cols, variable_name="quantity_type", value_name="count"
        )
        .drop_nulls("count")
        .lazy()
    )

    return CountSchema.validate(result)


def add_site_context(
    df: pl.LazyFrame, site: str, collector_type: str = "current"
) -> pl.LazyFrame:
    """Add site and collector_type columns for UsageSchema."""
    CountSchema.validate(df)
    result = df.with_columns(
        pl.lit(site).alias("site"),
        pl.lit(collector_type).alias("collector_type"),
    )
    return UsageSchema.validate(result)
