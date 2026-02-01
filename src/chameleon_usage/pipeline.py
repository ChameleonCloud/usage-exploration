"""Domain-aware pipeline wrappers with validation."""

import polars as pl

from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.math import sweepline, timeseries
from chameleon_usage.schemas import PipelineSpec


def intervals_to_counts(df: pl.LazyFrame, spec: PipelineSpec) -> pl.LazyFrame:
    """Intervals → counts via sweepline."""
    spec.validate_stage(df, "interval")
    result = sweepline.intervals_to_counts(df, "start", "end", list(spec.group_cols))
    spec.validate_stage(result, "count")
    return result


def resample(df: pl.LazyFrame, interval: str, spec: PipelineSpec) -> pl.LazyFrame:
    """Resample counts to regular intervals."""
    spec.validate_stage(df, "count")
    return timeseries.resample(
        df, "timestamp", "count", interval, list(spec.group_cols)
    )


def collapse_dimension(
    df: pl.LazyFrame,
    spec: PipelineSpec,
    drop: str,
    exclude: list[str] | None = None,
) -> tuple[pl.LazyFrame, PipelineSpec]:
    """Drop a group column, filtering out unwanted values first."""
    if drop not in spec.group_cols:
        raise ValueError(f"{drop} not in group_cols: {spec.group_cols}")

    if exclude:
        df = df.filter(~pl.col(drop).is_in(exclude))

    new_cols = tuple(c for c in spec.group_cols if c != drop)
    new_spec = PipelineSpec(group_cols=new_cols)

    result = df.group_by("timestamp", *new_cols).agg(pl.col("count").sum())
    return result, new_spec


def compute_derived_metrics(df: pl.LazyFrame, spec: PipelineSpec) -> pl.LazyFrame:
    """Compute available and idle from base metrics.

    available = reservable - committed
    idle = committed - occupied
    """
    spec.validate_stage(df, "count")

    # Pivot needs all non-value columns as index
    index_cols = ["timestamp", *[c for c in spec.group_cols if c != "quantity_type"]]

    pivoted = df.collect().pivot(on="quantity_type", index=index_cols, values="count")
    cols = pivoted.columns

    if QT.RESERVABLE in cols and QT.COMMITTED in cols:
        pivoted = pivoted.with_columns(
            (pl.col(QT.RESERVABLE) - pl.col(QT.COMMITTED)).alias(QT.AVAILABLE),
        )

    if QT.COMMITTED in cols and QT.OCCUPIED in cols:
        pivoted = pivoted.with_columns(
            (pl.col(QT.COMMITTED) - pl.col(QT.OCCUPIED)).alias(QT.IDLE),
        )

    result = (
        pivoted.unpivot(
            index=index_cols, variable_name="quantity_type", value_name="count"
        )
        .drop_nulls("count")
        .lazy()
    )

    spec.validate_stage(result, "count")
    return result


def add_site_context(
    df: pl.LazyFrame, spec: PipelineSpec, site: str, collector_type: str = "current"
) -> pl.LazyFrame:
    """Add site and collector_type columns."""
    spec.validate_stage(df, "count")
    return df.with_columns(
        pl.lit(site).alias("site"),
        pl.lit(collector_type).alias("collector_type"),
    )
