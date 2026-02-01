"""Domain-aware pipeline wrappers with validation.

PIPELINE STAGES:
    intervals_to_counts  → sweepline: [start,end) → point-in-time counts
    align_timestamps     → forward-fill to union of timestamps (required before derived)
    compute_derived_metrics → available = reservable - committed, etc.
    resample             → time-weighted bucketing for plotting

USE run_pipeline() for the standard flow. Individual functions for custom pipelines.
"""

import polars as pl

from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.math import sweepline, timeseries
from chameleon_usage.schemas import PipelineSpec


def run_pipeline(
    intervals: pl.LazyFrame,
    spec: PipelineSpec,
    resample_interval: str | None = None,
) -> pl.LazyFrame:
    """Standard pipeline: intervals → aligned counts with derived metrics.

    Args:
        intervals: Raw interval data with [entity_id, start, end, *group_cols]
        spec: Pipeline config with group_cols and time_range
        resample_interval: Optional bucket size (e.g. "1d") for time-weighted resampling

    Returns:
        Counts with derived metrics, optionally resampled.
    """
    counts = intervals_to_counts(intervals, spec)
    counts = clip_to_window(counts, spec)
    aligned = align_timestamps(counts, spec)
    derived = compute_derived_metrics(aligned, spec)

    if resample_interval:
        return resample(derived, resample_interval, spec)
    return derived


# =============================================================================
# INDIVIDUAL STAGES - for custom pipelines
# =============================================================================


def intervals_to_counts(df: pl.LazyFrame, spec: PipelineSpec) -> pl.LazyFrame:
    """Intervals → counts via sweepline."""
    spec.validate_stage(df, "interval")
    result = sweepline.intervals_to_counts(df, "start", "end", list(spec.group_cols))
    spec.validate_stage(result, "count")
    return result


def clip_to_window(df: pl.LazyFrame, spec: PipelineSpec) -> pl.LazyFrame:
    """Filter to timestamps within spec.time_range."""
    spec.validate_stage(df, "count")
    start, end = spec.time_range
    return df.filter((pl.col("timestamp") >= start) & (pl.col("timestamp") <= end))


def align_timestamps(df: pl.LazyFrame, spec: PipelineSpec) -> pl.LazyFrame:
    """Forward-fill counts to union of timestamps across groups.

    Required before compute_derived_metrics - can't subtract misaligned series.
    Fills nulls with 0 (sweepline counts are 0 before first event, not null).
    """
    spec.validate_stage(df, "count")
    result = timeseries.align_step_functions(
        df, "timestamp", "count", list(spec.group_cols)
    )
    return result.with_columns(pl.col("count").fill_null(0))


def resample(df: pl.LazyFrame, interval: str, spec: PipelineSpec) -> pl.LazyFrame:
    """Time-weighted resample for step-function data.

    Values persist until next event, contributing proportionally to buckets.
    """
    spec.validate_stage(df, "count")
    result = timeseries.resample_step_function(
        df, "timestamp", "count", interval, list(spec.group_cols), spec.time_range
    )
    return result.with_columns(pl.col("count").fill_null(0))


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
    new_spec = PipelineSpec(group_cols=new_cols, time_range=spec.time_range)

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
