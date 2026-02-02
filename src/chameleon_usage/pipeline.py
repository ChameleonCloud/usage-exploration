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
from chameleon_usage.constants import SchemaCols as S
from chameleon_usage.math import sweepline, timeseries
from chameleon_usage.schemas import (
    IntervalModel,
    PipelineSpec,
    TimelineModel,
    UsageModel,
)


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
    spec.validate_against(intervals)

    counts = intervals_to_counts(intervals, spec)
    counts = clip_to_window(counts, spec)
    aligned = align_timestamps(counts, spec)
    derived = compute_derived_metrics(aligned, spec)

    if resample_interval:
        resampled = resample(derived, resample_interval, spec)
        return resampled

    return derived


# =============================================================================
# INDIVIDUAL STAGES - for custom pipelines
# =============================================================================


def intervals_to_counts(df: pl.LazyFrame, spec: PipelineSpec) -> pl.LazyFrame:
    """Intervals → counts via sweepline."""
    df = IntervalModel.validate(df)
    result = sweepline.intervals_to_counts(
        df, "start", "end", list(spec.group_cols), value_col="value"
    )
    result = TimelineModel.validate(result)
    return result


def clip_to_window(df: pl.LazyFrame, spec: PipelineSpec) -> pl.LazyFrame:
    """Drop events after window end. Keep pre-window events for join_asof."""
    df = TimelineModel.validate(df)
    _, end = spec.time_range
    return df.filter(pl.col("timestamp") <= end)


def align_timestamps(df: pl.LazyFrame, spec: PipelineSpec) -> pl.LazyFrame:
    """Forward-fill counts to union of timestamps across groups.

    Required before compute_derived_metrics - can't subtract misaligned series.
    Fills nulls with 0 (sweepline counts are 0 before first event, not null).
    """
    df = TimelineModel.validate(df)
    result = timeseries.align_step_functions(
        df, "timestamp", "value", list(spec.group_cols)
    )
    return result.with_columns(pl.col("value").fill_null(0))


def resample(df: pl.LazyFrame, interval: str, spec: PipelineSpec) -> pl.LazyFrame:
    """Time-weighted resample for step-function data.

    Values persist until next event, contributing proportionally to buckets.
    """
    spec.validate_against(df)
    df = TimelineModel.validate(df)
    result = timeseries.resample_step_function(
        df, "timestamp", "value", interval, list(spec.group_cols), spec.time_range
    )
    return result.with_columns(pl.col("value").fill_null(0))


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

    print(f"rows into pivot: {df.collect().height}")
    new_cols = tuple(c for c in spec.group_cols if c != drop)
    new_spec = PipelineSpec(group_cols=new_cols, time_range=spec.time_range)

    result = df.group_by("timestamp", *new_cols).agg(pl.col("value").sum())
    return result, new_spec


def compute_derived_metrics(df: pl.LazyFrame, spec: PipelineSpec) -> pl.LazyFrame:
    """Compute available and idle from base metrics.

    available = reservable - committed
    idle = committed - occupied
    """
    df = TimelineModel.validate(df)

    # Pivot needs all non-value columns as index
    index_cols = ["timestamp", *[c for c in spec.group_cols if c != "metric"]]

    pivoted = df.collect().pivot(on="metric", index=index_cols, values="value")
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
        pivoted.unpivot(index=index_cols, variable_name="metric", value_name="value")
        .drop_nulls(S.VALUE)
        .lazy()
    )

    result = TimelineModel.validate(result)
    return result


def add_site_context(
    df: pl.LazyFrame, spec: PipelineSpec, site: str, collector_type: str = "current"
) -> pl.LazyFrame:
    """Add site and collector_type columns."""
    df = TimelineModel.validate(df)
    return df.with_columns(
        pl.lit(site).alias("site"),
        pl.lit(collector_type).alias("collector_type"),
    )
