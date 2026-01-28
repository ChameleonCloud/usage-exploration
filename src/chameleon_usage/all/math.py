# module for "pure" math operations, no domain knowledge
from datetime import datetime

import polars as pl


def spans_to_events(
    spans: pl.LazyFrame, group_cols: list[str] | None = None
) -> pl.LazyFrame:
    """
    Takes dataframe of canonical spans, splits to sorted event stream.

    Output columns:
    - timestamp: Datetime
    - delta: Int (+1 at start, -1 at end)
    - source: str (span type / series)
    - hypervisor_hostname (or resource_id): str (for per-host checks)
    """
    keys = group_cols or ["source", "resource_id"]
    start_events = spans.select(
        *keys,
        pl.col("start").alias("timestamp"),
        pl.lit(1).alias("delta"),
    )
    end_events = spans.select(
        *keys,
        pl.col("end").alias("timestamp"),
        pl.lit(-1).alias("delta"),
    )
    return pl.concat([start_events, end_events])


def sweepline(events: pl.LazyFrame) -> pl.LazyFrame:
    """Compute concurrent counts for each grouping in events.

    Input columns:
    - timestamp: timestamp of event
    - delta: change in count (+1 for start, -1 for end)
    - ...group_cols: any other columns are treated as grouping keys

    Output columns:
    - timestamp: timestamp (start of interval)
    - interval_end: timestamp of next event (end of interval, NULL for last)
    - concurrent: cumulative count at this timestamp
    - ...group_cols: preserved from input
    """

    group_cols = [
        c for c in events.collect_schema().names() if c not in ("timestamp", "delta")
    ]
    if not group_cols:
        raise ValueError("events must include at least one grouping column")

    return (
        events.sort("timestamp")
        .group_by([*group_cols, "timestamp"], maintain_order=True)
        .agg(pl.col("delta").sum())
        .sort([*group_cols, "timestamp"])
        .with_columns(pl.col("delta").cum_sum().over(group_cols).alias("concurrent"))
        .with_columns(
            pl.col("timestamp").shift(-1).over(group_cols).alias("interval_end")
        )
    )


def add_interval_duration(
    timeline: pl.LazyFrame,
    time_col: str = "timestamp",
    end_col: str = "interval_end",
) -> pl.LazyFrame:
    """Add duration column (in seconds) for each interval in a step function."""
    return timeline.filter(pl.col(end_col).is_not_null()).with_columns(
        duration=(pl.col(end_col) - pl.col(time_col)).dt.total_seconds()
    )


def resample_mean(
    df: pl.LazyFrame,
    time_col: str,
    value_col: str,
    every: str,
    group_by: str | None = None,
) -> pl.LazyFrame:
    """Resample using simple mean within each bucket."""
    return (
        df.sort([group_by, time_col] if group_by else [time_col])
        .group_by_dynamic(time_col, every=every, group_by=group_by)
        .agg(pl.col(value_col).mean().alias(value_col))
        .sort(time_col)
    )


def resample_weighted_mean(
    df: pl.LazyFrame,
    time_col: str,
    value_col: str,
    weight_col: str,
    every: str,
    group_by: str | None = None,
) -> pl.LazyFrame:
    """Resample using weighted mean: sum(value * weight) / sum(weight)."""
    return (
        df.sort([group_by, time_col] if group_by else [time_col])
        .group_by_dynamic(time_col, every=every, group_by=group_by)
        .agg(
            (pl.col(value_col) * pl.col(weight_col)).sum().alias("_weighted_sum"),
            pl.col(weight_col).sum().alias("_total_weight"),
        )
        .with_columns((pl.col("_weighted_sum") / pl.col("_total_weight")).alias(value_col))
        .drop("_weighted_sum", "_total_weight")
        .sort(time_col)
    )


def sweepline_to_wide(
    timeline: pl.LazyFrame,
    time_col: str = "timestamp",
    end_col: str = "interval_end",
    series_col: str = "source",
    value_col: str = "concurrent",
    every: str = "1d",
    window_start: datetime | None = None,
    window_end: datetime | None = None,
) -> pl.DataFrame:
    """Resample step function using time-weighted average and pivot to wide format."""
    lf = timeline
    if window_end is not None:
        lf = lf.with_columns(pl.col(end_col).fill_null(pl.lit(window_end)))
    if window_start is not None:
        lf = lf.with_columns(
            pl.when(pl.col(time_col) < window_start)
            .then(pl.lit(window_start))
            .otherwise(pl.col(time_col))
            .alias(time_col)
        )
    if window_end is not None:
        lf = lf.with_columns(
            pl.when(pl.col(end_col) > window_end)
            .then(pl.lit(window_end))
            .otherwise(pl.col(end_col))
            .alias(end_col)
        )
    lf = lf.filter(pl.col(end_col).is_not_null() & (pl.col(end_col) > pl.col(time_col)))

    with_duration = add_interval_duration(lf, time_col=time_col, end_col=end_col)
    resampled = resample_weighted_mean(
        with_duration,
        time_col=time_col,
        value_col=value_col,
        weight_col="duration",
        every=every,
        group_by=series_col,
    ).collect()
    wide = resampled.pivot(
        on=series_col,
        index=time_col,
        values=value_col,
    ).sort(time_col)

    value_cols = [c for c in wide.columns if c != time_col]
    return wide.with_columns(
        [pl.col(c).fill_null(strategy="forward") for c in value_cols]
    )


def filter_overlapping(
    spans: pl.LazyFrame,
    start_col: str,
    end_col: str,
    window_start: datetime,
    window_end: datetime,
) -> pl.LazyFrame:
    """Keep spans that touch [window_start, window_end)."""
    return spans.filter(
        (pl.col(start_col) < window_end)
        & ((pl.col(end_col) > window_start) | pl.col(end_col).is_null())
    )


def clip_timeline(
    timeline: pl.LazyFrame,
    time_col: str,
    window_start: datetime,
    window_end: datetime,
) -> pl.LazyFrame:
    """Keep rows within [window_start, window_end)."""
    return timeline.filter(
        (pl.col(time_col) >= window_start) & (pl.col(time_col) < window_end)
    )
