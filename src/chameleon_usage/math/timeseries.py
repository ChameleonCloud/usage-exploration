"""Generic time series transforms. No domain knowledge."""

from datetime import datetime

import polars as pl


def align_step_functions(
    df: pl.LazyFrame,
    timestamp_col: str,
    value_col: str,
    group_cols: list[str],
) -> pl.LazyFrame:
    """Forward-fill to union of timestamps. Preserves original timestamps."""
    all_ts = df.select(timestamp_col).unique()
    groups = df.select(group_cols).unique()
    scaffold = groups.join(all_ts, how="cross")

    return (
        scaffold.join(df, on=[*group_cols, timestamp_col], how="left")
        .sort([*group_cols, timestamp_col])
        .with_columns(pl.col(value_col).forward_fill().over(group_cols))
    )


def resample_step_function(
    df: pl.LazyFrame,
    timestamp_col: str,
    value_col: str,
    interval: str,
    group_cols: list[str],
    time_range: tuple[datetime, datetime],
) -> pl.LazyFrame:
    """Resample step-function data to regular intervals using point-in-time sampling.

    For each bucket timestamp, finds the most recent event value via join_asof.
    This gives the instantaneous value at each bucket boundary.

    For daily usage charts this is correct: "what was the count at midnight each day?"
    """
    start, end = time_range

    # Create bucket timestamps
    buckets = (
        pl.datetime_range(start, end, interval, eager=True)
        .alias(timestamp_col)
        .to_frame()
        .lazy()
        .filter(pl.col(timestamp_col) < end)
    )

    # Cross with groups to get scaffold
    groups = df.select(group_cols).unique()
    scaffold = groups.join(buckets, how="cross")

    # For each bucket, find the most recent event value
    sorted_events = df.sort([*group_cols, timestamp_col])
    return (
        scaffold.join_asof(
            sorted_events,
            on=timestamp_col,
            by=group_cols,
            strategy="backward",
        )
        .with_columns(pl.col(value_col).fill_null(0))
        .sort([*group_cols, timestamp_col])
    )


# TODO: Duration-weighted resampling for resource-hours calculation
#
# The current resample_step_function uses point-in-time sampling (join_asof),
# which answers: "what was the value at this moment?"
#
# For resource-hours (e.g., "total CPU-hours used this month"), we need
# duration-weighted averaging: each event's value contributes proportionally
# to the time it was active within the bucket.
#
# Example: bucket is [00:00, 24:00), event A (value=10) from 00:00-12:00,
# event B (value=20) from 12:00-24:00. Point-in-time at 00:00 gives 10.
# Duration-weighted gives (10*12h + 20*12h) / 24h = 15.
#
# Implementation approach:
# 1. Add _valid_until = shift(-1).over(group_cols).fill_null(end)
# 2. For each event, find which buckets it overlaps
# 3. Clip event duration to bucket boundaries
# 4. Compute weighted sum: sum(value * duration) / sum(duration)
#
# The naive cross-join approach (N_events × N_buckets) is O(n²) and slow.
# Better approaches:
# - Segment tree / interval tree for overlap queries
# - Sort-merge join on bucket boundaries
# - Cumulative sum trick: convert to cumsum, sample at bucket edges, diff
