"""Pure functions for pipeline stages.

This module contains only pure functions for the pipeline stages.
Dataframe in -> Dataframe out.
0 domain knowedge, data types and column names are abstracted.


These methods must *never* call collect()

adapters → intervals [entity_id, start, end, quantity_type, source]
  → core.intervals_to_deltas
  → core.deltas_to_counts
  → usage [timestamp, quantity_type, count]
"""

import polars as pl


def intervals_to_deltas(
    df: pl.LazyFrame,
    start_col: str,
    end_col: str,
    group_cols: list[str],
) -> pl.LazyFrame:
    """[start, end) intervals → +1 at start, -1 at end."""
    starts = df.select(
        pl.col(start_col).alias("timestamp"),
        *[pl.col(c) for c in group_cols],
        pl.lit(1).alias("change"),
    )
    ends = df.filter(pl.col(end_col).is_not_null()).select(
        pl.col(end_col).alias("timestamp"),
        *[pl.col(c) for c in group_cols],
        pl.lit(-1).alias("change"),
    )
    return pl.concat([starts, ends])


def deltas_to_counts(
    df: pl.LazyFrame,
    group_cols: list[str],
) -> pl.LazyFrame:
    """Aggregate deltas by timestamp, cumsum per group."""
    return (
        df.group_by(["timestamp", *group_cols])
        .agg(pl.col("change").sum())
        .sort(group_cols + ["timestamp"])
        .with_columns(pl.col("change").cum_sum().over(group_cols).alias("count"))
        .drop("change")
    )


def intervals_to_counts(
    df: pl.LazyFrame,
    start_col: str,
    end_col: str,
    group_cols: list[str],
) -> pl.LazyFrame:
    deltas = intervals_to_deltas(df, start_col, end_col, group_cols)
    counts = deltas_to_counts(deltas, group_cols)
    return counts


def resample(
    df: pl.LazyFrame,
    timestamp_col: str,
    value_col: str,
    interval: str,
    group_cols: list[str],
) -> pl.LazyFrame:
    """Resample time series to regular intervals.

    Assigns each record to its start bucket and averages values.
    TODO: how are nulls handled?
    TODO: Are timestamps aligned between group_cols?
    """
    return (
        df.with_columns(pl.col(timestamp_col).dt.truncate(interval).alias("_bucket"))
        .group_by(["_bucket", *group_cols])
        .agg(pl.col(value_col).mean())
        .rename({"_bucket": timestamp_col})
        .sort([timestamp_col, *group_cols])
    )
