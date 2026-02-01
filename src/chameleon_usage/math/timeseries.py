"""Generic time series transforms. No domain knowledge."""

import polars as pl


def align_timestamps() -> pl.LazyFrame: ...
def resample() -> pl.LazyFrame: ...
def to_wide() -> pl.DataFrame:
    """Pivot long → wide. Calls collect()."""
    ...


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
