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
    """Resample step-function data with duration weighting.

    Events span bucket boundaries: an event persists until the next event,
    contributing proportionally to each bucket it overlaps.
    """
    start, end = time_range

    # 1. Add "valid until" timestamp for each event
    events = df.sort([*group_cols, timestamp_col]).with_columns(
        pl.col(timestamp_col)
        .shift(-1)
        .over(group_cols)
        .fill_null(end)
        .alias("_valid_until")
    )

    # 2. Create bucket scaffold
    bucket_starts = (
        pl.datetime_range(start, end, interval, eager=True)
        .alias("_bucket_start")
        .to_frame()
        .lazy()
        .filter(pl.col("_bucket_start") < end)
    )
    bucket_starts = bucket_starts.with_columns(
        pl.col("_bucket_start")
        .dt.offset_by(interval)
        .clip(upper_bound=end)
        .alias("_bucket_end")
    )
    groups = df.select(group_cols).unique()
    scaffold = groups.join(bucket_starts, how="cross")

    # 3. Join events to buckets where event overlaps bucket
    # Event [ts, valid_until) overlaps bucket [bucket_start, bucket_end) if:
    #   ts < bucket_end AND valid_until > bucket_start
    joined = scaffold.join(events, on=group_cols, how="left").filter(
        (pl.col(timestamp_col) < pl.col("_bucket_end"))
        & (pl.col("_valid_until") > pl.col("_bucket_start"))
    )

    # 4. Clip duration to bucket boundaries
    joined = joined.with_columns(
        (
            pl.min_horizontal(pl.col("_valid_until"), pl.col("_bucket_end"))
            - pl.max_horizontal(pl.col(timestamp_col), pl.col("_bucket_start"))
        )
        .dt.total_microseconds()
        .alias("_duration_us")
    )

    # 5. Weighted average per bucket
    aggregated = (
        joined.group_by([*group_cols, "_bucket_start"])
        .agg(
            (pl.col(value_col) * pl.col("_duration_us")).sum().alias("_weighted"),
            pl.col("_duration_us").sum().alias("_total"),
        )
        .with_columns((pl.col("_weighted") / pl.col("_total")).alias(value_col))
        .drop(["_weighted", "_total"])
        .rename({"_bucket_start": timestamp_col})
    )

    # 6. Rejoin to scaffold to get nulls for buckets with no events
    scaffold_clean = scaffold.select([*group_cols, "_bucket_start"]).rename(
        {"_bucket_start": timestamp_col}
    )
    return scaffold_clean.join(
        aggregated, on=[*group_cols, timestamp_col], how="left"
    ).sort([*group_cols, timestamp_col])
