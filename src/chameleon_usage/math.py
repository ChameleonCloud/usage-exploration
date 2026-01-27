# module for "pure" math operations, no domain knowledge
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
    keys = group_cols or ["source", "hypervisor_hostname"]
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


def resample_steps(
    timeline: pl.LazyFrame,
    time_col: str = "timestamp",
    series_col: str = "source",
    value_col: str = "concurrent",
    every: str = "1d",
) -> pl.LazyFrame:
    return (
        timeline.sort([series_col, time_col])
        .group_by_dynamic(time_col, every=every, group_by=series_col)
        .agg(pl.col(value_col).last().alias(value_col))
        .sort(time_col)
    )


def sweepline_to_wide(
    timeline: pl.LazyFrame,
    time_col: str = "timestamp",
    series_col: str = "source",
    value_col: str = "concurrent",
    every: str = "1d",
) -> pl.DataFrame:
    daily = resample_steps(
        timeline,
        time_col=time_col,
        series_col=series_col,
        value_col=value_col,
        every=every,
    ).collect()
    wide = daily.pivot(
        on=series_col,
        index=time_col,
        values=value_col,
    ).sort(time_col)

    value_cols = [c for c in wide.columns if c != time_col]
    return wide.with_columns(
        [pl.col(c).fill_null(strategy="forward") for c in value_cols]
    )
