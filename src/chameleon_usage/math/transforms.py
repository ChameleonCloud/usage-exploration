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


class Sweepline:
    # internal columns for transform
    DELTA_COL = "change"
    TIME_COL = "timestamp"

    @classmethod
    def intervals_to_deltas(
        cls,
        df: pl.LazyFrame,
        start_col: str,
        end_col: str,
        group_cols: list[str],
    ) -> pl.LazyFrame:
        """[start, end) intervals → +1 at start, -1 at end."""
        starts = df.select(
            pl.col(start_col).alias(cls.TIME_COL),
            *[pl.col(c) for c in group_cols],
            pl.lit(1).alias(cls.DELTA_COL),
        )
        ends = df.filter(pl.col(end_col).is_not_null()).select(
            pl.col(end_col).alias(cls.TIME_COL),
            *[pl.col(c) for c in group_cols],
            pl.lit(-1).alias(cls.DELTA_COL),
        )
        return pl.concat([starts, ends])

    @classmethod
    def deltas_to_counts(
        cls,
        df: pl.LazyFrame,
        group_cols: list[str],
    ) -> pl.LazyFrame:
        """Aggregate deltas by timestamp, cumsum per group."""
        return (
            df.group_by([cls.TIME_COL, *group_cols])
            .agg(pl.col(cls.DELTA_COL).sum())
            .sort(group_cols + [cls.TIME_COL])
            .with_columns(
                pl.col(cls.DELTA_COL).cum_sum().over(group_cols).alias("count")
            )
            .drop(cls.DELTA_COL)
        )

    @classmethod
    def intervals_to_counts(
        cls,
        df: pl.LazyFrame,
        start_col: str,
        end_col: str,
        group_cols: list[str],
    ) -> pl.LazyFrame:
        deltas = cls.intervals_to_deltas(df, start_col, end_col, group_cols)
        counts = cls.deltas_to_counts(deltas, group_cols)
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
