"""Pure functions for pipeline stages.

This module contains only pure functions for the pipeline stages.
Dataframe in -> Dataframe out.
0 domain knowedge, data types and column names are abstracted.


These methods must *never* call collect()

adapters → intervals [entity_id, start, end, metric, source]
  → core.intervals_to_deltas
  → core.deltas_to_counts
  → usage [timestamp, metric, value]
"""

import polars as pl

# Module-private constants
_DELTA_COL = "change"
_TIME_COL = "timestamp"


def intervals_to_deltas(
    df: pl.LazyFrame,
    start_col: str,
    end_col: str,
    group_cols: list[str],
    value_col: str | None = None,
) -> pl.LazyFrame:
    """[start, end) intervals → +value at start, -value at end.

    Args:
        value_col: Column with resource quantity. If None, counts intervals (+1/-1).
    """
    delta_expr = pl.col(value_col) if value_col else pl.lit(1)

    starts = df.select(
        pl.col(start_col).alias(_TIME_COL),
        *[pl.col(c) for c in group_cols],
        delta_expr.alias(_DELTA_COL),
    )
    ends = df.filter(pl.col(end_col).is_not_null()).select(
        pl.col(end_col).alias(_TIME_COL),
        *[pl.col(c) for c in group_cols],
        (-delta_expr).alias(_DELTA_COL),
    )
    return pl.concat([starts, ends])


def deltas_to_counts(
    df: pl.LazyFrame,
    group_cols: list[str],
) -> pl.LazyFrame:
    """Aggregate deltas by timestamp, cumsum per group."""
    return (
        df.group_by([_TIME_COL, *group_cols])
        .agg(pl.col(_DELTA_COL).sum())
        .sort(group_cols + [_TIME_COL])
        .with_columns(pl.col(_DELTA_COL).cum_sum().over(group_cols).alias("value"))
        .drop(_DELTA_COL)
    )


def intervals_to_counts(
    df: pl.LazyFrame,
    start_col: str,
    end_col: str,
    group_cols: list[str],
    value_col: str | None = None,
) -> pl.LazyFrame:
    """Convert intervals to cumulative counts.

    Args:
        value_col: Column with resource quantity. If None, counts intervals.
    """
    deltas = intervals_to_deltas(df, start_col, end_col, group_cols, value_col)
    counts = deltas_to_counts(deltas, group_cols)
    return counts
