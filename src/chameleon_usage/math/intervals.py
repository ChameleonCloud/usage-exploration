"""Interval intersection: tag intervals with state from a second interval stream."""

import polars as pl


def tag_intervals(
    intervals: pl.LazyFrame,
    events: pl.LazyFrame,
    by: str,
    tag_col: str = "usable",
    event_start_col: str = "event_start",
    event_end_col: str = "event_end",
) -> pl.LazyFrame:
    """Split intervals at state change boundaries, adding a tag column.

    For each interval, finds overlapping event periods (by host) and clips
    the interval to each. Intervals with no matching events get tag=null.

    Args:
        intervals: Must have [start, end, <by>] plus any other columns.
        events: Must have [<by>, event_start, event_end, <tag_col>].
        by: Join key (e.g. "host", "hypervisor_hostname").
        tag_col: Column name from events to carry onto output.
    """
    interval_cols = intervals.collect_schema().names()

    # Split into matched (has host in events) and unmatched
    joined = intervals.join(events, on=by, how="left")

    matched = joined.filter(pl.col(event_start_col).is_not_null())
    unmatched = joined.filter(pl.col(event_start_col).is_null())

    # For matched: clip interval to the overlap with each event period
    # overlap = [max(start, event_start), min(end, event_end))
    clipped = (
        matched.with_columns(
            pl.max_horizontal("start", event_start_col).alias("_clip_start"),
            pl.min_horizontal("end", event_end_col).alias("_clip_end"),
        )
        .filter(
            # Keep only actual overlaps: clip_start < clip_end
            # Handle nulls (open-ended): null end means "forever", so always overlaps
            pl.col("_clip_end").is_null()
            | (pl.col("_clip_start") < pl.col("_clip_end"))
        )
        .with_columns(
            pl.col("_clip_start").alias("start"),
            pl.col("_clip_end").alias("end"),
        )
        .select(*interval_cols, tag_col)
    )

    # For unmatched: keep original interval, tag=null
    unmatched_out = unmatched.select(
        *interval_cols,
        pl.lit(None).cast(pl.Boolean).alias(tag_col),
    )

    return pl.concat([clipped, unmatched_out], how="diagonal")
