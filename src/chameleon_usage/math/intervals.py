"""Tag intervals with state from a second interval stream.

Given a set of intervals and a set of per-host "events" (time ranges
carrying a tag value), split each interval so every piece carries the
correct tag for its time range. The algorithm has two phases:

1. Normalize events: extend the event stream so that for every host that
   appears, the events form a partition of time — no overlaps (raise on
   detection) and no gaps (uncovered ranges become explicit rows with
   ``tag=null``). After this, events for a host tile ``(-inf, +inf)``.

2. Clip: left-join intervals onto normalized events and clip each joined
   pair to the overlap. An interval that spans N event rows on its host
   produces N output rows, each clipped to its event's range; because
   events are a partition, those N clipped pieces abut and together tile
   the original interval exactly. No duration is lost, none is doubled.
"""

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

    For each input row, emits one or more output rows whose [start, end)
    pieces cover the input exactly. Each piece carries the tag of the
    event covering it, or ``null`` where no event covers that time.

    Raises ``ValueError`` if two events for the same ``by`` key overlap.

    Args:
        intervals: Must have [start, end, <by>] plus any other columns.
        events: Must have [<by>, event_start, event_end, <tag_col>].
        by: Join key (e.g. "hypervisor_hostname").
        tag_col: Column from events to carry onto output.
    """
    interval_cols = intervals.collect_schema().names()
    events = _normalize_events(events, by, event_start_col, event_end_col, tag_col)

    # Left-join then clip each pair to its overlap. Null event bounds
    # mean "no constraint from this side"; max/min_horizontal skip nulls,
    # so bookend rows (one null bound) and unmatched hosts (both null)
    # both fall back to the interval's own bounds.
    return (
        intervals.join(events, on=by, how="left")
        .with_columns(
            pl.max_horizontal("start", event_start_col).alias("_clip_start"),
            pl.min_horizontal("end", event_end_col).alias("_clip_end"),
        )
        .filter(
            pl.col("_clip_end").is_null()
            | (pl.col("_clip_start") < pl.col("_clip_end"))
        )
        .with_columns(
            pl.col("_clip_start").alias("start"),
            pl.col("_clip_end").alias("end"),
        )
        .select(*interval_cols, tag_col)
    )


def _normalize_events(
    events: pl.LazyFrame,
    by: str,
    start_col: str,
    end_col: str,
    tag_col: str,
) -> pl.LazyFrame:
    """Return events extended so per-host coverage is total.

    Uncovered time comes in three flavors; each becomes a row with tag=null:
      1. Before the first event on a host:     ``(null, first.start)``
      2. Between consecutive events on a host: ``(prev.end, next.start)``
      3. After the last event on a host:       ``(last.end, null)`` —
         unless an event already has ``end=null`` (still ongoing).

    ``null`` in a bound means "no constraint"; the caller's clip step
    treats it as the interval's own bound.
    """
    events = events.sort([by, start_col])
    _raise_on_overlaps(events, by, start_col, end_col)

    schema = events.collect_schema()
    null_tag = pl.lit(None, dtype=pl.Boolean).alias(tag_col)

    # (2) Between-event gaps: rows where start > prev.end on the same host.
    between = (
        events.with_columns(pl.col(end_col).shift(1).over(by).alias("_prev_end"))
        .filter(
            pl.col("_prev_end").is_not_null()
            & (pl.col(start_col) > pl.col("_prev_end"))
        )
        .select(
            pl.col(by),
            pl.col("_prev_end").alias(start_col),
            pl.col(start_col).alias(end_col),
            null_tag,
        )
    )

    # (1) and (3) need per-host first-start and last-end.
    bounds = events.group_by(by).agg(
        pl.col(start_col).min().alias("_first"),
        pl.col(end_col).max().alias("_last"),
        pl.col(end_col).is_null().any().alias("_already_open"),
    )
    before = bounds.select(
        pl.col(by),
        pl.lit(None, dtype=schema[start_col]).alias(start_col),
        pl.col("_first").alias(end_col),
        null_tag,
    )
    after = bounds.filter(~pl.col("_already_open")).select(
        pl.col(by),
        pl.col("_last").alias(start_col),
        pl.lit(None, dtype=schema[end_col]).alias(end_col),
        null_tag,
    )

    return pl.concat([events, before, between, after], how="diagonal_relaxed")


def _raise_on_overlaps(
    events: pl.LazyFrame, by: str, start_col: str, end_col: str
) -> None:
    """Raise if any event's start precedes the previous event's end on the same host."""
    overlaps = (
        events.with_columns(pl.col(end_col).shift(1).over(by).alias("_prev_end"))
        .filter(
            pl.col("_prev_end").is_not_null()
            & (pl.col(start_col) < pl.col("_prev_end"))
        )
        .collect()
    )
    if overlaps.height > 0:
        raise ValueError(
            f"overlapping tag events on key {by!r}: {overlaps.height} row(s); "
            f"sample:\n{overlaps.head(5)}"
        )
