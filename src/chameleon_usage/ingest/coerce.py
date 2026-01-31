"""
Clamp child intervals to fit within parent windows.

Problem: Child records have [start, end] intervals that may extend beyond their
parent's lifetime. We need to clip children to their parent's window and tag
each row with what happened.

Matching: Each child is matched to ALL parents it overlaps (by join_keys).
A child overlaps a parent if their intervals intersect:
    child.start < parent.end  (or parent.end is null)
    child.end > parent.start  (or child.end is null)
One output row per (child, parent) overlap.

Clamping: For each overlap, clamp to the intersection:
    start = max(child.start, parent.start)
    end   = min(child.end, parent.end)      # null = infinity

Output columns added:
    original_start  - child's start before clamping
    original_end    - child's end before clamping
    coerce_status   - what happened:
        "valid"       : child fully inside parent, no change needed
        "clamped"     : child was clipped to fit parent
        "no_parent"   : no overlapping parent found for this join key
        "null_parent" : join key was null, cannot match

All rows preserved. Downstream decides what to do with each status.
"""

from typing import List

import polars as pl


def apply_temporal_clamp(
    children: pl.LazyFrame,
    parents: pl.LazyFrame,
    join_keys: List[str],
) -> pl.LazyFrame:
    children = children.with_columns(
        pl.col("start").alias("original_start"),
        pl.col("end").alias("original_end"),
    ).with_row_index("_row_id")

    # Null keys can't match - handle separately
    has_null_key = pl.any_horizontal(*[pl.col(k).is_null() for k in join_keys])
    null_key_rows = children.filter(has_null_key)
    matchable = children.filter(~has_null_key)

    # Cast to match parent dtypes (avoids schema mismatch on empty frames)
    matchable = matchable.cast({k: parents.collect_schema()[k] for k in join_keys})

    # Join children to all parents with same key
    parent_windows = parents.select(
        *join_keys,
        pl.col("start").alias("_p_start"),
        pl.col("end").alias("_p_end"),
    )
    joined = matchable.join(parent_windows, on=join_keys, how="left")

    # Overlap: child.start < parent.end AND child.end > parent.start (null = infinity)
    child_starts_before_parent_ends = pl.col("_p_end").is_null() | (
        pl.col("original_start") < pl.col("_p_end")
    )
    child_ends_after_parent_starts = pl.col("original_end").is_null() | (
        pl.col("original_end") > pl.col("_p_start")
    )
    overlaps = child_starts_before_parent_ends & child_ends_after_parent_starts

    # Split: rows that overlap vs rows with no overlapping parent
    has_parent = pl.col("_p_start").is_not_null()
    matched = joined.filter(has_parent & overlaps)

    # Children with no overlapping parent: exclude any child that has at least one match
    matched_row_ids = matched.select("_row_id").unique()
    no_parent_rows = (
        matchable.join(matched_row_ids, on="_row_id", how="anti")
        .with_columns(pl.lit("no_parent").alias("coerce_status"))
    )

    # Determine status and clamp matched rows
    start_outside = pl.col("original_start") < pl.col("_p_start")
    end_outside = pl.col("_p_end").is_not_null() & (
        pl.col("original_end").is_null() | (pl.col("original_end") > pl.col("_p_end"))
    )
    result = matched.with_columns(
        pl.when(start_outside | end_outside)
        .then(pl.lit("clamped"))
        .otherwise(pl.lit("valid"))
        .alias("coerce_status"),
        pl.max_horizontal("original_start", "_p_start").alias("start"),
        pl.min_horizontal("original_end", "_p_end").alias("end"),
    ).drop("_p_start", "_p_end")

    # Reassemble: matched + unmatched + null-key rows
    null_result = null_key_rows.with_columns(
        pl.lit("null_parent").alias("coerce_status")
    )
    return (
        pl.concat([result, no_parent_rows, null_result], how="diagonal")
        .sort("_row_id")
        .drop("_row_id")
    )
