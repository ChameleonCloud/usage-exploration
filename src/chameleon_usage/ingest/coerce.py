"""
Clamp child intervals to fit within parent (validator) windows.

Problem: Child records have [start, end] intervals that may extend beyond their
parent's lifetime. We need to clip children to their parent's window and tag
each row with what happened.

Matching: Each child is matched to ONE parent by join_keys. When multiple parents
exist for the same key (e.g., multiple "eras"), we match the parent whose start
is most recent but still <= child.start.

Clamping:
    start = max(child.start, parent.start)
    end   = min(child.end, parent.end)      # null = infinity

Output columns added:
    original_start  - child's start before clamping
    original_end    - child's end before clamping
    coerce_status   - what happened:
        "valid"       : child fully inside parent, no change needed
        "clamped"     : child was clipped to fit parent
        "no_overlap"  : matched a parent but child.start >= parent.end
        "no_parent"   : no parent found for this join key
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

    # Step 1: Match each child to a parent
    matched = _match_child_to_parent(matchable, parents, join_keys)

    # Step 2: Determine status and apply clamping
    no_match = pl.col("_parent_start").is_null()
    no_overlap = pl.col("_parent_end").is_not_null() & (
        pl.col("original_start") >= pl.col("_parent_end")
    )
    start_outside = pl.col("original_start") < pl.col("_parent_start")
    end_outside = pl.col("_parent_end").is_not_null() & (
        pl.col("original_end").is_null()
        | (pl.col("original_end") > pl.col("_parent_end"))
    )
    preserve_original = no_match | no_overlap

    status = (
        pl.when(no_match)
        .then(pl.lit("no_parent"))
        .when(no_overlap)
        .then(pl.lit("no_overlap"))
        .when(start_outside | end_outside)
        .then(pl.lit("clamped"))
        .otherwise(pl.lit("valid"))
        .alias("coerce_status")
    )
    clamped_start = (
        pl.when(preserve_original)
        .then(pl.col("original_start"))
        .otherwise(pl.max_horizontal("original_start", "_parent_start"))
        .alias("start")
    )
    clamped_end = (
        pl.when(preserve_original)
        .then(pl.col("original_end"))
        .otherwise(pl.min_horizontal("original_end", "_parent_end"))
        .alias("end")
    )

    result = matched.with_columns(status, clamped_start, clamped_end).drop(
        "_parent_start", "_parent_end"
    )

    # Reassemble: add back null-key rows with null_parent status
    null_result = null_key_rows.with_columns(
        pl.lit("null_parent").alias("coerce_status")
    )
    return (
        pl.concat([result, null_result], how="diagonal").sort("_row_id").drop("_row_id")
    )


def _match_child_to_parent(
    children: pl.LazyFrame,
    parents: pl.LazyFrame,
    join_keys: List[str],
) -> pl.LazyFrame:
    """
    Match each child to at most one parent window. Adds _parent_start/_parent_end.

    Two cases:
    1. Normal: child starts during or after some parent's start
       → backward ASOF finds most recent parent where parent.start <= child.start

    2. Early starter: child starts BEFORE all parents for its key
       → forward ASOF finds first parent the child might overlap
       → but only if child.end > parent.start (otherwise no overlap)
    """
    parent_windows = parents.sort("start").select(
        *join_keys,
        pl.col("start").alias("_parent_start"),
        pl.col("end").alias("_parent_end"),
    )

    # Identify early starters: children that start before all parents for their key
    earliest_parent = parents.group_by(join_keys).agg(
        pl.col("start").min().alias("_earliest")
    )
    children = children.join(earliest_parent, on=join_keys, how="left")
    is_early = pl.col("_earliest").is_not_null() & (
        pl.col("start") < pl.col("_earliest")
    )

    early = children.filter(is_early).drop("_earliest")
    normal = children.filter(~is_early).drop("_earliest")

    # Normal case: backward ASOF
    matched_normal = normal.sort("start").join_asof(
        parent_windows,
        left_on="start",
        right_on="_parent_start",
        by=join_keys,
        strategy="backward",
    )

    # Early starters: forward ASOF, but invalidate if no overlap
    matched_early = early.sort("start").join_asof(
        parent_windows,
        left_on="start",
        right_on="_parent_start",
        by=join_keys,
        strategy="forward",
    )
    child_ends_before_parent_starts = pl.col("original_end").is_not_null() & (
        pl.col("original_end") <= pl.col("_parent_start")
    )
    matched_early = matched_early.with_columns(
        pl.when(child_ends_before_parent_starts)
        .then(None)
        .otherwise(pl.col("_parent_start"))
        .alias("_parent_start"),
        pl.when(child_ends_before_parent_starts)
        .then(None)
        .otherwise(pl.col("_parent_end"))
        .alias("_parent_end"),
    )

    return pl.concat([matched_normal, matched_early], how="diagonal")
