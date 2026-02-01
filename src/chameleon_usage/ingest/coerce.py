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
    valid           - bool: True = use in analysis, False = exclude
    coerce_action   - what happened:
        "none"        : child fully inside parent, no change needed
        "clipped"     : child was clipped to fit parent
        "orphan"      : no valid parent found (no match or no overlap)
        "null_key"    : join key was null, cannot match

All rows preserved. Downstream filters on `valid`.
"""

import polars as pl


def apply_temporal_clamp(
    children: pl.LazyFrame,
    parents: pl.LazyFrame,
    join_keys: list[str],
    require_parent: pl.Expr | None = None,
) -> pl.LazyFrame:
    children = children.with_columns(
        pl.col("start").alias("original_start"),
        pl.col("end").alias("original_end"),
    ).with_row_index("_row_id")

    # If require_parent specified, rows not requiring parent are valid as-is
    if require_parent is not None:
        needs_parent = children.filter(require_parent)
        no_parent_needed = children.filter(~require_parent).with_columns(
            pl.lit(True).alias("valid"),
            pl.lit("none").alias("coerce_action"),
        )
    else:
        needs_parent = children
        no_parent_needed = children.clear()

    # Null keys can't match - handle separately
    has_null_key = pl.any_horizontal(*[pl.col(k).is_null() for k in join_keys])
    null_key_rows = needs_parent.filter(has_null_key)
    matchable = needs_parent.filter(~has_null_key)

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

    # Split: matched (has parent + overlaps) vs orphan
    has_parent = pl.col("_p_start").is_not_null()
    matched = joined.filter(has_parent & overlaps)

    # Children with no overlapping parent = orphan
    matched_row_ids = matched.select("_row_id").unique()
    orphan_rows = matchable.join(matched_row_ids, on="_row_id", how="anti").with_columns(
        pl.lit(False).alias("valid"),
        pl.lit("orphan").alias("coerce_action"),
    )

    # Determine status and clamp matched rows
    start_outside = pl.col("original_start") < pl.col("_p_start")
    end_outside = pl.col("_p_end").is_not_null() & (
        pl.col("original_end").is_null() | (pl.col("original_end") > pl.col("_p_end"))
    )
    was_clipped = start_outside | end_outside

    result = matched.with_columns(
        pl.lit(True).alias("valid"),
        pl.when(was_clipped).then(pl.lit("clipped")).otherwise(pl.lit("none")).alias("coerce_action"),
        pl.max_horizontal("original_start", "_p_start").alias("start"),
        pl.min_horizontal("original_end", "_p_end").alias("end"),
    ).drop("_p_start", "_p_end")

    # Null key rows - separate category
    null_result = null_key_rows.with_columns(
        pl.lit(False).alias("valid"),
        pl.lit("null_key").alias("coerce_action"),
    )

    return (
        pl.concat([result, orphan_rows, null_result, no_parent_needed], how="diagonal")
        .sort("_row_id")
        .drop("_row_id")
    )


def clamp_hierarchy(intervals: pl.LazyFrame) -> pl.LazyFrame:
    """Apply hierarchical temporal clamping: total → reservable → committed → occupied.

    Each layer is clamped to its parent's time window. Rows outside their parent's
    window are tagged with valid=False but preserved for debugging.

    Args:
        intervals: Raw intervals with quantity_type column

    Returns:
        Intervals with valid, coerce_action, original_start, original_end added.
    """
    total = intervals.filter(pl.col("quantity_type").eq("total"))
    reservable = intervals.filter(pl.col("quantity_type").eq("reservable"))
    committed = intervals.filter(pl.col("quantity_type").eq("committed"))
    occupied = intervals.filter(pl.col("quantity_type").eq("occupied"))

    clamped_reservable = apply_temporal_clamp(
        reservable, parents=total, join_keys=["hypervisor_hostname"]
    )
    clamped_committed = apply_temporal_clamp(
        committed, parents=clamped_reservable, join_keys=["blazar_host_id"]
    )
    # Deduplicate allocations per (hostname, reservation_id) for instance matching
    # flavor:instance reservations create many allocations per host with same time window
    committed_for_occupied = clamped_committed.unique(
        subset=["hypervisor_hostname", "blazar_reservation_id"], keep="first"
    )
    # Only reserved instances (booking_type="reservation") need allocation parents
    # On-demand instances are valid without clamping
    clamped_occupied = apply_temporal_clamp(
        occupied,
        parents=committed_for_occupied,
        join_keys=["blazar_reservation_id", "hypervisor_hostname"],
        require_parent=pl.col("booking_type").eq("reservation"),
    )

    total_with_status = total.with_columns(
        pl.col("start").alias("original_start"),
        pl.col("end").alias("original_end"),
        pl.lit(True).alias("valid"),
        pl.lit("none").alias("coerce_action"),
    )

    return pl.concat(
        [total_with_status, clamped_reservable, clamped_committed, clamped_occupied],
        how="diagonal",
    )
