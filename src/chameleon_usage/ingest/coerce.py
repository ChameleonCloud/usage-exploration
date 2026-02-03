"""
Clamp child intervals to parent windows and tag each row with what happened.

Decision tree for each child:

    require_parent?
    ├─ No  → EXEMPT (valid=True, unchanged)
    └─ Yes → has null join key?
             ├─ Yes → NULL_KEY (valid=False)
             └─ No  → join to parents → overlapping parent exists?
                      ├─ No  → ORPHAN (valid=False)
                      └─ Yes → fully inside parent?
                               ├─ Yes → NONE (valid=True, unchanged)
                               └─ No  → CLIPPED (valid=True, clamped)

Fan-out: A child overlapping N parents produces N output rows (one per parent).
Null end: Treated as infinity for overlap/enclosure checks.

Output columns added: original_start, original_end, valid, coerce_action
"""

import polars as pl

from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.constants import SchemaCols as S

# =============================================================================
# Interval math (pure expressions, no side effects)
# =============================================================================


def intervals_overlap(start_a: str, end_a: str, start_b: str, end_b: str) -> pl.Expr:
    """Two intervals overlap if each starts before the other ends. Null end = infinity."""
    a_before_b_ends = pl.col(end_b).is_null() | (pl.col(start_a) < pl.col(end_b))
    a_after_b_starts = pl.col(end_a).is_null() | (pl.col(end_a) > pl.col(start_b))
    return a_before_b_ends & a_after_b_starts


def interval_enclosed(
    inner_start: str, inner_end: str, outer_start: str, outer_end: str
) -> pl.Expr:
    """Inner is enclosed if it starts at/after outer.start and ends at/before outer.end."""
    start_ok = pl.col(inner_start) >= pl.col(outer_start)
    end_ok = pl.col(outer_end).is_null() | (
        pl.col(inner_end).is_not_null() & (pl.col(inner_end) <= pl.col(outer_end))
    )
    return start_ok & end_ok


def clamp_to_parent(child_col: str, parent_col: str, *, use_max: bool) -> pl.Expr:
    """Clamp child value to parent bound. use_max=True for start, False for end."""
    if use_max:
        return pl.max_horizontal(child_col, parent_col)
    return pl.min_horizontal(child_col, parent_col)


# =============================================================================
# Row tagging (adds valid + coerce_action columns)
# =============================================================================


def _tag(frame: pl.LazyFrame, *, valid: bool, action: str) -> pl.LazyFrame:
    """Add valid and coerce_action columns with fixed values."""
    return frame.with_columns(
        pl.lit(valid).alias("valid"),
        pl.lit(action).alias("coerce_action"),
    )


def _tag_matched(frame: pl.LazyFrame, enclosed: pl.Expr) -> pl.LazyFrame:
    """Tag matched rows: valid=True, action depends on enclosure, clamp start/end."""
    return frame.with_columns(
        pl.lit(True).alias("valid"),
        pl.when(enclosed)
        .then(pl.lit("none"))
        .otherwise(pl.lit("clipped"))
        .alias("coerce_action"),
        clamp_to_parent("original_start", "_p_start", use_max=True).alias("start"),
        clamp_to_parent("original_end", "_p_end", use_max=False).alias("end"),
    )


# =============================================================================
# Core algorithm
# =============================================================================


def apply_temporal_clamp(
    children: pl.LazyFrame,
    parents: pl.LazyFrame,
    join_keys: list[str],
    require_parent: pl.Expr | None = None,
) -> pl.LazyFrame:
    """
    Clamp children to parent windows. See module docstring for decision tree.

    Args:
        children: Intervals with start, end, and join_keys columns
        parents: Parent intervals to clamp against
        join_keys: Columns to match children to parents
        require_parent: Expression for rows that need a parent (default: all rows)

    Returns:
        Children with original_start, original_end, valid, coerce_action added.
        Rows may fan out if a child overlaps multiple parents.
    """
    # --- Validation ---
    child_cols = set(children.collect_schema().names())
    parent_cols = set(parents.collect_schema().names())
    missing = (set(join_keys) - child_cols) | (set(join_keys) - parent_cols)
    if missing:
        raise ValueError(f"Join keys missing: {missing}")

    # --- Setup ---
    # Preserve original timestamps, add row ID for orphan detection
    children = children.with_columns(
        pl.col("start").alias("original_start"),
        pl.col("end").alias("original_end"),
    ).with_row_index("_child_id")

    needs_parent = require_parent if require_parent is not None else pl.lit(True)
    has_null_key = pl.any_horizontal(*[pl.col(k).is_null() for k in join_keys])

    # --- Branch 1: EXEMPT (don't need parent) ---
    exempt = _tag(children.filter(~needs_parent), valid=True, action="none").drop(
        "_child_id"
    )

    # --- Branch 2: NULL_KEY (need parent but can't join) ---
    must_match = children.filter(needs_parent)
    null_key = _tag(
        must_match.filter(has_null_key), valid=False, action="null_key"
    ).drop("_child_id")

    # --- Branch 3+4: Join to find parents ---
    matchable = must_match.filter(~has_null_key)
    parent_windows = parents.select(
        *join_keys,
        pl.col("start").alias("_p_start"),
        pl.col("end").alias("_p_end"),
    )
    joined = matchable.join(parent_windows, on=join_keys, how="left")

    # --- Branch 3: ORPHAN (no overlapping parent) ---
    has_parent = pl.col("_p_start").is_not_null()
    overlaps = intervals_overlap("original_start", "original_end", "_p_start", "_p_end")
    matched_ids = joined.filter(has_parent & overlaps).select("_child_id").unique()
    orphan = _tag(
        matchable.join(matched_ids, on="_child_id", how="anti"),
        valid=False,
        action="orphan",
    )

    # --- Branch 4: MATCHED (has overlapping parent → none or clipped) ---
    enclosed = interval_enclosed("original_start", "original_end", "_p_start", "_p_end")
    matched = _tag_matched(joined.filter(has_parent & overlaps), enclosed).drop(
        "_p_start", "_p_end"
    )

    # --- Combine all branches ---
    return pl.concat([exempt, null_key, orphan, matched], how="diagonal").drop(
        "_child_id"
    )


# # =============================================================================
# Hierarchy clamping (domain-specific)
# =============================================================================


def _add_audit_cols(df: pl.LazyFrame) -> pl.LazyFrame:
    """Add audit columns for rows that skip clamping."""
    return df.with_columns(
        pl.col("start").alias("original_start"),
        pl.col("end").alias("original_end"),
        pl.lit(True).alias("valid"),
        pl.lit("none").alias("coerce_action"),
    )


def clamp_hierarchy(intervals: pl.LazyFrame) -> pl.LazyFrame:
    """
    Apply hierarchical temporal clamping: total → reservable → committed → occupied.

    Each layer is clamped to its parent's window:
    - reservable must fit within total (same hypervisor)
    - committed must fit within reservable (same blazar host)
    - occupied_reservation must fit within committed (same reservation + hypervisor)
    - occupied_ondemand skips clamping (no parent in the hierarchy)
    """
    total = intervals.filter(pl.col(S.METRIC).eq(QT.TOTAL))
    reservable = intervals.filter(pl.col(S.METRIC).eq(QT.RESERVABLE))
    committed = intervals.filter(pl.col(S.METRIC).eq(QT.COMMITTED))
    occupied_reservation = intervals.filter(
        pl.col(S.METRIC).eq(QT.OCCUPIED_RESERVATION)
    )
    occupied_ondemand = intervals.filter(pl.col(S.METRIC).eq(QT.OCCUPIED_ONDEMAND))

    # Level 1: reservable → total
    clamped_reservable = apply_temporal_clamp(
        reservable, parents=total, join_keys=["hypervisor_hostname", S.RESOURCE]
    )

    # Level 2: committed → reservable
    clamped_committed = apply_temporal_clamp(
        committed,
        parents=clamped_reservable,
        join_keys=["blazar_host_id", S.RESOURCE],
    )

    # Level 3: occupied_reservation → committed
    # Dedupe: flavor:instance reservations create duplicate allocations per host
    committed_for_occupied = clamped_committed.unique(
        subset=["hypervisor_hostname", "blazar_reservation_id", S.RESOURCE],
        keep="first",
    )
    clamped_occupied = apply_temporal_clamp(
        occupied_reservation,
        parents=committed_for_occupied,
        join_keys=["blazar_reservation_id", "hypervisor_hostname", S.RESOURCE],
    )

    return pl.concat(
        [
            _add_audit_cols(total),
            clamped_reservable,
            clamped_committed,
            clamped_occupied,
            _add_audit_cols(occupied_ondemand),
        ],
        how="diagonal",
    )
