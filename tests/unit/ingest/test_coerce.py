"""Tests for apply_temporal_clamp.

apply_temporal_clamp clips child intervals to fit within parent windows and
tags each row with an audit status explaining the result.

Contract:
---------
1. MATCHING: Each child is matched to ALL parents it overlaps (by join_keys).
   A child overlaps a parent if their intervals intersect:
   - child.start < parent.end (or parent.end is null)
   - child.end > parent.start (or child.end is null)
   One output row per (child, parent) overlap.

2. CLAMPING: For each overlap, clamp to the intersection:
   - start = max(child.start, parent.start)
   - end = min(child.end, parent.end)  # null treated as infinity

3. AUDIT COLUMNS:
   - original_start : child's start before clamping
   - original_end   : child's end before clamping
   - coerce_status  : one of:
       "null_parent" : join key was null, cannot match
       "no_parent"   : no parent found for this join key
       "clamped"     : interval was clipped to fit parent
       "valid"       : fully contained, no adjustment needed

4. NO FILTERING: Children with no overlapping parent are preserved with
   "no_parent" status. Downstream decides what to do.

5. COLUMNS: All child columns preserved. No parent columns leak through.
"""

from datetime import datetime

import polars as pl

from chameleon_usage.ingest.coerce import apply_temporal_clamp


def dt(day):
    return datetime(2024, 1, day)


def clamp(target_rows, validator_rows, join_keys=["key"]):
    target = pl.LazyFrame(target_rows)
    validators = pl.LazyFrame(validator_rows)
    return apply_temporal_clamp(target, validators, join_keys).collect()


# --- Status: valid (no clamping needed) ---


def test_valid_child_inside_parent():
    result = clamp(
        {"start": [dt(10)], "end": [dt(20)], "key": ["A"]},
        {"start": [dt(5)], "end": [dt(25)], "key": ["A"]},
    )
    assert len(result) == 1
    assert result["coerce_status"][0] == "valid"
    assert result["start"][0] == dt(10)
    assert result["end"][0] == dt(20)
    assert result["original_start"][0] == dt(10)
    assert result["original_end"][0] == dt(20)


# --- Status: clamped ---


def test_clamped_start_before_parent():
    result = clamp(
        {"start": [dt(3)], "end": [dt(15)], "key": ["A"]},
        {"start": [dt(5)], "end": [dt(25)], "key": ["A"]},
    )
    assert result["coerce_status"][0] == "clamped"
    assert result["start"][0] == dt(5)
    assert result["end"][0] == dt(15)
    assert result["original_start"][0] == dt(3)
    assert result["original_end"][0] == dt(15)


def test_clamped_end_after_parent():
    result = clamp(
        {"start": [dt(10)], "end": [dt(30)], "key": ["A"]},
        {"start": [dt(5)], "end": [dt(25)], "key": ["A"]},
    )
    assert result["coerce_status"][0] == "clamped"
    assert result["start"][0] == dt(10)
    assert result["end"][0] == dt(25)
    assert result["original_start"][0] == dt(10)
    assert result["original_end"][0] == dt(30)


def test_clamped_both_ends():
    result = clamp(
        {"start": [dt(3)], "end": [dt(30)], "key": ["A"]},
        {"start": [dt(5)], "end": [dt(25)], "key": ["A"]},
    )
    assert result["coerce_status"][0] == "clamped"
    assert result["start"][0] == dt(5)
    assert result["end"][0] == dt(25)
    assert result["original_start"][0] == dt(3)
    assert result["original_end"][0] == dt(30)


# --- Status: no_parent ---


def test_no_parent_key_mismatch():
    result = clamp(
        {"start": [dt(10)], "end": [dt(20)], "key": ["A"]},
        {"start": [dt(5)], "end": [dt(25)], "key": ["B"]},
    )
    assert len(result) == 1
    assert result["coerce_status"][0] == "no_parent"
    assert result["original_start"][0] == dt(10)
    assert result["original_end"][0] == dt(20)


def test_no_parent_child_ends_before_all_parents():
    """Child ends before any parent starts, no overlap."""
    result = clamp(
        {"start": [dt(5)], "end": [dt(8)], "key": ["A"]},
        {"start": [dt(10)], "end": [dt(25)], "key": ["A"]},
    )
    assert result["coerce_status"][0] == "no_parent"


def test_early_starter_overlaps_first_parent():
    """Child starts before parent but overlaps it."""
    result = clamp(
        {"start": [dt(5)], "end": [dt(15)], "key": ["A"]},
        {"start": [dt(10)], "end": [dt(25)], "key": ["A"]},
    )
    assert len(result) == 1
    assert result["coerce_status"][0] == "clamped"
    assert result["start"][0] == dt(10)
    assert result["end"][0] == dt(15)


# --- No overlap -> no_parent ---


def test_no_parent_child_after_all_parents():
    result = clamp(
        {"start": [dt(26)], "end": [dt(28)], "key": ["A"]},
        {"start": [dt(5)], "end": [dt(25)], "key": ["A"]},
    )
    assert result["coerce_status"][0] == "no_parent"
    assert result["original_start"][0] == dt(26)
    assert result["original_end"][0] == dt(28)


def test_no_parent_child_starts_exactly_at_parent_end():
    result = clamp(
        {"start": [dt(25)], "end": [dt(30)], "key": ["A"]},
        {"start": [dt(5)], "end": [dt(25)], "key": ["A"]},
    )
    assert result["coerce_status"][0] == "no_parent"


# --- Status: null_parent ---


def test_null_parent_null_join_key():
    result = clamp(
        {"start": [dt(10)], "end": [dt(20)], "key": [None]},
        {"start": [dt(5)], "end": [dt(25)], "key": ["A"]},
    )
    assert result["coerce_status"][0] == "null_parent"


# --- Null end times ---


def test_null_child_end_clamped_to_parent():
    result = clamp(
        {"start": [dt(10)], "end": [None], "key": ["A"]},
        {"start": [dt(5)], "end": [dt(25)], "key": ["A"]},
    )
    assert result["coerce_status"][0] == "clamped"
    assert result["start"][0] == dt(10)
    assert result["end"][0] == dt(25)
    assert result["original_end"][0] is None


def test_null_parent_end_child_unchanged():
    result = clamp(
        {"start": [dt(10)], "end": [dt(20)], "key": ["A"]},
        {"start": [dt(5)], "end": [None], "key": ["A"]},
    )
    assert result["coerce_status"][0] == "valid"
    assert result["start"][0] == dt(10)
    assert result["end"][0] == dt(20)


def test_both_null_end_stays_null():
    result = clamp(
        {"start": [dt(10)], "end": [None], "key": ["A"]},
        {"start": [dt(5)], "end": [None], "key": ["A"]},
    )
    assert result["coerce_status"][0] == "valid"
    assert result["start"][0] == dt(10)
    assert result["end"][0] is None


# --- Multiple parent eras ---


def test_child_overlaps_one_of_multiple_eras():
    """Child only overlaps second era, produces one row."""
    result = clamp(
        {"start": [dt(15)], "end": [dt(20)], "key": ["A"]},
        {
            "start": [dt(1), dt(10)],
            "end": [dt(8), dt(25)],
            "key": ["A", "A"],
        },
    )
    assert len(result) == 1
    assert result["coerce_status"][0] == "valid"
    assert result["start"][0] == dt(15)
    assert result["end"][0] == dt(20)


def test_child_spans_multiple_eras():
    """Child overlaps both eras, produces two rows."""
    result = clamp(
        {"start": [dt(5)], "end": [dt(20)], "key": ["A"]},
        {
            "start": [dt(1), dt(15)],
            "end": [dt(10), dt(25)],
            "key": ["A", "A"],
        },
    )
    assert len(result) == 2
    # First row: clamped to era1 [1-10], child is [5-20] -> [5-10]
    row1 = result.filter(pl.col("end") == dt(10))
    assert row1["start"][0] == dt(5)
    assert row1["coerce_status"][0] == "clamped"
    # Second row: clamped to era2 [15-25], child is [5-20] -> [15-20]
    row2 = result.filter(pl.col("start") == dt(15))
    assert row2["end"][0] == dt(20)
    assert row2["coerce_status"][0] == "clamped"


def test_early_starter_null_end_spans_multiple_eras():
    """Child starts before all parents, null end, spans all eras."""
    result = clamp(
        {"start": [dt(1)], "end": [None], "key": ["A"]},
        {
            "start": [dt(5), dt(15)],
            "end": [dt(10), dt(25)],
            "key": ["A", "A"],
        },
    )
    assert len(result) == 2
    # First row: [5-10]
    row1 = result.filter(pl.col("end") == dt(10))
    assert row1["start"][0] == dt(5)
    assert row1["coerce_status"][0] == "clamped"
    # Second row: [15-25]
    row2 = result.filter(pl.col("start") == dt(15))
    assert row2["end"][0] == dt(25)
    assert row2["coerce_status"][0] == "clamped"


def test_child_in_gap_between_eras():
    """Child falls entirely in gap between eras, no overlap."""
    result = clamp(
        {"start": [dt(11)], "end": [dt(14)], "key": ["A"]},
        {
            "start": [dt(5), dt(15)],
            "end": [dt(10), dt(25)],
            "key": ["A", "A"],
        },
    )
    assert len(result) == 1
    assert result["coerce_status"][0] == "no_parent"


# --- Column preservation ---


def test_extra_columns_preserved():
    target = pl.LazyFrame(
        {
            "start": [dt(10)],
            "end": [dt(20)],
            "key": ["A"],
            "extra": ["keep_me"],
        }
    )
    validators = pl.LazyFrame(
        {
            "start": [dt(5)],
            "end": [dt(25)],
            "key": ["A"],
        }
    )
    result = apply_temporal_clamp(target, validators, ["key"]).collect()
    assert result["extra"][0] == "keep_me"


def test_validator_columns_not_leaked():
    target = pl.LazyFrame(
        {
            "start": [dt(10)],
            "end": [dt(20)],
            "key": ["A"],
        }
    )
    validators = pl.LazyFrame(
        {
            "start": [dt(5)],
            "end": [dt(25)],
            "key": ["A"],
            "validator_only": ["should_not_appear"],
        }
    )
    result = apply_temporal_clamp(target, validators, ["key"]).collect()
    assert "validator_only" not in result.columns
    assert "val_start" not in result.columns
    assert "val_horizon" not in result.columns


def test_audit_columns_present():
    result = clamp(
        {"start": [dt(10)], "end": [dt(20)], "key": ["A"]},
        {"start": [dt(5)], "end": [dt(25)], "key": ["A"]},
    )
    assert "coerce_status" in result.columns
    assert "original_start" in result.columns
    assert "original_end" in result.columns
