"""Tests for apply_temporal_clamp.

apply_temporal_clamp clips child intervals to fit within parent windows and
tags each row with an audit status explaining the result.

Contract:
---------
1. MATCHING: Each child is matched to a parent by join_keys. If multiple parents
   exist for the same key, match the parent whose start is most recent but
   still <= child.start.

2. CLAMPING: For matched children, clamp [start, end] to [parent.start, parent.end]:
   - start = max(child.start, parent.start)
   - end = min(child.end, parent.end)  # null treated as infinity

3. AUDIT COLUMNS:
   - original_start : child's start before clamping
   - original_end   : child's end before clamping
   - coerce_status  : one of:
       "null_parent"  : join key was null, cannot match
       "no_parent"    : no parent found with matching join key
       "no_overlap"   : matched parent, but child.start >= parent.end
       "clamped"      : matched and adjusted (start or end changed)
       "valid"        : fully contained, no adjustment needed

4. NO FILTERING: All rows preserved. Downstream decides what to do with
   invalid rows based on coerce_status.

5. COLUMNS: All child columns preserved. No validator columns leak through.
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


def test_no_parent_child_before_all_validators():
    result = clamp(
        {"start": [dt(5)], "end": [dt(8)], "key": ["A"]},
        {"start": [dt(10)], "end": [dt(25)], "key": ["A"]},
    )
    assert result["coerce_status"][0] == "no_parent"


# --- Status: no_overlap ---


def test_no_overlap_child_starts_after_parent_ends():
    result = clamp(
        {"start": [dt(26)], "end": [dt(28)], "key": ["A"]},
        {"start": [dt(5)], "end": [dt(25)], "key": ["A"]},
    )
    assert result["coerce_status"][0] == "no_overlap"
    assert result["original_start"][0] == dt(26)
    assert result["original_end"][0] == dt(28)


def test_no_overlap_child_starts_exactly_at_parent_end():
    result = clamp(
        {"start": [dt(25)], "end": [dt(30)], "key": ["A"]},
        {"start": [dt(5)], "end": [dt(25)], "key": ["A"]},
    )
    assert result["coerce_status"][0] == "no_overlap"


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


# --- Multiple validators (era matching) ---


def test_matches_most_recent_validator_era():
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


# --- Column preservation ---


def test_extra_columns_preserved():
    target = pl.LazyFrame({
        "start": [dt(10)],
        "end": [dt(20)],
        "key": ["A"],
        "extra": ["keep_me"],
    })
    validators = pl.LazyFrame({
        "start": [dt(5)],
        "end": [dt(25)],
        "key": ["A"],
    })
    result = apply_temporal_clamp(target, validators, ["key"]).collect()
    assert result["extra"][0] == "keep_me"


def test_validator_columns_not_leaked():
    target = pl.LazyFrame({
        "start": [dt(10)],
        "end": [dt(20)],
        "key": ["A"],
    })
    validators = pl.LazyFrame({
        "start": [dt(5)],
        "end": [dt(25)],
        "key": ["A"],
        "validator_only": ["should_not_appear"],
    })
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
