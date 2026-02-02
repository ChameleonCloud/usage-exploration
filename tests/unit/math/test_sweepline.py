"""
Tests for chameleon_usage.math.transforms

DATA FORMAT: Long (tidy) — one row per observation, groups in columns not column names.
LAZYFRAME CONTRACT: All functions return LazyFrame, never call collect().

SWEEPLINE: [start, end) intervals → point-in-time counts
  - count(t) = intervals where start <= t < end
  - null end = never closes
  - groups partition independently

RESAMPLE: bucket time series, average values per (bucket, group)
  - sparse output: groups may have different bucket sets
    e.g. group A data at 00:00, group B at 01:00 → output has 2 rows, not 4 with nulls
"""

from datetime import datetime

import polars as pl

from chameleon_usage.math.sweepline import (
    deltas_to_counts,
    intervals_to_counts,
    intervals_to_deltas,
)

# =============================================================================
# SWEEPLINE: intervals_to_deltas
# [*group_cols, start, end] → [*group_cols, timestamp, change]
# =============================================================================


def test_deltas_emits_plus_at_start_minus_at_end():
    df = pl.LazyFrame(
        {
            "start": [datetime(2024, 1, 1)],
            "end": [datetime(2024, 1, 2)],
            "group": ["a"],
        }
    )
    deltas = intervals_to_deltas(df, "start", "end", ["group"]).collect()

    assert len(deltas) == 2
    assert deltas.filter(pl.col("change") == 1)["timestamp"][0] == datetime(2024, 1, 1)
    assert deltas.filter(pl.col("change") == -1)["timestamp"][0] == datetime(2024, 1, 2)


def test_deltas_skips_null_end():
    df = pl.LazyFrame(
        {
            "start": [datetime(2024, 1, 1)],
            "end": [None],
            "group": ["a"],
        }
    )
    deltas = intervals_to_deltas(df, "start", "end", ["group"]).collect()

    assert len(deltas) == 1
    assert deltas["change"][0] == 1


# MISSING: preserves group columns, multiple group columns


# =============================================================================
# SWEEPLINE: deltas_to_counts
# [*group_cols, timestamp, change] → [*group_cols, timestamp, count]
# =============================================================================


def test_counts_cumsums_per_group():
    df = pl.LazyFrame(
        {
            "timestamp": [
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
                datetime(2024, 1, 1),
            ],
            "change": [1, -1, 1],
            "group": ["a", "a", "b"],
        }
    )
    counts = deltas_to_counts(df, ["group"]).collect().sort(["group", "timestamp"])

    assert counts.filter(pl.col("group") == "a")["value"].to_list() == [1, 0]
    assert counts.filter(pl.col("group") == "b")["value"].to_list() == [1]


def test_counts_aggregates_same_timestamp():
    df = pl.LazyFrame(
        {
            "timestamp": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            "change": [1, 1],
            "group": ["a", "a"],
        }
    )
    counts = deltas_to_counts(df, ["group"]).collect()

    assert counts["value"][0] == 2


# MISSING: empty group_cols, multiple group columns


# =============================================================================
# SWEEPLINE: intervals_to_counts (integration)
# One row per (group, timestamp) where count changes—NOT a dense grid.
# =============================================================================


def test_single_interval_counts_one_then_zero():
    """An interval contributes 1 during [start, end), 0 after."""
    df = pl.LazyFrame(
        {
            "start": [datetime(2024, 1, 1)],
            "end": [datetime(2024, 1, 2)],
            "group": ["a"],
        }
    )
    counts = intervals_to_counts(df, "start", "end", ["group"]).collect()

    assert counts["value"].to_list() == [1, 0]


def test_overlapping_intervals_stack():
    """Two overlapping intervals produce count of 2 during overlap."""
    df = pl.LazyFrame(
        {
            "start": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
            "end": [datetime(2024, 1, 3), datetime(2024, 1, 4)],
            "group": ["a", "a"],
        }
    )
    counts = intervals_to_counts(df, "start", "end", ["group"]).collect()

    assert counts["value"].to_list() == [1, 2, 1, 0]


def test_open_interval_stays_active():
    """Null end means interval never closes."""
    df = pl.LazyFrame(
        {
            "start": [datetime(2024, 1, 1)],
            "end": [None],
            "group": ["a"],
        }
    )
    counts = intervals_to_counts(df, "start", "end", ["group"]).collect()

    assert counts["value"].to_list() == [1]


def test_groups_are_independent():
    """Different groups don't affect each other's counts."""
    df = pl.LazyFrame(
        {
            "start": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            "end": [datetime(2024, 1, 2), datetime(2024, 1, 2)],
            "group": ["a", "b"],
        }
    )
    counts = (
        intervals_to_counts(df, "start", "end", ["group"])
        .collect()
        .sort(["group", "timestamp"])
    )

    assert counts.filter(pl.col("group") == "a")["value"].to_list() == [1, 0]
    assert counts.filter(pl.col("group") == "b")["value"].to_list() == [1, 0]


# MISSING: adjacent intervals [t1,t2)+[t2,t3) seamless count=1
