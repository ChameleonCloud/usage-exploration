# tests/unit/test_intervals.py

from datetime import datetime

import polars as pl

from chameleon_usage.core.intervals import (
    deltas_to_counts,
    intervals_to_counts,
    intervals_to_deltas,
)

# --- intervals_to_deltas ---


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


# --- _deltas_to_counts ---


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

    assert counts.filter(pl.col("group") == "a")["count"].to_list() == [1, 0]
    assert counts.filter(pl.col("group") == "b")["count"].to_list() == [1]


def test_counts_aggregates_same_timestamp():
    df = pl.LazyFrame(
        {
            "timestamp": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            "change": [1, 1],
            "group": ["a", "a"],
        }
    )
    counts = deltas_to_counts(df, ["group"]).collect()

    assert counts["count"][0] == 2


# --- intervals_to_counts (integration) ---


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

    assert counts["count"].to_list() == [1, 0]


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

    assert counts["count"].to_list() == [1, 2, 1, 0]


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

    assert counts["count"].to_list() == [1]


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

    assert counts.filter(pl.col("group") == "a")["count"].to_list() == [1, 0]
    assert counts.filter(pl.col("group") == "b")["count"].to_list() == [1, 0]
