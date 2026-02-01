"""
Tests for chameleon_usage.math.timeseries

DATA FORMAT: Long (tidy) — groups in columns, not column names.
LAZYFRAME CONTRACT: align_counts, time_weighted_resample return LazyFrame.

ALIGN_COUNTS: forward-fill to union of timestamps across groups
  - preserves original timestamps (no new timestamps created except from other groups)
  - each group gets a value at every timestamp where ANY group has data
  - nulls remain where no prior value exists to fill from

TIME_WEIGHTED_RESAMPLE: bucket step-function data, weight by duration
  - input: step-function (value persists until next event)
  - output: time-weighted average per bucket
  - nulls before first event (domain layer handles fill_null(0) for sweepline data)
  - events spanning bucket boundaries are clipped
"""

from datetime import datetime

import polars as pl
import pytest

from chameleon_usage.math.timeseries import align_step_functions, resample_step_function

# =============================================================================
# ALIGN_COUNTS
# [*group_cols, timestamp, value] → [*group_cols, timestamp, value]
# Forward-fill to union of timestamps. Preserves original timestamps.
# =============================================================================


def test_align_fills_missing_timestamps_from_other_groups():
    """Each group gets rows at timestamps from other groups."""
    df = pl.LazyFrame(
        {
            "ts": [datetime(2024, 1, 1), datetime(2024, 1, 3)],
            "val": [10, 30],
            "group": ["a", "b"],
        }
    )
    result = (
        align_step_functions(df, "ts", "val", ["group"]).collect().sort(["group", "ts"])
    )

    # Group A: has t1, gets t3 from B
    # Group B: has t3, gets t1 from A
    assert len(result) == 4
    a_rows = result.filter(pl.col("group") == "a")
    b_rows = result.filter(pl.col("group") == "b")
    assert a_rows["ts"].to_list() == [datetime(2024, 1, 1), datetime(2024, 1, 3)]
    assert b_rows["ts"].to_list() == [datetime(2024, 1, 1), datetime(2024, 1, 3)]


def test_align_forward_fills_values():
    """Missing values filled with last known value per group."""
    df = pl.LazyFrame(
        {
            "ts": [datetime(2024, 1, 1), datetime(2024, 1, 3)],
            "val": [10, 30],
            "group": ["a", "b"],
        }
    )
    result = (
        align_step_functions(df, "ts", "val", ["group"]).collect().sort(["group", "ts"])
    )

    a_rows = result.filter(pl.col("group") == "a")
    # A has 10 at t1, forward-filled to 10 at t3
    assert a_rows["val"].to_list() == [10, 10]


def test_align_null_when_no_prior_value():
    """Timestamps before a group's first event remain null."""
    df = pl.LazyFrame(
        {
            "ts": [datetime(2024, 1, 1), datetime(2024, 1, 3)],
            "val": [10, 30],
            "group": ["a", "b"],
        }
    )
    result = (
        align_step_functions(df, "ts", "val", ["group"]).collect().sort(["group", "ts"])
    )

    b_rows = result.filter(pl.col("group") == "b")
    # B has no value at t1 (before its first event at t3)
    assert b_rows["val"][0] is None
    assert b_rows["val"][1] == 30


def test_align_preserves_original_timestamps():
    """No new timestamps invented—only union of existing."""
    df = pl.LazyFrame(
        {
            "ts": [datetime(2024, 1, 1), datetime(2024, 1, 3)],
            "val": [10, 30],
            "group": ["a", "b"],
        }
    )
    result = align_step_functions(df, "ts", "val", ["group"]).collect()

    unique_ts = result["ts"].unique().sort()
    assert unique_ts.to_list() == [datetime(2024, 1, 1), datetime(2024, 1, 3)]


def test_align_with_multiple_group_columns():
    """Works with composite group keys."""
    df = pl.LazyFrame(
        {
            "ts": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
            "val": [10, 20],
            "type": ["cpu", "gpu"],
            "source": ["nova", "nova"],
        }
    )
    result = align_step_functions(df, "ts", "val", ["type", "source"]).collect()

    # 2 groups × 2 timestamps = 4 rows
    assert len(result) == 4


def test_align_groups_are_independent():
    """Forward-fill happens within each group, not across groups."""
    df = pl.LazyFrame(
        {
            "ts": [
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
                datetime(2024, 1, 2),
            ],
            "val": [10, 20, 100],
            "group": ["a", "a", "b"],
        }
    )
    result = (
        align_step_functions(df, "ts", "val", ["group"]).collect().sort(["group", "ts"])
    )

    a_rows = result.filter(pl.col("group") == "a")
    b_rows = result.filter(pl.col("group") == "b")
    # A's fill doesn't leak into B
    assert a_rows["val"].to_list() == [10, 20]
    assert b_rows["val"][0] is None  # B has no value at t1
    assert b_rows["val"][1] == 100


# =============================================================================
# TIME_WEIGHTED_RESAMPLE
# [*group_cols, timestamp, value] → [*group_cols, timestamp, value]
# Step-function input: value persists until next event.
# Output: time-weighted average per bucket.
# =============================================================================


def test_time_weighted_accounts_for_duration():
    """Longer-lasting values dominate the average."""
    df = pl.LazyFrame(
        {
            "ts": [datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 23, 0)],
            "val": [1.0, 100.0],
            "group": ["a", "a"],
        }
    )
    # val=1 for 23 hours, val=100 for 1 hour
    # weighted avg = (1×23 + 100×1) / 24 = 123/24 ≈ 5.125
    result = resample_step_function(
        df,
        "ts",
        "val",
        "1d",
        ["group"],
        time_range=(datetime(2024, 1, 1), datetime(2024, 1, 2)),
    ).collect()

    assert result["val"][0] == pytest.approx(123 / 24)


def test_time_weighted_event_spans_multiple_buckets():
    """A single event contributes to all buckets it spans."""
    df = pl.LazyFrame(
        {
            "ts": [datetime(2024, 1, 1)],
            "val": [10.0],
            "group": ["a"],
        }
    )
    result = (
        resample_step_function(
            df,
            "ts",
            "val",
            "1d",
            ["group"],
            time_range=(datetime(2024, 1, 1), datetime(2024, 1, 4)),
        )
        .collect()
        .sort("ts")
    )

    assert len(result) == 3
    assert result["val"].to_list() == [10.0, 10.0, 10.0]


def test_time_weighted_bucket_boundary_clips_duration():
    """Duration is clipped at bucket boundaries."""
    df = pl.LazyFrame(
        {
            "ts": [datetime(2024, 1, 1, 18, 0), datetime(2024, 1, 2, 6, 0)],
            "val": [1.0, 2.0],
            "group": ["a", "a"],
        }
    )
    # Jan 1 bucket: val=1 for 6 hours (18:00-24:00), avg=1
    # Jan 2 bucket: val=1 for 6 hours (00:00-06:00), val=2 for 18 hours (06:00-24:00)
    # Jan 2 avg = (1×6 + 2×18) / 24 = 42/24 = 1.75
    result = (
        resample_step_function(
            df,
            "ts",
            "val",
            "1d",
            ["group"],
            time_range=(datetime(2024, 1, 1), datetime(2024, 1, 3)),
        )
        .collect()
        .sort("ts")
    )

    assert result["val"][0] == pytest.approx(1.0)
    assert result["val"][1] == pytest.approx(42 / 24)


def test_time_weighted_null_before_first_event():
    """Buckets before first event are null, not zero."""
    df = pl.LazyFrame(
        {
            "ts": [datetime(2024, 1, 3)],
            "val": [10.0],
            "group": ["a"],
        }
    )
    result = (
        resample_step_function(
            df,
            "ts",
            "val",
            "1d",
            ["group"],
            time_range=(datetime(2024, 1, 1), datetime(2024, 1, 4)),
        )
        .collect()
        .sort("ts")
    )

    assert result["val"][0] is None  # Jan 1
    assert result["val"][1] is None  # Jan 2
    assert result["val"][2] == 10.0  # Jan 3


def test_time_weighted_groups_independent():
    """Each group's time-weighted average computed separately."""
    df = pl.LazyFrame(
        {
            "ts": [datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 0, 0)],
            "val": [10.0, 100.0],
            "group": ["a", "b"],
        }
    )
    result = (
        resample_step_function(
            df,
            "ts",
            "val",
            "1d",
            ["group"],
            time_range=(datetime(2024, 1, 1), datetime(2024, 1, 2)),
        )
        .collect()
        .sort("group")
    )

    assert result.filter(pl.col("group") == "a")["val"][0] == 10.0
    assert result.filter(pl.col("group") == "b")["val"][0] == 100.0


def test_time_weighted_all_groups_get_all_buckets():
    """All groups get all buckets in time_range, even if no events in that bucket."""
    df = pl.LazyFrame(
        {
            "ts": [datetime(2024, 1, 1), datetime(2024, 1, 3)],
            "val": [10.0, 30.0],
            "group": ["a", "b"],
        }
    )
    result = resample_step_function(
        df,
        "ts",
        "val",
        "1d",
        ["group"],
        time_range=(datetime(2024, 1, 1), datetime(2024, 1, 4)),
    ).collect()

    # 2 groups × 3 days = 6 rows
    assert len(result) == 6


# MISSING: multiple group columns, event exactly at bucket boundary


# =============================================================================
# LAZYFRAME CONTRACT
# =============================================================================


def test_align_counts_returns_lazyframe():
    df = pl.LazyFrame({"ts": [datetime(2024, 1, 1)], "val": [1], "group": ["a"]})
    result = align_step_functions(df, "ts", "val", ["group"])
    assert isinstance(result, pl.LazyFrame)


def test_time_weighted_resample_returns_lazyframe():
    df = pl.LazyFrame({"ts": [datetime(2024, 1, 1)], "val": [1.0], "group": ["a"]})
    result = resample_step_function(
        df,
        "ts",
        "val",
        "1h",
        ["group"],
        time_range=(datetime(2024, 1, 1), datetime(2024, 1, 2)),
    )
    assert isinstance(result, pl.LazyFrame)
