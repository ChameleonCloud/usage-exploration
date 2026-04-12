"""Tests for interval tagging via intersection.

tag_intervals splits [start, end) intervals at state change points
from a second interval stream, adding a boolean column.

Example: a host is "usable" from Jan 1–Feb 1, then "unusable" from Feb 1–Mar 1.
An allocation spanning Jan 15–Feb 15 gets split into:
  [Jan 15, Feb 1) usable=true
  [Feb 1, Feb 15) usable=false
"""

from datetime import datetime

import polars as pl

from chameleon_usage.math.intervals import tag_intervals

T = datetime


def _tag(intervals, events, **kwargs):
    return (
        tag_intervals(intervals.lazy(), events.lazy(), **kwargs)
        .collect()
        .sort("entity_id", "start")
    )


# =============================================================================
# Basic: single interval, single state
# =============================================================================


def test_interval_fully_inside_one_state():
    """Interval falls entirely within one usability period."""
    intervals = pl.DataFrame(
        {
            "entity_id": ["a"],
            "start": [T(2024, 1, 10)],
            "end": [T(2024, 1, 20)],
            "metric": ["committed"],
            "host": ["h1"],
        }
    )
    events = pl.DataFrame(
        {
            "host": ["h1"],
            "event_start": [T(2024, 1, 1)],
            "event_end": [T(2024, 2, 1)],
            "usable": [True],
        }
    )
    result = _tag(intervals, events, by="host")

    assert len(result) == 1
    assert result["usable"][0] is True
    assert result["start"][0] == T(2024, 1, 10)
    assert result["end"][0] == T(2024, 1, 20)


# =============================================================================
# Core: splitting at state change points
# =============================================================================


def test_interval_split_at_state_change():
    """Interval spanning a usable→unusable transition gets split."""
    intervals = pl.DataFrame(
        {
            "entity_id": ["a"],
            "start": [T(2024, 1, 15)],
            "end": [T(2024, 2, 15)],
            "metric": ["committed"],
            "host": ["h1"],
        }
    )
    events = pl.DataFrame(
        {
            "host": ["h1", "h1"],
            "event_start": [T(2024, 1, 1), T(2024, 2, 1)],
            "event_end": [T(2024, 2, 1), T(2024, 3, 1)],
            "usable": [True, False],
        }
    )
    result = _tag(intervals, events, by="host")

    assert len(result) == 2
    assert result["start"].to_list() == [T(2024, 1, 15), T(2024, 2, 1)]
    assert result["end"].to_list() == [T(2024, 2, 1), T(2024, 2, 15)]
    assert result["usable"].to_list() == [True, False]


def test_interval_split_multiple_changes():
    """Interval spanning multiple state changes gets split at each."""
    intervals = pl.DataFrame(
        {
            "entity_id": ["a"],
            "start": [T(2024, 1, 1)],
            "end": [T(2024, 4, 1)],
            "metric": ["committed"],
            "host": ["h1"],
        }
    )
    events = pl.DataFrame(
        {
            "host": ["h1", "h1", "h1"],
            "event_start": [T(2024, 1, 1), T(2024, 2, 1), T(2024, 3, 1)],
            "event_end": [T(2024, 2, 1), T(2024, 3, 1), T(2024, 4, 1)],
            "usable": [True, False, True],
        }
    )
    result = _tag(intervals, events, by="host")

    assert len(result) == 3
    assert result["start"].to_list() == [T(2024, 1, 1), T(2024, 2, 1), T(2024, 3, 1)]
    assert result["end"].to_list() == [T(2024, 2, 1), T(2024, 3, 1), T(2024, 4, 1)]
    assert result["usable"].to_list() == [True, False, True]


# =============================================================================
# Edge cases: open intervals, no overlap, multiple entities
# =============================================================================


def test_open_ended_interval():
    """Null end means interval is ongoing — split still works."""
    intervals = pl.DataFrame(
        {
            "entity_id": ["a"],
            "start": [T(2024, 1, 15)],
            "end": [None],
            "metric": ["committed"],
            "host": ["h1"],
        }
    ).cast({"end": pl.Datetime("us")})
    events = pl.DataFrame(
        {
            "host": ["h1", "h1"],
            "event_start": [T(2024, 1, 1), T(2024, 2, 1)],
            "event_end": [T(2024, 2, 1), None],
            "usable": [True, False],
        }
    ).cast({"event_end": pl.Datetime("us")})
    result = _tag(intervals, events, by="host")

    assert len(result) == 2
    assert result["start"].to_list() == [T(2024, 1, 15), T(2024, 2, 1)]
    assert result["end"][0] == T(2024, 2, 1)
    assert result["end"][1] is None
    assert result["usable"].to_list() == [True, False]


def test_no_matching_host_gets_null_usable():
    """Intervals on hosts without audit data get usable=null."""
    intervals = pl.DataFrame(
        {
            "entity_id": ["a"],
            "start": [T(2024, 1, 1)],
            "end": [T(2024, 2, 1)],
            "metric": ["committed"],
            "host": ["unknown_host"],
        }
    )
    events = pl.DataFrame(
        {
            "host": ["h1"],
            "event_start": [T(2024, 1, 1)],
            "event_end": [T(2024, 2, 1)],
            "usable": [True],
        }
    )
    result = _tag(intervals, events, by="host")

    assert len(result) == 1
    assert result["usable"][0] is None


def test_multiple_entities_on_same_host():
    """Each entity is split independently based on the same host events."""
    intervals = pl.DataFrame(
        {
            "entity_id": ["a", "b"],
            "start": [T(2024, 1, 15), T(2024, 1, 15)],
            "end": [T(2024, 2, 15), T(2024, 2, 15)],
            "metric": ["committed", "committed"],
            "host": ["h1", "h1"],
        }
    )
    events = pl.DataFrame(
        {
            "host": ["h1", "h1"],
            "event_start": [T(2024, 1, 1), T(2024, 2, 1)],
            "event_end": [T(2024, 2, 1), T(2024, 3, 1)],
            "usable": [True, False],
        }
    )
    result = _tag(intervals, events, by="host")

    assert len(result) == 4
    for eid in ["a", "b"]:
        rows = result.filter(pl.col("entity_id") == eid)
        assert rows["usable"].to_list() == [True, False]


def test_multiple_hosts_independent():
    """Events on h1 don't affect intervals on h2."""
    intervals = pl.DataFrame(
        {
            "entity_id": ["a", "b"],
            "start": [T(2024, 1, 1), T(2024, 1, 1)],
            "end": [T(2024, 3, 1), T(2024, 3, 1)],
            "metric": ["committed", "committed"],
            "host": ["h1", "h2"],
        }
    )
    events = pl.DataFrame(
        {
            "host": ["h1", "h1", "h2"],
            "event_start": [T(2024, 1, 1), T(2024, 2, 1), T(2024, 1, 1)],
            "event_end": [T(2024, 2, 1), T(2024, 3, 1), T(2024, 3, 1)],
            "usable": [True, False, True],
        }
    )
    result = _tag(intervals, events, by="host")

    a = result.filter(pl.col("entity_id") == "a")
    assert len(a) == 2
    assert a["usable"].to_list() == [True, False]

    b = result.filter(pl.col("entity_id") == "b")
    assert len(b) == 1
    assert b["usable"].to_list() == [True]


def test_preserves_extra_columns():
    """Columns beyond the core set pass through untouched."""
    intervals = pl.DataFrame(
        {
            "entity_id": ["a"],
            "start": [T(2024, 1, 10)],
            "end": [T(2024, 1, 20)],
            "metric": ["committed"],
            "host": ["h1"],
            "resource": ["nodes"],
            "value": [1.0],
        }
    )
    events = pl.DataFrame(
        {
            "host": ["h1"],
            "event_start": [T(2024, 1, 1)],
            "event_end": [T(2024, 2, 1)],
            "usable": [True],
        }
    )
    result = _tag(intervals, events, by="host")

    assert result["resource"][0] == "nodes"
    assert result["value"][0] == 1.0
    assert result["metric"][0] == "committed"


def test_partial_coverage_null_for_unmatched():
    """Hosts without usability events keep usable=null."""
    intervals = pl.DataFrame(
        {
            "entity_id": ["a", "b", "c"],
            "start": [T(2024, 1, 1), T(2024, 1, 1), T(2024, 1, 1)],
            "end": [T(2024, 3, 1), T(2024, 3, 1), T(2024, 3, 1)],
            "metric": ["reservable"] * 3,
            "host": ["h1", "h2", "h3"],
            "value": [1.0, 1.0, 1.0],
        }
    )
    # Only h1 has usability events — h2 and h3 have none
    events = pl.DataFrame(
        {
            "host": ["h1", "h1"],
            "event_start": [T(2024, 1, 1), T(2024, 2, 1)],
            "event_end": [T(2024, 2, 1), T(2024, 3, 1)],
            "usable": [True, False],
        }
    )
    result = _tag(intervals, events, by="host")

    # h1 split into 2, h2 and h3 stay as 1 each = 4 rows
    assert len(result) == 4
    # h2 and h3 have usable=null
    nulls = result.filter(pl.col("usable").is_null())
    assert len(nulls) == 2
    assert set(nulls["entity_id"].to_list()) == {"b", "c"}


def test_split_interval_value_preserved():
    """Splitting an interval doesn't change the value — each piece keeps the original."""
    intervals = pl.DataFrame(
        {
            "entity_id": ["a"],
            "start": [T(2024, 1, 1)],
            "end": [T(2024, 3, 1)],
            "metric": ["reservable"],
            "host": ["h1"],
            "value": [1.0],
        }
    )
    events = pl.DataFrame(
        {
            "host": ["h1", "h1"],
            "event_start": [T(2024, 1, 1), T(2024, 2, 1)],
            "event_end": [T(2024, 2, 1), T(2024, 3, 1)],
            "usable": [True, False],
        }
    )
    result = _tag(intervals, events, by="host")

    assert len(result) == 2
    # Each piece has value=1.0, not 0.5
    assert result["value"].to_list() == [1.0, 1.0]
