"""Tests for `_usability_events` — chains intervals by hypervisor_hostname
so duplicate blazar `id` rows for the same hostname don't produce overlaps.
"""

import json
from datetime import datetime

import polars as pl

from chameleon_usage.ingest import _usability_events
from chameleon_usage.math.intervals import tag_intervals
from chameleon_usage.sources import Tables


def _make_audit_df(rows: list[dict]) -> pl.LazyFrame:
    """Build an `audit_blazar_computehosts` LazyFrame from row dicts."""
    normalized = []
    for r in rows:
        r = dict(r)
        if isinstance(r.get("data"), dict):
            r["data"] = json.dumps(r["data"])
        r.setdefault("audit_changed_at", r["audit_event_time"])
        r.setdefault("audit_id", 0)
        r.setdefault("audit_changed_by", "test")
        normalized.append(r)
    return pl.LazyFrame(normalized).cast(
        {"audit_event_time": pl.Datetime, "audit_changed_at": pl.Datetime}
    )


def _payload(hostname: str, reservable: int = 1, disabled: int = 0) -> dict:
    return {
        "hypervisor_hostname": hostname,
        "vcpus": 48,
        "memory_mb": 192000,
        "local_gb": 1000,
        "status": "active",
        "reservable": reservable,
        "disabled": disabled,
    }


def test_duplicate_ids_for_one_hostname_do_not_overlap():
    """The bug case: two blazar ids for one hypervisor with overlapping
    lifetimes used to crash the pipeline because per-id chains produced
    overlapping intervals when re-keyed to hypervisor_hostname.
    Chaining by hostname directly must produce non-overlapping intervals
    that survive `tag_intervals`'s `_raise_on_overlaps` check.
    """
    df = _make_audit_df(
        [
            {
                "id": "old",
                "audit_event_type": "INSERT",
                "audit_event_time": datetime(2015, 12, 22),
                "data": _payload("dup"),
            },
            {
                "id": "new",
                "audit_event_type": "INSERT",
                "audit_event_time": datetime(2022, 7, 27),
                "data": _payload("dup"),
            },
            {
                "id": "old",
                "audit_event_type": "UPDATE",
                "audit_event_time": datetime(2023, 1, 1),
                "data": _payload("dup", reservable=0, disabled=1),
            },
        ]
    )
    events = _usability_events({Tables.AUDIT_BLAZAR_HOSTS: df})
    intervals = pl.LazyFrame(
        {
            "hypervisor_hostname": ["dup"],
            "start": [datetime(2024, 1, 1)],
            "end": [datetime(2024, 6, 1)],
        }
    )
    # Would have raised IntervalOverlapError under the old per-id chain.
    out = tag_intervals(intervals, events, by="hypervisor_hostname").collect()
    assert len(out) >= 1


def test_pre_cutoff_intervals_dropped_and_clamped():
    """Earliest audit_changed_at sets the cutoff. Intervals ending entirely
    before cutoff get dropped; intervals straddling cutoff get clamped.
    """
    cutoff = datetime(2024, 1, 1)
    df = _make_audit_df(
        [
            {
                "id": "h1",
                "audit_event_type": "INSERT",
                "audit_event_time": datetime(2020, 1, 1),
                "audit_changed_at": cutoff,
                "data": _payload("h1"),
            },
            {
                "id": "h1",
                "audit_event_type": "UPDATE",
                "audit_event_time": datetime(2023, 6, 1),
                "audit_changed_at": datetime(2024, 6, 1),
                "data": _payload("h1", reservable=0),
            },
            {
                "id": "h1",
                "audit_event_type": "UPDATE",
                "audit_event_time": datetime(2025, 1, 1),
                "audit_changed_at": datetime(2025, 1, 1),
                "data": _payload("h1", reservable=1),
            },
        ]
    )
    result = _usability_events({Tables.AUDIT_BLAZAR_HOSTS: df}).collect()
    pairs = list(zip(result["event_start"].to_list(), result["event_end"].to_list()))
    # Pre-cutoff [2020 → 2023-06] dropped. Straddling [2023-06 → 2025] clamped to cutoff.
    # Final [2025 → null] kept as-is.
    assert (cutoff, datetime(2025, 1, 1)) in pairs
    assert (datetime(2025, 1, 1), None) in pairs
    assert all(end is None or end > cutoff for _, end in pairs)
