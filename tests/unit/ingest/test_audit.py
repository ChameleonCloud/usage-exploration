"""Tests for audit-table-to-interval conversion."""

import json
from datetime import datetime

import polars as pl
import pytest

from chameleon_usage.ingest.audit import audit_to_intervals, extract_json_fields


def _make_audit_df(rows: list[dict]) -> pl.LazyFrame:
    """Build an audit LazyFrame from a list of row dicts."""
    return pl.LazyFrame(rows).cast({"audit_changed_at": pl.Datetime})


# ---------------------------------------------------------------------------
# audit_to_intervals
# ---------------------------------------------------------------------------

class TestAuditToIntervals:
    """SPEC §4: INSERT/UPDATE/DELETE rows → intervals via lead() window."""

    def test_basic_lifecycle(self):
        """INSERT → UPDATE → UPDATE → DELETE = 3 intervals."""
        df = _make_audit_df([
            {"id": "h1", "audit_event_type": "INSERT", "audit_changed_at": datetime(2025, 1, 1), "data": "{}"},
            {"id": "h1", "audit_event_type": "UPDATE", "audit_changed_at": datetime(2025, 3, 15), "data": "{}"},
            {"id": "h1", "audit_event_type": "UPDATE", "audit_changed_at": datetime(2025, 3, 16), "data": "{}"},
            {"id": "h1", "audit_event_type": "DELETE", "audit_changed_at": datetime(2025, 6, 1), "data": "{}"},
        ])
        result = audit_to_intervals(df).collect()

        assert len(result) == 3
        assert result["start"].to_list() == [
            datetime(2025, 1, 1),
            datetime(2025, 3, 15),
            datetime(2025, 3, 16),
        ]
        assert result["end"].to_list() == [
            datetime(2025, 3, 15),
            datetime(2025, 3, 16),
            datetime(2025, 6, 1),
        ]

    def test_backfill_insert_delete(self):
        """Backfill-only: INSERT + DELETE = 1 interval."""
        df = _make_audit_df([
            {"id": "h1", "audit_event_type": "INSERT", "audit_changed_at": datetime(2025, 1, 1), "data": "{}"},
            {"id": "h1", "audit_event_type": "DELETE", "audit_changed_at": datetime(2025, 6, 1), "data": "{}"},
        ])
        result = audit_to_intervals(df).collect()

        assert len(result) == 1
        assert result["start"][0] == datetime(2025, 1, 1)
        assert result["end"][0] == datetime(2025, 6, 1)

    def test_backfill_insert_only(self):
        """Backfill-only active host: INSERT with no DELETE = open-ended interval."""
        df = _make_audit_df([
            {"id": "h1", "audit_event_type": "INSERT", "audit_changed_at": datetime(2025, 1, 1), "data": "{}"},
        ])
        result = audit_to_intervals(df).collect()

        assert len(result) == 1
        assert result["start"][0] == datetime(2025, 1, 1)
        assert result["end"][0] is None

    def test_multiple_entities(self):
        """Each entity gets independent intervals."""
        df = _make_audit_df([
            {"id": "h1", "audit_event_type": "INSERT", "audit_changed_at": datetime(2025, 1, 1), "data": "{}"},
            {"id": "h1", "audit_event_type": "DELETE", "audit_changed_at": datetime(2025, 6, 1), "data": "{}"},
            {"id": "h2", "audit_event_type": "INSERT", "audit_changed_at": datetime(2025, 2, 1), "data": "{}"},
        ])
        result = audit_to_intervals(df).collect()

        assert len(result) == 2
        h1 = result.filter(pl.col("id") == "h1")
        h2 = result.filter(pl.col("id") == "h2")
        assert h1["end"][0] == datetime(2025, 6, 1)
        assert h2["end"][0] is None

    def test_delete_rows_dropped(self):
        """DELETE rows should not appear in output."""
        df = _make_audit_df([
            {"id": "h1", "audit_event_type": "INSERT", "audit_changed_at": datetime(2025, 1, 1), "data": "{}"},
            {"id": "h1", "audit_event_type": "DELETE", "audit_changed_at": datetime(2025, 6, 1), "data": "{}"},
        ])
        result = audit_to_intervals(df).collect()
        assert "DELETE" not in result["audit_event_type"].to_list()


# ---------------------------------------------------------------------------
# extract_json_fields
# ---------------------------------------------------------------------------

class TestExtractJsonFields:
    def test_extracts_named_fields(self):
        data = json.dumps({"reservable": 1, "disabled": 0, "status": "active"})
        df = pl.LazyFrame({"data": [data]})
        result = extract_json_fields(df, ["reservable", "disabled", "status"]).collect()

        assert result["reservable"][0] == "1"
        assert result["disabled"][0] == "0"
        assert result["status"][0] == "active"

    def test_preserves_existing_columns(self):
        data = json.dumps({"vcpus": 48})
        df = pl.LazyFrame({"id": ["h1"], "data": [data]})
        result = extract_json_fields(df, ["vcpus"]).collect()

        assert "id" in result.columns
        assert result["id"][0] == "h1"
