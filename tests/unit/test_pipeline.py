"""
Tests for chameleon_usage.pipeline

PIPELINE: Domain-aware wrappers around pure transforms.
- PipelineSpec carries group_cols and time_range through the pipeline
- Validation at stage boundaries catches missing columns early
- run_pipeline() is the "can't hold it wrong" entry point
"""

from datetime import datetime

import polars as pl
import pytest

from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.pipeline import (
    clip_to_window,
    collapse_dimension,
    compute_derived_metrics,
)
from chameleon_usage.schemas import PipelineSpec

# Default time range for tests
TIME_RANGE = (datetime(2024, 1, 1), datetime(2024, 12, 31))


# =============================================================================
# PipelineSpec validation
# =============================================================================


def test_spec_validates_interval_stage():
    spec = PipelineSpec(group_cols=("quantity_type",), time_range=TIME_RANGE)
    df = pl.LazyFrame(
        {
            "entity_id": ["a"],
            "start": [datetime(2024, 1, 1)],
            "end": [datetime(2024, 1, 2)],
            "quantity_type": ["reservable"],
        }
    )
    spec.validate_stage(df, "interval")  # should not raise


def test_spec_rejects_missing_columns():
    spec = PipelineSpec(group_cols=("quantity_type",), time_range=TIME_RANGE)
    df = pl.LazyFrame(
        {
            "start": [datetime(2024, 1, 1)],
            "end": [datetime(2024, 1, 2)],
        }
    )
    with pytest.raises(ValueError, match="missing columns"):
        spec.validate_stage(df, "interval")


def test_spec_requires_group_cols():
    spec = PipelineSpec(group_cols=("quantity_type", "resource_type"), time_range=TIME_RANGE)
    df = pl.LazyFrame(
        {
            "entity_id": ["a"],
            "start": [datetime(2024, 1, 1)],
            "end": [datetime(2024, 1, 2)],
            "quantity_type": ["reservable"],
            # missing resource_type
        }
    )
    with pytest.raises(ValueError, match="resource_type"):
        spec.validate_stage(df, "interval")


# =============================================================================
# clip_to_window
# =============================================================================


def test_clip_to_window_filters_timestamps():
    spec = PipelineSpec(
        group_cols=("quantity_type",),
        time_range=(datetime(2024, 1, 2), datetime(2024, 1, 4)),
    )
    df = pl.LazyFrame(
        {
            "timestamp": [
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
                datetime(2024, 1, 3),
                datetime(2024, 1, 5),
            ],
            "quantity_type": ["reservable"] * 4,
            "count": [1, 2, 3, 4],
        }
    )
    result = clip_to_window(df, spec).collect()

    assert result["timestamp"].to_list() == [datetime(2024, 1, 2), datetime(2024, 1, 3)]


# =============================================================================
# collapse_dimension
# =============================================================================


def test_collapse_dimension_sums_counts():
    spec = PipelineSpec(group_cols=("quantity_type", "status"), time_range=TIME_RANGE)
    df = pl.LazyFrame(
        {
            "timestamp": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            "quantity_type": ["reservable", "reservable"],
            "status": ["valid", "clamped"],
            "count": [5, 3],
        }
    )
    collapsed, new_spec = collapse_dimension(df, spec, drop="status")

    assert new_spec.group_cols == ("quantity_type",)
    assert new_spec.time_range == TIME_RANGE  # preserved
    result = collapsed.collect()
    assert result["count"][0] == 8


def test_collapse_dimension_filters_excluded():
    spec = PipelineSpec(group_cols=("quantity_type", "status"), time_range=TIME_RANGE)
    df = pl.LazyFrame(
        {
            "timestamp": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            "quantity_type": ["reservable", "reservable"],
            "status": ["valid", "invalid"],
            "count": [5, 3],
        }
    )
    collapsed, _ = collapse_dimension(df, spec, drop="status", exclude=["invalid"])

    result = collapsed.collect()
    assert result["count"][0] == 5


def test_collapse_dimension_rejects_unknown_column():
    spec = PipelineSpec(group_cols=("quantity_type",), time_range=TIME_RANGE)
    df = pl.LazyFrame(
        {
            "timestamp": [datetime(2024, 1, 1)],
            "quantity_type": ["reservable"],
            "count": [5],
        }
    )
    with pytest.raises(ValueError, match="not in group_cols"):
        collapse_dimension(df, spec, drop="nonexistent")


# =============================================================================
# compute_derived_metrics
# =============================================================================


def test_derived_metrics_computes_available():
    spec = PipelineSpec(group_cols=("quantity_type",), time_range=TIME_RANGE)
    df = pl.LazyFrame(
        {
            "timestamp": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            "quantity_type": [QT.RESERVABLE, QT.COMMITTED],
            "count": [10.0, 3.0],
        }
    )
    result = compute_derived_metrics(df, spec).collect()

    available = result.filter(pl.col("quantity_type") == QT.AVAILABLE)
    assert available["count"][0] == 7.0


def test_derived_metrics_preserves_extra_group_cols():
    spec = PipelineSpec(group_cols=("quantity_type", "resource_type"), time_range=TIME_RANGE)
    df = pl.LazyFrame(
        {
            "timestamp": [datetime(2024, 1, 1)] * 4,
            "quantity_type": [QT.RESERVABLE, QT.COMMITTED, QT.RESERVABLE, QT.COMMITTED],
            "resource_type": ["vcpu", "vcpu", "memory", "memory"],
            "count": [10.0, 3.0, 100.0, 40.0],
        }
    )
    result = compute_derived_metrics(df, spec).collect()

    vcpu_avail = result.filter(
        (pl.col("quantity_type") == QT.AVAILABLE) & (pl.col("resource_type") == "vcpu")
    )
    mem_avail = result.filter(
        (pl.col("quantity_type") == QT.AVAILABLE)
        & (pl.col("resource_type") == "memory")
    )
    assert vcpu_avail["count"][0] == 7.0
    assert mem_avail["count"][0] == 60.0


