"""
Tests for chameleon_usage.pipeline

PIPELINE: Domain-aware wrappers around pure transforms.
- Pandera schemas validate at stage boundaries
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
# clip_to_window
# =============================================================================


def test_clip_to_window_filters_timestamps():
    spec = PipelineSpec(
        group_cols=("metric", "resource"),
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
            "metric": ["reservable"] * 4,
            "resource": ["vcpu"] * 4,
            "value": [1.0, 2.0, 3.0, 4.0],
        }
    )
    result = clip_to_window(df, spec).collect()

    assert result["timestamp"].to_list() == [datetime(2024, 1, 2), datetime(2024, 1, 3)]


# =============================================================================
# collapse_dimension
# =============================================================================


def test_collapse_dimension_sums_values():
    spec = PipelineSpec(group_cols=("metric", "status"), time_range=TIME_RANGE)
    df = pl.LazyFrame(
        {
            "timestamp": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            "metric": ["reservable", "reservable"],
            "status": ["valid", "clamped"],
            "value": [5, 3],
        }
    )
    collapsed, new_spec = collapse_dimension(df, spec, drop="status")

    assert new_spec.group_cols == ("metric",)
    assert new_spec.time_range == TIME_RANGE  # preserved
    result = collapsed.collect()
    assert result["value"][0] == 8


def test_collapse_dimension_filters_excluded():
    spec = PipelineSpec(group_cols=("metric", "status"), time_range=TIME_RANGE)
    df = pl.LazyFrame(
        {
            "timestamp": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            "metric": ["reservable", "reservable"],
            "status": ["valid", "invalid"],
            "value": [5, 3],
        }
    )
    collapsed, _ = collapse_dimension(df, spec, drop="status", exclude=["invalid"])

    result = collapsed.collect()
    assert result["value"][0] == 5


def test_collapse_dimension_rejects_unknown_column():
    spec = PipelineSpec(group_cols=("metric",), time_range=TIME_RANGE)
    df = pl.LazyFrame(
        {
            "timestamp": [datetime(2024, 1, 1)],
            "metric": ["reservable"],
            "value": [5],
        }
    )
    with pytest.raises(ValueError, match="not in group_cols"):
        collapse_dimension(df, spec, drop="nonexistent")


# =============================================================================
# compute_derived_metrics
# =============================================================================


def test_derived_metrics_computes_available():
    spec = PipelineSpec(group_cols=("metric", "resource"), time_range=TIME_RANGE)
    df = pl.LazyFrame(
        {
            "timestamp": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            "metric": [QT.RESERVABLE, QT.COMMITTED],
            "resource": ["vcpu", "vcpu"],
            "value": [10.0, 3.0],
        }
    )
    result = compute_derived_metrics(df, spec).collect()

    available = result.filter(pl.col("metric") == QT.AVAILABLE)
    assert available["value"][0] == 7.0


def test_derived_metrics_preserves_extra_group_cols():
    spec = PipelineSpec(group_cols=("metric", "resource"), time_range=TIME_RANGE)
    df = pl.LazyFrame(
        {
            "timestamp": [datetime(2024, 1, 1)] * 4,
            "metric": [QT.RESERVABLE, QT.COMMITTED, QT.RESERVABLE, QT.COMMITTED],
            "resource": ["vcpu", "vcpu", "memory", "memory"],
            "value": [10.0, 3.0, 100.0, 40.0],
        }
    )
    result = compute_derived_metrics(df, spec).collect()

    vcpu_avail = result.filter(
        (pl.col("metric") == QT.AVAILABLE) & (pl.col("resource") == "vcpu")
    )
    mem_avail = result.filter(
        (pl.col("metric") == QT.AVAILABLE) & (pl.col("resource") == "memory")
    )
    assert vcpu_avail["value"][0] == 7.0
    assert mem_avail["value"][0] == 60.0


