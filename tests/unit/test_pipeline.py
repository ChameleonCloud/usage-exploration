"""
Tests for chameleon_usage.pipeline

PIPELINE: Domain-aware wrappers around pure transforms.
- PipelineSpec carries group_cols through the pipeline
- Validation at stage boundaries catches missing columns early
- collapse_dimension handles the common pattern of filtering then re-aggregating
"""

from datetime import datetime

import polars as pl
import pytest

from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.pipeline import (
    add_site_context,
    collapse_dimension,
    compute_derived_metrics,
    intervals_to_counts,
    resample,
)
from chameleon_usage.schemas import PipelineSpec


# =============================================================================
# PipelineSpec validation
# =============================================================================


def test_spec_validates_interval_stage():
    spec = PipelineSpec(group_cols=("quantity_type",))
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
    spec = PipelineSpec(group_cols=("quantity_type",))
    df = pl.LazyFrame(
        {
            "start": [datetime(2024, 1, 1)],
            "end": [datetime(2024, 1, 2)],
        }
    )
    with pytest.raises(ValueError, match="missing columns"):
        spec.validate_stage(df, "interval")


def test_spec_requires_group_cols():
    spec = PipelineSpec(group_cols=("quantity_type", "resource_type"))
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
# intervals_to_counts
# =============================================================================


def test_intervals_to_counts_basic():
    spec = PipelineSpec(group_cols=("quantity_type",))
    df = pl.LazyFrame(
        {
            "entity_id": ["a", "b"],
            "start": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
            "end": [datetime(2024, 1, 3), datetime(2024, 1, 4)],
            "quantity_type": ["reservable", "reservable"],
        }
    )
    counts = intervals_to_counts(df, spec).collect()

    assert "timestamp" in counts.columns
    assert "count" in counts.columns
    assert "quantity_type" in counts.columns
    assert counts["count"].to_list() == [1, 2, 1, 0]


def test_intervals_to_counts_multiple_groups():
    spec = PipelineSpec(group_cols=("quantity_type", "site"))
    df = pl.LazyFrame(
        {
            "entity_id": ["a", "b"],
            "start": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            "end": [datetime(2024, 1, 2), datetime(2024, 1, 2)],
            "quantity_type": ["reservable", "reservable"],
            "site": ["uc", "tacc"],
        }
    )
    counts = intervals_to_counts(df, spec).collect()

    uc = counts.filter(pl.col("site") == "uc")
    tacc = counts.filter(pl.col("site") == "tacc")
    assert uc["count"].to_list() == [1, 0]
    assert tacc["count"].to_list() == [1, 0]


# =============================================================================
# collapse_dimension
# =============================================================================


def test_collapse_dimension_sums_counts():
    spec = PipelineSpec(group_cols=("quantity_type", "status"))
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
    result = collapsed.collect()
    assert result["count"][0] == 8


def test_collapse_dimension_filters_excluded():
    spec = PipelineSpec(group_cols=("quantity_type", "status"))
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
    spec = PipelineSpec(group_cols=("quantity_type",))
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
    spec = PipelineSpec(group_cols=("quantity_type",))
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


def test_derived_metrics_computes_idle():
    spec = PipelineSpec(group_cols=("quantity_type",))
    df = pl.LazyFrame(
        {
            "timestamp": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            "quantity_type": [QT.COMMITTED, QT.OCCUPIED],
            "count": [10.0, 4.0],
        }
    )
    result = compute_derived_metrics(df, spec).collect()

    idle = result.filter(pl.col("quantity_type") == QT.IDLE)
    assert idle["count"][0] == 6.0


def test_derived_metrics_preserves_extra_group_cols():
    spec = PipelineSpec(group_cols=("quantity_type", "resource_type"))
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


# =============================================================================
# resample
# =============================================================================


def test_resample_buckets_by_interval():
    spec = PipelineSpec(group_cols=("quantity_type",))
    df = pl.LazyFrame(
        {
            "timestamp": [
                datetime(2024, 1, 1, 0, 0),
                datetime(2024, 1, 1, 12, 0),
                datetime(2024, 1, 2, 6, 0),
            ],
            "quantity_type": ["reservable", "reservable", "reservable"],
            "count": [10.0, 20.0, 30.0],
        }
    )
    result = resample(df, "1d", spec).collect().sort("timestamp")

    assert len(result) == 2
    assert result["count"][0] == 15.0  # avg of 10, 20
    assert result["count"][1] == 30.0


# =============================================================================
# add_site_context
# =============================================================================


def test_add_site_context():
    spec = PipelineSpec(group_cols=("quantity_type",))
    df = pl.LazyFrame(
        {
            "timestamp": [datetime(2024, 1, 1)],
            "quantity_type": ["reservable"],
            "count": [10.0],
        }
    )
    result = add_site_context(df, spec, "chi_uc").collect()

    assert result["site"][0] == "chi_uc"
    assert result["collector_type"][0] == "current"
