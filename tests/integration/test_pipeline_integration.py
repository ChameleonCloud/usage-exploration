"""Integration tests for the full pipeline."""

from datetime import datetime

import polars as pl

from chameleon_usage.constants import Metrics as M
from chameleon_usage.constants import ResourceTypes as RT
from chameleon_usage.ingest.adapters import Adapter, AdapterRegistry
from chameleon_usage.pipeline import run_pipeline
from chameleon_usage.schemas import IntervalModel, PipelineSpec


def test_adapter_output_matches_interval_schema():
    """Adapter output must match IntervalModel schema."""
    fake_source = pl.LazyFrame(
        {"id": ["a"], "created_at": [datetime(2024, 1, 1)], "deleted_at": [None]}
    )
    adapter = Adapter(
        entity_col="id",
        metric=M.RESERVABLE,
        source=lambda _: fake_source,
        resource_cols={"nodes": pl.lit(1)},
    )
    registry = AdapterRegistry([adapter])
    result = registry.to_intervals({})
    IntervalModel.validate(result)


def test_intervals_to_pipeline():
    """Intervals can be fed directly to run_pipeline."""
    intervals = pl.LazyFrame(
        {
            "entity_id": ["host1", "blazar1", "alloc1", "inst1"],
            "start": [datetime(2024, 1, 1)] * 4,
            "end": [
                datetime(2024, 1, 5),
                datetime(2024, 1, 5),
                datetime(2024, 1, 3),
                datetime(2024, 1, 3),
            ],
            "metric": [M.TOTAL, M.RESERVABLE, M.COMMITTED, M.OCCUPIED_RESERVATION],
            "resource": [RT.NODE] * 4,
            "value": [1.0] * 4,
        }
    )
    spec = PipelineSpec(
        group_cols=("metric", "resource"),
        time_range=(datetime(2024, 1, 1), datetime(2024, 1, 5)),
    )

    result = run_pipeline(intervals, spec).collect()

    assert len(result) > 0


def test_run_pipeline_produces_derived_metrics():
    spec = PipelineSpec(
        group_cols=("metric", "resource"),
        time_range=(datetime(2024, 1, 1), datetime(2024, 1, 5)),
    )
    df = pl.LazyFrame(
        {
            "entity_id": ["a", "b"],
            "start": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            "end": [datetime(2024, 1, 3), datetime(2024, 1, 3)],
            "metric": [M.RESERVABLE, M.COMMITTED],
            "resource": ["vcpu", "vcpu"],
            "value": [1.0, 1.0],
        }
    )
    result = run_pipeline(df, spec).collect()

    assert M.AVAILABLE_RESERVABLE in result["metric"].to_list()
