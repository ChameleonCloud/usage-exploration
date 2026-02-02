"""Integration tests for the full pipeline."""

from datetime import datetime

import polars as pl

from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.pipeline import run_pipeline
from chameleon_usage.schemas import PipelineSpec


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
            "metric": [QT.RESERVABLE, QT.COMMITTED],
            "resource": ["vcpu", "vcpu"],
            "value": [1.0, 1.0],
        }
    )
    result = run_pipeline(df, spec).collect()

    assert QT.AVAILABLE in result["metric"].to_list()
