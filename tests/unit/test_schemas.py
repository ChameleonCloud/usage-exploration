"""Tests for chameleon_usage.schemas"""

from datetime import datetime

import polars as pl
import pytest
from pandera.errors import SchemaError

from chameleon_usage.schemas import _OrderedModel, PipelineSpec


class _TestModel(_OrderedModel):
    """Minimal schema for testing base behavior."""

    a: str
    b: int


def test_ordered_model_preserves_extras_and_reorders():
    """Extra columns kept, schema columns moved to front."""
    df = pl.LazyFrame({"extra": [1], "b": [2], "a": ["x"]})  # wrong order + extra
    result = _TestModel.validate(df)

    assert result.collect_schema().names() == ["a", "b", "extra"]


def test_ordered_model_enforces_schema():
    """Schema validation still runs on reordered data."""
    df = pl.LazyFrame({"a": [1], "b": [2]})  # a should be str, not int

    with pytest.raises(SchemaError):
        _TestModel.validate(df)


def test_pipeline_spec_validate_against_missing_cols():
    """Raises when group_cols missing from data."""
    spec = PipelineSpec(
        group_cols=("metric", "site"),
        time_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
    )
    df = pl.LazyFrame({"metric": ["x"], "value": [1]})  # missing 'site'

    with pytest.raises(ValueError, match="group_cols not in data.*site"):
        spec.validate_against(df)
