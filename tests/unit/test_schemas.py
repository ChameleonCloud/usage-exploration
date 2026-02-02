"""Tests for chameleon_usage.schemas"""

import polars as pl
import pytest
from pandera.errors import SchemaError

from chameleon_usage.schemas import _OrderedModel


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
