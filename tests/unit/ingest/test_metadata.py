"""Tests for parquet metadata reading."""

from datetime import datetime

import polars as pl
import pytest

from chameleon_usage.ingest.metadata import (
    get_site_time_range,
    read_site_meta,
    read_table_meta,
)
from chameleon_usage.sources import SOURCE_REGISTRY


@pytest.fixture
def parquet_dir(tmp_path):
    """Create a temp dir with a few parquet files matching SOURCE_REGISTRY keys."""
    specs_to_create = list(SOURCE_REGISTRY.items())[:3]
    for key, spec in specs_to_create:
        filename = f"{spec.db_schema}.{spec.db_table}.parquet"
        # Minimal parquet with the columns the schema expects
        df = pl.DataFrame({name: [] for name in spec.model.to_schema().columns.keys()})
        df.write_parquet(tmp_path / filename)
    return tmp_path, [k for k, _ in specs_to_create]


@pytest.fixture
def parquet_dir_with_data(tmp_path):
    """Create a parquet file with rows so column stats are populated."""
    from chameleon_usage.sources import Tables

    key = Tables.NOVA_INSTANCES
    spec = SOURCE_REGISTRY[key]
    filename = f"{spec.db_schema}.{spec.db_table}.parquet"
    df = pl.DataFrame(
        {
            "id": ["1", "2"],
            "uuid": ["aaa", "bbb"],
            "created_at": [datetime(2023, 1, 1), datetime(2024, 6, 15)],
            "deleted_at": pl.Series([None, None], dtype=pl.Datetime),
            "launched_at": [datetime(2023, 1, 1), datetime(2024, 6, 15)],
            "terminated_at": [datetime(2023, 1, 1), datetime(2024, 6, 15)],
            "host": ["h1", "h2"],
            "node": ["n1", "n2"],
            "vcpus": [1, 2],
            "memory_mb": [512, 1024],
            "root_gb": [10, 20],
        }
    )
    df.write_parquet(tmp_path / filename)
    return tmp_path, key, spec


def test_read_table_meta_returns_meta_for_existing_file(parquet_dir):
    path, keys = parquet_dir
    key = keys[0]
    spec = SOURCE_REGISTRY[key]
    result = read_table_meta(str(path), spec, key)
    assert result is not None
    assert result.key == key
    assert result.num_rows == 0
    # Empty file has no stats
    assert result.time_min is None
    assert result.time_max is None


def test_read_table_meta_has_time_stats(parquet_dir_with_data):
    path, key, spec = parquet_dir_with_data
    result = read_table_meta(str(path), spec, key)
    assert result is not None
    assert result.time_min == datetime(2023, 1, 1)
    assert result.time_max == datetime(2024, 6, 15)


def test_get_site_time_range_from_tables(parquet_dir_with_data):
    path, key, spec = parquet_dir_with_data
    tables = read_site_meta(str(path))
    time_range = get_site_time_range(tables)
    assert time_range is not None
    assert time_range[0] == datetime(2023, 1, 1)
    assert time_range[1] == datetime(2024, 6, 15)


def test_get_site_time_range_empty():
    assert get_site_time_range([]) is None


def test_read_table_meta_returns_none_for_missing_file(tmp_path):
    key, spec = next(iter(SOURCE_REGISTRY.items()))
    result = read_table_meta(str(tmp_path), spec, key)
    assert result is None


def test_read_table_meta_raises_on_non_missing_error(tmp_path):
    """A corrupt file (not a valid parquet) should raise, not return None."""
    key, spec = next(iter(SOURCE_REGISTRY.items()))
    filename = f"{spec.db_schema}.{spec.db_table}.parquet"
    (tmp_path / filename).write_text("not a parquet file")
    with pytest.raises(Exception):
        read_table_meta(str(tmp_path), spec, key)


def test_read_site_meta_finds_existing_tables(parquet_dir):
    path, keys = parquet_dir
    results = read_site_meta(str(path))
    found_keys = {t.key for t in results}
    assert found_keys == set(keys)


def test_read_site_meta_empty_dir(tmp_path):
    results = read_site_meta(str(tmp_path))
    assert results == []


def test_read_site_meta_raises_on_non_missing_error(tmp_path):
    """If every file fails with a non-FileNotFoundError, should raise."""
    # Write corrupt files for all registry entries
    for key, spec in SOURCE_REGISTRY.items():
        filename = f"{spec.db_schema}.{spec.db_table}.parquet"
        (tmp_path / filename).write_text("corrupt")
    with pytest.raises(Exception):
        read_site_meta(str(tmp_path))
