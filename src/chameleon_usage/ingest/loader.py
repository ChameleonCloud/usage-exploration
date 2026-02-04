"""Load raw data and convert to intervals."""

import os

import polars as pl

from chameleon_usage.sources import SOURCE_REGISTRY, SourceSpec


def _load_parquet(path: str, spec: SourceSpec, validate: bool = False):
    parquet_path = f"{path}/{spec.db_schema}.{spec.db_table}.parquet"
    df = pl.scan_parquet(parquet_path)

    if validate:
        df = spec.model.validate(df)

    return df


def load_raw_tables(parquet_path: str) -> dict[str, pl.LazyFrame]:
    """Load all interval sources for a site, validate, and concatenate.

    Skips tables whose parquet files don't exist.
    """
    tables = {}
    for key, spec in SOURCE_REGISTRY.items():
        parquet_file = f"{parquet_path}/{spec.db_schema}.{spec.db_table}.parquet"
        if os.path.exists(parquet_file):
            tables[key] = _load_parquet(path=parquet_path, spec=spec, validate=True)
    return tables
