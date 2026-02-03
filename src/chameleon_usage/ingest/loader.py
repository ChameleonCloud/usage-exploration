"""Load raw data and convert to intervals."""

import polars as pl

from chameleon_usage.sources import SOURCE_REGISTRY, SourceSpec


def _load_parquet(path: str, spec: SourceSpec, validate: bool = False):
    parquet_path = f"{path}/{spec.db_schema}.{spec.db_table}.parquet"
    df = pl.scan_parquet(parquet_path)

    if validate:
        df = spec.model.validate(df)

    return df


def load_raw_tables(base_path: str, site_name: str) -> dict[str, pl.LazyFrame]:
    """Load all interval sources for a site, validate, and concatenate."""
    path = f"{base_path}/{site_name}"

    # Load raw tables with schema validation
    return {
        key: _load_parquet(path=path, spec=spec, validate=True)
        for key, spec in SOURCE_REGISTRY.items()
    }
