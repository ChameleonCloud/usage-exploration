"""Load raw data and convert to intervals."""

import logging

import polars as pl

from chameleon_usage.exceptions import classify_raw_table_load_error
from chameleon_usage.ingest.metadata import parquet_path, read_site_meta
from chameleon_usage.sources import SOURCE_REGISTRY, SourceSpec

logger = logging.getLogger(__name__)


def _load_parquet(base: str, spec: SourceSpec, validate: bool = False):
    path = parquet_path(base, spec)
    df = pl.scan_parquet(path)

    if validate:
        df = spec.model.validate(df)

    return df


def load_raw_tables(base_path: str) -> dict[str, pl.LazyFrame]:
    """Load all interval sources for a site, validate, and concatenate.

    Uses parquet metadata to quickly discover which files exist before
    attempting to load them. This avoids slow timeouts on missing files
    in object stores.
    """
    site_meta = read_site_meta(base_path)
    available = {m.key for m in site_meta}
    missing_keys = [k for k in SOURCE_REGISTRY if k not in available]
    if missing_keys:
        logger.warning(
            "Missing raw tables (%d): %s", len(missing_keys), ", ".join(missing_keys)
        )

    tables = {}
    for meta in site_meta:
        spec = SOURCE_REGISTRY[meta.key]
        try:
            table = _load_parquet(base=base_path, spec=spec, validate=True)
            table.collect_schema()
        except Exception as exc:
            typed_error = classify_raw_table_load_error(meta.path, exc)
            logger.error("Failed loading %s: %s", meta.key, typed_error)
            raise typed_error from exc
        tables[meta.key] = table

    logger.info(
        "Loaded %d/%d raw tables from %s",
        len(tables),
        len(SOURCE_REGISTRY),
        base_path,
    )
    return tables
