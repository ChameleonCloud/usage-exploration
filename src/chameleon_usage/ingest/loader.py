"""Load raw data and convert to intervals."""

import logging

import polars as pl

from chameleon_usage.exceptions import (
    RawTableMissingError,
    classify_raw_table_load_error,
)
from chameleon_usage.sources import SOURCE_REGISTRY, SourceSpec

logger = logging.getLogger(__name__)


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
    missing: list[tuple[str, str]] = []
    for key, spec in SOURCE_REGISTRY.items():
        table_path = f"{parquet_path}/{spec.db_schema}.{spec.db_table}.parquet"
        try:
            table = _load_parquet(path=parquet_path, spec=spec, validate=True)
            table.collect_schema()
            logger.debug("Loaded %s from %s", key, table_path)
        except Exception as exc:
            typed_error = classify_raw_table_load_error(table_path, exc)
            if isinstance(typed_error, RawTableMissingError):
                logger.debug("Missing %s at %s; skipping", key, table_path)
                missing.append((key, table_path))
                continue
            logger.error("Failed loading %s from %s", key, table_path)
            raise typed_error from exc
        tables[key] = table
    logger.info(
        "Loaded %d/%d raw tables from %s",
        len(tables),
        len(SOURCE_REGISTRY),
        parquet_path,
    )
    if missing:
        missing_keys = ", ".join(key for key, _ in missing)
        logger.warning("Missing raw tables (%d): %s", len(missing), missing_keys)
        for key, path in missing:
            logger.info("  missing %s -> %s", key, path)
    return tables
