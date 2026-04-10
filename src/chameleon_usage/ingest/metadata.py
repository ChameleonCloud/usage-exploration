"""Read parquet metadata without loading data.

Uses pyarrow to read the file footer (a single range GET on S3),
extracting row counts and checking file existence.
"""

import logging
from dataclasses import dataclass

import pyarrow.parquet as pq

from chameleon_usage.exceptions import (
    RawTableMissingError,
    classify_raw_table_load_error,
)
from chameleon_usage.sources import SOURCE_REGISTRY, SourceSpec

logger = logging.getLogger(__name__)


@dataclass
class TableMeta:
    key: str
    path: str
    num_rows: int


def parquet_path(base: str, spec: SourceSpec) -> str:
    return f"{base}/{spec.db_schema}.{spec.db_table}.parquet"


def read_table_meta(base_path: str, spec: SourceSpec, key: str) -> TableMeta | None:
    """Read metadata for a single parquet file. Returns None if missing.

    Raises on non-missing errors (auth, connectivity, corrupt file).
    """
    path = parquet_path(base_path, spec)
    try:
        meta = pq.read_metadata(path)
    except Exception as exc:
        typed = classify_raw_table_load_error(path, exc)
        if isinstance(typed, RawTableMissingError):
            return None
        raise typed from exc
    return TableMeta(key=key, path=path, num_rows=meta.num_rows)


def read_site_meta(base_path: str) -> list[TableMeta]:
    """Read metadata for all known tables at a site.

    Raises on the first non-missing error (auth, connectivity).
    """
    results = []
    for key, spec in SOURCE_REGISTRY.items():
        table_meta = read_table_meta(base_path, spec, key)
        if table_meta:
            results.append(table_meta)
    return results
