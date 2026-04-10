"""Read parquet metadata without loading data.

Uses pyarrow to read the file footer (a single range GET on S3),
extracting row counts, column statistics, and checking file existence.
"""

import logging
from dataclasses import dataclass
from datetime import datetime

import pyarrow.parquet as pq

from chameleon_usage.exceptions import (
    RawTableMissingError,
    classify_raw_table_load_error,
)
from chameleon_usage.sources import SOURCE_REGISTRY, SourceSpec

logger = logging.getLogger(__name__)


# Audit tables use audit_changed_at; everything else uses created_at.
def _time_col_for(spec: SourceSpec) -> str:
    if spec.db_schema == "openstack_audit":
        return "audit_changed_at"
    return "created_at"


def _get_col_stats(
    meta: pq.FileMetaData, col_name: str
) -> tuple[datetime, datetime] | None:
    """Extract (min, max) for a named column from the parquet footer.

    Parquet files are split into row groups, each with per-column statistics
    (min, max, null_count) in the footer. This scans all row groups to find
    the global min and max for the given column. Returns None if the column
    is missing or any row group lacks statistics.
    """
    overall_min = None
    overall_max = None
    for rg_idx in range(meta.num_row_groups):
        rg = meta.row_group(rg_idx)
        for col_idx in range(rg.num_columns):
            col = rg.column(col_idx)
            if col.path_in_schema != col_name:
                continue
            if not col.statistics or not col.statistics.has_min_max:
                return None
            if overall_min is None or col.statistics.min < overall_min:
                overall_min = col.statistics.min
            if overall_max is None or col.statistics.max > overall_max:
                overall_max = col.statistics.max
    if overall_min is None:
        return None
    return (overall_min, overall_max)


@dataclass
class TableMeta:
    key: str
    path: str
    num_rows: int
    time_min: datetime | None = None
    time_max: datetime | None = None


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

    time_col = _time_col_for(spec)
    stats = _get_col_stats(meta, time_col)
    time_min, time_max = stats if stats else (None, None)

    return TableMeta(
        key=key, path=path, num_rows=meta.num_rows, time_min=time_min, time_max=time_max
    )


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


def get_site_time_range(
    tables: list[TableMeta],
) -> tuple[datetime, datetime] | None:
    """Derive the overall (min, max) time range across all tables."""
    mins = [t.time_min for t in tables if t.time_min is not None]
    maxs = [t.time_max for t in tables if t.time_max is not None]
    if not mins or not maxs:
        return None
    return (min(mins), max(maxs))
