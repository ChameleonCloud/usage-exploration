"""
Module: Dumps Openstack Mysql to parquet files.

Supports local paths or object store (s3://, gs://) if s3fs/gcsfs installed.
"""

import os
import warnings

import ibis
from ibis.common.exceptions import TableNotFound

from chameleon_usage.sources import SOURCE_REGISTRY


def _get_tables_to_dump() -> dict[str, list[str]]:
    """Derive table list from SOURCE_REGISTRY."""
    tables: dict[str, list[str]] = {}
    for spec in SOURCE_REGISTRY.values():
        tables.setdefault(spec.db_schema, []).append(spec.db_table)
    return tables


def _connect(db_uri: str) -> ibis.BaseBackend:
    """Connect to database, suppressing timezone warnings."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Unable to set session timezone")
        return ibis.connect(db_uri)


def dump_to_parquet(db_uri: str, output_path: str) -> dict[str, str]:
    """
    Dump all tables from SOURCE_REGISTRY to parquet files.

    Args:
        db_uri: Database connection string (mysql://user:pass@host:port)
        output_path: Local path or object store URL (s3://bucket/path)

    Returns:
        Dict mapping table key to result string
    """
    # Ensure output directory exists (for local paths)
    if not output_path.startswith(("s3://", "gs://", "az://")):
        os.makedirs(output_path, exist_ok=True)

    conn = _connect(db_uri)
    tables = _get_tables_to_dump()
    results = {}

    for schema, tablenames in tables.items():
        for tablename in tablenames:
            key = f"{schema}.{tablename}"
            output_file = f"{output_path}/{key}.parquet"

            try:
                table = conn.table(tablename, database=schema)
                table.to_parquet(output_file)

                num_rows = table.count().execute()
                results[key] = str(num_rows)
                print(f"  {key}: {num_rows} rows")
            except TableNotFound:
                results[key] = "MISSING"
                print(f"  {key}: MISSING")

    return results
