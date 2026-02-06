"""
Module: Dumps Openstack Mysql to parquet files.

Supports local paths or object store (s3://, gs://) if s3fs/gcsfs installed.
"""

import logging
import os
import warnings

import ibis
from ibis.backends.mysql import MySQLdb
from ibis.common.exceptions import TableNotFound

logger = logging.getLogger(__name__)
TABLE_FLIP = "(╯°□°)╯︵ ┻━┻"

# Tables to dump, grouped by schema.
# Keep in sync with sources.SOURCE_REGISTRY if adding new tables.
TABLES = {
    "nova": [
        "compute_nodes",
        "instances",
        "instance_actions",
        "instance_actions_events",
    ],
    "nova_api": [
        "request_specs",
    ],
    "blazar": [
        "computehosts",
        "computehost_allocations",
        "reservations",
        "instance_reservations",
        "leases",
        "devices",
        "device_allocations",
        "device_extra_capabilities",
        "device_reservations",
    ],
    "zun": [
        "container",
        "container_actions",
        "container_actions_events",
    ],
    "chameleon_usage": [
        "node_usage_report_cache",
        "node_count_cache",
        "node_usage",
        "node_event",
        "node_maintenance",
        "node_project_usage_report_cache",
    ],
}


def generate_grant_sql(user: str = "usage_exporter", host: str = "%") -> str:
    """Generate SQL GRANT statements for read access to required tables."""
    lines = [
        "-- Grant read access for chameleon-usage extractor",
        "-- Run as MySQL admin (e.g., root)",
        "",
        f"CREATE USER IF NOT EXISTS '{user}'@'{host}' IDENTIFIED BY 'CHANGE_ME';",
        "",
    ]
    for schema, tablenames in TABLES.items():
        for tablename in tablenames:
            lines.append(f"GRANT SELECT ON {schema}.{tablename} TO '{user}'@'{host}';")
        lines.append("")
    lines.append("FLUSH PRIVILEGES;")
    return "\n".join(lines)


def _connect(db_uri: str) -> ibis.BaseBackend:
    """Connect to database, suppressing timezone warnings."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Unable to set session timezone")
        return ibis.connect(db_uri)


def dump_to_parquet(db_uri: str, output_path: str) -> dict[str, str]:
    """
    Dump all tables to parquet files.

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
    results = {}
    logger.info("Extracting tables to %s", output_path)

    for schema, tablenames in TABLES.items():
        for tablename in tablenames:
            key = f"{schema}.{tablename}"
            output_file = f"{output_path}/{key}.parquet"

            try:
                table = conn.table(tablename, database=schema)
                table.to_parquet(output_file, compression="zstd")

                num_rows = table.count().execute()
                results[key] = str(num_rows)
                logger.info("  %s %s: %s rows", TABLE_FLIP, key, num_rows)
            except TableNotFound:
                results[key] = "MISSING"
                logger.info("  %s %s: MISSING", TABLE_FLIP, key)
            except MySQLdb.OperationalError as exc:
                code = exc.args[0] if exc.args else None
                if code in (2002, 2003):
                    status = "CONNECTION_ERROR"
                elif code in (1044, 1045):
                    status = "AUTH_ERROR"
                elif code in (1142, 1143):
                    status = "PERMISSION_ERROR"
                elif code == 1146:
                    status = "MISSING"
                else:
                    status = "OPERATIONAL_ERROR"
                results[key] = status
                logger.info("  %s %s: %s (%s)", TABLE_FLIP, key, status, exc)
            except MySQLdb.ProgrammingError as exc:
                code = exc.args[0] if exc.args else None
                if code == 1146:
                    status = "MISSING"
                elif code in (1044, 1045):
                    status = "AUTH_ERROR"
                elif code in (1142, 1143):
                    status = "PERMISSION_ERROR"
                else:
                    status = "PROGRAMMING_ERROR"
                results[key] = status
                logger.info("  %s %s: %s (%s)", TABLE_FLIP, key, status, exc)

    return results
