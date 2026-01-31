"""
Module: Dumps Openstack Mysql to parquet files.
"""

import os
import warnings

import ibis

from chameleon_usage.common import SiteConfig

TABLES = {
    "blazar": [
        # generic
        "leases",
        "reservations",
        # computehost: baremetal and kvm
        "computehosts",
        "computehost_extra_capabilities",
        "computehost_reservations",  # physical:host
        "instance_reservations",  # flavor:instance
        "computehost_allocations",
        # CHI@Edge: Device Model
        "devices",
        "device_extra_capabilities",
        "device_reservations",
        "device_allocations",
        # do we need these?
        # "extra_capabilities", # on edge not kvm?
        # resource_properties # on kvm but not edge?
        # "events",
    ],
    "nova": [
        "compute_nodes",
        "instances",
        "instance_actions",
        "instance_actions_events",
        "instance_faults",
        "instance_extra",
        "services",
    ],
    "nova_api": [
        "request_specs",
        "flavors",
        "flavor_extra_specs",
        "flavor_projects",
    ],
    "zun": [
        "compute_node",
        "container",
        "container_actions",
        "container_actions_events",
        "zun_service",
    ],
    "chameleon_usage": [
        "node_count_cache",
        "node_event",
        "node_maintenance",
        "node_usage",
        "node_usage_report_cache",
        "node_project_usage_report_cache",
    ],
}


def _fetch_table(db_conn: ibis.BaseBackend, schemaname: str, tablename: str):
    return db_conn.table(tablename, database=schemaname).to_polars()


def _connect_schema(db_uri) -> ibis.BaseBackend:
    """Wrapper to handle warning on temp DBs."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Unable to set session timezone")
        return ibis.connect(db_uri)


def dump_site_to_parquet(config: SiteConfig, force: bool = False) -> dict[str, str]:
    results = {}
    for schema, uri in config.db_uris.items():
        conn = _connect_schema(uri)
        for table in TABLES.get(schema, []):
            key = f"{schema}.{table}"
            output_file = f"{config.raw_spans}/{schema}.{table}.parquet"

            if os.path.exists(output_file) and not force:
                results[key] = "SKIP"
                continue

            try:
                df = _fetch_table(conn, schema, table)
                df.write_parquet(output_file)
                results[key] = str(df.height)
            except Exception:
                results[key] = "_FAIL_"

    return results
