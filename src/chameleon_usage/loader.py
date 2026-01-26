import warnings

import ibis
import polars as pl

from chameleon_usage.utils import SiteConfig

TABLES = {
    "blazar": [
        "computehosts",
        "leases",
        "reservations",
        "computehost_allocations",
        "computehost_reservations",
        "instance_reservations",
        "computehost_extra_capabilities",
        "extra_capabilities",
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
}


def _fetch_table(db_conn: ibis.BaseBackend, schemaname: str, tablename: str):
    return db_conn.table(tablename, database=schemaname).to_polars()


def dump_site_to_parquet(config: SiteConfig):
    for schema, tables in TABLES.items():
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Unable to set session timezone")
            conn = ibis.connect(getattr(config.db_uris, schema))

        for table in tables:
            tablename = f"{schema}.{table}"
            output_file = f"{config.raw_spans}/{tablename}"
            try:
                df = _fetch_table(conn, schema, table)
            except Exception as ex:
                print(f"Couldn't fetch {tablename}, got {ex}, skipping...")
            else:
                print(f"fetched {df.height} rows from {tablename}")
                df.write_parquet(output_file)
