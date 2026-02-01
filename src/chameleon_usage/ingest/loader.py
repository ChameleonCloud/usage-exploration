"""Load raw data and convert to intervals."""

import polars as pl

from chameleon_usage.constants import Tables
from chameleon_usage.ingest import rawschemas as raw


def load_raw_tables(base_path: str, site_name: str) -> dict[str, pl.LazyFrame]:
    """Load all interval sources for a site, validate, and concatenate."""
    path = f"{base_path}/{site_name}"

    # Load raw tables with schema validation
    return {
        Tables.NOVA_HOSTS: raw.NovaHostRaw.validate(
            pl.scan_parquet(f"{path}/nova.compute_nodes.parquet")
        ),
        Tables.BLAZAR_HOSTS: raw.BlazarHostRaw.validate(
            pl.scan_parquet(f"{path}/blazar.computehosts.parquet")
        ),
        Tables.BLAZAR_ALLOC: raw.BlazarAllocationRaw.validate(
            pl.scan_parquet(f"{path}/blazar.computehost_allocations.parquet")
        ),
        Tables.BLAZAR_RES: raw.BlazarReservationRaw.validate(
            pl.scan_parquet(f"{path}/blazar.reservations.parquet")
        ),
        Tables.BLAZAR_LEASES: raw.BlazarLeaseRaw.validate(
            pl.scan_parquet(f"{path}/blazar.leases.parquet")
        ),
        Tables.NOVA_INSTANCES: raw.NovaInstanceRaw.validate(
            pl.scan_parquet(f"{path}/nova.instances.parquet")
        ),
        Tables.NOVA_REQUEST_SPECS: raw.NovaRequestSpecRaw.validate(
            pl.scan_parquet(f"{path}/nova_api.request_specs.parquet")
        ),
        Tables.NOVA_ACTIONS: pl.scan_parquet(
            f"{path}/nova.instance_actions.parquet"
        ),
        Tables.NOVA_ACTION_EVENTS: pl.scan_parquet(
            f"{path}/nova.instance_actions_events.parquet"
        ),
    }
