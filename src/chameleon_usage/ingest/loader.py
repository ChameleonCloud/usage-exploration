"""Load raw data and convert to intervals."""

import polars as pl

from chameleon_usage.ingest import rawschemas as raw
from chameleon_usage.ingest import adapters
from chameleon_usage.schemas import IntervalSchema


def load_intervals(base_path: str, site_name: str) -> pl.LazyFrame:
    """Load all interval sources for a site, validate, and concatenate."""
    path = f"{base_path}/{site_name}"

    # Load raw tables with schema validation
    nova_hosts = raw.NovaHostRaw.validate(
        pl.scan_parquet(f"{path}/nova.compute_nodes.parquet")
    )
    blazar_hosts = raw.BlazarHostRaw.validate(
        pl.scan_parquet(f"{path}/blazar.computehosts.parquet")
    )
    blazar_alloc = raw.BlazarAllocationRaw.validate(
        pl.scan_parquet(f"{path}/blazar.computehost_allocations.parquet")
    )
    blazar_res = raw.BlazarReservationRaw.validate(
        pl.scan_parquet(f"{path}/blazar.reservations.parquet")
    )
    blazar_leases = raw.BlazarLeaseRaw.validate(
        pl.scan_parquet(f"{path}/blazar.leases.parquet")
    )
    nova_instances = raw.NovaInstanceRaw.validate(
        pl.scan_parquet(f"{path}/nova.instances.parquet")
    )

    # Convert to intervals
    intervals = pl.concat([
        adapters.nova_hosts_to_intervals(nova_hosts),
        adapters.blazar_hosts_to_intervals(blazar_hosts),
        adapters.blazar_allocations_to_intervals(
            blazar_alloc, blazar_res, blazar_leases, blazar_hosts
        ),
        adapters.nova_instances_to_intervals(nova_instances),
    ])

    return IntervalSchema.validate(intervals)
