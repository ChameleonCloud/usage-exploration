"""Ingest module: load raw data and convert to intervals."""

import polars as pl

from chameleon_usage.constants import Tables
from chameleon_usage.ingest.adapters import (
    Adapter,
    AdapterRegistry,
    blazar_allocations_source,
    nova_instances_source,
)
from chameleon_usage.ingest.coerce import apply_temporal_clamp
from chameleon_usage.ingest.loader import load_raw_tables

##################
# Default registry
##################

novaHostTotal = Adapter(
    entity_col="hypervisor_hostname",
    quantity_type="total",
    source=lambda t: t[Tables.NOVA_HOSTS],
    context_cols={
        "hypervisor_hostname": "hypervisor_hostname",
    },
)
blazarHostReservable = Adapter(
    entity_col="hypervisor_hostname",
    quantity_type="reservable",
    source=lambda t: t[Tables.BLAZAR_HOSTS],
    context_cols={
        "id": "blazar_host_id",
        "hypervisor_hostname": "hypervisor_hostname",
    },
)


blazarAllocCommitted = Adapter(
    entity_col="id",  # allocation ID
    quantity_type="committed",
    source=blazar_allocations_source,
    context_cols={
        "id": "blazar_allocation_id",
        "lease_id": "blazar_lease_id",
        "reservation_id": "blazar_reservation_id",
        "compute_host_id": "blazar_host_id",
        "hypervisor_hostname": "hypervisor_hostname",
    },
    start_col="effective_start",
    end_col="effective_end",
)
novaInstanceOccupied = Adapter(
    entity_col="uuid",
    quantity_type="occupied",
    source=nova_instances_source,
    context_cols={
        "uuid": "instance_id",
        "blazar_reservation_id": "blazar_reservation_id",
        "node": "hypervisor_hostname",
    },
)

REGISTRY = AdapterRegistry(
    [
        novaHostTotal,
        blazarHostReservable,
        blazarAllocCommitted,
        novaInstanceOccupied,
    ]
)


def load_intervals(base_path: str, site_name: str) -> pl.LazyFrame:
    tables = load_raw_tables(base_path, site_name)
    intervals = REGISTRY.to_intervals(tables)

    total = intervals.filter(pl.col("quantity_type").eq("total"))
    reservable = intervals.filter(pl.col("quantity_type").eq("reservable"))
    committed = intervals.filter(pl.col("quantity_type").eq("committed"))
    occupied = intervals.filter(pl.col("quantity_type").eq("occupied"))

    clamped_reservable = apply_temporal_clamp(
        reservable, parents=total, join_keys=["hypervisor_hostname"]
    )
    clamped_committed = apply_temporal_clamp(
        committed, parents=clamped_reservable, join_keys=["blazar_host_id"]
    )
    clamped_occupied = apply_temporal_clamp(
        occupied,
        parents=clamped_committed,
        join_keys=["blazar_reservation_id", "hypervisor_hostname"],
    )

    return pl.concat(
        [total, clamped_reservable, clamped_committed, clamped_occupied],
        how="diagonal",
    )
