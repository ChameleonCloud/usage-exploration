"""Ingest module: load raw data and convert to intervals."""

from datetime import datetime

import polars as pl

from chameleon_usage.constants import ResourceTypes, Tables
from chameleon_usage.ingest.adapters import (
    Adapter,
    AdapterRegistry,
    blazar_allocations_source,
    nova_instances_source,
)
from chameleon_usage.ingest.coerce import clamp_hierarchy
from chameleon_usage.ingest.loader import load_raw_tables

##################
# Default registry
##################

novaHostTotal = Adapter(
    entity_col="hypervisor_hostname",
    metric="total",
    source=lambda t: t[Tables.NOVA_HOSTS],
    context_cols={
        "hypervisor_hostname": "hypervisor_hostname",
    },
    resource_cols={
        ResourceTypes.NODE: pl.lit(1),
        ResourceTypes.VCPUS_OVERCOMMIT: (
            pl.col("vcpus") * pl.col("cpu_allocation_ratio")
        ),
        ResourceTypes.VCPUS: (pl.col("vcpus")),
        ResourceTypes.MEMORY_MB: pl.col("memory_mb"),  # TODO handle overcommit
        ResourceTypes.DISK_GB: pl.col("local_gb"),  # TODO handle overcommit
    },
)
blazarHostReservable = Adapter(
    entity_col="hypervisor_hostname",
    metric="reservable",
    source=lambda t: t[Tables.BLAZAR_HOSTS],
    context_cols={
        "id": "blazar_host_id",
        "hypervisor_hostname": "hypervisor_hostname",
    },
    resource_cols={
        ResourceTypes.NODE: pl.lit(1),
        ResourceTypes.VCPUS: pl.col("vcpus"),
        ResourceTypes.MEMORY_MB: pl.col("memory_mb"),
        ResourceTypes.DISK_GB: pl.col("local_gb"),
    },
)

## Conditions
is_host_reservation = pl.col("reservation_type").eq("physical:host")
is_baremetal = pl.col("hypervisor_type").eq("ironic")
use_host_resources = is_host_reservation | is_baremetal


def pick_resource(host_col: str, other_col: str) -> pl.Expr:
    """Use host resources for baremetal/host-reservations, else use other."""
    host_expr = pl.col(host_col)
    other_expr = pl.col(other_col)
    return pl.when(use_host_resources).then(host_expr).otherwise(other_expr)


blazarAllocCommitted = Adapter(
    entity_col="id",  # allocation ID
    metric="committed",
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
    resource_cols={
        ResourceTypes.NODE: pl.lit(1),  # TODO flavor fraction
        ResourceTypes.VCPUS: pl.col("effective_vcpus"),
        ResourceTypes.MEMORY_MB: pl.col("effective_memory_mb"),
        ResourceTypes.DISK_GB: pl.col("effective_disk_gb"),
    },
)
_occupied_context = {
    "uuid": "instance_id",
    "blazar_reservation_id": "blazar_reservation_id",
    "node": "hypervisor_hostname",
}
_occupied_resources = {
    ResourceTypes.NODE: pl.lit(1),
    ResourceTypes.VCPUS: pl.col("vcpus"),
    ResourceTypes.VCPUS_OVERCOMMIT: pl.col("vcpus"),
    ResourceTypes.MEMORY_MB: pl.col("memory_mb"),
    ResourceTypes.DISK_GB: pl.col("root_gb"),
}

novaInstanceOccupiedReservation = Adapter(
    entity_col="uuid",
    metric="occupied_reservation",
    source=lambda t: nova_instances_source(t).filter(
        pl.col("booking_type") == "reservation"
    ),
    context_cols=_occupied_context,
    resource_cols=_occupied_resources,
)
novaInstanceOccupiedOndemand = Adapter(
    entity_col="uuid",
    metric="occupied_ondemand",
    source=lambda t: nova_instances_source(t).filter(
        pl.col("booking_type") == "ondemand"
    ),
    context_cols=_occupied_context,
    resource_cols=_occupied_resources,
)

REGISTRY = AdapterRegistry(
    [
        novaHostTotal,
        blazarHostReservable,
        blazarAllocCommitted,
        novaInstanceOccupiedReservation,
        novaInstanceOccupiedOndemand,
    ]
)


def load_intervals(
    base_path: str,
    site_name: str,
    time_range: tuple[datetime, datetime] | None = None,
) -> pl.LazyFrame:
    """Load raw intervals from parquet, optionally filtered to time range.

    Args:
        base_path: Path to parquet files
        site_name: Site directory name
        time_range: Optional (start, end) to filter intervals that overlap this window
    """
    tables = load_raw_tables(base_path, site_name)
    intervals = REGISTRY.to_intervals(tables)

    if time_range is not None:
        range_start, range_end = time_range
        # Inclusive: interval touches or overlaps window
        overlaps = (pl.col("start") <= range_end) & (
            pl.col("end").is_null() | (pl.col("end") >= range_start)
        )
        intervals = intervals.filter(overlaps)

    return intervals
