"""Ingest module: load raw data and convert to intervals."""

from datetime import datetime

import polars as pl

from chameleon_usage.constants import Tables
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
