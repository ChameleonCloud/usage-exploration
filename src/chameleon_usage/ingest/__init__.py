"""Ingest module: load raw data and convert to intervals."""

from datetime import datetime

import polars as pl

from chameleon_usage.constants import Metrics as M
from chameleon_usage.constants import ResourceTypes
from chameleon_usage.ingest.adapters import (
    Adapter,
    AdapterRegistry,
    blazar_allocations_source,
    blazar_device_allocations_source,
    nova_instances_source,
)
from chameleon_usage.ingest.audit import audit_to_intervals, extract_json_fields
from chameleon_usage.ingest.loader import load_raw_tables
from chameleon_usage.sources import Tables

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
# TODO: fix magic strings.
is_host_reservation = pl.col("reservation_type").eq("physical:host")
is_baremetal = pl.col("hypervisor_type").eq("ironic")
is_kvm = pl.col("hypervisor_type").eq("QEMU")
use_host_resources = is_host_reservation | is_baremetal


def pick_resource(host_col: str, other_col: str) -> pl.Expr:
    """Use host resources for baremetal/host-reservations, else use other."""
    host_expr = pl.col(host_col)
    other_expr = pl.col(other_col)
    return pl.when(use_host_resources).then(host_expr).otherwise(other_expr)


def pick_fraction(
    scale_when: pl.Expr, numerator_col: str, denominator_col: str
) -> pl.Expr:
    return (
        pl.when(scale_when)
        .then(pl.col(numerator_col) / pl.col(denominator_col))
        .otherwise(pl.lit(1))
    )


blazarAllocCommitted = Adapter(
    entity_col="id",  # allocation ID
    metric="committed",
    source=blazar_allocations_source,
    context_cols={
        "id": "blazar_allocation_id",
        "lease_id": "blazar_lease_id",
        "reservation_id": "blazar_reservation_id",
        "reservation_type": "reservation_type",
        "compute_host_id": "blazar_host_id",
        "hypervisor_hostname": "hypervisor_hostname",
    },
    start_col="effective_start",
    end_col="effective_end",
    resource_cols={
        ResourceTypes.NODE: pick_fraction(
            ~is_host_reservation, "effective_vcpus", "host_vcpus"
        ),
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
    ResourceTypes.NODE: pick_fraction(is_kvm, "vcpus", "host_vcpus"),
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

# Chi@Edge device adapters
blazarDeviceReservable = Adapter(
    entity_col="id",
    metric="reservable",
    source=lambda t: t[Tables.BLAZAR_DEVICES].filter(pl.col("reservable") == 1),
    context_cols={
        "id": "blazar_device_id",
        "name": "device_name",
    },
    resource_cols={
        ResourceTypes.NODE: pl.lit(1),
        # ResourceTypes.DEVICE: pl.lit(1),
    },
)

blazarDeviceCommitted = Adapter(
    entity_col="id",
    metric="committed",
    source=blazar_device_allocations_source,
    context_cols={
        "id": "blazar_allocation_id",
        "lease_id": "blazar_lease_id",
        "reservation_id": "blazar_reservation_id",
        "reservation_type": "reservation_type",
        "device_id": "blazar_device_id",
        "name": "device_name",
        "project_id": "project_id",
    },
    start_col="effective_start",
    end_col="effective_end",
    resource_cols={
        ResourceTypes.NODE: pl.lit(1),
        # ResourceTypes.DEVICE: pl.lit(1),
    },
)




##################
# Audit adapters
##################

# Shared: convert audit rows → intervals and extract JSON fields.
# Each audit table gets a source function here; adapters filter on predicates.

_BLAZAR_HOST_AUDIT_FIELDS = [
    "hypervisor_hostname",
    "vcpus",
    "memory_mb",
    "local_gb",
    "status",
    "reservable",
    "disabled",
]

_blazar_host_usable = (
    pl.col("reservable").cast(pl.Int64).eq(1)
    & pl.col("disabled").cast(pl.Int64).eq(0)
)


def _audit_blazar_host_source(tables):
    """Audit rows → intervals with extracted JSON fields.

    Returns an empty frame if the audit table was not loaded.
    """
    if Tables.AUDIT_BLAZAR_HOSTS not in tables:
        return pl.LazyFrame(
            schema={"id": pl.Utf8, "start": pl.Datetime, "end": pl.Datetime}
        )
    intervals = audit_to_intervals(tables[Tables.AUDIT_BLAZAR_HOSTS])
    return extract_json_fields(intervals, _BLAZAR_HOST_AUDIT_FIELDS)


_audit_blazar_context = {"hypervisor_hostname": "hypervisor_hostname"}
_audit_blazar_resources = {
    ResourceTypes.NODE: pl.lit(1),
    ResourceTypes.VCPUS: pl.col("vcpus").cast(pl.Float64),
    ResourceTypes.MEMORY_MB: pl.col("memory_mb").cast(pl.Float64),
    ResourceTypes.DISK_GB: pl.col("local_gb").cast(pl.Float64),
}

auditBlazarHostUsable = Adapter(
    entity_col="id",
    metric=M.RESERVABLE_USABLE,
    source=lambda t: _audit_blazar_host_source(t).filter(_blazar_host_usable),
    context_cols=_audit_blazar_context,
    start_col="start",
    end_col="end",
    resource_cols=_audit_blazar_resources,
)

auditBlazarHostUnusable = Adapter(
    entity_col="id",
    metric=M.RESERVABLE_UNUSABLE,
    source=lambda t: _audit_blazar_host_source(t).filter(~_blazar_host_usable),
    context_cols=_audit_blazar_context,
    start_col="start",
    end_col="end",
    resource_cols=_audit_blazar_resources,
)

REGISTRY = AdapterRegistry(
    [
        novaHostTotal,
        blazarHostReservable,
        blazarAllocCommitted,
        novaInstanceOccupiedReservation,
        novaInstanceOccupiedOndemand,
        blazarDeviceReservable,
        blazarDeviceCommitted,
        auditBlazarHostUsable,
        auditBlazarHostUnusable,
    ]
)


def load_intervals(
    parquet_path: str,
    time_range: tuple[datetime, datetime] | None = None,
) -> pl.LazyFrame:
    """Load raw intervals from parquet, optionally filtered to time range.

    Args:
        base_path: Path to parquet files
        site_name: Site directory name
        time_range: Optional (start, end) to filter intervals that overlap this window
    """
    tables = load_raw_tables(parquet_path)
    intervals = REGISTRY.to_intervals(tables).with_columns(
        pl.lit("current").alias("collector_type")
    )

    if time_range is not None:
        range_start, range_end = time_range
        # Inclusive: interval touches or overlaps window
        overlaps = (pl.col("start") <= range_end) & (
            pl.col("end").is_null() | (pl.col("end") >= range_start)
        )
        intervals = intervals.filter(overlaps)

    return intervals
