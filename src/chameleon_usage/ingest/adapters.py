"""Adapters convert raw tables to IntervalSchema."""

from dataclasses import dataclass, field
from typing import Callable

import polars as pl

from chameleon_usage.constants import SchemaCols as S
from chameleon_usage.schemas import IntervalModel
from chameleon_usage.sources import Tables

RawTables = dict[str, pl.LazyFrame]


@dataclass
class Adapter:
    entity_col: str
    metric: str
    source: Callable[[RawTables], pl.LazyFrame]
    context_cols: dict[str, str] = field(default_factory=dict)
    resource_cols: dict[str, pl.Expr] = field(default_factory=dict)
    start_col: str = "created_at"
    end_col: str = "deleted_at"


class AdapterRegistry:
    """Orchestrates adapter → interval conversion."""

    def __init__(self, adapters: list[Adapter]):
        self.adapters = adapters

    def _convert(self, df: pl.LazyFrame, adapter: Adapter) -> pl.LazyFrame:
        return df.select(
            pl.col(adapter.entity_col).alias("entity_id"),
            pl.col(adapter.start_col).alias("start"),
            pl.col(adapter.end_col).alias("end"),
            pl.lit(adapter.metric).alias(S.METRIC),
            *[pl.col(src).alias(dst) for src, dst in adapter.context_cols.items()],
            *[
                expr.cast(pl.Float64).alias(resource_name)
                for resource_name, expr in adapter.resource_cols.items()
            ],
        )

    def _inflate_resources(
        self,
        df: pl.LazyFrame,
        resource_cols: dict[str, pl.Expr],
    ) -> pl.LazyFrame:
        """Explode each interval into N rows, one per resource type."""
        all_cols = df.collect_schema().names()
        resource_col_names = list(resource_cols.keys())
        index_cols = [c for c in all_cols if c not in resource_col_names]
        return df.unpivot(
            index=index_cols,
            on=resource_col_names,
            variable_name=S.RESOURCE,
            value_name=S.VALUE,
        )

    def to_intervals(self, tables: RawTables) -> pl.LazyFrame:
        intervals = []
        for adapter in self.adapters:
            normalized = self._convert(adapter.source(tables), adapter)
            # HACK: handle case where no resource columns are specified, "unpivot" will explode.
            if adapter.resource_cols:
                normalized = self._inflate_resources(normalized, adapter.resource_cols)

            # Validate core columns present - fails early, identifies which adapter broke
            IntervalModel.validate(normalized)
            intervals.append(normalized)

        return pl.concat(intervals, how="diagonal").lazy()


def _blazar_hosts(tables: RawTables) -> pl.LazyFrame:
    """Host info keyed by compute_host_id."""
    return tables[Tables.BLAZAR_HOSTS].select(
        pl.col("id").alias("compute_host_id"),
        "hypervisor_hostname",
        "hypervisor_type",
        pl.col("vcpus").alias("host_vcpus"),
        pl.col("memory_mb").alias("host_memory_mb"),
        pl.col("local_gb").alias("host_disk_gb"),
    )


def _blazar_reservations(tables: RawTables) -> pl.LazyFrame:
    """Reservation info keyed by reservation_id."""
    return tables[Tables.BLAZAR_RES].select(
        pl.col("id").alias("reservation_id"),
        "lease_id",
        pl.col("resource_type").alias("reservation_type"),
    )


def _blazar_flavor_resources(tables: RawTables) -> pl.LazyFrame:
    """Flavor resources keyed by reservation_id (for flavor:instance only)."""
    return tables[Tables.BLAZAR_INSTANCE_RES].select(
        "reservation_id",
        pl.col("vcpus").alias("flavor_vcpus"),
        pl.col("memory_mb").alias("flavor_memory_mb"),
        pl.col("disk_gb").alias("flavor_disk_gb"),
    )


def _blazar_lease_dates(tables: RawTables) -> pl.LazyFrame:
    """Lease dates keyed by id."""
    return tables[Tables.BLAZAR_LEASES].select(
        pl.col("id").alias("lease_id"),
        "project_id",
        "start_date",
        "end_date",
        pl.col("created_at").alias("lease_created_at"),
        pl.col("deleted_at").alias("lease_deleted_at"),
    )


def _effective_resources() -> list[pl.Expr]:
    """Pick flavor resources for flavor:instance, else host resources."""
    is_flavor = pl.col("reservation_type") == "flavor:instance"
    return [
        pl.when(is_flavor)
        .then("flavor_vcpus")
        .otherwise("host_vcpus")
        .alias("effective_vcpus"),
        pl.when(is_flavor)
        .then("flavor_memory_mb")
        .otherwise("host_memory_mb")
        .alias("effective_memory_mb"),
        pl.when(is_flavor)
        .then("flavor_disk_gb")
        .otherwise("host_disk_gb")
        .alias("effective_disk_gb"),
    ]


def blazar_allocations_source(tables: RawTables) -> pl.LazyFrame:
    hosts = _blazar_hosts(tables)
    reservations = _blazar_reservations(tables)
    flavor_resources = _blazar_flavor_resources(tables)
    lease_dates = _blazar_lease_dates(tables)

    return (
        tables[Tables.BLAZAR_ALLOC]
        .join(hosts, on="compute_host_id", how="left")
        .join(reservations, on="reservation_id", how="left")
        .join(flavor_resources, on="reservation_id", how="left")
        .join(lease_dates, on="lease_id", how="left")
        .with_columns(
            pl.max_horizontal("start_date", "lease_created_at").alias(
                "effective_start"
            ),
            pl.min_horizontal("end_date", "lease_deleted_at").alias("effective_end"),
            *_effective_resources(),
        )
        .filter(pl.col("effective_start") <= pl.col("effective_end"))
    )


def _blazar_devices(tables: RawTables) -> pl.LazyFrame:
    """Device info keyed by device_id."""
    return tables[Tables.BLAZAR_DEVICES].select(
        pl.col("id").alias("device_id"),
        "name",
        "device_type",
        "device_driver",
    )


def blazar_device_allocations_source(tables: RawTables) -> pl.LazyFrame:
    """Load device allocations for chi@edge."""
    devices = _blazar_devices(tables)
    reservations = _blazar_reservations(tables)
    lease_dates = _blazar_lease_dates(tables)

    return (
        tables[Tables.BLAZAR_DEVICE_ALLOCATIONS]
        .join(devices, on="device_id", how="left")
        .join(reservations, on="reservation_id", how="left")
        .join(lease_dates, on="lease_id", how="left")
        .with_columns(
            pl.max_horizontal("start_date", "lease_created_at").alias(
                "effective_start"
            ),
            pl.min_horizontal("end_date", "lease_deleted_at").alias("effective_end"),
        )
        .filter(pl.col("effective_start") <= pl.col("effective_end"))
    )


END_EVENTS = [
    "compute_terminate_instance",
    "compute_shelve_offload_instance",
    "compute_shelve_instance",
]
RESUME_EVENTS = ["compute_unshelve_instance"]

# Conditions for filtering events
is_end_event = pl.col("event").is_in(END_EVENTS)
is_resume_event = pl.col("event").is_in(RESUME_EVENTS)
is_after_last_resume = pl.col("last_resume").is_null() | (
    pl.col("start_time") > pl.col("last_resume")
)


def _instance_events(tables: RawTables) -> pl.LazyFrame:
    """Join actions → events, filter to successful."""
    return (
        tables[Tables.NOVA_ACTIONS]
        .join(
            tables[Tables.NOVA_ACTION_EVENTS],
            left_on="id",
            right_on="action_id",
        )
        .filter(pl.col("result").eq("Success"))
        .select("instance_uuid", "host", "event", "start_time")
    )


def _last_host(tables: RawTables) -> pl.LazyFrame:
    """Most recent compute host per instance from events.

    Only compute_* events have the actual hypervisor hostname in the host field.
    Other events (conductor_*, api_*, etc.) have the controller hostname which
    doesn't match any compute_nodes.hypervisor_hostname.
    """
    return (
        _instance_events(tables)
        .filter(pl.col("event").str.starts_with("compute_"))
        .group_by("instance_uuid")
        .agg(
            pl.col("host")
            .sort_by("start_time", descending=True)
            .first()
            .alias("last_host")
        )
    )


def _terminated_at(tables: RawTables) -> pl.LazyFrame:
    """First end event after last resume (or first end if no resume)."""
    events = _instance_events(tables).with_columns(
        pl.col("start_time")
        .filter(is_resume_event)
        .max()
        .over("instance_uuid")
        .alias("last_resume")
    )
    return events.group_by("instance_uuid").agg(
        pl.col("start_time")
        .filter(is_end_event & is_after_last_resume)
        .min()
        .alias(
            "event_terminated_at"
        )  # renamed to avoid collision with instances.terminated_at
    )


def _nova_host_resources(tables: RawTables) -> pl.LazyFrame:
    return (
        tables[Tables.NOVA_HOSTS]
        .select(
            pl.col("hypervisor_hostname").alias("node"),
            pl.col("created_at").alias("host_created_at"),
            "hypervisor_type",
            pl.col("vcpus").alias("host_vcpus"),
            pl.col("memory_mb").alias("host_memory_mb"),
            pl.col("local_gb").alias("host_disk_gb"),
        )
        .sort(["node", "host_created_at"])
    )


def _earliest_host_resources(tables: RawTables) -> pl.LazyFrame:
    """Earliest host record per node - fallback for instances before first host record."""
    return (
        tables[Tables.NOVA_HOSTS]
        .sort("created_at")
        .group_by("hypervisor_hostname")
        .first()
        .select(
            pl.col("hypervisor_hostname").alias("node"),
            pl.col("hypervisor_type").alias("_fb_hypervisor_type"),
            pl.col("vcpus").alias("_fb_host_vcpus"),
            pl.col("memory_mb").alias("_fb_host_memory_mb"),
            pl.col("local_gb").alias("_fb_host_disk_gb"),
        )
    )


def nova_instances_source(tables: RawTables) -> pl.LazyFrame:
    """Load instances with blazar reservation_id and recovered host from events.

    Uses launched_at as start (instances that never launched are filtered out).
    End time is min(terminated_at, deleted_at, event_terminated_at) to handle
    both KVM (accurate table values) and baremetal (needs event-derived).
    """
    instances = tables[Tables.NOVA_INSTANCES]

    # Extract reservation_id: scheduler_hints
    res_hint = (
        pl.col("spec")
        .str.json_path_match("$['nova_object.data'].scheduler_hints.reservation[0]")
        .alias("res_hint")
    )
    # Extract reservation_id from flavor name (reservation:<uuid>)
    res_flavor = (
        pl.col("spec")
        .str.json_path_match("$['nova_object.data'].flavor['nova_object.data'].name")
        .str.extract(r"^reservation:(.+)$", 1)
        .alias("res_flavor")
    )
    request_specs = tables[Tables.NOVA_REQUEST_SPECS].select(
        "instance_uuid", res_hint, res_flavor
    )

    # Recover last known host and termination time from events
    last_host = _last_host(tables)
    event_terminated = _terminated_at(tables)
    host_resources = _nova_host_resources(tables)
    earliest_hosts = _earliest_host_resources(tables)

    return (
        instances.filter(pl.col("launched_at").is_not_null())  # skip never-launched
        .join(request_specs, left_on="uuid", right_on="instance_uuid", how="left")
        .join(last_host, left_on="uuid", right_on="instance_uuid", how="left")
        .join(event_terminated, left_on="uuid", right_on="instance_uuid", how="left")
        .with_columns(
            pl.coalesce("res_hint", "res_flavor").alias("blazar_reservation_id"),
            pl.when(pl.col("res_hint").is_null() & pl.col("res_flavor").is_null())
            .then(pl.lit("ondemand"))
            .otherwise(pl.lit("reservation"))
            .alias("booking_type"),
            pl.coalesce("node", "last_host").alias("node"),
            # some instances occupy resources during BUILD, may have long time between created_at and launched_at
            # use created_at directly, commenting out below override.
            # pl.col("launched_at").alias("created_at"),
            # End = min of table values and event-derived (handles both KVM and baremetal)
            pl.min_horizontal(
                "terminated_at", "deleted_at", "event_terminated_at"
            ).alias("deleted_at"),
        )
        .sort(["node", "created_at"])
        .join_asof(
            host_resources,
            left_on="created_at",
            right_on="host_created_at",
            by="node",
            strategy="backward",
        )
        # Fallback: use earliest host record when join_asof fails (instance before first host record)
        .join(earliest_hosts, on="node", how="left")
        .with_columns(
            pl.coalesce("hypervisor_type", "_fb_hypervisor_type").alias(
                "hypervisor_type"
            ),
            pl.coalesce("host_vcpus", "_fb_host_vcpus").alias("host_vcpus"),
            pl.coalesce("host_memory_mb", "_fb_host_memory_mb").alias("host_memory_mb"),
            pl.coalesce("host_disk_gb", "_fb_host_disk_gb").alias("host_disk_gb"),
        )
        .drop(
            "res_hint",
            "res_flavor",
            "last_host",
            "terminated_at",
            "event_terminated_at",
            "launched_at",
            "host_created_at",
            "_fb_hypervisor_type",
            "_fb_host_vcpus",
            "_fb_host_memory_mb",
            "_fb_host_disk_gb",
        )
    )


# Terminal container statuses (container is no longer running)
ZUN_TERMINAL_STATUSES = ["Deleted", "Error", "Stopped"]


def zun_containers_source(tables: RawTables) -> pl.LazyFrame:
    """Load Zun containers for chi@edge.

    Uses started_at as start (containers that never started are filtered out).
    End time is updated_at when status is terminal, else null (ongoing).
    """
    containers = tables[Tables.ZUN_CONTAINERS]

    is_terminal = pl.col("status").is_in(ZUN_TERMINAL_STATUSES)

    return containers.filter(pl.col("started_at").is_not_null()).with_columns(
        pl.col("started_at").alias("created_at"),
        pl.when(is_terminal).then(pl.col("updated_at")).alias("deleted_at"),
    )
