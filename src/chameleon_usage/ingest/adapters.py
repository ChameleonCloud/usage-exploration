"""Adapters convert raw tables to IntervalSchema."""

from dataclasses import dataclass, field
from typing import Callable

import polars as pl

from chameleon_usage.constants import Tables

RawTables = dict[str, pl.LazyFrame]


@dataclass
class Adapter:
    entity_col: str
    quantity_type: str
    source: Callable[[RawTables], pl.LazyFrame]
    context_cols: dict[str, str] = field(default_factory=dict)
    resource_cols: list[str] = field(default_factory=list)
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
            pl.lit(adapter.quantity_type).alias("quantity_type"),
            *[pl.col(src).alias(dst) for src, dst in adapter.context_cols.items()],
            *[pl.col(res_col).cast(pl.Float64) for res_col in adapter.resource_cols],
        )

    def _inflate_resources(
        self,
        df: pl.LazyFrame,
        resource_cols: list[str],
    ) -> pl.LazyFrame:
        """Explode each interval into N rows, one per resource type."""
        all_cols = df.collect_schema().names()
        index_cols = [c for c in all_cols if c not in resource_cols]
        return df.unpivot(
            index=index_cols,
            on=resource_cols,
            variable_name="resource_type",
            value_name="resource_value",
        )

    def to_intervals(self, tables: RawTables) -> pl.LazyFrame:
        intervals = []
        # run each registered adapter, normalize, and then generate long-format
        # row per resource type, resource value
        for adapter in self.adapters:
            normalized = self._convert(adapter.source(tables), adapter)
            row_per_resource = self._inflate_resources(
                normalized, adapter.resource_cols
            )
            intervals.append(row_per_resource)

        return pl.concat(intervals, how="diagonal")


def blazar_allocations_source(tables: RawTables) -> pl.LazyFrame:
    return (
        tables[Tables.BLAZAR_ALLOC]
        .join(
            tables[Tables.BLAZAR_HOSTS].select(["id", "hypervisor_hostname"]),
            left_on="compute_host_id",
            right_on="id",
            how="left",
            suffix="_host",
        )
        .join(
            tables[Tables.BLAZAR_RES].select(["id", "lease_id"]),
            left_on="reservation_id",
            right_on="id",
            how="left",
            suffix="_res",
        )
        .join(
            tables[Tables.BLAZAR_LEASES].select(
                ["id", "start_date", "end_date", "created_at", "deleted_at"]
            ),
            left_on="lease_id",
            right_on="id",
            how="left",
            suffix="_lease",
        )
        .with_columns(
            pl.max_horizontal("start_date", "created_at_lease").alias(
                "effective_start"
            ),
            pl.min_horizontal("end_date", "deleted_at_lease").alias("effective_end"),
        )
        .filter(pl.col("effective_start") <= pl.col("effective_end"))
        # .filter(pl.col("hypervisor_hostname").is_not_null())
    )


END_EVENTS = ["compute_terminate_instance", "compute_shelve_offload_instance"]
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
    """Most recent host per instance from events."""
    return (
        _instance_events(tables)
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
        .alias("terminated_at")
    )


def nova_instances_source(tables: RawTables) -> pl.LazyFrame:
    """Load instances with blazar reservation_id and recovered host from events."""
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
    terminated_at = _terminated_at(tables)

    return (
        instances.join(
            request_specs, left_on="uuid", right_on="instance_uuid", how="left"
        )
        .join(last_host, left_on="uuid", right_on="instance_uuid", how="left")
        .join(terminated_at, left_on="uuid", right_on="instance_uuid", how="left")
        .with_columns(
            pl.coalesce("res_hint", "res_flavor").alias("blazar_reservation_id"),
            pl.when(pl.col("res_hint").is_null() & pl.col("res_flavor").is_null())
            .then(pl.lit("ondemand"))
            .otherwise(pl.lit("reservation"))
            .alias("booking_type"),
            pl.coalesce("node", "last_host").alias("node"),
            pl.min_horizontal("deleted_at", "terminated_at").alias("deleted_at"),
        )
        .drop("res_hint", "res_flavor", "last_host", "terminated_at")
    )
