"""Single source of truth for mapping parquet to tables."""

from dataclasses import dataclass

from pandera.polars import DataFrameModel

from chameleon_usage.ingest import rawschemas as raw


@dataclass(frozen=True)
class SourceSpec:
    db_schema: str
    db_table: str
    model: type[DataFrameModel]


class Tables:
    NOVA_HOSTS = "nova_hosts"
    NOVA_INSTANCES = "nova_instances"
    NOVA_REQUEST_SPECS = "nova_request_specs"
    NOVA_ACTIONS = "nova_actions"
    NOVA_ACTION_EVENTS = "nova_action_events"
    BLAZAR_HOSTS = "blazar_hosts"
    BLAZAR_ALLOC = "blazar_alloc"
    BLAZAR_RES = "blazar_res"
    BLAZAR_INSTANCE_RES = "blazar_instance_res"
    BLAZAR_LEASES = "blazar_leases"


SOURCE_REGISTRY = {
    Tables.NOVA_HOSTS: SourceSpec("nova", "compute_nodes", raw.NovaHostRaw),
    Tables.NOVA_INSTANCES: SourceSpec("nova", "instances", raw.NovaInstanceRaw),
    Tables.NOVA_REQUEST_SPECS: SourceSpec(
        "nova_api", "request_specs", raw.NovaRequestSpecRaw
    ),
    Tables.NOVA_ACTIONS: SourceSpec(
        "nova", "instance_actions", raw.NovaInstanceActionsRaw
    ),
    Tables.NOVA_ACTION_EVENTS: SourceSpec(
        "nova", "instance_actions_events", raw.NovaInstanceActionsEventsRaw
    ),
    Tables.BLAZAR_HOSTS: SourceSpec("blazar", "computehosts", raw.BlazarHostRaw),
    Tables.BLAZAR_ALLOC: SourceSpec(
        "blazar", "computehost_allocations", raw.BlazarAllocationRaw
    ),
    Tables.BLAZAR_RES: SourceSpec("blazar", "reservations", raw.BlazarReservationRaw),
    Tables.BLAZAR_INSTANCE_RES: SourceSpec(
        "blazar", "instance_reservations", raw.BlazarInstanceReservationRaw
    ),
    Tables.BLAZAR_LEASES: SourceSpec("blazar", "leases", raw.BlazarLeaseRaw),
}
