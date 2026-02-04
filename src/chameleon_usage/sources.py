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
    BLAZAR_DEVICE_ALLOCATIONS = "blazar_device_allocations"
    BLAZAR_DEVICE_EXTRA_CAPABILITIES = "blazar_device_extra_capabilities"
    BLAZAR_DEVICE_RESERVATIONS = "blazar_device_reservations"
    BLAZAR_DEVICES = "blazar_devices"
    # Chi@Edge (Zun)
    ZUN_CONTAINERS = "zun_containers"
    ZUN_CONTAINER_ACTIONS = "zun_container_actions"
    ZUN_CONTAINER_ACTION_EVENTS = "zun_container_action_events"


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
    Tables.BLAZAR_DEVICE_ALLOCATIONS: SourceSpec(
        "blazar", "device_allocations", raw.BlazarDeviceAllocationRaw
    ),
    Tables.BLAZAR_DEVICE_EXTRA_CAPABILITIES: SourceSpec(
        "blazar", "device_extra_capabilities", raw.BlazarDeviceExtraCapabilityRaw
    ),
    Tables.BLAZAR_DEVICE_RESERVATIONS: SourceSpec(
        "blazar", "device_reservations", raw.BlazarDeviceReservationRaw
    ),
    Tables.BLAZAR_DEVICES: SourceSpec("blazar", "devices", raw.BlazarDeviceRaw),
    # Chi@Edge (Zun)
    Tables.ZUN_CONTAINERS: SourceSpec("zun", "container", raw.ZunContainerRaw),
    Tables.ZUN_CONTAINER_ACTIONS: SourceSpec(
        "zun", "container_actions", raw.ZunContainerActionsRaw
    ),
    Tables.ZUN_CONTAINER_ACTION_EVENTS: SourceSpec(
        "zun", "container_actions_events", raw.ZunContainerActionsEventsRaw
    ),
}
