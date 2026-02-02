"""
Constants defined in one place for reuse.
"""


class ResourceTypes:
    NODE = "nodes"
    VCPUS = "vcpus"
    MEMORY_MB = "memory_mb"
    DISK_GB = "disk_gb"
    GPUS = "gpus"


class Tables:
    NOVA_HOSTS = "nova_hosts"
    NOVA_INSTANCES = "nova_instances"
    NOVA_REQUEST_SPECS = "nova_request_specs"
    NOVA_ACTIONS = "nova_actions"
    NOVA_ACTION_EVENTS = "nova_action_events"
    BLAZAR_HOSTS = "blazar_hosts"
    BLAZAR_ALLOC = "blazar_alloc"
    BLAZAR_RES = "blazar_res"
    BLAZAR_LEASES = "blazar_leases"


class QuantityTypes:
    TOTAL = "total"
    RESERVABLE = "reservable"
    COMMITTED = "committed"
    OCCUPIED = "occupied"
    ACTIVE = "active"

    # Derived
    AVAILABLE = "available"
    IDLE = "idle"


class SchemaCols:
    """Canonical pipeline schema column names."""

    METRIC = "metric"
    RESOURCE = "resource"
    VALUE = "value"
    TIMESTAMP = "timestamp"


class Cols:
    """Raw DB column names."""

    ID = "id"
    CREATED_AT = "created_at"
    DELETED_AT = "deleted_at"
    START_DATE = "start_date"
    END_DATE = "end_date"
    HYPERVISOR_HOSTNAME = "hypervisor_hostname"
    ENTITY_ID = "entity_id"
    SOURCE = "source"
    TIMESTAMP = "timestamp"
    VALUE = "value"
    PREV_VALUE = "prev_value"
    DELTA = "delta"
