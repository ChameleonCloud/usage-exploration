"""
Constants defined in one place for reuse.
"""


class ResourceTypes:
    NODE = "nodes"
    VCPUS_OVERCOMMIT = "vcpus_overcommit"
    VCPUS = "vcpus"
    MEMORY_MB = "memory_mb"
    DISK_GB = "disk_gb"
    GPUS = "gpus"
    DEVICE = "devices"


class Metrics:
    TOTAL = "total"
    RESERVABLE = "reservable"
    COMMITTED = "committed"
    OCCUPIED_RESERVATION = "occupied_reservation"
    OCCUPIED_ONDEMAND = "occupied_ondemand"

    # Derived
    ONDEMAND_CAPACITY = "ondemand_capacity"
    AVAILABLE_RESERVABLE = "available_reservable"
    AVAILABLE_ONDEMAND = "available_ondemand"
    IDLE = "idle"


class SchemaCols:
    """Canonical pipeline schema column names."""

    METRIC = "metric"
    RESOURCE = "resource"
    VALUE = "value"
    TIMESTAMP = "timestamp"


class CollectorTypes:
    NEWCOLLECTOR = "current"
    EXISTING = "legacy"


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
