"""
Constants defined in one place for reuse.
"""


class QuantityTypes:
    TOTAL = "total"
    RESERVABLE = "reservable"
    COMMITTED = "committed"
    OCCUPIED = "occupied"
    ACTIVE = "active"

    # Derived
    AVAILABLE = "available"
    IDLE = "idle"


class States:
    ACTIVE = "active"
    DELETED = "deleted"


class Cols:
    """
    Use these instead of magic strings.
    Particularly helpful anywhere pl.alias() is called.
    """

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
    QUANTITY_TYPE = "quantity_type"
    COUNT = "count"
