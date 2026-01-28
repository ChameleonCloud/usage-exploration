"""
Constants defined in one place for reuse.
"""


class Sources:
    NOVA = "nova"


class States:
    ACTIVE = "active"


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
