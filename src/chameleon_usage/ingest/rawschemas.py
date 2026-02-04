"""Pandera schemas for raw input tables.

Names necessary columns, minimal type coercion only.
"""

import pandera.polars as pa
import polars as pl
from pandera.api.polars.model_config import BaseConfig


class BaseRaw(pa.DataFrameModel):
    class Config(BaseConfig):
        strict = "filter"


class BlazarHostRaw(BaseRaw):
    id: str = pa.Field(unique=True)
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    hypervisor_hostname: str = pa.Field()
    hypervisor_type: str = pa.Field()
    vcpus: int = pa.Field(coerce=True)  # copied from nova computenode on create
    memory_mb: int = pa.Field(coerce=True)  # copied from nova computenode on create
    local_gb: int = pa.Field(coerce=True)  # copied from nova computenode on create


class BlazarLeaseRaw(BaseRaw):
    id: str = pa.Field(unique=True)
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    start_date: pl.Datetime = pa.Field(coerce=True)
    end_date: pl.Datetime = pa.Field(nullable=True, coerce=True)


class BlazarReservationRaw(BaseRaw):
    id: str = pa.Field(unique=True)
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    lease_id: str = pa.Field(unique=True)
    resource_type: str


class BlazarAllocationRaw(BaseRaw):
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    id: str = pa.Field(unique=True)
    compute_host_id: str = pa.Field(unique=True)
    reservation_id: str = pa.Field(unique=True)


class BlazarInstanceReservationRaw(BaseRaw):
    reservation_id: str = pa.Field()
    vcpus: int = pa.Field(coerce=True)
    memory_mb: int = pa.Field(coerce=True)
    disk_gb: int = pa.Field(coerce=True)


class NovaHostRaw(BaseRaw):
    id: str = pa.Field(unique=True, coerce=True)
    created_at: pl.Datetime = pa.Field(coerce=True)
    updated_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    deleted_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    hypervisor_hostname: str = pa.Field(coerce=True)
    hypervisor_type: str = pa.Field(coerce=True)
    vcpus: int = pa.Field(coerce=True)  # int32 -> int64
    memory_mb: int = pa.Field(coerce=True)  # int32 -> int64
    local_gb: int = pa.Field(coerce=True)  # int32 -> int64
    cpu_allocation_ratio: float = pa.Field(coerce=True)  # float32 -> float64
    ram_allocation_ratio: float = pa.Field(coerce=True)  # float32 -> float64
    disk_allocation_ratio: float = pa.Field(coerce=True)  # float32 -> float64


class NovaServiceRaw(BaseRaw):
    id: int = pa.Field(coerce=True)
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    host: str = pa.Field()
    binary: str = pa.Field()


class NovaInstanceRaw(BaseRaw):
    id: str = pa.Field(unique=True, coerce=True)
    uuid: str = pa.Field(unique=True)
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    launched_at: pl.Datetime = pa.Field(coerce=True)
    terminated_at: pl.Datetime = pa.Field(coerce=True)
    host: str = pa.Field()
    node: str = pa.Field()
    vcpus: int = pa.Field(coerce=True)
    memory_mb: int = pa.Field(coerce=True)
    root_gb: int = pa.Field(coerce=True)


class NovaInstanceActionsRaw(BaseRaw):
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(coerce=True, nullable=True)
    id: int = pa.Field(coerce=True)
    action: str
    instance_uuid: str
    start_time: pl.Datetime = pa.Field(coerce=True, nullable=True)
    finish_time: pl.Datetime = pa.Field(coerce=True, nullable=True)
    # TODO: use success/fail and message to analyze launch failures


class NovaInstanceActionsEventsRaw(BaseRaw):
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(coerce=True, nullable=True)
    id: int = pa.Field(coerce=True)
    event: str
    action_id: int = pa.Field(coerce=True)
    start_time: pl.Datetime = pa.Field(coerce=True, nullable=True)
    finish_time: pl.Datetime = pa.Field(coerce=True, nullable=True)
    result: str
    host: str
    # TODO: use success/fail and message to analyze launch failures


class NovaRequestSpecRaw(BaseRaw):
    instance_uuid: str = pa.Field()
    spec: str = pa.Field()


class NodeUsageReportCache(BaseRaw):
    date: pl.Datetime = pa.Field(coerce=True)
    node_type: str = pa.Field()
    maint_hours: float = pa.Field()
    reserved_hours: float = pa.Field()
    used_hours: float = pa.Field()
    idle_hours: float = pa.Field()
    total_hours: float = pa.Field(coerce=True)


class NodeCountCache(BaseRaw):
    date: pl.Date = pa.Field(coerce=True)
    node_type: str = pa.Field()
    cnt: int = pa.Field(coerce=True)


############
# CHI@Edge #
############
class ZunComputeNodeRaw(BaseRaw):
    """Not super useful for chi@edge, just one for whole k3s cluster."""

    created_at: pl.Datetime = pa.Field(coerce=True)
    updated_at: pl.Datetime = pa.Field(coerce=True, nullable=True)
    uuid: str
    hostname: str


class ZunContainerRaw(BaseRaw):
    created_at: pl.Datetime = pa.Field(coerce=True)
    updated_at: pl.Datetime = pa.Field(coerce=True, nullable=True)
    id: int = pa.Field(coerce=True)
    uuid: str
    name: str
    status: str
    container_id: str = pa.Field(nullable=True)
    hostname: str = pa.Field(nullable=True)
    labels: str = pa.Field(nullable=True)
    status_reason: str = pa.Field(nullable=True)
    host: str = pa.Field(nullable=True)
    status_detail: str = pa.Field(nullable=True)
    started_at: pl.Datetime = pa.Field(coerce=True, nullable=True)
    cpu: float = pa.Field(coerce=True)
    memory: float = pa.Field(coerce=True)
    disk: int = pa.Field(coerce=True)


class ZunContainerActionsRaw(BaseRaw):
    created_at: pl.Datetime = pa.Field(coerce=True)
    updated_at: pl.Datetime = pa.Field(coerce=True, nullable=True)
    id: int = pa.Field(coerce=True)
    action: str
    container_uuid: str
    start_time: pl.Datetime = pa.Field(coerce=True, nullable=True)
    finish_time: pl.Datetime = pa.Field(coerce=True, nullable=True)


class ZunContainerActionsEventsRaw(BaseRaw):
    created_at: pl.Datetime = pa.Field(coerce=True)
    updated_at: pl.Datetime = pa.Field(coerce=True, nullable=True)
    id: int = pa.Field(coerce=True)
    event: str
    action_id: int = pa.Field(coerce=True)
    start_time: pl.Datetime = pa.Field(coerce=True, nullable=True)
    finish_time: pl.Datetime = pa.Field(coerce=True, nullable=True)
    result: str = pa.Field(nullable=True)


class BlazarDeviceAllocationRaw(BaseRaw):
    created_at: pl.Datetime = pa.Field(coerce=True)
    updated_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(coerce=True, nullable=True)
    deleted: str
    id: str
    device_id: str
    reservation_id: str


class BlazarDeviceExtraCapabilityRaw(BaseRaw):
    created_at: pl.Datetime = pa.Field(coerce=True)
    updated_at: pl.Datetime = pa.Field(coerce=True)
    id: str
    device_id: str
    # capability_id: str
    capability_value: str
    deleted: str
    deleted_at: pl.Datetime = pa.Field(coerce=True, nullable=True)


class BlazarDeviceReservationRaw(BaseRaw):
    created_at: pl.Datetime = pa.Field(coerce=True)
    updated_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(coerce=True, nullable=True)
    deleted: str
    id: str
    reservation_id: str
    count_range: str
    resource_properties: str
    before_end: str


class BlazarDeviceRaw(BaseRaw):
    created_at: pl.Datetime = pa.Field(coerce=True)
    updated_at: pl.Datetime = pa.Field(coerce=True)
    id: str
    name: str
    device_type: str
    device_driver: str
    reservable: int = pa.Field(coerce=True)
    deleted: str
    deleted_at: pl.Datetime = pa.Field(coerce=True, nullable=True)
