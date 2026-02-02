"""Pandera schemas for raw input tables.

Names necessary columns, minimal type coercion only.
"""

import pandera.polars as pa
import polars as pl


class BlazarHostRaw(pa.DataFrameModel):
    id: str = pa.Field(unique=True)
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    hypervisor_hostname: str = pa.Field()
    hypervisor_type: str = pa.Field()
    vcpus: int = pa.Field(coerce=True)  # copied from nova computenode on create
    memory_mb: int = pa.Field(coerce=True)  # copied from nova computenode on create
    local_gb: int = pa.Field(coerce=True)  # copied from nova computenode on create


class BlazarLeaseRaw(pa.DataFrameModel):
    id: str = pa.Field(unique=True)
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    start_date: pl.Datetime = pa.Field(coerce=True)
    end_date: pl.Datetime = pa.Field(nullable=True, coerce=True)


class BlazarReservationRaw(pa.DataFrameModel):
    id: str = pa.Field(unique=True)
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    lease_id: str = pa.Field(unique=True)


class BlazarAllocationRaw(pa.DataFrameModel):
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    id: str = pa.Field(unique=True)
    compute_host_id: str = pa.Field(unique=True)
    reservation_id: str = pa.Field(unique=True)


class NovaHostRaw(pa.DataFrameModel):
    id: str = pa.Field(unique=True, coerce=True)
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    hypervisor_hostname: str = pa.Field(coerce=True)
    hypervisor_type: str = pa.Field(coerce=True)
    vcpus: int = pa.Field(coerce=True)  # int32 -> int64
    memory_mb: int = pa.Field(coerce=True)  # int32 -> int64
    local_gb: int = pa.Field(coerce=True)  # int32 -> int64
    cpu_allocation_ratio: float = pa.Field(coerce=True)  # float32 -> float64
    ram_allocation_ratio: float = pa.Field(coerce=True)  # float32 -> float64
    disk_allocation_ratio: float = pa.Field(coerce=True)  # float32 -> float64


class NovaServiceRaw(pa.DataFrameModel):
    id: int = pa.Field(coerce=True)
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    host: str = pa.Field()
    binary: str = pa.Field()


class NovaInstanceRaw(pa.DataFrameModel):
    id: str = pa.Field(unique=True, coerce=True)
    uuid: str = pa.Field(unique=True)
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    host: str = pa.Field()
    node: str = pa.Field()
    vcpus: int = pa.Field(coerce=True)
    memory_mb: int = pa.Field(coerce=True)
    root_gb: int = pa.Field(coerce=True)


class NovaRequestSpecRaw(pa.DataFrameModel):
    instance_uuid: str = pa.Field()
    spec: str = pa.Field()


class NodeUsageReportCache(pa.DataFrameModel):
    date: pl.Datetime = pa.Field(coerce=True)
    node_type: str = pa.Field()
    maint_hours: float = pa.Field()
    reserved_hours: float = pa.Field()
    used_hours: float = pa.Field()
    idle_hours: float = pa.Field()
    total_hours: float = pa.Field(coerce=True)


class NodeCountCache(pa.DataFrameModel):
    date: pl.Date = pa.Field(coerce=True)
    node_type: str = pa.Field()
    cnt: int = pa.Field(coerce=True)
