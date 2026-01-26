import pandera.polars as pa
import polars as pl


class BlazarHostRaw(pa.DataFrameModel):
    id: str = pa.Field(unique=True)
    created_at: pl.Datetime = pa.Field(coerce=True)
    deleted_at: pl.Datetime = pa.Field(nullable=True, coerce=True)
    hypervisor_hostname: str = pa.Field()
    hypervisor_type: str = pa.Field()


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


# class CanonicalSpan(pa.DataFrameModel):
#     entity_id: str = pa.Field(unique=True)
#     entity_type: str
#     start: pl.datetime_
#     en

# [
#     "created_at",
#     "updated_at",
#     "id",
#     "vcpus",
#     "cpu_info",
#     "hypervisor_type",
#     "hypervisor_version",
#     "hypervisor_hostname",
#     "service_name",
#     "memory_mb",
#     "local_gb",
#     "status",
#     "trust_id",
#     "reservable",
#     "availability_zone",
#     "deleted",
#     "deleted_at",
#     "disabled",
# ]
# [
#     "created_at",
#     "updated_at",
#     "id",
#     "computehost_id",
#     "capability_value",
#     "property_id",
#     "deleted",
#     "deleted_at",
# ]
