import pandera.polars as pa
import polars as pl
from pandera.typing.polars import DataFrame, Series


class BlazarHostRaw(pa.DataFrameModel):
    id: Series[str] = pa.Field(nullable=False)
    created_at: Series[pl.Datetime] = pa.Field(nullable=False)
    deleted_at: Series[pl.Datetime] = pa.Field(nullable=True)
    hypervisor_hostname: Series[str] = pa.Field(nullable=False)
