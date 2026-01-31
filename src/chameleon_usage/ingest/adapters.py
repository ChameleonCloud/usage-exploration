"""Adapters convert raw tables to IntervalSchema."""

from dataclasses import dataclass, field
from typing import Callable

import polars as pl

from chameleon_usage.constants import Tables
from chameleon_usage.schemas import IntervalSchema

RawTables = dict[str, pl.LazyFrame]


@dataclass
class Adapter:
    entity_col: str
    quantity_type: str
    source: Callable[[RawTables], pl.LazyFrame]
    context_cols: dict[str, str] = field(default_factory=dict)
    start_col: str = "created_at"
    end_col: str = "deleted_at"


class AdapterRegistry:
    """Orchestrates adapter → interval conversion."""

    def __init__(self, adapters: list[Adapter]):
        self.adapters = adapters

    def _convert(self, df: pl.LazyFrame, adapter: Adapter) -> pl.LazyFrame:
        return df.select(
            pl.col(adapter.entity_col).alias("entity_id"),
            pl.col(adapter.start_col).alias("start"),
            pl.col(adapter.end_col).alias("end"),
            pl.lit(adapter.quantity_type).alias("quantity_type"),
            *[pl.col(src).alias(dst) for src, dst in adapter.context_cols.items()],
        )

    def to_intervals(self, tables: RawTables) -> pl.LazyFrame:
        intervals = [self._convert(a.source(tables), a) for a in self.adapters]
        return IntervalSchema.validate(pl.concat(intervals, how="diagonal"))


def blazar_allocations_source(tables: RawTables) -> pl.LazyFrame:
    return (
        tables[Tables.BLAZAR_ALLOC]
        .join(
            tables[Tables.BLAZAR_HOSTS].select(["id", "hypervisor_hostname"]),
            left_on="compute_host_id",
            right_on="id",
            how="left",
            suffix="_host",
        )
        .join(
            tables[Tables.BLAZAR_RES].select(["id", "lease_id"]),
            left_on="reservation_id",
            right_on="id",
            how="left",
            suffix="_res",
        )
        .join(
            tables[Tables.BLAZAR_LEASES].select(
                ["id", "start_date", "end_date", "created_at", "deleted_at"]
            ),
            left_on="lease_id",
            right_on="id",
            how="left",
            suffix="_lease",
        )
        .with_columns(
            pl.max_horizontal("start_date", "created_at_lease").alias(
                "effective_start"
            ),
            pl.min_horizontal("end_date", "deleted_at_lease").alias("effective_end"),
        )
        .filter(pl.col("hypervisor_hostname").is_not_null())
        .filter(pl.col("effective_start") <= pl.col("effective_end"))
    )
