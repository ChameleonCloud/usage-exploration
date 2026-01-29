from typing import Protocol, runtime_checkable

import polars as pl
from pandera.typing.polars import LazyFrame as LazyGeneric

from chameleon_usage.config import SourceConfig
from chameleon_usage.constants import Cols as C
from chameleon_usage.constants import Sources, States
from chameleon_usage.models.domain import FactSchema


@runtime_checkable  # Optional: allows isinstance(obj, FactAdapter) at runtime
class FactAdapter(Protocol):
    def to_facts(self) -> LazyGeneric[FactSchema]:
        """
        Must return a LazyFrame adhering to FactSchema.
        """
        raise NotImplementedError


class GenericFactAdapter:
    """
    Adapter for single-table sources.

    Accepts sources with entity_id, start, end columns.
    Returns facts via to_facts protocol.
    """

    def __init__(self, raw_df: pl.LazyFrame, config: SourceConfig):
        self.raw_df = raw_df
        self.cfg = config

    def _expand_events(self, base_df: pl.LazyFrame) -> LazyGeneric[FactSchema]:
        """
        Consumes a standardized LazyFrame [entity_id, source, created_at, deleted_at]
        and explodes it into the Start/End FactSchema.
        """
        quantity_type = self.cfg.quantity_type

        starts = base_df.select(
            pl.col(C.CREATED_AT).alias(C.TIMESTAMP),
            pl.col(C.ENTITY_ID),
            pl.lit(quantity_type).alias(C.QUANTITY_TYPE),
            pl.lit(States.ACTIVE).alias(C.VALUE),
            pl.col(C.SOURCE),
        )

        # Important! Filters out events with null deleted_at
        # they are not an "observation" point
        ends = base_df.filter(pl.col(C.DELETED_AT).is_not_null()).select(
            pl.col(C.DELETED_AT).alias(C.TIMESTAMP),
            pl.col(C.ENTITY_ID),
            pl.lit(quantity_type).alias(C.QUANTITY_TYPE),
            pl.lit(States.DELETED).alias(C.VALUE),
            pl.col(C.SOURCE),
        )

        events = pl.concat([starts, ends])
        return FactSchema.validate(events)

    def to_facts(self) -> LazyGeneric[FactSchema]:
        # Filter if specified in registry
        df = self.raw_df
        if self.cfg.filter_expr is not None:
            df = df.filter(self.cfg.filter_expr)

        # col_map is { Target : Source }
        selection = [
            pl.col(raw_col).alias(std_col)
            for std_col, raw_col in self.cfg.col_map.items()
        ]
        selection.append(pl.lit(self.cfg.source).alias(C.SOURCE))

        base = df.select(selection)

        return self._expand_events(base)


class BlazarAllocationAdapter(GenericFactAdapter):
    """
    Takes input values from nova computenode table and generates facts.
    """

    def __init__(
        self,
        alloc: pl.LazyFrame,
        res: pl.LazyFrame,
        lease: pl.LazyFrame,
        blazarhost: pl.LazyFrame,
        config: SourceConfig,
    ):
        self.alloc = alloc
        self.res = res
        self.lease = lease
        self.blazarhost = blazarhost
        super().__init__(None, config)

    def to_facts(self) -> LazyGeneric[FactSchema]:
        """
        Generates 2 Facts from each row:
        1. Created At -> Value: "active" , timestamp
        2. Deleted At -> Value: "null" , timestamp
        """

        # join 1: get hypervisor_hostname
        alloc_hh = self.alloc.join(
            other=self.blazarhost.select(["id", "hypervisor_hostname"]),
            left_on="compute_host_id",
            right_on="id",
            how="left",
            suffix="_host",
        )

        # Join 2: get lease from reservation
        alloc_res = alloc_hh.join(
            self.res,
            left_on="reservation_id",
            right_on="id",
            how="left",
            suffix="_res",
        )

        # Join 3: Get timestamps from lease
        alloc_lease = alloc_res.join(
            self.lease,
            left_on="lease_id",
            right_on="id",
            how="left",
            suffix="_lease",
        )

        effective_start = pl.max_horizontal(
            pl.col("start_date"),
            pl.col("created_at_lease"),
        )

        effective_end = pl.min_horizontal(
            pl.col("end_date"),
            pl.col("deleted_at_lease"),
        )

        # rename outputs to known columns
        base = alloc_lease.select(
            [
                pl.col("hypervisor_hostname").alias(C.ENTITY_ID),
                pl.col("id").alias("allocation_id"),
                pl.col("reservation_id"),
                pl.col("lease_id"),
                pl.col("compute_host_id").alias("blazar_host_id"),
                effective_start.alias(C.CREATED_AT),
                effective_end.alias(C.DELETED_AT),
                pl.lit(Sources.BLAZAR).alias(C.SOURCE),
            ]
        ).filter(
            # TODO: Exclude all leases deleted before starting
            pl.col("created_at") < pl.col("deleted_at"),
        )
        print(base.collect())
        return self._expand_events(base)
