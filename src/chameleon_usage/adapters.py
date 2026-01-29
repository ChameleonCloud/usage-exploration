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
        # col_map is { Target : Source }
        selection = [
            pl.col(raw_col).alias(std_col)
            for std_col, raw_col in self.cfg.col_map.items()
        ]
        selection.append(pl.lit(self.cfg.source).alias(C.SOURCE))
        base = self.raw_df.select(selection)
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
        config: SourceConfig,
    ):
        self.alloc = alloc
        self.res = res
        self.lease = lease
        super().__init__(None, config)

    def to_facts(self) -> LazyGeneric[FactSchema]:
        """
        Generates 2 Facts from each row:
        1. Created At -> Value: "active" , timestamp
        2. Deleted At -> Value: "null" , timestamp
        """

        base = (
            self.alloc.join(
                self.res,
                left_on="reservation_id",
                right_on="id",
                how="left",
                suffix="_res",
            )
            .join(
                self.lease,
                left_on="lease_id",
                right_on="id",
                how="left",
                suffix="_lease",
            )
            .select(
                [
                    pl.col("compute_host_id").alias(C.ENTITY_ID),
                    pl.min_horizontal(
                        pl.max_horizontal(
                            pl.col("start_date"),
                            pl.col("created_at_lease"),
                        ),
                        pl.col("deleted_at_lease"),
                    ).alias(C.CREATED_AT),
                    pl.min_horizontal(
                        pl.col("end_date"),
                        pl.col("deleted_at_lease"),
                    ).alias(C.DELETED_AT),
                    pl.lit(Sources.BLAZAR).alias(C.SOURCE),
                ]
            )
        )

        return self._expand_events(base)
