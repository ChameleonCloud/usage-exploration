from abc import ABC
from typing import Protocol, runtime_checkable

import polars as pl
from pandera.typing.polars import LazyFrame as LazyGeneric

from chameleon_usage.constants import Cols as C
from chameleon_usage.constants import QuantityTypes, Sources, States
from chameleon_usage.models.domain import FactSchema
from chameleon_usage.models.raw import (
    BlazarAllocationRaw,
    BlazarHostRaw,
    BlazarLeaseRaw,
    BlazarReservationRaw,
    NovaHostRaw,
    NovaInstanceRaw,
)


@runtime_checkable  # Optional: allows isinstance(obj, FactAdapter) at runtime
class FactAdapter(Protocol):
    def to_facts(self) -> LazyGeneric[FactSchema]:
        """
        Must return a LazyFrame adhering to FactSchema.
        """
        raise NotImplementedError


def _expand_events(
    base_df: pl.LazyFrame, quantity_type: str
) -> LazyGeneric[FactSchema]:
    """
    Consumes a standardized LazyFrame [entity_id, source, created_at, deleted_at]
    and explodes it into the Start/End FactSchema.
    """
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


class NovaComputeAdapter:
    """
    Generates Facts for "nova hosts"
    """

    def __init__(self, raw_df: LazyGeneric[NovaHostRaw]):
        self.raw_df = raw_df
        self.quantity_type = QuantityTypes.TOTAL

    def to_facts(self) -> LazyGeneric[FactSchema]:
        """
        Generates 2 Facts from each NovaHostRaw row:
        1. Created At -> Value: "active" , timestamp
        2. Deleted At -> Value: "null" , timestamp
        """

        # map raw columns to standard column names
        base = self.raw_df.select(
            [
                pl.col(NovaHostRaw.hypervisor_hostname).alias(C.ENTITY_ID),
                pl.col(NovaHostRaw.created_at).alias(C.CREATED_AT),
                pl.col(NovaHostRaw.deleted_at).alias(C.DELETED_AT),
                pl.lit(Sources.NOVA).alias(C.SOURCE),
            ]
        )
        events = _expand_events(base, self.quantity_type)
        return FactSchema.validate(events)


class NovaInstanceAdapter:
    """
    Takes input values from nova computenode table and generates facts.
    """

    def __init__(self, raw_df: LazyGeneric[NovaInstanceRaw]):
        self.raw_df = raw_df
        self.quantity_type = QuantityTypes.OCCUPIED

    def to_facts(self) -> LazyGeneric[FactSchema]:
        """
        Generates 2 Facts from each NovaHostRaw row:
        1. Created At -> Value: "active" , timestamp
        2. Deleted At -> Value: "null" , timestamp
        """

        # map raw columns to standard column names
        base = self.raw_df.select(
            [
                pl.col(NovaInstanceRaw.node).alias(C.ENTITY_ID),
                pl.col(NovaInstanceRaw.created_at).alias(C.CREATED_AT),
                pl.col(NovaInstanceRaw.deleted_at).alias(C.DELETED_AT),
                pl.lit(Sources.NOVA).alias(C.SOURCE),
            ]
        )

        events = _expand_events(base, self.quantity_type)
        return FactSchema.validate(events)


class BlazarComputehostAdapter:
    """
    Takes input values from nova computenode table and generates facts.
    """

    def __init__(self, raw_df: LazyGeneric[BlazarHostRaw]):
        self.raw_df = raw_df
        self.quantity_type = QuantityTypes.RESERVABLE

    def to_facts(self) -> LazyGeneric[FactSchema]:
        """
        Generates 2 Facts from each NovaHostRaw row:
        1. Created At -> Value: "active" , timestamp
        2. Deleted At -> Value: "null" , timestamp
        """

        # map raw columns to standard column names
        base = self.raw_df.select(
            [
                pl.col(BlazarHostRaw.hypervisor_hostname).alias(C.ENTITY_ID),
                pl.col(BlazarHostRaw.created_at).alias(C.CREATED_AT),
                pl.col(BlazarHostRaw.deleted_at).alias(C.DELETED_AT),
                pl.lit(Sources.BLAZAR).alias(C.SOURCE),
            ]
        )

        events = _expand_events(base, self.quantity_type)
        return FactSchema.validate(events)


class BlazarAllocationAdapter:
    """
    Takes input values from nova computenode table and generates facts.
    """

    def __init__(
        self,
        alloc: LazyGeneric[BlazarAllocationRaw],
        res: LazyGeneric[BlazarReservationRaw],
        lease: LazyGeneric[BlazarLeaseRaw],
    ):
        self.alloc = alloc
        self.res = res
        self.lease = lease
        self.quantity_type = QuantityTypes.COMMITTED

    def to_facts(self) -> LazyGeneric[FactSchema]:
        """
        Generates 2 Facts from each row:
        1. Created At -> Value: "active" , timestamp
        2. Deleted At -> Value: "null" , timestamp
        """

        base = (
            self.alloc.join(
                self.res,
                left_on=BlazarAllocationRaw.reservation_id,
                right_on=BlazarReservationRaw.id,
                how="left",
                suffix="_res",
            )
            .join(
                self.lease,
                left_on=BlazarReservationRaw.lease_id,
                right_on=BlazarLeaseRaw.id,
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

        events = _expand_events(base, self.quantity_type)
        return FactSchema.validate(events)
