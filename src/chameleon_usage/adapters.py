from abc import ABC

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


class BaseAdapter(ABC):
    quantity_type: str

    def __init__(self, raw_df: pl.LazyFrame):
        self.raw_df = raw_df

    def to_facts(self) -> LazyGeneric[FactSchema]:
        raise NotImplementedError


class NovaComputeAdapter(BaseAdapter):
    """
    Takes input values from nova computenode table and generates facts.
    """

    quantity_type = QuantityTypes.TOTAL

    def __init__(self, raw_df: LazyGeneric[NovaHostRaw]):
        self.raw_df = raw_df

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

        starts = base.select(
            pl.col(C.CREATED_AT).alias(C.TIMESTAMP),
            pl.col(C.ENTITY_ID),
            pl.lit(self.quantity_type).alias(C.QUANTITY_TYPE),
            pl.lit(States.ACTIVE).alias(C.VALUE),
            pl.col(C.SOURCE),
        )
        ends = base.select(
            pl.col(C.DELETED_AT).alias(C.TIMESTAMP),
            pl.col(C.ENTITY_ID),
            pl.lit(self.quantity_type).alias(C.QUANTITY_TYPE),
            pl.lit(States.DELETED).alias(C.VALUE),
            pl.col(C.SOURCE),
        )

        combined = pl.concat([starts, ends])

        return FactSchema.validate(combined)


class BlazarComputehostAdapter(BaseAdapter):
    """
    Takes input values from nova computenode table and generates facts.
    """

    quantity_type = QuantityTypes.RESERVABLE

    def __init__(self, raw_df: LazyGeneric[BlazarHostRaw]):
        self.raw_df = raw_df

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

        starts = base.select(
            pl.col(C.CREATED_AT).alias(C.TIMESTAMP),
            pl.col(C.ENTITY_ID),
            pl.lit(self.quantity_type).alias(C.QUANTITY_TYPE),
            pl.lit(States.ACTIVE).alias(C.VALUE),
            pl.col(C.SOURCE),
        )
        ends = base.select(
            pl.col(C.DELETED_AT).alias(C.TIMESTAMP),
            pl.col(C.ENTITY_ID),
            pl.lit(self.quantity_type).alias(C.QUANTITY_TYPE),
            pl.lit(States.DELETED).alias(C.VALUE),
            pl.col(C.SOURCE),
        )

        combined = pl.concat([starts, ends])

        return FactSchema.validate(combined)


class BlazarAllocationAdapter(BaseAdapter):
    """
    Takes input values from nova computenode table and generates facts.
    """

    quantity_type = QuantityTypes.COMMITTED

    def __init__(
        self,
        alloc: LazyGeneric[BlazarAllocationRaw],
        res: LazyGeneric[BlazarReservationRaw],
        lease: LazyGeneric[BlazarLeaseRaw],
    ):
        self.alloc = alloc
        self.res = res
        self.lease = lease

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

        starts = base.select(
            pl.col(C.CREATED_AT).alias(C.TIMESTAMP),
            pl.col(C.ENTITY_ID),
            pl.lit(self.quantity_type).alias(C.QUANTITY_TYPE),
            pl.lit(States.ACTIVE).alias(C.VALUE),
            pl.col(C.SOURCE),
        )
        ends = base.select(
            pl.col(C.DELETED_AT).alias(C.TIMESTAMP),
            pl.col(C.ENTITY_ID),
            pl.lit(self.quantity_type).alias(C.QUANTITY_TYPE),
            pl.lit(States.DELETED).alias(C.VALUE),
            pl.col(C.SOURCE),
        )

        combined = pl.concat([starts, ends])

        return FactSchema.validate(combined)
