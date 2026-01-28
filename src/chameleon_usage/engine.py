import polars as pl
from pandera.typing.polars import LazyFrame as LazyGeneric

from chameleon_usage.constants import Cols as C
from chameleon_usage.constants import Sources
from chameleon_usage.models.domain import FactSchema


class SegmentBuilder:
    def build(self, raw_facts: LazyGeneric[FactSchema]) -> pl.LazyFrame:
        """
        Takes list of facts:
        Produces list of segments
        """

        valid_facts = FactSchema.validate(raw_facts)

        return (
            valid_facts.collect()
            .pivot(
                on=FactSchema.source,
                values=FactSchema.value,
                index=[str(FactSchema.timestamp), FactSchema.entity_id],
                aggregate_function="first",
            )
            .lazy()
            # 2. PAINT: Forward Fill logic
            .with_columns(pl.col(Sources.NOVA).forward_fill().over(C.ENTITY_ID))
            # 3. RESULT
            .select(
                [
                    pl.col(C.TIMESTAMP),
                    pl.col(C.ENTITY_ID),
                    pl.col(Sources.NOVA).alias("final_state"),
                ]
            )
            .sort([C.ENTITY_ID, C.TIMESTAMP])
        )
