import polars as pl
from pandera.typing.polars import LazyFrame as LazyGeneric

from chameleon_usage.constants import Cols as C
from chameleon_usage.constants import Sources, States
from chameleon_usage.models.domain import FactSchema, TimelineSchema


class TimelineBuilder:
    def build(self, raw_facts: LazyGeneric[FactSchema]) -> LazyGeneric[TimelineSchema]:
        """
        Takes list of facts:
        Produces list of segments
        """

        valid_facts = FactSchema.validate(raw_facts)

        state_sequence = (
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

        return TimelineSchema.validate(state_sequence)

    def calculate_concurrency(
        self, timeline: LazyGeneric[TimelineSchema]
    ) -> pl.LazyFrame:
        return (
            timeline
            # 1. MAP STATE TO NUMBER (The "Height" of the resource)
            #    Active = 1, Maintenance = 0 (or maybe 1 if you count it as 'occupied')
            .with_columns(
                pl.when(pl.col(TimelineSchema.final_state) == States.ACTIVE)
                .then(1)
                .otherwise(0)
                .alias(C.VALUE)
            )
            # 2. CALCULATE DELTA (The Change)
            #    Compare current row to previous row PER ENTITY.
            #    Row 1 (Active): Val 1, Prev 0 -> Delta +1
            #    Row 2 (Delete): Val 0, Prev 1 -> Delta -1
            .with_columns(
                pl.col(C.VALUE)
                .shift(1)
                .over(C.ENTITY_ID)
                .fill_null(0)
                .alias(C.PREV_VALUE)
            )
            .with_columns((pl.col(C.VALUE) - pl.col(C.PREV_VALUE)).alias(C.DELTA))
            # 3. COMPRESS (Global Aggregation)
            #    Sum all deltas happening at the exact same microsecond across all entities.
            .group_by(C.TIMESTAMP)
            .agg(pl.col(C.DELTA).sum())
            .sort(C.TIMESTAMP)
            # 4. INTEGRATE (Running Total)
            #    Walk forward in time to determine the total count.
            .with_columns(pl.col(C.DELTA).cum_sum().alias(C.TOTAL_QUANTITY))
            .select([C.TIMESTAMP, C.TOTAL_QUANTITY])
        )
