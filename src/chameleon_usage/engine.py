import polars as pl
from pandera.typing.polars import LazyFrame as LazyGeneric

from chameleon_usage.constants import Cols as C
from chameleon_usage.constants import States
from chameleon_usage.models.domain import FactSchema, TimelineSchema


class TimelineBuilder:
    def build(self, raw_facts: LazyGeneric[FactSchema]) -> LazyGeneric[TimelineSchema]:
        """
        Takes list of facts:
        Produces list of segments
        """

        valid_facts = FactSchema.validate(raw_facts)

        index_cols = [C.TIMESTAMP, C.ENTITY_ID, C.QUANTITY_TYPE]

        pivoted = valid_facts.collect().pivot(
            on=FactSchema.source,
            values=FactSchema.value,
            index=index_cols,
            aggregate_function="first",
        )

        source_cols = [c for c in pivoted.columns if c not in index_cols]

        state_sequence = (
            pivoted.lazy()
            # Forward fill each source column per (entity_id, quantity_type)
            .with_columns(
                [
                    pl.col(s).forward_fill().over([C.ENTITY_ID, C.QUANTITY_TYPE])
                    for s in source_cols
                ]
            )
            # Coalesce sources in column order (first non-null wins)
            .with_columns(
                pl.coalesce([pl.col(s) for s in source_cols]).alias("final_state")
            )
            .select([C.TIMESTAMP, C.ENTITY_ID, C.QUANTITY_TYPE, "final_state"])
            .sort([C.ENTITY_ID, C.QUANTITY_TYPE, C.TIMESTAMP])
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
                .over([C.ENTITY_ID, C.QUANTITY_TYPE])
                .fill_null(0)
                .alias(C.PREV_VALUE)
            )
            .with_columns((pl.col(C.VALUE) - pl.col(C.PREV_VALUE)).alias(C.DELTA))
            # 3. COMPRESS (Global Aggregation)
            #    Sum all deltas happening at the exact same microsecond across all entities.
            .group_by([C.TIMESTAMP, C.QUANTITY_TYPE])
            .agg(pl.col(C.DELTA).sum())
            .sort([C.QUANTITY_TYPE, C.TIMESTAMP])
            # 4. INTEGRATE (Running Total)
            #    Walk forward in time to determine the total count PER QUANTITY_TYPE.
            .with_columns(
                pl.col(C.DELTA).cum_sum().over(C.QUANTITY_TYPE).alias(C.COUNT),
            )
            .select([C.TIMESTAMP, C.QUANTITY_TYPE, C.COUNT])
        )

    def resample_time_weighted(
        self, usage: pl.LazyFrame, interval: str = "1d"
    ) -> pl.LazyFrame:
        return (
            usage.sort([C.QUANTITY_TYPE, C.TIMESTAMP])
            .with_columns(
                pl.col(C.TIMESTAMP)
                .shift(-1)
                .over(C.QUANTITY_TYPE)
                .alias("next_timestamp")
            )
            .with_columns(
                (pl.col("next_timestamp") - pl.col(C.TIMESTAMP))
                .dt.total_seconds()
                .alias("duration_seconds")
            )
            .filter(pl.col("duration_seconds").is_not_null())
            .with_columns(pl.col(C.TIMESTAMP).dt.truncate(interval).alias("bucket"))
            .group_by(["bucket", C.QUANTITY_TYPE])
            .agg(
                (pl.col(C.COUNT) * pl.col("duration_seconds")).sum()
                / pl.col("duration_seconds").sum()
            )
            .rename({"bucket": C.TIMESTAMP})
            .sort([C.QUANTITY_TYPE, C.TIMESTAMP])
        )
