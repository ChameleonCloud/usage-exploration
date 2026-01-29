from typing import List

import polars as pl
from pandera.typing.polars import LazyFrame as LazyGeneric

from chameleon_usage.constants import Cols as C
from chameleon_usage.constants import Sources, States
from chameleon_usage.models.domain import FactSchema, SegmentSchema, UsageSchema


class SegmentBuilder:
    def __init__(
        self,
        site_name: str,
        priority_order: List[str] | None = None,
        group_cols: List[str] | None = None,
    ):
        """
        Args:
            priority_order: List of sources in descending priority.
            e.g. ["manual_override", "blazar_allocations", "nova_host"]
        """
        self.site_name = site_name
        self.group_cols = group_cols if group_cols else [C.ENTITY_ID, C.QUANTITY_TYPE]
        self.priority_order = priority_order or [Sources.NOVA, Sources.BLAZAR]

    def build(self, facts: pl.LazyFrame) -> pl.LazyFrame:
        """
        Transformation:
            1. Facts (Raw Logs) -> Events (Resolved Change Log)
            2. Events -> Segments (Clean [Start, End) Intervals)
        """

        events = self._facts_to_events(FactSchema.validate(facts))
        segments = self._events_to_segments(events)
        return SegmentSchema.validate(segments)

    def _facts_to_events(self, facts: pl.LazyFrame) -> pl.LazyFrame:
        # Defaults to timestamp, entity_id
        index_cols = [C.TIMESTAMP] + self.group_cols
        sort_order = self.group_cols + [C.TIMESTAMP]

        pivoted = facts.collect().pivot(
            on=C.SOURCE,
            index=index_cols,
            values=C.VALUE,
            aggregate_function="first",
        )
        available_sources = [s for s in self.priority_order if s in pivoted.columns]

        return (
            pivoted.lazy()
            .sort(sort_order)
            .with_columns(
                [
                    # Forward Fill over composite identity
                    pl.col(s).forward_fill().over(self.group_cols)
                    for s in available_sources
                ]
            )
            .select(index_cols + [pl.coalesce(available_sources).alias("final_state")])
            .with_columns(
                pl.col("final_state").shift(1).over(self.group_cols).alias("prev_state")
            )
            .filter(
                # Only output state changes, use prev_state to catch first entry
                pl.col("final_state")
                != pl.col("final_state").shift(1).over(self.group_cols)
            )
            .drop("prev_state")
        )

    def _events_to_segments(self, events: pl.LazyFrame) -> pl.LazyFrame:
        sort_order = self.group_cols + [C.TIMESTAMP]

        return (
            events.sort(sort_order)
            .select(
                [pl.col(c) for c in self.group_cols]
                + [
                    pl.col("final_state").alias("final_state"),
                    pl.col(C.TIMESTAMP).alias("start"),
                    pl.col(C.TIMESTAMP).shift(-1).over(self.group_cols).alias("end"),
                ]
            )
            .filter(pl.col("end").is_not_null())
            # DELETED exist only to terminate spans, don't start a new one.
            .filter(pl.col("final_state") != States.DELETED)
        )

    def calculate_concurrency(self, segments: pl.LazyFrame) -> pl.LazyFrame:
        active = segments.filter(pl.col("final_state") == States.ACTIVE)

        starts = active.select(
            [
                *self.group_cols,
                pl.col("start").alias(C.TIMESTAMP),
                pl.lit(1).alias(C.DELTA),
            ]
        )
        ends = active.filter(pl.col("end").is_not_null()).select(
            [
                *self.group_cols,
                pl.col("end").alias(C.TIMESTAMP),
                pl.lit(-1).alias(C.DELTA),
            ]
        )

        usage = (
            pl.concat([starts, ends])
            .group_by([*self.group_cols, C.TIMESTAMP])
            .agg(pl.col(C.DELTA).sum())
            .sort([*self.group_cols, C.TIMESTAMP])
            .with_columns(
                pl.col(C.DELTA).cum_sum().over(self.group_cols).alias(C.COUNT)
            )
        ).with_columns(
            pl.lit(self.site_name).alias("site"),
            pl.lit("current").alias("collector_type"),
        )

        return UsageSchema.validate(usage)
