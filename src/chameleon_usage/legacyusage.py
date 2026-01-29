# load legacy usage tables


import polars as pl
from pandera.typing.polars import LazyFrame as LazyGeneric

from chameleon_usage.constants import Cols as C
from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.models.domain import UsageSchema
from chameleon_usage.models.raw import NodeCountCache, NodeUsageReportCache


class LegacyUsageLoader:
    """load legacy usage, output usge timeline.

    1. Loads parquet files for nodecountcache and nodeusagereportcache
    2...
    3. Output Usage timeline
    """

    node_count_cache: pl.LazyFrame
    node_event: pl.LazyFrame
    node_maintenance: pl.LazyFrame
    node_usage_report_cache: pl.LazyFrame
    node_usage: pl.LazyFrame

    def __init__(self, input_path: str, site_name: str):
        self.input_path = input_path
        self.site_name = site_name

        self.parquet_path = f"{self.input_path}/{self.site_name}"

    def load_facts(self):
        for key in [
            "node_count_cache",
            "node_event",
            "node_maintenance",
            "node_usage_report_cache",
            "node_usage",
        ]:
            setattr(
                self,
                key,
                pl.scan_parquet(f"{self.parquet_path}/chameleon_usage.{key}.parquet"),
            )

    HOURS_PER_DAY = 24

    def _aggregate_hours_by_date(self) -> pl.LazyFrame:
        node_hours = NodeUsageReportCache.validate(self.node_usage_report_cache)
        return node_hours.group_by("date").agg(
            pl.col("maint_hours").sum(),
            pl.col("reserved_hours").sum(),
            pl.col("used_hours").sum(),
            pl.col("idle_hours").sum(),
            pl.col("total_hours").sum(),
        )

    def _to_current_hours(self, aggregated: pl.LazyFrame) -> pl.LazyFrame:
        reservable = pl.col("total_hours") - pl.col("maint_hours")
        committed = pl.col("reserved_hours") + pl.col("used_hours")

        return aggregated.select(
            pl.col("date"),
            pl.col("total_hours"),
            reservable.alias("reservable_hours"),
            committed.alias("committed_hours"),
            (reservable - committed).alias("available_hours"),
            pl.col("reserved_hours").alias("idle_hours"),
            # pl.col("idle_hours").alias("idle_hours_orig"),
            pl.col("used_hours").alias("occupied_hours"),
        )

    def _hours_to_counts(self, hours: pl.LazyFrame) -> pl.LazyFrame:
        return hours.select(
            pl.col("date").alias(C.TIMESTAMP),
            (pl.col("total_hours") / self.HOURS_PER_DAY).alias(QT.TOTAL),
            (pl.col("reservable_hours") / self.HOURS_PER_DAY).alias(QT.RESERVABLE),
            (pl.col("committed_hours") / self.HOURS_PER_DAY).alias(QT.COMMITTED),
            (pl.col("occupied_hours") / self.HOURS_PER_DAY).alias(QT.OCCUPIED),
            (pl.col("available_hours") / self.HOURS_PER_DAY).alias(QT.AVAILABLE),
            (pl.col("idle_hours") / self.HOURS_PER_DAY).alias(QT.IDLE),
        )

    def _hours_to_percent(self, hours: pl.LazyFrame) -> pl.LazyFrame:
        return hours.select(
            pl.col("date").alias(C.TIMESTAMP),
            (pl.col("committed_hours") / pl.col("reservable_hours") * 100).alias(
                QT.COMMITTED
            ),
            (pl.col("occupied_hours") / pl.col("reservable_hours") * 100).alias(
                QT.OCCUPIED
            ),
            (pl.col("occupied_hours") / pl.col("reservable_hours") * 100).alias(
                QT.ACTIVE
            ),
            (pl.col("idle_hours") / pl.col("reservable_hours") * 100).alias(QT.IDLE),
        )

    def _to_long_format(self, wide: pl.LazyFrame) -> pl.LazyFrame:
        return (
            wide.unpivot(
                index=C.TIMESTAMP,
                variable_name=C.QUANTITY_TYPE,
                value_name=C.COUNT,
            )
            .group_by([C.TIMESTAMP, C.QUANTITY_TYPE])
            .agg(pl.col(C.COUNT).sum())
            .sort([C.TIMESTAMP, C.QUANTITY_TYPE])
            .with_columns(
                pl.lit(self.site_name).alias("site"),
                pl.lit("legacy").alias("collector_type"),
            )
        )

    def get_usage(self, as_percent: bool = False) -> LazyGeneric[UsageSchema]:
        aggregated = self._aggregate_hours_by_date()
        hours = self._to_current_hours(aggregated)
        if as_percent:
            wide = self._hours_to_percent(hours)
        else:
            wide = self._hours_to_counts(hours)
        return UsageSchema.validate(self._to_long_format(wide))
