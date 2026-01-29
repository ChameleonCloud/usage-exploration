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

    def get_usage(self) -> LazyGeneric[UsageSchema]:
        #  date       ┆ node_type         ┆ cnt
        # node_counts = NodeCountCache.validate(self.node_count_cache)
        # TODO: check how this is computed...

        # date       ┆ node_type         ┆ maint_hours ┆ reserved_hours ┆ used_hours ┆ idle_hours ┆ total_hours
        node_hours = NodeUsageReportCache.validate(self.node_usage_report_cache)
        hours_per_period = 24  # it is bucketed daily

        # need long with quantity_type

        output_format = (
            node_hours.select(
                (pl.col("date")).alias(C.TIMESTAMP),
                (pl.col("total_hours") / hours_per_period).alias(QT.RESERVABLE),
                (pl.col("reserved_hours") / hours_per_period).alias(QT.COMMITTED),
                # treat all used as active
                (pl.col("used_hours") / hours_per_period).alias(QT.OCCUPIED),
                (pl.col("used_hours") / hours_per_period).alias(QT.ACTIVE),
                # trust source calc for "idle"
                (pl.col("idle_hours") / hours_per_period).alias(QT.IDLE),
            )
            .unpivot(
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
        return UsageSchema.validate(output_format)
