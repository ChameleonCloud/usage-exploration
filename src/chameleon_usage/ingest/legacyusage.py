"""Transform legacy usage data to UsageModel.

Legacy data is pre-aggregated (hours per day per node_type), so it bypasses
the interval->cumsum pipeline entirely.

Available tables:
node_count_cache: pl.LazyFrame
node_event: pl.LazyFrame
node_maintenance: pl.LazyFrame
node_usage_report_cache: pl.LazyFrame
node_usage: pl.LazyFrame
"""

####
# TODO!!!! check if we can load "hours per node" as fake events
#################################

from pathlib import Path

import polars as pl
from pandera.typing.polars import LazyFrame as LazyGeneric

from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.constants import SchemaCols as S
from chameleon_usage.ingest import rawschemas as raw
from chameleon_usage.schemas import UsageModel

HOURS_PER_DAY = 24


def load_legacy_usage_cache(base_path: str, site_name: str) -> pl.LazyFrame:
    parquet_path = (
        Path(base_path) / site_name / "chameleon_usage.node_usage_report_cache.parquet"
    )
    if not parquet_path.exists():
        return raw.NodeUsageReportCache.empty().lazy()

    return raw.NodeUsageReportCache.validate(pl.scan_parquet(parquet_path))


def _aggregate_hours_by_date(usage_cache: pl.LazyFrame) -> pl.LazyFrame:
    return usage_cache.group_by("date").agg(
        pl.col("maint_hours").sum(),
        pl.col("reserved_hours").sum(),
        pl.col("used_hours").sum(),
        pl.col("idle_hours").sum(),
        pl.col("total_hours").sum(),
    )


def _to_current_hours(aggregated: pl.LazyFrame) -> pl.LazyFrame:
    reservable = pl.col("total_hours") - pl.col("maint_hours")
    committed = pl.col("reserved_hours") + pl.col("used_hours")

    return aggregated.select(
        pl.col("date"),
        pl.col("total_hours"),
        reservable.alias("reservable_hours"),
        committed.alias("committed_hours"),
        (reservable - committed).alias("available_hours"),
        pl.col("reserved_hours").alias("idle_hours"),
        pl.col("used_hours").alias("occupied_hours"),
    )


def _hours_to_counts(hours: pl.LazyFrame) -> pl.LazyFrame:
    return hours.select(
        pl.col("date").alias(S.TIMESTAMP),
        (pl.col("total_hours") / HOURS_PER_DAY).alias(QT.TOTAL),
        (pl.col("reservable_hours") / HOURS_PER_DAY).alias(QT.RESERVABLE),
        (pl.col("committed_hours") / HOURS_PER_DAY).alias(QT.COMMITTED),
        (pl.col("occupied_hours") / HOURS_PER_DAY).alias(QT.OCCUPIED_RESERVATION),
        (pl.col("available_hours") / HOURS_PER_DAY).alias(QT.AVAILABLE_RESERVABLE),
        (pl.col("idle_hours") / HOURS_PER_DAY).alias(QT.IDLE),
    )


def _to_long_format(wide: pl.LazyFrame) -> pl.LazyFrame:
    return (
        wide.unpivot(
            index=S.TIMESTAMP,
            variable_name=S.METRIC,
            value_name=S.VALUE,
        )
        .group_by([S.TIMESTAMP, S.METRIC])
        .agg(pl.col(S.VALUE).sum())
        .sort([S.TIMESTAMP, S.METRIC])
    )


def get_legacy_usage_counts(
    base_path: str, site_name: str, collector_type: str
) -> LazyGeneric[UsageModel]:
    """Transform legacy usage cache to UsageModel."""

    usage_cache = load_legacy_usage_cache(base_path, site_name)
    aggregated = _aggregate_hours_by_date(usage_cache)
    hours = _to_current_hours(aggregated)
    wide = _hours_to_counts(hours)

    long_output = _to_long_format(wide).with_columns(
        pl.lit(site_name).alias("site"),
        pl.lit("legacy").alias("collector_type"),
        pl.lit("nodes").alias(S.RESOURCE),
    )
    return UsageModel.validate(long_output)
