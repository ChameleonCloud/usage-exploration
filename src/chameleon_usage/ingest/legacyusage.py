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

from pathlib import Path

import polars as pl
from pandera.typing.polars import LazyFrame as LazyGeneric

from chameleon_usage.constants import Metrics as M
from chameleon_usage.constants import SchemaCols as S
from chameleon_usage.ingest import rawschemas as raw
from chameleon_usage.schemas import TimelineModel

HOURS_PER_DAY = 24


def load_legacy_usage_cache(path: str) -> pl.LazyFrame:
    parquet_path = Path(path) / "chameleon_usage.node_usage_report_cache.parquet"
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
        (pl.col("total_hours") / HOURS_PER_DAY).alias(M.TOTAL),
        (pl.col("reservable_hours") / HOURS_PER_DAY).alias(M.RESERVABLE),
        (pl.col("committed_hours") / HOURS_PER_DAY).alias(M.COMMITTED),
        (pl.col("occupied_hours") / HOURS_PER_DAY).alias(M.OCCUPIED_RESERVATION),
        (pl.col("available_hours") / HOURS_PER_DAY).alias(M.AVAILABLE_RESERVABLE),
        (pl.col("idle_hours") / HOURS_PER_DAY).alias(M.IDLE),
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


def get_legacy_usage_counts(path: str) -> LazyGeneric[TimelineModel]:
    """Transform legacy usage cache to UsageModel."""

    usage_cache = load_legacy_usage_cache(path)
    aggregated = _aggregate_hours_by_date(usage_cache)
    hours = _to_current_hours(aggregated)
    wide = _hours_to_counts(hours)

    long_output = _to_long_format(wide).with_columns(pl.lit("nodes").alias(S.RESOURCE))
    return TimelineModel.validate(long_output)
