import ibis
import polars as pl
import logging

from chameleon_usage.constants import Metrics as M
from chameleon_usage.constants import ResourceTypes as RT
from chameleon_usage.schemas import UsageModel, WideOutput

logger = logging.getLogger(__name__)

OUTPUT_DATABASE = "usage_compat"
OUTPUT_TABLE = "usage_wide"


def to_compat_format(long_df: pl.DataFrame) -> pl.DataFrame:
    usage: pl.DataFrame = UsageModel.validate(long_df)

    nodes = usage.filter(
        (pl.col("collector_type") == "current"),
        (pl.col("resource") == RT.NODE),
    )

    # Coerce usable to string for pivot key: "true"/"false"/"null"
    has_usable = "usable" in nodes.columns
    if has_usable:
        nodes = nodes.with_columns(
            pl.col("usable").cast(pl.Utf8).fill_null("null").alias("_usable_key")
        )
    else:
        nodes = nodes.with_columns(pl.lit("null").alias("_usable_key"))

    # Pivot on metric_usable composite key
    nodes = nodes.with_columns(
        (pl.col("metric") + pl.lit("_") + pl.col("_usable_key")).alias("_pivot_key")
    )

    # Totals: sum across usable states per (timestamp, site, metric)
    totals = (
        nodes.select("timestamp", "site", "metric", "value")
        .group_by(["timestamp", "site", "metric"])
        .agg(pl.col("value").sum())
        .pivot(on="metric", index=["timestamp", "site"], values="value")
    )

    # Usable breakdowns: pivot on metric_usable
    breakdowns = (
        nodes.filter(pl.col("_usable_key") != "null")
        .select("timestamp", "site", "_pivot_key", "value")
        .group_by(["timestamp", "site", "_pivot_key"])
        .agg(pl.col("value").sum())
        .pivot(on="_pivot_key", index=["timestamp", "site"], values="value")
    )

    pivoted = totals.join(breakdowns, on=["timestamp", "site"], how="left")

    # Ensure all expected columns exist
    for col in [
        M.TOTAL,
        M.RESERVABLE,
        M.COMMITTED,
        M.OCCUPIED_RESERVATION,
        M.OCCUPIED_ONDEMAND,
        "reservable_true",
        "reservable_false",
        "committed_true",
        "committed_false",
    ]:
        if col not in pivoted.columns:
            pivoted = pivoted.with_columns(pl.lit(0.0).alias(col))

    wide = (
        pivoted.select(
            pl.col("timestamp").alias("time"),
            pl.col("site"),
            pl.lit(RT.NODE).alias("resource"),
            pl.col(M.TOTAL),
            pl.col(M.RESERVABLE),
            pl.col("reservable_true").alias("reservable_usable"),
            pl.col("reservable_false").alias("reservable_unusable"),
            pl.col(M.COMMITTED),
            pl.col("committed_true").alias("committed_usable"),
            pl.col("committed_false").alias("committed_unusable"),
            pl.col(M.OCCUPIED_ONDEMAND),
            pl.col(M.OCCUPIED_RESERVATION).alias("occupied_reserved"),
            pl.col(M.OCCUPIED_ONDEMAND).alias("active_ondemand"),
            pl.col(M.OCCUPIED_RESERVATION).alias("active_reserved"),
        )
        .sort(["time", "site"])
        .lazy()
    )

    return WideOutput.validate(wide).collect()


def write_compat_to_db(
    compat_df: pl.LazyFrame | pl.DataFrame, db_uri: str, overwrite: bool = True
) -> None:
    data = compat_df.collect() if isinstance(compat_df, pl.LazyFrame) else compat_df
    conn = ibis.connect(db_uri)
    conn.create_table(
        OUTPUT_TABLE,
        obj=data,
        database=OUTPUT_DATABASE,
        overwrite=overwrite,
    )
