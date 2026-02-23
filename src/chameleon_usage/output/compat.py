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

    pivoted = (
        usage.filter(
            (pl.col("collector_type") == "current"),
            (pl.col("resource") == RT.NODE),
        )
        .select("timestamp", "site", "metric", "value")
        .group_by(["timestamp", "site", "metric"])
        .agg(pl.col("value").sum())
        .pivot(on="metric", index=["timestamp", "site"], values="value")
    )

    # Handle case where columns are totally missing
    cols = set(pivoted.columns)
    if M.TOTAL not in cols:
        pivoted = pivoted.with_columns(pl.col(M.RESERVABLE).alias(M.TOTAL))
        logger.warning("TOTAL not in columns, setting == RESERVABLE")
    if M.OCCUPIED_RESERVATION not in cols:
        pivoted = pivoted.with_columns(pl.col(M.COMMITTED).alias(M.OCCUPIED_RESERVATION))
        logger.warning("OCCUPIED_RESERVED not in columns, setting == COMMITTED")
    if M.OCCUPIED_ONDEMAND not in cols:
        pivoted = pivoted.with_columns(pl.lit(0.0).alias(M.OCCUPIED_ONDEMAND))
        logger.warning("OCCUPIED_ONDEMAND not in columns, setting == 0")

    # Handle case where columns missing for a specific site
    site_missing_total = pl.col(M.TOTAL).is_null().all().over("site")
    site_missing_occ_res = pl.col(M.OCCUPIED_RESERVATION).is_null().all().over("site")
    site_missing_occ_on = pl.col(M.OCCUPIED_ONDEMAND).is_null().all().over("site")

    pivoted = pivoted.with_columns(
        # Set total = reservable if missing
        pl.when(site_missing_total).then(pl.col(M.RESERVABLE)).otherwise(pl.col(M.TOTAL)).alias(M.TOTAL),
        # Set occupied reserved = committed if missing
        pl.when(site_missing_occ_res).then(pl.col(M.COMMITTED)).otherwise(pl.col(M.OCCUPIED_RESERVATION)).alias(M.OCCUPIED_RESERVATION),
        # Set occupied ondemand = 0 if missing
        pl.when(site_missing_occ_on).then(pl.lit(0.0)).otherwise(pl.col(M.OCCUPIED_ONDEMAND)).alias(M.OCCUPIED_ONDEMAND),
    )


    wide = (
        pivoted.select(
            pl.col("timestamp").alias("time"),
            pl.col("site"),
            pl.lit(RT.NODE).alias("resource"),
            pl.col(M.TOTAL),
            pl.col(M.RESERVABLE),
            pl.col(M.COMMITTED),
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
