import ibis
import polars as pl

from chameleon_usage.constants import Metrics as M
from chameleon_usage.constants import ResourceTypes as RT
from chameleon_usage.schemas import UsageModel, WideOutput

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
