import polars as pl

from chameleon_usage.constants import Cols as C
from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.models.domain import (
    UsageSchema,
)


def resample_simple(usage: pl.LazyFrame, interval: str = "1d") -> pl.LazyFrame:
    """Simple resampling - assigns each record to its start bucket.

    Loses accuracy when records span multiple buckets.
    """
    schema_cols = usage.collect_schema().names()
    group_cols = [c for c in schema_cols if c not in {C.TIMESTAMP, C.COUNT}]

    return (
        usage.with_columns(pl.col(C.TIMESTAMP).dt.truncate(interval).alias("bucket"))
        .group_by(["bucket", *group_cols])
        .agg(pl.col(C.COUNT).mean())
        .rename({"bucket": C.TIMESTAMP})
        .sort([C.TIMESTAMP, *group_cols])
    )


def compute_derived_metrics(df: pl.LazyFrame) -> pl.LazyFrame:
    """Compute available and idle from base metrics.

    available = reservable - committed
    idle = committed - used (only if used exists)
    """
    index_cols = [
        C.TIMESTAMP,
        "collector_type",
        "site",
    ]

    # long to wide
    pivoted = df.collect().pivot(on=C.QUANTITY_TYPE, index=index_cols, values=C.COUNT)

    # only know these after pivot
    cols = pivoted.columns

    # simple subtraction
    if QT.RESERVABLE in cols and QT.COMMITTED in cols:
        pivoted = pivoted.with_columns(
            (pl.col(QT.RESERVABLE) - pl.col(QT.COMMITTED)).alias(QT.AVAILABLE),
        )

    if QT.COMMITTED in cols and QT.OCCUPIED in cols:
        pivoted = pivoted.with_columns(
            (pl.col(QT.COMMITTED) - pl.col(QT.OCCUPIED)).alias(QT.IDLE),
        )

    # wide to long
    unpivoted = (
        pivoted.unpivot(
            index=index_cols, variable_name=C.QUANTITY_TYPE, value_name=C.COUNT
        )
        .drop_nulls(C.COUNT)
        .lazy()
    ).select(["timestamp", "quantity_type", "count", "site", "collector_type"])

    return UsageSchema.validate(unpivoted)
