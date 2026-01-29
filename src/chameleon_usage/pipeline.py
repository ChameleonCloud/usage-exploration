import polars as pl

from chameleon_usage.adapters import (
    BlazarAllocationAdapter,
    BlazarComputehostAdapter,
    NovaComputeAdapter,
    NovaInstanceAdapter,
)
from chameleon_usage.constants import Cols as C
from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.models.domain import (
    FactFrame,
    SegmentFrame,
    UsageFrame,
    UsageSchema,
)
from chameleon_usage.models.raw import (
    BlazarAllocationRaw,
    BlazarHostRaw,
    BlazarLeaseRaw,
    BlazarReservationRaw,
    NovaHostRaw,
    NovaInstanceRaw,
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


def compute_derived_metrics(df: UsageFrame) -> UsageFrame:
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

    # simple subtraction
    pivoted = pivoted.with_columns(
        (pl.col(QT.RESERVABLE) - pl.col(QT.COMMITTED)).alias(QT.AVAILABLE),
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


def load_facts(input_data: str, site_name: str) -> FactFrame:
    base_path = f"{input_data}/{site_name}"
    all_facts = []
    all_facts.append(
        NovaComputeAdapter(
            NovaHostRaw.validate(
                pl.scan_parquet(f"{base_path}/nova.compute_nodes.parquet")
            )
        ).to_facts()
    )
    all_facts.append(
        NovaInstanceAdapter(
            NovaInstanceRaw.validate(
                pl.scan_parquet(f"{base_path}/nova.instances.parquet")
            )
        ).to_facts()
    )
    all_facts.append(
        BlazarComputehostAdapter(
            BlazarHostRaw.validate(
                pl.scan_parquet(f"{base_path}/blazar.computehosts.parquet")
            )
        ).to_facts()
    )

    blazardata = [
        BlazarAllocationRaw.validate(
            pl.scan_parquet(f"{base_path}/blazar.computehost_allocations.parquet")
        ),
        BlazarReservationRaw.validate(
            pl.scan_parquet(f"{base_path}/blazar.reservations.parquet")
        ),
        BlazarLeaseRaw.validate(pl.scan_parquet(f"{base_path}/blazar.leases.parquet")),
    ]
    all_facts.append(BlazarAllocationAdapter(*blazardata).to_facts())
    facts = pl.concat(all_facts)
    return facts
