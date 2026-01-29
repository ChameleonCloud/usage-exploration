import polars as pl

from chameleon_usage.adapters import (
    BlazarAllocationAdapter,
    BlazarComputehostAdapter,
    NovaComputeAdapter,
    NovaInstanceAdapter,
)
from chameleon_usage.constants import Cols as C
from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.engine import SegmentBuilder
from chameleon_usage.legacyusage import LegacyUsageLoader
from chameleon_usage.models.raw import (
    BlazarAllocationRaw,
    BlazarHostRaw,
    BlazarLeaseRaw,
    BlazarReservationRaw,
    NovaHostRaw,
    NovaInstanceRaw,
)
from chameleon_usage.plots import make_plots


def compute_derived_metrics(df: pl.LazyFrame) -> pl.LazyFrame:
    """Compute available and idle from base metrics.

    available = reservable - committed
    idle = committed - used (only if used exists)
    """
    index_cols = [C.TIMESTAMP, "collector_type"]

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
    )

    return unpivoted


def load_facts(input_data: str, site_name: str):
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


def main():
    # get data

    for site_name in ["chi_uc", "chi_tacc", "kvm_tacc"]:
        # load data, convert to facts timeline
        facts_list = load_facts(input_data="data/raw_spans", site_name=site_name)

        usage_loader = LegacyUsageLoader("data/raw_spans", site_name)

        legacy_usage_series = None
        try:
            usage_loader.load_facts()
            legacy_usage_series = usage_loader.get_usage()
        except FileNotFoundError:
            pass

        # process facts, convert to state timeline
        engine = SegmentBuilder(site_name=site_name, priority_order=[])
        segments = engine.build(facts_list)

        # process state timeline into usage timeserices
        usage_timeseries = engine.calculate_concurrency(segments)

        print(
            usage_timeseries.collect().group_by("collector_type", "quantity_type").len()
        )

        # reservable_ts = usage_timeseries.filter(
        #     pl.col("quantity_type") == "reservable"
        # ).collect()
        # print(reservable_ts.select(C.TIMESTAMP).describe())
        # print(
        #     reservable_ts.select(pl.col(C.TIMESTAMP).dt.truncate("7d")).unique().height
        # )

        if legacy_usage_series is not None:
            both_usage = pl.concat([usage_timeseries, legacy_usage_series])
        else:
            both_usage = usage_timeseries

        # # resample timeseries for plotting
        # resampled_usage = engine.resample_time_weighted(both_usage, interval="1d")
        # print(
        #     resampled_usage.collect().group_by("collector_type", "quantity_type").len()
        # )

        # resampled_derived = compute_derived_metrics(resampled_usage)
        # make_plots(resampled_derived, output_path="output/plots/", site_name=site_name)


if __name__ == "__main__":
    main()
