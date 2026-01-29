import polars as pl

from chameleon_usage.adapters import (
    BlazarAllocationAdapter,
    BlazarComputehostAdapter,
    NovaComputeAdapter,
    NovaInstanceAdapter,
)
from chameleon_usage.constants import Cols as C
from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.engine import TimelineBuilder
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
    # long to wide
    pivoted = df.collect().pivot(on=C.QUANTITY_TYPE, index=C.TIMESTAMP, values=C.COUNT)

    # simple subtraction
    pivoted = pivoted.with_columns(
        (pl.col(QT.RESERVABLE) - pl.col(QT.COMMITTED)).alias(QT.AVAILABLE),
        (pl.col(QT.COMMITTED) - pl.col(QT.OCCUPIED)).alias(QT.IDLE),
    )

    # wide to long
    unpivoted = (
        pivoted.unpivot(
            index=C.TIMESTAMP, variable_name=C.QUANTITY_TYPE, value_name=C.COUNT
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

        # process facts, convert to state timeline
        engine = TimelineBuilder()
        state_timeline = engine.build(facts_list)

        # process state timeline into usage timeserices
        usage_timeseries = engine.calculate_concurrency(state_timeline)

        # resample timeseries for plotting
        resampled_usage = engine.resample_time_weighted(usage_timeseries, interval="7d")
        resampled_derived = compute_derived_metrics(resampled_usage)
        make_plots(resampled_derived, output_path="output/plots/", site_name=site_name)


if __name__ == "__main__":
    main()
