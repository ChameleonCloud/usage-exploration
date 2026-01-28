from datetime import datetime

import altair as alt
import polars as pl

from chameleon_usage.adapters import (
    BlazarAllocationAdapter,
    BlazarComputehostAdapter,
    NovaComputeAdapter,
)
from chameleon_usage.constants import Cols as C
from chameleon_usage.engine import TimelineBuilder
from chameleon_usage.models.raw import (
    BlazarAllocationRaw,
    BlazarHostRaw,
    BlazarLeaseRaw,
    BlazarReservationRaw,
    NovaHostRaw,
    NovaInstanceRaw,
)


def run_demo():
    # get data

    all_facts = []

    all_facts.append(
        NovaComputeAdapter(
            NovaHostRaw.validate(
                pl.scan_parquet("data/raw_spans/chi_tacc/nova.compute_nodes.parquet")
            )
        ).to_facts()
    )
    # all_facts.append(
    #     NovaComputeAdapter(
    #         NovaHostRaw.validate(
    #             pl.scan_parquet("data/raw_spans/chi_tacc/nova.compute_nodes.parquet")
    #         )
    #     ).to_facts()
    # )
    all_facts.append(
        BlazarComputehostAdapter(
            BlazarHostRaw.validate(
                pl.scan_parquet("data/raw_spans/chi_tacc/blazar.computehosts.parquet")
            )
        ).to_facts()
    )

    blazardata = [
        BlazarAllocationRaw.validate(
            pl.scan_parquet(
                "data/raw_spans/chi_tacc/blazar.computehost_allocations.parquet"
            )
        ),
        BlazarReservationRaw.validate(
            pl.scan_parquet("data/raw_spans/chi_tacc/blazar.reservations.parquet")
        ),
        BlazarLeaseRaw.validate(
            pl.scan_parquet("data/raw_spans/chi_tacc/blazar.leases.parquet")
        ),
    ]

    print(blazardata[2].collect().describe())

    all_facts.append(BlazarAllocationAdapter(*blazardata).to_facts())

    facts = pl.concat(all_facts)
    print(f"Facts: {facts.collect().shape}")
    print(facts.collect())

    # 3. Build
    engine = TimelineBuilder()
    timeline = engine.build(facts)
    print(f"Timeline: {timeline.collect().shape}")
    print(timeline.collect())

    concurrent = engine.calculate_concurrency(timeline)
    print(f"Concurrent: {concurrent.collect().shape}")
    print(concurrent.collect())

    resampled = engine.resample_time_weighted(concurrent, interval="30d")
    print(f"Resampled: {resampled.collect().shape}")
    print(resampled.collect())

    plottable = resampled.collect()
    fig = (
        plottable.plot.line(
            x=C.TIMESTAMP,
            y=C.COUNT,
            color=C.QUANTITY_TYPE,
        )
        .encode(
            x=alt.X(C.TIMESTAMP, scale=alt.Scale(domain=["2015-01-01", "2025-10-01"])),
            # y=alt.Y(C.COUNT, scale=alt.Scale(domain=[-100, 1000])),
        )
        .properties(width=900, height=600)
    )
    fig.save("temp.png")


if __name__ == "__main__":
    run_demo()
