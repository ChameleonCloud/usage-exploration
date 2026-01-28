from datetime import datetime

import polars as pl

from chameleon_usage.adapters import BlazarComputehostAdapter, NovaComputeAdapter
from chameleon_usage.constants import Cols as C
from chameleon_usage.engine import TimelineBuilder
from chameleon_usage.models.domain import TimelineSchema, UsageSchema
from chameleon_usage.models.raw import BlazarHostRaw, NovaHostRaw


def run_demo():
    # get data

    nova_adapter = NovaComputeAdapter(
        NovaHostRaw.validate(
            pl.scan_parquet("data/raw_spans/chi_tacc/nova.compute_nodes.parquet")
        )
    )
    nova_facts = nova_adapter.to_facts()
    blazar_adapter = BlazarComputehostAdapter(
        BlazarHostRaw.validate(
            pl.scan_parquet("data/raw_spans/chi_tacc/blazar.computehosts.parquet")
        )
    )
    blazar_facts = blazar_adapter.to_facts()

    facts = pl.concat([nova_facts, blazar_facts])
    print(f"Facts: {facts.collect().shape}")
    print(facts.collect())

    # 3. Build
    engine = TimelineBuilder()
    timeline = engine.build(facts)
    print(f"Timeline: {timeline.collect().shape}")
    print(timeline.collect())

    concurrent = engine.calculate_concurrency(timeline)
    print(f"Concurrent: {concurrent.collect().shape}")
    output = concurrent.collect()
    print(output)

    fig = output.plot.line(x=C.TIMESTAMP, y=C.COUNT, color=C.QUANTITY_TYPE)
    fig.save("temp.png")


if __name__ == "__main__":
    run_demo()
