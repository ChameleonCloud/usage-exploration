from datetime import datetime

import polars as pl

from chameleon_usage.adapters import NovaComputeAdapter
from chameleon_usage.engine import TimelineBuilder
from chameleon_usage.models.domain import TimelineSchema, UsageSchema
from chameleon_usage.models.raw import NovaHostRaw


def run_demo():
    # get data
    raw_nodes = pl.scan_parquet("data/raw_spans/chi_tacc/nova.compute_nodes.parquet")
    validated = NovaHostRaw.validate(raw_nodes)

    # 2. Adapt
    adapter = NovaComputeAdapter(validated.lazy())
    facts = adapter.to_facts()

    print(f"Facts: {facts.collect().shape}")
    print(facts.collect().head())

    # 3. Build
    engine = TimelineBuilder()
    timeline = engine.build(facts)

    print(f"Timeline: {timeline.collect().shape}")

    concurrent = engine.calculate_concurrency(timeline)

    print(f"Concurrent: {concurrent.collect().shape}")

    output = concurrent.collect()
    print(output)

    fig = output.plot.line(x=UsageSchema.timestamp, y=UsageSchema.total_quantity)
    fig.save("temp.png")


if __name__ == "__main__":
    run_demo()
