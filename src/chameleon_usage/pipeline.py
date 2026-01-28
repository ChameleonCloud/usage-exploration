from datetime import datetime

import polars as pl

from chameleon_usage.adapters import NovaComputeAdapter
from chameleon_usage.engine import SegmentBuilder
from chameleon_usage.models.raw import NovaHostRaw


def run_demo():
    # get data
    raw_nodes = pl.scan_parquet("data/raw_spans/chi_tacc/nova.compute_nodes.parquet")
    validated = NovaHostRaw.validate(raw_nodes)

    # 2. Adapt
    adapter = NovaComputeAdapter(validated.lazy())
    facts = adapter.to_facts()

    # 3. Build
    engine = SegmentBuilder()
    result = engine.build(facts)

    print(result.collect())


if __name__ == "__main__":
    run_demo()
