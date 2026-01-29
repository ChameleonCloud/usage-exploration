"""Generate usage reports for all sites."""

from datetime import datetime

import polars as pl

from chameleon_usage.engine import SegmentBuilder
from chameleon_usage.legacyusage import LegacyUsageLoader
from chameleon_usage.models.domain import UsageSchema
from chameleon_usage.pipeline import (
    compute_derived_metrics,
    resample_simple,
)
from chameleon_usage.plots import make_plots
from chameleon_usage.registry import ADAPTER_PRIORITY, load_facts

# used as SENTINEL for null spans, clips
# used as boundary for non-null events, filters
WINDOW_END = datetime(2025, 11, 1)


def main():
    for site_name in ["chi_uc", "chi_tacc", "kvm_tacc"]:
        ########################
        # current usage pipeline
        ########################

        source_order = [s.config.source for s in ADAPTER_PRIORITY]
        print(f"Source Order!: {source_order}")
        engine = SegmentBuilder(site_name=site_name, priority_order=source_order)

        # input to facts list
        facts = load_facts(base_path="data/raw_spans", site_name=site_name)
        print(
            facts.collect()
            .group_by(
                [
                    "entity_id",
                    "quantity_type",
                    "source",
                ]
            )
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
        )

        # facts (thing, ts) -> segments [t1,t2)
        segments = engine.build(facts)
        # cumulative sum on segments -> ts, resource, counts
        usage = engine.calculate_concurrency(segments, window_end=WINDOW_END)
        # filter out results in the future
        current_filtered = usage.filter(pl.col("timestamp") <= WINDOW_END)

        # resample to consistent time steps across all columns
        current_resampled = resample_simple(current_filtered, interval="90d")
        current = compute_derived_metrics(UsageSchema.validate(current_resampled))

        # legacy usage pipeline
        legacy = None
        try:
            loader = LegacyUsageLoader("data/raw_spans", site_name)
            loader.load_facts()
            legacy = (
                loader.get_usage()
                .filter(pl.col("timestamp") <= WINDOW_END)
                .pipe(resample_simple, interval="30d")
            )
        except FileNotFoundError:
            pass

        # merge
        if legacy is not None:
            resampled = pl.concat([current, legacy], how="diagonal")
        else:
            resampled = current

        print(
            resampled.collect()
            .group_by(["collector_type", "quantity_type"])
            .agg(pl.col("count").count().alias("n_rows"))
        )

        make_plots(resampled, output_path="output/plots/", site_name=site_name)


if __name__ == "__main__":
    main()
