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
from chameleon_usage.plots import make_plots, source_facet_plot
from chameleon_usage.registry import ADAPTER_PRIORITY, load_facts

# used as SENTINEL for null spans, clips
# used as boundary for non-null events, filters


def counts_by_source(facts: pl.LazyFrame) -> pl.LazyFrame:
    starts = facts.filter(pl.col("value") == "active").select(
        "timestamp", "quantity_type", "source", pl.lit(1).alias("change")
    )
    ends = facts.filter(pl.col("value") == "deleted").select(
        "timestamp", "quantity_type", "source", pl.lit(-1).alias("change")
    )
    return (
        pl.concat([starts, ends])
        .group_by(["timestamp", "quantity_type", "source"])
        .agg(pl.col("change").sum())
        .sort(["source", "quantity_type", "timestamp"])
        .with_columns(
            pl.col("change").cum_sum().over(["quantity_type", "source"]).alias("count")
        )
    )


WINDOW_END = datetime(2025, 11, 1)
BUCKET_LENGTH = "30d"


def main():
    for site_name in ["chi_uc", "chi_tacc", "kvm_tacc"]:
        ########################
        # current usage pipeline
        ########################

        source_order = [s.config.source for s in ADAPTER_PRIORITY]

        engine = SegmentBuilder(site_name=site_name, priority_order=source_order)

        # input to facts list
        facts = load_facts(base_path="data/raw_spans", site_name=site_name)

        ### Debug: Contribution by source
        counts = counts_by_source(facts).collect()
        counts = counts.filter((pl.col("timestamp") <= WINDOW_END))
        source_facet_plot(counts).save(
            f"output/{site_name}_source_facet.png", scale_factor=3
        )

        # Usage:

        # facts (thing, ts) -> segments [t1,t2)
        segments = engine.build(facts)
        # cumulative sum on segments -> ts, resource, counts
        usage = engine.calculate_concurrency(segments, window_end=WINDOW_END)
        # filter out results in the future
        current_filtered = usage.filter(pl.col("timestamp") <= WINDOW_END)

        # resample to consistent time steps across all columns
        current_resampled = resample_simple(current_filtered, BUCKET_LENGTH)
        current = compute_derived_metrics(UsageSchema.validate(current_resampled))

        # legacy usage pipeline
        legacy = None
        try:
            loader = LegacyUsageLoader("data/raw_spans", site_name)
            loader.load_facts()
            legacy = (
                loader.get_usage()
                .filter(pl.col("timestamp") <= WINDOW_END)
                .pipe(resample_simple, interval=BUCKET_LENGTH)
            )
        except FileNotFoundError:
            pass

        # merge
        if legacy is not None:
            resampled = pl.concat([current, legacy], how="diagonal")
        else:
            resampled = current

        make_plots(resampled, output_path="output/plots/", site_name=site_name)


if __name__ == "__main__":
    main()
