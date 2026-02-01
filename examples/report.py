"""Generate usage reports for all sites."""

from datetime import datetime

import polars as pl

from chameleon_usage.ingest import clamp_hierarchy, load_intervals
from chameleon_usage.pipeline import (
    add_site_context,
    align_timestamps,
    clip_to_window,
    compute_derived_metrics,
    intervals_to_counts,
    resample,
)
from chameleon_usage.schemas import PipelineSpec
from chameleon_usage.viz.plots import make_plots

TIME_RANGE = (datetime(2016, 1, 1), datetime(2025, 1, 1))
BUCKET_LENGTH = "30d"


def main():
    spec = PipelineSpec(
        group_cols=("quantity_type", "site", "collector_type"), time_range=TIME_RANGE
    )

    all_intervals = []
    SITE_NAMES = ["chi_uc", "chi_tacc", "kvm_tacc"]

    for site_name in SITE_NAMES:
        # Stage 1: Load raw intervals (filtered to time range)
        # TODO this should take PipelineSpec
        raw_intervals = load_intervals("data/raw_spans", site_name, TIME_RANGE)
        # Stage 2: Hierarchical clamping (total → reservable → committed → occupied)
        # clamps where child overlaps parent, filters otherwise
        clamped_intervals = clamp_hierarchy(raw_intervals)
        all_intervals.append(
            clamped_intervals.with_columns(
                pl.lit(site_name).alias("site"),
                pl.lit("current").alias("collector_type"),
            )
        )

    clamped = pl.concat(all_intervals)

    # DEBUG timing
    cache_clamped = clamped.collect()
    audit_intervals = cache_clamped.filter(~pl.col("valid"))
    print(
        audit_intervals.group_by(
            "site",
            "quantity_type",
            "coerce_action",
        )
        .len()
        .sort(by=["site", "quantity_type", "len"])
    )
    valid_intervals = cache_clamped.filter(pl.col("valid"))

    # ALSO DEBUG
    valid_intervals = valid_intervals.lazy()
    # Stage 3: Pipeline (sweepline → align → derived → resample)
    counts = intervals_to_counts(valid_intervals, spec)
    time_clipped_counts = clip_to_window(counts, spec)
    aligned = align_timestamps(time_clipped_counts, spec)
    derived = compute_derived_metrics(aligned, spec)
    usage = resample(derived, BUCKET_LENGTH, spec)

    for site_name in SITE_NAMES:
        subset = usage.filter(
            pl.col("site").eq(site_name),
            pl.col("quantity_type").is_in(
                ["total", "reservable", "available", "idle", "occupied"],
            ),
        )
        make_plots(subset, output_path="output/plots/", site_name=site_name)

    # usage_with_context = add_site_context(usage, spec, site_name)


if __name__ == "__main__":
    main()
