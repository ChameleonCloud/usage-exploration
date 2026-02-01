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

TIME_RANGE = (datetime(2010, 1, 1), datetime(2025, 11, 1))
BUCKET_LENGTH = "1d"


def main():
    spec = PipelineSpec(group_cols=("quantity_type",), time_range=TIME_RANGE)

    # for site_name in ["chi_uc", "chi_tacc", "kvm_tacc"]:
    for site_name in ["chi_tacc"]:
        # Stage 1: Load raw intervals (filtered to time range)
        # TODO this should take PipelineSpec
        raw_intervals = load_intervals("data/raw_spans", site_name, TIME_RANGE)

        # Stage 2: Hierarchical clamping (total → reservable → committed → occupied)
        # clamps where child overlaps parent, filters otherwise
        clamped = clamp_hierarchy(raw_intervals)

        # DEBUG timing
        cache_clamped = clamped.collect()
        audit_intervals = cache_clamped.filter(~pl.col("valid"))
        print(audit_intervals)
        valid_intervals = cache_clamped.filter(pl.col("valid"))
        print(valid_intervals)

        # ALSO DEBUG
        valid_intervals = valid_intervals.lazy()

        # Stage 3: Pipeline (sweepline → align → derived → resample)
        print("Starting intervals_to_counts...")
        counts = intervals_to_counts(valid_intervals, spec)
        print(f"After sweepline: {counts.select(pl.len()).collect()}")

        print("Starting clip_to_window...")
        counts = clip_to_window(counts, spec)
        print(f"After clip: {counts.select(pl.len()).collect()}")

        print("Starting align_timestamps...")
        aligned = align_timestamps(counts, spec)
        print(f"After align: {aligned.select(pl.len()).collect()}")

        print("Starting compute_derived_metrics...")
        derived = compute_derived_metrics(aligned, spec)
        print(f"After derived: {derived.select(pl.len()).collect()}")

        print("Starting resample...")
        usage = resample(derived, BUCKET_LENGTH, spec)
        print(f"After resample: {usage.select(pl.len()).collect()}")

        usage_with_context = add_site_context(usage, spec, site_name)

        make_plots(usage_with_context, output_path="output/plots/", site_name=site_name)


if __name__ == "__main__":
    main()
