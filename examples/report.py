"""Generate usage reports for all sites."""

from datetime import datetime

import polars as pl

from chameleon_usage.ingest import clamp_hierarchy, load_intervals
from chameleon_usage.ingest.legacyusage import get_legacy_usage_counts
from chameleon_usage.pipeline import (
    add_site_context,
    align_timestamps,
    clip_to_window,
    compute_derived_metrics,
    intervals_to_counts,
    resample,
)
from chameleon_usage.schemas import PipelineSpec, UsageSchema
from chameleon_usage.viz.plots import make_plots

TIME_RANGE = (datetime(2020, 1, 1), datetime(2026, 1, 1))
BUCKET_LENGTH = "30d"


def main():
    spec = PipelineSpec(
        group_cols=("quantity_type", "site", "collector_type"), time_range=TIME_RANGE
    )

    all_intervals = []
    all_legacy_counts = []
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
        # load legacy usage for comparison, format is hours per day
        legacy_counts = get_legacy_usage_counts(
            base_path="data/raw_spans", site_name=site_name, collector_type="legacy"
        )
        all_legacy_counts.append(legacy_counts)

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
    usage_with_derived = compute_derived_metrics(aligned, spec)

    # legacy counts are already time-aligned and have derived metrics, just resample to match.
    # TODO: got real messy
    valid_current_counts = UsageSchema.validate(usage_with_derived, lazy=True)
    all_legacy_counts = pl.concat(all_legacy_counts)
    valid_legacy_counts = UsageSchema.validate(all_legacy_counts, lazy=True)
    all_counts = pl.concat(
        [valid_current_counts, valid_legacy_counts],
        how="diagonal",
    )

    usage = resample(all_counts, BUCKET_LENGTH, spec)

    ########################################
    # Legacy usage for validation/comparison
    ########################################

    print(all_legacy_counts.collect())

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
