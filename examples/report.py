"""Generate usage reports for all sites."""

from datetime import datetime

import polars as pl

from chameleon_usage.ingest import load_intervals
from chameleon_usage.pipeline import (
    intervals_to_counts,
    resample,
    compute_derived_metrics,
    add_site_context,
)
from chameleon_usage.viz.plots import make_plots

WINDOW_END = datetime(2025, 11, 1)
BUCKET_LENGTH = "7d"


def main():
    for site_name in ["chi_uc", "chi_tacc", "kvm_tacc"]:
        # Load intervals from raw data
        intervals = load_intervals("data/raw_spans", site_name)

        # Pipeline: intervals → counts → resample → derived metrics
        usage = (
            intervals_to_counts(intervals)
            .filter(pl.col("timestamp") <= WINDOW_END)
            .pipe(resample, BUCKET_LENGTH)
            .pipe(compute_derived_metrics)
            .pipe(add_site_context, site_name)
        )

        # Filter to base metrics for plotting
        usage_filtered = usage.filter(
            pl.col("quantity_type").is_in([
                "total", "reservable", "committed", "occupied"
            ])
        )

        print(f"\n{site_name}:")
        print(usage_filtered.collect())

        make_plots(usage_filtered, output_path="output/plots/", site_name=site_name)


if __name__ == "__main__":
    main()
