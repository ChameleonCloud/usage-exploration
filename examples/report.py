"""Generate usage reports for all sites."""

from datetime import datetime

import polars as pl

from chameleon_usage.ingest import load_intervals
from chameleon_usage.pipeline import (
    add_site_context,
    compute_derived_metrics,
    intervals_to_counts,
    resample,
)
from chameleon_usage.viz.plots import make_plots

WINDOW_END = datetime(2025, 11, 1)
BUCKET_LENGTH = "7d"


def inspect_intervals(intervals: pl.LazyFrame, site_name: str) -> None:
    """Print unique value counts per column, grouped by site and quantity_type."""
    df = intervals.collect()

    print(f"\n{'=' * 60}")
    print(f"INTERVALS: {site_name}")
    print(f"{'=' * 60}")

    for qt in df["quantity_type"].unique().sort().to_list():
        subset = df.filter(pl.col("quantity_type") == qt)
        print(f"\n  {qt}: {subset.height} rows")

        for col in sorted(df.columns):
            if col in ["start", "end"]:
                continue
            n_unique = subset[col].n_unique()
            n_null = subset[col].null_count()
            print(f"    {col}: {n_unique} unique, {n_null} null")


def main():
    # for site_name in ["chi_uc", "chi_tacc", "kvm_tacc"]:
    for site_name in [
        "chi_tacc",
    ]:
        # Stage 1: Load intervals (pre-cumsum)
        intervals = load_intervals("data/raw_spans", site_name)
        # inspect_intervals(intervals, site_name)

        # Stage 2: Pipeline - intervals → counts → resample → derived metrics
        usage = (
            intervals_to_counts(intervals)
            .filter(pl.col("timestamp") <= WINDOW_END)
            .pipe(resample, BUCKET_LENGTH)
            .pipe(compute_derived_metrics)
            .pipe(add_site_context, site_name)
        )

        print(f"\n{site_name}:")
        print(usage.collect())

        make_plots(usage, output_path="output/plots/", site_name=site_name)


if __name__ == "__main__":
    main()
