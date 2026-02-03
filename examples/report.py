"""Generate usage reports for all sites."""

import time
from datetime import datetime

import polars as pl

from chameleon_usage.constants import ResourceTypes as RT
from chameleon_usage.ingest import clamp_hierarchy, load_intervals
from chameleon_usage.ingest.legacyusage import get_legacy_usage_counts
from chameleon_usage.pipeline import resample, run_pipeline
from chameleon_usage.schemas import PipelineSpec
from chameleon_usage.viz.plots import make_plots

SITES = ["chi_uc", "chi_tacc", "kvm_tacc"]
TIME_RANGE = (datetime(2015, 1, 1), datetime(2026, 1, 1))
BUCKET_LENGTH = "7d"
SPEC = PipelineSpec(
    group_cols=("metric", "resource", "site", "collector_type"),
    time_range=TIME_RANGE,
)


def process_current(site_name: str) -> pl.LazyFrame:
    """Process current collector data for one site."""
    t1 = time.perf_counter()
    intervals = load_intervals("data/raw_spans", site_name, TIME_RANGE)
    intervals = intervals.collect().lazy()  # checkpoint after loading data
    t2 = time.perf_counter()
    print(f"{site_name}: intervals in {t2 - t1}s")
    clamped = clamp_hierarchy(intervals)
    clamped = clamped.collect().lazy()  # checkpoint
    t3 = time.perf_counter()
    print(f"{site_name}: clamped in {t3 - t2}s")
    valid = clamped.filter(pl.col("valid")).with_columns(
        pl.lit(site_name).alias("site"),
        pl.lit("current").alias("collector_type"),
    )
    valid = valid.collect().lazy()  # checkpoint
    result = run_pipeline(valid, SPEC)
    result = result.collect().lazy()  # checkpoint
    t4 = time.perf_counter()
    print(f"{site_name}: pipeline in {t4 - t3}s")
    return result


def process_legacy(site_name: str) -> pl.LazyFrame:
    """Load legacy data for one site (already has derived metrics)."""
    return get_legacy_usage_counts("data/raw_spans", site_name, "legacy")


def main():
    # Process each site independently (avoids cross-join explosion in align_timestamps)
    results = []
    for site in SITES:
        results.append(process_current(site))
        results.append(process_legacy(site))

    # Resample after concat so both use same bucket timestamps
    combined = pl.concat(results)

    r1 = time.perf_counter()
    usage = resample(combined, BUCKET_LENGTH, SPEC)
    usage = usage.collect().lazy()
    r2 = time.perf_counter()
    print(f"Resampled in {r2 - r1}s")

    # Generate plots

    ptotal1 = time.perf_counter()

    site_resources = {
        "chi_tacc": [RT.NODE],
        "chi_uc": [RT.NODE],
        "kvm_tacc": [RT.VCPUS],
    }

    for site_name in SITES:
        for resource_type in site_resources[site_name]:
            p1 = time.perf_counter()
            subset = usage.filter(
                pl.col("site") == site_name,
                pl.col("resource") == resource_type,
                pl.col("metric").is_in(
                    [
                        "total",
                        "reservable",
                        "ondemand_capacity",
                        "committed",
                        "available_reservable",
                        "idle",
                        "occupied_reservation",
                        "occupied_ondemand",
                        "available_ondemand",
                    ]
                ),
            )
            make_plots(
                subset, "output/plots/", f"{site_name}_{resource_type}", resource_type
            )
            p2 = time.perf_counter()
            print(f"{site_name} - {resource_type} {p2 - p1}s")

    ptotal2 = time.perf_counter()
    print(f"plot total{ptotal2 - ptotal1}s")


if __name__ == "__main__":
    main()
