"""Generate usage reports for all sites."""

from datetime import datetime

import polars as pl

from chameleon_usage.constants import ResourceTypes as RT
from chameleon_usage.ingest import clamp_hierarchy, load_intervals
from chameleon_usage.ingest.legacyusage import get_legacy_usage_counts
from chameleon_usage.pipeline import resample, run_pipeline
from chameleon_usage.schemas import PipelineSpec
from chameleon_usage.viz.plots import make_plots

TIME_RANGE = (datetime(2022, 1, 1), datetime(2026, 1, 1))
BUCKET_LENGTH = "30d"
SPEC = PipelineSpec(
    group_cols=("metric", "resource", "site", "collector_type"),
    time_range=TIME_RANGE,
)

SITE_RESOURCES = {
    "chi_tacc": [RT.NODE],
    "chi_uc": [RT.NODE],
    "kvm_tacc": [RT.NODE, RT.VCPUS],
}
SITES = list(SITE_RESOURCES.keys())

PLOT_METRICS = [
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


def process_current(site_name: str) -> pl.LazyFrame:
    intervals = (
        load_intervals(f"data/raw_spans/{site_name}", TIME_RANGE).collect().lazy()
    )
    clamped = clamp_hierarchy(intervals).collect().lazy()
    valid = clamped.filter(pl.col("valid")).with_columns(
        pl.lit(site_name).alias("site")
    )
    return run_pipeline(valid, SPEC).collect().lazy()


def process_legacy(site_name: str) -> pl.LazyFrame:
    return (
        get_legacy_usage_counts("data/raw_spans", site_name, "legacy").collect().lazy()
    )


def main():
    results = []
    for site in SITES:
        results.append(process_current(site))
        results.append(process_legacy(site))

    combined = pl.concat(results).lazy()
    usage = resample(combined, BUCKET_LENGTH, SPEC).collect().lazy()

    for site_name in SITES:
        for resource_type in SITE_RESOURCES[site_name]:
            subset = usage.filter(
                pl.col("site") == site_name,
                pl.col("resource") == resource_type,
                pl.col("metric").is_in(PLOT_METRICS),
            )
            make_plots(
                subset, "output/plots/", f"{site_name}_{resource_type}", resource_type
            )


if __name__ == "__main__":
    main()
