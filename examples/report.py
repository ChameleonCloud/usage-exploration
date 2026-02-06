"""Generate usage reports for all sites."""

import logging
from datetime import datetime

import polars as pl

from chameleon_usage.constants import CollectorTypes as CT
from chameleon_usage.constants import ResourceTypes as RT
from chameleon_usage.exceptions import RawTableLoadError, log_raw_table_load_error
from chameleon_usage.ingest import clamp_hierarchy, load_intervals
from chameleon_usage.ingest.legacyusage import get_legacy_usage_counts
from chameleon_usage.pipeline import resample, run_pipeline, to_wide
from chameleon_usage.schemas import PipelineSpec
from chameleon_usage.viz.plots import PlotAnnotation
from chameleon_usage.viz.prepare import (
    plot_collector_comparison,
    plot_site_comparison,
    plot_stacked_usage,
)

logger = logging.getLogger(__name__)


def _process_new_collector(site_name, pipeline_spec: PipelineSpec):
    time_range = pipeline_spec.time_range

    path = f"s3://usage_new_collector/{site_name}"
    # path = f"data/current/{site_name}"

    intervals = load_intervals(path, time_range).collect().lazy()
    preprocessed = clamp_hierarchy(intervals).collect().lazy()
    filtered = preprocessed.filter(pl.col("valid")).with_columns(
        pl.lit(site_name).alias("site"),
        pl.lit(CT.NEWCOLLECTOR).alias("collector_type"),
    )
    new_collector_results = run_pipeline(filtered, pipeline_spec)
    return new_collector_results


def _process_current_collector(site_name, pipeline_spec: PipelineSpec):
    path = f"data/current/{site_name}"
    existing_collector_results = get_legacy_usage_counts(path).with_columns(
        pl.lit(site_name).alias("site"),
        pl.lit("legacy").alias("collector_type"),
    )
    return existing_collector_results


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    time_range = (datetime(2021, 1, 1), datetime(2025, 11, 1))

    default_spec = PipelineSpec(
        group_cols=("metric", "resource", "site", "collector_type"),
        time_range=time_range,
    )

    bucket_length = "1d"

    sites_to_plot = ["chi_uc", "chi_tacc", "kvm_tacc"]

    # Load and process data
    results = []

    # for each site, load intervals, compute usage timeline, and combine
    for site in sites_to_plot:
        try:
            results.append(_process_new_collector(site, default_spec))
        except RawTableLoadError as exc:
            log_raw_table_load_error(logger, site, exc)
            exit(1)
        results.append(_process_current_collector(site, default_spec))

    if not results:
        logger.error("No data loaded; exiting.")
        return
    combined = pl.concat(results).lazy()

    # resample results to align timestamps and reduce length
    usage = resample(combined, bucket_length, default_spec).collect()

    # Pivot to wide for consumption by matplotlib
    wide = to_wide(usage, pivot_cols=["metric", "collector_type"])

    # CHI sites: exclude on-demand (not applicable)
    plot_stacked_usage(
        wide,
        "chi_uc",
        RT.NODE,
        "output/plots",
        include_ondemand=False,
        title="CHI@UC Node Usage over Time",
        y_label="Nodes",
        time_range=time_range,
        bucket=bucket_length,
    )
    plot_stacked_usage(
        wide,
        "chi_tacc",
        RT.NODE,
        "output/plots",
        include_ondemand=False,
        title="CHI@TACC Node Usage over Time",
        y_label="Nodes",
        time_range=time_range,
        bucket=bucket_length,
    )
    # KVM: include on-demand
    plot_stacked_usage(
        wide,
        "kvm_tacc",
        RT.NODE,
        "output/plots",
        title="KVM@TACC Node Usage over Time",
        y_label="Nodes (Normalized)",
        time_range=time_range,
        bucket=bucket_length,
    )
    plot_stacked_usage(
        wide,
        "kvm_tacc",
        RT.VCPUS,
        "output/plots",
        time_range=time_range,
        bucket=bucket_length,
    )
    plot_site_comparison(
        wide,
        sites_to_plot,
        RT.NODE,
        "output/plots",
        time_range=time_range,
        bucket=bucket_length,
        annotations=[
            PlotAnnotation(
                "UC Phase3",
                datetime(2022, 2, 10),
                datetime(2022, 1, 10),
                120,
                tip_offset=15,
            ),
            PlotAnnotation(
                "KVM H100",
                datetime(2025, 9, 1),
                datetime(2024, 12, 1),
                90,
                tip_offset=15,
            ),
        ],
    )

    # compare collection types to identify gaps
    plot_collector_comparison(
        wide,
        "chi_uc",
        RT.NODE,
        "output/plots",
        time_range=time_range,
        bucket=bucket_length,
    )
    plot_collector_comparison(
        wide,
        "chi_tacc",
        RT.NODE,
        "output/plots",
        time_range=time_range,
        bucket=bucket_length,
    )


if __name__ == "__main__":
    main()
