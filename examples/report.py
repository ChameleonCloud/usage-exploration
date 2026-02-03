"""Generate usage reports for all sites."""

from datetime import datetime
from pathlib import Path

import polars as pl

from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.constants import ResourceTypes as RT
from chameleon_usage.constants import SchemaCols as S
from chameleon_usage.ingest import clamp_hierarchy, load_intervals
from chameleon_usage.ingest.legacyusage import get_legacy_usage_counts
from chameleon_usage.pipeline import (
    align_timestamps,
    clip_to_window,
    compute_derived_metrics,
    intervals_to_counts,
    resample,
)
from chameleon_usage.schemas import PipelineSpec
from chameleon_usage.viz.matplotlib_plots import (
    plot_legacy_comparison,
    plot_resource_utilization,
    plot_site_comparison,
)
from chameleon_usage.viz.plots import make_plots
from chameleon_usage.viz.prepare import (
    prepare_resource_series,
    prepare_site_comparison_series,
)

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

LEGACY_COMPARISON_SITES = {"chi_tacc", "chi_uc"}


def process_current(site_name: str) -> pl.LazyFrame:
    intervals = (
        load_intervals(f"data/raw_spans/{site_name}", TIME_RANGE).collect().lazy()
    )
    clamped = clamp_hierarchy(intervals).collect().lazy()
    valid = clamped.filter(pl.col("valid")).with_columns(
        pl.lit(site_name).alias("site")
    )
    counts = intervals_to_counts(valid, SPEC)
    counts = clip_to_window(counts, SPEC)
    aligned = align_timestamps(counts, SPEC)
    derived = compute_derived_metrics(aligned, SPEC)
    return derived.collect().lazy()


def process_legacy(site_name: str) -> pl.LazyFrame:
    return (
        get_legacy_usage_counts("data/raw_spans", site_name, "legacy").collect().lazy()
    )


def render_legacy_comparison(
    usage: pl.DataFrame, *, site_name: str, output_path: str
) -> None:
    current_total = (
        usage.filter(
            pl.col("site") == site_name,
            pl.col(S.RESOURCE) == RT.NODE,
            pl.col("collector_type") == "current",
            pl.col(S.METRIC) == QT.TOTAL,
        )
        .sort(S.TIMESTAMP)
        .select(S.TIMESTAMP, S.VALUE)
    )
    current_reservable = (
        usage.filter(
            pl.col("site") == site_name,
            pl.col(S.RESOURCE) == RT.NODE,
            pl.col("collector_type") == "current",
            pl.col(S.METRIC) == QT.RESERVABLE,
        )
        .sort(S.TIMESTAMP)
        .select(S.TIMESTAMP, S.VALUE)
    )
    legacy_reservable = (
        usage.filter(
            pl.col("site") == site_name,
            pl.col(S.RESOURCE) == RT.NODE,
            pl.col("collector_type") == "legacy",
            pl.col(S.METRIC) == QT.RESERVABLE,
        )
        .sort(S.TIMESTAMP)
        .select(S.TIMESTAMP, S.VALUE)
    )
    if (
        current_total.is_empty()
        or current_reservable.is_empty()
        or legacy_reservable.is_empty()
    ):
        return

    plot_legacy_comparison(
        current_timestamps=current_total.get_column(S.TIMESTAMP).to_list(),
        current_total=current_total.get_column(S.VALUE).to_list(),
        current_reservable=current_reservable.get_column(S.VALUE).to_list(),
        legacy_timestamps=legacy_reservable.get_column(S.TIMESTAMP).to_list(),
        legacy_reservable=legacy_reservable.get_column(S.VALUE).to_list(),
        title=f"{site_name} - nodes",
        output_path=output_path,
    )


def main():
    results = []
    for site in SITES:
        results.append(process_current(site))
        results.append(process_legacy(site))

    combined = pl.concat(results).lazy()
    usage = resample(combined, BUCKET_LENGTH, SPEC).collect()
    usage_lazy = usage.lazy()

    for site_name in SITES:
        for resource_type in SITE_RESOURCES[site_name]:
            subset = usage_lazy.filter(
                pl.col("site") == site_name,
                pl.col("resource") == resource_type,
                pl.col("metric").is_in(PLOT_METRICS),
            )
            make_plots(
                subset, "output/plots/", f"{site_name}_{resource_type}", resource_type
            )

    output_dir = Path("output/plots_matplotlib")
    output_dir.mkdir(parents=True, exist_ok=True)

    for site_name in SITES:
        for resource_type in SITE_RESOURCES[site_name]:
            series = prepare_resource_series(
                usage, site=site_name, resource=resource_type
            )
            if not series.timestamps:
                continue
            plot_resource_utilization(
                series,
                title=f"{site_name} - {resource_type}",
                y_label=resource_type,
                output_path=str(output_dir / f"{site_name}_{resource_type}_util.png"),
            )

    sites_for_nodes = [site for site in SITES if RT.NODE in SITE_RESOURCES[site]]
    site_series = prepare_site_comparison_series(
        usage, sites=sites_for_nodes, resource=RT.NODE
    )
    if site_series:
        plot_site_comparison(
            site_series,
            occupied_label="Used",
            output_path=str(output_dir / "sites_nodes.png"),
        )

    for site_name in sorted(LEGACY_COMPARISON_SITES):
        render_legacy_comparison(
            usage,
            site_name=site_name,
            output_path=str(output_dir / f"{site_name}_reservable_compare.png"),
        )


if __name__ == "__main__":
    main()
