import argparse
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl
from chameleon_usage.common import SiteConfig, load_sites_yaml, merge_pipelines

from chameleon_usage import audit, math
from chameleon_usage.data_import.dump_db import dump_site_to_parquet
from chameleon_usage.pipeline import UsagePipeline


def _spans_to_daily_wide(
    spans: pl.DataFrame,
    window_start: datetime,
    window_end: datetime,
    group_cols: list[str],
    every: str = "1d",
) -> pl.DataFrame:
    """spans → filter → events → sweepline → clip → resample → wide"""
    lf = spans.lazy()

    # Create composite series column for pivot
    if len(group_cols) > 1:
        lf = lf.with_columns(pl.concat_str(group_cols, separator="_").alias("series"))
        series_col = "series"
    else:
        series_col = group_cols[0]

    lf = math.filter_overlapping(lf, "start", "end", window_start, window_end)
    events = math.spans_to_events(lf, group_cols=[series_col])
    timeline = math.sweepline(events)
    return math.sweepline_to_wide(
        timeline,
        series_col=series_col,
        every=every,
        window_start=window_start,
        window_end=window_end,
    )


def _resample_legacy_counts(
    legacy_df: pl.DataFrame,
    site_name: str,
    every: str,
) -> pl.DataFrame | None:
    """Resample legacy node counts to same interval as new data.

    Legacy data has daily counts per node_type. We:
    1. Sum across node_types to get daily total
    2. Resample using mean (for daily data, mean == time-weighted average)
    """
    legacy_site = legacy_df.filter(pl.col("site") == site_name)
    if legacy_site.height == 0:
        return None

    # Sum across node_types per day
    daily_totals = (
        legacy_site.lazy()
        .group_by("date")
        .agg(pl.col("cnt").sum().alias("legacy_total"))
    )
    # Resample using mean
    return math.resample_mean(
        daily_totals, time_col="date", value_col="legacy_total", every=every
    ).collect()


def _plot_site(
    daily_wide: pl.DataFrame,
    legacy_df: pl.DataFrame | None,
    site_name: str,
    plot_dir: Path,
    every: str = "30d",
) -> None:
    """Plot new data with legacy overlay for comparison."""
    site_cols = [
        c
        for c in daily_wide.columns
        if c.startswith(f"{site_name}_") or c == "timestamp"
    ]
    if len(site_cols) <= 1:
        return

    fig, ax = plt.subplots(figsize=(12, 6))

    # Plot new data (solid lines)
    site_df = (
        daily_wide.select(site_cols).to_pandas().set_index("timestamp").sort_index()
    )
    site_df.plot(ax=ax, title=f"{site_name}: new (solid) vs legacy (dashed)")

    # Overlay legacy data if available (dashed lines)
    if legacy_df is not None:
        legacy_resampled = _resample_legacy_counts(legacy_df, site_name, every)
        if legacy_resampled is not None:
            legacy_pd = legacy_resampled.to_pandas().set_index("date")
            legacy_pd.plot(ax=ax, linestyle="--", alpha=0.7)

    plt.tight_layout()
    plt.savefig(plot_dir / f"{site_name}_daily_sources.png", dpi=150)
    plt.close()


def load_site(site: SiteConfig):
    print(f"Processing {site.site_name}")

    print("Syncing DB to parquet cache...")
    result = dump_site_to_parquet(site)

    # TODO! output if debug true, or if something not "skipped" or "null"
    # print(result)

    print("\nLoading pipeline...")
    pipeline = UsagePipeline(site)
    pipelineresult = pipeline.compute_spans()

    return pipelineresult


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", "-o", type=Path, default=Path("output"))
    parser.add_argument("--sites-config", type=Path, default=Path("etc/sites.yaml"))
    parser.add_argument("--raw-spans", type=Path, default=None)  # None = use yaml value
    return parser.parse_args()


def main():
    args = parse_args()

    sites = load_sites_yaml(args.sites_config)

    # Override raw_spans path if provided
    if args.raw_spans:
        for name, site in sites.items():
            site.raw_spans = str(args.raw_spans / name)

    output_dir = args.output
    output_dir.mkdir(parents=True, exist_ok=True)
    plot_dir = output_dir / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    # Load, audit, and collect data from each site
    pipeline_results = {}
    for site_name, site_config in sites.items():
        pipeline_results[site_name] = load_site(site_config)

    merged = merge_pipelines(pipeline_results)

    # Run audit checks and output report
    audit_result = audit.run_audit_checks(merged)

    audit_results_dir = output_dir / "audit"
    audit_results_dir.mkdir(parents=True, exist_ok=True)
    audit_result.write_csv(file=audit_results_dir / "report.csv")
    print(audit_result)

    window_start = datetime(2010, 1, 1)
    window_end = datetime(2025, 1, 1)
    daily_wide = _spans_to_daily_wide(
        merged.valid_spans.collect(),
        window_start=window_start,
        window_end=window_end,
        group_cols=["site", "source"],  # ensure grouping by both site and span source
        every="30d",
    )

    for site_name in sites.keys():
        _plot_site(daily_wide, None, site_name, plot_dir)
        print(f"saved {site_name} plots to: {plot_dir}")


if __name__ == "__main__":
    main()
