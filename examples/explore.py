import argparse
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl

from chameleon_usage import math, utils
from chameleon_usage.dump_db import dump_site_to_parquet
from chameleon_usage.pipeline import UsagePipeline


def _format_audit_summary(audit_df: pl.DataFrame) -> None:
    for site in audit_df["site"].unique().sort().to_list():
        site_df = audit_df.filter(pl.col("site") == site)
        total = site_df.height
        summary = (
            site_df.with_columns(year=pl.col("start_date").dt.year())
            .group_by("source", "data_status", "year")
            .len()
            .with_columns((pl.col("len") / total * 100).round(1).alias("pct"))
            .sort("source", "data_status", "year")
        )
        print(f"\n=== {site} ({total:,} rejected) ===")
        print(summary)


def _emit_audit(audit_df: pl.DataFrame, output_dir: Path) -> None:
    print(f"\n=== AUDIT: {audit_df.shape[0]} rejected rows ===")
    if audit_df.shape[0] == 0:
        return
    _format_audit_summary(audit_df)
    audit_path = output_dir / "audit.parquet"
    audit_df.write_parquet(audit_path)
    print(f"wrote: {audit_path}")


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
    timeline = math.clip_timeline(timeline, "timestamp", window_start, window_end)
    return math.sweepline_to_wide(timeline, series_col=series_col, every=every)


def _plot_site(daily_wide: pl.DataFrame, site_name: str, plot_dir: Path) -> None:
    site_cols = [
        c
        for c in daily_wide.columns
        if c.startswith(f"{site_name}_") or c == "timestamp"
    ]
    if len(site_cols) <= 1:
        return
    site_df = daily_wide.select(site_cols)
    (
        site_df.to_pandas()
        .set_index("timestamp")
        .sort_index()
        .plot(title=f"{site_name} daily concurrent spans")
    )
    plt.tight_layout()
    plt.savefig(plot_dir / f"{site_name}_daily_sources.png", dpi=150)
    plt.close()


def load_site(site: utils.SiteConfig):
    print(f"Processing {site.site_name}")

    print("Syncing DB to parquet cache...")
    result = dump_site_to_parquet(site)

    # TODO! output if debug true, or if something not "skipped" or "null"
    # print(result)

    print("\nLoading pipeline...")
    pipeline = UsagePipeline(site)
    usage, audit = pipeline.compute_spans()

    try:
        legacy = pipeline.span_loader.legacy_usage
    except FileNotFoundError:
        print(f"Couldn't load legacy data for {site.site_name}, skipping.")
        legacy = pl.LazyFrame()

    return usage, audit, legacy


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", "-o", type=Path, default=Path("output"))
    parser.add_argument("--sites-config", type=Path, default=Path("etc/sites.yaml"))
    parser.add_argument("--raw-spans", type=Path, default=None)  # None = use yaml value
    return parser.parse_args()


def main():
    args = parse_args()

    sites = utils.load_sites_yaml(args.sites_config)

    # Override raw_spans path if provided
    if args.raw_spans:
        for name, site in sites.items():
            site.raw_spans = str(args.raw_spans / name)

    output_dir = args.output
    output_dir.mkdir(parents=True, exist_ok=True)
    plot_dir = output_dir / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    # load and categorize data from each site

    usages = []
    audits = []
    legacy_usages = []
    for key, site_config in sites.items():
        usage_df, audit_df, legacy_df = load_site(site_config)
        usages.append(usage_df.with_columns(site=pl.lit(key)))
        audits.append(audit_df.with_columns(site=pl.lit(key)))
        legacy_usages.append(legacy_df.with_columns(site=pl.lit(key)))
    all_usage = pl.concat(usages)  # consistent schema
    all_audit = pl.concat(audits, how="diagonal")  # messier schema
    all_legacy_usage = pl.concat(legacy_usages)

    ###################
    # process all sites
    ###################

    """
    TODO!!!!!

    outputs tables of site, source, data status, year, len (rows)
    
    """

    _emit_audit(all_audit.collect(), output_dir)

    # TODO: fix this naming properly!!!
    spans = all_usage.rename({"resource_id": "hypervisor_hostname"})
    daily_wide = _spans_to_daily_wide(
        spans,
        window_start=datetime(2010, 1, 1),
        window_end=datetime(2025, 1, 1),
        group_cols=["site", "source"],  # ensure grouping by both site and span source
        every="30d",
    )

    for site_name in sites.keys():
        _plot_site(daily_wide, site_name, plot_dir)
        print(f"saved {site_name} plots to: {plot_dir}")


if __name__ == "__main__":
    main()
