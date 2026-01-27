import argparse
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl

from chameleon_usage import audit, math, utils
from chameleon_usage.dump_db import dump_site_to_parquet
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
    pipelineresult = pipeline.compute_spans()

    try:
        legacy = pipeline.span_loader.legacy_usage
    except FileNotFoundError:
        print(f"Couldn't load legacy data for {site.site_name}, skipping.")
        legacy = pl.LazyFrame()

    return pipelineresult, legacy


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

    window_start = datetime(2010, 1, 1)
    window_end = datetime(2025, 1, 1)

    # Load, audit, and collect data from each site
    all_valid = []
    all_legacy = []
    for site_name, site_config in sites.items():
        pipelineresult, legacy_df = load_site(site_config)

        # Collect per-site data
        raw_df = pipelineresult.raw_spans.collect()
        valid_df = pipelineresult.valid_spans.collect()
        audit_df = pipelineresult.audit_spans.collect()

        # Run audit checks per-site (includes row + hour invariants)
        audit.run_site_audit(
            raw_df,
            valid_df,
            audit_df,
            site=site_name,
            window_start=window_start,
            window_end=window_end,
            output_dir=output_dir,
        )

        # Accumulate for combined processing
        all_valid.append(valid_df.with_columns(site=pl.lit(site_name)))
        all_legacy.append(legacy_df.with_columns(site=pl.lit(site_name)))

    # Combine all sites for plotting
    valid_spans = pl.concat(all_valid)
    daily_wide = _spans_to_daily_wide(
        valid_spans,
        window_start=window_start,
        window_end=window_end,
        group_cols=["site", "source"],  # ensure grouping by both site and span source
        every="30d",
    )

    for site_name in sites.keys():
        _plot_site(daily_wide, site_name, plot_dir)
        print(f"saved {site_name} plots to: {plot_dir}")


if __name__ == "__main__":
    main()
