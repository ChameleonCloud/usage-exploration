import argparse
from datetime import datetime
from pathlib import Path

import polars as pl

from chameleon_usage.config import SiteConfig, load_config
from chameleon_usage.extract.dump_db import dump_site_to_parquet
from chameleon_usage.ingest import clamp_hierarchy, load_intervals
from chameleon_usage.pipeline import run_pipeline
from chameleon_usage.schemas import PipelineSpec


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sites-config",
        required=True,
        help="Path to sites.yaml",
    )
    parser.add_argument(
        "--site",
        action="append",
        help="Site key from sites.yaml (repeatable). Defaults to all sites.",
    )
    parser.add_argument(
        "--parquet-dir",
        required=True,
        help="Base path to store or load parquet files.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    extract = subparsers.add_parser("extract")
    extract.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing parquet files.",
    )

    process = subparsers.add_parser("process")
    process.add_argument(
        "--output",
        required=True,
        help="Output directory for usage parquet per site.",
    )
    process.add_argument(
        "--start-date",
        required=True,
        help="Start date (YYYY-MM-DD).",
    )
    process.add_argument(
        "--end-date",
        required=True,
        help="End date (YYYY-MM-DD).",
    )
    process.add_argument(
        "--resample",
        help="Optional resample interval (e.g. 1d, 7d).",
    )

    return parser.parse_args()


def process_site(config: SiteConfig, spec: PipelineSpec, resample: str) -> pl.LazyFrame:
    intervals = load_intervals(config.raw_parquet, spec.time_range)
    clamped = clamp_hierarchy(intervals)
    valid = clamped.filter(pl.col("valid"))

    cols = set(valid.collect_schema().names())
    if "site" not in cols:
        valid = valid.with_columns(pl.lit(config.key).alias("site"))

    return run_pipeline(valid, spec, resample_interval=resample)


def main() -> None:
    args = parse_args()
    sites_config = load_config(args.sites_config)
    site_keys = args.site or list(
        sites_config.keys()
    )  # default to all configured sites

    # override parquet dir if cli set.
    for site_key in site_keys:
        config = sites_config[site_key]
        config.raw_parquet = f"{args.parquet_dir}/{site_key}"

        if args.command == "extract":
            dump_site_to_parquet(config, force=args.force)
            return

        if args.command == "process":
            start = datetime.fromisoformat(args.start_date)
            end = datetime.fromisoformat(args.end_date)
            spec = PipelineSpec(
                group_cols=("metric", "resource", "site", "collector_type"),
                time_range=(start, end),
            )

            output_base = Path(args.output)
            output_base.mkdir(parents=True, exist_ok=True)

            usage = process_site(config, spec, args.resample)

            output_dir = output_base / site_key
            output_dir.mkdir(parents=True, exist_ok=True)
            usage.collect().write_parquet(output_dir / "usage.parquet")


if __name__ == "__main__":
    main()
