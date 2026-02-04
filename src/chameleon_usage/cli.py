import argparse
import os
from datetime import datetime
from pathlib import Path

from chameleon_usage.config import SiteConfig, load_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sites-config",
        help="Path to sites.yaml (required for process command)",
    )
    parser.add_argument(
        "--site",
        action="append",
        help="Site key from sites.yaml (repeatable). Defaults to all sites.",
    )
    parser.add_argument(
        "--parquet-dir",
        help="Local directory or s3://. Overrides config.raw_parquet if set.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    extract = subparsers.add_parser("extract")
    extract.add_argument(
        "--db-uri",
        help="Database URI (mysql://user:pass@host:port). Falls back to $DATABASE_URI.",
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


def process_site(config: SiteConfig, spec, resample: str):
    """Process a site's data through the pipeline. Requires [pipeline] extras."""
    import polars as pl

    from chameleon_usage.ingest import clamp_hierarchy, load_intervals
    from chameleon_usage.pipeline import run_pipeline

    intervals = (
        load_intervals(config.raw_parquet, spec.time_range).collect().lazy()
    )  # checkpoint
    clamped = clamp_hierarchy(intervals).collect().lazy()  # checkpoint

    valid = clamped.filter(pl.col("valid"))

    cols = set(valid.collect_schema().names())
    if "site" not in cols:
        valid = valid.with_columns(pl.lit(config.key).alias("site"))

    return run_pipeline(valid, spec, resample_interval=resample)


def main() -> None:
    args = parse_args()

    if args.command == "extract":
        from chameleon_usage.extract.dump_db import dump_to_parquet

        # Priority: --db-uri > config > $DATABASE_URI
        db_uri = args.db_uri or os.environ.get("DATABASE_URI")

        if args.sites_config:
            sites_config = load_config(args.sites_config)
            site_keys = args.site or list(sites_config.keys())
            for site_key in site_keys:
                config = sites_config[site_key]
                site_db_uri = db_uri or config.db_uri
                if not site_db_uri:
                    raise SystemExit(f"Error: no db_uri for site {site_key}")
                # Priority: --parquet-dir > config.raw_parquet
                output_path = args.parquet_dir or config.raw_parquet
                if not output_path:
                    raise SystemExit(f"Error: no output path for site {site_key}")
                output_path = output_path.rstrip("/")
                print(f"Extracting {site_key}...")
                dump_to_parquet(site_db_uri, output_path)
        elif db_uri:
            if not args.parquet_dir:
                raise SystemExit("Error: --parquet-dir required when not using config")
            dump_to_parquet(db_uri, args.parquet_dir.rstrip("/"))
        else:
            raise SystemExit("Error: --db-uri, $DATABASE_URI, or --sites-config required")
        return

    if args.command == "process":
        if not args.sites_config:
            raise SystemExit("Error: --sites-config required for process command")

        from chameleon_usage.schemas import PipelineSpec

        sites_config = load_config(args.sites_config)
        site_keys = args.site or list(sites_config.keys())

        start = datetime.fromisoformat(args.start_date)
        end = datetime.fromisoformat(args.end_date)
        spec = PipelineSpec(
            group_cols=("metric", "resource", "site", "collector_type"),
            time_range=(start, end),
        )

        output_base = Path(args.output)
        output_base.mkdir(parents=True, exist_ok=True)

        for site_key in site_keys:
            config = sites_config[site_key]
            if args.parquet_dir:
                config.raw_parquet = args.parquet_dir.rstrip("/")
            if not config.raw_parquet:
                raise SystemExit(f"Error: no raw_parquet for site {site_key}")

            usage = process_site(config, spec, args.resample)

            output_dir = output_base / site_key
            output_dir.mkdir(parents=True, exist_ok=True)
            usage.collect().write_parquet(output_dir / "usage.parquet")


if __name__ == "__main__":
    main()
