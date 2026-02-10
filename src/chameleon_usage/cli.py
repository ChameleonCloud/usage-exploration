import argparse
import logging
import os
from datetime import datetime
from pathlib import Path

from chameleon_usage.config import SiteConfig, load_config
from chameleon_usage.output import compat

logger = logging.getLogger(__name__)


def _add_shared_args(
    parser: argparse.ArgumentParser, default: object | None = None
) -> None:
    parser.add_argument(
        "--config",
        help="Path to etc/site.yml (required for process command)",
        default=default,
    )
    parser.add_argument(
        "--site",
        action="append",
        help="Site key from etc/site.yml (repeatable). Defaults to all sites.",
        default=default,
    )
    parser.add_argument(
        "--data-dir",
        help="Local directory or s3://. Overrides config.data_dir if set.",
        default=default,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    _add_shared_args(parser)

    subparsers = parser.add_subparsers(dest="command", required=True)

    extract = subparsers.add_parser("extract")
    _add_shared_args(extract, default=argparse.SUPPRESS)
    extract.add_argument(
        "--db-uri",
        help="Database URI (mysql://user:pass@host:port). Falls back to $DATABASE_URI.",
    )

    grant_sql = subparsers.add_parser(
        "print-grant-sql", help="Print SQL to grant read access"
    )
    grant_sql.add_argument("--user", default="usage_exporter", help="MySQL username")
    grant_sql.add_argument(
        "--host", default="%", help="MySQL host patterns (default: %%)"
    )

    process = subparsers.add_parser("process")
    _add_shared_args(process, default=argparse.SUPPRESS)
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

    process.add_argument(
        "--export-uri",
        help="Optional DB uri to push output data into.",
    )

    return parser.parse_args()


def process_site(config: SiteConfig, spec, resample: str):
    """Process a site's data through the pipeline. Requires [pipeline] extras."""
    import polars as pl

    from chameleon_usage.ingest import clamp_hierarchy, load_intervals
    from chameleon_usage.pipeline import run_pipeline

    data_dir = config.data_dir
    if data_dir is None:
        raise SystemExit(f"Error: no data_dir for site {config.key}")

    intervals = load_intervals(data_dir, spec.time_range).collect().lazy()  # checkpoint
    clamped = clamp_hierarchy(intervals).collect().lazy()  # checkpoint

    valid = clamped.filter(pl.col("valid"))

    cols = set(valid.collect_schema().names())
    if "site" not in cols:
        valid = valid.with_columns(pl.lit(config.key).alias("site"))

    return run_pipeline(valid, spec, resample_interval=resample)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args()

    if args.command == "print-grant-sql":
        from chameleon_usage.extract.dump_db import generate_grant_sql

        logger.info("%s", generate_grant_sql(args.user, args.host))
        return

    if args.command == "extract":
        from chameleon_usage.extract.dump_db import dump_to_parquet

        # Priority: --db-uri > $DATABASE_URI > config.db_uri
        db_uri = args.db_uri or os.environ.get("DATABASE_URI")

        if args.config:
            sites_config = load_config(args.config)
            site_keys = args.site or list(sites_config.keys())
            for site_key in site_keys:
                config = sites_config[site_key]
                site_db_uri = db_uri or config.db_uri
                if not site_db_uri:
                    raise SystemExit(f"Error: no db_uri for site {site_key}")
                # Priority: --data-dir > config.data_dir
                output_path = args.data_dir or config.data_dir
                if not output_path:
                    raise SystemExit(f"Error: no output path for site {site_key}")
                output_path = output_path.rstrip("/")
                logger.info("Extracting %s...", site_key)
                dump_to_parquet(site_db_uri, output_path)
        elif db_uri:
            if not args.data_dir:
                raise SystemExit("Error: --data-dir required when not using --config")
            dump_to_parquet(db_uri, args.data_dir.rstrip("/"))
        else:
            raise SystemExit("Error: --db-uri, $DATABASE_URI, or --config required")
        return

    if args.command == "process":
        import polars as pl

        if not args.config:
            raise SystemExit("Error: --config required for process command")

        from chameleon_usage.exceptions import (
            RawTableLoadError,
            log_raw_table_load_error,
        )
        from chameleon_usage.schemas import PipelineSpec

        sites_config = load_config(args.config)
        site_keys = args.site or list(sites_config.keys())

        start = datetime.fromisoformat(args.start_date)
        end = datetime.fromisoformat(args.end_date)
        spec = PipelineSpec(
            group_cols=("metric", "resource", "site", "collector_type"),
            time_range=(start, end),
        )

        output_base = Path(args.output)
        output_base.mkdir(parents=True, exist_ok=True)
        usage_frames: list[pl.DataFrame] = []

        for site_key in site_keys:
            config = sites_config[site_key]
            if args.data_dir:
                config.data_dir = args.data_dir.rstrip("/")
            if not config.data_dir:
                raise SystemExit(f"Error: no data_dir for site {site_key}")

            try:
                site_usage = process_site(config, spec, args.resample)
            except RawTableLoadError as exc:
                log_raw_table_load_error(logger, site_key, exc)
                continue
            except Exception:
                logger.exception("[%s] unhandled exception", site_key)
                raise

            output_dir = output_base / site_key
            output_dir.mkdir(parents=True, exist_ok=True)
            site_usage_df = site_usage.collect()
            site_usage_df.write_parquet(output_dir / "usage.parquet")
            usage_frames.append(site_usage_df)

        if args.export_uri and usage_frames:
            combined_usage = pl.concat(usage_frames)
            compat_output = compat.to_compat_format(combined_usage)
            compat.write_compat_to_db(compat_output, db_uri=args.export_uri)


if __name__ == "__main__":
    main()
