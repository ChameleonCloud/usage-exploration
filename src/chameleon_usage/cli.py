import argparse
import logging
import os
from datetime import datetime
from pathlib import Path

import polars as pl

from chameleon_usage.config import SiteConfig, load_config
from chameleon_usage.exceptions import (
    IntervalOverlapError,
    RawTableLoadError,
    log_raw_table_load_error,
)
from chameleon_usage.extract.dump_db import dump_to_parquet, generate_grant_sql
from chameleon_usage.ingest import load_intervals
from chameleon_usage.ingest.metadata import (
    get_last_modified,
    get_site_time_range,
    read_site_meta,
)
from chameleon_usage.output import compat
from chameleon_usage.pipeline import run_pipeline
from chameleon_usage.schemas import PipelineSpec

logger = logging.getLogger(__name__)


def _resolve_sites(args) -> list[SiteConfig]:
    """Load config, resolve site list and data_dir overrides."""
    if not args.config:
        raise SystemExit("Error: --config required")
    sites_config = load_config(args.config)
    site_keys = args.site or list(sites_config.keys())
    configs = []
    for key in site_keys:
        config = sites_config[key]
        if args.data_dir:
            config.data_dir = args.data_dir.rstrip("/")
        configs.append(config)
    return configs


# ── Subcommands ──────────────────────────────────────────────────────────


def cmd_info(args):
    for config in _resolve_sites(args):
        if not config.data_dir:
            logger.warning("[%s] no data_dir configured, skipping", config.key)
            continue
        logger.info("[%s] %s", config.key, config.data_dir)
        tables = read_site_meta(config.data_dir)
        if not tables:
            logger.info("  no tables found")
            continue
        for t in tables:
            time_range = ""
            if t.time_min and t.time_max:
                time_range = f"  {t.time_min} — {t.time_max}"
            logger.info("  %-40s %8d rows%s", t.key, t.num_rows, time_range)
        site_range = get_site_time_range(tables)
        if site_range:
            logger.info("  data range: %s — %s", site_range[0], site_range[1])


def cmd_extract(args):
    if args.print_grant_sql:
        logger.info("%s", generate_grant_sql(args.grant_user, args.grant_host))
        return

    # Priority: --db-uri > $DATABASE_URI > config.db_uri
    db_uri = args.db_uri or os.environ.get("DATABASE_URI")

    if args.config:
        for config in _resolve_sites(args):
            site_db_uri = db_uri or config.db_uri
            if not site_db_uri:
                raise SystemExit(f"Error: no db_uri for site {config.key}")
            if not config.data_dir:
                raise SystemExit(f"Error: no output path for site {config.key}")
            logger.info("Extracting %s...", config.key)
            dump_to_parquet(site_db_uri, config.data_dir)
    elif db_uri:
        if not args.data_dir:
            raise SystemExit("Error: --data-dir required when not using --config")
        dump_to_parquet(db_uri, args.data_dir.rstrip("/"))
    else:
        raise SystemExit("Error: --db-uri, $DATABASE_URI, or --config required")


def cmd_process(args):
    output_base = Path(args.output)
    output_base.mkdir(parents=True, exist_ok=True)
    usage_frames: list[pl.DataFrame] = []
    export_uri = args.export_uri or os.environ.get("EXPORT_URI")

    for config in _resolve_sites(args):
        if not config.data_dir:
            raise SystemExit(f"Error: no data_dir for site {config.key}")

        if bool(args.start_date) != bool(args.end_date):
            raise SystemExit(
                "Error: --start-date and --end-date must be provided together, or both omitted"
            )

        if args.start_date:
            start = datetime.fromisoformat(args.start_date)
            end = datetime.fromisoformat(args.end_date)
        else:
            tables = read_site_meta(config.data_dir)
            time_range = get_site_time_range(tables)
            if not time_range:
                raise SystemExit(
                    f"Error: no date range for {config.key}; use --start-date and --end-date"
                )
            start = time_range[0]
            end = get_last_modified(tables) or time_range[1]
            logger.info("[%s] derived date range: %s — %s", config.key, start, end)

        spec = PipelineSpec(
            group_cols=("metric", "resource", "site", "collector_type", "usable"),
            time_range=(start, end),
        )

        try:
            intervals = load_intervals(config.data_dir, spec.time_range).with_columns(
                pl.lit(config.key).alias("site")
            )
            site_usage = run_pipeline(intervals, spec, resample_interval=args.resample)
        except RawTableLoadError as exc:
            log_raw_table_load_error(logger, config.key, exc)
            continue
        except IntervalOverlapError as exc:
            logger.error("[%s] interval overlap, skipping site: %s", config.key, exc)
            continue
        except Exception:
            logger.exception("[%s] unhandled exception", config.key)
            raise

        output_dir = output_base / config.key
        output_dir.mkdir(parents=True, exist_ok=True)
        site_usage_df = site_usage.collect()
        site_usage_df.write_parquet(output_dir / "usage.parquet")
        usage_frames.append(site_usage_df)

    if export_uri and usage_frames:
        combined_usage = pl.concat(usage_frames)
        compat_output = compat.to_compat_format(combined_usage)
        compat.write_compat_to_db(compat_output, db_uri=export_uri)


# ── Parser ───────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Chameleon cloud resource usage reporting.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    info = subparsers.add_parser("info", help="Show available tables and row counts")
    info.add_argument("--config", required=True, help="Path to site config YAML.")
    info.add_argument(
        "--site", action="append", help="Site key (repeatable). Defaults to all sites."
    )
    info.add_argument(
        "--data-dir", help="Local directory or s3://. Overrides config data_dir."
    )
    info.set_defaults(func=cmd_info)

    extract = subparsers.add_parser(
        "extract", help="Dump database tables to parquet files"
    )
    extract.add_argument("--config", help="Path to site config YAML.")
    extract.add_argument(
        "--site", action="append", help="Site key (repeatable). Defaults to all sites."
    )
    extract.add_argument(
        "--data-dir", help="Local directory or s3://. Overrides config data_dir."
    )
    extract.add_argument(
        "--db-uri",
        help="Database URI (mysql://user:pass@host:port). Falls back to $DATABASE_URI.",
    )
    extract.add_argument(
        "--print-grant-sql",
        action="store_true",
        help="Print SQL to grant read access and exit.",
    )
    extract.add_argument(
        "--grant-user", default="usage_exporter", help="MySQL username for grant SQL."
    )
    extract.add_argument(
        "--grant-host", default="%%", help="MySQL host pattern for grant SQL."
    )
    extract.set_defaults(func=cmd_extract)

    process = subparsers.add_parser(
        "process", help="Compute usage timelines from parquet data"
    )
    process.add_argument("--config", required=True, help="Path to site config YAML.")
    process.add_argument(
        "--site", action="append", help="Site key (repeatable). Defaults to all sites."
    )
    process.add_argument(
        "--data-dir", help="Local directory or s3://. Overrides config data_dir."
    )
    process.add_argument(
        "--output",
        required=True,
        help="Output directory for usage parquet per site.",
    )
    process.add_argument(
        "--start-date",
        help="Start date (YYYY-MM-DD). Derived from data if omitted.",
    )
    process.add_argument(
        "--end-date",
        help="End date (YYYY-MM-DD). Derived from data if omitted.",
    )
    process.add_argument(
        "--resample",
        help="Resample interval (e.g. 1h, 1d, 7d).",
    )
    process.add_argument(
        "--export-uri",
        help="DB URI to push output data into. Falls back to $EXPORT_URI.",
    )
    process.set_defaults(func=cmd_process)

    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
