import argparse
from datetime import datetime
from pathlib import Path

from chameleon_usage.config import SiteConfig, load_sites
from chameleon_usage.extract.dump_db import dump_site_to_parquet
from chameleon_usage.pipeline import process_site
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

    subparsers = parser.add_subparsers(dest="command", required=True)

    extract = subparsers.add_parser("extract")
    extract.add_argument(
        "--output",
        help="Base directory to write parquet. Each site writes to <output>/<site>.",
    )
    extract.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing parquet files.",
    )

    process = subparsers.add_parser("process")
    process.add_argument(
        "--input",
        help="Base directory containing per-site parquet folders.",
    )
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


def _with_output_dir(config: SiteConfig, output_dir: str, site_key: str) -> SiteConfig:
    raw_spans = str(Path(output_dir) / site_key)
    Path(raw_spans).mkdir(parents=True, exist_ok=True)
    return SiteConfig(
        site_name=config.site_name,
        raw_spans=raw_spans,
        db_uris=config.db_uris,
    )


def _resolve_raw_spans(
    config: SiteConfig, site_key: str, input_dir: str | None
) -> tuple[str, str]:
    if input_dir:
        return input_dir, site_key

    raw_path = Path(config.raw_spans)
    if raw_path.name == site_key:
        return str(raw_path.parent), raw_path.name

    return str(raw_path), site_key


def main() -> None:
    args = parse_args()
    sites = load_sites(args.sites_config)
    site_keys = args.site or list(sites.keys())

    if args.command == "extract":
        for site_key in site_keys:
            config = sites[site_key]
            if args.output:
                config = _with_output_dir(config, args.output, site_key)
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

        for site_key in site_keys:
            config = sites[site_key]
            base_path, site_name = _resolve_raw_spans(config, site_key, args.input)

            usage = process_site(
                base_path=base_path,
                site_name=site_name,
                spec=spec,
                resample_interval=args.resample,
            )
            output_dir = output_base / site_key
            output_dir.mkdir(parents=True, exist_ok=True)
            usage.collect().write_parquet(output_dir / "usage.parquet")


if __name__ == "__main__":
    main()
