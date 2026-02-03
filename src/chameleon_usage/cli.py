import argparse
from pathlib import Path

from chameleon_usage.config import SiteConfig, load_sites
from chameleon_usage.extract.dump_db import dump_site_to_parquet


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

    return parser.parse_args()


def _with_output_dir(config: SiteConfig, output_dir: str, site_key: str) -> SiteConfig:
    raw_spans = str(Path(output_dir) / site_key)
    Path(raw_spans).mkdir(parents=True, exist_ok=True)
    return SiteConfig(
        site_name=config.site_name,
        raw_spans=raw_spans,
        db_uris=config.db_uris,
    )


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


if __name__ == "__main__":
    main()
