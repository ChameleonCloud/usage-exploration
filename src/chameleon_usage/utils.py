from dataclasses import dataclass

import polars as pl
import yaml


@dataclass
class SiteConfig:
    site_name: str
    raw_spans: str
    db_uris: dict[str, str]


def load_sites_yaml(yaml_path: str) -> dict[str, SiteConfig]:
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    return {
        key: SiteConfig(
            site_name=site["site_name"],
            raw_spans=site["raw_spans"],
            db_uris=site["db_uris"],
        )
        for key, site in data.items()
    }


def print_summary(results: dict[str, dict[str, str]]):
    rows = [
        {"table": table, "site": site, "status": status}
        for site, tables in results.items()
        for table, status in tables.items()
    ]
    df = pl.DataFrame(rows).pivot(on="site", index="table", values="status")
    with pl.Config(tbl_cols=-1, tbl_rows=-1, fmt_str_lengths=100):
        print(df)
