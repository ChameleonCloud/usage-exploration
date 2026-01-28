from dataclasses import dataclass
from typing import Optional

import polars as pl
import yaml


@dataclass
class PipelineOutput:
    raw_spans: pl.LazyFrame
    valid_spans: pl.LazyFrame
    audit_spans: pl.LazyFrame
    legacy_usage: pl.LazyFrame


def merge_pipelines(inputs: dict[str, PipelineOutput]) -> PipelineOutput:
    raw_arr = []
    valid_arr = []
    audit_arr = []
    legacy_arr = []

    for site_name, input in inputs.items():
        raw_arr.append(input.raw_spans.with_columns(site=pl.lit(site_name)))
        valid_arr.append(input.valid_spans.with_columns(site=pl.lit(site_name)))
        audit_arr.append(input.audit_spans.with_columns(site=pl.lit(site_name)))
        legacy_arr.append(input.legacy_usage.with_columns(site=pl.lit(site_name)))

    return PipelineOutput(
        raw_spans=pl.concat(raw_arr, how="diagonal"),
        valid_spans=pl.concat(valid_arr),
        audit_spans=pl.concat(audit_arr, how="diagonal"),
        legacy_usage=pl.concat(legacy_arr),
    )


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
