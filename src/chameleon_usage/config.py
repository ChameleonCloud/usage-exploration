from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass(frozen=False)
class SiteConfig:
    key: str
    site_name: str
    adapters: Optional[list[str]] = None
    data_dir: Optional[str] = None
    db_uri: Optional[str] = None


# TODO: kinda messy, use pydantic?
def load_config(path: str | Path) -> dict[str, SiteConfig]:
    data = yaml.safe_load(Path(path).read_text()) or {}
    sites: dict[str, SiteConfig] = {}
    for key, value in data.items():
        payload = dict(value)
        if "data_dir" not in payload and "raw_parquet" in payload:
            payload["data_dir"] = payload.pop("raw_parquet")
        payload["key"] = key
        sites[key] = SiteConfig(**payload)
    return sites


def get_config_for_site(path: str | Path, site_key: str) -> SiteConfig:
    return load_config(path)[site_key]
