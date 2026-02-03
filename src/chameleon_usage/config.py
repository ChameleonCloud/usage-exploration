from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class SiteConfig:
    site_name: str
    raw_spans: str
    db_uris: dict[str, str]


def load_sites(path: str | Path) -> dict[str, SiteConfig]:
    data = yaml.safe_load(Path(path).read_text()) or {}
    return {key: SiteConfig(**value) for key, value in data.items()}


def load_site(path: str | Path, site_key: str) -> SiteConfig:
    return load_sites(path)[site_key]
