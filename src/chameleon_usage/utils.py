from dataclasses import dataclass

import yaml


@dataclass
class DbUris:
    blazar: str
    nova: str
    nova_api: str


@dataclass
class SiteConfig:
    site_name: str
    raw_spans: str
    db_uris: DbUris


def load_sites_yaml(yaml_path: str) -> dict[str, SiteConfig]:
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    return {
        key: SiteConfig(
            site_name=site["site_name"],
            raw_spans=site["raw_spans"],
            db_uris=DbUris(**site["db_uris"]),
        )
        for key, site in data.items()
    }
