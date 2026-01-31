"""Ingest module: load raw data and convert to intervals."""

import polars as pl

from chameleon_usage.constants import Tables
from chameleon_usage.ingest.adapters import (
    Adapter,
    AdapterRegistry,
    blazar_allocations_source,
)
from chameleon_usage.ingest.loader import load_raw_tables

##################
# Default registry
##################
REGISTRY = AdapterRegistry(
    [
        Adapter(
            entity_col="hypervisor_hostname",
            quantity_type="total",
            source=lambda t: t[Tables.NOVA_HOSTS],
        ),
        Adapter(
            entity_col="hypervisor_hostname",
            quantity_type="reservable",
            source=lambda t: t[Tables.BLAZAR_HOSTS],
            context_cols={"id": "blazar_host_id"},
        ),
        Adapter(
            entity_col="hypervisor_hostname",
            quantity_type="committed",
            source=blazar_allocations_source,
            start_col="effective_start",
            end_col="effective_end",
        ),
        Adapter(
            entity_col="uuid",
            quantity_type="occupied",
            source=lambda t: t[Tables.NOVA_INSTANCES],
            context_cols={
                "node": "hypervisor_hostname",
                "reservation_id": "blazar_reservation_id",
            },
        ),
    ]
)


def load_intervals(base_path: str, site_name: str) -> pl.LazyFrame:
    tables = load_raw_tables(base_path, site_name)
    return REGISTRY.to_intervals(tables)
