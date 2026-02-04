"""Generate usage report for chi@edge (device allocations)."""

from datetime import datetime

import polars as pl

from chameleon_usage.constants import ResourceTypes as RT
from chameleon_usage.ingest import blazarDeviceCommitted
from chameleon_usage.ingest.adapters import AdapterRegistry
from chameleon_usage.ingest.rawschemas import (
    BlazarDeviceAllocationRaw,
    BlazarDeviceRaw,
    BlazarLeaseRaw,
    BlazarReservationRaw,
)
from chameleon_usage.pipeline import resample, run_pipeline
from chameleon_usage.schemas import PipelineSpec
from chameleon_usage.sources import Tables


def load_edge_tables(path: str) -> dict[str, pl.LazyFrame]:
    """Load only the tables needed for chi@edge device allocations."""
    return {
        Tables.BLAZAR_DEVICE_ALLOCATIONS: BlazarDeviceAllocationRaw.validate(
            pl.scan_parquet(f"{path}/blazar.device_allocations.parquet")
        ),
        Tables.BLAZAR_DEVICES: BlazarDeviceRaw.validate(
            pl.scan_parquet(f"{path}/blazar.devices.parquet")
        ),
        Tables.BLAZAR_RES: BlazarReservationRaw.validate(
            pl.scan_parquet(f"{path}/blazar.reservations.parquet")
        ),
        Tables.BLAZAR_LEASES: BlazarLeaseRaw.validate(
            pl.scan_parquet(f"{path}/blazar.leases.parquet")
        ),
    }


def main():
    path = "data/current/chi_edge"
    time_range = (datetime(2021, 1, 1), datetime(2026, 1, 1))

    spec = PipelineSpec(
        group_cols=("metric", "resource", "site", "collector_type"),
        time_range=time_range,
    )

    tables = load_edge_tables(path)
    registry = AdapterRegistry([blazarDeviceCommitted])
    intervals = registry.to_intervals(tables).with_columns(
        pl.lit("chi_edge").alias("site"),
        pl.lit("current").alias("collector_type"),
    )

    results = run_pipeline(intervals, spec)
    usage = resample(results, "1d", spec).collect()

    pl.Config.set_tbl_cols(-1)
    print(f"Rows: {usage.height}")
    with pl.Config(tbl_rows=100, tbl_cols=20):
        print(usage.filter(pl.col("resource") == RT.DEVICE).tail(20))


if __name__ == "__main__":
    main()
