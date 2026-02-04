"""Generate usage report for chi@edge (device allocations)."""

from datetime import datetime

import polars as pl

from chameleon_usage.constants import ResourceTypes as RT
from chameleon_usage.ingest import blazarDeviceCommitted, blazarDeviceReservable
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
from chameleon_usage.viz.plots import AreaLayer, LineLayer, plot_stacked_step_with_pct


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
    time_range = (datetime(2022, 3, 1), datetime(2026, 1, 1))
    bucket_length = "7d"

    spec = PipelineSpec(
        group_cols=("metric", "resource", "site", "collector_type"),
        time_range=time_range,
    )

    tables = load_edge_tables(path)
    registry = AdapterRegistry([blazarDeviceReservable, blazarDeviceCommitted])
    intervals = registry.to_intervals(tables).with_columns(
        pl.lit("chi_edge").alias("site"),
        pl.lit("current").alias("collector_type"),
    )

    results = run_pipeline(intervals, spec)
    usage = resample(results, bucket_length, spec).collect()

    # Filter to device data
    devices = usage.filter(pl.col("resource") == RT.DEVICE)
    committed = devices.filter(pl.col("metric") == "committed")
    available = devices.filter(pl.col("metric") == "available_reservable")
    reservable = devices.filter(pl.col("metric") == "reservable")

    x = committed.get_column("timestamp").to_list()
    committed_vals = committed.get_column("value").fill_null(0).to_list()
    available_vals = available.get_column("value").fill_null(0).to_list()
    reservable_vals = reservable.get_column("value").fill_null(0).to_list()

    # Stack: committed + available = reservable total
    areas = [
        AreaLayer(committed_vals, "#2ca02c", "Committed"),
        AreaLayer(available_vals, "#aec7e8", "Available"),
    ]
    lines = [LineLayer(reservable_vals, "#333333", "Reservable", linewidth=2)]

    plot_stacked_step_with_pct(
        x,
        areas,
        lines=lines,
        title="chi_edge - devices",
        y_label="devices",
        output_path="output/plots/chi_edge_devices.png",
    )


if __name__ == "__main__":
    main()
