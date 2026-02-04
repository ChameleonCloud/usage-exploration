"""Generate usage report for chi@edge (device allocations)."""

import json
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


def load_project_ids(path: str) -> dict[str, dict]:
    with open(path) as f:
        projects = json.load(f)
    return {p["ID"]: {"name": p["Name"]} for p in projects}


# Add project IDs here to mark as ops
OPS_PROJECT_IDS: set[str] = set(
    [
        "a22956f2fc75458c80d6915942ba8771",  # "testing-01"
        "4ef13b0192a343f7a23b8cfa993478f9",  # "testing-02"
        "080121fff65c48b7b438befc08725618",  # "testing-03"
        "ba407e6926e9488997c667306f842209",  # "testing-04"
        "c4cc808c8ad64a9091344d45da8dcfbc",  # "openstack"
        "a5f0758da4a5404bbfcef0a64206614c",  # "Chameleon"
    ]
)


def main():
    path = "data/current/chi_edge"
    time_range = (datetime(2022, 3, 1), datetime(2025, 1, 1))
    bucket_length = "7d"

    project_ids = load_project_ids("etc/edge_projects.json")

    spec = PipelineSpec(
        group_cols=("metric", "resource", "site", "collector_type", "ops"),
        time_range=time_range,
    )

    tables = load_edge_tables(path)
    registry = AdapterRegistry([blazarDeviceReservable, blazarDeviceCommitted])
    intervals = registry.to_intervals(tables).with_columns(
        pl.lit("chi_edge").alias("site"),
        pl.lit("current").alias("collector_type"),
        pl.col("project_id").is_in(OPS_PROJECT_IDS).fill_null(False).alias("ops"),
    )

    # Project summary: count allocations per project
    alloc_counts = (
        intervals.filter(pl.col("metric") == "committed")
        .group_by("project_id")
        .agg(pl.len().alias("allocations"))
        .collect()
    )
    summary = alloc_counts.with_columns(
        pl.col("project_id")
        .replace_strict(project_ids, default=None)
        .struct.field("name")
        .alias("name"),
        pl.col("project_id").is_in(OPS_PROJECT_IDS).alias("ops"),
    ).sort("allocations", descending=True)
    with pl.Config(tbl_rows=200, tbl_width_chars=200, fmt_str_lengths=50):
        print(summary.head(20))

    results = run_pipeline(intervals, spec)
    usage = resample(results, bucket_length, spec).collect()

    # Filter to device data
    devices = usage.filter(pl.col("resource") == RT.DEVICE)
    committed_ops = devices.filter((pl.col("metric") == "committed") & pl.col("ops"))
    committed_user = devices.filter((pl.col("metric") == "committed") & ~pl.col("ops"))
    reservable = devices.filter((pl.col("metric") == "reservable") & ~pl.col("ops"))

    # Compute available = reservable - ops - user (pipeline's available_reservable is wrong due to ops grouping)
    combined = (
        committed_ops.select("timestamp", pl.col("value").alias("ops"))
        .join(
            committed_user.select("timestamp", pl.col("value").alias("user")),
            on="timestamp",
        )
        .join(
            reservable.select("timestamp", pl.col("value").alias("reservable")),
            on="timestamp",
        )
        .with_columns(
            (pl.col("reservable") - pl.col("ops") - pl.col("user")).alias("available")
        )
        .fill_null(0)
    )

    x = combined.get_column("timestamp").to_list()
    committed_ops_vals = combined.get_column("ops").to_list()
    committed_user_vals = combined.get_column("user").to_list()
    available_vals = combined.get_column("available").to_list()
    reservable_vals = combined.get_column("reservable").to_list()

    # Stack: ops committed, user committed, available
    areas = [
        AreaLayer(committed_ops_vals, "#e7a23b", "Ops"),
        AreaLayer(committed_user_vals, "#2ca02c", "User"),
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
