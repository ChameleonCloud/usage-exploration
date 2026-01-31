"""Adapters convert raw tables to IntervalSchema."""

import polars as pl

from chameleon_usage.schemas import IntervalSchema


def nova_hosts_to_intervals(df: pl.LazyFrame) -> pl.LazyFrame:
    """Nova compute nodes → TOTAL intervals."""
    result = df.select(
        pl.col("hypervisor_hostname").alias("entity_id"),
        pl.col("created_at").alias("start"),
        pl.col("deleted_at").alias("end"),
        pl.lit("total").alias("quantity_type"),
    )
    return IntervalSchema.validate(result)


def blazar_hosts_to_intervals(df: pl.LazyFrame) -> pl.LazyFrame:
    """Blazar compute hosts → RESERVABLE intervals."""
    result = df.select(
        pl.col("hypervisor_hostname").alias("entity_id"),
        pl.col("created_at").alias("start"),
        pl.col("deleted_at").alias("end"),
        pl.lit("reservable").alias("quantity_type"),
    )
    return IntervalSchema.validate(result)


def blazar_allocations_to_intervals(
    alloc: pl.LazyFrame,
    res: pl.LazyFrame,
    lease: pl.LazyFrame,
    blazarhost: pl.LazyFrame,
) -> pl.LazyFrame:
    """Blazar allocations (joined) → COMMITTED intervals."""
    # Join 1: get hypervisor_hostname from blazar host
    alloc_hh = alloc.join(
        other=blazarhost.select(["id", "hypervisor_hostname"]),
        left_on="compute_host_id",
        right_on="id",
        how="left",
        suffix="_host",
    )

    # Join 2: get lease_id from reservation
    alloc_res = alloc_hh.join(
        res.select(["id", "lease_id"]),
        left_on="reservation_id",
        right_on="id",
        how="left",
        suffix="_res",
    )

    # Join 3: get timestamps from lease
    alloc_lease = alloc_res.join(
        lease.select(["id", "start_date", "end_date", "created_at", "deleted_at"]),
        left_on="lease_id",
        right_on="id",
        how="left",
        suffix="_lease",
    )

    # Effective start/end from lease
    effective_start = pl.max_horizontal(
        pl.col("start_date"),
        pl.col("created_at_lease"),
    )
    effective_end = pl.min_horizontal(
        pl.col("end_date"),
        pl.col("deleted_at_lease"),
    )

    result = (
        alloc_lease.select(
            pl.col("hypervisor_hostname").alias("entity_id"),
            effective_start.alias("start"),
            effective_end.alias("end"),
            pl.lit("committed").alias("quantity_type"),
        )
        .filter(
            pl.col("entity_id").is_not_null()
            & (pl.col("start") <= pl.col("end"))
        )
    )
    return IntervalSchema.validate(result)


def nova_instances_to_intervals(df: pl.LazyFrame) -> pl.LazyFrame:
    """Nova instances → OCCUPIED intervals."""
    result = df.select(
        pl.col("node").alias("entity_id"),
        pl.col("created_at").alias("start"),
        pl.col("deleted_at").alias("end"),
        pl.lit("occupied").alias("quantity_type"),
    )
    return IntervalSchema.validate(result)
