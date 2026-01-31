"""Coerce intervals: clip child lifetimes to parent windows via ASOF join."""

from typing import List

import polars as pl


def apply_temporal_clamp(
    target: pl.LazyFrame,
    validators: pl.LazyFrame,
    join_keys: List[str],
    # buffer: timedelta | None,
):
    """
    Clamps target end_ts based on a validator stream with reused keys.
    Uses ASOF join to match the specific era of the hostname.
    """

    # 1. Prepare Validators (The Horizon)
    # We need to know when each specific 'era' of the hostname ended.

    join_columns = [pl.col(k) for k in join_keys]

    horizon_stream = validators.sort("start").select(
        [
            *join_columns,
            pl.col("start").alias("val_start"),
            # TODO buffer
            (pl.col("end")).alias("val_horizon"),
        ]
    )

    # 2. Temporal Match (ASOF Join)
    # "Find the version of host-01 that started most recently before I did."
    joined_state = target.sort("start").join_asof(
        horizon_stream,
        left_on="start",
        right_on="val_start",
        by=join_keys,
        strategy="backward",
    )

    # 3. Filter orphans: no parent matched, or parent ended before child started
    valid = joined_state.filter(
        pl.col("val_horizon").is_not_null() & (pl.col("start") < pl.col("val_horizon"))
    )

    # 4. Apply Clamping Logic
    clamped = valid.with_columns(
        # Clamp start: child can't start before parent
        pl.max_horizontal("start", "val_start").alias("start"),
        # Clamp end: child can't end after parent
        pl.min_horizontal("end", "val_horizon").alias("end"),
    )

    # 5. Filter degenerate intervals and drop temp columns
    return clamped.filter(
        pl.col("end").is_null() | (pl.col("start") < pl.col("end"))
    ).drop(["val_start", "val_horizon"])
