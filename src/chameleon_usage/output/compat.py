"""Backwards-compatible output format.

Canonical mutually exclusive states that partition total:
    total = maintenance + available + idle_reservation + active

Maps from both legacy and current pipeline sources.
"""

import polars as pl

from chameleon_usage.constants import Metrics as M

HOURS_PER_DAY = 24


class CanonicalStates:
    MAINTENANCE = "maintenance"
    AVAILABLE = "available"
    IDLE_RESERVATION = "idle_reservation"
    ACTIVE = "active"


def to_compat_format(df: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeline output (UsageSchema) → backwards-compatible wide format.

    Input: long format with metric in [total, reservable, available, idle, occupied]
    Output: wide format with canonical states as columns, node_type = "unknown"
    """
    wide = (
        df.filter(
            pl.col("metric").is_in(
                [
                    M.TOTAL,
                    M.RESERVABLE,
                    M.AVAILABLE_RESERVABLE,
                    M.IDLE,
                    M.OCCUPIED_RESERVATION,
                ]
            )
        )
        .group_by(["timestamp", "site", "metric"])
        .agg(pl.col("value").sum())
        .collect()
        .pivot(on="metric", index=["timestamp", "site"], values="value")
    )

    return wide.select(
        pl.col("timestamp").alias("date"),
        pl.col("site"),
        pl.lit("unknown").alias("node_type"),
        ((pl.col(M.TOTAL) - pl.col(M.RESERVABLE)) * HOURS_PER_DAY).alias(
            CanonicalStates.MAINTENANCE
        ),
        (pl.col(M.AVAILABLE_RESERVABLE) * HOURS_PER_DAY).alias(
            CanonicalStates.AVAILABLE
        ),
        (pl.col(M.IDLE) * HOURS_PER_DAY).alias(CanonicalStates.IDLE_RESERVATION),
        (pl.col(M.OCCUPIED_RESERVATION) * HOURS_PER_DAY).alias(CanonicalStates.ACTIVE),
        (pl.col(M.TOTAL) * HOURS_PER_DAY).alias("total_hours"),
    ).lazy()
