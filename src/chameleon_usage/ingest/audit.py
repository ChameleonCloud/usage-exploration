"""Audit-table helpers.

Two pure transforms that convert append-only audit event rows into the
interval shape the existing ``Adapter`` class expects (``start``/``end``
columns).  See SPEC.md §4 for the algorithm.

Usage: call ``audit_to_intervals`` then ``extract_json_fields`` in an
adapter *source* function, and feed the result into a standard ``Adapter``
with ``start_col="start"`` and ``end_col="end"``.
"""

import polars as pl


def audit_to_intervals(
    df: pl.LazyFrame,
    entity_col: str = "id",
) -> pl.LazyFrame:
    """Convert audit event rows into intervals via lead() window.

    Uses ``audit_event_time`` (when the change happened in the source table)
    for interval boundaries, not ``audit_changed_at`` (when the audit row
    was physically inserted).  The next row's event time becomes ``end``.
    DELETE rows only serve as end-markers and are dropped from output.

    Fully vectorized: sort → shift → rename → filter.
    """
    return (
        df.sort(entity_col, "audit_event_time")
        .with_columns(
            pl.col("audit_event_time").shift(-1).over(entity_col).alias("end"),
        )
        .rename({"audit_event_time": "start"})
        .filter(pl.col("audit_event_type") != "DELETE")
    )


def extract_json_fields(
    df: pl.LazyFrame,
    fields: list[str],
    json_col: str = "data",
) -> pl.LazyFrame:
    """Extract named fields from a JSON string column.

    Each field becomes a new ``Utf8`` column (cast downstream as needed).
    """
    return df.with_columns(
        pl.col(json_col).str.json_path_match(f"$.{field}").alias(field)
        for field in fields
    )
