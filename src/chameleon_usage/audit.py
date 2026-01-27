# Audit and invariant checking for span data
# Run per-site to verify data integrity and report on rejected rows

from datetime import datetime
from pathlib import Path

import polars as pl


def check_row_invariant(
    raw_df: pl.DataFrame,
    valid_df: pl.DataFrame,
    audit_df: pl.DataFrame,
    site: str,
) -> bool:
    """Verify: raw_rows = valid_rows + audit_rows per source.

    Returns True if invariant holds, False otherwise.
    """
    raw_counts = raw_df.group_by("source").len().rename({"len": "raw"})
    valid_counts = valid_df.group_by("source").len().rename({"len": "valid"})
    audit_counts = audit_df.group_by("source").len().rename({"len": "audit"})

    check = (
        raw_counts
        .join(valid_counts, on="source", how="full", coalesce=True)
        .join(audit_counts, on="source", how="full", coalesce=True)
        .fill_null(0)
        .with_columns(
            computed=(pl.col("valid") + pl.col("audit")),
            match=(pl.col("raw") == (pl.col("valid") + pl.col("audit"))),
        )
        .sort("source")
    )

    violations = check.filter(~pl.col("match"))
    if violations.height > 0:
        print(f"\n=== {site}: ROW INVARIANT VIOLATED ===")
        print(violations)
        return False

    total_raw = check["raw"].sum()
    total_valid = check["valid"].sum()
    total_audit = check["audit"].sum()
    print(f"\n=== {site}: ROW INVARIANT OK ===")
    print(f"    {total_raw:,} raw = {total_valid:,} valid + {total_audit:,} audit")
    return True


def report_hours(
    valid_df: pl.DataFrame,
    audit_df: pl.DataFrame,
    site: str,
) -> None:
    """Report span-hours for valid and audit data.

    Note: raw_df doesn't have calc_end (computed during validation),
    so we can't compute raw hours directly. Row invariant ensures no rows lost.
    """
    # valid uses start/end, audit uses start_date/calc_end
    valid_hours = (
        valid_df
        .with_columns(hours=((pl.col("end") - pl.col("start")).dt.total_hours()))
        .group_by("source")
        .agg(pl.col("hours").sum().alias("valid"))
    )
    audit_hours = (
        audit_df
        .with_columns(hours=((pl.col("calc_end") - pl.col("start_date")).dt.total_hours()))
        .group_by("source")
        .agg(pl.col("hours").sum().alias("audit"))
    )

    check = (
        valid_hours
        .join(audit_hours, on="source", how="full", coalesce=True)
        .fill_null(0.0)
        .with_columns(total=(pl.col("valid") + pl.col("audit")))
        .sort("source")
    )

    total_valid = check["valid"].sum()
    total_audit = check["audit"].sum()
    total = check["total"].sum()
    print(f"\n=== {site}: SPAN-HOURS ===")
    print(f"    {total:,.1f} total hrs = {total_valid:,.1f} valid + {total_audit:,.1f} audit")
    with pl.Config(tbl_rows=-1):
        print(check)


def format_audit_summary(
    audit_df: pl.DataFrame,
    valid_df: pl.DataFrame,
    site: str,
    window_start: datetime,
    window_end: datetime,
) -> None:
    """Print audit summary for a single site, filtered to window."""
    # Filter to rows that overlap the analysis window
    audit_df = audit_df.filter(
        (pl.col("start_date") < window_end)
        & ((pl.col("start_date") >= window_start) | pl.col("start_date").is_null())
    )
    valid_df = valid_df.filter(
        (pl.col("start") < window_end) & (pl.col("start") >= window_start)
    )

    if audit_df.height == 0:
        print(f"\n=== {site}: no rejected rows in window ===")
        return

    # Count rows per source
    valid_counts = valid_df.group_by("source").len().rename({"len": "valid"})
    rejected_counts = audit_df.group_by("source").len().rename({"len": "rejected"})

    totals = (
        valid_counts
        .join(rejected_counts, on="source", how="full", coalesce=True)
        .fill_null(0)
        .with_columns(total=(pl.col("valid") + pl.col("rejected")))
    )

    rejected = audit_df.height
    total = totals["total"].sum()

    summary = (
        audit_df.with_columns(year=pl.col("start_date").dt.year())
        .group_by("source", "data_status", "year")
        .len()
        .join(totals.select("source", "total"), on="source")
        .with_columns((pl.col("len") / pl.col("total") * 100).round(1).alias("pct"))
        .drop("total")
        .sort(
            ["year", "pct", "source", "data_status"],
            descending=[True, True, False, False],
        )
        .select(
            pl.col("year").alias("Start Year"),
            pl.col("source").alias("Span Type"),
            pl.col("data_status").alias("DQ Category"),
            pl.col("len").alias("# Affected Rows"),
            pl.col("pct").alias("% Of Total Rows"),
        )
    )

    print(f"\n=== {site}: {rejected:,} rejected / {total:,} total ===")
    with pl.Config(tbl_rows=-1):
        print(summary)


def run_site_audit(
    raw_df: pl.DataFrame,
    valid_df: pl.DataFrame,
    audit_df: pl.DataFrame,
    site: str,
    window_start: datetime,
    window_end: datetime,
    output_dir: Path | None = None,
) -> bool:
    """Run all audit checks for a single site.

    Returns True if row invariant passes, False otherwise.
    """
    rows_ok = check_row_invariant(raw_df, valid_df, audit_df, site)
    report_hours(valid_df, audit_df, site)

    format_audit_summary(audit_df, valid_df, site, window_start, window_end)

    if output_dir:
        audit_path = output_dir / f"{site}_audit.parquet"
        audit_df.write_parquet(audit_path)
        print(f"wrote: {audit_path}")

    return rows_ok
