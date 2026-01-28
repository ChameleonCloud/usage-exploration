import polars as pl

from chameleon_usage.common import PipelineOutput, SiteConfig


def agg_by(
    df: pl.DataFrame,
    agg: pl.Expr,
    group_by: list[str],
    name: str,
) -> pl.DataFrame:
    """Aggregate df by group_by columns. Empty list = aggregate all rows."""
    if not group_by:
        return df.select(agg.alias(name))
    return df.group_by(group_by).agg(agg.alias(name))


def check_invariant(
    raw: pl.DataFrame,
    valid: pl.DataFrame,
    audit: pl.DataFrame,
    agg: pl.Expr,
    group_by: list[str],
) -> pl.DataFrame:
    """Check raw == valid + audit for any aggregation and grouping."""
    raw_agg = agg_by(raw, agg, group_by, "raw")
    valid_agg = agg_by(valid, agg, group_by, "valid")
    audit_agg = agg_by(audit, agg, group_by, "audit")

    if not group_by:
        combined = pl.concat([raw_agg, valid_agg, audit_agg], how="horizontal")
    else:
        combined = raw_agg.join(valid_agg, on=group_by, how="full", coalesce=True).join(
            audit_agg, on=group_by, how="full", coalesce=True
        )

    return (
        combined.fill_null(0)
        .with_columns(match=(pl.col("raw") == pl.col("valid") + pl.col("audit")))
        .filter(~pl.col("match"))
    )


def summarize_audit_data(
    valid: pl.DataFrame,
    audit: pl.DataFrame,
    group_by: list[str] = [],
    bucket: str | None = None,  # e.g., "1y", "1mo", "1w", "1d"
):
    """
    Analyze audit data in context of full set.
    We run invariant checks first to ensure no leaks.
    Then use tagged columns from valid and audit dfs.
    """

    if not group_by:
        group_by = ["site", "source", "data_status"]

    # tag so we can group by data status together
    valid = valid.with_columns(
        data_status=pl.lit("valid"),
        time=pl.col("start"),
    )

    audit = audit.with_columns(
        time=pl.col("start_date"),
    )

    # many, many nulls after this concat, but we'll see :)
    total_data = pl.concat([valid, audit], how="diagonal")

    # add time bucket if requested
    if bucket:
        # rounds datetime column down to the nearest "bucket length"
        total_data = total_data.with_columns(bucket=pl.col("time").dt.truncate(bucket))
        group_by = group_by + ["bucket"]

    return agg_by(total_data, pl.len(), group_by, "rows")


def run_audit_checks(data: PipelineOutput):
    raw = data.raw_spans.collect()
    valid = data.valid_spans.collect()
    audit = data.audit_spans.collect()

    all_passed = True

    # Check row counts invariant
    failures = check_invariant(raw, valid, audit, pl.len(), [])
    if failures.height > 0:
        print("FAIL: total rows")
        print(failures)
        all_passed = False

    # Rows by source
    failures = check_invariant(raw, valid, audit, pl.len(), ["source"])
    if failures.height > 0:
        print("FAIL: rows by source")
        print(failures)
        all_passed = False

    if all_passed:
        print("All invariants passed")

    result = summarize_audit_data(valid, audit, bucket="1y").sort(
        by=["site", "rows"], descending=[False, True]
    )
    return result
