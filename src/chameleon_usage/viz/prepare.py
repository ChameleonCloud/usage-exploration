from __future__ import annotations

from pathlib import Path
from typing import Literal

import polars as pl

from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.constants import SchemaCols as S
from chameleon_usage.viz.matplotlib_plots import (
    LegacyComparisonSeries,
    ResourceSeries,
    SiteSeries,
    do_plot_collector_comparison,
    do_plot_site_comparison,
    do_plot_stacked_usage,
)

PlotType = Literal["resource", "site_comparison", "legacy_comparison"]


def to_wide(usage: pl.DataFrame) -> pl.DataFrame:
    """Transform long format to wide format once. Metrics become columns like 'total_current'."""
    return (
        usage.with_columns(
            (pl.col(S.METRIC) + "_" + pl.col("collector_type")).alias("metric_collector")
        )
        .pivot(
            values=S.VALUE,
            index=[S.TIMESTAMP, "site", "resource"],
            on="metric_collector",
        )
        .sort(S.TIMESTAMP)
        .fill_null(0)
    )


def plot_stacked_usage(
    wide: pl.DataFrame, site_name: str, resource: str, output_dir: str
) -> None:
    df = wide.filter(
        pl.col("site") == site_name,
        pl.col("resource") == resource,
    )
    if df.is_empty():
        return

    series = ResourceSeries(
        timestamps=df.get_column(S.TIMESTAMP).to_list(),
        total=_col(df, f"{QT.TOTAL}_current"),
        reservable=_col(df, f"{QT.RESERVABLE}_current"),
        active_reserved=_col(df, f"{QT.OCCUPIED_RESERVATION}_current"),
        active_ondemand=_col(df, f"{QT.OCCUPIED_ONDEMAND}_current"),
        idle=_col(df, f"{QT.IDLE}_current"),
        available=_sum(
            _col(df, f"{QT.AVAILABLE_RESERVABLE}_current"),
            _col(df, f"{QT.AVAILABLE_ONDEMAND}_current"),
        ),
    )
    if not series.timestamps:
        return

    title = f"{site_name} - {resource}"
    filename = f"{site_name}_{resource}_util.png"
    do_plot_stacked_usage(
        series, title=title, y_label=resource, output_path=f"{output_dir}/{filename}"
    )


def plot_site_comparison(
    wide: pl.DataFrame, site_names: list[str], resource: str, output_dir: str
) -> None:
    site_series: list[SiteSeries] = []

    for site in site_names:
        df = wide.filter(
            pl.col("site") == site,
            pl.col("resource") == resource,
        )
        if df.is_empty():
            continue
        site_series.append(
            SiteSeries(
                name=site,
                timestamps=df.get_column(S.TIMESTAMP).to_list(),
                capacity=_col(df, f"{QT.TOTAL}_current"),
                occupied=_sum(
                    _col(df, f"{QT.COMMITTED}_current"),
                    _col(df, f"{QT.OCCUPIED_ONDEMAND}_current"),
                ),
                available=_sum(
                    _col(df, f"{QT.AVAILABLE_RESERVABLE}_current"),
                    _col(df, f"{QT.AVAILABLE_ONDEMAND}_current"),
                ),
            )
        )

    if not site_series:
        return

    title = "Utilization by Site (Stacked)"
    filename = "sites_comparison.png"
    do_plot_site_comparison(
        site_series,
        title=title,
        occupied_label="Used",
        output_path=f"{output_dir}/{filename}",
    )


def plot_collector_comparison(wide: pl.DataFrame, spec: dict, output_dir: Path) -> None:
    site, resource = spec["site"], spec.get("resource", "node")
    df = wide.filter(pl.col("site") == site, pl.col("resource") == resource)
    if df.is_empty():
        return

    total_cur = f"{QT.TOTAL}_current"
    res_cur = f"{QT.RESERVABLE}_current"
    res_leg = f"{QT.RESERVABLE}_legacy"

    if not all(c in df.columns for c in [total_cur, res_cur, res_leg]):
        return

    series = LegacyComparisonSeries(
        timestamps=df.get_column(S.TIMESTAMP).to_list(),
        current_total=_col(df, total_cur),
        current_reservable=_col(df, res_cur),
        legacy_reservable=_col(df, res_leg),
    )

    title = spec.get("title", f"{site} - nodes")
    filename = spec.get("filename", f"{site}_reservable_compare.png")
    do_plot_collector_comparison(
        series, title=title, output_path=str(output_dir / filename)
    )


def _col(df: pl.DataFrame, name: str) -> list[float]:
    if name in df.columns:
        return df.get_column(name).fill_null(0).to_list()
    return [0.0] * df.height


def _sum(*lists: list[float]) -> list[float]:
    return [sum(values) for values in zip(*lists)]
