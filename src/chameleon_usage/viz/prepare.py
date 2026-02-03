from __future__ import annotations

import polars as pl

from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.constants import SchemaCols as S
from chameleon_usage.viz.matplotlib_plots import ResourceSeries, SiteSeries


def prepare_resource_series(
    usage: pl.DataFrame | pl.LazyFrame,
    *,
    site: str,
    resource: str,
    collector_type: str = "current",
) -> ResourceSeries:
    df = _to_wide(usage, site=site, resource=resource, collector_type=collector_type)
    if df.height == 0:
        return ResourceSeries(
            timestamps=[],
            total=[],
            reservable=[],
            active_reserved=[],
            active_ondemand=[],
            idle=[],
            available=[],
        )

    return ResourceSeries(
        timestamps=_timestamps(df),
        total=_series(df, QT.TOTAL),
        reservable=_series(df, QT.RESERVABLE),
        active_reserved=_series(df, QT.OCCUPIED_RESERVATION),
        active_ondemand=_series(df, QT.OCCUPIED_ONDEMAND),
        idle=_series(df, QT.IDLE),
        available=_sum_lists(
            _series(df, QT.AVAILABLE_RESERVABLE),
            _series(df, QT.AVAILABLE_ONDEMAND),
        ),
    )


def prepare_site_series(
    usage: pl.DataFrame | pl.LazyFrame,
    *,
    sites: list[str],
    resource: str,
    collector_type: str = "current",
) -> list[SiteSeries]:
    output: list[SiteSeries] = []
    for site in sites:
        df = _to_wide(
            usage, site=site, resource=resource, collector_type=collector_type
        )
        if df.height == 0:
            continue
        output.append(
            SiteSeries(
                name=site,
                timestamps=_timestamps(df),
                capacity=_series(df, QT.TOTAL),
                occupied=_sum_lists(
                    _series(df, QT.OCCUPIED_RESERVATION),
                    _series(df, QT.OCCUPIED_ONDEMAND),
                ),
                available=_sum_lists(
                    _series(df, QT.AVAILABLE_RESERVABLE),
                    _series(df, QT.AVAILABLE_ONDEMAND),
                ),
            )
        )
    return output


def _to_wide(
    usage: pl.DataFrame | pl.LazyFrame,
    *,
    site: str,
    resource: str,
    collector_type: str,
) -> pl.DataFrame:
    if isinstance(usage, pl.LazyFrame):
        usage = usage.collect()

    subset = usage.filter(
        pl.col("site") == site,
        pl.col("resource") == resource,
        pl.col("collector_type") == collector_type,
    )
    if subset.is_empty():
        return pl.DataFrame({S.TIMESTAMP: []})

    return (
        subset.pivot(
            values=S.VALUE,
            index=S.TIMESTAMP,
            on=S.METRIC,
            aggregate_function="first",
        )
        .sort(S.TIMESTAMP)
        .fill_null(0)
    )


def _series(df: pl.DataFrame, name: str) -> list[float]:
    if name in df.columns:
        return df.get_column(name).fill_null(0).to_list()
    return [0.0] * df.height


def _timestamps(df: pl.DataFrame) -> list:
    return df.get_column(S.TIMESTAMP).to_list()


def _sum_lists(*lists: list[float]) -> list[float]:
    return [sum(values) for values in zip(*lists)]
