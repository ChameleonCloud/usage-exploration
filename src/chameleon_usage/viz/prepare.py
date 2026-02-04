import polars as pl

from chameleon_usage.constants import Metrics as M
from chameleon_usage.constants import SchemaCols as S
from chameleon_usage.viz.plots import (
    AreaLayer,
    LineLayer,
    plot_diff_comparison,
    plot_multi_site_stacked,
    plot_stacked_step_with_pct,
)


class C:
    """Wide-format column names for plotting."""

    TOTAL = f"{M.TOTAL}_current"
    RESERVABLE = f"{M.RESERVABLE}_current"
    COMMITTED = f"{M.COMMITTED}_current"
    OCCUPIED_RESERVATION = f"{M.OCCUPIED_RESERVATION}_current"
    OCCUPIED_ONDEMAND = f"{M.OCCUPIED_ONDEMAND}_current"
    IDLE = f"{M.IDLE}_current"
    AVAILABLE_RESERVABLE = f"{M.AVAILABLE_RESERVABLE}_current"
    AVAILABLE_ONDEMAND = f"{M.AVAILABLE_ONDEMAND}_current"
    RESERVABLE_LEGACY = f"{M.RESERVABLE}_legacy"
    AVAILABLE = "available"  # synthetic: sum of reservable + ondemand


# Visual styles: column → (color, label)
STYLES = {
    C.OCCUPIED_RESERVATION: ("#2ca02c", "Active (Reserved)"),
    C.OCCUPIED_ONDEMAND: ("#98df8a", "Active (On-demand)"),
    C.IDLE: ("#1f77b4", "Idle (Reserved)"),
    C.AVAILABLE: ("#aec7e8", "Available"),
    C.AVAILABLE_RESERVABLE: ("#17becf", "Available (Reservable)"),
    C.AVAILABLE_ONDEMAND: ("#aec7e8", "Available (On-demand)"),
    C.TOTAL: ("#333333", "Total"),
    C.RESERVABLE: ("#333333", "Reservable"),
    C.RESERVABLE_LEGACY: ("#1f77b4", "Legacy Reservable"),
}

UTIL_AREA_COLS = [C.OCCUPIED_RESERVATION, C.OCCUPIED_ONDEMAND, C.IDLE]
LINE_COLS = [C.TOTAL, C.RESERVABLE, C.RESERVABLE_LEGACY]
SITE_COLORS = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]


def _filter(wide: pl.DataFrame, site: str, resource: str) -> pl.DataFrame:
    return wide.filter(pl.col("site") == site, pl.col("resource") == resource)


def _col(df: pl.DataFrame, name: str) -> list[float]:
    if name in df.columns:
        return df.get_column(name).fill_null(0).to_list()
    return [0.0] * df.height


def _sum(*lists: list[float]) -> list[float]:
    return [sum(values) for values in zip(*lists)]


def _area(df: pl.DataFrame, col: str) -> AreaLayer:
    color, label = STYLES[col]
    return AreaLayer(_col(df, col), color, label)


def _line(df: pl.DataFrame, col: str, **kwargs) -> LineLayer:
    color, label = STYLES[col]
    return LineLayer(_col(df, col), color, label, **kwargs)


def plot_stacked_usage(
    wide: pl.DataFrame, site_name: str, resource: str, output_dir: str
) -> None:
    df = _filter(wide, site_name, resource)
    if df.is_empty():
        return

    x = df.get_column(S.TIMESTAMP).to_list()
    available = _sum(_col(df, C.AVAILABLE_RESERVABLE), _col(df, C.AVAILABLE_ONDEMAND))

    areas = [_area(df, col) for col in UTIL_AREA_COLS]
    color, label = STYLES[C.AVAILABLE]
    areas.append(AreaLayer(available, color, label))

    lines = [
        _line(df, C.TOTAL, zorder=10),
        _line(df, C.RESERVABLE, linestyle="--", zorder=9),
    ]

    plot_stacked_step_with_pct(
        x,
        areas,
        lines,
        title=f"{site_name} - {resource}",
        y_label=resource,
        output_path=f"{output_dir}/{site_name}_{resource}_util.png",
    )


def plot_site_comparison(
    wide: pl.DataFrame, site_names: list[str], resource: str, output_dir: str
) -> None:
    site_stacks: list[tuple[str, list[AreaLayer]]] = []
    total_values: list[float] = []

    for i, site in enumerate(site_names):
        df = _filter(wide, site, resource)
        if df.is_empty():
            continue

        occupied = _sum(_col(df, C.COMMITTED), _col(df, C.OCCUPIED_ONDEMAND))
        capacity = _col(df, C.TOTAL)
        color = SITE_COLORS[i % len(SITE_COLORS)]

        if not total_values:
            total_values = [0.0] * len(capacity)
        total_values = _sum(total_values, capacity)

        area = AreaLayer(occupied, color, f"{site} (used)")
        site_stacks.append((site, [area]))

    if not site_stacks:
        return

    x = _filter(wide, site_names[0], resource).get_column(S.TIMESTAMP).to_list()
    color, _ = STYLES[C.TOTAL]
    total_line = LineLayer(total_values, color, "Total Capacity", zorder=10)

    plot_multi_site_stacked(
        x,
        site_stacks,
        total_line,
        title=f"Utilization by Site - {resource}",
        output_path=f"{output_dir}/sites_{resource}_comparison.png",
    )


def plot_collector_comparison(
    wide: pl.DataFrame, site_name: str, resource: str, output_dir: str
) -> None:
    df = _filter(wide, site_name, resource)
    if df.is_empty():
        return

    required = [C.TOTAL, C.RESERVABLE, C.RESERVABLE_LEGACY]
    if not all(c in df.columns for c in required):
        return

    x = df.get_column(S.TIMESTAMP).to_list()
    lines = [
        _line(df, col, linewidth=2.5 if i == 0 else 2, zorder=10 - i)
        for i, col in enumerate(LINE_COLS)
    ]

    plot_diff_comparison(
        x,
        _col(df, C.RESERVABLE),
        _col(df, C.RESERVABLE_LEGACY),
        lines,
        title=f"{site_name} - {resource}: Collector Comparison",
        y_label=resource,
        diff_labels=("Legacy > Nova", "Nova > Legacy"),
        output_path=f"{output_dir}/{site_name}_{resource}_collector_compare.png",
    )
