import polars as pl

from chameleon_usage.constants import Metrics as M
from chameleon_usage.constants import SchemaCols as S
from chameleon_usage.viz.plots import (
    AreaLayer,
    LineLayer,
    PlotAnnotation,
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
    C.OCCUPIED_RESERVATION: ("#2DA02D", "In Use (Reserved)"),
    C.OCCUPIED_ONDEMAND: ("#90EE91", "In Use (On-demand)"),
    C.IDLE: ("#1f77b4", "Reserved"),
    C.AVAILABLE: ("#88CEEB", "Idle"),
    C.AVAILABLE_RESERVABLE: ("#88CEEB", "Idle (Reservable)"),
    C.AVAILABLE_ONDEMAND: ("#88CEEB", "Idle (On-demand)"),
    C.TOTAL: ("#494949", "Total"),
    C.RESERVABLE: ("#6B6F71", "Reservable Pool"),
    C.RESERVABLE_LEGACY: ("#1f77b4", "Usable"),
}

UTIL_AREA_COLS = [
    C.OCCUPIED_ONDEMAND,
    C.OCCUPIED_RESERVATION,
    C.IDLE,
]
LINE_COLS = [C.TOTAL, C.RESERVABLE, C.RESERVABLE_LEGACY]
SITE_COLORS = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]
# Lighter versions for "available" areas
SITE_COLORS_LIGHT = ["#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5"]
SITE_ORDER = ["chi_tacc", "chi_uc", "kvm_tacc"]
SITE_STYLE = {
    "chi_tacc": {
        "used_color": "#1f77b4",
        "available_color": "#aec7e8",
        "label": "CHI@TACC",
    },
    "chi_uc": {
        "used_color": "#ff7f0e",
        "available_color": "#ffbb78",
        "label": "CHI@UC",
    },
    "kvm_tacc": {
        "used_color": "#2ca02c",
        "available_color": "#98df8a",
        "label": "KVM@TACC",
    },
}


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


DENOM_COLS = {
    "total": C.TOTAL,
    "reservable": C.RESERVABLE,
    "reservable_legacy": C.RESERVABLE_LEGACY,
}


def _make_filename(
    output_dir: str,
    site_name: str,
    resource: str,
    suffix: str,
    time_range: tuple | None = None,
    bucket: str | None = None,
) -> str:
    parts = [site_name, resource]
    if time_range:
        start, end = time_range
        parts.append(f"{start:%Y%m%d}-{end:%Y%m%d}")
    if bucket:
        parts.append(bucket)
    parts.append(suffix)
    return f"{output_dir}/{'_'.join(parts)}.png"


def plot_stacked_usage(
    wide: pl.DataFrame,
    site_name: str,
    resource: str,
    output_dir: str,
    include_ondemand: bool = True,
    merge_reserved: bool = False,
    pct_denom: str | None = None,
    show_pct: bool = True,
    title: str | None = None,
    y_label: str | None = None,
    time_range: tuple | None = None,
    bucket: str | None = None,
) -> None:
    df = _filter(wide, site_name, resource)
    if df.is_empty():
        return

    x = df.get_column(S.TIMESTAMP).to_list()
    # Default denominator: "total" for on-demand sites, "reservable" for reservation-only
    if pct_denom is None:
        pct_denom = "total" if include_ondemand else "reservable"
    denom_col = DENOM_COLS.get(pct_denom, C.RESERVABLE)
    denom_values = _col(df, denom_col)

    if include_ondemand:
        area_cols = [c for c in UTIL_AREA_COLS if not (merge_reserved and c == C.IDLE)]
        available = _sum(
            _col(df, C.AVAILABLE_RESERVABLE), _col(df, C.AVAILABLE_ONDEMAND)
        )
        areas = []
        for col in area_cols:
            values = _col(df, col)
            if merge_reserved and col == C.OCCUPIED_RESERVATION:
                values = _sum(values, _col(df, C.IDLE))
            color, label = STYLES[col]
            areas.append(AreaLayer(values, color, label))
        lines = [
            _line(df, C.TOTAL, zorder=10),
            _line(df, C.RESERVABLE, linestyle="--", zorder=9),
        ]
    else:
        area_cols = [c for c in UTIL_AREA_COLS if c != C.OCCUPIED_ONDEMAND]
        if merge_reserved:
            area_cols = [c for c in area_cols if c != C.IDLE]
        available = _col(df, C.AVAILABLE_RESERVABLE)
        areas = []
        for col in area_cols:
            values = _col(df, col)
            color, label = STYLES[col]
            if col == C.OCCUPIED_RESERVATION:
                label = "In Use"
                if merge_reserved:
                    values = _sum(values, _col(df, C.IDLE))
            areas.append(AreaLayer(values, color, label))
        # Single "Total" line using reservable
        lines = [LineLayer(_col(df, C.RESERVABLE), "#333333", "Total", zorder=10)]

    color, label = STYLES[C.AVAILABLE]
    areas.append(
        AreaLayer(available, color, label, edgecolor="#000000", edgewidth=0.15)
    )

    plot_stacked_step_with_pct(
        x,
        areas,
        lines,
        title=title or f"{site_name} - {resource}",
        y_label=y_label or resource,
        denom_values=denom_values,
        show_pct=show_pct,
        output_path=_make_filename(
            output_dir, site_name, resource, "util", time_range, bucket
        ),
    )


def plot_site_comparison(
    wide: pl.DataFrame,
    site_names: list[str],
    resource: str,
    output_dir: str,
    time_range: tuple | None = None,
    bucket: str | None = None,
    annotations: list[PlotAnnotation] | None = None,
) -> None:
    used_areas: list[AreaLayer] = []
    available_areas: list[AreaLayer] = []
    total_values: list[float] = []
    x: list | None = None

    site_order_idx = {name: i for i, name in enumerate(SITE_ORDER)}
    original_idx = {name: i for i, name in enumerate(site_names)}
    ordered_sites = sorted(
        site_names,
        key=lambda name: (
            site_order_idx.get(name, len(SITE_ORDER)),
            original_idx[name],
        ),
    )

    for i, site in enumerate(ordered_sites):
        df = _filter(wide, site, resource)
        if df.is_empty():
            continue

        if x is None:
            x = df.get_column(S.TIMESTAMP).to_list()

        # Used = occupied + idle (committed resources)
        used = _sum(
            _col(df, C.OCCUPIED_RESERVATION),
            _sum(_col(df, C.OCCUPIED_ONDEMAND), _col(df, C.IDLE)),
        )
        # Available = available_reservable + available_ondemand
        if site.startswith("kvm"):
            available = _sum(
                _col(df, C.AVAILABLE_RESERVABLE), _col(df, C.AVAILABLE_ONDEMAND)
            )
        else:
            available = _col(df, C.AVAILABLE_RESERVABLE)

        # For CHI sites use reservable (total can be erroneously high), for KVM use total
        capacity_col = C.TOTAL if site.startswith("kvm") else C.RESERVABLE
        capacity = _col(df, capacity_col)
        style = SITE_STYLE.get(site)
        color = style["used_color"] if style else SITE_COLORS[i % len(SITE_COLORS)]
        available_color = (
            style["available_color"]
            if style
            else SITE_COLORS_LIGHT[i % len(SITE_COLORS_LIGHT)]
        )
        base_label = style["label"] if style else site

        if not total_values:
            total_values = [0.0] * len(capacity)
        total_values = _sum(total_values, capacity)

        used_areas.append(AreaLayer(used, color, base_label))
        available_areas.append(AreaLayer(available, available_color, base_label))

    if not used_areas:
        return

    if x is None:
        return

    total_line = LineLayer(
        total_values, "#000000", "Total Capacity", linewidth=1.5, zorder=10
    )

    plot_multi_site_stacked(
        x,
        used_areas,
        available_areas,
        total_line,
        title="Utilization by Site (Stacked Used + Available)",
        annotations=annotations,
        output_path=_make_filename(
            output_dir, "sites", resource, "comparison", time_range, bucket
        ),
    )


def plot_collector_comparison(
    wide: pl.DataFrame,
    site_name: str,
    resource: str,
    output_dir: str,
    time_range: tuple | None = None,
    bucket: str | None = None,
) -> None:
    df = _filter(wide, site_name, resource)
    if df.is_empty():
        return

    required = [C.TOTAL, C.RESERVABLE, C.RESERVABLE_LEGACY]
    if not all(c in df.columns for c in required):
        return

    x = df.get_column(S.TIMESTAMP).to_list()
    lines = [
        _line(
            df,
            col,
            linewidth=1,
            zorder=10 - i,
            linestyle="--" if col == C.RESERVABLE else "-",
        )
        for i, col in enumerate(LINE_COLS)
    ]

    plot_diff_comparison(
        x,
        _col(df, C.RESERVABLE),
        _col(df, C.RESERVABLE_LEGACY),
        lines,
        title=f"{site_name} - {resource}: Collector Comparison",
        y_label=resource,
        diff_labels=("Missing History", "Maintenance (untracked)"),
        output_path=_make_filename(
            output_dir, site_name, resource, "collector_compare", time_range, bucket
        ),
    )
