from dataclasses import dataclass

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure

# Publication styling: single-column journal figure
SINGLE_COL_WIDTH = 3.5  # inches
DOUBLE_COL_WIDTH = 7.0


def set_publication_style():
    sns.set_theme(context="paper", style="ticks", font_scale=1.0)
    plt.rcParams.update(
        {
            "axes.linewidth": 0.5,
            "grid.linewidth": 0.5,
            "lines.linewidth": 1.0,
        }
    )


@dataclass(frozen=True)
class AreaLayer:
    values: list[float]
    color: str
    label: str


@dataclass(frozen=True)
class LineLayer:
    values: list[float]
    color: str
    label: str
    linestyle: str = "-"
    linewidth: float | None = None  # None = use rcParams
    zorder: int | None = None


def _stack_areas(ax: Axes, x: list, areas: list[AreaLayer], **kwargs) -> list[float]:
    """Stack areas on axis, return cumulative total."""
    lower = [0.0] * len(x)
    for area in areas:
        upper = [lo + val for lo, val in zip(lower, area.values)]
        ax.fill_between(
            x,
            lower,
            upper,
            color=area.color,
            label=area.label,
            step="post",
            alpha=kwargs.get("alpha", 0.8),
            **{k: v for k, v in kwargs.items() if k != "alpha"},
        )
        lower = upper
    return lower


def _draw_lines(ax: Axes, x: list, lines: list[LineLayer] | None) -> None:
    for line in lines or []:
        kwargs = {
            "color": line.color,
            "linestyle": line.linestyle,
            "label": line.label,
            "drawstyle": "steps-post",
            "zorder": line.zorder,
        }
        if line.linewidth is not None:
            kwargs["linewidth"] = line.linewidth
        ax.plot(x, line.values, **kwargs)


def _setup_time_axis(fig: Figure, *axes: Axes) -> None:
    for ax in axes:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()


def _save(fig: Figure, path: str | None) -> None:
    if path:
        fig.savefig(path, dpi=300, bbox_inches="tight")


def _bottom_legend(fig: Figure, ax: Axes) -> None:
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=3,
        framealpha=0.9,
        bbox_to_anchor=(0.5, 0.01),
    )
    plt.tight_layout(rect=(0, 0.12, 1, 1))


def plot_stacked_step_with_pct(
    x: list,
    areas: list[AreaLayer],
    lines: list[LineLayer] | None = None,
    *,
    title: str,
    y_label: str,
    denom_values: list[float] | None = None,
    output_path: str | None = None,
) -> Figure:
    set_publication_style()
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(DOUBLE_COL_WIDTH, 4), sharex=True)

    stack_total = _stack_areas(ax1, x, areas)
    _draw_lines(ax1, x, lines)
    ax1.set_ylabel(y_label)
    ax1.set_title(f"{title}: Resource Utilization Over Time")

    # Percentage subplot - use provided denominator or fall back to stack total
    raw_denom = denom_values if denom_values is not None else stack_total
    denom = [v if v != 0 else 1 for v in raw_denom]
    lower_pct = [0.0] * len(x)
    for area in areas:
        pct = [v / d * 100 for v, d in zip(area.values, denom)]
        upper_pct = [lo + val for lo, val in zip(lower_pct, pct)]
        ax2.fill_between(
            x, lower_pct, upper_pct, color=area.color, step="post", alpha=0.8
        )
        lower_pct = upper_pct

    ax2.set_ylabel("Percentage (%)")
    ax2.set_ylim(0, 100)
    ax2.set_title(f"{title}: Percentage Distribution")

    for ax in (ax1, ax2):
        ax.set_xlim(x[0], x[-1])
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        ax.grid(True, alpha=0.3)
        sns.despine(ax=ax)

    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=len(handles),
        fontsize="x-small",
        bbox_to_anchor=(0.5, 0.01),
    )
    plt.tight_layout(rect=(0, 0.08, 1, 1))
    _save(fig, output_path)
    return fig


def plot_multi_site_stacked(
    x: list,
    used_areas: list[AreaLayer],
    available_areas: list[AreaLayer],
    total_line: LineLayer | None = None,
    *,
    title: str,
    output_path: str | None = None,
) -> Figure:
    set_publication_style()
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(DOUBLE_COL_WIDTH, 4), sharex=True)

    # Stack: all used first, then all available
    all_areas = used_areas + available_areas

    _stack_areas(
        ax1, x, all_areas, alpha=0.9, linewidth=0, edgecolor="none", antialiased=False
    )
    if total_line:
        _draw_lines(ax1, x, [total_line])

    ax1.set_ylabel("Nodes / Equiv")
    ax1.set_title(title)

    # Per-site % used in subplot
    for area in used_areas:
        ax2.plot(x, area.values, color=area.color, drawstyle="steps-post")

    ax2.axhline(y=20, color="gray", linestyle="--", linewidth=0.5)
    ax2.set_ylabel("% Used")
    ax2.set_ylim(0, 100)
    ax2.set_title("% Used by Site")

    for ax in (ax1, ax2):
        ax.set_xlim(x[0], x[-1])
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        ax.grid(True, alpha=0.3)
        sns.despine(ax=ax)

    # Custom 2-row legend: Available row on top, Used row on bottom
    # matplotlib fills legends column-by-column, so we interleave accordingly
    from matplotlib.patches import Patch
    avail_label = Patch(facecolor="none", edgecolor="none", label="Available:")
    used_label = Patch(facecolor="none", edgecolor="none", label="Used:")

    # Interleave: [labels, then site pairs] so columns are (label, site1, site2, site3)
    handles = [avail_label, used_label]
    for avail, used in zip(available_areas, used_areas):
        handles.append(Patch(facecolor=avail.color, label=avail.label))
        handles.append(Patch(facecolor=used.color, label=used.label))

    ncol = len(used_areas) + 1

    fig.legend(
        handles,
        [h.get_label() for h in handles],
        loc="lower center",
        ncol=ncol,
        fontsize="x-small",
        bbox_to_anchor=(0.5, 0.01),
        handlelength=1.5,
        columnspacing=1.0,
    )
    plt.tight_layout(rect=(0, 0.12, 1, 1))
    _save(fig, output_path)
    return fig


def plot_diff_comparison(
    x: list,
    series_a: list[float],
    series_b: list[float],
    lines: list[LineLayer] | None = None,
    *,
    title: str,
    y_label: str,
    diff_colors: tuple[str, str] = ("#9CC9FF", "#FDD9A0"),
    diff_labels: tuple[str, str] = ("B > A", "A > B"),
    output_path: str | None = None,
) -> Figure:
    set_publication_style()
    arr_a, arr_b = np.asarray(series_a), np.asarray(series_b)
    fig, ax = plt.subplots(1, 1, figsize=(DOUBLE_COL_WIDTH, 2.5))

    ax.fill_between(
        x,
        arr_a,
        arr_b,
        where=arr_b > arr_a,
        color=diff_colors[0],
        alpha=0.45,
        step="post",
        label=diff_labels[0],
    )
    ax.fill_between(
        x,
        arr_a,
        arr_b,
        where=arr_a > arr_b,
        color=diff_colors[1],
        alpha=0.45,
        step="post",
        label=diff_labels[1],
    )
    _draw_lines(ax, x, lines)

    ax.set_ylabel(y_label)
    ax.set_title(title)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.grid(True, alpha=0.3)
    sns.despine(ax=ax)
    ax.legend(
        loc="upper center", bbox_to_anchor=(0.5, -0.15), ncol=3, fontsize="x-small"
    )
    plt.tight_layout()
    _save(fig, output_path)
    return fig
