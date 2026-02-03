from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Iterable

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib import colors as mcolors
from matplotlib.figure import Figure


@dataclass(frozen=True)
class SiteSeries:
    name: str
    timestamps: list
    capacity: list[float]
    occupied: list[float]
    available: list[float]


@dataclass(frozen=True)
class ResourceSeries:
    timestamps: list
    total: list[float]
    reservable: list[float]
    active_reserved: list[float]
    active_ondemand: list[float]
    idle: list[float]
    available: list[float]


def plot_resource_utilization(
    series: ResourceSeries,
    *,
    title: str,
    y_label: str,
    output_path: str | None = None,
) -> Figure:
    x = series.timestamps
    active_reserved = series.active_reserved
    active_ondemand = series.active_ondemand
    idle = series.idle
    available = series.available
    total = series.total
    reservable = series.reservable
    active_total = _sum_lists(active_reserved, active_ondemand)
    used_total = _sum_lists(active_total, idle)
    stack_total = _sum_lists(used_total, available)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    ax1.fill_between(
        x,
        0,
        active_reserved,
        label="Active (Reserved)",
        color="#2ca02c",
        step="post",
        alpha=0.8,
    )
    ax1.fill_between(
        x,
        active_reserved,
        active_total,
        label="Active (On-Demand)",
        color="#98df8a",
        step="post",
        alpha=0.8,
    )
    ax1.fill_between(
        x,
        active_total,
        used_total,
        label="Idle (Reserved)",
        color="#1f77b4",
        step="post",
        alpha=0.8,
    )
    ax1.fill_between(
        x,
        used_total,
        stack_total,
        label="Available",
        color="#87CEEB",
        step="post",
        alpha=0.8,
    )

    ax1.plot(
        x,
        total,
        color="#333333",
        linewidth=2,
        label="Total Capacity",
        drawstyle="steps-post",
        zorder=10,
    )
    ax1.plot(
        x,
        reservable,
        color="#666666",
        linewidth=2,
        linestyle="--",
        label="Reservable Capacity",
        drawstyle="steps-post",
        zorder=10,
    )

    ax1.set_ylabel(y_label)
    ax1.set_title(f"{title}: Resource Utilization Over Time")
    ax1.grid(True, alpha=0.3)

    denom = [v if v != 0 else 1 for v in stack_total]
    pct_active_reserved = _percent(active_reserved, denom)
    pct_active_ondemand = _percent(active_ondemand, denom)
    pct_idle = _percent(idle, denom)
    pct_available = _percent(available, denom)
    pct_active_total = _sum_lists(pct_active_reserved, pct_active_ondemand)
    pct_used_total = _sum_lists(pct_active_total, pct_idle)
    pct_stack_total = _sum_lists(pct_used_total, pct_available)

    ax2.fill_between(
        x,
        0,
        pct_active_reserved,
        label="Active (Reserved)",
        color="#2ca02c",
        step="post",
        alpha=0.8,
    )
    ax2.fill_between(
        x,
        pct_active_reserved,
        pct_active_total,
        label="Active (On-Demand)",
        color="#98df8a",
        step="post",
        alpha=0.8,
    )
    ax2.fill_between(
        x,
        pct_active_total,
        pct_used_total,
        label="Idle (Reserved)",
        color="#1f77b4",
        step="post",
        alpha=0.8,
    )
    ax2.fill_between(
        x,
        pct_used_total,
        pct_stack_total,
        label="Available",
        color="#87CEEB",
        step="post",
        alpha=0.8,
    )

    ax2.set_ylabel("Percentage (%)")
    ax2.set_xlabel("Date")
    ax2.set_ylim(0, 100)
    ax2.set_title(f"{title}: Percentage Distribution")
    ax2.grid(True, alpha=0.3)

    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    fig.autofmt_xdate()
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=3,
        framealpha=0.9,
        bbox_to_anchor=(0.5, 0.01),
    )
    plt.tight_layout(rect=(0, 0.08, 1, 1))

    if output_path:
        fig.savefig(output_path, dpi=300, bbox_inches="tight")

    return fig


def plot_site_comparison(
    series: Iterable[SiteSeries],
    *,
    output_path: str | None = None,
    title: str = "Utilization by Site (Stacked)",
    occupied_label: str = "Occupied",
) -> Figure:
    series = list(series)
    if not series:
        raise ValueError("No site data available for plot.")

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    timestamps = series[0].timestamps

    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]
    stack_values = []
    stack_colors = []
    stack_labels = []

    for i, site in enumerate(series):
        base_color = colors[i % len(colors)]
        stack_values.append(site.occupied)
        stack_colors.append(base_color)
        stack_labels.append(f"{site.name} - {occupied_label}")

    for i, site in enumerate(series):
        base_color = colors[i % len(colors)]
        stack_values.append(site.available)
        stack_colors.append(_lighten(base_color, factor=0.55))
        stack_labels.append(f"{site.name} - Available")

    lower = [0.0] * len(timestamps)
    for values, color, label in zip(stack_values, stack_colors, stack_labels):
        upper = _sum_lists(lower, values)
        ax1.fill_between(
            timestamps,
            lower,
            upper,
            color=color,
            label=label,
            step="post",
            alpha=0.9,
            linewidth=0,
            edgecolor="none",
            antialiased=False,
        )
        lower = upper

    total_capacity = [0.0] * len(timestamps)
    for site in series:
        total_capacity = _sum_lists(total_capacity, site.capacity)
    ax1.plot(
        timestamps,
        total_capacity,
        color="#333333",
        linewidth=2,
        drawstyle="steps-post",
        label="Total Capacity",
        zorder=10,
    )

    ax1.set_ylabel("Nodes / Equiv")
    ax1.set_title(title)
    ax1.grid(True, alpha=0.3)

    for i, site in enumerate(series):
        cap = [v if v != 0 else 1 for v in site.capacity]
        pct_used = _percent(site.occupied, cap)
        ax2.plot(
            timestamps,
            pct_used,
            color=colors[i % len(colors)],
            linewidth=2,
            drawstyle="steps-post",
        )

    total_used = [0.0] * len(timestamps)
    for site in series:
        total_used = _sum_lists(total_used, site.occupied)
    total_cap_denom = [v if v != 0 else 1 for v in total_capacity]
    pct_total_used = _percent(total_used, total_cap_denom)
    ax2.plot(
        timestamps,
        pct_total_used,
        color="#111111",
        linewidth=2.0,
        drawstyle="steps-post",
    )

    ax2.axhline(y=20, color="gray", linestyle="--", linewidth=1)
    ax2.set_ylabel("% Used")
    ax2.set_xlabel("Date")
    ax2.set_ylim(0, 100)
    ax2.set_title("% Used by Site")
    ax2.grid(True, alpha=0.3)

    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    fig.autofmt_xdate()
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=3,
        framealpha=0.9,
        bbox_to_anchor=(0.5, 0.01),
    )
    plt.tight_layout(rect=(0, 0.08, 1, 1))

    if output_path:
        fig.savefig(output_path, dpi=300, bbox_inches="tight")

    return fig


def _sum_lists(*lists: list[float]) -> list[float]:
    return [sum(values) for values in zip(*lists)]


def _percent(values: list[float], denom: list[float]) -> list[float]:
    return [v / d * 100 if d else 0 for v, d in zip(values, denom)]


def _rolling_mean(values: list[float], timestamps: list, days: int) -> list[float]:
    if len(values) < 2:
        return values

    deltas = []
    for a, b in zip(timestamps[:-1], timestamps[1:]):
        if b > a:
            deltas.append((b - a).total_seconds())
    if not deltas:
        return values

    step = median(deltas)
    window = max(1, round((days * 86400) / step))

    out = []
    total = 0.0
    buf = []
    for v in values:
        buf.append(v)
        total += v
        if len(buf) > window:
            total -= buf.pop(0)
        out.append(total / len(buf))
    return out


def _with_alpha(color: str, alpha: float) -> tuple[float, float, float, float]:
    r, g, b = mcolors.to_rgb(color)
    return (r, g, b, alpha)


def _lighten(color: str, factor: float) -> tuple[float, float, float, float]:
    r, g, b = mcolors.to_rgb(color)
    return (
        r + (1.0 - r) * factor,
        g + (1.0 - g) * factor,
        b + (1.0 - b) * factor,
        1.0,
    )
