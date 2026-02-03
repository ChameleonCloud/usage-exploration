import altair as alt
import polars as pl

from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.constants import ResourceTypes as RT
from chameleon_usage.constants import SchemaCols as S

# Resource type to axis label mapping
RESOURCE_LABELS = {
    RT.NODE: "Nodes",
    RT.VCPUS: "vCPUs",
    RT.MEMORY_MB: "Memory (MB)",
    RT.DISK_GB: "Disk (GB)",
    RT.GPUS: "GPUs",
}

# Chart dimensions
WIDTH = 516

# Standard print font sizes (in points)
FONT_TITLE = 12
FONT_AXIS_LABEL = 10
FONT_TICK = 9
FONT_LEGEND = 9

# Export settings
SCALE_FACTOR = 3

# Shared color scheme for quantity types
QTY_COLORS = {
    QT.TOTAL: "black",
    QT.RESERVABLE: "grey",
    QT.ONDEMAND_CAPACITY: "#9467bd",
    QT.COMMITTED: "#1f77b4",
    QT.AVAILABLE_RESERVABLE: "#aec7e8",
    QT.AVAILABLE_ONDEMAND: "#c5b0d5",
    QT.IDLE: "orange",
    QT.OCCUPIED_RESERVATION: "green",
    QT.OCCUPIED_ONDEMAND: "#2ca02c",
}
QTY_ORDER = [
    QT.TOTAL,
    QT.RESERVABLE,
    # QT.ONDEMAND_CAPACITY,
    QT.AVAILABLE_RESERVABLE,
    QT.AVAILABLE_ONDEMAND,
    # QT.COMMITTED,
    QT.IDLE,
    QT.OCCUPIED_RESERVATION,
    QT.OCCUPIED_ONDEMAND,
]
QTY_COLOR_SCALE = alt.Scale(domain=QTY_ORDER, range=[QTY_COLORS[t] for t in QTY_ORDER])


def usage_stack_plot(
    data: pl.DataFrame,
    stack_metrics: list[str] | None = None,
    line_metrics: list[str] | None = None,
    y_label: str = "Count",
) -> alt.LayerChart:
    """Stacked area chart with overlay lines.

    Args:
        data: Usage timeseries with metric column
        stack_metrics: Mutually exclusive metrics to stack (must sum to reservable).
                       Default: [occupied, idle, available]
        line_metrics: Metrics to show as overlay lines. Default: [total, reservable]
        y_label: Label for y-axis (e.g., "vCPUs", "Memory (MB)")
    """
    if stack_metrics is None:
        stack_metrics = [
            QT.OCCUPIED_ONDEMAND,
            QT.OCCUPIED_RESERVATION,
            QT.IDLE,
            QT.AVAILABLE_ONDEMAND,
            QT.AVAILABLE_RESERVABLE,
        ]
    if line_metrics is None:
        line_metrics = [QT.TOTAL, QT.RESERVABLE]

    base = alt.Chart(data)
    x_time = alt.X(f"{S.TIMESTAMP}:T", axis=alt.Axis(format="%Y", tickCount="year"))

    areas = (
        base.transform_filter(
            alt.FieldOneOfPredicate(field=S.METRIC, oneOf=stack_metrics)
        )
        .transform_calculate(stack_order=f"indexof({stack_metrics}, datum.{S.METRIC})")
        .mark_area(interpolate="step-after")
        .encode(
            x=x_time,
            y=alt.Y(f"{S.VALUE}:Q", stack=True, title=y_label),
            color=alt.Color(f"{S.METRIC}:N", scale=QTY_COLOR_SCALE),
            order=alt.Order("stack_order:Q"),
        )
    )

    lines = (
        base.transform_filter(
            alt.FieldOneOfPredicate(field=S.METRIC, oneOf=line_metrics)
        )
        .mark_line(strokeWidth=2, interpolate="step-after")
        .encode(
            x=x_time,
            y=alt.Y(f"{S.VALUE}:Q", title=y_label),
            color=alt.Color(f"{S.METRIC}:N", scale=QTY_COLOR_SCALE),
        )
    )

    return areas + lines


def usage_line_plot(data: pl.DataFrame, y_label: str = "Count") -> alt.FacetChart:
    fig = (
        alt.Chart(data)
        .mark_line(strokeWidth=1, interpolate="step-after")
        .encode(
            x=alt.X(f"{S.TIMESTAMP}:T", axis=alt.Axis(format="%Y", tickCount="year")),
            y=alt.Y(f"{S.VALUE}:Q", title=y_label),
            color=alt.Color(f"{S.METRIC}:N", scale=QTY_COLOR_SCALE),
        )
        .properties(width=WIDTH, height=150)
        .facet(row=alt.Row("collector_type:N", sort=["current", "legacy"]))
    )

    return fig


def usage_facet_plot(data: pl.DataFrame, y_label: str = "Count") -> alt.VConcatChart:
    """Faceted line plot comparing legacy vs current by metric."""
    charts = []
    for qty_type in QTY_ORDER:
        subset = data.filter(pl.col(S.METRIC) == qty_type)
        if subset.is_empty():
            continue
        qty_color = QTY_COLORS[qty_type]
        chart = (
            alt.Chart(subset)
            .mark_line(strokeWidth=1, interpolate="step-after")
            .encode(
                x=alt.X(
                    f"{S.TIMESTAMP}:T", axis=alt.Axis(format="%Y", tickCount="year")
                ),
                y=alt.Y(f"{S.VALUE}:Q", title=f"{qty_type} ({y_label})"),
                color=alt.Color(
                    "collector_type:N",
                    scale=alt.Scale(
                        domain=["current", "legacy"], range=[qty_color, qty_color]
                    ),
                    legend=alt.Legend(title=qty_type),
                ),
                strokeDash=alt.StrokeDash(
                    "collector_type:N",
                    scale=alt.Scale(
                        domain=["current", "legacy"], range=[[1, 0], [4, 4]]
                    ),
                    legend=None,
                ),
            )
            .properties(width=WIDTH, height=120)
        )
        charts.append(chart)

    return alt.vconcat(*charts).resolve_scale(color="independent")


def make_plots(
    usage_timeseries: pl.LazyFrame,
    output_path: str,
    site_name: str,
    resource_type: str | None = None,
):
    data_to_plot = usage_timeseries.collect()
    y_label = RESOURCE_LABELS.get(resource_type, "Count") if resource_type else "Count"

    usage_line_plot(data_to_plot, y_label=y_label).save(
        f"{output_path}/{site_name}.png", scale_factor=SCALE_FACTOR
    )
    usage_facet_plot(data_to_plot, y_label=y_label).save(
        f"{output_path}/{site_name}_facet.png", scale_factor=SCALE_FACTOR * 2
    )
    stack_subset = data_to_plot.filter(pl.col("collector_type") == "current")
    usage_stack_plot(stack_subset, y_label=y_label).properties(
        width=WIDTH, height=WIDTH * 0.6
    ).save(f"{output_path}/{site_name}_stack.png", scale_factor=SCALE_FACTOR)


def source_facet_plot(data: pl.DataFrame) -> alt.FacetChart:
    return (
        alt.Chart(data)
        .mark_line(interpolate="step-after")
        .encode(
            x=alt.X(f"{S.TIMESTAMP}:T", axis=alt.Axis(format="%Y", tickCount="year")),
            y=alt.Y(f"{S.VALUE}:Q"),
            color="source:N",
        )
        .properties(width=500, height=120)
        .facet(row=f"{S.METRIC}:N")
    )
