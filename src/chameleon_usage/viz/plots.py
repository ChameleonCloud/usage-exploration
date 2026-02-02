from datetime import datetime

import altair as alt
import polars as pl

from chameleon_usage.constants import Cols as C
from chameleon_usage.constants import QuantityTypes as QT

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
    QT.OCCUPIED: "green",
    QT.IDLE: "orange",
    QT.COMMITTED: "#1f77b4",
    QT.AVAILABLE: "#aec7e8",
    QT.TOTAL: "black",
    QT.RESERVABLE: "grey",
}
QTY_ORDER = [QT.TOTAL, QT.RESERVABLE, QT.AVAILABLE, QT.COMMITTED, QT.IDLE, QT.OCCUPIED]
QTY_COLOR_SCALE = alt.Scale(domain=QTY_ORDER, range=[QTY_COLORS[t] for t in QTY_ORDER])


def usage_stack_plot(data: pl.DataFrame) -> alt.LayerChart:
    base = alt.Chart(data)
    x_time = alt.X(f"{C.TIMESTAMP}:T", axis=alt.Axis(format="%Y", tickCount="year"))

    area_types = [QT.OCCUPIED, QT.IDLE, QT.COMMITTED, QT.AVAILABLE]

    areas = (
        base.transform_filter(
            alt.FieldOneOfPredicate(field="quantity_type", oneOf=area_types)
        )
        .transform_calculate(stack_order=f"indexof({area_types}, datum.quantity_type)")
        .mark_area(interpolate="step-after")
        .encode(
            x=x_time,
            y=alt.Y(f"{C.COUNT}:Q", stack=True),
            color=alt.Color(f"{C.QUANTITY_TYPE}:N", scale=QTY_COLOR_SCALE),
            order=alt.Order("stack_order:Q"),
        )
    )

    line_total = (
        base.transform_filter(alt.datum.quantity_type == "total")
        .mark_line(strokeWidth=2, interpolate="step-after")
        .encode(
            x=x_time,
            y=alt.Y(f"{C.COUNT}:Q"),
            color=alt.Color(f"{C.QUANTITY_TYPE}:N", scale=QTY_COLOR_SCALE),
        )
    )

    line_reservable = (
        base.transform_filter(alt.datum.quantity_type == "reservable")
        .mark_line(strokeWidth=2, strokeDash=[4, 4], interpolate="step-after")
        .encode(
            x=x_time,
            y=alt.Y(f"{C.COUNT}:Q"),
            color=alt.Color(f"{C.QUANTITY_TYPE}:N", scale=QTY_COLOR_SCALE),
        )
    )

    return areas + line_total + line_reservable


def usage_line_plot(data: pl.DataFrame) -> alt.FacetChart:
    fig = (
        alt.Chart(data)
        .mark_line(strokeWidth=1, interpolate="step-after")
        .encode(
            x=alt.X(f"{C.TIMESTAMP}:T", axis=alt.Axis(format="%Y", tickCount="year")),
            y=alt.Y(f"{C.COUNT}:Q"),
            color=alt.Color(f"{C.QUANTITY_TYPE}:N", scale=QTY_COLOR_SCALE),
        )
        .properties(width=WIDTH, height=150)
        .facet(row=alt.Row("collector_type:N", sort=["current", "legacy"]))
    )

    return fig


def usage_facet_plot(data: pl.DataFrame) -> alt.VConcatChart:
    """Faceted line plot comparing legacy vs current by quantity type."""
    charts = []
    for qty_type in QTY_ORDER:
        subset = data.filter(pl.col(C.QUANTITY_TYPE) == qty_type)
        if subset.is_empty():
            continue
        qty_color = QTY_COLORS[qty_type]
        chart = (
            alt.Chart(subset)
            .mark_line(strokeWidth=1, interpolate="step-after")
            .encode(
                x=alt.X(
                    f"{C.TIMESTAMP}:T", axis=alt.Axis(format="%Y", tickCount="year")
                ),
                y=alt.Y(f"{C.COUNT}:Q", title=f"# {qty_type}"),
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


def make_plots(usage_timeseries: pl.LazyFrame, output_path: str, site_name: str):
    data_to_plot = usage_timeseries.collect()

    # recent = data_to_plot.filter(pl.col("timestamp") >= datetime(2022, 1, 1))
    usage_line_plot(data_to_plot).save(
        f"{output_path}/{site_name}.png", scale_factor=SCALE_FACTOR
    )
    usage_facet_plot(data_to_plot).save(
        f"{output_path}/{site_name}_facet.png", scale_factor=SCALE_FACTOR * 2
    )
    stack_subset = data_to_plot.filter(pl.col("collector_type") == "current")
    usage_stack_plot(stack_subset).properties(width=WIDTH, height=WIDTH * 0.6).save(
        f"{output_path}/{site_name}_stack.png", scale_factor=SCALE_FACTOR
    )


def source_facet_plot(data: pl.DataFrame) -> alt.FacetChart:
    return (
        alt.Chart(data)
        .mark_line(interpolate="step-after")
        .encode(
            x=alt.X("timestamp:T", axis=alt.Axis(format="%Y", tickCount="year")),
            y=alt.Y("count:Q"),
            color="source:N",
        )
        .properties(width=500, height=120)
        .facet(row="quantity_type:N")
    )
