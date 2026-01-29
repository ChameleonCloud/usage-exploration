import altair as alt
import polars as pl

from chameleon_usage.constants import Cols as C

# Chart dimensions
WIDTH = 516

# Standard print font sizes (in points)
FONT_TITLE = 12
FONT_AXIS_LABEL = 10
FONT_TICK = 9
FONT_LEGEND = 9

# Export settings
SCALE_FACTOR = 3


def usage_stack_plot(data: pl.DataFrame) -> alt.LayerChart:
    base = alt.Chart(data)
    x_time = alt.X(f"{C.TIMESTAMP}:T", axis=alt.Axis(format="%Y", tickCount="year"))

    # Stack order: used (bottom), idle, committed, available (top)
    # Legend order: top-to-bottom matching visual stack, then lines
    area_types = ["used", "idle", "committed", "available"]
    line_types = ["total", "reservable"]
    all_types = (
        line_types + area_types[::-1]
    )  # legend order: lines first, then stack top-to-bottom
    all_colors = {
        "used": "green",
        "idle": "orange",
        "committed": "#1f77b4",
        "available": "#aec7e8",
        "total": "black",
        "reservable": "grey",
    }

    color_scale = alt.Scale(domain=all_types, range=[all_colors[t] for t in all_types])

    areas = (
        base.transform_filter(
            alt.FieldOneOfPredicate(field="quantity_type", oneOf=area_types)
        )
        .transform_calculate(stack_order=f"indexof({area_types}, datum.quantity_type)")
        .mark_area()
        .encode(
            x=x_time,
            y=alt.Y(f"{C.COUNT}:Q", stack=True),
            color=alt.Color(f"{C.QUANTITY_TYPE}:N", scale=color_scale),
            order=alt.Order("stack_order:Q"),
        )
    )

    line_total = (
        base.transform_filter(alt.datum.quantity_type == "total")
        .mark_line(strokeWidth=2)
        .encode(
            x=x_time,
            y=alt.Y(f"{C.COUNT}:Q"),
            color=alt.Color(f"{C.QUANTITY_TYPE}:N", scale=color_scale),
        )
    )

    line_reservable = (
        base.transform_filter(alt.datum.quantity_type == "reservable")
        .mark_line(strokeWidth=2, strokeDash=[4, 4])
        .encode(
            x=x_time,
            y=alt.Y(f"{C.COUNT}:Q"),
            color=alt.Color(f"{C.QUANTITY_TYPE}:N", scale=color_scale),
        )
    )

    return areas + line_total + line_reservable


def usage_line_plot(data: pl.DataFrame) -> alt.Chart:
    X_TIME = alt.X(
        f"{C.TIMESTAMP}:T",
        axis=alt.Axis(format="%Y", tickCount="year"),
    )
    Y_COUNT = alt.Y(f"{C.COUNT}:Q")
    LEGEND = alt.Legend(
        orient="bottom",
        strokeColor="black",
        padding=10,
        labelFontSize=FONT_LEGEND,
        titleFontSize=FONT_AXIS_LABEL,
    )
    COLOR_QTY = alt.Color(f"{C.QUANTITY_TYPE}:N", legend=LEGEND)

    fig = (
        alt.Chart(data)
        .mark_line()
        .encode(x=X_TIME, y=Y_COUNT, color=COLOR_QTY)
        .properties(width=WIDTH, height=WIDTH * 0.6)
        .configure_axis(labelFontSize=FONT_TICK, titleFontSize=FONT_AXIS_LABEL)
        .configure_legend(labelFontSize=FONT_LEGEND, titleFontSize=FONT_AXIS_LABEL)
        .configure_title(fontSize=FONT_TITLE)
    )

    return fig


def make_plots(usage_timeseries: pl.LazyFrame, output_path: str, site_name: str):
    data_to_plot = usage_timeseries.collect()
    usage_line_plot(data_to_plot).save(
        f"{output_path}/{site_name}.png", scale_factor=SCALE_FACTOR
    )
    usage_stack_plot(data_to_plot).properties(width=WIDTH, height=WIDTH * 0.6).save(
        f"{output_path}/{site_name}_stack.png", scale_factor=SCALE_FACTOR
    )
