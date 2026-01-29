from datetime import datetime

import altair as alt
import polars as pl

from chameleon_usage.adapters import (
    BlazarAllocationAdapter,
    BlazarComputehostAdapter,
    NovaComputeAdapter,
)
from chameleon_usage.constants import Cols as C
from chameleon_usage.engine import TimelineBuilder
from chameleon_usage.models.raw import (
    BlazarAllocationRaw,
    BlazarHostRaw,
    BlazarLeaseRaw,
    BlazarReservationRaw,
    NovaHostRaw,
    NovaInstanceRaw,
)


def load_facts(input_data: str, site_name: str):
    base_path = f"{input_data}/{site_name}"
    all_facts = []
    all_facts.append(
        NovaComputeAdapter(
            NovaHostRaw.validate(
                pl.scan_parquet(f"{base_path}/nova.compute_nodes.parquet")
            )
        ).to_facts()
    )
    # all_facts.append(
    #     NovaComputeAdapter(
    #         NovaHostRaw.validate(
    #             pl.scan_parquet(f"{base_path}/nova.compute_nodes.parquet")
    #         )
    #     ).to_facts()
    # )
    all_facts.append(
        BlazarComputehostAdapter(
            BlazarHostRaw.validate(
                pl.scan_parquet(f"{base_path}/blazar.computehosts.parquet")
            )
        ).to_facts()
    )

    blazardata = [
        BlazarAllocationRaw.validate(
            pl.scan_parquet(f"{base_path}/blazar.computehost_allocations.parquet")
        ),
        BlazarReservationRaw.validate(
            pl.scan_parquet(f"{base_path}/blazar.reservations.parquet")
        ),
        BlazarLeaseRaw.validate(pl.scan_parquet(f"{base_path}/blazar.leases.parquet")),
    ]
    all_facts.append(BlazarAllocationAdapter(*blazardata).to_facts())
    facts = pl.concat(all_facts)
    return facts


def usage_stack_plot():
    pass


def usage_line_plot(data: pl.DataFrame) -> alt.Chart:
    """line plot of usage timeseries data."""
    WIDTH = 516
    # Standard print font sizes (in points)
    FONT_TITLE = 12
    FONT_AXIS_LABEL = 10
    FONT_TICK = 9
    FONT_LEGEND = 9

    X_TIME = alt.X(
        f"{C.TIMESTAMP}:T",
        axis=alt.Axis(format="%Y", tickCount="year"),
    )
    Y_COUNT = alt.Y(f"{C.COUNT}:Q")
    LEGEND = alt.Legend(
        orient="bottom",
        # orient="none",
        # legendX=WIDTH - 90,  # adjust based on legend width
        # legendY=5,
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
    scale_factor = 3
    data_to_plot = usage_timeseries.collect()
    usage_line_plot(data_to_plot).save(
        f"{output_path}/{site_name}.png", scale_factor=scale_factor
    )


def main():
    # get data

    for site_name in ["chi_uc", "chi_tacc", "kvm_tacc"]:
        # load data, convert to facts timeline
        facts_list = load_facts(input_data="data/raw_spans", site_name=site_name)

        # process facts, convert to state timeline
        engine = TimelineBuilder()
        state_timeline = engine.build(facts_list)

        # process state timeline into usage timeserices
        usage_timeseries = engine.calculate_concurrency(state_timeline)

        # resample timeseries for plotting
        resampled_usage = engine.resample_time_weighted(
            usage_timeseries, interval="30d"
        )
        make_plots(resampled_usage, output_path="output/plots/", site_name=site_name)


if __name__ == "__main__":
    main()
