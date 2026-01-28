import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns


def plot_legacy_usage(df: pl.DataFrame):
    sns.set_theme()
    fig, ax = plt.subplots()

    axes = (
        df.drop(["node_type", "maint_hours", "idle_hours"])
        .group_by("date")
        .sum()
        .sort("date")
        .to_pandas()
        .set_index("date")
        .plot(ax=ax)
    )

    return fig
