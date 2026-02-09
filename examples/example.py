from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl

from chameleon_usage.ingest import load_intervals
from chameleon_usage.ingest.coerce import clamp_hierarchy
from chameleon_usage.pipeline import resample, run_pipeline
from chameleon_usage.schemas import PipelineSpec

########
# Config
########

# Where do we load the data from (already exported separately.)
DATA_DIR = "data/current/chi_tacc"

# what time range to look at?
TIME_RANGE = (datetime(2024, 1, 1), datetime(2024, 12, 31))

# how fine-grained should the output be?
BUCKET = "1d"

# where to put the output
OUTPUT_DIR = Path("output/example/")
OUTPUT_FILE = OUTPUT_DIR / "usage_timeline.parquet"
PLOTS_DIR = OUTPUT_DIR / "plots"

# Configures what the grouping columns are.
# By default, you probably want "metric", for what kind of usage,
# and "resource", for what unit the values are in.
SPEC = PipelineSpec(group_cols=("metric", "resource"), time_range=TIME_RANGE)

###########################
# Load and process the data
###########################
# load the source data and map to standardized intervals
intervals = load_intervals(DATA_DIR, TIME_RANGE)
intervals = intervals.collect().lazy()  # force checkpoint for speedup

# Apply basic cleaning: instance < reservation < reservable < total
# invalid intervals are those that were filtered out by sanity checks:
# instances with no host, resevations with no blazar host, ...
cleaned_intervals, invalid_intervals = clamp_hierarchy(intervals)
cleaned_intervals = cleaned_intervals.collect().lazy()  # force checkpoint for speedup

# Convert intervals to usage timeline
usage = run_pipeline(cleaned_intervals, SPEC)

# downsample to fixed time steps for output (optional)
resampled = resample(usage, BUCKET, SPEC)

# call collect to execute all of the transformations
thunk = resampled.collect()

print(thunk.select("timestamp", "metric", "resource", "value").head(20))

# Write the output to file
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
thunk.write_parquet(OUTPUT_FILE)


#############
# Quick plots
#############
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

metrics = [
    "total",
    "reservable",
    "committed",
    "occupied_reservation",
    "occupied_ondemand",
]

for resource, title in [("nodes", "Nodes"), ("vcpus", "vCPUs")]:
    frame = (
        thunk.filter(pl.col("resource") == resource)
        .filter(pl.col("metric").is_in(metrics))
        .pivot(on="metric", index="timestamp", values="value")
        .sort("timestamp")
    )
    if frame.is_empty():
        continue

    x = frame.get_column("timestamp").to_list()

    fig, ax = plt.subplots(figsize=(9, 4))
    for metric in metrics:
        if metric not in frame.columns:
            continue
        y = frame.get_column(metric).to_list()
        ax.plot(x, y, label=metric)

    ax.set_title(f"{title} usage ({BUCKET})")
    ax.set_xlabel("timestamp")
    ax.set_ylabel(title)
    ax.grid(alpha=0.3)
    ax.legend()
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / f"{resource}_quick.png", dpi=200)
    plt.close(fig)

print(f"Wrote parquet: {OUTPUT_FILE}")
print(f"Wrote plots: {PLOTS_DIR}")
