from pathlib import Path

import matplotlib.pyplot as plt

from chameleon_usage import math, utils
from chameleon_usage.dump_db import dump_site_to_parquet
from chameleon_usage.pipeline import UsagePipeline

# TODO: transform new spans into legacy output shape (daily totals, legacy column names).


# TODO: confirm legacy date grain and required output columns for comparison.
# TODO: list legacy columns we must match exactly (names + units).
def _print_legacy_usage(pipeline):
    legacy = pipeline.span_loader.legacy_usage.collect()
    print(f"legacy_usage: {legacy.shape}")
    print(f"  columns: {legacy.columns}")


# TODO: compute legacy-shaped new output, join on date, and print per-column diffs.
# TODO: print % error per column so we can decide if we are "close enough."
def main():
    sites = utils.load_sites_yaml("etc/sites.yaml")
    site = sites["chi_tacc"]

    print("Syncing DB to parquet cache...")
    dump_site_to_parquet(site)

    print("\nLoading pipeline...")
    pipeline = UsagePipeline(site)

    usage, audit = pipeline.compute_spans()

    # TODO: fix this naming properly
    spans = usage.rename({"resource_id": "hypervisor_hostname"})

    events = math.spans_to_events(spans)
    timeline = math.sweepline(events)
    daily_wide = math.sweepline_to_wide(timeline, every="1d")

    print("\n=== DataFrame Shapes ===")
    _print_legacy_usage(pipeline)
    print(f"daily_wide: {daily_wide.shape}")
    print(f"  columns: {daily_wide.columns}")

    plot_dir = Path("output/plots")
    plot_dir.mkdir(parents=True, exist_ok=True)
    plot_path = plot_dir / "chi_tacc_daily_sources.png"

    (
        daily_wide.to_pandas()
        .set_index("timestamp")
        .sort_index()
        .plot(title="Daily concurrent spans by source")
    )
    plt.tight_layout()
    plt.savefig(plot_path, dpi=150)
    print(f"saved plot: {plot_path}")


if __name__ == "__main__":
    main()
