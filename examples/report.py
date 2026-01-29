"""Generate usage reports for all sites."""

from datetime import datetime

import polars as pl

from chameleon_usage.engine import SegmentBuilder
from chameleon_usage.legacyusage import LegacyUsageLoader
from chameleon_usage.pipeline import (
    compute_derived_metrics,
    load_facts,
    resample_simple,
)
from chameleon_usage.plots import make_plots

# used as SENTINEL for null spans, clips
# used as boundary for non-null events, filters
WINDOW_END = datetime(2025, 11, 1)


def main():
    for site_name in ["chi_uc", "chi_tacc", "kvm_tacc"]:
        facts = load_facts(input_data="data/raw_spans", site_name=site_name)
        facts.collect_schema()

        # legacy data (optional)
        legacy_usage = None
        try:
            loader = LegacyUsageLoader("data/raw_spans", site_name)
            loader.load_facts()
            legacy_usage = loader.get_usage()
            legacy_usage.collect_schema()
        except FileNotFoundError:
            pass

        # process facts → segments → usage
        engine = SegmentBuilder(site_name=site_name, priority_order=[])
        segments = engine.build(facts)
        segments.collect_schema()

        usage = engine.calculate_concurrency(segments, window_end=WINDOW_END)
        usage.collect_schema()

        usage_derived = compute_derived_metrics(usage)
        usage_derived.collect_schema()

        # combine with legacy if available
        # ensure columns math
        if legacy_usage is not None:
            usage_merged = pl.concat([usage_derived, legacy_usage])
        else:
            usage_merged = usage_derived

        filtered = usage_merged.filter(pl.col("timestamp") <= WINDOW_END)

        # resample and compute derived metrics
        resampled = resample_simple(filtered, interval="30d")
        resampled.collect_schema()
        print(resampled.collect())

        make_plots(resampled, output_path="output/plots/", site_name=site_name)


if __name__ == "__main__":
    main()
