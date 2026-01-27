from chameleon_usage import utils
from chameleon_usage.dump_db import dump_site_to_parquet
from chameleon_usage.pipeline import UsagePipeline

# TODO: transform new spans into legacy output shape (daily totals, legacy column names).


# TODO: confirm legacy date grain and required output columns for comparison.
# TODO: list legacy columns we must match exactly (names + units).
def _print_legacy_usage(pipeline):
    legacy = pipeline.span_loader.legacy_usage.collect()
    print(f"legacy_usage: {legacy.shape}")
    print(f"  columns: {legacy.columns}")


# TODO: aggregate new spans to the same daily totals and columns as legacy output.
# TODO: emit a DataFrame keyed by date that mirrors the legacy schema.
def _print_valid_usage(pipeline):
    usage, _ = pipeline.compute_spans()
    valid = usage.collect()
    print(f"valid_usage: {valid.shape}")
    print(f"  columns: {valid.columns}")


# TODO: summarize rejected hours by date so legacy deltas are explainable.
# TODO: report how much invalid data could shift each legacy column.
def _print_invalid_usage(pipeline):
    _, audit = pipeline.compute_spans()
    invalid = audit.collect()
    print(f"invalid_usage: {invalid.shape}")
    print(f"  columns: {invalid.columns}")


# TODO: compute legacy-shaped new output, join on date, and print per-column diffs.
# TODO: print % error per column so we can decide if we are "close enough."
def main():
    sites = utils.load_sites_yaml("etc/sites.yaml")
    site = sites["chi_tacc"]

    print("Syncing DB to parquet cache...")
    dump_site_to_parquet(site)

    print("\nLoading pipeline...")
    pipeline = UsagePipeline(site)

    print("\n=== DataFrame Shapes ===")
    _print_legacy_usage(pipeline)
    _print_valid_usage(pipeline)
    _print_invalid_usage(pipeline)


if __name__ == "__main__":
    main()
