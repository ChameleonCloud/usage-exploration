import polars as pl

from chameleon_usage import spans, utils
from chameleon_usage.utils import SiteConfig


class UsagePipeline:
    def __init__(self, site_conf: SiteConfig):
        self.site_conf = site_conf
        self.span_loader = spans.RawSpansLoader(self.site_conf)

    def compute_spans(self) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        """
        Orchestrates the loading, cleaning, and stacking of all span types.
        """

        # 2. Instantiate the Logic Engines (The Entity Subclasses)
        blazar_src = spans.BlazarCommitmentSource(self.span_loader)
        nova_src = spans.NovaOccupiedSource(self.span_loader)

        # 3. Execute the Pipelines (Lazy)
        # Each source applies its own cleaning rules (Phantom detection, etc)
        blazar_valid, blazar_audit = blazar_src.get_spans()
        nova_valid, nova_audit = nova_src.get_spans()

        # 4. Stack the Valid Outputs
        # Because BaseSpanSource enforces the schema, these concat perfectly.
        all_usage = pl.concat([blazar_valid, nova_valid])
        all_audit = pl.concat([blazar_audit, nova_audit], how="diagonal")

        # 5. Optional: Return audit logs alongside if needed
        # return all_usage, {"blazar": blazar_audit, "nova": nova_audit}

        return all_usage, all_audit


def main():
    print("Loading Tables")
    site_yaml = utils.load_sites_yaml("etc/sites.yaml")

    # results = {
    #     site.site_name: loader.dump_site_to_parquet(site) for site in site_yaml.values()
    # }
    # utils.print_summary(results)

    tacc_spans = UsagePipeline(site_yaml["chi_tacc"])
    legacy = tacc_spans.span_loader.legacy_usage
    print(legacy.collect())

    # usage, audit = tacc_spans.compute_spans()

    # # Print the DataFrame with increased column visibility and width
    # with pl.Config(tbl_cols=20, tbl_width_chars=2000):
    #     print(usage.collect())
    #     print(audit.collect())


if __name__ == "__main__":
    main()
