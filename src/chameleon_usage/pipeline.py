import polars as pl

from chameleon_usage import plots, spans, utils
from chameleon_usage.utils import SiteConfig


class UsagePipeline:
    def __init__(self, site_conf: SiteConfig):
        self.site_conf = site_conf
        self.span_loader = spans.RawSpansLoader(self.site_conf)

    def compute_spans(self) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        """
        Orchestrates the loading, cleaning, and stacking of all span types.
        """
        sources = [
            spans.NovaHostSource(self.span_loader),
            spans.BlazarHostSource(self.span_loader),  # when you add it
            spans.BlazarCommitmentSource(self.span_loader),
            spans.NovaOccupiedSource(self.span_loader),
        ]

        valids: list[pl.LazyFrame] = []
        audits: list[pl.LazyFrame] = []
        for src in sources:
            v, a = src.get_spans()
            valids.append(v)
            audits.append(a)

        return pl.concat(valids), pl.concat(audits, how="diagonal")


def main():
    print("Loading Tables")
    site_yaml = utils.load_sites_yaml("etc/sites.yaml")

    # results = {
    #     site.site_name: loader.dump_site_to_parquet(site) for site in site_yaml.values()
    # }
    # utils.print_summary(results)

    tacc_spans = UsagePipeline(site_yaml["chi_tacc"])
    legacy = tacc_spans.span_loader.legacy_usage.collect()
    plots.plot_legacy_usage(legacy).show()

    # usage, audit = tacc_spans.compute_spans()

    # # Print the DataFrame with increased column visibility and width
    # with pl.Config(tbl_cols=20, tbl_width_chars=2000):
    #     print(usage.collect())
    #     print(audit.collect())


if __name__ == "__main__":
    main()
