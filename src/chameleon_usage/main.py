import polars as pl

from chameleon_usage import loader, make_spans, utils


def main():
    print("Loading Tables")
    site_yaml = utils.load_sites_yaml("etc/sites.yaml")

    # results = {
    #     site.site_name: loader.dump_site_to_parquet(site) for site in site_yaml.values()
    # }
    # utils.print_summary(results)

    tacc_spans = make_spans.RawSpansLoader(site_yaml["chi_tacc"])
    tacc_spans.load_raw_tables()

    # Print the DataFrame with increased column visibility and width
    with pl.Config(tbl_cols=20, tbl_width_chars=2000):
        print(tacc_spans.compute_spans())


if __name__ == "__main__":
    main()
