import polars as pl

from chameleon_usage import loader, utils


def main():
    print("Loading Tables")
    site_yaml = utils.load_sites_yaml("etc/sites.yaml")

    results = {
        site.site_name: loader.dump_site_to_parquet(site) for site in site_yaml.values()
    }
    utils.print_summary(results)


if __name__ == "__main__":
    main()
