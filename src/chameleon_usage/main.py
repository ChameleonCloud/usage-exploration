from chameleon_usage import loader, utils


def main():
    print("Loading Tables")
    site_yaml = utils.load_sites_yaml("etc/sites.yaml")

    for _, config in site_yaml.items():
        loader.dump_site_to_parquet(config)


if __name__ == "__main__":
    main()
