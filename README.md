# Chameleon Usage Reporting

This repo packages tooling to generate resource usage timelines from data in OpenStack service databases.

Its goal is to provide reliable capacity/utilization reporting over time, avoid 
dependence on brittle, hard to debug SQL statements, and standardize on 
intermediate representations for intervals and timelines that can be easily 
used in notebooks, dashboards, and periodic reporting.

## What You Get

- Easy to deploy and configure tool to fetch raw tables from one or more OpenStack DBs.
- Reusable and extensible pipeline to accommodate additional sources and output formats
- Auditable data in parquet format: raw tables, normalized intervals, and output timelines
- Example scripts and notebooks illustrating an end-to-end analysis pipeline.

## Installation

```bash
# Core (extract + process)
pip install chameleon-usage

# With S3-compatible object storage support (AWS S3, Ceph RGW, MinIO)
pip install chameleon-usage[s3]

# With plotting support
pip install chameleon-usage[plots]

# With plotting + S3 support
pip install "chameleon-usage[plots,s3]"
```

### MySQL backend requirements

Core install includes `ibis-framework[mysql]`, which uses `mysqlclient` for DB access.
If a prebuilt wheel is unavailable for your platform, install MySQL client development
libraries and a C build toolchain first, then run `pip install chameleon-usage`.

Ubuntu/Debian:
```bash
sudo apt-get update
sudo apt-get install -y build-essential pkg-config python3-dev default-libmysqlclient-dev
```

macOS (Homebrew):
```bash
brew install mysql pkg-config
```

As this is only needed for the extract stage, future work will make this dependency optional.

## Quickstart

This quickstart runs through collecting data, processing it, and viewing results, locally, against a single OpenStack site.

### Set up Access

This tool only needs read-only privileges, and only to specific tables.
The following command shows how to create a SQL user and set these permissions, 
and outputs a series of SQL commands to run manually, setting up this least-privilege user.

```bash
chameleon-usage print-grant-sql --user ccusage
```

Example, generated for user `ccusage` and default host `%`
```sql
-- Grant read access for chameleon-usage extractor
-- Run as MySQL admin (e.g., root)

CREATE USER IF NOT EXISTS 'ccusage'@'%' IDENTIFIED BY 'CHANGE_ME';

-- Minimal Tables needed for "total" and "used" capacity.
GRANT SELECT ON nova.compute_nodes TO 'ccusage'@'%';
GRANT SELECT ON nova.instances TO 'ccusage'@'%';

-- Used to gather data about nova instance lifecycle
GRANT SELECT ON nova.instance_actions TO 'ccusage'@'%';
GRANT SELECT ON nova.instance_actions_events TO 'ccusage'@'%';

-- Used to look up Flavor and Scheduler hints info for Instances
GRANT SELECT ON nova_api.request_specs TO 'ccusage'@'%';

-- Used for Blazar Physical:Host reservations
GRANT SELECT ON blazar.leases TO 'ccusage'@'%';
GRANT SELECT ON blazar.reservations TO 'ccusage'@'%';
GRANT SELECT ON blazar.computehost_allocations TO 'ccusage'@'%';
GRANT SELECT ON blazar.computehosts TO 'ccusage'@'%';
-- Used for Blazar Flavor:Instance reservations
GRANT SELECT ON blazar.instance_reservations TO 'ccusage'@'%';

-- Used for CHI@Edge reservable containers
GRANT SELECT ON blazar.devices TO 'ccusage'@'%';
GRANT SELECT ON blazar.device_allocations TO 'ccusage'@'%';
GRANT SELECT ON blazar.device_extra_capabilities TO 'ccusage'@'%';
GRANT SELECT ON blazar.device_reservations TO 'ccusage'@'%';

GRANT SELECT ON zun.container TO 'ccusage'@'%';
GRANT SELECT ON zun.container_actions TO 'ccusage'@'%';
GRANT SELECT ON zun.container_actions_events TO 'ccusage'@'%';

-- Optional, imports cache from other usage reporting tools run by Chameleon
GRANT SELECT ON chameleon_usage.node_usage_report_cache TO 'ccusage'@'%';
GRANT SELECT ON chameleon_usage.node_count_cache TO 'ccusage'@'%';
GRANT SELECT ON chameleon_usage.node_usage TO 'ccusage'@'%';
GRANT SELECT ON chameleon_usage.node_event TO 'ccusage'@'%';
GRANT SELECT ON chameleon_usage.node_maintenance TO 'ccusage'@'%';
GRANT SELECT ON chameleon_usage.node_project_usage_report_cache TO 'ccusage'@'%';

FLUSH PRIVILEGES;
```

### Configure the exporter

While the exporter can be run using only cli arguments or environment variables,
we recommend using a simple configuration file. An example is located in this repo
at `etc/site.yml`

```yaml
---
site_key_name:
  site_name: "Human Readable Name"
  # DB connection string, user created above.
  db_uri: "mysql://ccusage:CHANGE_ME@dbhost:3306"
  # Directory where raw database exports and intermediate files may be saved.
  data_dir: "/opt/usage_data/site_key_name"
```


### Running the exporter

The following command uses the provided config to fetch data from the OpenStack DB and write it to file for later analysis.

```bash
chameleon-usage --config etc/site.yml --site site_key_name extract
```

### Generating the usage timeline

Finally, the `process` command takes the raw data and configuration, and outputs the usage timeline.

```bash
chameleon-usage \
  --config etc/site.yml \
  --site site_key_name \
  process \
  --output output/usage \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

## CLI Details

`extract`
- Dumps configured DB tables to parquet under `data_dir`.
- Required inputs: `--db-uri` or `$DATABASE_URI` or `config.db_uri`, and a data path from `--data-dir` or `config.data_dir`.

`process`
- Loads raw span parquet and writes usage parquet by site.
  Optional DB export uses `--export-uri` or `$EXPORT_URI`.
- Included in core install (`pip install chameleon-usage`).

`print-grant-sql`
- Prints SQL grants needed by the extractor user.

Shared flags:
- `--config`: path to `etc/site.yml`
- `--site`: repeatable site keys
- `--data-dir`: overrides `config.data_dir`

Priority:
- `--db-uri` > `$DATABASE_URI` > `config.db_uri`
- `--data-dir` > `config.data_dir`
- `--export-uri` > `$EXPORT_URI`

## Environment Variables

| Variable | Description |
|---|---|
| `DATABASE_URI` | Database URI fallback when `--db-uri` is not passed |
| `AWS_ACCESS_KEY_ID` | Access key for S3-compatible object storage |
| `AWS_SECRET_ACCESS_KEY` | Secret key for S3-compatible object storage |
| `AWS_ENDPOINT_URL` | Endpoint for non-AWS S3 providers (Ceph RGW, MinIO), e.g. `https://host:port` |
| `AWS_REGION` | Optional AWS region |
| `EXPORT_URI` | Process export DB URI fallback when `--export-uri` is not passed |

## Output Layout

Extract output:
- `<data_dir>/<schema>.<table>.parquet`

Process output:
- `<output>/<site>/usage.parquet`

## Troubleshooting

- Docker `--env-file`: do not quote values.
- Use `AWS_ENDPOINT_URL=https://...`, not `AWS_ENDPOINT_URL="https://..."`.
- Endpoint hosts must be valid DNS labels.
- If using S3-compatible storage, hyphenated bucket names are safer than underscore names.
- Missing or unauthorized tables are reported and extraction continues.

## Examples

- `examples/report.py`
- `examples/report_edge.py`

For development workflow, tests, linting, and PR expectations, see `CONTRIBUTING.md`.
