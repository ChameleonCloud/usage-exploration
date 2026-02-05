# Chameleon Usage Reporting

Reconstruct historical OpenStack capacity and usage from source databases.
Write outputs to local disk or S3-compatible object storage (`s3://...`).

## Installation

```bash
# Core (extract only)
pip install chameleon-usage

# With S3-compatible object storage support (AWS S3, Ceph RGW, MinIO)
pip install chameleon-usage[s3]

# With pipeline processing
pip install chameleon-usage[pipeline]

# Everything
pip install chameleon-usage[all]
```

## Quickstart

1. Generate read-only SQL grants:

```bash
chameleon-usage print-grant-sql
chameleon-usage print-grant-sql --user usage_exporter --host '10.0.0.%'
```

This is a one-time setup step per DB/user. The command only prints SQL.
Have a DB admin run that SQL (as root/admin) to create the extractor user and grant
read-only table access. After that, extraction can run with that least-privilege user.

2. Create `etc/site.yml`:

```yaml
chi_uc:
  site_name: "CHI@UC"
  db_uri: "mysql://user:pass@host:3306"
  data_dir: "s3://usage-new-collector/chi_uc"
```

3. Extract source tables:

```bash
chameleon-usage --config etc/site.yml --site chi_uc extract
```

4. Process extracted spans into usage:

```bash
chameleon-usage \
  --config etc/site.yml \
  --site chi_uc \
  process \
  --output output/usage \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

## CLI

`extract`
- Dumps configured DB tables to parquet under `data_dir`.
- Required inputs: `--db-uri` or `$DATABASE_URI` or `config.db_uri`, and a data path from `--data-dir` or `config.data_dir`.

`process`
- Loads raw span parquet and writes usage parquet by site.
- Requires `chameleon-usage[pipeline]`.

`print-grant-sql`
- Prints SQL grants needed by the extractor user.

Shared flags:
- `--config`: path to `etc/site.yml`
- `--site`: repeatable site keys
- `--data-dir`: overrides `config.data_dir`

Priority:
- `--db-uri` > `$DATABASE_URI` > `config.db_uri`
- `--data-dir` > `config.data_dir`

## Environment Variables

| Variable | Description |
|---|---|
| `DATABASE_URI` | Database URI fallback when `--db-uri` is not passed |
| `AWS_ACCESS_KEY_ID` | Access key for S3-compatible object storage |
| `AWS_SECRET_ACCESS_KEY` | Secret key for S3-compatible object storage |
| `AWS_ENDPOINT_URL` | Endpoint for non-AWS S3 providers (Ceph RGW, MinIO), e.g. `https://host:port` |
| `AWS_REGION` | Optional AWS region |

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
