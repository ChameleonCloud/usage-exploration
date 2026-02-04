# Chameleon Usage Reporting 2026

Library and pipeline to reconstruct historical openstack usage metrics.

Primary features:
- Archive source data: extract data from production DBs or other sources and archive to parquet files.
- Process source data containing timestamps and unique identifiers into timeline of usage over time.
- Plotting and analysis methods for usage over time results.

## Installation

```bash
# Core (extract only)
pip install chameleon-usage

# With S3/Ceph support
pip install chameleon-usage[s3]

# With pipeline processing
pip install chameleon-usage[pipeline]

# Everything
pip install chameleon-usage[all]
```

## Usage

### Extract: Database to Parquet

Extract dumps tables from a MySQL database to parquet files.

**To local directory:**
```bash
chameleon-usage --parquet-dir ./data extract --db-uri mysql://user:pass@host:3306
```

**To S3 or Ceph RGW:**
```bash
# Set credentials via environment
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_ENDPOINT_URL=https://rgw.example.com:8080  # for Ceph RGW

chameleon-usage --parquet-dir s3://bucket/path extract --db-uri mysql://user:pass@host:3306
```

**Using environment variable for database:**
```bash
export DATABASE_URI=mysql://user:pass@host:3306
chameleon-usage --parquet-dir ./data extract
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABASE_URI` | Database URI (fallback if `--db-uri` not provided) |
| `AWS_ACCESS_KEY_ID` | S3/Ceph access key |
| `AWS_SECRET_ACCESS_KEY` | S3/Ceph secret key |
| `AWS_ENDPOINT_URL` | Custom S3 endpoint (for Ceph RGW, MinIO, etc.) |
| `AWS_REGION` | AWS region (optional) |

### Process: Parquet to Usage Metrics

Process extracted data into usage metrics. Requires `[pipeline]` extras.

```bash
chameleon-usage \
  --sites-config etc/sites.yaml \
  --parquet-dir data/raw_spans \
  --site chi_tacc --site chi_uc \
  process \
  --output output/usage \
  --start-date 2015-01-01 \
  --end-date 2026-01-01
```

Optional: add `--resample 7d` to bucket results before writing.

Minimal `etc/sites.yaml` (only needed for process command):
```yaml
chi_tacc:
  site_name: "CHI@TACC"
```

See [examples/report.py](examples/report.py) for a complete example with plotting.


## Data Model

### Utilization Stack

Capacity decomposes into **mutually exclusive states** at any point in time:

```
┌────────────────────────────────────────────────────────────────────┐
│                          TOTAL CAPACITY                            │
├─────────────────────────────────────┬──────────────────────────────┤
│             RESERVABLE              │           ONDEMAND           │
├─────────────────────────┬───────────┼──────────────────┬───────────┤
│        COMMITTED        │           │                  │           │
├──────────────────┬──────┤           │     OCCUPIED     │ AVAILABLE │
│     OCCUPIED     │ IDLE │ AVAILABLE │                  │           │
├────────┬─────────┤      │           ├────────┬─────────┤           │
│ ACTIVE │ STOPPED │      │           │ ACTIVE │ STOPPED │           │
└────────┴─────────┴──────┴───────────┴────────┴─────────┴───────────┘
```

### What's Easy

1. Most data is stored as soft-deleted rows, with created_at, deleted_at, and a unique ID.
2. Each "usage state" above corresponds to a particular openstack entity, with unique ids.
   1. "total capacity" is "all nova hypervisors that existed at time T.
   2. "reservable" is "all blazar hosts that existed at time T."
   3. "committed" -> blazar allocations in an active lease
   4. Active and Stopped -> Nova instances by state, and if they had a blazar reservation_id or not
3. Converting these "spans" to total capacity and usage over time is mechanically straightforward:
   1. each openstack entity has a unique identifer we can join on
   2. grouping by "type, cumulative sum produces counts for each type for every timestamp 
   3. data is small enough to fit into ram
   4. we only need to downsample for plotting

### What's Hard

1. Some data we want doesn't exist in the live DB. Columns like "maintenance" refer to state "now", and historial state isn't available.
2. Some data is missing: some historical nova hosts and blazar hosts are not in the DB, primarily pre-2018
3. We may have multiple data sources that disagree:
   1. what if a nova instance has deleted_at after the blazar allocation ends?
   2. what if we import a backup for historical data, and it has conflicting records?
