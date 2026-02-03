# Chameleon Usage Reporting 2026

Library and pipeline to reconstruct historical openstack usage metrics.

Primary features:
- Archive source data: extract data from production DBs or other sources and archive to parquet files.
- Process source data containing timestamps and unique identifiers into timeline of usage over time.
- Plotting and analysis methods for usage over time results.

## Usage (of the tool, not the data)

Extract data for one site (writes to `<parquet-dir>/<site>/`):
```bash
chameleon-usage \
  --sites-config etc/sites.yaml \
  --parquet-dir data/raw_spans \
  --site chi_tacc \
  extract
```

Process already extracted data (repeat `--site` or omit to run all configured sites):
```bash
chameleon-usage \
  --sites-config etc/sites.yaml \
  --parquet-dir data/raw_spans \
  --site chi_tacc --site chi_uc --site kvm_tacc \
  process \
  --output output/usage \
  --start-date 2015-01-01 \
  --end-date 2026-01-01
```

Optional: add `--resample 7d` to bucket results before writing.

Minimal `etc/sites.yaml`:
```yaml
chi_tacc:
  site_name: "CHI@TACC"
  raw_parquet: "data/raw_spans"
  db_uris:
    nova: "mysql://user:pass@127.0.0.1:3307/nova"
    nova_api: "user://user:pass@127.0.0.1:3307/nova_api"
    blazar: "mysql://user:pass@127.0.0.1:3307/blazar"
```
`db_uris` are only needed for extracting the raw data to parquet, usage analysis
consuming parquet does not need them, and can run anywhere with access to the data.

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
   a. "total capacity" is "all nova hypervisors that existed at time T.
   b. "reservable" is "all blazar hosts that existed at time T."
   c. "committed" -> blazar allocations in an active lease
   d. Active and Stopped -> Nova instances by state, and if they had a blazar reservation_id or not
3. Converting these "spans" to total capacity and usage over time is mechanically straightforward:
   a. each openstack entity has a unique identifer we can join on
   b. grouping by "type, cumulative sum produces counts for each type for every timestamp 
   c. data is small enough to fit into ram
   d. we only need to downsample for plotting

### What's Hard

1. Some data we want doesn't exist in the live DB. Columns like "maintenance" refer to state "now", and historial state isn't available.
2. Some data is missing: some historical nova hosts and blazar hosts are not in the DB, primarily pre-2018
3. We may have multiple data sources that disagree:
   a. what if a nova instance has deleted_at after the blazar allocation ends?
   b. what if we import a backup for historical data, and it has conflicting records?

### Open Questions

#### : Representing Maintenance

1. How to represent maintenance?
   1. Maintenance may remove total capacity.
   2. Maintenance comes from multiple sources: portal outages, nova->disabled, blazar->not reservable


## Goals

1. Must be reproducible: same source data -> same results
2. Must be auditable: understand *why* data was transformed, and trace decisions to impact on output
3. Must be explicit and understandable. No magic, and no "write once, read never" code.

## Architecture

```
┌──────────────┐ ┌───────────┐ ┌───────────────────┐ ┌────────────┐
│ openstack db │ │ backup db │ │ legacy usage data │ │ prometheus │
└──────┬───────┘ └─────┬─────┘ └─────────┬─────────┘ └──────┬─────┘
       │               │                 │                  │
       └───────────────┴────────┬────────┴──────────────────┘
                                │
                                ▼
                        ┌───────────────┐
                        │  data loader  │
                        └───────┬───────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  folders of parquet   │
                    └───────────┬───────────┘
                                │
                                ▼
              ┌─────────────────────────────────────────┐
              │  load tables, apply convenience schema  │
              └──────────────────┬──────────────────────┘
                                 │
                 ┌───────────────┴───────────────┐
                 │                               │
                 ▼                               ▼
    ┌────────────────────────┐      ┌───────────────────────┐
    │  parse observation     │      │  map leases →         │
    │  events                │      │  lead times           │
    └───────────┬────────────┘      └───────────────────────┘
                │
                ▼
    ┌────────────────────────────────────────────────────────────────────┐
    │  map events → timelines: join sources, group by id, sum by type    │
    ├────────────────────────────────────────────────────────────────────┤
    │                                                                    │
    │  ┌───────────────────┐                                             │
    │  │ nova hosts        │──┐                                          │
    │  │ blazar hosts      │  │                                          │
    │  │ blazar allocs     │  ├──► join on hypervisor_hostname ──► total │
    │  │ nova instances    │  │                                 capacity │
    │  │ ...               │──┤                                          │
    │  └───────────────────┘  │                                          │
    │                         ├──► join on hypervisor_hostname ──► reserv.
    │                         │                                  capacity│
    │                         │                                          │
    │                         ├──► join on allocation_id ──► allocated   │
    │                         │                             capacity     │
    │                         │                                          │
    │                         └──► join on instance_uuid ──► used        │
    │                                                        capacity    │
    └────────────────────────────────────────────────────────────────────┘
```
