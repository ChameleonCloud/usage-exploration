# Chameleon Usage Reporting 2026

Library and pipeline to reconstruct historical openstack usage metrics.

Primary features:
- Archive source data: extract data from production DBs or other sources and archive to parquet files.
- Process source data containing timestamps and unique identifiers into timeline of usage over time.
- Plotting and analysis methods for usage over time results.

## Goals

1. Must be reproducible: same source data -> same results
2. Must be auditable: understand *why* data was transformed, and trace decisions to impact on output
3. Must be explicit and understandable. No magic, and no "write once, read never" code.

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


## Usage (of the tool, not the data)

*Note* This will get a CLI shortly.

### 1. Import Data to Parquet

Configure database connections in `etc/sites.yaml`:

```yaml
chi_uc:
  site_name: "CHI@UC"
  raw_spans: 'data/raw_spans/chi_uc'
  db_uris:
    nova: "mysql://user:pass@host:port/nova"
    nova_api: "mysql://user:pass@host:port/nova_api"
    blazar: "mysql://user:pass@host:port/blazar"
    chameleon_usage: "mysql://user:pass@host:port/chameleon_usage"
```

Dump tables to parquet:

```python
from chameleon_usage.common import load_sites_yaml
from chameleon_usage.data_import.dump_db import dump_site_to_parquet

sites = load_sites_yaml("etc/sites.yaml")
for site_name, site_config in sites.items():
    dump_site_to_parquet(site_config, force=False)
```

Output goes to `data/raw_spans/<site>/` as `.parquet` files.

### 2. Process Data

```python
from datetime import datetime
import polars as pl
from chameleon_usage.engine import SegmentBuilder
from chameleon_usage.pipeline import compute_derived_metrics, resample_simple
from chameleon_usage.registry import ADAPTER_PRIORITY, load_facts

site_name = "chi_uc"
window_end = datetime(2025, 11, 1)

# Load facts from parquet files
facts = load_facts(base_path="data/raw_spans", site_name=site_name)

# Build segments (applies source priority to resolve conflicts)
source_order = [s.config.source for s in ADAPTER_PRIORITY]
engine = SegmentBuilder(site_name=site_name, priority_order=source_order)
segments = engine.build(facts)

# Calculate concurrency (cumulative counts over time)
usage = engine.calculate_concurrency(segments, window_end=window_end)

# Resample to fixed intervals and compute derived metrics
resampled = resample_simple(usage.filter(pl.col("timestamp") <= window_end), "30d")
final = compute_derived_metrics(resampled)
```

See [examples/report.py](examples/report.py) for a complete example with plotting.
