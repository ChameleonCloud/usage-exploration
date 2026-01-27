# urgent, ordered, implementation plan



## Invariants that are always true.

1. never lose rows. input rows = output rows + rejected_with_reason rows
2. never lose hours. hours = end-start per row. Per row: input hours = output hours + rejected_with_reason hours

## Day 1
- [x]: define DB tables to fetch
- [x]: fetch tables, per site, from local dbs to parquet files.
   stored in input/{site_name}/{schemaname}.{tablename}.{date}.parquet
- [x]: fetch legacy `chameleon_usage` tables
- [x]: import data from parquet
- [x]: plot "legacy" chameleon usage
- [x]: do initial joins?

## Day 2 base
- [x] Fix `etc/sites.yaml:5` to use `output/raw_spans/chi_tacc`
- [x] Add `NovaHostSource` from `nova.services` (compute services only)
- [x] Add `BlazarHostSource` from `blazar.computehosts`
- [x] Tag both valid and audit rows with `source`
- [x] Implement minimal ledger → cumsum aggregation and plot the 4 series
   - [x] split spans to events
   - [x] sweepling alg on events
   - [x] add pure math method to resample for outputs
   - [x] plot results

## Day 2 iteration

### Goals (augmentation & derived metrics)

- [x] fix grouping of audit data: group by source, data status, year(start date)
- [x] emit audit data with results
- [x] better output format
- [x] check invariant: no lost rows
- [ ] check invariant: no lost hours
- [ ] legacy comparison plot




- [ ] investigate available nova services data: does it give better timestamps and history for nova compute nodes?
- [ ] decide how to combine nova services and compute hosts. is it a join? host spine?

- [ ] **Host Spine**: Use `nova.services` as canonical host existence (has disabled_reason, heartbeat history). Currently using `nova.compute_nodes` which lacks this.

- [ ] **Active vs Occupied**: Replay `instance_actions` to distinguish running vs stopped instances. Adds `nova_instance_active` series.

- [ ] **Derived Metrics**: Pure algebra after sweepline:
  - `available = admittable - committed`
  - `idle = committed - occupied`
  - `stopped = occupied - active`

- [ ] **Maintenance Mask**: Load `chameleon_usage.node_maintenance`, emit negative overlay spans that subtract from admittable.

- [ ] **Conflict Exclusion**: Detect duplicate Blazar hosts, emit exclusion spans. Nothing silently deduplicated.

- [ ] **Three-State Pool Membership**: Blazar gaps ≠ ondemand. Represent as `reservable | not_reservable | unknown`. Uncertainty is explicit.


### Smaller next steps

- [ ] (minor): resampling math uses "last", should use time-weighted average
