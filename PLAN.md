# urgent, ordered, implementation plan



## Invariants that are always true.

1. never lose rows. input rows = output rows + rejected_with_reason rows
2. never lose hours. hours = end-start per row. Per row: input hours = output hours + rejected_with_reason hours

## Order of operations

Day 1
- [x]: define DB tables to fetch
- [x]: fetch tables, per site, from local dbs to parquet files.
   stored in input/{site_name}/{schemaname}.{tablename}.{date}.parquet
- [x]: fetch legacy `chameleon_usage` tables
- [x]: import data from parquet
- [x]: plot "legacy" chameleon usage
- [x]: do initial joins?

Day 2
- [x] Fix `etc/sites.yaml:5` to use `output/raw_spans/chi_tacc`
- [x] Add `NovaHostSource` from `nova.services` (compute services only)
- [x] Add `BlazarHostSource` from `blazar.computehosts`
- [x] Tag both valid and audit rows with `source`
- [ ] Implement minimal ledger → cumsum aggregation and plot the 4 series
   - [ ] split spans to events
   - [ ] sweepling alg on events
- [ ] add pure math method to resample for outputs
