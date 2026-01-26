# urgent, ordered, implementation plan



## Invariants that are always true.

1. never lose rows. input rows = output rows + rejected_with_reason rows
2. never lose hours. hours = end-start per row. Per row: input hours = output hours + rejected_with_reason hours

## Order of operations

- [x]: define DB tables to fetch
- [x]: fetch tables, per site, from local dbs to parquet files.
   stored in input/{site_name}/{schemaname}.{tablename}.{date}.parquet
- [x]: fetch legacy `chameleon_usage` tables
- [x]: import data from parquet
- [ ]: plot "legacy" chameleon usage
- [ ]: do initial joins?
