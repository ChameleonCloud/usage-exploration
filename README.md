# Chameleon Usage Reporting 2026


## Data Model

### 2.1 Utilization Stack

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


## What's Easy

The core algorithm is ~120 lines: sweepline + cumsum converts overlapping time spans into concurrent counts at each point in time.

## What's Hard

The openstack DB stores "current", not historical data (generally)

| Problem | Why It's Hard |
|---------|---------------|
| **Hostname reuse** | Same hostname → different physical hosts over time. Must join by hostname + time overlap. |
| **Instance state reconstruction** | `nova.instances` is a snapshot. Must replay `instance_actions` to get historical running/stopped states. |
| **3-year data gap** | Instances from 2015, host tracking from 2018 due to hard-deletes. |
| **Missing FKs** | Blazar has no FK to Nova. Join via hostname + temporal overlap. |
| **Derived bounds** | Allocation dates come from 3-way join: `allocation → reservation → lease`. |



## Invariants we need:

1. never drop rows, len(output) + len(rejected) == len(input)
2. never drop "hours": row*(end-start): for each row, output hours = accepted hours + rejected hours




## Prior implementation:

Timestamp, total reservable, total reservations, and occupied reservations

Issue 1: poor per-node-type breakdown, no maintenance, no active vs stopped
What's Easy

## “Entity”
Nova host
Blazar host
Blazar allocation
When is a allocation “active”
Nova instance


Step 1:
Just plot the “current”/ old data, totals of “reservable” and “used”
Beginning through “now
Have implementation, we have calculated
“New data”
Openstack DBs for uc, tacc, kvm
Instances -> 2015
Blazar allocation -> 2015
Blazar hosts -> 2018-2020ish with gaps
Nova hosts -> 2018-2020ish with gaps
“Nova services” -> nova hosts, with created and deleted times
Reference repo: created, deleted, resources, for all hosts since 2015
UC db backup from 2020, with 70%ish of missing data
TACC: backups from 2020,2019,2018,2016, constructed all

## Proposal

Verify approach is sane, plot current DB vs legacy usage reporting
Verify: how many rows and hours do we throw out from orphans, how many do we clip, what % of total, and what years are affected
Decision point
Potential exit: Maybe it’s ok to report from 2023+?
Or come up with set of rules to “fix” pre-2023 data so it maps to post-2023 data for analysis (with rules clearly documented for how/what/why we did it). These rules can be defined and run to generate our new data. (And rerun at any point in the future on our dbs to get the exact same data every time.)
Based on the ranked “hourly error” (by site, year, category), see how much we can fix from the supplemental issues
What does that give us the output



## Outputs

Post-2023: everything for all sites
Pre-2023:
After augmentation, what can we say.


Goal: analysis will be valid for “all future”, can run every day or constantly.


Results we can generate with 2023+ data as-is:
Usage over time: total > reservable > committed > occupied > active back to 2023.

Results we can generate with pre-2023 data as-is:
Effective lead time: “lease start - created”. Doesn’t tell us “why” a user made future lease. Gpu node saturation vs course or workshop…
Results we can generate if we “fix” pre-2023 data:
Ideally this will be everything we can do with 2023+ results but from 2015+???
Usage over time pre-2023
Join legacy usage reporting total capacity - reservable
“Projected lead time”: if a user came to the system at time T, what would they have seen in the scheduler.
Leases can be extended
Can be deleted midway through
Can be deleted before they start
Gaps: when did a blazar host exist, and when was it reservable. 
But: legacy “total” is exists - maintenance
