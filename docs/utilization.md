# OpenStack Historical Utilization Reconstruction

## Internal Design Note

### Purpose

This document describes the design used to reconstruct historical
capacity and utilization metrics across OpenStack regions using
incomplete, inconsistent, and partially corrupted control-plane data.
The goal is **auditability, reproducibility, and explainability**, not
perfect historical truth.

------------------------------------------------------------------------

## Core Principles

### 1. Facts Are Immutable

We never overwrite or "fix" historical data. All raw observations are
preserved as evidence.

### 2. State Is Derived

Capacity, availability, and utilization are **derived views** built from
evidence, not stored directly.

### 3. Time Is First-Class

All reasoning is temporal. Every fact has a timestamp and confidence.
Missing time is explicit.

### 4. Uncertainty Is Explicit

Unknown or conflicting data is represented as `unknown`, never coerced
into true/false.

------------------------------------------------------------------------

## Conceptual Model

    Evidence (facts, sightings, events)
            ↓
    State Segments (intervalized attributes)
            ↓
    Canonical Spans (capacity / usage facts)
            ↓
    Event Ledger (+/- deltas)
            ↓
    Cumulative Sums
            ↓
    Derived Metrics & Reports

------------------------------------------------------------------------

## Evidence Layer (Facts)

### Evidence Points

An evidence point means: \> "At time T, we observed attribute A = value
V for entity E."

Examples: - Nova service reports host disabled at T - Backup snapshot
shows host present on day D - Allocation exists referencing host H -
Manual rule asserts site-wide policy

Evidence is **append-only**.

------------------------------------------------------------------------

## Host Identity Model

### Canonical Identity

-   Host identity is `(region, hypervisor_hostname)`
-   Database primary keys are treated as *versions*, not identities

This avoids: - PK churn from migrations - Re-enrollment issues - Double
counting

------------------------------------------------------------------------

## Host Spine

### Why Services Are Canonical

-   `nova.services` is consistent historically
-   Represents actual compute service existence
-   Used to derive:
    -   host existence bounds
    -   disabled / enabled state

### Inventory

-   Inventory is attached **as-of time**
-   Derived from `nova.compute_nodes` + backups
-   Missing inventory is flagged, not guessed

------------------------------------------------------------------------

## Pool Membership (Reservable vs Ondemand)

### Three-State Logic

Host pool membership is: - `reservable` - `not_reservable` - `unknown`

Blazar table gaps do **not** imply ondemand.

### Blazar Signals

-   Blazar computehost presence
-   Allocations referencing host
-   Manual rules

Used to derive reservable status segments.

------------------------------------------------------------------------

## Canonical Spans

A span represents: \> "Between start and end, this entity contributed
quantity Q of resource R."

Span types: - `nova_host` (full usable capacity) - `blazar_host`
(reservable capacity) - `blazar_allocation` (committed capacity) -
`nova_instance_occupied` - `nova_instance_active` - `maintenance_mask` -
`conflict_exclusion`

All spans share a single schema.

------------------------------------------------------------------------

## Maintenance & Conflicts

### Maintenance

Maintenance is an **overlay**, not a consumer.

We distinguish: - usable capacity - admittable capacity

Maintenance produces **mask spans** that subtract from admittable
totals.

### Control-Plane Conflicts

Known bugs (e.g., duplicate Blazar hosts) are modeled as: - explicit
conflict intervals - exclusion spans removing affected capacity

Nothing is silently deduplicated.

------------------------------------------------------------------------

## Event Ledger

Each span produces two ledger events: - `+quantity` at start -
`-quantity` at end

Ledger rules: - immutable - strictly ordered - deterministic replay

------------------------------------------------------------------------

## Aggregation

Cumulative sums are computed over: - `(region, series, resource_type)`

Derived metrics are pure algebra: - available = admittable - committed -
idle = committed - occupied - stopped = occupied - active

------------------------------------------------------------------------

## Validation & Debugging

When violations occur (e.g., usage \> capacity): 1. Identify timestamp
and series 2. Inspect ledger deltas at that time 3. List active spans
contributing 4. Group by issue type (open-ended, conflict, missing
inventory)

All anomalies are explainable by construction.

------------------------------------------------------------------------

## Manual Rules

Manual knowledge is encoded as **signals**, not code paths: - site-wide
policies - known historical constraints

Rules have: - scope - time bounds - confidence - provenance

They override lower-precedence evidence but are fully traceable.

------------------------------------------------------------------------

## What This Model Guarantees

-   Reproducible results from versioned inputs
-   Clear attribution of every number
-   Explicit uncertainty
-   Safe extension as new data sources appear

------------------------------------------------------------------------

## What This Model Does Not Claim

-   Perfect historical truth
-   Exact intent of operators
-   Recovery of data that never existed

------------------------------------------------------------------------

## Summary

This system applies event sourcing, accounting-style ledgers, and
temporal modeling to OpenStack capacity reconstruction. It is
intentionally conservative, explicit, and auditable.

That is a feature, not a limitation.
