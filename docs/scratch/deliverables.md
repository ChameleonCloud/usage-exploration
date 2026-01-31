# Paper Deliverables

## Background for paper context:

* Preamble: how resources are reserved, leases
* Next: challenges of gathering the data
* Bullets for each challenge
* Representation of chameleon usage
    * Like SAB
    * Absolute graphs
    * By % graph
    * Site "sum" and comparison graph
* Prioritize chi@edge data asap
* How is the system implemented
    * Usage database
    * Doni, inventory
    * Additions, decommissioning, maintenance
* Generalizability
    * What is openstack specific
    * What is general
    * What is chameleon specific
* Applicability
    * Here's a github repo
    * Recommendation for how to use it


## What we need:
* Need working usage gathering system
    * Reliable framework that gathers data from 2022+
* Documentation:
    * Clear, no "magic" for how primary data sources work.
    * Primary DB:
        * Nova hosts
        * Blazar hosts
        * Blazar allocations
        * Nova instances
* Self-contained runnable system
    * Outputs compatible with KPI report and grafana
* Before 2022:
    * What we know
    * Why is it hard, complex
    * Lessons learned

# Phase 1 Implementation Checklist

- [x] Ingest input schemas.
  - [x] Nova: computenode. <!-- models/raw.py:43-48 -->
  - [x] Nova: instance. <!-- models/raw.py:59-64 -->
  - [x] Blazar: lease. <!-- models/raw.py:20-25 -->
  - [x] Blazar: reservation. <!-- models/raw.py:28-32 -->
  - [x] Blazar: computehost_allocation. <!-- models/raw.py:35-40 -->
  - [x] Blazar: computehost. <!-- models/raw.py:12-17 -->
  - [x] Legacy: node count cache. <!-- models/raw.py:77-80 -->
  - [x] Legacy: node hours cache. <!-- models/raw.py:67-74 -->
- [x] Adapters (Raw → Facts).
  - [x] NovaComputeAdapter → TOTAL. <!-- adapters.py:29-73 -->
  - [x] BlazarComputehostAdapter → RESERVABLE. <!-- adapters.py:123-167 -->
  - [x] BlazarAllocationAdapter → COMMITTED. <!-- adapters.py:170-245 -->
  - [x] NovaInstanceAdapter → OCCUPIED. <!-- adapters.py:76-120 -->
  - [x] LegacyAdapter → legacy counts.
- [x] Build Facts Timeline pipeline schema with fields.
  - [x] timestamp, source, entity_id, quantity_type, value. <!-- models/domain.py:7-12 -->
- [x] Pipeline stages.
  - [x] TimelineBuilder.build: Facts → Timeline. <!-- engine.py:10-46 -->
  - [x] calculate_concurrency: Timeline → Counts. <!-- engine.py:48-84 -->
  - [x] resample_time_weighted: Counts → Resampled. <!-- engine.py:86-111 -->
  - [x] Compute derived states from resampled data. <!-- pipeline.py:23-47 -->
  - [x] Format output with site, collector type columns.
- [ ] Build Usage Timeline output table.
  - [x] site, timestamp, collector type, count type, value. <!-- models/domain.py:24-27 partial: has timestamp, quantity_type, count; missing site, collector type -->
  - [ ] Uniqueness: 1 row per site + timestamp + collector type + count type.
- [x] Compute derived states.
  - [x] Available = Reservable - Committed. <!-- pipeline.py:34 -->
  - [x] Idle = Committed - Occupied. <!-- pipeline.py:35 -->
- [x] Produce usage plots.
  - [x] Legacy vs current facets: output/plots/chi_tacc_facet.png, output/plots/chi_uc_facet.png. <!-- plots.py:93-145 -->
  - [x] Usage stack: output/plots/chi_tacc_stack.png, output/plots/chi_uc_stack.png. <!-- plots.py:34-74 -->
  - [ ] Cross-site utilization: output/plots/cross_site_usage.png.
- [ ] Produce comparison output.
  - [ ] Artifact comparing uncorrected vs legacy with defined join keys.
- [ ] Build Leadtime Timeline output table.
  - [ ] site, timestamp, leadtime type, value.
  - [ ] Uniqueness: 1 row per site + timestamp + leadtime type.
- [ ] Produce leadtime plot.
  - [ ] Point plot with x=timestamp, y=value.
- [ ] Acceptance criteria.
  - [ ] Date coverage: 2016-01-01 through 2025-12-31 (UTC).
  - [ ] Known gaps: nova hosts prior to 2018; blazar hosts prior to 2020.
  - [ ] All schemas have required columns and no extra columns.
  - [ ] All plot files exist and are non-zero size.


# Phase 1.5 (scope creep)

- [ ] Blazar: computehost_extra_capability schema.
- [ ] Add value types.
  - [ ] vcpus.
  - [ ] memory_mb.
  - [ ] disk_gb.
  - [ ] gpus.
- [ ] Add Active usage type.
- [ ] Compute derived state: Stopped = Occupied - Active.
- [ ] Add grouping columns to output schemas.
  - [ ] node type.
  - [ ] hypervisor hostname.
- [x] Produce KVM usage stack plot: output/plots/kvm_tacc_stack.png. <!-- pipeline.py:92 -->
- [ ] Add KVM to cross-site utilization (yellow color).
- [ ] Add leadtime plot comparison: requested vs effective.


# Phase 3
- [ ] Add projected leadtime type (reconstruct past timeline).


# Architectural Debt (Prioritized)

Ordered by impact/effort ratio. High impact + low effort first.

## P0: Split Registry Responsibilities (HIGH impact, LOW effort)

- **Evidence:** `registry.py` handles adapter ordering, file discovery, schema validation, source cataloging, and fact concatenation.
- **Fix:** Split into `loader.py` (file I/O), `registry.py` (adapter orchestration), and keep schemas in `models/`.
- **Result:** Each concern is isolated; new maintainers can understand one without all.

## P1: Decouple Ingestion from Compute (HIGH impact, LOW effort)

- **Evidence:** `load_facts()` couples file layout to compute. Missing parquet files break the pipeline even when those sources are optional.
- **Fix:** Add `load_raw_inputs(path, inputs)` and `facts_from_inputs(...)`. Accept adapter list + "skip missing" flag.
- **Result:** Non-blazar sites run via config; compute can consume prebuilt facts.

## P2: Make Source Priority Configurable (HIGH impact, MEDIUM effort)

- **Evidence:** `engine.py` pivot + forward_fill + coalesce is undocumented. Can't express "gap-fill from another source."
- **Fix:** Extract `resolve_source_priority()` as named function. Make resolution a policy per `quantity_type` (authoritative, gap-fill, supplement).
- **Result:** Gap-filling nova+blazar becomes config, not code change.

## P3: Site Config in YAML (MEDIUM impact, LOW effort)

- **Evidence:** `etc/sites.yaml` has DB URIs only. Adapter selection hardcoded in `registry.py`. Unused `rules.yaml` creates confusion.
- **Fix:** Extend `sites.yaml` to declare enabled adapters and priority. Delete or integrate `rules.yaml`.
- **Result:** New sites are config changes, not code changes.

## P4: Add Join Logging (HIGH impact, MEDIUM effort)

- **Evidence:** `BlazarAllocationAdapter` chains 3 LEFT joins, silently drops failures. TODO on line 155 confirms this was known-problematic.
- **Fix:** Add `_log_join_stats()` after each join. Track row counts in/out.
- **Result:** Silent data loss becomes visible.

## P5: Explicit Pipeline Stages (MEDIUM impact, MEDIUM effort)

- **Evidence:** `examples/report.py` mixes ingestion, compute, merge, plotting. No stage I/O helpers.
- **Fix:** Add `save/load_facts()`, `save/load_usage()`. Create `run_pipeline()` that returns usage without plotting.
- **Result:** Stages cacheable via parquet; viz runs without recompute.

## P6: Column Naming Cleanup (MEDIUM impact, LOW effort)

- **Evidence:** `value` in FactSchema = state string; `count` in UsageSchema = numeric; `final_state` means different things per stage.
- **Fix:** Rename `FactSchema.value` → `state`. Add column docs to `domain.py`.
- **Result:** Names match semantics.

## P7: Audit Trail (MEDIUM impact, HIGH effort)

- **Evidence:** Exclusions scattered across adapters, engine, pipeline. No provenance tracking.
- **Fix:** Add optional "rejects" LazyFrame output from adapters/engine (reason + row id + hours impact).
- **Result:** Audit report is a stage output, not code-wide patch.

## P8: Tests (HIGH impact, HIGH effort)

- **Evidence:** Zero tests despite pytest in dev deps. Can't safely change anything.
- **Fix:** Start with 3 tests: one per adapter type + `engine.build()` smoke test. Build incrementally.
- **Result:** Changes become safe.

---

## Target Architecture

```
src/chameleon_usage/
├── core/
│   ├── loader.py        # File I/O only
│   ├── registry.py      # Adapter orchestration only
│   ├── engine.py        # Timeline building (with extracted priority resolution)
│   └── pipeline.py      # Resampling + derived metrics
├── ingest/
│   └── adapters.py      # All adapters (or split per source if large)
├── models/
│   ├── raw.py
│   └── domain.py
├── viz/
│   └── plots.py
└── io/
    ├── dump_db.py
    └── audit.py         # Optional audit output
```
