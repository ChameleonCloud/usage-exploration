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
