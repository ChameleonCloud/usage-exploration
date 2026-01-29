# Phase 1 Implementation Checklist

- [ ] Ingest input schemas.
  - [ ] Nova: computenode, instance.
  - [ ] Blazar: lease, reservation, computehost_allocation, computehost, computehost_extra_capability.
  - [ ] Legacy: node count cache, node hours cache.
- [ ] Build Facts Timeline pipeline schema with fields.
  - [ ] timestamp, source, entity_id, quantity_type, value, node type, hypervisor hostname.
- [ ] Build Usage Timeline output table.
  - [ ] site, timestamp, collector type, count type, value.
  - [ ] Uniqueness: 1 row per site + timestamp + collector type + count type.
- [ ] Compute derived states.
  - [ ] Available = Reservable - Committed.
  - [ ] Idle = Committed - Occupied.
- [ ] Produce usage plots.
  - [ ] Legacy vs current facets: output/plots/chi_tacc_legacy_facets.png, output/plots/chi_uc_legacy_facets.png.
  - [ ] Usage stack: output/plots/chi_tacc_usage_stack.png, output/plots/chi_uc_usage_stack.png.
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
- [ ] Produce KVM usage stack plot: output/plots/kvm_tacc_usage_stack.png.
- [ ] Add KVM to cross-site utilization (yellow color).
- [ ] Add leadtime plot comparison: requested vs effective.


# Phase 3
- [ ] Add projected leadtime type (reconstruct past timeline).
