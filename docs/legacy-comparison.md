# Design Comparison: chameleon_usage vs pandera_plan

## Overview

This document compares the current `chameleon_usage` repo against the `pandera_plan` reference implementation in `~/Developer/scratch/chameleon-ops/cc_usage/pandera_plan/`.

**Current repo:** Class-based span computation (~300 lines)
**Reference:** Functional layer-based pipeline (~2000 lines)

Both need the same features: hierarchy validation, bi-temporal modeling, row traceability. The question is which design gets there with less complexity.

---

## Design Philosophy Comparison

### Reference: Functional Layers

```
Layer 0 (Extract)     → raw/*.parquet
Layer 1 (Build Spans) → spans.parquet + rejected.parquet
Layer 2 (Validate)    → validated.parquet (hierarchy via DuckDB temporal joins)
Layer 3-4 (Aggregate) → utilization.parquet (sweepline)
```

- Each layer is a function: `build_spans(raw) → spans`
- Layers communicate via parquet files
- Hierarchy relationships encoded in join logic
- Validation is schema checks at boundaries

Pros:
- Clear data flow
- Independent layer testing
- Checkpointing for free

Cons:
- Lots of boilerplate for layer boundaries
- Hierarchy relationships spread across join code
- ~400 lines just for hierarchy validation (DuckDB SQL)
- Hard to see entity relationships at a glance

### Current Repo: Class-Based Entities

```
BaseSpanSource
    ├── BlazarCommitmentSource
    └── NovaOccupiedSource
```

- `BaseSpanSource` defines the span contract
- Subclasses encode entity-specific logic (joins, end signals)
- `get_spans()` returns (valid, audit)

Pros:
- Entity-specific logic stays with entity
- Hierarchy can be encoded in class relationships
- Less boilerplate
- Easier to reason about parent-child

Cons:
- Lazy evaluation makes debugging harder
- No automatic checkpointing
- Validation logic mixed with transform logic

---

## Feature Gap Analysis

### 1. Row Traceability (INV-3)

**Current:** No assertion. Audit log exists but `input != valid + audit` is never checked.

**Reference:** Explicit assertion:
```python
assert input.height == spans.height + rejected.height
```

**Gap:** Silent data loss possible.

**Class-based fix:** Add assertion in `BaseSpanSource.get_spans()`.

---

### 2. Rejection Tracking

**Current:** Single `data_status` column with 4 values.

**Reference:** Two columns:
- `rejection_reason`: null_start, invalid_time_range, missing_fk
- `rejection_detail`: which join failed, original values

**Class-based fix:** Extend `_tag_rejections()` in base class:
```python
def _tag_rejections(self, df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(
        rejection_reason=...,
        rejection_detail=...,
    )
```

---

### 3. Span Hours

**Current:** Not implemented.

**Reference:** Computed on all spans:
```python
hours = (coalesce(end, reference_time) - start).total_seconds() / 3600
```

**Class-based fix:** Add to base class output schema.

---

### 4. Hierarchy Validation

**Current:** None. Blazar and Nova spans independent.

**Reference:** ~400 lines of DuckDB SQL for temporal joins. Produces `hierarchy_status`: direct/clamped/orphan.

**Class-based alternative:** Encode hierarchy in class relationships:

```python
class BaseSpanSource:
    parent_source: Optional["BaseSpanSource"] = None
    parent_join_key: str = "hypervisor_hostname"

class NovaHostSource(BaseSpanSource):
    parent_source = None  # root of hierarchy

class BlazarHostSource(BaseSpanSource):
    parent_source = NovaHostSource  # joined via hostname + time overlap

class AllocationSource(BaseSpanSource):
    parent_source = BlazarHostSource  # joined via compute_host_id FK

class InstanceSource(BaseSpanSource):
    parent_source = AllocationSource  # reserved booking
    # OR parent_source = NovaHostSource  # on-demand booking
```

Hierarchy validation becomes a method on base class:
```python
def validate_hierarchy(self) -> pl.LazyFrame:
    if self.parent_source is None:
        return self.spans.with_columns(hierarchy_status=pl.lit("root"))

    parent = self.parent_source.get_spans().spans
    return temporal_join(self.spans, parent, self.parent_join_key)
```

**Advantage:** Hierarchy is visible in type system. Reference buries it in SQL.

---

### 5. Bi-Temporal Modeling

**Current:** Only `start_date`, `end_date`.

**Reference:** Four columns:
- `valid_start`, `valid_end`: when resource state was true
- `tx_start`, `tx_end`: when system learned about it

**Class-based fix:** Column mapping in base class:
```python
class BaseSpanSource:
    valid_start_col: str = "start_date"
    valid_end_col: str = "end_date"
    tx_start_col: str = "created_at"
    tx_end_col: str = "updated_at"
```

Lens selection in `get_spans()`:
```python
def get_spans(self, lens: Literal["effective", "projected"] = "effective"):
    # effective: what actually happened (valid_start/end)
    # projected: what was known at time T (tx_start/end)
```

---

### 6. Legacy Comparison

**Current:** Loads `node_usage_report_cache`, plots it. No comparison.

**Reference:** Framework exists but incomplete.

**Fix:** Same either way - aggregate computed hours, diff against legacy.

---

### 7. Manual Exclusions

**Current:** None.

**Reference:** `etc/exclusions.yaml` for known-bad records.

**Fix:** Load YAML, filter in `RawSpansLoader` before returning tables.

---

## Complexity Comparison (Feature Complete)

### Line Count Estimate

| Component | Reference (pandera_plan) | Class-Based (this repo) |
|-----------|--------------------------|-------------------------|
| **Schemas** | | |
| Raw schemas | 80 | 60 (existing) |
| Span schemas | 90 | 40 |
| Validated schemas | 70 | 30 |
| Utilization schemas | 60 | 30 |
| **Core Logic** | | |
| Extraction / Loading | 220 | 50 (existing) |
| Span building | 170 | 100 |
| Rejection tagging | 80 | 40 |
| Hierarchy validation | 413 | 80 |
| Temporal join logic | 150 (DuckDB SQL) | 60 (Polars) |
| Bi-temporal lenses | 100 | 30 |
| Aggregation (sweepline) | 400 | 400 (same complexity) |
| **Infrastructure** | | |
| Invariant checks | 142 | 60 |
| Contracts/assertions | 100 | 30 |
| Pipeline orchestration | 150 | 60 |
| Reporting | 184 | 80 |
| CLI | 50 | 30 |
| **Tests** | | |
| Invariant tests | 200 | 100 |
| Layer tests | 300 | 150 |
| **Total** | **~2960 lines** | **~1460 lines** |

### Complexity Drivers

| Aspect | Reference | Class-Based | Winner |
|--------|-----------|-------------|--------|
| Hierarchy definition | SQL joins, spread across files | Class attributes, one place | Class |
| Adding new entity type | New layer function + schema + SQL | New subclass | Class |
| Debugging data flow | Read parquet checkpoints | Collect LazyFrame mid-chain | Reference |
| Schema evolution | Edit schema + layer + tests | Edit schema + class | Class |
| Temporal join edge cases | DuckDB handles well | Polars asof_join limited | Reference |
| Bi-temporal queries | Separate lens functions | Method parameter | Class |
| Team onboarding | Follow layer flow | Understand inheritance | Tie |

### Where Complexity is Equal

- **Sweepline aggregation:** Same algorithm either way (~400 lines)
- **Legacy comparison:** Same logic either way
- **Exclusions handling:** Same YAML + filter pattern

### Where Class-Based Wins

- **Hierarchy is declarative:** `parent_source = BlazarHostSource` vs 50 lines of SQL
- **Entity logic is colocated:** Joins, end signals, rejection rules all in one class
- **Less indirection:** No layer boundaries, no parquet serialization between steps
- **Smaller surface area:** ~50% less code to maintain

### Where Reference Wins

- **Checkpointing:** Each layer output is inspectable parquet
- **Temporal joins:** DuckDB handles hostname ambiguity edge cases better
- **Independent testing:** Can test Layer 2 without running Layer 1
- **Clearer data contracts:** Schema files are the API

### Risk Assessment

| Risk | Reference | Class-Based |
|------|-----------|-------------|
| Silent data loss | Low (INV-3 everywhere) | Medium (must add INV-3) |
| Hostname ambiguity | Low (DuckDB handles) | Medium (Polars asof_join) |
| Double-counting | Low (INV-7 checks) | Medium (must add INV-7) |
| Schema drift | Low (boundary validation) | Medium (output not validated) |
| Debugging production issues | Low (checkpoints) | High (no checkpoints) |

### Recommendation

Class-based is **~50% less code** but requires:
1. Adding invariant assertions (INV-3, INV-7)
2. Adding output schema validation
3. Careful testing of temporal join edge cases
4. Optional: checkpoint parquet writes for debugging

The reference's extra complexity buys safety. The class-based approach trades safety for simplicity - acceptable if invariants are enforced.

---

## Reference Invariants to Adopt

Keep these regardless of design:

| ID | Invariant | Check |
|----|-----------|-------|
| INV-1 | `valid_start NOT NULL` | Reject unmeasurable spans |
| INV-2 | `valid_start < valid_end` (or NULL) | Reject invalid ranges |
| INV-3 | `input = accepted + rejected` | Assert row count |
| INV-5 | Orphans not clamped | No original_start/end on orphans |
| INV-6 | Clamped → original preserved | Audit trail |
| INV-7 | No duplicate entity | Prevent double-count |

---

## Recommended Path

### Phase 1: Validation Foundation

1. Add `rejection_reason`, `rejection_detail` columns
2. Add INV-3 assertion (row traceability)
3. Add `span_hours` calculation
4. Build rejection summary (count + hours by reason)

### Phase 2: Hierarchy

1. Add `parent_source` to `BaseSpanSource`
2. Implement `validate_hierarchy()` with temporal join
3. Add `hierarchy_status`: root/direct/clamped/orphan
4. Preserve `original_start/end` when clamping

### Phase 3: Bi-Temporal

1. Add temporal column config to base class
2. Implement lens selection (effective/projected)
3. Add INV-11, INV-12 checks

### Phase 4: Comparison

1. Aggregate to daily node-hours
2. Compare against legacy `node_usage_report_cache`
3. Report discrepancies by date, node_type

---

## Open Questions

1. **Temporal join implementation:** Polars asof_join vs DuckDB SQL? Polars is simpler but DuckDB handles edge cases better.

2. **Checkpointing:** Add parquet writes between phases? Useful for debugging but adds complexity.

3. **On-demand vs reserved instances:** Different parent chains. One class with conditional parent, or two subclasses?

4. **Hostname ambiguity:** Reference uses exclusions.yaml. Is that sufficient or do we need runtime detection?
