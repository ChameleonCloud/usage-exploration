# Refactor Plan: Simplify Pipeline

## Goal
Remove ~200 lines of code, reduce coupling, make pipeline trustworthy.

## Target Structure

```
src/chameleon_usage/
├── schemas.py             # Pipeline schemas: IntervalSchema, CountSchema, UsageSchema
├── constants.py           # Cols, QuantityTypes (no States)
├── math/
│   └── transforms.py      # Pure functions, no validation, no domain knowledge
├── ingest/
│   ├── loader.py          # load_intervals()
│   ├── adapters.py        # to_intervals()
│   └── rawschemas.py      # Raw table schemas (Nova, Blazar)
├── pipeline.py            # Domain wrappers with validation, compute_derived_metrics
├── viz/
│   └── plots.py
└── extract/
    └── dump_db.py
```

## Schema Validation Pattern

`math/` stays pure - no validation, no domain knowledge, column names as parameters.

`pipeline.py` provides domain-aware wrappers that validate at boundaries:

```python
# pipeline.py
from chameleon_usage.math import transforms
from chameleon_usage.schemas import IntervalSchema, CountSchema

def intervals_to_counts(df: pl.LazyFrame) -> pl.LazyFrame:
    """Validated wrapper around pure function."""
    IntervalSchema.validate(df)
    result = transforms.intervals_to_counts(df, "start", "end", ["quantity_type"])
    return CountSchema.validate(result)
```

## Steps

### Step 1: Create `math/transforms.py`
- [x] `intervals_to_deltas()`
- [x] `deltas_to_counts()`
- [x] `intervals_to_counts()`
- [ ] Add `resample()` - move from pipeline.py, make column-agnostic

### Step 2: Create `schemas.py`
- [ ] `IntervalSchema` (entity_id, start, end, quantity_type)
- [ ] `CountSchema` (timestamp, quantity_type, count)
- [ ] `UsageSchema` (timestamp, quantity_type, count, site, collector_type)

### Step 3: Simplify `ingest/`
- [ ] Create `ingest/loader.py` with `load_intervals(base_path, site_name)`
- [ ] Move raw schemas to `ingest/rawschemas.py`
- [ ] Simplify `adapters.py`: return intervals directly, no `_expand_events()`
- [ ] Delete `ADAPTER_PRIORITY` and priority resolution

### Step 4: Update `pipeline.py`
- [ ] Add validated wrappers: `intervals_to_counts()`, `resample()`
- [ ] Keep `compute_derived_metrics()`
- [ ] Import from `schemas.py`

### Step 5: Delete dead code
- [ ] Delete `engine.py` (~160 lines)
- [ ] Delete `registry.py` (~135 lines)
- [ ] Delete `config.py` (~42 lines)
- [ ] Delete `models/` directory
- [ ] Delete `States` enum from `constants.py`

### Step 6: Update `examples/report.py`
- [ ] Use `ingest.load_intervals()`
- [ ] Use `pipeline.intervals_to_counts()` (validated)
- [ ] Use `pipeline.resample()` (validated)
- [ ] Use `pipeline.compute_derived_metrics()`
- [ ] Delete `counts_by_source()` local function
- [ ] Target: ~25 lines

### Step 7: Update tests
- [x] `test_intervals.py` for math/transforms
- [ ] Add test for `resample()`
- [ ] Delete tests for removed code (engine, registry)

## Files to Delete
| File | Lines | Reason |
|------|-------|--------|
| `engine.py` | ~160 | Replaced by math/transforms |
| `registry.py` | ~135 | Replaced by ingest/loader |
| `config.py` | ~42 | Inlined into ingest/ |
| `models/` | ~80 | Split into schemas.py + ingest/rawschemas.py |

## Net Result
- ~300 lines deleted
- Clear separation: `math/` (pure) vs `pipeline/` (domain + validation)
- Strong schema validation at stage boundaries
- `report.py` becomes obvious ~25 lines
- Core functions tested and trusted
