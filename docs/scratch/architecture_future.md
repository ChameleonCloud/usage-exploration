# Usage ETL Project Architecture

> **Note:** This document describes the intended end-state architecture for `usage_etl`.

---

## Part 1: Architecture

### Overview

This document defines the file structure and module responsibilities for the `usage_etl` library. The project follows the src layout pattern to ensure robust testing and packaging.

### File Tree

```
usage_etl/
├── pyproject.toml           # Build/Dependency Config
├── README.md
├── tests/                   # External Test Suite
│   ├── conftest.py          # Backend Runners (Ibis/DuckDB)
│   ├── unit/
│   │   ├── __init__.py
│   │   ├── test_core.py     # Math verification
│   │   ├── test_config.py   # Pydantic validation
│   │   ├── test_rules.py    # Refinery logic
│   │   └── test_enrich.py   # SCD Join logic
│   └── integration/
│       ├── __init__.py
│       ├── test_pipe.py     # End-to-end Orchestrator
│       └── test_cli.py      # CLI execution
└── src/
    └── usage_etl/
        ├── __init__.py      # Public API Facade
        ├── __main__.py      # Execution Entrypoint
        ├── cli/
        │   ├── __init__.py
        │   └── entry.py     # Argparse & Handlers
        ├── config/
        │   ├── __init__.py
        │   └── schema.py    # Pydantic Models
        ├── core/
        │   ├── __init__.py
        │   ├── intervals.py # Segment/Winner Math
        │   ├── usage.py     # Delta/CumSum Math
        │   ├── enrichment.py# SCD/Context Join Math
        │   └── audit.py     # Invariant Verification
        ├── ingest/
        │   ├── __init__.py
        │   ├── catalog.py   # Ibis Repository
        │   ├── registry.py  # Adapter Factory
        │   └── adapters/
        │       ├── __init__.py
        │       ├── base.py  # Protocol Definition
        │       ├── generic.py # Config-driven logic
        │       └── ping.py  # Sessionization Logic
        ├── refinery/
        │   ├── __init__.py
        │   ├── engine.py    # Rule Dispatcher
        │   └── rules.py     # Clamp/Split Logic
        ├── pipeline/
        │   ├── __init__.py
        │   └── runner.py    # Stateful Orchestrator
        ├── extractors/
        │   ├── __init__.py  # PEP 562 Lazy Load
        │   ├── sql.py       # SQL logic (Optional)
        │   └── prom.py      # PromQL logic (Optional)
        └── analysis/
            ├── __init__.py  # PEP 562 Lazy Load
            └── plots.py     # Matplotlib logic (Optional)
```

### Module Responsibilities

#### Root Configuration

- **pyproject.toml**: Defines project metadata, build backend, and dependency sets (core, extract, viz).

#### CLI (`src/usage_etl/cli/`)

- **entry.py**: The main entry point using argparse. Handles subcommand routing (`run`, `extract`, `viz`) and implements local imports to prevent crashes when optional dependencies are missing.

#### Config (`src/usage_etl/config/`)

- **schema.py**: Contains Pydantic models (`PipelineConfig`, `RefineryRule`) to parse and validate YAML configuration files. Ensures fail-fast behavior for typos.

#### Core Logic (`src/usage_etl/core/`)

- **intervals.py**: Pure Ibis/Polars logic for "Gap and Islands" problems, atomic segmentation, and winner resolution. Zero I/O.
- **enrichment.py**: Logic for joining resolved segments against Slowly Changing Dimensions (SCDs). Splits usage segments further based on attribute changes.
- **usage.py**: Logic for converting resolved segments into Tidy Usage data (Unpivot → Sort → CumSum).
- **audit.py**: Helper classes (`InvariantVerifier`) to mathematically prove conservation of time/mass during data cleaning.

#### Ingest (`src/usage_etl/ingest/`)

- **catalog.py**: Implements the Repository Pattern. Manages Ibis connections and physical file paths/globs.
- **registry.py**: Maps configuration strings (e.g., `"ping_metric"`) to concrete Adapter classes.
- **adapters/**: Contains transformation logic to standardize raw inputs into the internal `[entity_id, start, end]` schema.

#### Refinery (`src/usage_etl/refinery/`)

- **engine.py**: Iterates through the Configuration Rules and dispatches them to specific handlers.
- **rules.py**: Implements the logic for modifying intervals, such as "Clamping" (truncation) or "Splitting" (valid/audit separation).

#### Pipeline (`src/usage_etl/pipeline/`)

- **runner.py**: The `LifecyclePipeline` class. Manages state (`evidence`, `segments`, `resolved`) to allow interactive use in Notebooks (load → pause → refine → pause → calculate).

#### Optional Modules (Lazy Loaded)

- **extractors/**: Contains code for fetching data from upstream APIs (OpenStack, Prometheus). Uses PEP 562 to error gracefully if `sqlalchemy` or `requests` are missing.
- **analysis/**: Contains code for generating Stack Plots and Truth Charts. Uses PEP 562 to error gracefully if `matplotlib` is missing.

### Pipeline Data Flow

The pipeline consists of six distinct transformation stages. Each stage is strictly typed and decoupled from the specific execution engine (Ibis/Polars/DuckDB).

```
┌─────────────────┐
│  Raw Sources    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Stage 1: Ingest │  → StandardInterval
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Stage 2: Refine │  → StandardInterval (cleaned) + AuditInterval (rejected)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Stage 3: Resolve│  → ResolvedSegment
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Stage 4: Enrich │  → EnrichedSegment
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Stage 5: Calc   │  → UsageStat
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Stage 6: Export │  → Parquet / Plots
└─────────────────┘
```

#### Stage 1: Ingest & Normalization

- **Module**: `ingest.adapters`
- **Goal**: Convert disparate raw inputs into a standard "Evidence" schema.
- **Input**: Raw Parquet / SQL Tables (Any Schema)
- **Output**: `StandardInterval`

#### Stage 2: Refinery (Cleaning)

- **Module**: `refinery.engine`
- **Goal**: Apply business rules (Clamping, Splitting) to enforce logical constraints (e.g., "No Zombies").
- **Input**: `StandardInterval` (Raw Evidence)
- **Output A** (Primary): `StandardInterval` (Cleaned Evidence)
- **Output B** (Secondary): `AuditInterval` (Rejected/Truncated Evidence)

#### Stage 3: Core Resolution

- **Module**: `core.intervals`
- **Goal**: Resolve conflicting evidence into a single non-overlapping timeline.
- **Input**: `StandardInterval` (Cleaned Evidence)
- **Intermediate**: `AtomicSegment`
- **Output**: `ResolvedSegment`

#### Stage 4: Contextual Enrichment (SCDs)

- **Module**: `core.enrichment`
- **Goal**: Join Usage Segments with Attribute History.
- **Input**: `ResolvedSegment` + `AttributeInterval`
- **Output**: `EnrichedSegment`

#### Stage 5: Usage Calculation

- **Module**: `core.usage`
- **Goal**: Convert duration-based segments into time-series counts.
- **Input**: `EnrichedSegment` or `ResolvedSegment`
- **Output**: `UsageStat`
- **Invariant**: Sum of counts across all sources equals total concurrent entities.

#### Stage 6: Analysis & Export

- **Module**: `analysis.exports` / `analysis.viz`
- **Goal**: Prepare Tidy data for consumption (Parquet/Plots).
- **Input**: `UsageStat`
- **Output A** (File): Wide-Format Parquet (Pivot source → Columns, densified via ffill)
- **Output B** (Plot): Stack Plot / Truth Chart (Resampled to target frequency, e.g., 1H or 1D)

### Interfaces & Usage Patterns

#### 1. Command Line Interface (CLI)

**Target**: Operations, CI/CD, Scheduled Jobs (Cron/Airflow).

The CLI is a thin wrapper around the core library, facilitating standard batch operations.

```bash
# Ingest Raw Data (Extract)
usage-etl extract openstack --db-url postgres://user:pass@host/nova --out-dir raw_data/

# Run Pipeline
usage-etl run --config config/prod.yaml --site lax --output output/usage.parquet

# Generate Report
usage-etl viz output/usage.parquet --out-dir reports/
```

#### 2. Python Library (Ad-Hoc Scripting)

**Target**: Custom automation, Integration into larger ETL frameworks.

Uses the Facade Pattern via `__init__.py` to expose a clean API.

```python
from usage_etl import LifecyclePipeline, PipelineConfig

# 1. Load Configuration
cfg = PipelineConfig.from_yaml("config/custom_policy.yaml")

# 2. Instantiate & Run
pipe = LifecyclePipeline(cfg)
df = pipe.run()

# 3. Export (Interoperability with Pandas/Polars)
df.to_parquet("custom_output.parquet")
```

#### 3. Jupyter Notebook (Interactive Exploration)

**Target**: Data Scientists, Audit Analysis, Rule Tuning.

Leverages the Stateful Orchestrator to inspect intermediate stages.

```python
from usage_etl import LifecyclePipeline, PipelineConfig
from usage_etl.analysis import plots

# Step 1: Load Evidence & Inspect
pipe = LifecyclePipeline(cfg)
pipe.load()
display(pipe.evidence.head())

# Step 2: Refine & Audit
pipe.refine()
display(pipe.audit_log.head())  # View rejected records

# Step 3: Calculate & Visualize
usage = pipe.process()
plots.plot_truth_chart(usage)
```

---

## Part 2: Data Schemas

These contracts define the shape of data passed between pipeline stages. All DataFrames must conform to these structures.

### Evidence Schemas

#### `StandardInterval`

The raw currency of the pipeline.

```python
class StandardInterval(TypedDict):
    entity_id: str
    start: datetime
    end: datetime
    source: str
    priority: int
```

#### `AuditInterval`

Rejected evidence for savings analysis.

```python
class AuditInterval(TypedDict):
    entity_id: str
    start: datetime
    end: datetime
    source: str
    audit_reason: str
```

### Segmentation Schemas

#### `AtomicSegment`

Non-overlapping micro-segments used for topological operations.

```python
class AtomicSegment(TypedDict):
    entity_id: str
    start: datetime
    end: datetime
```

#### `ResolvedSegment`

The "Winner" for a specific time range.

```python
class ResolvedSegment(TypedDict):
    entity_id: str
    start: datetime
    end: datetime
    winning_source: str
    winning_priority: int
```

### Enrichment Schemas

#### `AttributeInterval`

Slowly Changing Dimension (SCD) context.

```python
class AttributeInterval(TypedDict):
    entity_id: str
    start: datetime
    end: datetime
    attribute_name: str
    attribute_value: Any
```

#### `EnrichedSegment`

Resolved segment with added context attributes.

```python
class EnrichedSegment(TypedDict):
    entity_id: str
    start: datetime
    end: datetime
    winning_source: str
    attributes: dict[str, Any]
```

### Usage Schemas

#### `UsageStat`

Tidy data for billing/plotting.

```python
class UsageStat(TypedDict):
    timestamp: datetime
    source: str
    count: int
    attributes: dict[str, Any]
```

---

## Part 3: Test Specifications

This section defines the strict behavioral contracts that every backend (Polars, DuckDB, Ibis) must satisfy. Tests are defined in `tests/unit/test_core.py` and `tests/unit/test_rules.py`.

### Test: Sessionization

- **Goal**: Convert point-in-time pings into duration intervals.
- **Method**: `backend.sessionize_stream(events, timeout)`
- **Input**: `list[dict]` with keys `entity_id`, `timestamp`
- **Output**: `list[StandardInterval]`

**Invariants**:

1. **Gap Enforcement**: Time between `end(n)` and `start(n+1)` must be > `timeout`.
2. **Internal Continuity**: Time between any two events within a session must be ≤ `timeout`.
3. **Monotonicity**: `start <= end`.

### Test: Refinery (Clamping)

- **Goal**: Enforce business rules by splitting intervals into 'Valid' and 'Audit' portions.
- **Method**: `backend.apply_clamp_split(target, validators, buffer)`
- **Input**: `target: list[StandardInterval]`, `validators: list[StandardInterval]`
- **Output**: `SplitResult(valid: list[StandardInterval], audit: list[AuditInterval])`

**Invariants**:

1. **Conservation of Time**: `Sum(Target.duration) == Sum(Valid.duration) + Sum(Audit.duration)`.
2. **Conservation of Lineage**: Every `entity_id` in Target must appear in Valid OR Audit.
3. **Strict Truncation**: `Valid.end` must never exceed `Max(Validators.end) + Buffer`.

### Test: Atomic Segmentation

- **Goal**: Decompose overlapping intervals into non-overlapping micro-segments.
- **Method**: `backend.create_atomic_segments(data)`
- **Input**: `list[StandardInterval]` (overlapping allowed)
- **Output**: `list[AtomicSegment]` (strictly contiguous, non-overlapping)

**Invariants**:

1. **Topological Equivalence**: `Union(Input Intervals) == Union(Output Segments)`.
2. **Disjointness**: For any distinct segments A, B, `Intersection(A, B)` is empty.
3. **Completeness**: No gaps exist within the min/max range of any connected component.

### Test: SCD Enrichment

- **Goal**: Augment usage segments with attribute history.
- **Method**: `backend.enrich_segments(usage, context)`
- **Input**: `usage: list[ResolvedSegment]`, `context: list[AttributeInterval]`
- **Output**: `list[EnrichedSegment]`

**Invariants**:

1. **Total Duration Invariant**: `Sum(Usage.duration) == Sum(Enriched.duration)`.
2. **Attribute Integrity**: Every micro-segment in Enriched must carry the attribute value valid for that specific time range.

### Test: Winner Resolution

- **Goal**: Assign a single authoritative source to each time segment.
- **Method**: `backend.resolve_winner(segments, evidence)`
- **Input**: `segments: list[AtomicSegment]`, `evidence: list[StandardInterval]`
- **Output**: `list[ResolvedSegment]`

**Invariants**:

1. **Single Winner**: Every segment has exactly 1 winning source (unless no evidence exists).
2. **Priority Adherence**: `WinningSource.priority <= AnyOtherSource.priority` for that time range.
3. **Temporal Integrity**: `ResolvedSegment.start` and `end` match the input Segment exactly.

### Test: Usage Aggregation

- **Goal**: Transform durations into discrete counts over time.
- **Method**: `backend.calculate_usage_stats(resolved)`
- **Input**: `list[ResolvedSegment]`
- **Output**: `list[UsageStat]`

**Invariants**:

1. **Handover Zero-Sum**: If Source A switches to Source B at time T, `Count(A)` drops by 1 and `Count(B)` rises by 1; TotalCount is invariant.
2. **Reversibility**: `Sum(Count * Duration)` roughly equals `Sum(Input Durations)` (accounting for resolution).
3. **Non-Negativity**: Counts must strictly be ≥ 0.

---

## Part 4: Maintenance & Extension Guide

This section provides cookbook recipes for common maintenance tasks and feature extensions.

### Configuring Rules & Priorities

The behavior of the pipeline is controlled by `config/policies.yaml`. You do not need to change Python code to adjust trust levels or cleaning logic.

#### Setting Source Priorities

Lower numbers indicate higher trust. The resolver will always pick the lowest available priority for any given time segment.

```yaml
source_priorities:
  nodes_db: 1    # Authoritative (Highest Trust)
  nodestatus: 2  # Explicit Status
  metrics: 5     # Inferred (Lowest Trust)
```

#### Configuring Refinery Rules

Rules clean data before it reaches the resolver. Currently supported types: `clamp_horizon`, `require_overlap`.

```yaml
rules:
  # Zombie Protection: Truncate 'target' if 'validators' stop reporting
  - name: "Clamp Zombies"
    rule_type: "clamp_horizon"
    target: "nodes_db"
    validators: ["metrics", "nodestatus"]
    params:
      buffer: "4h"  # Allow 4h of silence before clamping

  # Orphan Protection: Remove instances that don't match a known node
  - name: "Filter Orphans"
    rule_type: "require_overlap"
    target: "instances"
    validators: ["nodes_db"]
```

### Debugging

- **Inspect `pipe.evidence`** to check if adapters loaded data correctly.
- **Inspect `pipe.audit_log`** to see if clamping rules are deleting valid data.
- **Run `pipe.process()` on a filtered subset** to isolate logic errors.
- **Use `InvariantVerifier.verify_conservation(input, output)`** inside custom adapters or rules to catch data loss.
- **Query `ResolvedSegment` for a specific time range** — the `winning_source` and `winning_priority` columns show which rule "won".

### Adding Features

#### 1. Add a New Single-Table Data Source

Use the Generic Adapter. Do not write a Python class for simple column mapping.

```yaml
# config/sources.yaml
- name: "network_logs"
  type: "generic"
  table: "raw_net_logs"
  mapping:
    entity_id: "host_id"
    start: "connect_time"
    end: "disconnect_time"
```

#### 2. Add a Complex Adapter (Multi-table/Filter)

Use a Custom Class. Only necessary if logic requires joins or multi-pass filtering.

1. Create `src/usage_etl/ingest/adapters/complex.py`.
2. Inject `DataCatalog` into the adapter's `__init__`.
3. In `transform()`, use Ibis to perform the join: `jobs.join(users, ...)`.
4. Apply filters (`users.status == 'active'`) before standardizing output.
5. Register in `registry.py`.

#### 3. Add a New Usage Category (e.g., "Ad-Hoc")

**Goal**: Separate "Ad-Hoc" usage from "Committed" usage in final reports.

- **Option A (Source-based)**: Ensure your adapter sets `source="ad_hoc"` in the `StandardInterval`. The pipeline will automatically preserve this distinct source in the final Tidy Data.
- **Option B (Attribute-based)**: If "Ad-Hoc" is a property of a job, use the SCD Enrichment stage. Load a separate table of Job Attributes, and `usage.attributes["type"]` will contain `"ad_hoc"`.

#### 4. Add a New Refinery Rule (e.g., Start Clamping)

**Goal**: Truncate the start of an interval based on a validator stream.

1. Add `CLAMP_START` to the `RuleType` Enum in `src/usage_etl/config/schema.py`.
2. Add a `_handle_start_clamp` method to `src/usage_etl/refinery/engine.py`.
3. Implement `apply_start_clamp_split` in `src/usage_etl/refinery/rules.py`.
4. Update the backend Protocol in `ingest/adapters/base.py` if needed.
