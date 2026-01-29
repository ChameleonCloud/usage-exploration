# Technical Design & Implementation: Cloud Resource Timeline Reconstruction

**Version:** 1.0.0
**Context:** `src/chameleon_usage/`
**Objective:** Reconstruct historical resource utilization from inconsistent, prioritized data sources using the Painter's Algorithm.

---

## 1. Directory Structure

```text
src/
└── chameleon_usage/
    ├── __init__.py
    ├── config.yaml          # Priority Rules
    ├── constants.py         # Magic Strings (Enums)
    ├── models.py            # Data Contracts (Pandera)
    ├── adapters.py          # Ingestion (Raw -> Facts)
    ├── engine.py            # Synthesis (Painter's Algo)
    └── analytics.py         # Reporting (Timeline -> Usage)

```

---

## 2. Configuration (`config.yaml`)

This defines the "Rules as Data." The order of sources in the list defines the priority (First = Winner).

```yaml
resources:
  compute_instance:
    sources:
      - manual_override   # Prio 1: Humans (e.g. "Ignore this VM")
      - nova_db           # Prio 2: System of Record
      - inference_engine  # Prio 3: Gap filling

```

---

## 3. Constants & Contracts

### `src/chameleon_usage/constants.py`

```python
from enum import StrEnum

class SourceName(StrEnum):
    MANUAL   = "manual_override"
    NOVA_DB  = "nova_db"
    INFERENCE = "inference_engine"

class Tombstone(StrEnum):
    TERMINATED = "terminated"  # Active Kill Signal

```

### `src/chameleon_usage/models.py`

```python
import pandera.polars as pa
import polars as pl

# --- 1. Raw Input Contracts (Validate your dumps) ---
class NovaComputeRaw(pa.DataFrameModel):
    id: int
    created_at: pl.Datetime
    deleted_at: pl.Datetime = pa.Field(nullable=True)
    host: str

# --- 2. Internal Fact Contract (The Engine's Language) ---
class FactSchema(pa.DataFrameModel):
    timestamp: pl.Datetime
    entity_id: str
    source: str
    value: str = pa.Field(nullable=True) # Null = Yield (Stop Override), "terminated" = Kill

```

---

## 4. Ingestion Layer (`adapters.py`)

Handles the conversion of **Durations** (Start/End) into **Events** (Start/Stop). Critical: Must use `Tombstone.TERMINATED` for explicit deletions, not `None`.

```python
import polars as pl
from .models import FactSchema
from .constants import SourceName, Tombstone

class NovaComputeAdapter:
    def __init__(self, raw_df: pl.LazyFrame):
        self.raw_df = raw_df

    def to_facts(self) -> pl.LazyFrame:
        base = self.raw_df.select([
            pl.col("host").alias("entity_id"),
            pl.col("created_at"),
            pl.col("deleted_at"),
            pl.lit(SourceName.NOVA_DB).alias("source")
        ])

        # Event 1: Start (Active)
        starts = base.select([
            pl.col("created_at").alias("timestamp"),
            pl.col("entity_id"),
            pl.lit("active").alias("value"),
            pl.col("source")
        ])

        # Event 2: End (Terminated) - The Tombstone
        ends = base.filter(pl.col("deleted_at").is_not_null()).select([
            pl.col("deleted_at").alias("timestamp"),
            pl.col("entity_id"),
            pl.lit(Tombstone.TERMINATED).alias("value"), 
            pl.col("source")
        ])

        return pl.concat([starts, ends])

```

---

## 5. Synthesis Engine (`engine.py`)

The core logic. Implements **Pivot -> Paint -> Coalesce**.

```python
import polars as pl
from typing import List
from .constants import Tombstone

class TimelineBuilder:
    def __init__(self, priority_order: List[str]):
        self.sources = priority_order  # e.g. ["manual_override", "nova_db"]

    def build(self, facts: pl.LazyFrame) -> pl.LazyFrame:
        return (
            facts
            # 1. PIVOT: Create parallel timelines
            .collect().pivot(
                values="value",
                index=["timestamp", "entity_id"],
                on="source",
                aggregate_function="first"
            ).lazy()
            .sort(["entity_id", "timestamp"])

            # 2. PAINT: Forward fill each source independently
            #    (Tombstones propagate, Nulls do not)
            .with_columns([
                pl.col(src).forward_fill().over("entity_id")
                for src in self.sources if src in facts.columns
            ])

            # 3. RESOLVE: Coalesce based on priority list
            .with_columns(
                pl.coalesce([
                    pl.col(src) for src in self.sources if src in facts.columns
                ]).alias("winner_raw")
            )

            # 4. CLEANUP: Convert Tombstones back to Nulls for clean output
            .with_columns(
                pl.when(pl.col("winner_raw") == Tombstone.TERMINATED)
                .then(None)
                .otherwise(pl.col("winner_raw"))
                .alias("state")
            )
            .select(["timestamp", "entity_id", "state"])
        )

```

---

## 6. Analytics Layer (`analytics.py`)

Converts the Timeline (State) into Concurrency Metrics (Numbers) using Differentiation/Integration.

```python
import polars as pl

def calculate_concurrency(timeline: pl.LazyFrame) -> pl.LazyFrame:
    return (
        timeline
        # 1. Map State -> Number
        .with_columns(
            pl.when(pl.col("state") == "active").then(1)
            .otherwise(0)
            .alias("val")
        )
        # 2. Calculate Delta (Change per entity)
        .with_columns(
            (pl.col("val") - pl.col("val").shift(1).over("entity_id").fill_null(0))
            .alias("delta")
        )
        # 3. Aggregate (Sum deltas across all entities)
        .group_by("timestamp")
        .agg(pl.col("delta").sum())
        .sort("timestamp")
        # 4. Integrate (Running Total)
        .with_columns(
            pl.col("delta").cum_sum().alias("total_quantity")
        )
        .select(["timestamp", "total_quantity"])
    )

```

---

## 7. Execution (`main.py`)

```python
import polars as pl
from src.chameleon_usage.adapters import NovaComputeAdapter
from src.chameleon_usage.engine import TimelineBuilder

# Usage Example
# adapter = NovaComputeAdapter(pl.scan_parquet("nova.parquet"))
# facts = adapter.to_facts()
# builder = TimelineBuilder(priority_order=["manual_override", "nova_db"])
# timeline = builder.build(facts)
# usage = calculate_concurrency(timeline)
# usage.collect()

```
