from datetime import datetime

import pandera.polars as pa
import polars as pl


class FactSchema(pa.DataFrameModel):
    timestamp: pl.Datetime
    entity_id: str
    value: str = pa.Field(nullable=True)  # Null = Reset/Delete
    source: str
