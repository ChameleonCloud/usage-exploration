from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional, Type

import pandera.polars as pa
import polars as pl


class Inputs(Enum):
    NOVA_COMPUTE = auto()
    NOVA_SERVICE = auto()
    NOVA_INSTANCES = auto()
    BLAZAR_HOSTS = auto()
    BLAZAR_ALLOC = auto()
    BLAZAR_RES = auto()
    BLAZAR_LEASES = auto()


@dataclass(frozen=True)
class SourceConfig:
    quantity_type: str
    source: str
    # Map of { StandardColumn : RawColumn }
    # e.g. { C.ENTITY_ID : "hypervisor_hostname" }
    col_map: dict[str, str]
    filter_expr: pl.Expr | None = None


@dataclass
class AdapterDef:
    """Map input files to adapter."""

    adapter_class: Type
    required_inputs: List[Inputs]
    config: SourceConfig


@dataclass
class FileResource:
    filename: str
    model: Type[pa.DataFrameModel]
