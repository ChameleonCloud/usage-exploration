from abc import ABC, abstractmethod
from pathlib import Path

import pandera as pa
import polars as pl

from chameleon_usage import schemas
from chameleon_usage.utils import SiteConfig

MAX_DATE = pl.datetime(2099, 12, 31)


class RawSpansLoader:
    def __init__(self, site_conf: SiteConfig):
        self.raw_spans = site_conf.raw_spans

    def _load(self, schema: str, table: str) -> pl.LazyFrame:
        path = Path(self.raw_spans) / f"{schema}.{table}.parquet"
        if not path.exists():
            raise FileNotFoundError(
                (f"Missing parquet: {path} Did you dump_db? Table name right?")
            )

        return pl.scan_parquet(path)

    @property
    def blazar_hosts(self) -> pl.LazyFrame:
        return schemas.BlazarHostRaw.validate(self._load("blazar", "computehosts"))

    @property
    def blazar_allocations(self) -> pl.LazyFrame:
        return schemas.BlazarAllocationRaw.validate(
            self._load("blazar", "computehost_allocations")
        )

    @property
    def blazar_leases(self) -> pl.LazyFrame:
        return schemas.BlazarLeaseRaw.validate(self._load("blazar", "leases"))

    @property
    def blazar_reservations(self) -> pl.LazyFrame:
        return schemas.BlazarReservationRaw.validate(
            self._load("blazar", "reservations")
        )

    @property
    def nova_computenodes(self) -> pl.LazyFrame:
        return schemas.NovaHostRaw.validate(self._load("nova", "compute_nodes"))

    @property
    def nova_instances(self) -> pl.LazyFrame:
        return schemas.NovaInstanceRaw.validate(self._load("nova", "instances"))

    @property
    def nova_services(self) -> pl.LazyFrame:
        return schemas.NovaServiceRaw.validate(self._load("nova", "services"))

    @property
    def legacy_usage(self) -> pl.LazyFrame:
        return schemas.NodeUsageReportCache.validate(
            self._load("chameleon_usage", "node_usage_report_cache")
        )


class BaseSpanSource(ABC):
    required_colums = {
        "hypervisor_hostname",
        "entity_id",
        "start_date",
    }

    def __init__(
        self, loader: "RawSpansLoader", source_name: str, resource_id_col: str
    ):
        """
        Store the loader and the three config knobs used by `get_spans()`.

        - loader: where raw tables come from
        - source_name: label written to the output `source` column
        - resource_id_col: raw column validated and renamed to `resource_id`
        """
        self.loader = loader
        self.source_name = source_name
        self.resource_id_col = resource_id_col

    @staticmethod
    def _require_columns(lf: pl.LazyFrame, required: set[str], where: str) -> None:
        names = set(lf.collect_schema().names())
        missing = required - names
        if missing:
            raise ValueError(
                f"{where}.get_raw_events(): missing {sorted(missing)}; "
                f"required={sorted(required)}"
            )

    @abstractmethod
    def get_raw_events(self) -> pl.LazyFrame:
        """
        Must return a LazyFrame with at least:
        - start_date
        - [resource_id_col]
        - Plus any columns needed for end_signals
        """

        raise NotImplementedError

    @property
    @abstractmethod
    def end_signals(self) -> list[pl.Expr]:
        """List of columns/expressions to check for the earliest end date."""
        raise NotImplementedError

    def get_spans(self) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        # 1. Load Source-Specific Data (The "Messy" Part)
        raw = self.get_raw_events()

        self._require_columns(raw, self.required_colums, self.source_name)
        # validate that columns exist
        try:
            raw = schemas.RawEventBase.validate(raw, lazy=True)
        except pa.errors.SchemaErrors as e:
            raise ValueError(
                f"{self.source_name}: RawEventBase failed\n{e.failure_cases}"
            ) from e

        # 2. Calculate Robust End (The "Min" Logic)
        # We fill nulls with MAX_DATE to ensure min() picks real dates.
        signals = [s.fill_null(MAX_DATE) for s in self.end_signals]

        tagged = raw.with_columns(calc_end=pl.min_horizontal(signals)).with_columns(
            # 3. Standardized Validation Tags
            data_status=pl.when(pl.col("start_date").is_null())
            .then(pl.lit("INVALID_MISSING_START"))
            .when(pl.col(self.resource_id_col).is_null())
            .then(pl.lit("INVALID_MISSING_RESOURCE"))
            .when(pl.col("calc_end") <= pl.col("start_date"))
            .then(pl.lit("INVALID_PHANTOM_SPAN"))
            .otherwise(pl.lit("VALID_COMMITMENT"))
        )
        tagged = tagged.with_columns(source=pl.lit(self.source_name))

        # 4. Split - Valid Spans (Strict Schema)
        valid = tagged.filter(pl.col("data_status") == "VALID_COMMITMENT").select(
            pl.col(self.resource_id_col).alias("resource_id"),
            pl.col("start_date").alias("start"),
            pl.col("calc_end").alias("end"),
            pl.lit(self.source_name).alias("source"),
        )

        # 5. Split - Audit Log (Everything)
        audit = tagged.filter(pl.col("data_status") != "VALID_COMMITMENT")

        return valid, audit


class NovaHostSource(BaseSpanSource):
    """
    Source Loader for nova computehosts.

    This span type produces "total" usage/capacity.
    TODO: Handling of missing data via nova services
    """

    def __init__(self, loader):
        super().__init__(loader, "nova_computenodes", "hypervisor_hostname")

    def get_raw_events(self) -> pl.LazyFrame:
        return self.loader.nova_computenodes.select(
            pl.col("hypervisor_hostname"),
            pl.col("id").alias("entity_id"),
            pl.col("created_at").alias("start_date"),
            pl.col("deleted_at"),
        )

    @property
    def end_signals(self) -> list[pl.Expr]:
        return [pl.col("deleted_at")]


class BlazarHostSource(BaseSpanSource):
    """
    Source Loader for blazar computehosts.

    This span type produces "reservable" usage/capacity.
    """

    def __init__(self, loader):
        super().__init__(loader, "blazar_computehosts", "hypervisor_hostname")

    def get_raw_events(self) -> pl.LazyFrame:
        return self.loader.blazar_hosts.select(
            pl.col("hypervisor_hostname"),
            pl.col("id").alias("entity_id"),
            pl.col("created_at").alias("start_date"),
            pl.col("deleted_at"),
        )

    @property
    def end_signals(self) -> list[pl.Expr]:
        return [pl.col("deleted_at")]


class BlazarCommitmentSource(BaseSpanSource):
    """
    Source Loader for blazar allocations.

    This span type produces "committed" usage/capacity.
    """

    def __init__(self, loader):
        super().__init__(loader, "blazar_allocations", "hypervisor_hostname")

    def get_raw_events(self) -> pl.LazyFrame:
        # The specific join logic moved here
        return (
            self.loader.blazar_allocations.join(
                self.loader.blazar_reservations,
                how="left",
                left_on="reservation_id",
                right_on="id",
                suffix="_res",
            )
            .join(
                self.loader.blazar_leases,
                how="left",
                left_on="lease_id",
                right_on="id",
                suffix="_lease",
            )
            .join(
                self.loader.blazar_hosts,
                how="left",
                left_on="compute_host_id",
                right_on="id",
            )
            .with_columns(pl.col("id").alias("entity_id"))  # use allocation pk
            # Standardize 'created_at' to 'start_date' for the Base Class contract
            # (Assuming blazar_allocations already has start_date, but ensuring consistency)
        )

    @property
    def end_signals(self) -> list[pl.Expr]:
        # The specific Blazar hierarchy
        return [
            pl.col("end_date"),  # Scheduled End
            pl.col("deleted_at"),  # Allocation Deleted
            pl.col("deleted_at_res"),  # Reservation Deleted
            pl.col("deleted_at_lease"),  # Lease Deleted
        ]


class NovaOccupiedSource(BaseSpanSource):
    """
    Source Loader for nova instances.

    This span type produces "occupied" usage/capacity.
    TODO: Later work will *also* prodice active usage.
    """

    def __init__(self, loader):
        # Note: Nova often uses 'host' or 'node' instead of hypervisor_hostname
        super().__init__(loader, "nova_instances", "hypervisor_hostname")

    def get_raw_events(self) -> pl.LazyFrame:
        return self.loader.nova_instances.select(
            pl.col("host").alias("hypervisor_hostname"),  # Rename to match contract
            pl.col("created_at").alias("start_date"),  # Rename to match contract
            pl.col("deleted_at"),
            pl.col("uuid").alias("entity_id"),
        )

    @property
    def end_signals(self) -> list[pl.Expr]:
        # Nova only has one way to end: Deletion
        # TODO: need to handle instance_actions_lifecyle, terminated at, ...
        return [pl.col("deleted_at")]
