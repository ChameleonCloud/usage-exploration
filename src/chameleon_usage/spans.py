from abc import ABC, abstractmethod

import polars as pl

from chameleon_usage import schemas
from chameleon_usage.utils import SiteConfig

MAX_DATE = pl.datetime(2099, 12, 31)


class RawSpansLoader:
    def __init__(self, site_conf: SiteConfig):
        self.raw_spans = site_conf.raw_spans

    def _load(self, schema: str, table: str) -> pl.LazyFrame:
        path = f"{self.raw_spans}/{schema}.{table}.parquet"
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
        return schemas.NovaInstanceRaw.validate(self._load("nova", "computenodes"))

    @property
    def nova_instances(self) -> pl.LazyFrame:
        return schemas.NovaInstanceRaw.validate(self._load("nova", "instances"))

    @property
    def legacy_usage(self) -> pl.LazyFrame:
        return schemas.NodeUsageReportCache.validate(
            self._load("chameleon_usage", "node_usage_report_cache")
        )


class BaseSpanSource(ABC):
    def __init__(
        self, loader: "RawSpansLoader", source_name: str, resource_id_col: str
    ):
        self.loader = loader
        self.source_name = source_name
        self.resource_id_col = resource_id_col

    @abstractmethod
    def get_raw_events(self) -> pl.LazyFrame:
        """
        Must return a LazyFrame with at least:
        - start_date
        - [resource_id_col]
        - Plus any columns needed for end_signals
        """
        pass

    @property
    @abstractmethod
    def end_signals(self) -> list[pl.Expr]:
        """List of columns/expressions to check for the earliest end date."""
        pass

    def get_spans(self) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        # 1. Load Source-Specific Data (The "Messy" Part)
        raw = self.get_raw_events()

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


class BlazarCommitmentSource(BaseSpanSource):
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
        return [pl.col("deleted_at")]
